#!/usr/bin/env tsx

import { Collection, MongoClient } from 'mongodb';
import crypto from 'node:crypto';


const MONGO_A_URI =
  process.env.MONGO_A_URI ||
  'mongodb://localhost:27017/syncdb?directConnection=true';
const MONGO_B_URI =
  process.env.MONGO_B_URI ||
  'mongodb://localhost:27018/syncdb?directConnection=true';

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
} as const;

function info(msg: string): void {
  console.log(`${colors.blue}ℹ${colors.reset} ${msg}`);
}
function success(msg: string): void {
  console.log(`${colors.green}✓${colors.reset} ${msg}`);
}
function error(msg: string): void {
  console.log(`${colors.red}✗${colors.reset} ${msg}`);
}
function warn(msg: string): void {
  console.log(`${colors.yellow}⚠${colors.reset} ${msg}`);
}

function sha256(str: string): string {
  return crypto.createHash('sha256').update(str).digest('hex');
}

function stableStringify(value: unknown): string {
  if (value === null || value === undefined) return 'null';
  if (typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) {
    return '[' + value.map(stableStringify).join(',') + ']';
  }
  const keys = Object.keys(value).sort();
  const parts: string[] = [];
  for (const k of keys) {
    if (k === '__h' || k.startsWith('_')) continue; // Skip MongoDB internal fields
    parts.push(
      JSON.stringify(k) +
        ':' +
        stableStringify((value as Record<string, unknown>)[k]),
    );
  }
  return '{' + parts.join(',') + '}';
}

interface CollectionHashResult {
  count: number;
  hash: string;
  docHashes?: string[];
}

async function computeCollectionHash(
  collection: Collection,
  collName: string,
): Promise<CollectionHashResult> {
  info(`Computing hash for collection: ${collName}...`);

  const docs = await collection.find({}).sort({ _id: 1 }).toArray();
  info(`  Found ${docs.length} documents`);

  if (docs.length === 0) {
    return { count: 0, hash: 'EMPTY' };
  }

  // Create hash from all document hashes in order
  const docHashes = docs.map((doc) => sha256(stableStringify(doc)));
  const combinedHash = sha256(docHashes.join('|'));

  return { count: docs.length, hash: combinedHash, docHashes };
}

async function quickStateComparison(): Promise<boolean> {
  let clientA: MongoClient | undefined;
  let clientB: MongoClient | undefined;

  try {
    info('Connecting to MongoDB instances...');
    clientA = new MongoClient(MONGO_A_URI);
    clientB = new MongoClient(MONGO_B_URI);

    await clientA.connect();
    await clientB.connect();
    success('Connected to both MongoDB instances\n');

    const dbA = clientA.db('syncdb');
    const dbB = clientB.db('syncdb');

    console.log('='.repeat(60));
    console.log(
      `${colors.cyan}Computing State Hash for MongoDB A${colors.reset}`,
    );
    console.log('='.repeat(60));

    const articlesA = dbA.collection('articles');
    const hashA = await computeCollectionHash(articlesA, 'articles');

    console.log('\n' + '='.repeat(60));
    console.log(
      `${colors.cyan}Computing State Hash for MongoDB B${colors.reset}`,
    );
    console.log('='.repeat(60));

    const articlesB = dbB.collection('articles');
    const hashB = await computeCollectionHash(articlesB, 'articles');

    console.log('\n' + '='.repeat(60));
    console.log(`${colors.cyan}Comparison Results${colors.reset}`);
    console.log('='.repeat(60) + '\n');

    console.log('Collection: articles');
    console.log(`  MongoDB A: ${hashA.count} documents`);
    console.log(`  MongoDB B: ${hashB.count} documents`);
    console.log(`  Hash A: ${hashA.hash}`);
    console.log(`  Hash B: ${hashB.hash}`);
    console.log('');

    if (hashA.hash === hashB.hash) {
      success('✨ HASHES MATCH! Databases are synchronized! ✨');

      // Show sample of first few doc hashes
      if (hashA.docHashes && hashA.docHashes.length > 0) {
        console.log('\nFirst 5 document hashes (A):');
        hashA.docHashes.slice(0, 5).forEach((h, i) => {
          console.log(`  ${i + 1}. ${h.substring(0, 16)}...`);
        });
      }

      return true;
    } else {
      error('HASHES DO NOT MATCH! Databases are out of sync!');

      if (hashA.count !== hashB.count) {
        warn(
          `Document count mismatch: A has ${hashA.count}, B has ${hashB.count}`,
        );
      } else {
        warn('Same number of documents but different content');

        // Find first difference
        if (hashA.docHashes && hashB.docHashes) {
          for (let i = 0; i < hashA.docHashes.length; i++) {
            if (hashA.docHashes[i] !== hashB.docHashes[i]) {
              warn(`First difference at document index ${i + 1}`);
              console.log(`  A: ${hashA.docHashes[i]}`);
              console.log(`  B: ${hashB.docHashes[i]}`);
              break;
            }
          }
        }
      }

      return false;
    }
  } catch (err) {
    const errObj = err as Error;
    error(`ERROR: ${errObj.message}`);
    console.error(errObj);
    return false;
  } finally {
    if (clientA) await clientA.close();
    if (clientB) await clientB.close();
    info('\nClosed MongoDB connections');
  }
}

console.log('\n╔' + '═'.repeat(58) + '╗');
console.log(
  '║' +
    ' '.repeat(15) +
    colors.cyan +
    'Quick State Hash Check' +
    colors.reset +
    ' '.repeat(21) +
    '║',
);
console.log('╚' + '═'.repeat(58) + '╝\n');

quickStateComparison()
  .then((match) => {
    console.log('');
    process.exit(match ? 0 : 1);
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
