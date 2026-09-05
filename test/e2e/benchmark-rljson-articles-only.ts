#!/usr/bin/env tsx

/**
 * RLJSON Extraction Benchmark - Articles Only
 *
 * Tests RLJSON extraction on just the articles collection
 * Skips internal sync/state collections to focus on real data
 */

import { BsMem } from '@rljson/bs';

import { MongoClient } from 'mongodb';

import { MongoScanner } from '../../src/index.js';

const MONGO_A_URI =
  process.env.MONGO_A_URI ||
  'mongodb://localhost:27017/syncdb?directConnection=true';

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
  bold: '\x1b[1m',
} as const;

function formatTime(ms: number): string {
  if (ms < 1000) return `${ms.toFixed(0)}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
  return `${(ms / 60000).toFixed(2)}min`;
}

function formatNumber(n: number): string {
  return n.toLocaleString('en-US');
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)}KB`;
  if (bytes < 1024 * 1024 * 1024)
    return `${(bytes / 1024 / 1024).toFixed(2)}MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)}GB`;
}

async function benchmark(): Promise<void> {
  let client: MongoClient | undefined;

  try {
    console.log('\n' + '═'.repeat(70));
    console.log(
      `${colors.bold}${colors.cyan}  RLJSON Extraction - Articles Collection Only${colors.reset}`,
    );
    console.log('═'.repeat(70) + '\n');

    console.log(`${colors.blue}ℹ${colors.reset} Connecting to MongoDB...`);
    client = new MongoClient(MONGO_A_URI);
    await client.connect();
    const db = client.db('syncdb');
    console.log(`${colors.green}✓${colors.reset} Connected\n`);

    // Get articles collection stats
    const count = await db.collection('articles').countDocuments({});
    const stats = await db.command({ collStats: 'articles' });

    console.log('─'.repeat(70));
    console.log(
      `${colors.cyan}${colors.bold}Collection: articles${colors.reset}`,
    );
    console.log('─'.repeat(70));
    console.log(`  Documents: ${formatNumber(count)}`);
    console.log(`  Size: ${formatBytes(stats.size)}`);
    console.log(`  Avg doc size: ${formatBytes(stats.size / count)}`);
    console.log('');

    // Initialize scanner with blob storage
    const bs = new BsMem();
    const scanner = new MongoScanner(db, bs);

    console.log('─'.repeat(70));
    console.log(`${colors.cyan}${colors.bold}RLJSON Extraction${colors.reset}`);
    console.log('─'.repeat(70));
    console.log(`${colors.blue}ℹ${colors.reset} Starting extraction...`);
    console.log('');

    const startTime = Date.now();
    let progressInterval = setInterval(() => {
      const elapsed = Date.now() - startTime;
      process.stdout.write(
        `\r${colors.yellow}⏳${colors.reset} Extracting... ${formatTime(elapsed)} elapsed`,
      );
    }, 1000);

    const tree = await scanner.scan();

    clearInterval(progressInterval);
    const extractTime = Date.now() - startTime;

    console.log(
      `\r${colors.green}✓${colors.reset} Extraction completed in ${colors.bold}${formatTime(extractTime)}${colors.reset}`,
    );
    console.log('');

    // Analyze results
    console.log('─'.repeat(70));
    console.log(`${colors.cyan}${colors.bold}Results${colors.reset}`);
    console.log('─'.repeat(70));

    console.log(`  Root Hash: ${tree.rootHash}`);
    console.log(`  Total Tree Nodes: ${formatNumber(tree.trees.size)}`);
    console.log('');

    // Count nodes by type
    let dbNodes = 0;
    let collNodes = 0;
    let docNodes = 0;
    let blobCount = 0;
    let totalBlobSize = 0;

    for (const [, node] of tree.trees) {
      const meta = node.meta as any;
      if (meta?.type === 'database') dbNodes++;
      else if (meta?.type === 'collection') collNodes++;
      else if (meta?.type === 'document') {
        docNodes++;
        if (meta?.blobId) {
          blobCount++;
          const blob = await bs.getBlob(meta.blobId);
          totalBlobSize += blob.content.length;
        }
      }
    }

    console.log(`  Node Distribution:`);
    console.log(`    Database nodes: ${dbNodes}`);
    console.log(`    Collection nodes: ${collNodes}`);
    console.log(`    Document nodes: ${formatNumber(docNodes)}`);
    console.log('');

    console.log(`  Blob Storage:`);
    console.log(`    Total blobs: ${formatNumber(blobCount)}`);
    console.log(`    Total size: ${formatBytes(totalBlobSize)}`);
    console.log(`    Avg size: ${formatBytes(totalBlobSize / blobCount)}`);
    console.log('');

    // Performance metrics
    console.log('─'.repeat(70));
    console.log(`${colors.cyan}${colors.bold}Performance${colors.reset}`);
    console.log('─'.repeat(70));

    const docsPerSec = count / (extractTime / 1000);
    const bytesPerSec = stats.size / (extractTime / 1000);
    const timePerDoc = extractTime / count;

    console.log(`  Total time: ${formatTime(extractTime)}`);
    console.log(`  Documents: ${formatNumber(count)}`);
    console.log(`  Data size: ${formatBytes(stats.size)}`);
    console.log('');
    console.log(`  Throughput:`);
    console.log(`    ${formatNumber(Math.round(docsPerSec))} docs/sec`);
    console.log(`    ${formatBytes(bytesPerSec)}/sec`);
    console.log(`    ${timePerDoc.toFixed(3)}ms per document`);
    console.log('');

    console.log(`  Storage Efficiency:`);
    console.log(`    Original: ${formatBytes(stats.size)}`);
    console.log(`    Blobs: ${formatBytes(totalBlobSize)}`);
    const ratio = (totalBlobSize / stats.size) * 100;
    console.log(`    Ratio: ${ratio.toFixed(1)}%`);
    console.log('');

    // Summary
    console.log('═'.repeat(70));
    console.log(`${colors.bold}${colors.green}Summary${colors.reset}`);
    console.log('═'.repeat(70));
    console.log(`  ✓ Extracted ${formatNumber(count)} documents`);
    console.log(
      `  ✓ Stored ${formatNumber(blobCount)} blobs (${formatBytes(totalBlobSize)})`,
    );
    console.log(
      `  ✓ Created ${formatNumber(tree.trees.size)} hash-chained nodes`,
    );
    console.log(
      `  ✓ Throughput: ${formatNumber(Math.round(docsPerSec))} docs/sec`,
    );
    console.log(`  ✓ Time: ${formatTime(extractTime)}`);
    console.log('═'.repeat(70) + '\n');
  } catch (err) {
    console.error(`${colors.reset}\n✗ Error:`, err);
    process.exit(1);
  } finally {
    if (client) {
      await client.close();
    }
  }
}

benchmark().catch(console.error);
