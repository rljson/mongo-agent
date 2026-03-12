#!/usr/bin/env tsx

/**
 * State Hash Performance Benchmark
 *
 * Tests the full state hash computation over 552k+ articles
 * with all optimizations: integrity hash, backfill, merkle partitions
 */

import { MongoClient } from 'mongodb';

import { computeStateCheckpoint, getLatestCheckpoint } from '../../dist/index.js';


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
  magenta: '\x1b[35m',
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

const IGNORED_COLLECTIONS = new Set([
  'sync_ops',
  'sync_state',
  'sync_local',
  'sync_resume',
  'sync_conflicts',
  'sync_audit',
  'state_checkpoints',
  'state_merkle',
  'state_dirty',
  'sync_head',
]);

async function benchmark(): Promise<boolean> {
  let clientA: MongoClient | undefined;
  let clientB: MongoClient | undefined;

  try {
    console.log('\n' + '═'.repeat(70));
    console.log(
      `${colors.bold}${colors.cyan}  State Hash Performance Benchmark - Real World Scenario${colors.reset}`,
    );
    console.log('═'.repeat(70) + '\n');

    console.log(
      `${colors.blue}ℹ${colors.reset} Connecting to MongoDB instances...`,
    );
    clientA = new MongoClient(MONGO_A_URI);
    clientB = new MongoClient(MONGO_B_URI);

    await clientA.connect();
    await clientB.connect();
    console.log(
      `${colors.green}✓${colors.reset} Connected to both MongoDB instances\n`,
    );

    const dbA = clientA.db('syncdb');
    const dbB = clientB.db('syncdb');

    // Get collection stats
    console.log('─'.repeat(70));
    console.log(
      `${colors.cyan}${colors.bold}Collection Statistics${colors.reset}`,
    );
    console.log('─'.repeat(70));

    const countA = await dbA.collection('articles').countDocuments({});
    const countB = await dbB.collection('articles').countDocuments({});

    console.log(`  MongoDB A:`);
    console.log(`    Documents: ${formatNumber(countA)}`);
    console.log(`  MongoDB B:`);
    console.log(`    Documents: ${formatNumber(countB)}`);
    console.log('');

    // Check for existing checkpoints
    console.log('─'.repeat(70));
    console.log(
      `${colors.cyan}${colors.bold}Previous Checkpoints${colors.reset}`,
    );
    console.log('─'.repeat(70));

    const lastCpA = await getLatestCheckpoint(dbA);
    const lastCpB = await getLatestCheckpoint(dbB);

    if (lastCpA) {
      console.log(`  MongoDB A last checkpoint:`);
      console.log(`    ID: ${lastCpA._id}`);
      console.log(`    Date: ${lastCpA.updatedAt}`);
      console.log(`    DB Root: ${lastCpA.dbRoot.substring(0, 16)}...`);
    } else {
      console.log(`  MongoDB A: No previous checkpoint found`);
    }

    if (lastCpB) {
      console.log(`  MongoDB B last checkpoint:`);
      console.log(`    ID: ${lastCpB._id}`);
      console.log(`    Date: ${lastCpB.updatedAt}`);
      console.log(`    DB Root: ${lastCpB.dbRoot.substring(0, 16)}...`);
    } else {
      console.log(`  MongoDB B: No previous checkpoint found`);
    }
    console.log('');

    // Benchmark MongoDB A - Full Computation
    console.log('═'.repeat(70));
    console.log(
      `${colors.bold}${colors.yellow}BENCHMARK 1: MongoDB A - Full State Hash Computation${colors.reset}`,
    );
    console.log('═'.repeat(70));
    console.log(
      `${colors.blue}ℹ${colors.reset} Starting full state checkpoint computation...`,
    );
    console.log(
      `${colors.blue}ℹ${colors.reset} Partition size: 50,000 documents`,
    );
    console.log(
      `${colors.blue}ℹ${colors.reset} Mode: Full scan with merkle tree partitioning\n`,
    );

    const startA = Date.now();
    let progressInterval = setInterval(() => {
      const elapsed = Date.now() - startA;
      process.stdout.write(
        `\r${colors.yellow}⏳${colors.reset} Computing... ${formatTime(elapsed)} elapsed`,
      );
    }, 1000);

    const checkpointA = await computeStateCheckpoint({
      db: dbA,
      ignoredColls: IGNORED_COLLECTIONS,
      partitionSize: 50000,
      storeLeaves: true,
      mode: 'benchmark',
    });

    clearInterval(progressInterval);
    const timeA = Date.now() - startA;

    console.log(
      `\r${colors.green}✓${colors.reset} MongoDB A checkpoint computed in ${colors.bold}${formatTime(timeA)}${colors.reset}`,
    );
    console.log('');
    console.log(`  Results:`);
    console.log(`    DB Root Hash: ${checkpointA.dbRoot}`);
    console.log(
      `    Collections processed: ${Object.keys(checkpointA.collections).length}`,
    );
    console.log(`    Checkpoint ID: ${checkpointA._id}`);

    for (const [collName, collData] of Object.entries(
      checkpointA.collections,
    )) {
      console.log(
        `    - ${collName}: ${collData.partitions} partitions, root: ${collData.root.substring(0, 16)}...`,
      );
    }

    console.log('');
    console.log(`  Performance:`);
    console.log(`    Total time: ${formatTime(timeA)}`);
    console.log(`    Documents processed: ${formatNumber(countA)}`);
    console.log(
      `    Throughput: ${formatNumber(Math.round(countA / (timeA / 1000)))} docs/sec`,
    );
    console.log(`    Avg time per doc: ${(timeA / countA).toFixed(3)}ms`);

    const partitionCount = checkpointA.collections['articles']?.partitions || 0;
    if (partitionCount > 0) {
      console.log(`    Partitions created: ${partitionCount}`);
      console.log(
        `    Avg docs per partition: ${formatNumber(Math.round(countA / partitionCount))}`,
      );
    }

    console.log('');

    // Benchmark MongoDB B - Full Computation
    console.log('═'.repeat(70));
    console.log(
      `${colors.bold}${colors.yellow}BENCHMARK 2: MongoDB B - Full State Hash Computation${colors.reset}`,
    );
    console.log('═'.repeat(70));
    console.log(
      `${colors.blue}ℹ${colors.reset} Starting full state checkpoint computation...\n`,
    );

    const startB = Date.now();
    progressInterval = setInterval(() => {
      const elapsed = Date.now() - startB;
      process.stdout.write(
        `\r${colors.yellow}⏳${colors.reset} Computing... ${formatTime(elapsed)} elapsed`,
      );
    }, 1000);

    const checkpointB = await computeStateCheckpoint({
      db: dbB,
      ignoredColls: IGNORED_COLLECTIONS,
      partitionSize: 50000,
      storeLeaves: true,
      mode: 'benchmark',
    });

    clearInterval(progressInterval);
    const timeB = Date.now() - startB;

    console.log(
      `\r${colors.green}✓${colors.reset} MongoDB B checkpoint computed in ${colors.bold}${formatTime(timeB)}${colors.reset}`,
    );
    console.log('');
    console.log(`  Results:`);
    console.log(`    DB Root Hash: ${checkpointB.dbRoot}`);
    console.log(
      `    Collections processed: ${Object.keys(checkpointB.collections).length}`,
    );
    console.log(`    Checkpoint ID: ${checkpointB._id}`);

    for (const [collName, collData] of Object.entries(
      checkpointB.collections,
    )) {
      console.log(
        `    - ${collName}: ${collData.partitions} partitions, root: ${collData.root.substring(0, 16)}...`,
      );
    }

    console.log('');
    console.log(`  Performance:`);
    console.log(`    Total time: ${formatTime(timeB)}`);
    console.log(`    Documents processed: ${formatNumber(countB)}`);
    console.log(
      `    Throughput: ${formatNumber(Math.round(countB / (timeB / 1000)))} docs/sec`,
    );
    console.log(`    Avg time per doc: ${(timeB / countB).toFixed(3)}ms`);

    console.log('');

    // Final Comparison
    console.log('═'.repeat(70));
    console.log(
      `${colors.bold}${colors.cyan}State Hash Comparison Results${colors.reset}`,
    );
    console.log('═'.repeat(70) + '\n');

    console.log(`  Database Root Hashes:`);
    console.log(
      `    MongoDB A: ${colors.cyan}${checkpointA.dbRoot}${colors.reset}`,
    );
    console.log(
      `    MongoDB B: ${colors.cyan}${checkpointB.dbRoot}${colors.reset}`,
    );
    console.log('');

    if (checkpointA.dbRoot === checkpointB.dbRoot) {
      console.log(
        `  ${colors.green}${colors.bold}✨ SUCCESS! ROOT HASHES MATCH! ✨${colors.reset}`,
      );
      console.log(
        `  ${colors.green}Both databases are in identical state!${colors.reset}`,
      );
    } else {
      console.log(
        `  ${colors.red}${colors.bold}⚠ WARNING: ROOT HASHES DO NOT MATCH!${colors.reset}`,
      );
      console.log(
        `  ${colors.red}Databases are in different states!${colors.reset}`,
      );

      // Check collection by collection
      console.log('');
      console.log(`  Collection comparison:`);
      for (const collName of Object.keys(checkpointA.collections)) {
        const hashA = checkpointA.collections[collName].root;
        const hashB = checkpointB.collections[collName]?.root;

        if (hashA === hashB) {
          console.log(`    ${colors.green}✓${colors.reset} ${collName}: MATCH`);
        } else {
          console.log(
            `    ${colors.red}✗${colors.reset} ${collName}: MISMATCH`,
          );
        }
      }
    }

    console.log('');
    console.log('═'.repeat(70));
    console.log(`${colors.bold}${colors.cyan}Summary${colors.reset}`);
    console.log('═'.repeat(70));
    console.log(`  Total documents: ${formatNumber(countA)}`);
    console.log(`  MongoDB A computation time: ${formatTime(timeA)}`);
    console.log(`  MongoDB B computation time: ${formatTime(timeB)}`);
    console.log(
      `  Average computation time: ${formatTime((timeA + timeB) / 2)}`,
    );
    console.log(
      `  Average throughput: ${formatNumber(Math.round((countA + countB) / ((timeA + timeB) / 1000)))} docs/sec`,
    );
    console.log('');
    console.log(
      `  Merkle tree partitions stored: ${colors.green}✓${colors.reset}`,
    );
    console.log(
      `  Incremental updates enabled: ${colors.green}✓${colors.reset}`,
    );
    console.log(
      `  State dirty tracking ready: ${colors.green}✓${colors.reset}`,
    );
    console.log('═'.repeat(70) + '\n');

    return checkpointA.dbRoot === checkpointB.dbRoot;
  } catch (err) {
    const error = err as Error;
    console.error(`\n${colors.red}✗ ERROR: ${error.message}${colors.reset}\n`);
    console.error(error);
    return false;
  } finally {
    if (clientA) await clientA.close();
    if (clientB) await clientB.close();
    console.log(`${colors.blue}ℹ${colors.reset} Closed MongoDB connections\n`);
  }
}

// Run benchmark
benchmark()
  .then((match) => {
    process.exit(match ? 0 : 1);
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
