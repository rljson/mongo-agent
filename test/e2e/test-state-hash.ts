#!/usr/bin/env tsx

import { MongoClient } from 'mongodb';

import { computeStateCheckpoint } from '../../dist/index.js';


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
} as const;

function log(color: string, symbol: string, message: string): void {
  console.log(`${color}${symbol}${colors.reset} ${message}`);
}

function success(msg: string): void {
  log(colors.green, '✓', msg);
}
function error(msg: string): void {
  log(colors.red, '✗', msg);
}
function info(msg: string): void {
  log(colors.blue, 'ℹ', msg);
}
function warn(msg: string): void {
  log(colors.yellow, '⚠', msg);
}
function header(msg: string): void {
  log(colors.cyan, '═', msg);
}

// Collections to ignore in state hash
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

async function compareStateHashes(): Promise<boolean> {
  let clientA: MongoClient | undefined;
  let clientB: MongoClient | undefined;

  try {
    info('Connecting to MongoDB instances...');
    clientA = new MongoClient(MONGO_A_URI);
    clientB = new MongoClient(MONGO_B_URI);

    await clientA.connect();
    await clientB.connect();
    success('Connected to both MongoDB instances');

    const dbA = clientA.db('syncdb');
    const dbB = clientB.db('syncdb');

    console.log('\n' + '='.repeat(60));
    header('Computing State Hash for MongoDB A');
    console.log('='.repeat(60));

    info('Computing state checkpoint for MongoDB A...');
    const startA = Date.now();
    const checkpointA = await computeStateCheckpoint({
      db: dbA,
      ignoredColls: IGNORED_COLLECTIONS,
      partitionSize: 50000,
      storeLeaves: true,
      mode: 'test',
    });
    const timeA = Date.now() - startA;
    success(`MongoDB A checkpoint computed in ${timeA}ms`);

    console.log('\n' + '='.repeat(60));
    header('Computing State Hash for MongoDB B');
    console.log('='.repeat(60));

    info('Computing state checkpoint for MongoDB B...');
    const startB = Date.now();
    const checkpointB = await computeStateCheckpoint({
      db: dbB,
      ignoredColls: IGNORED_COLLECTIONS,
      partitionSize: 50000,
      storeLeaves: true,
      mode: 'test',
    });
    const timeB = Date.now() - startB;
    success(`MongoDB B checkpoint computed in ${timeB}ms`);

    console.log('\n' + '='.repeat(60));
    header('Comparison Results');
    console.log('='.repeat(60) + '\n');

    // Compare DB roots
    console.log('Database Root Hashes:');
    console.log(
      `  MongoDB A: ${colors.cyan}${checkpointA.dbRoot}${colors.reset}`,
    );
    console.log(
      `  MongoDB B: ${colors.cyan}${checkpointB.dbRoot}${colors.reset}`,
    );

    if (checkpointA.dbRoot === checkpointB.dbRoot) {
      console.log('');
      success('✨ DATABASE ROOT HASHES MATCH! ✨');
      success('Both databases are in the same state!');
    } else {
      console.log('');
      error('DATABASE ROOT HASHES DO NOT MATCH!');
      warn('Databases are in different states - detailed comparison below:');
    }

    console.log('\n' + '-'.repeat(60));
    console.log('Collection-by-Collection Comparison:');
    console.log('-'.repeat(60) + '\n');

    const allCollections = new Set([
      ...Object.keys(checkpointA.collections),
      ...Object.keys(checkpointB.collections),
    ]);

    let matchCount = 0;
    let mismatchCount = 0;

    for (const collName of Array.from(allCollections).sort()) {
      const collA = checkpointA.collections[collName];
      const collB = checkpointB.collections[collName];

      if (!collA) {
        warn(`Collection "${collName}" exists only in MongoDB B`);
        console.log(`  B: ${collB.partitions} partitions, root: ${collB.root}`);
        mismatchCount++;
        continue;
      }

      if (!collB) {
        warn(`Collection "${collName}" exists only in MongoDB A`);
        console.log(`  A: ${collA.partitions} partitions, root: ${collA.root}`);
        mismatchCount++;
        continue;
      }

      if (collA.root === collB.root) {
        success(`${collName}: MATCH`);
        console.log(`  Hash: ${collA.root.substring(0, 16)}...`);
        console.log(
          `  Partitions: A=${collA.partitions}, B=${collB.partitions}`,
        );
        matchCount++;
      } else {
        error(`${collName}: MISMATCH`);
        console.log(`  A: ${collA.partitions} partitions, root: ${collA.root}`);
        console.log(`  B: ${collB.partitions} partitions, root: ${collB.root}`);
        mismatchCount++;
      }
      console.log('');
    }

    console.log('='.repeat(60));
    console.log('Summary:');
    console.log(
      `  ${colors.green}✓${colors.reset} Matching collections: ${matchCount}`,
    );
    console.log(
      `  ${colors.red}✗${colors.reset} Mismatched collections: ${mismatchCount}`,
    );
    console.log(`  Total collections compared: ${allCollections.size}`);
    console.log('='.repeat(60) + '\n');

    // Show statistics
    console.log('Statistics:');
    console.log(
      `  MongoDB A: ${Object.keys(checkpointA.collections).length} collections`,
    );
    console.log(
      `  MongoDB B: ${Object.keys(checkpointB.collections).length} collections`,
    );
    console.log(`  Computation time A: ${timeA}ms`);
    console.log(`  Computation time B: ${timeB}ms`);
    console.log('');

    info('Latest checkpoints saved to state_checkpoints collection');
    info(`  MongoDB A: ${checkpointA._id}`);
    info(`  MongoDB B: ${checkpointB._id}`);

    return checkpointA.dbRoot === checkpointB.dbRoot;
  } catch (err) {
    const errObj = err as Error;
    console.error('\n' + '='.repeat(60));
    error(`ERROR: ${errObj.message}`);
    console.error('='.repeat(60) + '\n');
    console.error(errObj);
    return false;
  } finally {
    if (clientA) await clientA.close();
    if (clientB) await clientB.close();
    info('Closed MongoDB connections');
  }
}

// Run comparison
console.log('\n');
console.log('╔' + '═'.repeat(58) + '╗');
console.log(
  '║' +
    ' '.repeat(10) +
    colors.cyan +
    'MongoDB State Hash Comparison' +
    colors.reset +
    ' '.repeat(19) +
    '║',
);
console.log('╚' + '═'.repeat(58) + '╝');
console.log('');

compareStateHashes()
  .then((match) => {
    console.log('');
    if (match) {
      console.log(
        colors.green +
          '┌─────────────────────────────────────────────┐' +
          colors.reset,
      );
      console.log(
        colors.green +
          '│  ✨ SUCCESS! Databases are synchronized! ✨  │' +
          colors.reset,
      );
      console.log(
        colors.green +
          '└─────────────────────────────────────────────┘' +
          colors.reset,
      );
      console.log('');
      process.exit(0);
    } else {
      console.log(
        colors.red +
          '┌─────────────────────────────────────────────┐' +
          colors.reset,
      );
      console.log(
        colors.red +
          '│  ⚠️  WARNING: Databases are out of sync!  ⚠️   │' +
          colors.reset,
      );
      console.log(
        colors.red +
          '└─────────────────────────────────────────────┘' +
          colors.reset,
      );
      console.log('');
      process.exit(1);
    }
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
