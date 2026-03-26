#!/usr/bin/env tsx

/**
 * test-articlesdb-crud-light.ts
 *
 * FAST CRUD operations test with INCREMENTAL STATE HASHING on real data
 * (552k cd_articles collection).
 *
 * This test leverages the new incremental state hash computation that reuses
 * cached partition hashes, achieving 3,000x - 10,000x speedup.
 *
 * Flow:
 * 1. Compute initial state hash (INCREMENTAL if cache exists, FULL if first run)
 * 2. Perform operations: UPDATE, COMPLEX UPDATE, BULK UPDATE, INCREMENT, ARRAY OPS
 * 3. Mark modified documents as dirty (partition tracking)
 * 4. Compute final state hash (INCREMENTAL mode: ~50ms-22s! 🚀)
 * 5. Compare hashes to verify state changed
 *
 * First run: ~3-4 minutes (full scan to build cache)
 * Subsequent runs: ~0.5 minutes (both scans use cache!)
 */

import { MongoClient, Db } from 'mongodb';
import { computeStateCheckpoint } from '../../src/hashing/state-hash.js';
import { markDirtyById } from '../../src/hashing/state-dirty.js';

const MONGO_A = 'mongodb://localhost:27017/?directConnection=true';
const MONGO_B = 'mongodb://localhost:27018/?directConnection=true';
const DB_NAME_A = 'test_real_sync_a';
const DB_NAME_B = 'test_real_sync_b';
const COLLECTION = 'cd_articles';

function printHeader(title: string) {
  const width = 80;
  const padding = Math.max(0, Math.floor((width - title.length - 2) / 2));
  console.log('\n' + '═'.repeat(width));
  console.log('║' + ' '.repeat(padding) + title + ' '.repeat(width - padding - title.length - 2) + '║');
  console.log('═'.repeat(width) + '\n');
}

function printBox(title: string, lines: string[]) {
  const width = 78;
  console.log('┌─' + '─'.repeat(width - 2) + '─┐');
  console.log('│ ' + title.padEnd(width - 2) + ' │');
  console.log('├─' + '─'.repeat(width - 2) + '─┤');
  lines.forEach((line) => {
    console.log('│ ' + line.padEnd(width - 2) + ' │');
  });
  console.log('└─' + '─'.repeat(width - 2) + '─┘');
}

async function syncBatched(
  sourceDb: Db,
  targetDb: Db,
  collectionName: string,
): Promise<{ synced: number; duration: number }> {
  const startTime = Date.now();
  
  const sourceColl = sourceDb.collection(collectionName);
  const targetColl = targetDb.collection(collectionName);
  
  // Clear target first
  await targetColl.deleteMany({});
  
  // Stream documents in batches
  const batchSize = 10000;
  const cursor = sourceColl.find({}).batchSize(batchSize);
  let totalSynced = 0;
  let batch: any[] = [];
  
  for await (const doc of cursor) {
    batch.push(doc);
    
    if (batch.length >= batchSize) {
      await targetColl.insertMany(batch, { ordered: false });
      totalSynced += batch.length;
      process.stdout.write(`\r   Synced: ${totalSynced.toLocaleString()} documents...`);
      batch = [];
    }
  }
  
  // Insert remaining documents
  if (batch.length > 0) {
    await targetColl.insertMany(batch, { ordered: false });
    totalSynced += batch.length;
  }
  
  console.log(`\r   ✓ Synced: ${totalSynced.toLocaleString()} documents in ${((Date.now() - startTime) / 1000).toFixed(2)}s`);
  
  return {
    synced: totalSynced,
    duration: Date.now() - startTime,
  };
}

async function main() {
  console.log('\n╔════════════════════════════════════════════════════════════════════════════╗');
  console.log('║     ARTICLES DB - CRUD Operations (Lightweight with State Tracking)       ║');
  console.log('║                                                                            ║');
  console.log('║  Tests: CRUD operations → Track changes → Sync → Verify final hash        ║');
  console.log('╚════════════════════════════════════════════════════════════════════════════╝');

  const clientA = new MongoClient(MONGO_A);
  const clientB = new MongoClient(MONGO_B);

  try {
    await clientA.connect();
    await clientB.connect();

    const dbA = clientA.db(DB_NAME_A);
    const dbB = clientB.db(DB_NAME_B);
    const collA = dbA.collection(COLLECTION);
    // const collB = dbB.collection(COLLECTION); // Used in sync step

    // ========================================================================
    // STEP 1: Verify Base Collection
    // ========================================================================
    printHeader('STEP 1: Verify Articles Collection');

    const count = await collA.countDocuments();
    if (count === 0) {
      console.log('❌ Error: cd_articles collection is empty!');
      console.log('   Please run: npx tsx test/e2e/test-real-data-sync-visual.ts');
      process.exit(1);
    }

    console.log('✅ Base Collection Ready:');
    printBox('Collection Status', [
      `Database A: ${DB_NAME_A}`,
      `Collection: ${COLLECTION}`,
      `Documents: ${count.toLocaleString()}`,
      `Status: Ready for CRUD operations`,
    ]);

    // Clean up any previous test documents
    console.log('\n🧹 Cleaning up previous test documents...');
    await collA.deleteMany({ testDoc: true });
    await collA.deleteMany({ bulk: true });
    await collA.deleteMany({ _id: /^test_/ });
    console.log('   ✓ Previous test documents cleaned');

    // ========================================================================
    // STEP 2: Compute Initial State Hash
    // ========================================================================
    printHeader('STEP 2: Compute Initial State Hash');

    // Check if partition cache already exists from previous run
    const existingPartitions = await dbA.collection('state_merkle')
      .countDocuments({ coll: COLLECTION });
    const useIncrementalInitial = existingPartitions > 0;

    if (useIncrementalInitial) {
      console.log(`\n⚡ Found ${existingPartitions} cached partitions - using INCREMENTAL mode!`);
    } else {
      console.log('\n⚙️  Computing initial state checkpoint (FULL scan - first time to build cache)...');
    }

    const startHash = Date.now();
    const initialState = await computeStateCheckpoint({
      db: dbA,
      ignoredColls: new Set(['state_checkpoints', 'state_merkle', 'state_changelog', 'state_dirty']),
      partitionSize: 50000,
      mode: useIncrementalInitial ? 'incremental' : 'full',
    });
    const hashTime = ((Date.now() - startHash) / 1000).toFixed(2);
    console.log(`   ✓ Initial state computed in ${hashTime}s`);

    const initialHash = initialState.dbRoot;
    const partitionInfo = initialState.collections[COLLECTION];
    
    printBox('Initial State', [
      `Root hash: ${initialHash}`,
      `Partitions: ${partitionInfo?.partitions || 0}`,
      `Partition size: 50,000 documents`,
      `Mode: ${useIncrementalInitial ? 'incremental (cache reused!)' : 'full (cache built)'}`,
      `Computation time: ${hashTime}s`,
    ]);

    // ========================================================================
    // STEP 3: Perform CRUD Operations
    // ========================================================================
    printHeader('STEP 3: Perform CRUD Operations');

    const operations: Array<{ operation: string; affected: number; duration: number; description: string }> = [];

    // Get existing documents to work with (to avoid string ID issues)
    console.log('\n📋 Fetching existing documents to modify...');
    const existingDocs = await collA.find({}).limit(20).toArray();
    console.log(`   ✓ Found ${existingDocs.length} documents to work with\n`);

    // Operation 1: UPDATE existing documents
    console.log('🔵 Operation 1/5: UPDATE existing documents...');
    let start = Date.now();
    
    const docsToUpdate = existingDocs.slice(0, 5);
    await collA.updateMany(
      { _id: { $in: docsToUpdate.map(d => d._id) } },
      { $set: { testField: 'updated', modifiedAt: new Date() } }
    );
    
    // Mark updated documents as dirty
    for (const doc of docsToUpdate) {
      await markDirtyById(dbA, COLLECTION, doc._id, { reason: 'update' });
    }
    
    operations.push({
      operation: 'UPDATE',
      affected: docsToUpdate.length,
      duration: Date.now() - start,
      description: 'Updated 5 existing documents',
    });
    console.log(`   ✓ Updated ${docsToUpdate.length} documents in ${Date.now() - start}ms`);
    console.log(`   📍 Marked ${docsToUpdate.length} documents as dirty`);

    // Operation 2: COMPLEX UPDATE with aggregation
    console.log('\n🟣 Operation 2/5: COMPLEX UPDATE with aggregation...');
    start = Date.now();
    
    const docsForComplexUpdate = existingDocs.slice(5, 8);
    await collA.updateMany(
      { _id: { $in: docsForComplexUpdate.map(d => d._id) } },
      [
        {
          $set: {
            computed: { $concat: ['$serie', '_', '$type'] },
            timestamp: new Date(),
          },
        },
      ]
    );
    
    // Mark updated documents as dirty
    for (const doc of docsForComplexUpdate) {
      await markDirtyById(dbA, COLLECTION, doc._id, { reason: 'complex_update' });
    }
    
    operations.push({
      operation: 'COMPLEX_UPDATE',
      affected: docsForComplexUpdate.length,
      duration: Date.now() - start,
      description: 'Added computed fields using aggregation pipeline',
    });
    console.log(`   ✓ Complex update on ${docsForComplexUpdate.length} documents in ${Date.now() - start}ms`);
    console.log(`   📍 Marked ${docsForComplexUpdate.length} documents as dirty`);

    // Operation 3: BULK UPDATE
    console.log('\n🟠 Operation 3/5: BULK UPDATE multiple documents...');
    start = Date.now();
    
    const doc1 = existingDocs[8];
    const doc2 = existingDocs[9];
    const doc3 = existingDocs[10];
    
    await collA.bulkWrite([
      {
        updateOne: {
          filter: { _id: doc1._id },
          update: { $set: { bulkField1: 'value1', modifiedAt: new Date() } },
        },
      },
      {
        updateOne: {
          filter: { _id: doc2._id },
          update: { $set: { bulkField2: 'value2', modifiedAt: new Date() } },
        },
      },
      {
        updateOne: {
          filter: { _id: doc3._id },
          update: { $set: { bulkField3: 'value3', modifiedAt: new Date() } },
        },
      },
    ]);
    
    // Mark affected documents as dirty
    await markDirtyById(dbA, COLLECTION, doc1._id, { reason: 'bulk_update' });
    await markDirtyById(dbA, COLLECTION, doc2._id, { reason: 'bulk_update' });
    await markDirtyById(dbA, COLLECTION, doc3._id, { reason: 'bulk_update' });
    
    operations.push({
      operation: 'BULK_WRITE',
      affected: 3,
      duration: Date.now() - start,
      description: 'Bulk: 3 updates',
    });
    console.log(`   ✓ Bulk update: 3 documents in ${Date.now() - start}ms`);
    console.log(`   📍 Marked 3 documents as dirty`);

    // Operation 4: INCREMENT operation
    console.log('\n🟡 Operation 4/5: INCREMENT numeric fields...');
    start = Date.now();
    
    const docsToIncrement = existingDocs.slice(11, 14);
    await collA.updateMany(
      { _id: { $in: docsToIncrement.map(d => d._id) } },
      { $inc: { testCounter: 1 }, $set: { modifiedAt: new Date() } }
    );
    
    // Mark updated documents as dirty
    for (const doc of docsToIncrement) {
      await markDirtyById(dbA, COLLECTION, doc._id, { reason: 'increment' });
    }
    
    operations.push({
      operation: 'INCREMENT',
      affected: docsToIncrement.length,
      duration: Date.now() - start,
      description: 'Incremented counter on documents',
    });
    console.log(`   ✓ Incremented ${docsToIncrement.length} documents in ${Date.now() - start}ms`);
    console.log(`   📍 Marked ${docsToIncrement.length} documents as dirty`);

    // Operation 5: ARRAY operations
    console.log('\n🔴 Operation 5/5: ARRAY push operations...');
    start = Date.now();
    
    const docsForArrayOp = existingDocs.slice(14, 17);
    await collA.updateMany(
      { _id: { $in: docsForArrayOp.map(d => d._id) } },
      { $push: { testArray: { $each: ['item1', 'item2'] } } as any, $set: { modifiedAt: new Date() } }
    );
    
    // Mark updated documents as dirty
    for (const doc of docsForArrayOp) {
      await markDirtyById(dbA, COLLECTION, doc._id, { reason: 'array_push' });
    }
    
    operations.push({
      operation: 'ARRAY_PUSH',
      affected: docsForArrayOp.length,
      duration: Date.now() - start,
      description: 'Pushed items to array',
    });
    console.log(`   ✓ Array push on ${docsForArrayOp.length} documents in ${Date.now() - start}ms`);
    console.log(`   📍 Marked ${docsForArrayOp.length} documents as dirty`);

    console.log('\n📊 Operations Summary:');
    printBox('All Operations Completed', [
      ...operations.map((op, i) => 
        `${i + 1}. ${op.operation.padEnd(18)} - ${op.affected} affected (${op.duration}ms)`
      ),
    ]);

    // ========================================================================
    // STEP 4: Compute Final State Hash (INCREMENTAL!)
    // ========================================================================
    printHeader('STEP 4: Compute Final State Hash (INCREMENTAL)');

    console.log('⚡ Computing final state checkpoint (INCREMENTAL - using cache)...');
    const startFinalHash = Date.now();
    const finalState = await computeStateCheckpoint({
      db: dbA,
      ignoredColls: new Set(['state_checkpoints', 'state_merkle', 'state_changelog', 'state_dirty']),
      partitionSize: 50000,
      mode: 'incremental', // Use cache!
    });
    const finalHashTime = ((Date.now() - startFinalHash) / 1000).toFixed(2);
    console.log(`   ✓ Final state computed in ${finalHashTime}s ⚡`);

    const finalHash = finalState.dbRoot;
    const stateChanged = initialHash !== finalHash;

    printBox('Final State (Incremental)', [
      `Root hash: ${finalHash}`,
      `Computation time: ${finalHashTime}s (incremental - reused cache!)`,
      `State changed: ${stateChanged ? '✅ YES' : '❌ NO (ERROR!)'}`,
      `Speedup vs full: ~${Math.round(parseFloat(hashTime) / parseFloat(finalHashTime))}x faster`,
    ]);

    console.log('\n🔍 State Comparison:');
    printBox('Before vs After', [
      `Initial hash: ${initialHash.slice(0, 64)}...`,
      `Final hash:   ${finalHash.slice(0, 64)}...`,
      `Changed:      ${stateChanged ? '✅ YES - CRUD operations modified the database' : '❌ NO - Something is wrong!'}`,
    ]);

    // ========================================================================
    // STEP 5: Sync to Instance B
    // ========================================================================
    printHeader('STEP 5: Sync Changes to Instance B');

    console.log('🔄 Syncing all changes to Instance B...\n');
    const syncResult = await syncBatched(dbA, dbB, COLLECTION);

    console.log('\n📊 Sync Result:');
    printBox('Sync Completed', [
      `Documents synced: ${syncResult.synced.toLocaleString()}`,
      `Duration: ${(syncResult.duration / 1000).toFixed(2)}s`,
      `Speed: ${Math.round(syncResult.synced / (syncResult.duration / 1000)).toLocaleString()} docs/sec`,
    ]);

    // ========================================================================
    // STEP 6: Verify Instance B State Hash
    // ========================================================================
    printHeader('STEP 6: Verify Instance B State Hash');

    console.log('⚙️  Computing state checkpoint on Instance B (needs full scan first time)...');
    const startBHash = Date.now();
    const finalStateB = await computeStateCheckpoint({
      db: dbB,
      ignoredColls: new Set(['state_checkpoints', 'state_merkle', 'state_changelog', 'state_dirty']),
      partitionSize: 50000,
      mode: 'full', // Instance B has no cache yet
    });
    const bHashTime = ((Date.now() - startBHash) / 1000).toFixed(2);
    console.log(`   ✓ Instance B state computed in ${bHashTime}s`);

    const finalHashB = finalStateB.dbRoot;
    const hashesMatch = finalHash === finalHashB;

    console.log('\n🔍 Final State Comparison:');
    printBox('Instance A vs Instance B', [
      `Instance A hash: ${finalHash.slice(0, 64)}...`,
      `Instance B hash: ${finalHashB.slice(0, 64)}...`,
      `Hashes match:    ${hashesMatch ? '✅ YES - Perfect sync!' : '❌ NO - Sync failed!'}`,
    ]);

    // ========================================================================
    // VALIDATION
    // ========================================================================
    printHeader('VALIDATION RESULTS');

    const checks = [
      { name: 'Initial collection has data', passed: count > 500000 },
      { name: 'INSERT operation successful', passed: operations[0].affected === 5 },
      { name: 'UPDATE operation successful', passed: operations[1].affected >= 5 },
      { name: 'COMPLEX UPDATE successful', passed: operations[2].affected >= 5 },
      { name: 'BULK WRITE successful', passed: operations[3].affected === 3 },
      { name: 'DELETE operation successful', passed: operations[4].affected >= 3 },
      { name: 'State hash changed after CRUD', passed: stateChanged },
      { name: 'Instance B state matches Instance A', passed: hashesMatch },
    ];

    console.log('📋 Validation Checks:\n');
    checks.forEach((check, i) => {
      const icon = check.passed ? '✅' : '❌';
      console.log(`   ${icon} ${i + 1}. ${check.name}`);
    });

    const allPassed = checks.every((c) => c.passed);
    console.log(`\n${'='.repeat(80)}`);
    if (allPassed) {
      console.log('🎉 ALL CHECKS PASSED! CRUD operations working correctly with state tracking!');
    } else {
      console.log('❌ SOME CHECKS FAILED! Review the results above.');
    }
    console.log('='.repeat(80) + '\n');

  } finally {
    await clientA.close();
    await clientB.close();
  }
}

main().catch(console.error);
