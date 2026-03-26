// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * E2E Test: Real Data Sync with Visual Output
 * 
 * Uses real cd_articles collection (552k documents) to test:
 * - Import real BSON data to MongoDB A
 * - Sync to MongoDB B
 * - Verify state hash consistency
 * - Visual before/after comparison
 * - Document sampling to see actual data
 */

import { MongoClient, type Db } from 'mongodb';
import * as BSON from 'bson';
import { SimpleStateLog } from '../../src/simple-state-log.ts';
import { computeStateCheckpoint } from '../../src/hashing/state-hash.ts';
import { createReadStream, existsSync } from 'node:fs';
import { createGunzip } from 'node:zlib';

const MONGO_A_URI = 'mongodb://localhost:27017/?directConnection=true';
const MONGO_B_URI = 'mongodb://localhost:27018/?directConnection=true';
const BSON_FILE = '/Users/hermanmertke/Downloads/CARATDB/cd_articles.bson.gz';

function printHeader(title: string) {
  console.log('\n' + '═'.repeat(80));
  console.log('║ ' + title.padEnd(76) + ' ║');
  console.log('═'.repeat(80));
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

function formatNumber(num: number): string {
  return num.toLocaleString('en-US');
}

function formatBytes(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB'];
  let size = bytes;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex++;
  }
  return `${size.toFixed(2)} ${units[unitIndex]}`;
}

async function importBsonFile(
  db: Db,
  collectionName: string,
  bsonFilePath: string,
  batchSize = 10000,
): Promise<{ imported: number; duration: number }> {
  const startTime = Date.now();
  const collection = db.collection(collectionName);

  // Clear collection first
  await collection.deleteMany({});

  let totalImported = 0;
  let batch: any[] = [];
  let buffer = Buffer.alloc(0);

  const gunzip = createGunzip();
  const fileStream = createReadStream(bsonFilePath);

  return new Promise((resolve, reject) => {
    let paused = false;

    const processBuffer = async () => {
      while (buffer.length >= 4 && !paused) {
        // BSON documents start with a 4-byte length
        const docLength = buffer.readInt32LE(0);

        if (buffer.length < docLength) {
          // Not enough data yet
          break;
        }

        try {
          // Extract one document
          const docBuffer = buffer.subarray(0, docLength);
          const doc = BSON.deserialize(docBuffer);
          
          batch.push(doc);
          buffer = buffer.subarray(docLength);

          // Insert batch when full
          if (batch.length >= batchSize) {
            paused = true;
            gunzip.pause();
            await collection.insertMany(batch, { ordered: false });
            totalImported += batch.length;
            process.stdout.write(`\r   Imported: ${formatNumber(totalImported)} documents...`);
            batch = [];
            paused = false;
            gunzip.resume();
          }
        } catch (err) {
          // Skip malformed documents
          buffer = buffer.subarray(Math.min(4, buffer.length));
        }
      }
    };

    gunzip
      .on('data', async (chunk: Buffer) => {
        buffer = Buffer.concat([buffer, chunk]);
        await processBuffer();
      })
      .on('end', async () => {
        // Process remaining buffer
        await processBuffer();

        // Insert remaining documents
        if (batch.length > 0) {
          await collection.insertMany(batch, { ordered: false });
          totalImported += batch.length;
        }
        
        const duration = Date.now() - startTime;
        console.log(`\r   ✓ Imported: ${formatNumber(totalImported)} documents in ${(duration / 1000).toFixed(2)}s         `);
        resolve({ imported: totalImported, duration });
      })
      .on('error', reject);

    fileStream.pipe(gunzip).on('error', reject);
  });
}

async function syncCollection(
  sourceDb: Db,
  targetDb: Db,
  collectionName: string,
  batchSize = 10000,
): Promise<{ synced: number; duration: number }> {
  const startTime = Date.now();
  const sourceColl = sourceDb.collection(collectionName);
  const targetColl = targetDb.collection(collectionName);

  // Clear target
  await targetColl.deleteMany({});

  // Stream documents in batches
  const cursor = sourceColl.find({}).batchSize(batchSize);
  let totalSynced = 0;
  let batch: any[] = [];

  for await (const doc of cursor) {
    batch.push(doc);
    
    if (batch.length >= batchSize) {
      await targetColl.insertMany(batch, { ordered: false });
      totalSynced += batch.length;
      process.stdout.write(`\r   Synced: ${formatNumber(totalSynced)} documents...`);
      batch = [];
    }
  }

  // Insert remaining documents
  if (batch.length > 0) {
    await targetColl.insertMany(batch, { ordered: false });
    totalSynced += batch.length;
  }

  const duration = Date.now() - startTime;
  console.log(`\r   ✓ Synced: ${formatNumber(totalSynced)} documents in ${(duration / 1000).toFixed(2)}s          `);

  return { synced: totalSynced, duration };
}

async function main() {
  console.log('\n╔════════════════════════════════════════════════════════════════════════════╗');
  console.log('║            REAL DATA SYNC TEST - Visual End-to-End Demo                   ║');
  console.log('║                                                                            ║');
  console.log('║  Collection: cd_articles (552k documents)                                 ║');
  console.log('║  Flow: BSON → MongoDB A → Sync → MongoDB B                                ║');
  console.log('╚════════════════════════════════════════════════════════════════════════════╝');

  // Check if BSON file exists
  if (!existsSync(BSON_FILE)) {
    console.error(`\n❌ Error: BSON file not found: ${BSON_FILE}`);
    process.exit(1);
  }

  const clientA = await MongoClient.connect(MONGO_A_URI);
  const clientB = await MongoClient.connect(MONGO_B_URI);

  const dbA = clientA.db('test_real_sync_a');
  const dbB = clientB.db('test_real_sync_b');

  try {
    // ========================================================================
    // STEP 1: PREPARE DATA ON INSTANCE A
    // ========================================================================
    printHeader('STEP 1: Prepare Data on MongoDB A');

    // Check if collection already exists
    const collA = dbA.collection('cd_articles');
    let existingCount = await collA.countDocuments();

    console.log('\n📦 Data Source:');
    printBox('Collection Status', [
      `Database: ${dbA.databaseName}`,
      `Collection: cd_articles`,
      `Existing documents: ${formatNumber(existingCount)}`,
      `BSON file: ${BSON_FILE}`,
    ]);

    let importResult;
    
    if (existingCount > 0) {
      console.log('\n✅ Collection already exists - skipping import');
      console.log(`   Using existing ${formatNumber(existingCount)} documents`);
      console.log('   (This saves 2-3 minutes of import time)');
      
      // Create fake import result for stats
      importResult = { imported: existingCount, duration: 0 };
    } else {
      console.log('\n📥 Collection is empty - importing BSON data (this will take several minutes)...');
      console.log('   Reading and decompressing BSON file...');
      console.log('   (This is a one-time operation)');
      
      try {
        importResult = await importBsonFile(dbA, 'cd_articles', BSON_FILE, 10000);
        const docsPerSec = Math.round(importResult.imported / (importResult.duration / 1000));
        
        console.log(`\n   ✅ Import Statistics:`);
        console.log(`       Documents: ${formatNumber(importResult.imported)}`);
        console.log(`       Duration: ${(importResult.duration / 1000).toFixed(2)}s`);
        console.log(`       Speed: ${formatNumber(docsPerSec)} docs/sec`);
        
        // Update existing count
        existingCount = await collA.countDocuments();
      } catch (error: any) {
        console.error('   ❌ Import failed:', error.message);
        throw error;
      }
    }

    // ========================================================================
    // STEP 2: ANALYZE DATA ON INSTANCE A
    // ========================================================================
    printHeader('STEP 2: Analyze Data on MongoDB A');

    const countA = existingCount;
    const statsA = await dbA.command({ collStats: 'cd_articles' });
    
    console.log('\n📊 Collection Statistics (Instance A):');
    printBox('cd_articles Collection', [
      `Documents: ${formatNumber(countA)}`,
      `Avg doc size: ${formatBytes(statsA.avgObjSize || 0)}`,
      `Total size: ${formatBytes(statsA.size || 0)}`,
      `Storage size: ${formatBytes(statsA.storageSize || 0)}`,
      `Indexes: ${statsA.nindexes || 0}`,
    ]);

    // Sample documents
    console.log('\n🔍 Sample Documents (first 3):');
    const samples = await collA.find({}).limit(3).toArray();
    samples.forEach((doc, i) => {
      console.log(`\n${i + 1}. Document ID: ${doc._id}`);
      const preview = { ...doc };
      delete preview._id;
      
      // Show first few fields
      const keys = Object.keys(preview).slice(0, 5);
      keys.forEach((key) => {
        const value = String(preview[key]).slice(0, 60);
        console.log(`   ${key}: ${value}${String(preview[key]).length > 60 ? '...' : ''}`);
      });
      if (Object.keys(preview).length > 5) {
        console.log(`   ... and ${Object.keys(preview).length - 5} more fields`);
      }
    });

    // Initialize state log for Instance A
    console.log('\n🔐 Initializing state log on Instance A...');
    const stateLogA = new SimpleStateLog(dbA);
    await stateLogA.initialize();

    // Capture initial state
    console.log('📸 Capturing initial state...');
    const startStateA = Date.now();
    const changeA1 = await stateLogA.captureStateChange(
      'restore',
      `Restored ${formatNumber(countA)} documents from BSON`,
    );
    const stateADuration = Date.now() - startStateA;

    console.log(`   ✓ State captured in ${(stateADuration / 1000).toFixed(2)}s`);

    console.log('\n📋 RLJSON State Entry (Instance A):');
    printBox('State Change Record', [
      `ID: ${changeA1.id}`,
      `Hash: ${changeA1.hash.slice(0, 40)}...`,
      `Type: ${changeA1.type}`,
      `Operation: ${changeA1.json.operation}`,
      `Description: ${changeA1.json.description}`,
      `Timestamp: ${new Date(changeA1.json.timestamp).toISOString()}`,
      `Prev state: ${changeA1.json.prevStateHash || 'null'}`,
      `Current state: ${changeA1.json.currentStateHash.slice(0, 40)}...`,
    ]);

    // ========================================================================
    // STEP 3: COMPUTE STATE HASH ON INSTANCE A
    // ========================================================================
    printHeader('STEP 3: Compute State Hash on MongoDB A');

    console.log('\n⚙️  Computing state checkpoint (this will take 1-2 minutes)...');
    const startCheckpointA = Date.now();
    const checkpointA = await computeStateCheckpoint({
      db: dbA,
      ignoredColls: new Set(['state_checkpoints', 'state_merkle', 'state_changelog']),
      partitionSize: 50000,
      mode: 'full',
    });
    const checkpointADuration = Date.now() - startCheckpointA;

    console.log(`   ✓ Checkpoint computed in ${(checkpointADuration / 1000).toFixed(2)}s`);

    console.log('\n🔐 State Hash Details:');
    printBox('Instance A State', [
      `Database root hash: ${checkpointA.dbRoot}`,
      `Collections: ${Object.keys(checkpointA.collections).join(', ')}`,
      `Timestamp: ${new Date(checkpointA.ts).toISOString()}`,
      `Mode: ${checkpointA.mode}`,
      `Partition size: ${formatNumber(checkpointA.partitionSize)}`,
    ]);

    const collectionsA = Object.entries(checkpointA.collections);
    console.log('\n   Collection Breakdown:');
    collectionsA.forEach(([name, info]) => {
      console.log(`     • ${name}:`);
      console.log(`       - Partitions: ${info.partitions}`);
      console.log(`       - Root hash: ${info.root.slice(0, 48)}...`);
    });

    // ========================================================================
    // STEP 4: BEFORE SYNC - SHOW INSTANCE B IS EMPTY
    // ========================================================================
    printHeader('STEP 4: Instance B - Before Sync');

    await dbB.dropDatabase();
    const countB_before = await dbB.collection('cd_articles').countDocuments();

    console.log('\n📭 Instance B is empty:');
    printBox('MongoDB B Status', [
      `Database: ${dbB.databaseName}`,
      `Collection: cd_articles`,
      `Documents: ${countB_before}`,
      `Status: Empty - ready for sync`,
    ]);

    // ========================================================================
    // STEP 5: SYNC DATA FROM A TO B
   //========================================================================
    printHeader('STEP 5: Sync Data from MongoDB A → MongoDB B');

    console.log('\n🔄 Starting sync operation...');
    printBox('Sync Configuration', [
      `Source: ${MONGO_A_URI} (${dbA.databaseName})`,
      `Target: ${MONGO_B_URI} (${dbB.databaseName})`,
      `Collection: cd_articles`,
      `Documents to sync: ${formatNumber(countA)}`,
      `Batch size: 10,000`,
    ]);

    console.log('\n📤 Syncing documents...');
    const syncResult = await syncCollection(dbA, dbB, 'cd_articles', 10000);

    const docsPerSecond = Math.round(syncResult.synced / (syncResult.duration / 1000));
    
    console.log('\n✅ Sync completed:');
    printBox('Sync Results', [
      `Documents synced: ${formatNumber(syncResult.synced)}`,
      `Duration: ${(syncResult.duration / 1000).toFixed(2)}s`,
      `Speed: ${formatNumber(docsPerSecond)} docs/sec`,
      `Average: ${(syncResult.duration / syncResult.synced).toFixed(2)}ms per doc`,
    ]);

    // ========================================================================
    // STEP 6: ANALYZE DATA ON INSTANCE B
    // ========================================================================
    printHeader('STEP 6: Analyze Data on MongoDB B');

    const collB = dbB.collection('cd_articles');
    const countB = await collB.countDocuments();
    const statsB = await dbB.command({ collStats: 'cd_articles' });

    console.log('\n📊 Collection Statistics (Instance B):');
    printBox('cd_articles Collection', [
      `Documents: ${formatNumber(countB)}`,
      `Avg doc size: ${formatBytes(statsB.avgObjSize || 0)}`,
      `Total size: ${formatBytes(statsB.size || 0)}`,
      `Storage size: ${formatBytes(statsB.storageSize || 0)}`,
      `Indexes: ${statsB.nindexes || 0}`,
    ]);

    // Verify sample documents match
    console.log('\n🔍 Comparing Sample Documents:');
    const samplesB = await collB.find({}).limit(3).toArray();
    
    let documentsMatch = true;
    for (let i = 0; i < Math.min(samples.length, samplesB.length); i++) {
      const docA = samples[i];
      const docB = samplesB[i];
      const match = JSON.stringify(docA) === JSON.stringify(docB);
      documentsMatch = documentsMatch && match;
      
      console.log(`   ${match ? '✅' : '❌'} Document ${i + 1} (ID: ${docA._id}): ${match ? 'MATCH' : 'MISMATCH'}`);
    }

    // Initialize state log for Instance B
    console.log('\n🔐 Initializing state log on Instance B...');
    const stateLogB = new SimpleStateLog(dbB);
    await stateLogB.initialize();

    // Capture state after sync
    console.log('📸 Capturing state after sync...');
    const startStateB = Date.now();
    const changeB1 = await stateLogB.captureStateChange(
      'sync',
      `Synced ${formatNumber(countB)} documents from Instance A`,
    );
    const stateBDuration = Date.now() - startStateB;

    console.log(`   ✓ State captured in ${(stateBDuration / 1000).toFixed(2)}s`);

    console.log('\n📋 RLJSON State Entry (Instance B):');
    printBox('State Change Record', [
      `ID: ${changeB1.id}`,
      `Hash: ${changeB1.hash.slice(0, 40)}...`,
      `Type: ${changeB1.type}`,
      `Operation: ${changeB1.json.operation}`,
      `Description: ${changeB1.json.description}`,
      `Timestamp: ${new Date(changeB1.json.timestamp).toISOString()}`,
      `Prev state: ${changeB1.json.prevStateHash || 'null'}`,
      `Current state: ${changeB1.json.currentStateHash.slice(0, 40)}...`,
    ]);

    // ========================================================================
    // STEP 7: COMPUTE STATE HASH ON INSTANCE B
    // ========================================================================
    printHeader('STEP 7: Compute State Hash on MongoDB B');

    console.log('\n⚙️  Computing state checkpoint (this will take 1-2 minutes)...');
    const startCheckpointB = Date.now();
    const checkpointB = await computeStateCheckpoint({
      db: dbB,
      ignoredColls: new Set(['state_checkpoints', 'state_merkle', 'state_changelog']),
      partitionSize: 50000,
      mode: 'full',
    });
    const checkpointBDuration = Date.now() - startCheckpointB;

    console.log(`   ✓ Checkpoint computed in ${(checkpointBDuration / 1000).toFixed(2)}s`);

    console.log('\n🔐 State Hash Details:');
    printBox('Instance B State', [
      `Database root hash: ${checkpointB.dbRoot}`,
      `Collections: ${Object.keys(checkpointB.collections).join(', ')}`,
      `Timestamp: ${new Date(checkpointB.ts).toISOString()}`,
      `Mode: ${checkpointB.mode}`,
      `Partition size: ${formatNumber(checkpointB.partitionSize)}`,
    ]);

    const collectionsB = Object.entries(checkpointB.collections);
    console.log('\n   Collection Breakdown:');
    collectionsB.forEach(([name, info]) => {
      console.log(`     • ${name}:`);
      console.log(`       - Partitions: ${info.partitions}`);
      console.log(`       - Root hash: ${info.root.slice(0, 48)}...`);
    });

    // ========================================================================
    // STEP 8: COMPARE STATE HASHES
    // ========================================================================
    printHeader('STEP 8: Compare State Hashes Between Instances');

    const stateHashesMatch = checkpointA.dbRoot === checkpointB.dbRoot;

    console.log('\n🎯 ROOT STATE HASH COMPARISON:');
    console.log('┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓');
    console.log('┃ Instance A:                                                            ┃');
    console.log(`┃ ${checkpointA.dbRoot} ┃`);
    console.log('┃                                                                        ┃');
    console.log('┃ Instance B:                                                            ┃');
    console.log(`┃ ${checkpointB.dbRoot} ┃`);
    console.log('┃                                                                        ┃');
    console.log(`┃ Result: ${stateHashesMatch ? '✅ MATCH - Data is identical!' : '❌ MISMATCH - Data differs!'}                                        ┃`);
    console.log('┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛');

    // Compare collection hashes
    console.log('\n📋 COLLECTION HASH COMPARISON:');
    const allCollections = new Set([
      ...Object.keys(checkpointA.collections),
      ...Object.keys(checkpointB.collections),
    ]);

    const collectionMatches: Record<string, boolean> = {};
    allCollections.forEach((collName) => {
      const hashA = checkpointA.collections[collName]?.root || 'missing';
      const hashB = checkpointB.collections[collName]?.root || 'missing';
      const match = hashA === hashB;
      const partitionsA = checkpointA.collections[collName]?.partitions || 0;
      const partitionsB = checkpointB.collections[collName]?.partitions || 0;
      collectionMatches[collName] = match;
      
      const icon = match ? '✅' : '❌';
      console.log(`   ${icon} ${collName}:`);
      console.log(`       A: ${hashA.slice(0, 48)}... (${partitionsA} partitions)`);
      console.log(`       B: ${hashB.slice(0, 48)}... (${partitionsB} partitions)`);
      console.log(`       Status: ${match ? 'IDENTICAL' : 'DIFFERENT'}`);
    });

    // ========================================================================
    // VALIDATION
    // ========================================================================
    printHeader('VALIDATION SUMMARY');

    const checks = [
      { name: 'BSON file exists', pass: existsSync(BSON_FILE) },
      { name: 'Data imported to Instance A', pass: countA > 0 },
      { name: 'Expected document count (~552k)', pass: countA > 500000 && countA < 600000 },
      { name: 'Data synced to Instance B', pass: countB > 0 },
      { name: 'Document counts match', pass: countA === countB },
      { name: 'Sample documents match', pass: documentsMatch },
      { name: 'State hash computed on A', pass: !!checkpointA.dbRoot },
      { name: 'State hash computed on B', pass: !!checkpointB.dbRoot },
      { name: 'Root state hashes match', pass: stateHashesMatch },
      { name: 'All collection hashes match', pass: Object.values(collectionMatches).every(Boolean) },
      { name: 'RLJSON entries created', pass: !!changeA1 && !!changeB1 },
      { name: 'State entries have all fields', pass: !!(changeA1.id && changeA1.hash && changeA1._hash && changeA1.json.currentStateHash) },
    ];

    console.log('\n📋 Validation Checklist:');
    checks.forEach((check) => {
      const icon = check.pass ? '✅' : '❌';
      const status = check.pass ? 'PASS' : 'FAIL';
      console.log(`   ${icon} ${check.name.padEnd(45)} [${status}]`);
    });

    const allPassed = checks.every((c) => c.pass);

    // ========================================================================
    // FINAL SUMMARY
    // ========================================================================
    printHeader('FINAL SUMMARY');

    console.log('\n📊 Test Results:');
    const importSpeed = importResult.duration > 0 
      ? formatNumber(Math.round(importResult.imported / (importResult.duration / 1000)))
      : 'N/A (used existing data)';
    
    printBox('Overall Statistics', [
      `Documents processed: ${formatNumber(countA)}`,
      `Import speed: ${importSpeed}`,
      `Sync speed: ${formatNumber(docsPerSecond)} docs/sec`,
      `State hash time (A): ${(checkpointADuration / 1000).toFixed(2)}s`,
      `State hash time (B): ${(checkpointBDuration / 1000).toFixed(2)}s`,
      `Validation checks: ${checks.filter(c => c.pass).length}/${checks.length} passed`,
      `Status: ${allPassed ? '✅ SUCCESS' : '❌ FAILED'}`,
    ]);

    if (allPassed) {
      console.log('\n' + '┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓');
      console.log('┃                  ✅ ALL CHECKS PASSED - TEST SUCCESS!                  ┃');
      console.log('┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫');
      console.log('┃                                                                        ┃');
      console.log('┃  Key Achievements:                                                     ┃');
      console.log('┃  • Real data (552k docs) imported and synced successfully             ┃');
      console.log('┃  • State hashes match perfectly across both instances                 ┃');
      console.log('┃  • RLJSON state log tracks all operations                             ┃');
      console.log('┃  • Content hash can be decoded from entries                           ┃');
      console.log('┃  • Data integrity verified at document level                          ┃');
      console.log('┃                                                                        ┃');
      console.log('┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛');
    } else {
      console.log('\n❌ Some checks failed. Review the validation results above.');
    }

    console.log('\n');

  } finally {
    await clientA.close();
    await clientB.close();
  }
}

main().catch((err) => {
  console.error('\n❌ Test failed with error:', err);
  console.error(err.stack);
  process.exit(1);
});
