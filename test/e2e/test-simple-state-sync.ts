// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * E2E Test: Simple State Log - Cross-Instance Sync
 * 
 * Tests:
 * - Sync data from MongoDB A → MongoDB B
 * - Verify state hashes match after sync
 * - Decode content hash from RLJSON entries
 * - Verify state log can track sync operations
 */

import { MongoClient, type Db } from 'mongodb';
import { SimpleStateLog } from '../../src/simple-state-log.ts';
import { computeStateCheckpoint } from '../../src/hashing/state-hash.ts';

const MONGO_A_URI = 'mongodb://localhost:27017/?directConnection=true';
const MONGO_B_URI = 'mongodb://localhost:27018/?directConnection=true';

function printSection(title: string) {
  console.log('\n' + '━'.repeat(80));
  console.log(`  ${title}`);
  console.log('━'.repeat(80));
}

function printBox(title: string, content: Record<string, any>) {
  console.log('┌────────────────────────────────────────────────────────────┐');
  console.log(`│ ${title.padEnd(58)} │`);
  console.log('├────────────────────────────────────────────────────────────┤');
  Object.entries(content).forEach(([key, value]) => {
    const line = `${key}: ${String(value).slice(0, 50)}`;
    console.log(`│ ${line.padEnd(58)} │`);
  });
  console.log('└────────────────────────────────────────────────────────────┘');
}

async function syncCollection(
  sourceDb: Db,
  targetDb: Db,
  collectionName: string,
): Promise<number> {
  const sourceColl = sourceDb.collection(collectionName);
  const targetColl = targetDb.collection(collectionName);

  // Get all documents from source
  const docs = await sourceColl.find({}).toArray();

  if (docs.length === 0) {
    return 0;
  }

  // Clear target and insert documents
  await targetColl.deleteMany({});
  await targetColl.insertMany(docs);

  return docs.length;
}

async function syncAllCollections(sourceDb: Db, targetDb: Db): Promise<Map<string, number>> {
  const collections = await sourceDb.listCollections().toArray();
  const syncedCounts = new Map<string, number>();

  for (const collInfo of collections) {
    const collName = collInfo.name;

    // Skip system collections
    if (collName.startsWith('system.')) continue;

    // Skip internal collections
    if (['state_checkpoints', 'state_merkle', 'state_changelog'].includes(collName)) {
      continue;
    }

    const count = await syncCollection(sourceDb, targetDb, collName);
    syncedCounts.set(collName, count);
  }

  return syncedCounts;
}

async function main() {
  console.log('\n╔════════════════════════════════════════════════════════════════════════════╗');
  console.log('║              SIMPLE STATE LOG - CROSS-INSTANCE SYNC TEST                  ║');
  console.log('║                                                                            ║');
  console.log('║  Tests: State hash consistency across MongoDB instances                   ║');
  console.log('╚════════════════════════════════════════════════════════════════════════════╝');

  const clientA = await MongoClient.connect(MONGO_A_URI);
  const clientB = await MongoClient.connect(MONGO_B_URI);

  const dbA = clientA.db('test_sync_state_a');
  const dbB = clientB.db('test_sync_state_b');

  try {
    // Clean both databases
    await dbA.dropDatabase();
    await dbB.dropDatabase();

    console.log('\n✅ Connected to both MongoDB instances');
    console.log('   MongoDB A: localhost:27017');
    console.log('   MongoDB B: localhost:27018');

    // ========================================================================
    // STEP 1: CREATE DATA ON INSTANCE A
    // ========================================================================
    printSection('📦 STEP 1: Create Initial Data on Instance A');

    const stateLogA = new SimpleStateLog(dbA);
    await stateLogA.initialize();

    console.log('\n🔵 Inserting data on Instance A...');
    
    // Insert users
    await dbA.collection('users').insertMany([
      { _id: 1, name: 'Alice', email: 'alice@example.com', role: 'admin' },
      { _id: 2, name: 'Bob', email: 'bob@example.com', role: 'user' },
      { _id: 3, name: 'Charlie', email: 'charlie@example.com', role: 'user' },
    ]);
    console.log('   ✓ Inserted 3 users');

    // Insert products
    await dbA.collection('products').insertMany([
      { _id: 101, name: 'Laptop', price: 999.99, stock: 15 },
      { _id: 102, name: 'Mouse', price: 29.99, stock: 50 },
      { _id: 103, name: 'Keyboard', price: 79.99, stock: 30 },
      { _id: 104, name: 'Monitor', price: 299.99, stock: 8 },
    ]);
    console.log('   ✓ Inserted 4 products');

    // Insert orders
    await dbA.collection('orders').insertMany([
      { _id: 201, userId: 1, productId: 101, quantity: 1, total: 999.99 },
      { _id: 202, userId: 2, productId: 102, quantity: 2, total: 59.98 },
    ]);
    console.log('   ✓ Inserted 2 orders');

    // Capture state A initial
    const changeA1 = await stateLogA.captureStateChange('insert', 'Initial data on Instance A');

    console.log('\n📊 Instance A - Initial State:');
    printBox('RLJSON State Entry', {
      id: changeA1.id,
      hash: changeA1.hash.slice(0, 32) + '...',
      type: changeA1.type,
      prevStateHash: changeA1.json.prevStateHash || 'null',
      currentStateHash: changeA1.json.currentStateHash.slice(0, 32) + '...',
      operation: changeA1.json.operation,
    });

    // ========================================================================
    // STEP 2: COMPUTE STATE HASH ON INSTANCE A
    // ========================================================================
    printSection('🔐 STEP 2: Compute State Hash on Instance A');

    const checkpointA = await computeStateCheckpoint({
      db: dbA,
      ignoredColls: new Set(['state_checkpoints', 'state_merkle', 'state_changelog']),
      partitionSize: 50000,
      mode: 'full',
    });

    console.log('\n📈 Instance A - State Checkpoint:');
    console.log(`   Database Root Hash: ${checkpointA.dbRoot}`);
    console.log(`   Collections: ${Object.keys(checkpointA.collections).join(', ')}`);
    console.log(`   Timestamp: ${new Date(checkpointA.ts).toISOString()}`);

    const collectionsA = Object.entries(checkpointA.collections);
    console.log('\n   Collection Details:');
    collectionsA.forEach(([name, info]) => {
      console.log(`     • ${name}: ${info.partitions} partition(s), hash: ${info.root.slice(0, 16)}...`);
    });

    // ========================================================================
    // STEP 3: SYNC DATA FROM A TO B
    // ========================================================================
    printSection('🔄 STEP 3: Sync Data from Instance A → Instance B');

    console.log('\n📤 Syncing collections...');
    const syncedCounts = await syncAllCollections(dbA, dbB);

    console.log('\n✅ Sync completed:');
    syncedCounts.forEach((count, collName) => {
      console.log(`   • ${collName}: ${count} document(s) synced`);
    });

    const stateLogB = new SimpleStateLog(dbB);
    await stateLogB.initialize();

    const changeB1 = await stateLogB.captureStateChange('sync', 'Synced from Instance A');

    console.log('\n📊 Instance B - After Sync:');
    printBox('RLJSON State Entry', {
      id: changeB1.id,
      hash: changeB1.hash.slice(0, 32) + '...',
      type: changeB1.type,
      prevStateHash: changeB1.json.prevStateHash || 'null',
      currentStateHash: changeB1.json.currentStateHash.slice(0, 32) + '...',
      operation: changeB1.json.operation,
    });

    // ========================================================================
    // STEP 4: COMPUTE STATE HASH ON INSTANCE B
    // ========================================================================
    printSection('🔐 STEP 4: Compute State Hash on Instance B');

    const checkpointB = await computeStateCheckpoint({
      db: dbB,
      ignoredColls: new Set(['state_checkpoints', 'state_merkle', 'state_changelog']),
      partitionSize: 50000,
      mode: 'full',
    });

    console.log('\n📈 Instance B - State Checkpoint:');
    console.log(`   Database Root Hash: ${checkpointB.dbRoot}`);
    console.log(`   Collections: ${Object.keys(checkpointB.collections).join(', ')}`);
    console.log(`   Timestamp: ${new Date(checkpointB.ts).toISOString()}`);

    const collectionsB = Object.entries(checkpointB.collections);
    console.log('\n   Collection Details:');
    collectionsB.forEach(([name, info]) => {
      console.log(`     • ${name}: ${info.partitions} partition(s), hash: ${info.root.slice(0, 16)}...`);
    });

    // ========================================================================
    // STEP 5: COMPARE STATE HASHES
    // ========================================================================
    printSection('🔍 STEP 5: Compare State Hashes');

    const stateHashesMatch = checkpointA.dbRoot === checkpointB.dbRoot;

    console.log('\n🎯 State Hash Comparison:');
    console.log('┌────────────────────────────────────────────────────────────┐');
    console.log('│ Instance A:                                                │');
    console.log(`│ ${checkpointA.dbRoot}     │`);
    console.log('│                                                            │');
    console.log('│ Instance B:                                                │');
    console.log(`│ ${checkpointB.dbRoot}     │`);
    console.log('│                                                            │');
    console.log(`│ Match: ${stateHashesMatch ? '✅ YES' : '❌ NO'}                                                  │`);
    console.log('└────────────────────────────────────────────────────────────┘');

    // Compare collection hashes
    console.log('\n📋 Collection Hash Comparison:');
    const allCollections = new Set([
      ...Object.keys(checkpointA.collections),
      ...Object.keys(checkpointB.collections),
    ]);

    const collectionMatches: Record<string, boolean> = {};
    allCollections.forEach((collName) => {
      const hashA = checkpointA.collections[collName]?.root || 'missing';
      const hashB = checkpointB.collections[collName]?.root || 'missing';
      const match = hashA === hashB;
      collectionMatches[collName] = match;
      const icon = match ? '✅' : '❌';
      console.log(`   ${icon} ${collName}: ${match ? 'MATCH' : 'MISMATCH'}`);
    });

    // ========================================================================
    // STEP 6: DECODE CONTENT HASH
    // ========================================================================
    printSection('🔓 STEP 6: Decode Content Hash from RLJSON');

    console.log('\n📄 RLJSON Entry Structure (Instance A):');
    console.log(JSON.stringify(changeA1, null, 2));

    console.log('\n📄 RLJSON Entry Structure (Instance B):');
    console.log(JSON.stringify(changeB1, null, 2));

    console.log('\n🔍 Decoding Content:');
    console.log('   Content hash (Instance A): ' + changeA1.hash);
    console.log('   Decoded from json field:');
    console.log('   ├─ prevStateHash:', changeA1.json.prevStateHash || 'null');
    console.log('   ├─ currentStateHash:', changeA1.json.currentStateHash.slice(0, 48) + '...');
    console.log('   ├─ timestamp:', new Date(changeA1.json.timestamp).toISOString());
    console.log('   ├─ operation:', changeA1.json.operation);
    console.log('   └─ description:', changeA1.json.description);

    console.log('\n   Content hash (Instance B): ' + changeB1.hash);
    console.log('   Decoded from json field:');
    console.log('   ├─ prevStateHash:', changeB1.json.prevStateHash || 'null');
    console.log('   ├─ currentStateHash:', changeB1.json.currentStateHash.slice(0, 48) + '...');
    console.log('   ├─ timestamp:', new Date(changeB1.json.timestamp).toISOString());
    console.log('   ├─ operation:', changeB1.json.operation);
    console.log('   └─ description:', changeB1.json.description);

    // ========================================================================
    // STEP 7: VERIFY DATA INTEGRITY
    // ========================================================================
    printSection('✔️  STEP 7: Verify Data Integrity');

    console.log('\n🔎 Verifying document counts match:');
    const verifications: Array<{ collection: string; countA: number; countB: number; match: boolean }> = [];

    for (const collName of ['users', 'products', 'orders']) {
      const countA = await dbA.collection(collName).countDocuments();
      const countB = await dbB.collection(collName).countDocuments();
      const match = countA === countB;
      verifications.push({ collection: collName, countA, countB, match });
      
      const icon = match ? '✅' : '❌';
      console.log(`   ${icon} ${collName}: A=${countA}, B=${countB}`);
    }

    console.log('\n🔎 Verifying document content matches:');
    for (const collName of ['users', 'products', 'orders']) {
      const docsA = await dbA.collection(collName).find({}).sort({ _id: 1 }).toArray();
      const docsB = await dbB.collection(collName).find({}).sort({ _id: 1 }).toArray();
      
      let contentMatch = true;
      if (docsA.length === docsB.length) {
        for (let i = 0; i < docsA.length; i++) {
          if (JSON.stringify(docsA[i]) !== JSON.stringify(docsB[i])) {
            contentMatch = false;
            break;
          }
        }
      } else {
        contentMatch = false;
      }
      
      const icon = contentMatch ? '✅' : '❌';
      console.log(`   ${icon} ${collName}: ${contentMatch ? 'IDENTICAL' : 'DIFFERENT'}`);
    }

    // ========================================================================
    // STEP 8: UPDATE ON INSTANCE B AND VERIFY DIVERGENCE
    // ========================================================================
    printSection('📝 STEP 8: Update Instance B and Verify Divergence');

    console.log('\n🔵 Making changes on Instance B...');
    await dbB.collection('users').updateOne(
      { _id: 1 },
      { $set: { email: 'alice.updated@example.com' } },
    );
    console.log('   ✓ Updated user email on Instance B');

    const changeB2 = await stateLogB.captureStateChange('update', 'Updated user on Instance B');

    console.log('\n📊 Instance B - After Update:');
    printBox('RLJSON State Entry', {
      id: changeB2.id,
      hash: changeB2.hash.slice(0, 32) + '...',
      prevStateHash: changeB2.json.prevStateHash.slice(0, 32) + '...',
      currentStateHash: changeB2.json.currentStateHash.slice(0, 32) + '...',
      operation: changeB2.json.operation,
    });

    // Compute new state on B
    const checkpointB2 = await computeStateCheckpoint({
      db: dbB,
      ignoredColls: new Set(['state_checkpoints', 'state_merkle', 'state_changelog']),
      partitionSize: 50000,
      mode: 'full',
    });

    const stateHashesDiverged = checkpointA.dbRoot !== checkpointB2.dbRoot;

    console.log('\n🔍 State Hash After Update:');
    console.log('┌────────────────────────────────────────────────────────────┐');
    console.log('│ Instance A (unchanged):                                    │');
    console.log(`│ ${checkpointA.dbRoot}     │`);
    console.log('│                                                            │');
    console.log('│ Instance B (updated):                                      │');
    console.log(`│ ${checkpointB2.dbRoot}     │`);
    console.log('│                                                            │');
    console.log(`│ Diverged: ${stateHashesDiverged ? '✅ YES (as expected)' : '❌ NO (unexpected)'}                              │`);
    console.log('└────────────────────────────────────────────────────────────┘');

    // ========================================================================
    // VALIDATION
    // ========================================================================
    printSection('✅ VALIDATION RESULTS');

    const checks = [
      { name: 'Instances A and B connected', pass: true },
      { name: 'Data synced from A to B', pass: syncedCounts.size > 0 },
      { name: 'State hashes match after sync', pass: stateHashesMatch },
      { name: 'All collection hashes match', pass: Object.values(collectionMatches).every(Boolean) },
      { name: 'Document counts match', pass: verifications.every(v => v.match) },
      { name: 'Content hash can be decoded', pass: !!changeA1.json.currentStateHash },
      { name: 'RLJSON entries created on both instances', pass: !!changeA1 && !!changeB1 },
      { name: 'State entries have all required fields', pass: !!(changeA1.id && changeA1.hash && changeA1.type && changeA1._hash) },
      { name: 'State chain tracked correctly', pass: changeB2.json.prevStateHash === changeB1.json.currentStateHash },
      { name: 'Update causes state divergence', pass: stateHashesDiverged },
    ];

    console.log('\n');
    checks.forEach((check) => {
      const icon = check.pass ? '✓' : '✗';
      const status = check.pass ? 'PASS' : 'FAIL';
      console.log(`  ${icon} ${check.name.padEnd(50)} [${status}]`);
    });

    const allPassed = checks.every((c) => c.pass);

    console.log('\n' + '═'.repeat(80));
    if (allPassed) {
      console.log('  ✅ ALL CHECKS PASSED - Cross-Instance Sync Verified!');
      console.log('  ');
      console.log('  Key Results:');
      console.log('  • State hashes match after sync (data integrity confirmed)');
      console.log('  • Content hash can be decoded from RLJSON entries');
      console.log('  • State log tracks changes across both instances');
      console.log('  • Updates are detected and cause hash divergence');
    } else {
      console.log('  ❌ SOME CHECKS FAILED');
    }
    console.log('═'.repeat(80) + '\n');

  } finally {
    await clientA.close();
    await clientB.close();
  }
}

main().catch((err) => {
  console.error('❌ Test failed:', err);
  process.exit(1);
});
