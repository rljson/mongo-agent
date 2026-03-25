// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * E2E Test: Visual Simple State Log Demonstration
 *
 * Shows clear before/after transformations with:
 * - MongoDB document state (BEFORE)
 * - State hash computation
 * - RLJSON format (AFTER)
 * - Type structure comparison
 */

import { MongoClient } from 'mongodb';

import { computeStateCheckpoint } from '../../src/hashing/state-hash.ts';
import { SimpleStateLog } from '../../src/simple-state-log.ts';

function printBox(title: string, content: string, width = 80) {
  const border = '═'.repeat(width - 2);
  console.log(`╔${border}╗`);
  console.log(`║ ${title.padEnd(width - 4)} ║`);
  console.log(`╠${border}╣`);
  content.split('\n').forEach((line) => {
    console.log(`║ ${line.padEnd(width - 4)} ║`);
  });
  console.log(`╚${border}╝`);
}

function printSection(title: string) {
  console.log('\n' + '━'.repeat(80));
  console.log(`  ${title}`);
  console.log('━'.repeat(80));
}

async function main() {
  console.log(
    '\n╔════════════════════════════════════════════════════════════════════════════╗',
  );
  console.log(
    '║                    SIMPLE STATE LOG - VISUAL DEMO                          ║',
  );
  console.log(
    '║                                                                            ║',
  );
  console.log(
    '║  Shows: MongoDB State → State Hash → RLJSON Format                        ║',
  );
  console.log(
    '╚════════════════════════════════════════════════════════════════════════════╝\n',
  );

  const client = await MongoClient.connect(
    'mongodb://localhost:27017/?directConnection=true',
  );
  const db = client.db('test_simple_state_visual');

  try {
    await db.dropDatabase();

    // ========================================================================
    // SCENARIO 1: INITIAL STATE
    // ========================================================================
    printSection('📦 SCENARIO 1: INITIAL DATABASE STATE');

    console.log('\n🔵 BEFORE - MongoDB Collections (Empty):');
    console.log(
      '┌────────────────────────────────────────────────────────────┐',
    );
    console.log(
      '│ collections: []                                            │',
    );
    console.log(
      '│ documents: 0                                               │',
    );
    console.log(
      '│ state: EMPTY                                               │',
    );
    console.log(
      '└────────────────────────────────────────────────────────────┘',
    );

    const stateLog = new SimpleStateLog(db);
    await stateLog.initialize();

    console.log('\n⚙️  Computing initial state hash...');
    const checkpoint0 = await computeStateCheckpoint({
      db,
      ignoredColls: new Set([
        'state_checkpoints',
        'state_merkle',
        'state_changelog',
      ]),
      partitionSize: 50000,
      mode: 'incremental',
    });

    console.log('\n📊 State Hash Computation:');
    console.log(
      '┌────────────────────────────────────────────────────────────┐',
    );
    console.log(
      `│ Collections scanned: ${Object.keys(checkpoint0.collections).length}                                      │`,
    );
    console.log(
      `│ Total partitions: ${Object.values(checkpoint0.collections).reduce((sum, c) => sum + c.partitions, 0)}                                         │`,
    );
    console.log(`│ State hash: ${checkpoint0.dbRoot.slice(0, 32)}... │`);
    console.log(
      '└────────────────────────────────────────────────────────────┘',
    );

    // ========================================================================
    // SCENARIO 2: INSERT DOCUMENTS
    // ========================================================================
    printSection('📝 SCENARIO 2: INSERT USERS COLLECTION');

    console.log('\n🔵 BEFORE - Insert Operation:');
    console.log(
      '┌────────────────────────────────────────────────────────────┐',
    );
    console.log(
      '│ Operation: db.collection("users").insertMany([...])       │',
    );
    console.log(
      '│                                                            │',
    );
    console.log(
      '│ Documents to insert:                                       │',
    );
    console.log(
      '│   { _id: 1, name: "Alice", age: 30, role: "admin" }       │',
    );
    console.log(
      '│   { _id: 2, name: "Bob", age: 25, role: "user" }          │',
    );
    console.log(
      '│   { _id: 3, name: "Charlie", age: 35, role: "user" }      │',
    );
    console.log(
      '└────────────────────────────────────────────────────────────┘',
    );

    await db.collection('users').insertMany([
      { _id: 1, name: 'Alice', age: 30, role: 'admin' },
      { _id: 2, name: 'Bob', age: 25, role: 'user' },
      { _id: 3, name: 'Charlie', age: 35, role: 'user' },
    ]);

    const change1 = await stateLog.captureStateChange(
      'insert',
      'Added users collection',
    );

    console.log('\n🟢 AFTER - RLJSON State Change Entry:');
    console.log(
      '┌────────────────────────────────────────────────────────────┐',
    );
    console.log(
      '│ RLJSON STRUCTURE:                                          │',
    );
    console.log(
      '├────────────────────────────────────────────────────────────┤',
    );
    console.log(
      `│ id:     "${change1.id}"                                    │`,
    );
    console.log(`│ hash:   "${change1.hash.slice(0, 48)}..." │`);
    console.log(
      `│ type:   "state_change"                                     │`,
    );
    console.log(`│ _hash:  "${change1._hash?.slice(0, 48)}..." │`);
    console.log(
      '│                                                            │',
    );
    console.log(
      '│ json:   {                                                  │',
    );
    console.log(
      `│   prevStateHash: ${change1.json.prevStateHash === null ? 'null (initial)                             ' : `"${change1.json.prevStateHash.slice(0, 32)}..."`}│`,
    );
    console.log(
      `│   currentStateHash: "${change1.json.currentStateHash.slice(0, 32)}..." │`,
    );
    console.log(
      `│   timestamp: ${change1.json.timestamp}                             │`,
    );
    console.log(
      `│   operation: "${change1.json.operation}"                                  │`,
    );
    console.log(`│   description: "${change1.json.description}"            │`);
    console.log(
      '│ }                                                          │',
    );
    console.log(
      '└────────────────────────────────────────────────────────────┘',
    );

    console.log('\n📊 State Comparison:');
    console.log(
      '┌────────────────────────────────────────────────────────────┐',
    );
    console.log(
      `│ Previous: ${change1.json.prevStateHash || 'null (empty DB)'}                               │`,
    );
    console.log(
      `│ Current:  ${change1.json.currentStateHash.slice(0, 48)}... │`,
    );
    console.log(
      `│ Changed:  ${change1.json.prevStateHash !== change1.json.currentStateHash ? '✓ YES' : '✗ NO'}                                              │`,
    );
    console.log(
      '└────────────────────────────────────────────────────────────┘',
    );

    // ========================================================================
    // SCENARIO 3: UPDATE DOCUMENT
    // ========================================================================
    printSection('✏️  SCENARIO 3: UPDATE A USER');

    console.log('\n🔵 BEFORE - Update Operation:');
    console.log(
      '┌────────────────────────────────────────────────────────────┐',
    );
    console.log(
      '│ Operation: db.collection("users").updateOne(...)          │',
    );
    console.log(
      '│                                                            │',
    );
    console.log(
      '│ Match: { _id: 1 }                                          │',
    );
    console.log(
      '│ Update: { $set: { age: 31, lastLogin: "2026-03-25" } }    │',
    );
    console.log(
      '│                                                            │',
    );
    console.log(
      '│ Before: { _id: 1, name: "Alice", age: 30, role: "admin" } │',
    );
    console.log(
      '│ After:  { _id: 1, name: "Alice", age: 31, role: "admin",  │',
    );
    console.log(
      '│           lastLogin: "2026-03-25" }                        │',
    );
    console.log(
      '└────────────────────────────────────────────────────────────┘',
    );

    await db
      .collection('users')
      .updateOne({ _id: 1 }, { $set: { age: 31, lastLogin: '2026-03-25' } });

    const change2 = await stateLog.captureStateChange(
      'update',
      'Updated Alice profile',
    );

    console.log('\n🟢 AFTER - RLJSON State Change Entry:');
    console.log(
      '┌────────────────────────────────────────────────────────────┐',
    );
    console.log(
      '│ RLJSON STRUCTURE:                                          │',
    );
    console.log(
      '├────────────────────────────────────────────────────────────┤',
    );
    console.log(
      `│ id:     "${change2.id}"                                    │`,
    );
    console.log(`│ hash:   "${change2.hash.slice(0, 48)}..." │`);
    console.log(
      `│ type:   "state_change"                                     │`,
    );
    console.log(`│ _hash:  "${change2._hash?.slice(0, 48)}..." │`);
    console.log(
      '│                                                            │',
    );
    console.log(
      '│ json:   {                                                  │',
    );
    console.log(
      `│   prevStateHash: "${change2.json.prevStateHash?.slice(0, 32)}..." │`,
    );
    console.log(
      `│   currentStateHash: "${change2.json.currentStateHash.slice(0, 32)}..." │`,
    );
    console.log(
      `│   timestamp: ${change2.json.timestamp}                             │`,
    );
    console.log(
      `│   operation: "${change2.json.operation}"                                  │`,
    );
    console.log(`│   description: "${change2.json.description}"           │`);
    console.log(
      '│ }                                                          │',
    );
    console.log(
      '└────────────────────────────────────────────────────────────┘',
    );

    console.log('\n📊 State Comparison:');
    console.log(
      '┌────────────────────────────────────────────────────────────┐',
    );
    console.log(`│ Previous: ${change2.json.prevStateHash?.slice(0, 48)}... │`);
    console.log(
      `│ Current:  ${change2.json.currentStateHash.slice(0, 48)}... │`,
    );
    console.log(
      `│ Changed:  ${change2.json.prevStateHash !== change2.json.currentStateHash ? '✓ YES' : '✗ NO'}                                              │`,
    );
    console.log(
      '└────────────────────────────────────────────────────────────┘',
    );

    // ========================================================================
    // SCENARIO 4: ADD NEW COLLECTION
    // ========================================================================
    printSection('📚 SCENARIO 4: ADD PRODUCTS COLLECTION');

    console.log('\n🔵 BEFORE - Insert New Collection:');
    console.log(
      '┌────────────────────────────────────────────────────────────┐',
    );
    console.log(
      '│ Operation: db.collection("products").insertMany([...])    │',
    );
    console.log(
      '│                                                            │',
    );
    console.log(
      '│ Documents to insert:                                       │',
    );
    console.log(
      '│   { _id: 101, name: "Widget", price: 9.99, stock: 100 }   │',
    );
    console.log(
      '│   { _id: 102, name: "Gadget", price: 19.99, stock: 50 }   │',
    );
    console.log(
      '│   { _id: 103, name: "Doohickey", price: 5.99, stock: 200 }│',
    );
    console.log(
      '└────────────────────────────────────────────────────────────┘',
    );

    await db.collection('products').insertMany([
      { _id: 101, name: 'Widget', price: 9.99, stock: 100 },
      { _id: 102, name: 'Gadget', price: 19.99, stock: 50 },
      { _id: 103, name: 'Doohickey', price: 5.99, stock: 200 },
    ]);

    const change3 = await stateLog.captureStateChange(
      'insert',
      'Added products collection',
    );

    console.log('\n🟢 AFTER - RLJSON State Change Entry:');
    console.log(
      '┌────────────────────────────────────────────────────────────┐',
    );
    console.log(
      '│ RLJSON STRUCTURE:                                          │',
    );
    console.log(
      '├────────────────────────────────────────────────────────────┤',
    );
    console.log(
      `│ id:     "${change3.id}"                                    │`,
    );
    console.log(`│ hash:   "${change3.hash.slice(0, 48)}..." │`);
    console.log(
      `│ type:   "state_change"                                     │`,
    );
    console.log(`│ _hash:  "${change3._hash?.slice(0, 48)}..." │`);
    console.log(
      '│                                                            │',
    );
    console.log(
      '│ json:   {                                                  │',
    );
    console.log(
      `│   prevStateHash: "${change3.json.prevStateHash?.slice(0, 32)}..." │`,
    );
    console.log(
      `│   currentStateHash: "${change3.json.currentStateHash.slice(0, 32)}..." │`,
    );
    console.log(
      `│   timestamp: ${change3.json.timestamp}                             │`,
    );
    console.log(
      `│   operation: "${change3.json.operation}"                                  │`,
    );
    console.log(`│   description: "${change3.json.description}"        │`);
    console.log(
      '│ }                                                          │',
    );
    console.log(
      '└────────────────────────────────────────────────────────────┘',
    );

    console.log('\n📊 State Comparison:');
    console.log(
      '┌────────────────────────────────────────────────────────────┐',
    );
    console.log(`│ Previous: ${change3.json.prevStateHash?.slice(0, 48)}... │`);
    console.log(
      `│ Current:  ${change3.json.currentStateHash.slice(0, 48)}... │`,
    );
    console.log(
      `│ Changed:  ${change3.json.prevStateHash !== change3.json.currentStateHash ? '✓ YES' : '✗ NO'}                                              │`,
    );
    console.log(
      '└────────────────────────────────────────────────────────────┘',
    );

    // ========================================================================
    // TYPE COMPARISON
    // ========================================================================
    printSection('🔍 TYPE STRUCTURE COMPARISON');

    console.log('\n📋 MONGODB DOCUMENT (BEFORE):');
    console.log(
      '┌────────────────────────────────────────────────────────────┐',
    );
    console.log(
      '│ Type: MongoDB Document (BSON)                              │',
    );
    console.log(
      '├────────────────────────────────────────────────────────────┤',
    );
    console.log(
      '│ {                                                          │',
    );
    console.log(
      '│   _id: ObjectId | string | number                         │',
    );
    console.log(
      '│   ...fields: any                                           │',
    );
    console.log(
      '│ }                                                          │',
    );
    console.log(
      '│                                                            │',
    );
    console.log(
      '│ Storage: MongoDB Collections                               │',
    );
    console.log(
      '│ Format: BSON                                               │',
    );
    console.log(
      '│ Tracking: Per-document                                     │',
    );
    console.log(
      '└────────────────────────────────────────────────────────────┘',
    );

    console.log('\n📋 RLJSON STATE CHANGE (AFTER):');
    console.log(
      '┌────────────────────────────────────────────────────────────┐',
    );
    console.log(
      '│ Type: SimpleStateChange                                    │',
    );
    console.log(
      '├────────────────────────────────────────────────────────────┤',
    );
    console.log(
      '│ {                                                          │',
    );
    console.log(
      '│   id: string           // Unique identifier               │',
    );
    console.log(
      '│   hash: string         // Content hash                     │',
    );
    console.log(
      '│   type: string         // Always "state_change"            │',
    );
    console.log(
      '│   _hash: string        // Row hash                         │',
    );
    console.log(
      '│   json: {                                                  │',
    );
    console.log(
      '│     prevStateHash: string | null  // Before state          │',
    );
    console.log(
      '│     currentStateHash: string      // After state           │',
    );
    console.log(
      '│     timestamp: number             // When changed          │',
    );
    console.log(
      '│     operation: string             // What changed          │',
    );
    console.log(
      '│     description?: string          // Why changed           │',
    );
    console.log(
      '│   }                                                        │',
    );
    console.log(
      '│ }                                                          │',
    );
    console.log(
      '│                                                            │',
    );
    console.log(
      '│ Storage: BsMem (in-memory) or MongoDB                      │',
    );
    console.log(
      '│ Format: JSON                                               │',
    );
    console.log(
      '│ Tracking: Entire DB state                                  │',
    );
    console.log(
      '└────────────────────────────────────────────────────────────┘',
    );

    // ========================================================================
    // STATE CHAIN EVOLUTION
    // ========================================================================
    printSection('🔗 STATE CHAIN EVOLUTION');

    const chain = stateLog.getStateChain();
    console.log('\n📜 Complete State History:');
    console.log(
      '┌────────────────────────────────────────────────────────────┐',
    );
    chain.forEach((link, idx) => {
      console.log(
        `│ [${idx + 1}] ${link.operation.toUpperCase().padEnd(10)} at ${new Date(link.timestamp).toLocaleTimeString().padEnd(11)}             │`,
      );
      console.log(
        `│     From: ${(link.from?.slice(0, 40) || 'null').padEnd(40)}     │`,
      );
      console.log(`│     To:   ${link.to.slice(0, 40).padEnd(40)}     │`);
      if (idx < chain.length - 1)
        console.log(
          '│     ↓                                                      │',
        );
    });
    console.log(
      '└────────────────────────────────────────────────────────────┘',
    );

    // ========================================================================
    // VALIDATION
    // ========================================================================
    printSection('✅ VALIDATION CHECKS');

    const checks = [
      {
        name: 'RLJSON has id field',
        pass: !!change1.id && typeof change1.id === 'string',
      },
      {
        name: 'RLJSON has hash field',
        pass: !!change1.hash && typeof change1.hash === 'string',
      },
      { name: 'RLJSON has type field', pass: change1.type === 'state_change' },
      {
        name: 'RLJSON has _hash field',
        pass: !!change1._hash && typeof change1._hash === 'string',
      },
      {
        name: 'RLJSON has json.prevStateHash',
        pass: 'prevStateHash' in change1.json,
      },
      {
        name: 'RLJSON has json.currentStateHash',
        pass: !!change1.json.currentStateHash,
      },
      {
        name: 'RLJSON has json.timestamp',
        pass: typeof change1.json.timestamp === 'number',
      },
      {
        name: 'RLJSON has json.operation',
        pass: typeof change1.json.operation === 'string',
      },
      { name: 'State chain is valid', pass: chain.length === 3 },
      {
        name: 'All states are different',
        pass: new Set(chain.map((c) => c.to)).size === 3,
      },
      {
        name: 'State chain is continuous',
        pass: chain.every(
          (link, i) => i === 0 || link.from === chain[i - 1].to,
        ),
      },
      { name: 'First state has null prev', pass: chain[0].from === null },
    ];

    console.log('\n');
    checks.forEach((check) => {
      const icon = check.pass ? '✓' : '✗';
      const status = check.pass ? 'PASS' : 'FAIL';
      console.log(`  ${icon} ${check.name.padEnd(40)} [${status}]`);
    });

    const allPassed = checks.every((c) => c.pass);

    console.log('\n' + '═'.repeat(80));
    if (allPassed) {
      console.log(
        '  ✅ ALL CHECKS PASSED - Simple State Log Working Correctly!',
      );
    } else {
      console.log('  ❌ SOME CHECKS FAILED');
    }
    console.log('═'.repeat(80));

    // ========================================================================
    // KEY BENEFITS
    // ========================================================================
    printSection('💡 KEY BENEFITS');

    console.log(`
  ✓ Simple Structure
    - Just 5 fields: id, hash, type, json, _hash
    - Easy to understand and implement

  ✓ Uses Existing State Hash System
    - No new hashing infrastructure needed
    - Leverages computeStateCheckpoint()
    - Proven merkle tree partitioning

  ✓ Tracks Entire DB State
    - One hash represents entire database
    - Includes all collections and documents
    - Fast comparison: just compare 2 hashes

  ✓ Enables State Recreation
    - Can restore DB to any previous state
    - State hash → checkpoint → partitions → documents
    - Fast and deterministic

  ✓ Flexible Storage
    - In-memory (BsMem) for testing
    - MongoDB for persistence
    - Blob storage ready

  ✓ No Schema Discovery
    - No type conversion needed
    - No TablesCfgTable complexity
    - Works with any MongoDB schema
`);

    console.log('\n' + '═'.repeat(80));
    console.log('  🎉 Visual demonstration complete!');
    console.log('═'.repeat(80) + '\n');
  } finally {
    await client.close();
  }
}

main().catch((err) => {
  console.error('❌ Test failed:', err);
  process.exit(1);
});
