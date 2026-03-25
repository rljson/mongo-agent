// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * E2E Test: Simple State Log with RLJSON
 * 
 * Demonstrates:
 * - Simple RLJSON format: {id, hash, type, json: {prevStateHash, currentStateHash}}
 * - Captures entire DB state changes
 * - Stores in memory (BsMem)
 * - Enables fast state comparison and recreation
 * - Uses existing state hash system
 */

import { MongoClient } from 'mongodb';
import { BsMem } from '@rljson/bs';
import { SimpleStateLog } from '../../src/simple-state-log.ts';

async function main() {
  console.log('='.repeat(80));
  console.log('SIMPLE STATE LOG TEST - RLJSON Format');
  console.log('='.repeat(80));

  const client = await MongoClient.connect(
    'mongodb://localhost:27017/?directConnection=true',
  );
  const db = client.db('test_simple_state_log');


  console.log('\n📦 Step 1: Initialize SimpleStateLog');
  console.log('-'.repeat(80));
  const stateLog = new SimpleStateLog(db, bs);
  await stateLog.initialize();
  console.log('✓ SimpleStateLog initialized');

  console.log('\n📝 Step 2: Insert initial documents');
  console.log('-'.repeat(80));
  await db.collection('users').insertMany([
    { _id: 1, name: 'Alice', age: 30 },
    { _id: 2, name: 'Bob', age: 25 },
  ]);
  console.log('✓ Inserted 2 users');

  const change1 = await stateLog.captureStateChange('insert', 'Initial users');
  console.log('\n🎯 State Change #1:');
  console.log(JSON.stringify(change1, null, 2));

  console.log('\n📝 Step 3: Update documents');
  console.log('-'.repeat(80));
  await db.collection('users').updateOne({ _id: 1 }, { $set: { age: 31 } });
  console.log('✓ Updated Alice age: 30 → 31');

  const change2 = await stateLog.captureStateChange('update', 'Updated Alice age');
  console.log('\n🎯 State Change #2:');
  console.log(JSON.stringify(change2, null, 2));

  console.log('\n📝 Step 4: Add new collection');
  console.log('-'.repeat(80));
  await db.collection('products').insertMany([
    { _id: 101, name: 'Widget', price: 9.99 },
    { _id: 102, name: 'Gadget', price: 19.99 },
  ]);
  console.log('✓ Inserted 2 products');

  const change3 = await stateLog.captureStateChange('insert', 'Added products collection');
  console.log('\n🎯 State Change #3:');
  console.log(JSON.stringify(change3, null, 2));

  console.log('\n📊 Step 5: Analyze state chain');
  console.log('-'.repeat(80));
  const chain = stateLog.getStateChain();
  console.log('State Evolution Chain:');
  chain.forEach((link, idx) => {
    console.log(`\n[${idx + 1}] ${link.operation}`);
    console.log(`    From: ${link.from || 'null (initial)'}`);
    console.log(`    To:   ${link.to}`);
    console.log(`    Time: ${new Date(link.timestamp).toISOString()}`);
  });

  console.log('\n🔍 Step 6: State comparison');
  console.log('-'.repeat(80));
  const stateChanged12 = !stateLog.compareStates(
    change1.json.currentStateHash,
    change2.json.currentStateHash,
  );
  const stateChanged23 = !stateLog.compareStates(
    change2.json.currentStateHash,
    change3.json.currentStateHash,
  );

  console.log(`Change 1 → 2: ${stateChanged12 ? '✓ State changed' : '✗ State unchanged'}`);
  console.log(`Change 2 → 3: ${stateChanged23 ? '✓ State changed' : '✗ State unchanged'}`);

  console.log('\n💾 Step 7: Persist to MongoDB');
  console.log('-'.repeat(80));
  await stateLog.persist('state_changelog');
  const count = await db.collection('state_changelog').countDocuments();
  console.log(`✓ Persisted ${count} state changes to MongoDB`);

  console.log('\n🔄 Step 8: Load from MongoDB');
  console.log('-'.repeat(80));
    const newStateLog = new SimpleStateLog(db);
  await newStateLog.load('state_changelog');
  const loadedLog = newStateLog.getChangeLog();
  console.log(`✓ Loaded ${loadedLog.length} state changes from MongoDB`);

  console.log('\n📋 Step 9: Verify RLJSON structure');
  console.log('-'.repeat(80));
  const sampleEntry = loadedLog[0];
  console.log('Sample RLJSON Entry:');
  console.log('├─ id:', sampleEntry.id);
  console.log('├─ hash:', sampleEntry.hash);
  console.log('├─ type:', sampleEntry.type);
  console.log('├─ _hash:', sampleEntry._hash);
  console.log('└─ json:');
  console.log('   ├─ prevStateHash:', sampleEntry.json.prevStateHash || 'null');
  console.log('   ├─ currentStateHash:', sampleEntry.json.currentStateHash.slice(0, 16) + '...');
  console.log('   ├─ timestamp:', new Date(sampleEntry.json.timestamp).toISOString());
  console.log('   ├─ operation:', sampleEntry.json.operation);
  console.log('   └─ description:', sampleEntry.json.description);

  console.log('\n✅ VALIDATION CHECKS');
  console.log('-'.repeat(80));
  
  const checks = [
    { name: 'Has id field', pass: !!sampleEntry.id },
    { name: 'Has hash field', pass: !!sampleEntry.hash },
    { name: 'Has type field', pass: sampleEntry.type === 'state_change' },
    { name: 'Has _hash field', pass: !!sampleEntry._hash },
    { name: 'Has json.prevStateHash', pass: 'prevStateHash' in sampleEntry.json },
    { name: 'Has json.currentStateHash', pass: !!sampleEntry.json.currentStateHash },
    { name: 'Has json.timestamp', pass: typeof sampleEntry.json.timestamp === 'number' },
    { name: 'Has json.operation', pass: !!sampleEntry.json.operation },
    { name: 'State chain valid', pass: chain.length === 3 },
    { name: 'States are different', pass: stateChanged12 && stateChanged23 },
  ];

  checks.forEach((check) => {
    console.log(`${check.pass ? '✓' : '✗'} ${check.name}`);
  });

  const allPassed = checks.every((c) => c.pass);
  
  console.log('\n' + '='.repeat(80));
  console.log(allPassed ? '✅ ALL CHECKS PASSED' : '❌ SOME CHECKS FAILED');
  console.log('='.repeat(80));

  console.log('\n💡 KEY BENEFITS:');
  console.log('   • Simple RLJSON format - easy to understand');
  console.log('   • Uses existing state hash system');
  console.log('   • Tracks entire DB state over time');
  console.log('   • Enables fast state comparison');
  console.log('   • Can recreate DB to any previous state');
  console.log('   • Stores in memory (BsMem) or MongoDB');
  console.log('   • No schema discovery needed');
  console.log('   • Lightweight and fast');

  console.log('\n📚 COMPARISON:');
  console.log('   Complex RLJSON (for future):');
  console.log('   • ComponentsTable per collection');
  console.log('   • TablesCfgTable with schemas');
  console.log('   • Full type conversion');
  console.log('   • Detailed document tracking');
  console.log('');
  console.log('   Simple RLJSON (current):');
  console.log('   • One log for entire DB');
  console.log('   • Just track state hashes');
  console.log('   • No schema needed');
  console.log('   • Fast and simple');

  } finally {
    await client.close();
  }
}

main().catch((err) => {
  console.error('❌ Test failed:', err);
  process.exit(1);
});
