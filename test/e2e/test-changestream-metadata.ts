#!/usr/bin/env node
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * Demonstrates change stream metadata and blockchain chain relationship
 * Shows what MongoDB provides vs what we track for the blockchain
 */

import { BsMem } from '@rljson/bs';

import { MongoClient } from 'mongodb';

const MONGO_URI =
  process.env.MONGO_URI || 'mongodb://localhost:27017/?directConnection=true';
const DB_NAME = 'test_changestream_chain';

function printSection(title: string) {
  console.log('\n' + '═'.repeat(80));
  console.log(`  ${title}`);
  console.log('═'.repeat(80) + '\n');
}

async function main() {
  console.log('\n🔗 Change Stream Metadata & Blockchain Chain Demo\n');

  const client = new MongoClient(MONGO_URI);
  await client.connect();

  try {
    const db = client.db(DB_NAME);

    // Clean slate
    await db.dropDatabase();

    // ========================================================================
    // PART 1: What MongoDB Change Stream Provides
    // ========================================================================
    printSection('PART 1: MongoDB Change Stream Event Structure');

    console.log('MongoDB change stream events include:\n');
    console.log('📄 Change Event Structure:');
    console.log(`{
  _id: {                              ← Resume Token (unique identifier)
    _data: "8267ABC123...",
    _typeBits: ...
  },
  operationType: "insert",            ← What happened
  clusterTime: Timestamp(1234567890), ← MongoDB cluster timestamp
  wallTime: 2026-03-24T10:00:00.000Z, ← Wall clock time
  ns: {                               ← Namespace
    db: "mydb",
    coll: "users"
  },
  documentKey: {                      ← Document identifier
    _id: "user123"
  },
  fullDocument: {                     ← The actual document
    _id: "user123",
    name: "Alice",
    ...
  }
}`);

    console.log('\n🔑 Resume Token (_id):');
    console.log('   • Unique identifier for this change event');
    console.log('   • Can be used to resume watching from this point');
    console.log('   • MongoDB guarantees: no duplicates, ordering preserved');
    console.log('   • Format: Opaque token (encoded timestamp + position)');

    console.log('\n⏰ Cluster Time:');
    console.log('   • MongoDB internal timestamp (BSON Timestamp)');
    console.log('   • Represents the logical time in the MongoDB cluster');
    console.log('   • Used for causality and ordering in distributed systems');

    // ========================================================================
    // PART 2: What We Currently Store
    // ========================================================================
    printSection('PART 2: Current sync_ops Storage');

    console.log('Our sync_ops ComponentsTable stores:\n');
    console.log('📊 SyncOpDoc Structure:');
    console.log(`{
  _id: "nodeA_1",              ← Our ID (origin_sequence)
  origin: "nodeA",             ← Which node captured this
  seq: 1,                      ← Sequence number

  // Blockchain chain fields
  prevHash: "GENESIS",         ← Previous operation's chainHash
  opHash: "abc123...",         ← Hash of current operation data
  chainHash: "xyz789...",      ← Hash(prevHash + opHash)

  // MongoDB change data
  ns: { db: "mydb", coll: "users" },
  operationType: "insert",
  docId: "user123",
  payload: { fullDocument: {...} },
  ts: "2026-03-24T10:00:00.000Z"

  // MISSING: MongoDB change stream metadata!
  // ❌ No resume token
  // ❌ No cluster time
}`);

    console.log('\n✅ What we track:');
    console.log('   • Blockchain chain (prevHash → chainHash)');
    console.log('   • Sequence number (seq)');
    console.log('   • Operation content (operationType, payload)');
    console.log('   • Our timestamp (ts)');

    console.log("\n❌ What we DON'T track:");
    console.log('   • MongoDB resume token (change._id)');
    console.log('   • MongoDB cluster time');
    console.log('   • Original change stream event ID');

    // ========================================================================
    // PART 3: Why Change Stream Metadata Matters
    // ========================================================================
    printSection('PART 3: Why Change Stream Metadata Matters');

    console.log('🎯 Resume Token Benefits:\n');
    console.log('1. Correlation');
    console.log('   • Link sync_ops back to MongoDB change stream');
    console.log("   • Verify our operations match MongoDB's order");

    console.log('\n2. Verification');
    console.log('   • MongoDB guarantees: resumeToken1 < resumeToken2');
    console.log('   • Can verify: our seq order matches MongoDB order');
    console.log('   • Detect if we missed any events');

    console.log('\n3. Debugging');
    console.log('   • Trace back to exact MongoDB change event');
    console.log('   • Reproduce issues from specific resume point');
    console.log('   • Correlate with MongoDB logs');

    console.log('\n4. Multi-Source Sync');
    console.log('   • If syncing from multiple MongoDB instances');
    console.log('   • Resume token helps identify source');
    console.log('   • Cluster time helps with causality');

    console.log('\n⏰ Cluster Time Benefits:\n');
    console.log("   • MongoDB's logical clock");
    console.log('   • Guaranteed monotonic (always increasing)');
    console.log('   • Useful for distributed causality');
    console.log('   • Can detect clock drift vs wall time');

    // ========================================================================
    // PART 4: Live Demonstration
    // ========================================================================
    printSection('PART 4: Live Change Stream Capture');

    console.log('Starting change stream to capture real events...\n');

    const bs = new BsMem();
    const capturedEvents: any[] = [];

    // Create a raw change stream to see the full event
    const collection = db.collection('test_data');
    const rawChangeStream = collection.watch([], {
      fullDocument: 'updateLookup',
    });

    // Capture first event
    const eventPromise = new Promise((resolve) => {
      rawChangeStream.once('change', (change) => {
        capturedEvents.push(change);
        resolve(change);
      });
    });

    // Insert a document to trigger change event
    console.log('Inserting document to trigger change event...');
    await collection.insertOne({ name: 'Test User', value: 42 });

    // Wait for event
    const changeEvent = (await eventPromise) as any;
    await rawChangeStream.close();

    console.log('\n📡 Raw Change Stream Event:');
    console.log('─'.repeat(80));
    console.log(JSON.stringify(changeEvent, null, 2));
    console.log('─'.repeat(80));

    console.log('\n🔍 Key Fields Extraction:');
    console.log(`   Resume Token (_id): ${JSON.stringify(changeEvent._id)}`);
    console.log(`   Cluster Time: ${changeEvent.clusterTime}`);
    console.log(`   Wall Time: ${changeEvent.wallTime}`);
    console.log(`   Operation Type: ${changeEvent.operationType}`);
    console.log(`   Document _id: ${changeEvent.documentKey._id}`);

    // ========================================================================
    // PART 5: Proposed Enhanced Schema
    // ========================================================================
    printSection('PART 5: Proposed Enhanced sync_ops Schema');

    console.log('Enhanced SyncOpDoc with change stream metadata:\n');
    console.log('📊 Enhanced Structure:');
    console.log(`{
  // Our fields (existing)
  _id: "nodeA_1",
  origin: "nodeA",
  seq: 1,
  prevHash: "GENESIS",
  opHash: "abc123...",
  chainHash: "xyz789...",

  // Operation data (existing)
  ns: { db: "mydb", coll: "users" },
  operationType: "insert",
  docId: "user123",
  payload: { fullDocument: {...} },
  ts: "2026-03-24T10:00:00.000Z",

  // NEW: Change stream metadata
  changeStreamId: {                    ← MongoDB resume token
    _data: "8267ABC123...",
    _typeBits: ...
  },
  clusterTime: Timestamp(1234567890),  ← MongoDB cluster time
  wallTime: "2026-03-24T10:00:00.000Z" ← MongoDB wall time
}`);

    console.log('\n📋 Required TableCfg Updates:');
    console.log(`
SYNC_OPS_TABLE_CFG columns should include:

// Existing columns...
{ key: '_hash', type: 'string' },
{ key: '_id', type: 'string' },
{ key: 'origin', type: 'string' },
{ key: 'seq', type: 'number' },
{ key: 'operationType', type: 'string' },
{ key: 'prevHash', type: 'string' },
{ key: 'opHash', type: 'string' },
{ key: 'chainHash', type: 'string' },
{ key: 'ns', type: 'json' },
{ key: 'docId', type: 'string' },
{ key: 'payload', type: 'json' },
{ key: 'ts', type: 'string' },

// NEW columns for change stream metadata
{ key: 'changeStreamId', type: 'json' },    // Resume token
{ key: 'clusterTime', type: 'json' },       // BSON Timestamp
{ key: 'wallTime', type: 'string' }         // ISO timestamp
`);

    // ========================================================================
    // PART 6: Blockchain Chain vs Resume Token
    // ========================================================================
    printSection('PART 6: Blockchain Chain vs Resume Token');

    console.log('Two Different Guarantees:\n');

    console.log('🔗 Our Blockchain Chain:');
    console.log('   Purpose: Content integrity and ordering');
    console.log('   Mechanism: Hash chain (prevHash → chainHash)');
    console.log('   Guarantee: Tamper detection, sequential processing');
    console.log('   Scope: Our sync system');
    console.log('   Example:');
    console.log('     Op1: prevHash=GENESIS, chainHash=abc123');
    console.log('     Op2: prevHash=abc123,  chainHash=xyz789');
    console.log('     Op3: prevHash=xyz789,  chainHash=def456');

    console.log('\n🎫 MongoDB Resume Token:');
    console.log('   Purpose: Resume point and MongoDB ordering');
    console.log('   Mechanism: MongoDB internal event ID');
    console.log('   Guarantee: Can resume from exact point, no duplicates');
    console.log('   Scope: MongoDB change stream');
    console.log('   Example:');
    console.log('     Event1: _id={_data:"8267A...", ...}');
    console.log('     Event2: _id={_data:"8267B...", ...}');
    console.log('     Event3: _id={_data:"8267C...", ...}');

    console.log('\n🤝 Both Together Provide:');
    console.log('   ✓ Content integrity (blockchain chain)');
    console.log('   ✓ Resume capability (resume token)');
    console.log('   ✓ Ordering verification (both)');
    console.log('   ✓ Correlation with MongoDB (resume token)');
    console.log('   ✓ Tamper detection (blockchain chain)');

    // ========================================================================
    // Summary
    // ========================================================================
    printSection('SUMMARY & RECOMMENDATION');

    console.log('📌 Current State:');
    console.log('   ✅ Blockchain chain works (prevHash → chainHash)');
    console.log('   ✅ Resume token stored in sync_resume collection');
    console.log('   ❌ Resume token NOT in sync_ops themselves');
    console.log('   ❌ Cluster time NOT captured');

    console.log('\n💡 Recommendation: ADD change stream metadata\n');
    console.log('Benefits:');
    console.log('   1. Correlation: Link sync_ops to MongoDB events');
    console.log('   2. Verification: Validate our order matches MongoDB');
    console.log('   3. Debugging: Trace back to exact change event');
    console.log('   4. Completeness: Full context for each operation');

    console.log('\n🔧 Implementation:');
    console.log('   1. Add columns to SYNC_OPS_TABLE_CFG');
    console.log('      - changeStreamId (json)');
    console.log('      - clusterTime (json)');
    console.log('      - wallTime (string)');

    console.log('\n   2. Capture in appendOp:');
    console.log('      const doc: SyncOpDoc = {');
    console.log('        ...existing fields...,');
    console.log('        changeStreamId: change._id,      // Resume token');
    console.log('        clusterTime: change.clusterTime, // Cluster time');
    console.log('        wallTime: change.wallTime        // Wall time');
    console.log('      };');

    console.log('\n   3. Pass change event to appendOp:');
    console.log('      await appendOp(db, bs, nodeId, op, change);');

    console.log('\n✅ This gives you BOTH:');
    console.log('   • Your blockchain chain (content integrity)');
    console.log("   • MongoDB's resume token (resumability & correlation)");
    console.log('   • Best of both worlds!');

    console.log('\n');
  } catch (error) {
    console.error('\n❌ Error:', error);
    process.exit(1);
  } finally {
    await client.close();
  }
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
