#!/usr/bin/env node
'use strict';

/**
 * Integration Test: Change Stream Monitoring & Sync
 * Tests that agents detect changes and sync bidirectionally
 */

const { MongoClient } = require('mongodb');

const MONGO_A_URI =
  process.env.MONGO_A_URI ||
  'mongodb://localhost:27017/syncdb?directConnection=true';
const MONGO_B_URI =
  process.env.MONGO_B_URI ||
  'mongodb://localhost:27018/syncdb?directConnection=true';
const WAIT_TIME_MS = 3000; // Wait for sync to propagate

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
};

function log(color, symbol, message) {
  console.log(`${color}${symbol}${colors.reset} ${message}`);
}

function success(msg) {
  log(colors.green, '✓', msg);
}
function error(msg) {
  log(colors.red, '✗', msg);
}
function info(msg) {
  log(colors.blue, 'ℹ', msg);
}
function waiting(msg) {
  log(colors.yellow, '⏳', msg);
}
function header(msg) {
  console.log(`\n${colors.cyan}${'═'.repeat(70)}${colors.reset}`);
  console.log(`  ${msg}`);
  console.log(`${colors.cyan}${'═'.repeat(70)}${colors.reset}\n`);
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

let testsRun = 0;
let testsPassed = 0;
let testsFailed = 0;

function assert(condition, message) {
  testsRun++;
  if (condition) {
    success(message);
    testsPassed++;
  } else {
    error(message);
    testsFailed++;
  }
}

async function testChangeStreamMonitoring() {
  header('Change Stream Monitoring Test');

  let clientA, clientB;
  const testId = `changestream_${Date.now()}`;

  try {
    info('Connecting to MongoDB instances...');
    clientA = new MongoClient(MONGO_A_URI);
    clientB = new MongoClient(MONGO_B_URI);

    await clientA.connect();
    await clientB.connect();
    success('Connected to both MongoDB instances');

    const dbA = clientA.db('syncdb');
    const dbB = clientB.db('syncdb');
    const collA = dbA.collection('articles');
    const collB = dbB.collection('articles');

    // Test 1: Insert in A, should appear in sync_ops
    info('Test 1: Insert document in MongoDB A...');
    const docA = {
      testId,
      title: 'Change Stream Test Article',
      content: 'Testing change stream monitoring',
      created: new Date(),
    };

    const resultA = await collA.insertOne(docA);
    success(`Inserted document with _id: ${resultA.insertedId}`);

    waiting(`Waiting ${WAIT_TIME_MS}ms for change stream to capture...`);
    await sleep(WAIT_TIME_MS);

    // Check if operation was recorded in sync_ops
    const syncOpsA = await dbA.collection('sync_ops').findOne({
      origin: 'nodeA',
      operationType: 'insert',
      docId: resultA.insertedId,
    });

    assert(
      syncOpsA !== null,
      'Change stream captured insert operation in sync_ops',
    );
    if (syncOpsA) {
      assert(
        syncOpsA.ns.coll === 'articles',
        'Operation has correct collection name',
      );
      assert(syncOpsA.operationType === 'insert', 'Operation type is insert');
      info(`  Operation: ${syncOpsA._id}, seq: ${syncOpsA.seq}`);
    }

    // Test 2: Update in A, should be captured
    info('Test 2: Update document in MongoDB A...');
    await collA.updateOne(
      { _id: resultA.insertedId },
      { $set: { updated: true, updateTime: new Date() } },
    );
    success('Updated document');

    waiting(`Waiting ${WAIT_TIME_MS}ms for change stream to capture update...`);
    await sleep(WAIT_TIME_MS);

    const updateOp = await dbA.collection('sync_ops').findOne({
      origin: 'nodeA',
      operationType: 'update',
      docId: resultA.insertedId,
    });

    assert(updateOp !== null, 'Change stream captured update operation');

    // Test 3: Delete in A, should be captured
    info('Test 3: Delete document in MongoDB A...');
    await collA.deleteOne({ _id: resultA.insertedId });
    success('Deleted document');

    waiting(`Waiting ${WAIT_TIME_MS}ms for change stream to capture delete...`);
    await sleep(WAIT_TIME_MS);

    const deleteOp = await dbA.collection('sync_ops').findOne({
      origin: 'nodeA',
      operationType: 'delete',
      docId: resultA.insertedId,
    });

    assert(deleteOp !== null, 'Change stream captured delete operation');

    // Test 4: Check sync_local was updated
    info('Test 4: Checking sync_local state...');
    const localState = await dbA
      .collection('sync_local')
      .findOne({ _id: 'local' });

    assert(localState !== null, 'sync_local document exists');
    if (localState) {
      assert(
        localState.seq > 0,
        `sync_local has seq > 0 (seq: ${localState.seq})`,
      );
      assert(
        localState.headHash && localState.headHash !== 'GENESIS',
        `sync_local has valid headHash: ${localState.headHash.slice(0, 8)}...`,
      );
    }
  } catch (e) {
    error(`Test failed with error: ${e.message}`);
    testsFailed++;
    testsRun++;
  } finally {
    if (clientA) await clientA.close();
    if (clientB) await clientB.close();
  }
}

async function testBidirectionalSync() {
  header('Bidirectional Sync Test');

  let clientA, clientB;
  const testId = `bidir_${Date.now()}`;

  try {
    info('Connecting to MongoDB instances...');
    clientA = new MongoClient(MONGO_A_URI);
    clientB = new MongoClient(MONGO_B_URI);

    await clientA.connect();
    await clientB.connect();
    success('Connected to both MongoDB instances');

    const dbA = clientA.db('syncdb');
    const dbB = clientB.db('syncdb');
    const collA = dbA.collection('articles');
    const collB = dbB.collection('articles');

    // Test 1: A → B sync
    info('Test 1: Insert in A, verify sync to B...');
    const docFromA = {
      testId,
      source: 'nodeA',
      title: 'Document from Node A',
      created: new Date(),
    };

    const resultA = await collA.insertOne(docFromA);
    success(`Inserted in MongoDB A: ${resultA.insertedId}`);

    waiting(`Waiting ${WAIT_TIME_MS}ms for sync A→B...`);
    await sleep(WAIT_TIME_MS);

    const foundInB = await collB.findOne({ _id: resultA.insertedId });
    assert(foundInB !== null, 'Document synced from A to B');
    if (foundInB) {
      assert(
        foundInB.source === 'nodeA',
        'Synced document has correct source field',
      );
      assert(
        foundInB.title === docFromA.title,
        'Synced document has correct title',
      );
    }

    // Test 2: B → A sync
    info('Test 2: Insert in B, verify sync to A...');
    const docFromB = {
      testId,
      source: 'nodeB',
      title: 'Document from Node B',
      created: new Date(),
    };

    const resultB = await collB.insertOne(docFromB);
    success(`Inserted in MongoDB B: ${resultB.insertedId}`);

    waiting(`Waiting ${WAIT_TIME_MS}ms for sync B→A...`);
    await sleep(WAIT_TIME_MS);

    const foundInA = await collA.findOne({ _id: resultB.insertedId });
    assert(foundInA !== null, 'Document synced from B to A');
    if (foundInA) {
      assert(
        foundInA.source === 'nodeB',
        'Synced document has correct source field',
      );
      assert(
        foundInA.title === docFromB.title,
        'Synced document has correct title',
      );
    }

    // Test 3: Verify both nodes have both documents
    info('Test 3: Verify both nodes have all documents...');
    const countA = await collA.countDocuments({ testId });
    const countB = await collB.countDocuments({ testId });

    assert(countA === 2, `MongoDB A has 2 test documents (has: ${countA})`);
    assert(countB === 2, `MongoDB B has 2 test documents (has: ${countB})`);

    // Test 4: Update in A, should sync to B
    info('Test 4: Update in A, verify sync to B...');
    await collA.updateOne(
      { _id: resultA.insertedId },
      { $set: { updated: true, updateCount: 1 } },
    );
    success('Updated document in A');

    waiting(`Waiting ${WAIT_TIME_MS}ms for update sync A→B...`);
    await sleep(WAIT_TIME_MS);

    const updatedInB = await collB.findOne({ _id: resultA.insertedId });
    assert(
      updatedInB !== null && updatedInB.updated === true,
      'Update synced from A to B',
    );

    // Cleanup
    info('Cleaning up test documents...');
    await collA.deleteMany({ testId });
    await collB.deleteMany({ testId });
  } catch (e) {
    error(`Test failed with error: ${e.message}`);
    testsFailed++;
    testsRun++;
  } finally {
    if (clientA) await clientA.close();
    if (clientB) await clientB.close();
  }
}

async function testHashChainIntegrity() {
  header('Hash Chain Integrity Test');

  let clientA;

  try {
    info('Connecting to MongoDB A...');
    clientA = new MongoClient(MONGO_A_URI);
    await clientA.connect();
    success('Connected to MongoDB A');

    const db = clientA.db('syncdb');
    const syncOps = db.collection('sync_ops');

    // Get last 5 operations from nodeA
    info('Fetching recent operations from nodeA...');
    const ops = await syncOps
      .find({ origin: 'nodeA' })
      .sort({ seq: -1 })
      .limit(5)
      .toArray();

    if (ops.length === 0) {
      info('No operations found yet (database might be empty)');
      return;
    }

    success(`Found ${ops.length} recent operations`);

    // Verify hash chain
    info('Verifying hash chain integrity...');
    const reversed = ops.reverse(); // Sort by seq ascending

    for (let i = 1; i < reversed.length; i++) {
      const prev = reversed[i - 1];
      const curr = reversed[i];

      // Check seq is sequential
      const seqOk = curr.seq === prev.seq + 1;
      assert(seqOk, `Seq is sequential: ${prev.seq} → ${curr.seq}`);

      // Check prevHash matches
      const hashOk = curr.prevHash === prev.chainHash;
      assert(
        hashOk,
        `Hash chain valid: op ${curr.seq} prevHash matches op ${prev.seq} chainHash`,
      );

      if (!hashOk) {
        info(`  Expected: ${prev.chainHash}`);
        info(`  Got:      ${curr.prevHash}`);
      }
    }
  } catch (e) {
    error(`Test failed with error: ${e.message}`);
    testsFailed++;
    testsRun++;
  } finally {
    if (clientA) await clientA.close();
  }
}

async function main() {
  console.log('\n' + '═'.repeat(70));
  console.log('  Change Stream & Sync Integration Tests');
  console.log('═'.repeat(70));
  console.log();

  info(`Testing against:`);
  info(`  MongoDB A: ${MONGO_A_URI}`);
  info(`  MongoDB B: ${MONGO_B_URI}`);
  info(`  Sync wait time: ${WAIT_TIME_MS}ms`);

  try {
    await testChangeStreamMonitoring();
    await testBidirectionalSync();
    await testHashChainIntegrity();

    console.log();
    console.log('═'.repeat(70));
    console.log(`  Test Results: ${testsPassed}/${testsRun} passed`);
    if (testsFailed > 0) {
      console.log(`  ${colors.red}${testsFailed} tests failed${colors.reset}`);
    } else {
      console.log(`  ${colors.green}All tests passed!${colors.reset}`);
    }
    console.log('═'.repeat(70));

    process.exit(testsFailed > 0 ? 1 : 0);
  } catch (e) {
    error(`Fatal error: ${e.message}`);
    console.error(e.stack);
    process.exit(1);
  }
}

main();
