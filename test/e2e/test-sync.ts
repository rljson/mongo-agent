#!/usr/bin/env tsx

import { MongoClient } from 'mongodb';


const MONGO_A_URI =
  process.env.MONGO_A_URI || 'mongodb://localhost:27017/syncdb?replicaSet=rsA';
const MONGO_B_URI =
  process.env.MONGO_B_URI || 'mongodb://localhost:27018/syncdb?replicaSet=rsB';
const WAIT_TIME_MS = 4000; // Wait 4 seconds for sync to propagate

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
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
function waiting(msg: string): void {
  log(colors.yellow, '⏳', msg);
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function testSync(): Promise<boolean> {
  let clientA: MongoClient | undefined;
  let clientB: MongoClient | undefined;
  const testId = `test_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

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

    // Test 1: A -> B sync
    info('\n=== Test 1: Sync from A to B ===');
    const docFromA = {
      testId,
      testCase: 'A->B',
      title: `Test A->B ${testId}`,
      content: 'Testing sync from agent A to agent B',
      createdBy: 'test-nodeA',
      timestamp: new Date(),
      syncTestValue: Math.random(),
    };

    const resultA = await collA.insertOne(docFromA);
    success(`Inserted document on MongoDB A with ID: ${resultA.insertedId}`);

    waiting(`Waiting ${WAIT_TIME_MS}ms for sync to propagate...`);
    await sleep(WAIT_TIME_MS);

    const foundOnB = await collB.findOne({ testId, testCase: 'A->B' });
    if (foundOnB) {
      success('Document successfully synced from A to B!');
      info(`  - Title: ${foundOnB.title}`);
      info(`  - Content: ${foundOnB.content}`);
    } else {
      error('Document NOT found on MongoDB B after sync!');
      throw new Error('A->B sync failed');
    }

    // Test 2: B -> A sync
    info('\n=== Test 2: Sync from B to A ===');
    const docFromB = {
      testId,
      testCase: 'B->A',
      title: `Test B->A ${testId}`,
      content: 'Testing sync from agent B to agent A',
      createdBy: 'test-nodeB',
      timestamp: new Date(),
      syncTestValue: Math.random(),
    };

    const resultB = await collB.insertOne(docFromB);
    success(`Inserted document on MongoDB B with ID: ${resultB.insertedId}`);

    waiting(`Waiting ${WAIT_TIME_MS}ms for sync to propagate...`);
    await sleep(WAIT_TIME_MS);

    const foundOnA = await collA.findOne({ testId, testCase: 'B->A' });
    if (foundOnA) {
      success('Document successfully synced from B to A!');
      info(`  - Title: ${foundOnA.title}`);
      info(`  - Content: ${foundOnA.content}`);
    } else {
      error('Document NOT found on MongoDB A after sync!');
      throw new Error('B->A sync failed');
    }

    // Test 3: Update sync A -> B
    info('\n=== Test 3: Update sync from A to B ===');
    const updateResult = await collA.updateOne(
      { _id: resultA.insertedId },
      { $set: { content: 'UPDATED from A', updatedAt: new Date() } },
    );

    if (updateResult.modifiedCount === 1) {
      success('Updated document on MongoDB A');

      waiting(`Waiting ${WAIT_TIME_MS}ms for sync to propagate...`);
      await sleep(WAIT_TIME_MS);

      const updatedOnB = await collB.findOne({ _id: resultA.insertedId });
      if (updatedOnB && updatedOnB.content === 'UPDATED from A') {
        success('Update successfully synced from A to B!');
        info(`  - New content: ${updatedOnB.content}`);
      } else {
        error('Update NOT synced from A to B!');
        throw new Error('Update sync A->B failed');
      }
    }

    // Cleanup
    info('\n=== Cleanup ===');
    const deleteA = await collA.deleteMany({ testId });
    const deleteB = await collB.deleteMany({ testId });
    success(`Cleaned up ${deleteA.deletedCount} documents from MongoDB A`);
    success(`Cleaned up ${deleteB.deletedCount} documents from MongoDB B`);

    // Final result
    console.log('\n' + '='.repeat(50));
    success('ALL TESTS PASSED! ✨');
    console.log('='.repeat(50) + '\n');

    return true;
  } catch (err) {
    const error = err as Error;
    console.error('\n' + '='.repeat(50));
    log(colors.red, '✗', `TEST FAILED: ${error.message}`);
    console.error('='.repeat(50) + '\n');
    console.error(error);
    return false;
  } finally {
    if (clientA) await clientA.close();
    if (clientB) await clientB.close();
    info('Closed MongoDB connections');
  }
}

// Run tests
testSync()
  .then((success) => process.exit(success ? 0 : 1))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
