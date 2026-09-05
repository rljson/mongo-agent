#!/usr/bin/env tsx
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * End-to-End Test: Record Locking Mechanism
 *
 * This test verifies that the locking mechanism prevents concurrent edits:
 * 1. Node A locks a user record
 * 2. Node B attempts to update the same user (should fail)
 * 3. Node A releases the lock
 * 4. Node B can now update the user (should succeed)
 *
 * This simulates the real-world scenario where multiple nodes/clients
 * attempt to edit the same record simultaneously.
 */

import { Db as MongoDb, MongoClient } from 'mongodb';

import { createLockManager, EntityType, LockManager } from '../../../src/lock-manager.ts';


// Test configuration
const MONGO_URI =
  process.env.MONGO_URI || 'mongodb://localhost:27017/?directConnection=true';
const TEST_DB = 'test_locking';

// Node identities
const NODE_A = {
  key: 'nodeA',
  name: 'Node A - Primary',
  compName: 'SERVER-A-001',
};

const NODE_B = {
  key: 'nodeB',
  name: 'Node B - Secondary',
  compName: 'SERVER-B-002',
};

// ANSI color codes for output
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  green: '\x1b[32m',
  blue: '\x1b[34m',
  yellow: '\x1b[33m',
  cyan: '\x1b[36m',
  magenta: '\x1b[35m',
  red: '\x1b[31m',
};

function log(message: string, color = colors.reset) {
  console.log(`${color}${message}${colors.reset}`);
}

function header(message: string) {
  log(`\n${'='.repeat(70)}`, colors.bright);
  log(message, colors.bright + colors.cyan);
  log('='.repeat(70), colors.bright);
}

function section(message: string) {
  log(`\n${message}`, colors.bright + colors.yellow);
  log('-'.repeat(message.length), colors.yellow);
}

function success(message: string) {
  log(`✓ ${message}`, colors.green);
}

function info(message: string) {
  log(`  ${message}`, colors.blue);
}

function error(message: string) {
  log(`✗ ${message}`, colors.red);
}

function highlight(message: string) {
  log(message, colors.magenta);
}

/**
 * Setup test database and collections
 */
async function setupTestData(db: MongoDb): Promise<void> {
  section('Setting up test data');

  // Drop existing collections
  try {
    await db.collection('users').drop();
    await db.collection('locking').drop();
  } catch (err) {
    // Collections might not exist
  }

  // Insert test users
  await db.collection('users').insertMany([
    {
      _id: 'user-123',
      name: 'Alice Johnson',
      email: 'alice@example.com',
      role: 'admin',
      updatedBy: 'system',
    },
    {
      _id: 'user-456',
      name: 'Bob Smith',
      email: 'bob@example.com',
      role: 'user',
      updatedBy: 'system',
    },
    {
      _id: 'user-789',
      name: 'Charlie Brown',
      email: 'charlie@example.com',
      role: 'user',
      updatedBy: 'system',
    },
  ]);

  success('Inserted 3 test users');
}

/**
 * Test: Basic lock acquisition and release
 */
async function testBasicLocking(lockManager: LockManager): Promise<boolean> {
  section('Test 1: Basic Lock Acquisition and Release');

  const userId = 'user-123';
  const userTyp = EntityType.USERS;

  // Node A acquires lock
  info('Node A attempting to acquire lock...');
  const acquired = await lockManager.acquireLock({
    typ: userTyp,
    value: userId,
    key: NODE_A.key,
    name: NODE_A.name,
    compName: NODE_A.compName,
    eMail: 'nodea@example.com',
  });

  if (!acquired) {
    error('Failed to acquire lock');
    return false;
  }
  success('Node A successfully acquired lock');

  // Verify lock exists
  const lock = await lockManager.isLocked(userTyp, userId);
  if (!lock) {
    error('Lock not found in database');
    return false;
  }
  success(`Lock verified in database: ${lock._id}`);
  info(`  Locked by: ${lock.name} (${lock.key})`);
  info(`  Computer: ${lock.compName}`);

  // Node A releases lock
  info('Node A releasing lock...');
  const released = await lockManager.releaseLock(userTyp, userId, NODE_A.key);
  if (!released) {
    error('Failed to release lock');
    return false;
  }
  success('Node A successfully released lock');

  // Verify lock is gone
  const lockAfterRelease = await lockManager.isLocked(userTyp, userId);
  if (lockAfterRelease) {
    error('Lock still exists after release');
    return false;
  }
  success('Lock correctly removed from database');

  return true;
}

/**
 * Test: Lock prevents concurrent modifications
 */
async function testConcurrentLockPrevention(
  db: MongoDb,
  lockManager: LockManager,
): Promise<boolean> {
  section('Test 2: Lock Prevents Concurrent Modifications');

  const userId = 'user-456';
  const userTyp = EntityType.USERS;

  // Node A acquires lock
  info('Node A acquiring lock on user-456...');
  const acquiredA = await lockManager.acquireLock({
    typ: userTyp,
    value: userId,
    key: NODE_A.key,
    name: NODE_A.name,
    compName: NODE_A.compName,
  });

  if (!acquiredA) {
    error('Node A failed to acquire lock');
    return false;
  }
  success('Node A successfully locked user-456');

  // Node B attempts to acquire the same lock
  info('Node B attempting to acquire the same lock...');
  const acquiredB = await lockManager.acquireLock({
    typ: userTyp,
    value: userId,
    key: NODE_B.key,
    name: NODE_B.name,
    compName: NODE_B.compName,
  });

  if (acquiredB) {
    error(
      'Node B should NOT have acquired the lock (already locked by Node A)',
    );
    return false;
  }
  success('Node B correctly denied lock acquisition (locked by Node A)');

  // Node B checks if it can modify
  const canModify = await lockManager.canModify(userTyp, userId, NODE_B.key);
  if (canModify) {
    error('Node B should NOT be allowed to modify');
    return false;
  }
  success('Node B correctly prevented from modifying');

  // Node B attempts to update the user (should be blocked)
  info('Node B attempting to update user (should be prevented)...');
  try {
    const lockCheck = await lockManager.isLocked(userTyp, userId);
    if (lockCheck && lockCheck.key !== NODE_B.key) {
      info('  Update blocked: Record is locked by another node');
      success('Lock mechanism correctly prevented update');
    } else {
      error('Lock check failed');
      return false;
    }
  } catch (err) {
    error(`Unexpected error: ${err}`);
    return false;
  }

  // Node A can still modify (lock owner)
  const canModifyA = await lockManager.canModify(userTyp, userId, NODE_A.key);
  if (!canModifyA) {
    error('Node A (lock owner) should be allowed to modify');
    return false;
  }
  success('Node A (lock owner) can modify the record');

  // Node A updates the user
  info('Node A updating user...');
  const updateResult = await db
    .collection('users')
    .updateOne(
      { _id: userId },
      { $set: { name: 'Bob Smith Jr.', updatedBy: NODE_A.key } },
    );
  if (updateResult.modifiedCount !== 1) {
    error('Failed to update user');
    return false;
  }
  success('Node A successfully updated user');

  // Verify update
  const updatedUser = await db.collection('users').findOne({ _id: userId });
  if (!updatedUser || updatedUser.name !== 'Bob Smith Jr.') {
    error('User update not persisted correctly');
    return false;
  }
  success(`User updated: ${updatedUser.name}`);

  // Node A releases lock
  info('Node A releasing lock...');
  await lockManager.releaseLock(userTyp, userId, NODE_A.key);
  success('Node A released lock');

  // Now Node B can acquire the lock
  info('Node B attempting to acquire lock again...');
  const acquiredB2 = await lockManager.acquireLock({
    typ: userTyp,
    value: userId,
    key: NODE_B.key,
    name: NODE_B.name,
    compName: NODE_B.compName,
  });

  if (!acquiredB2) {
    error('Node B should be able to acquire lock now');
    return false;
  }
  success('Node B successfully acquired lock');

  // Node B updates the user
  info('Node B updating user...');
  const updateResultB = await db
    .collection('users')
    .updateOne(
      { _id: userId },
      { $set: { email: 'bob.smith.jr@example.com', updatedBy: NODE_B.key } },
    );
  if (updateResultB.modifiedCount !== 1) {
    error('Failed to update user');
    return false;
  }
  success('Node B successfully updated user');

  // Verify update
  const finalUser = await db.collection('users').findOne({ _id: userId });
  if (!finalUser || finalUser.email !== 'bob.smith.jr@example.com') {
    error('User update not persisted correctly');
    return false;
  }
  success(`User updated: ${finalUser.email} (by ${finalUser.updatedBy})`);

  // Cleanup
  await lockManager.releaseLock(userTyp, userId, NODE_B.key);

  return true;
}

/**
 * Test: Re-entrant locks (same node acquiring lock twice)
 */
async function testReentrantLock(lockManager: LockManager): Promise<boolean> {
  section('Test 3: Re-entrant Lock (Same Node Acquiring Twice)');

  const userId = 'user-789';
  const userTyp = EntityType.USERS;

  // Node A acquires lock first time
  info('Node A acquiring lock (first time)...');
  const acquired1 = await lockManager.acquireLock({
    typ: userTyp,
    value: userId,
    key: NODE_A.key,
    name: NODE_A.name,
    compName: NODE_A.compName,
  });

  if (!acquired1) {
    error('Failed to acquire lock');
    return false;
  }
  success('Node A acquired lock (first time)');

  // Node A acquires lock second time (re-entrant)
  info('Node A acquiring lock again (re-entrant)...');
  const acquired2 = await lockManager.acquireLock({
    typ: userTyp,
    value: userId,
    key: NODE_A.key,
    name: NODE_A.name,
    compName: NODE_A.compName,
  });

  if (!acquired2) {
    error('Re-entrant lock should succeed for same node');
    return false;
  }
  success('Node A successfully re-acquired lock (re-entrant)');

  // Verify only one lock exists
  const locks = await lockManager.getLocksBy(NODE_A.key);
  const userLocks = locks.filter((l) => l.value === userId);
  if (userLocks.length !== 1) {
    error(`Expected 1 lock, found ${userLocks.length}`);
    return false;
  }
  success('Only one lock record exists (re-entrant behavior correct)');

  // Cleanup
  await lockManager.releaseLock(userTyp, userId, NODE_A.key);

  return true;
}

/**
 * Test: Lock cleanup
 */
async function testLockCleanup(lockManager: LockManager): Promise<boolean> {
  section('Test 4: Lock Cleanup');

  const userTyp = EntityType.USERS;

  // Node A acquires multiple locks
  info('Node A acquiring locks on multiple users...');
  await lockManager.acquireLock({
    typ: userTyp,
    value: 'user-123',
    key: NODE_A.key,
    name: NODE_A.name,
    compName: NODE_A.compName,
  });
  await lockManager.acquireLock({
    typ: userTyp,
    value: 'user-456',
    key: NODE_A.key,
    name: NODE_A.name,
    compName: NODE_A.compName,
  });
  success('Node A acquired 2 locks');

  // Verify locks
  const locks = await lockManager.getLocksBy(NODE_A.key);
  if (locks.length !== 2) {
    error(`Expected 2 locks, found ${locks.length}`);
    return false;
  }
  success(`Verified ${locks.length} locks by Node A`);

  // Release all locks for Node A
  info('Releasing all locks for Node A...');
  const released = await lockManager.releaseAllLocks(NODE_A.key);
  if (released !== 2) {
    error(`Expected to release 2 locks, released ${released}`);
    return false;
  }
  success(`Successfully released ${released} locks`);

  // Verify all locks are gone
  const remainingLocks = await lockManager.getLocksBy(NODE_A.key);
  if (remainingLocks.length !== 0) {
    error(`Expected 0 locks, found ${remainingLocks.length}`);
    return false;
  }
  success('All locks successfully removed');

  return true;
}

/**
 * Main test runner
 */
async function runTests() {
  header('RECORD LOCKING E2E TEST');

  let client: MongoClient | null = null;
  let testsPassed = 0;
  let testsFailed = 0;

  try {
    // Connect to MongoDB
    section('Connecting to MongoDB');
    client = new MongoClient(MONGO_URI);
    await client.connect();
    success(`Connected to MongoDB: ${MONGO_URI}`);

    const db = client.db(TEST_DB);
    const lockManager = createLockManager(db);

    // Initialize lock manager
    info('Initializing lock manager...');
    await lockManager.initialize();
    success('Lock manager initialized');

    // Setup test data
    await setupTestData(db);

    // Run tests
    const tests = [
      { name: 'Basic Locking', fn: () => testBasicLocking(lockManager) },
      {
        name: 'Concurrent Lock Prevention',
        fn: () => testConcurrentLockPrevention(db, lockManager),
      },
      { name: 'Re-entrant Lock', fn: () => testReentrantLock(lockManager) },
      { name: 'Lock Cleanup', fn: () => testLockCleanup(lockManager) },
    ];

    for (const test of tests) {
      try {
        const passed = await test.fn();
        if (passed) {
          testsPassed++;
          highlight(`\n✓ ${test.name} PASSED`);
        } else {
          testsFailed++;
          highlight(`\n✗ ${test.name} FAILED`);
        }
      } catch (err) {
        testsFailed++;
        error(`\n✗ ${test.name} FAILED WITH ERROR:`);
        console.error(err);
      }
    }

    // Summary
    header('TEST SUMMARY');
    log(`Total Tests: ${tests.length}`, colors.bright);
    log(`Passed: ${testsPassed}`, colors.green);
    log(`Failed: ${testsFailed}`, testsFailed > 0 ? colors.red : colors.green);

    if (testsFailed === 0) {
      success('\n🎉 All tests passed!');
    } else {
      error(`\n${testsFailed} test(s) failed`);
      process.exit(1);
    }
  } catch (err) {
    error('Test execution failed:');
    console.error(err);
    process.exit(1);
  } finally {
    if (client) {
      await client.close();
      info('MongoDB connection closed');
    }
  }
}

// Run tests
runTests().catch(console.error);
