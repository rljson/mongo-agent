#!/usr/bin/env tsx
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * End-to-End Test: Offline Conflict Detection
 *
 * This test verifies the offline conflict detection mechanism:
 * 1. Node A locks a user record and makes changes
 * 2. Node B goes offline
 * 3. Node B makes offline changes to the same user
 * 4. Node A releases the lock
 * 5. Node B comes back online
 * 6. System detects conflict and creates conflict record
 *
 * This simulates real-world scenarios where nodes lose connectivity
 * and make changes that conflict with locked records.
 */

import { Db as MongoDb, MongoClient } from 'mongodb';

import { createLockManager, EntityType, LockManager } from '../../../src/lock-manager.ts';


// Test configuration
const MONGO_URI =
  process.env.MONGO_URI || 'mongodb://localhost:27017/?directConnection=true';
const TEST_DB = 'test_offline_conflicts';

// Node identities
const NODE_A = {
  key: 'nodeA',
  name: 'Node A - Online',
  compName: 'SERVER-A-001',
};

const NODE_B = {
  key: 'nodeB',
  name: 'Node B - Goes Offline',
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
    await db.collection('lock_history').drop();
    await db.collection('offline_changes').drop();
    await db.collection('sync_conflicts').drop();
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
  ]);

  success('Inserted 2 test users');
}

/**
 * Test: Offline conflict detection
 */
async function testOfflineConflictDetection(
  db: MongoDb,
  lockManager: LockManager,
): Promise<boolean> {
  section('Test 1: Offline Conflict Detection');

  const userId = 'user-123';
  const userTyp = EntityType.USERS;

  // Step 1: Node A acquires lock
  info('Node A acquiring lock on user-123...');
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
  success('Node A successfully locked user-123');

  // Step 2: Node A makes changes (while online)
  info('Node A making changes to user-123...');
  await db.collection('users').updateOne(
    { _id: userId },
    {
      $set: {
        name: 'Alice Smith-Johnson',
        email: 'alice.smith@example.com',
        updatedBy: NODE_A.key,
      },
    },
  );
  success('Node A updated user-123');

  // Step 3: Simulate Node B going offline and making changes
  info('Node B goes offline and makes changes...');
  const offlineChangeData = {
    _id: userId,
    name: 'Alice Thompson',
    email: 'alice.thompson@example.com',
    role: 'senior-admin',
    updatedBy: NODE_B.key,
  };

  // Wait a bit to ensure timestamp difference
  await new Promise((resolve) => setTimeout(resolve, 100));

  // Record offline change
  await lockManager.recordOfflineChange(
    userTyp,
    userId,
    NODE_B.key,
    offlineChangeData,
    'users',
    TEST_DB,
  );
  success('Node B recorded offline change');

  // Step 4: Node A releases lock
  info('Node A releasing lock...');
  const released = await lockManager.releaseLock(userTyp, userId, NODE_A.key);
  if (!released) {
    error('Node A failed to release lock');
    return false;
  }
  success('Node A released lock (saved to lock history)');

  // Verify lock history was created
  const lockHistory = await db.collection('lock_history').findOne({
    typ: userTyp,
    value: userId,
    key: NODE_A.key,
  });

  if (!lockHistory) {
    error('Lock history not created');
    return false;
  }
  success(`Lock history created: ${lockHistory._id}`);
  info(`  Acquired: ${new Date(lockHistory.acquiredAt).toISOString()}`);
  info(`  Released: ${new Date(lockHistory.releasedAt).toISOString()}`);

  // Step 5: Node B comes back online and detects conflicts
  info('Node B comes back online...');
  const conflicts = await lockManager.detectOfflineConflicts(NODE_B.key);

  if (conflicts.length === 0) {
    error('No conflicts detected - expected at least 1');
    return false;
  }
  success(`Detected ${conflicts.length} conflict(s)`);

  const conflict = conflicts[0];
  info(`  Conflict on: ${conflict.change.collection}.${conflict.change.value}`);
  info(`  Offline change by: ${conflict.change.key}`);
  info(`  Was locked by: ${conflict.lock.key} (${conflict.lock.name})`);
  info(`  Lock period: ${new Date(conflict.lock.acquiredAt).toISOString()} - ${new Date(conflict.lock.releasedAt).toISOString()}`);
  info(`  Change made at: ${new Date(conflict.change.changeTimestamp).toISOString()}`);

  // Step 6: Create conflict records
  info('Creating conflict records in sync_conflicts collection...');
  const conflictCount = await lockManager.createConflictRecords(conflicts);

  if (conflictCount === 0) {
    error('Failed to create conflict records');
    return false;
  }
  success(`Created ${conflictCount} conflict record(s)`);

  // Verify conflict record
  const syncConflict = await db.collection('sync_conflicts').findOne({
    conflictType: 'offline-lock-conflict',
  });

  if (!syncConflict) {
    error('Conflict record not found in sync_conflicts');
    return false;
  }
  success('Conflict record verified in sync_conflicts collection');
  info(`  Conflict ID: ${syncConflict.conflictId}`);
  info(`  Status: ${syncConflict.status}`);
  info(`  Type: ${syncConflict.conflictType}`);

  // Step 7: Clear offline changes
  info('Clearing offline changes for Node B...');
  const cleared = await lockManager.clearOfflineChanges(NODE_B.key);
  success(`Cleared ${cleared} offline change(s)`);

  return true;
}

/**
 * Test: No conflict when changes don't overlap with locks
 */
async function testNoConflictWhenNoOverlap(
  db: MongoDb,
  lockManager: LockManager,
): Promise<boolean> {
  section('Test 2: No Conflict When Changes Don\'t Overlap');

  const userId = 'user-456';
  const userTyp = EntityType.USERS;

  // Step 1: Node A acquires and releases lock
  info('Node A acquiring and releasing lock...');
  await lockManager.acquireLock({
    typ: userTyp,
    value: userId,
    key: NODE_A.key,
    name: NODE_A.name,
    compName: NODE_A.compName,
  });

  await new Promise((resolve) => setTimeout(resolve, 100));

  await lockManager.releaseLock(userTyp, userId, NODE_A.key);
  success('Node A completed lock cycle');

  // Wait to ensure timestamps are different
  await new Promise((resolve) => setTimeout(resolve, 200));

  // Step 2: Node B makes offline change AFTER lock was released
  info('Node B making offline change after lock was released...');
  await lockManager.recordOfflineChange(
    userTyp,
    userId,
    NODE_B.key,
    { name: 'Bob Jones', updatedBy: NODE_B.key },
    'users',
    TEST_DB,
  );
  success('Node B recorded offline change');

  // Step 3: Check for conflicts
  info('Detecting conflicts...');
  const conflicts = await lockManager.detectOfflineConflicts(NODE_B.key);

  if (conflicts.length > 0) {
    error(`Found ${conflicts.length} conflict(s) - expected 0`);
    return false;
  }
  success('No conflicts detected (as expected)');

  // Cleanup
  await lockManager.clearOfflineChanges(NODE_B.key);

  return true;
}

/**
 * Test: Multiple offline conflicts
 */
async function testMultipleOfflineConflicts(
  db: MongoDb,
  lockManager: LockManager,
): Promise<boolean> {
  section('Test 3: Multiple Offline Conflicts');

  const userIds = ['user-123', 'user-456'];
  const userTyp = EntityType.USERS;

  // Step 1: Node A locks both users
  info('Node A locking multiple users...');
  for (const userId of userIds) {
    await lockManager.acquireLock({
      typ: userTyp,
      value: userId,
      key: NODE_A.key,
      name: NODE_A.name,
      compName: NODE_A.compName,
    });
  }
  success('Node A locked 2 users');

  // Step 2: Node B makes offline changes to both
  info('Node B making offline changes to both users...');
  await new Promise((resolve) => setTimeout(resolve, 100));

  for (const userId of userIds) {
    await lockManager.recordOfflineChange(
      userTyp,
      userId,
      NODE_B.key,
      { name: `Updated ${userId}`, updatedBy: NODE_B.key },
      'users',
      TEST_DB,
    );
  }
  success('Node B recorded 2 offline changes');

  // Step 3: Node A releases locks
  info('Node A releasing locks...');
  for (const userId of userIds) {
    await lockManager.releaseLock(userTyp, userId, NODE_A.key);
  }
  success('Node A released all locks');

  // Step 4: Detect conflicts
  info('Detecting conflicts...');
  const conflicts = await lockManager.detectOfflineConflicts(NODE_B.key);

  if (conflicts.length !== 2) {
    error(`Expected 2 conflicts, found ${conflicts.length}`);
    return false;
  }
  success(`Detected ${conflicts.length} conflicts`);

  // Step 5: Create conflict records
  const conflictCount = await lockManager.createConflictRecords(conflicts);
  success(`Created ${conflictCount} conflict records`);

  // Cleanup
  await lockManager.clearOfflineChanges(NODE_B.key);

  return true;
}

/**
 * Test: Clean old lock history
 */
async function testCleanLockHistory(
  db: MongoDb,
  lockManager: LockManager,
): Promise<boolean> {
  section('Test 4: Clean Old Lock History');

  // Count current lock history
  const beforeCount = await db.collection('lock_history').countDocuments();
  info(`Lock history records before cleanup: ${beforeCount}`);

  // Clean history older than 1ms (should remove all existing ones from previous tests)
  const cleaned = await lockManager.cleanLockHistory(1);
  success(`Cleaned ${cleaned} old lock history record(s)`);

  const afterCount = await db.collection('lock_history').countDocuments();
  info(`Lock history records after cleanup: ${afterCount}`);

  if (afterCount >= beforeCount) {
    error('Lock history cleanup did not reduce records');
    return false;
  }

  success('Lock history cleanup working correctly');
  return true;
}

/**
 * Main test runner
 */
async function runTests() {
  header('Offline Conflict Detection E2E Tests');

  let mongoClient: MongoClient | null = null;

  try {
    // Connect to MongoDB
    info(`Connecting to MongoDB at ${MONGO_URI}...`);
    mongoClient = new MongoClient(MONGO_URI);
    await mongoClient.connect();
    success('Connected to MongoDB');

    const db = mongoClient.db(TEST_DB);

    // Initialize lock manager
    const lockManager = createLockManager(db);
    await lockManager.initialize();
    success('Lock manager initialized');

    // Setup test data
    await setupTestData(db);

    // Run tests
    const tests = [
      { name: 'Offline Conflict Detection', fn: testOfflineConflictDetection },
      { name: 'No Conflict When No Overlap', fn: testNoConflictWhenNoOverlap },
      { name: 'Multiple Offline Conflicts', fn: testMultipleOfflineConflicts },
      { name: 'Clean Lock History', fn: testCleanLockHistory },
    ];

    let passed = 0;
    let failed = 0;

    for (const test of tests) {
      try {
        const result = await test.fn(db, lockManager);
        if (result) {
          passed++;
        } else {
          failed++;
        }
      } catch (err) {
        error(`Test "${test.name}" threw an error: ${err}`);
        failed++;
      }
    }

    // Summary
    header('Test Summary');
    log(`Total Tests: ${tests.length}`, colors.bright);
    log(`Passed: ${passed}`, colors.green);
    log(`Failed: ${failed}`, failed > 0 ? colors.red : colors.reset);

    if (failed === 0) {
      success('\n🎉 All tests passed!');
      process.exit(0);
    } else {
      error(`\n❌ ${failed} test(s) failed`);
      process.exit(1);
    }
  } catch (err) {
    error(`Fatal error: ${err}`);
    process.exit(1);
  } finally {
    if (mongoClient) {
      await mongoClient.close();
      info('MongoDB connection closed');
    }
  }
}

// Run tests
runTests();
