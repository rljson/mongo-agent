// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * Unit tests for LockManager
 */

import { Db, MongoClient } from 'mongodb';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { createLockManager, EntityType, LockManager } from '../src/lock-manager.ts';


const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017';
const TEST_DB = 'test_lock_manager';

describe('LockManager', () => {
  let client: MongoClient;
  let db: Db;
  let lockManager: LockManager;

  beforeAll(async () => {
    client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(TEST_DB);
  });

  afterAll(async () => {
    await client.close();
  });

  beforeEach(async () => {
    // Drop locking collection before each test
    try {
      await db.collection('locking').drop();
    } catch (err) {
      // Collection might not exist
    }

    lockManager = createLockManager(db);
    await lockManager.initialize();
  });

  describe('acquireLock', () => {
    it('should acquire a lock successfully', async () => {
      const acquired = await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      expect(acquired).toBe(true);

      // Verify lock exists
      const lock = await lockManager.isLocked(EntityType.USERS, 'user-1');
      expect(lock).not.toBeNull();
      expect(lock?.key).toBe('node-a');
    });

    it('should prevent lock acquisition by another node', async () => {
      // Node A acquires lock
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      // Node B tries to acquire same lock
      const acquired = await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-b',
        name: 'Node B',
        compName: 'SERVER-B',
      });

      expect(acquired).toBe(false);
    });

    it('should allow re-entrant lock by same node', async () => {
      // First acquisition
      const acquired1 = await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      // Second acquisition by same node
      const acquired2 = await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      expect(acquired1).toBe(true);
      expect(acquired2).toBe(true);
    });
  });

  describe('releaseLock', () => {
    it('should release a lock successfully', async () => {
      // Acquire lock
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      // Release lock
      const released = await lockManager.releaseLock(
        EntityType.USERS,
        'user-1',
        'node-a',
      );
      expect(released).toBe(true);

      // Verify lock is gone
      const lock = await lockManager.isLocked(EntityType.USERS, 'user-1');
      expect(lock).toBeNull();
    });

    it('should not release lock owned by another node', async () => {
      // Node A acquires lock
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      // Node B tries to release
      const released = await lockManager.releaseLock(
        EntityType.USERS,
        'user-1',
        'node-b',
      );
      expect(released).toBe(false);

      // Lock should still exist
      const lock = await lockManager.isLocked(EntityType.USERS, 'user-1');
      expect(lock).not.toBeNull();
      expect(lock?.key).toBe('node-a');
    });
  });

  describe('isLocked', () => {
    it('should return null for unlocked record', async () => {
      const lock = await lockManager.isLocked(EntityType.USERS, 'user-1');
      expect(lock).toBeNull();
    });

    it('should return lock record for locked record', async () => {
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
        eMail: 'nodea@example.com',
      });

      const lock = await lockManager.isLocked(EntityType.USERS, 'user-1');
      expect(lock).not.toBeNull();
      expect(lock?.key).toBe('node-a');
      expect(lock?.name).toBe('Node A');
      expect(lock?.compName).toBe('SERVER-A');
      expect(lock?.eMail).toBe('nodea@example.com');
    });
  });

  describe('canModify', () => {
    it('should allow modification when no lock exists', async () => {
      const canModify = await lockManager.canModify(
        EntityType.USERS,
        'user-1',
        'node-a',
      );
      expect(canModify).toBe(true);
    });

    it('should allow modification for lock owner', async () => {
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      const canModify = await lockManager.canModify(
        EntityType.USERS,
        'user-1',
        'node-a',
      );
      expect(canModify).toBe(true);
    });

    it('should prevent modification for non-owner', async () => {
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      const canModify = await lockManager.canModify(
        EntityType.USERS,
        'user-1',
        'node-b',
      );
      expect(canModify).toBe(false);
    });
  });

  describe('getLocksBy', () => {
    it('should return all locks for a node', async () => {
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-2',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      await lockManager.acquireLock({
        typ: EntityType.PRODUCTS,
        value: 'product-1',
        key: 'node-b',
        name: 'Node B',
        compName: 'SERVER-B',
      });

      const locksA = await lockManager.getLocksBy('node-a');
      expect(locksA).toHaveLength(2);
      expect(locksA.every((l) => l.key === 'node-a')).toBe(true);

      const locksB = await lockManager.getLocksBy('node-b');
      expect(locksB).toHaveLength(1);
      expect(locksB[0].key).toBe('node-b');
    });
  });

  describe('releaseAllLocks', () => {
    it('should release all locks for a node', async () => {
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-2',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      const released = await lockManager.releaseAllLocks('node-a');
      expect(released).toBe(2);

      const locks = await lockManager.getLocksBy('node-a');
      expect(locks).toHaveLength(0);
    });
  });

  describe('isLockedBy', () => {
    it('should return true if locked by specified key', async () => {
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      const isLockedByA = await lockManager.isLockedBy(
        EntityType.USERS,
        'user-1',
        'node-a',
      );
      expect(isLockedByA).toBe(true);

      const isLockedByB = await lockManager.isLockedBy(
        EntityType.USERS,
        'user-1',
        'node-b',
      );
      expect(isLockedByB).toBe(false);
    });
  });

  describe('removeOldLocks', () => {
    it('should remove stale locks', async () => {
      // Create a lock
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      // Wait a bit
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Remove locks older than 50ms (should remove our lock)
      const removed = await lockManager.removeOldLocks(50);
      expect(removed).toBe(1);

      const lock = await lockManager.isLocked(EntityType.USERS, 'user-1');
      expect(lock).toBeNull();
    });

    it('should not remove recent locks', async () => {
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      // Try to remove locks older than 1 hour
      const removed = await lockManager.removeOldLocks(3600000);
      expect(removed).toBe(0);

      const lock = await lockManager.isLocked(EntityType.USERS, 'user-1');
      expect(lock).not.toBeNull();
    });
  });

  describe('recordOfflineChange', () => {
    it('should record an offline change', async () => {
      await lockManager.recordOfflineChange(
        EntityType.USERS,
        'user-1',
        'node-b',
        { name: 'Updated Name' },
        'users',
        TEST_DB,
      );

      const changes = await lockManager.getOfflineChanges('node-b');
      expect(changes).toHaveLength(1);
      expect(changes[0].typ).toBe(EntityType.USERS);
      expect(changes[0].value).toBe('user-1');
      expect(changes[0].changeData.name).toBe('Updated Name');
    });
  });

  describe('detectOfflineConflicts', () => {
    beforeEach(async () => {
      // Drop additional collections
      try {
        await db.collection('lock_history').drop();
        await db.collection('offline_changes').drop();
      } catch (err) {
        // Collections might not exist
      }
    });

    it('should detect conflict when offline change conflicts with lock', async () => {
      // Node A acquires lock
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      // Wait a bit
      await new Promise((resolve) => setTimeout(resolve, 50));

      // Node B makes offline change
      await lockManager.recordOfflineChange(
        EntityType.USERS,
        'user-1',
        'node-b',
        { name: 'Offline Update' },
        'users',
        TEST_DB,
      );

      // Node A releases lock (creates history)
      await lockManager.releaseLock(EntityType.USERS, 'user-1', 'node-a');

      // Detect conflicts
      const conflicts = await lockManager.detectOfflineConflicts('node-b');
      expect(conflicts).toHaveLength(1);
      expect(conflicts[0].change.value).toBe('user-1');
      expect(conflicts[0].lock.key).toBe('node-a');
    });

    it('should not detect conflict when no lock overlap', async () => {
      // Node A acquires and releases lock
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });
      await lockManager.releaseLock(EntityType.USERS, 'user-1', 'node-a');

      // Wait to ensure different timestamp
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Node B makes offline change AFTER lock was released
      await lockManager.recordOfflineChange(
        EntityType.USERS,
        'user-1',
        'node-b',
        { name: 'Safe Update' },
        'users',
        TEST_DB,
      );

      // Should not detect any conflicts
      const conflicts = await lockManager.detectOfflineConflicts('node-b');
      expect(conflicts).toHaveLength(0);
    });

    it('should not detect conflict when same node had the lock', async () => {
      // Node B acquires and releases lock
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-b',
        name: 'Node B',
        compName: 'SERVER-B',
      });

      await new Promise((resolve) => setTimeout(resolve, 50));

      // Node B makes offline change while it had the lock
      await lockManager.recordOfflineChange(
        EntityType.USERS,
        'user-1',
        'node-b',
        { name: 'Own Update' },
        'users',
        TEST_DB,
      );

      await lockManager.releaseLock(EntityType.USERS, 'user-1', 'node-b');

      // Should not detect conflicts (same node)
      const conflicts = await lockManager.detectOfflineConflicts('node-b');
      expect(conflicts).toHaveLength(0);
    });
  });

  describe('createConflictRecords', () => {
    beforeEach(async () => {
      try {
        await db.collection('sync_conflicts').drop();
        await db.collection('lock_history').drop();
        await db.collection('offline_changes').drop();
      } catch (err) {
        // Collections might not exist
      }
    });

    it('should create conflict records in sync_conflicts collection', async () => {
      // Simulate a conflict
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });

      await new Promise((resolve) => setTimeout(resolve, 50));

      await lockManager.recordOfflineChange(
        EntityType.USERS,
        'user-1',
        'node-b',
        { name: 'Conflict Update' },
        'users',
        TEST_DB,
      );

      await lockManager.releaseLock(EntityType.USERS, 'user-1', 'node-a');

      const conflicts = await lockManager.detectOfflineConflicts('node-b');
      const created = await lockManager.createConflictRecords(conflicts);

      expect(created).toBe(1);

      const syncConflict = await db
        .collection('sync_conflicts')
        .findOne({ conflictType: 'offline-lock-conflict' });

      expect(syncConflict).not.toBeNull();
      expect(syncConflict?.status).toBe('pending');
      expect(syncConflict?.offlineChange.nodeId).toBe('node-b');
      expect(syncConflict?.lockInfo.lockedBy).toBe('node-a');
    });
  });

  describe('clearOfflineChanges', () => {
    it('should clear offline changes for a node', async () => {
      await lockManager.recordOfflineChange(
        EntityType.USERS,
        'user-1',
        'node-b',
        { name: 'Update 1' },
        'users',
        TEST_DB,
      );
      await lockManager.recordOfflineChange(
        EntityType.USERS,
        'user-2',
        'node-b',
        { name: 'Update 2' },
        'users',
        TEST_DB,
      );

      const cleared = await lockManager.clearOfflineChanges('node-b');
      expect(cleared).toBe(2);

      const remaining = await lockManager.getOfflineChanges('node-b');
      expect(remaining).toHaveLength(0);
    });
  });

  describe('cleanLockHistory', () => {
    beforeEach(async () => {
      try {
        await db.collection('lock_history').drop();
      } catch (err) {
        // Collection might not exist
      }
    });

    it('should clean old lock history records', async () => {
      // Create and release a lock (creates history)
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });
      await lockManager.releaseLock(EntityType.USERS, 'user-1', 'node-a');

      // Wait a bit
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Clean history older than 50ms
      const cleaned = await lockManager.cleanLockHistory(50);
      expect(cleaned).toBe(1);
    });

    it('should not clean recent lock history', async () => {
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A',
      });
      await lockManager.releaseLock(EntityType.USERS, 'user-1', 'node-a');

      // Try to clean history older than 1 hour
      const cleaned = await lockManager.cleanLockHistory(3600000);
      expect(cleaned).toBe(0);
    });
  });
});
