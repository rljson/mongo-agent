// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * Unit tests for LockManager
 */

import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { MongoClient, Db } from 'mongodb';
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
        compName: 'SERVER-A'
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
        compName: 'SERVER-A'
      });

      // Node B tries to acquire same lock
      const acquired = await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-b',
        name: 'Node B',
        compName: 'SERVER-B'
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
        compName: 'SERVER-A'
      });

      // Second acquisition by same node
      const acquired2 = await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A'
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
        compName: 'SERVER-A'
      });

      // Release lock
      const released = await lockManager.releaseLock(EntityType.USERS, 'user-1', 'node-a');
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
        compName: 'SERVER-A'
      });

      // Node B tries to release
      const released = await lockManager.releaseLock(EntityType.USERS, 'user-1', 'node-b');
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
        eMail: 'nodea@example.com'
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
      const canModify = await lockManager.canModify(EntityType.USERS, 'user-1', 'node-a');
      expect(canModify).toBe(true);
    });

    it('should allow modification for lock owner', async () => {
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A'
      });

      const canModify = await lockManager.canModify(EntityType.USERS, 'user-1', 'node-a');
      expect(canModify).toBe(true);
    });

    it('should prevent modification for non-owner', async () => {
      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-1',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A'
      });

      const canModify = await lockManager.canModify(EntityType.USERS, 'user-1', 'node-b');
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
        compName: 'SERVER-A'
      });

      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-2',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A'
      });

      await lockManager.acquireLock({
        typ: EntityType.PRODUCTS,
        value: 'product-1',
        key: 'node-b',
        name: 'Node B',
        compName: 'SERVER-B'
      });

      const locksA = await lockManager.getLocksBy('node-a');
      expect(locksA).toHaveLength(2);
      expect(locksA.every(l => l.key === 'node-a')).toBe(true);

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
        compName: 'SERVER-A'
      });

      await lockManager.acquireLock({
        typ: EntityType.USERS,
        value: 'user-2',
        key: 'node-a',
        name: 'Node A',
        compName: 'SERVER-A'
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
        compName: 'SERVER-A'
      });

      const isLockedByA = await lockManager.isLockedBy(EntityType.USERS, 'user-1', 'node-a');
      expect(isLockedByA).toBe(true);

      const isLockedByB = await lockManager.isLockedBy(EntityType.USERS, 'user-1', 'node-b');
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
        compName: 'SERVER-A'
      });

      // Wait a bit
      await new Promise(resolve => setTimeout(resolve, 100));

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
        compName: 'SERVER-A'
      });

      // Try to remove locks older than 1 hour
      const removed = await lockManager.removeOldLocks(3600000);
      expect(removed).toBe(0);

      const lock = await lockManager.isLocked(EntityType.USERS, 'user-1');
      expect(lock).not.toBeNull();
    });
  });
});
