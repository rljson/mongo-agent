// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.

import { describe, expect, it, vi } from 'vitest';

import {
  LockManager,
  createLockManager,
  EntityType,
  LockOptions,
} from '../src/lock-manager.ts';

/**
 * Unit tests for LockManager. No real Mongo connection is used — every
 * collection is a lightweight stub whose methods return canned values, so we
 * can drive each branch (re-entrant lock, race-condition duplicate-key error,
 * release of an unheld lock, offline-conflict detection, etc.) deterministically.
 */

/** A fake Mongo collection where each method is a vi.fn the test can program. */
const makeColl = () => ({
  createIndex: vi.fn().mockResolvedValue(undefined),
  findOne: vi.fn().mockResolvedValue(null),
  insertOne: vi.fn().mockResolvedValue({ insertedId: 'x' }),
  insertMany: vi.fn().mockResolvedValue({ insertedCount: 0 }),
  updateOne: vi.fn().mockResolvedValue({ modifiedCount: 1 }),
  deleteOne: vi.fn().mockResolvedValue({ deletedCount: 1 }),
  deleteMany: vi.fn().mockResolvedValue({ deletedCount: 0 }),
  find: vi.fn().mockReturnValue({ toArray: vi.fn().mockResolvedValue([]) }),
});

/**
 * Build a fake Db plus handles to the named collections the constructor wires
 * up. `extraColls` lets callers pre-register additional collections (e.g.
 * sync_conflicts, or per-database target collections) returned by name.
 */
const makeDb = (extraColls: Record<string, any> = {}, client?: any) => {
  const locking = makeColl();
  const lock_history = makeColl();
  const offline_changes = makeColl();
  const byName: Record<string, any> = {
    locking,
    lock_history,
    offline_changes,
    ...extraColls,
  };
  const db: any = {
    collection: vi.fn((name: string) => {
      if (!byName[name]) byName[name] = makeColl();
      return byName[name];
    }),
    client,
  };
  return { db, locking, lock_history, offline_changes, byName };
};

const baseOptions = (over: Partial<LockOptions> = {}): LockOptions => ({
  typ: EntityType.USERS,
  value: '127731',
  key: 'node-A',
  name: 'Alice',
  compName: 'PC-1',
  ...over,
});

describe('LockManager construction + EntityType', () => {
  it('createLockManager returns a LockManager and wires the 3 collections', () => {
    const { db } = makeDb();
    const lm = createLockManager(db);
    expect(lm).toBeInstanceOf(LockManager);
    expect(db.collection).toHaveBeenCalledWith('locking');
    expect(db.collection).toHaveBeenCalledWith('lock_history');
    expect(db.collection).toHaveBeenCalledWith('offline_changes');
  });

  it('exposes the entity-type mapping', () => {
    expect(EntityType.USERS).toBe(8);
    expect(EntityType.ARTICLES).toBe(11);
  });
});

describe('initialize', () => {
  it('creates all indexes on the three collections', async () => {
    const { db, locking, lock_history, offline_changes } = makeDb();
    const lm = new LockManager(db);
    await lm.initialize();
    expect(locking.createIndex).toHaveBeenCalledTimes(3);
    expect(lock_history.createIndex).toHaveBeenCalledTimes(2);
    expect(offline_changes.createIndex).toHaveBeenCalledTimes(2);
  });
});

describe('acquireLock', () => {
  it('creates a new lock when none exists and fills defaults for optional fields', async () => {
    const { db, locking } = makeDb();
    const lm = new LockManager(db);
    const ok = await lm.acquireLock(baseOptions());
    expect(ok).toBe(true);
    const inserted = locking.insertOne.mock.calls[0][0];
    expect(inserted._id).toBe('8-127731');
    // optional fields defaulted to empty strings
    expect(inserted.telefone).toBe('');
    expect(inserted.eMail).toBe('');
    expect(inserted.clientName).toBe('');
    expect(inserted.commonFields.createdBy).toBe('node-A');
  });

  it('keeps caller-supplied optional fields', async () => {
    const { db, locking } = makeDb();
    const lm = new LockManager(db);
    await lm.acquireLock(
      baseOptions({ telefone: '123', eMail: 'a@b.c', clientName: 'C' }),
    );
    const inserted = locking.insertOne.mock.calls[0][0];
    expect(inserted.telefone).toBe('123');
    expect(inserted.eMail).toBe('a@b.c');
    expect(inserted.clientName).toBe('C');
  });

  it('is re-entrant: same key re-acquiring updates timestamp and returns true', async () => {
    const { db, locking } = makeDb();
    locking.findOne.mockResolvedValue({ _id: '8-127731', key: 'node-A' });
    const lm = new LockManager(db);
    const ok = await lm.acquireLock(baseOptions());
    expect(ok).toBe(true);
    expect(locking.updateOne).toHaveBeenCalledTimes(1);
    expect(locking.insertOne).not.toHaveBeenCalled();
    const setArg = locking.updateOne.mock.calls[0][1].$set;
    expect(setArg['commonFields.updatedBy']).toBe('node-A');
  });

  it('returns false when lock is held by a different key', async () => {
    const { db, locking } = makeDb();
    locking.findOne.mockResolvedValue({ _id: '8-127731', key: 'other' });
    const lm = new LockManager(db);
    const ok = await lm.acquireLock(baseOptions());
    expect(ok).toBe(false);
    expect(locking.updateOne).not.toHaveBeenCalled();
    expect(locking.insertOne).not.toHaveBeenCalled();
  });

  it('returns false on a duplicate-key race (err.code 11000)', async () => {
    const { db, locking } = makeDb();
    locking.insertOne.mockRejectedValue(
      Object.assign(new Error('dup'), { code: 11000 }),
    );
    const lm = new LockManager(db);
    const ok = await lm.acquireLock(baseOptions());
    expect(ok).toBe(false);
  });

  it('rethrows non-duplicate insert errors', async () => {
    const { db, locking } = makeDb();
    locking.insertOne.mockRejectedValue(
      Object.assign(new Error('boom'), { code: 99 }),
    );
    const lm = new LockManager(db);
    await expect(lm.acquireLock(baseOptions())).rejects.toThrow('boom');
  });
});

describe('releaseLock', () => {
  it('returns false when no matching lock exists', async () => {
    const { db, locking, lock_history } = makeDb();
    locking.findOne.mockResolvedValue(null);
    const lm = new LockManager(db);
    const ok = await lm.releaseLock(8, '127731', 'node-A');
    expect(ok).toBe(false);
    expect(lock_history.insertOne).not.toHaveBeenCalled();
  });

  it('saves history and deletes the lock, returning true on success', async () => {
    const { db, locking, lock_history } = makeDb();
    const createdAt = new Date('2026-01-01T00:00:00Z');
    locking.findOne.mockResolvedValue({
      _id: '8-127731',
      key: 'node-A',
      commonFields: { createdAt },
    });
    locking.deleteOne.mockResolvedValue({ deletedCount: 1 });
    const lm = new LockManager(db);
    const ok = await lm.releaseLock(8, '127731', 'node-A');
    expect(ok).toBe(true);
    const hist = lock_history.insertOne.mock.calls[0][0];
    expect(hist.acquiredAt).toBe(createdAt);
    expect(hist.releasedAt).toBeInstanceOf(Date);
  });

  it('returns false when deleteOne removes nothing', async () => {
    const { db, locking } = makeDb();
    locking.findOne.mockResolvedValue({
      _id: '8-127731',
      key: 'node-A',
      commonFields: { createdAt: new Date() },
    });
    locking.deleteOne.mockResolvedValue({ deletedCount: 0 });
    const lm = new LockManager(db);
    const ok = await lm.releaseLock(8, '127731', 'node-A');
    expect(ok).toBe(false);
  });
});

describe('isLocked / isLockedBy / canModify', () => {
  it('isLocked returns the lock record or null', async () => {
    const { db, locking } = makeDb();
    const lm = new LockManager(db);
    locking.findOne.mockResolvedValue(null);
    expect(await lm.isLocked(8, 'v')).toBeNull();
    const rec = { _id: '8-v', key: 'node-A' };
    locking.findOne.mockResolvedValue(rec);
    expect(await lm.isLocked(8, 'v')).toBe(rec);
  });

  it('isLockedBy true only when the holder key matches', async () => {
    const { db, locking } = makeDb();
    const lm = new LockManager(db);
    locking.findOne.mockResolvedValue(null);
    expect(await lm.isLockedBy(8, 'v', 'node-A')).toBe(false);
    locking.findOne.mockResolvedValue({ key: 'node-A' });
    expect(await lm.isLockedBy(8, 'v', 'node-A')).toBe(true);
    locking.findOne.mockResolvedValue({ key: 'other' });
    expect(await lm.isLockedBy(8, 'v', 'node-A')).toBe(false);
  });

  it('canModify true with no lock, true for same key, false for other key', async () => {
    const { db, locking } = makeDb();
    const lm = new LockManager(db);
    locking.findOne.mockResolvedValue(null);
    expect(await lm.canModify(8, 'v', 'node-A')).toBe(true);
    locking.findOne.mockResolvedValue({ key: 'node-A' });
    expect(await lm.canModify(8, 'v', 'node-A')).toBe(true);
    locking.findOne.mockResolvedValue({ key: 'other' });
    expect(await lm.canModify(8, 'v', 'node-A')).toBe(false);
  });
});

describe('getLocksBy / releaseAllLocks / removeOldLocks', () => {
  it('getLocksBy returns the array from the cursor', async () => {
    const { db, locking } = makeDb();
    const rows = [{ _id: '8-1' }];
    locking.find.mockReturnValue({ toArray: vi.fn().mockResolvedValue(rows) });
    const lm = new LockManager(db);
    expect(await lm.getLocksBy('node-A')).toBe(rows);
    expect(locking.find).toHaveBeenCalledWith({ key: 'node-A' });
  });

  it('releaseAllLocks returns deletedCount', async () => {
    const { db, locking } = makeDb();
    locking.deleteMany.mockResolvedValue({ deletedCount: 4 });
    const lm = new LockManager(db);
    expect(await lm.releaseAllLocks('node-A')).toBe(4);
  });

  it('removeOldLocks deletes by updatedAt cutoff and returns the count', async () => {
    const { db, locking } = makeDb();
    locking.deleteMany.mockResolvedValue({ deletedCount: 2 });
    const lm = new LockManager(db);
    expect(await lm.removeOldLocks(1000)).toBe(2);
    const filter = locking.deleteMany.mock.calls[0][0];
    expect(filter['commonFields.updatedAt'].$lt).toBeInstanceOf(Date);
  });
});

describe('acquireLockWithRetry', () => {
  it('returns true immediately on first success without delaying', async () => {
    const { db, locking } = makeDb();
    const lm = new LockManager(db);
    const ok = await lm.acquireLockWithRetry(baseOptions(), 3, 50);
    expect(ok).toBe(true);
    expect(locking.insertOne).toHaveBeenCalledTimes(1);
  });

  it('retries with delay and eventually succeeds', async () => {
    vi.useFakeTimers();
    try {
      const { db, locking } = makeDb();
      // Held by another node twice, then free.
      locking.findOne
        .mockResolvedValueOnce({ key: 'other' })
        .mockResolvedValueOnce({ key: 'other' })
        .mockResolvedValueOnce(null);
      const lm = new LockManager(db);
      const p = lm.acquireLockWithRetry(baseOptions(), 3, 100);
      await vi.advanceTimersByTimeAsync(250);
      expect(await p).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it('returns false after exhausting all retries', async () => {
    vi.useFakeTimers();
    try {
      const { db, locking } = makeDb();
      locking.findOne.mockResolvedValue({ key: 'other' });
      const lm = new LockManager(db);
      const p = lm.acquireLockWithRetry(baseOptions(), 2, 100);
      await vi.advanceTimersByTimeAsync(250);
      expect(await p).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('offline changes', () => {
  it('recordOfflineChange inserts a change with a composed id', async () => {
    const { db, offline_changes } = makeDb();
    const lm = new LockManager(db);
    await lm.recordOfflineChange(8, '127731', 'node-A', { a: 1 }, 'coll', 'dbx');
    const inserted = offline_changes.insertOne.mock.calls[0][0];
    expect(inserted._id).toMatch(/^8-127731-\d+$/);
    expect(inserted.changeData).toEqual({ a: 1 });
    expect(inserted.collection).toBe('coll');
    expect(inserted.database).toBe('dbx');
  });

  it('getOfflineChanges returns the cursor array', async () => {
    const { db, offline_changes } = makeDb();
    const rows = [{ _id: 'c1' }];
    offline_changes.find.mockReturnValue({
      toArray: vi.fn().mockResolvedValue(rows),
    });
    const lm = new LockManager(db);
    expect(await lm.getOfflineChanges('node-A')).toBe(rows);
  });

  it('clearOfflineChanges returns deletedCount', async () => {
    const { db, offline_changes } = makeDb();
    offline_changes.deleteMany.mockResolvedValue({ deletedCount: 3 });
    const lm = new LockManager(db);
    expect(await lm.clearOfflineChanges('node-A')).toBe(3);
  });
});

describe('detectOfflineConflicts', () => {
  it('returns empty when there are no offline changes', async () => {
    const { db } = makeDb();
    const lm = new LockManager(db);
    expect(await lm.detectOfflineConflicts('node-A')).toEqual([]);
  });

  it('flags a change that overlaps a lock held by another node', async () => {
    const { db, offline_changes, lock_history } = makeDb();
    const change = {
      _id: 'c1',
      typ: 8,
      value: '1',
      key: 'node-A',
      changeTimestamp: new Date('2026-01-02T00:00:00Z'),
    };
    const noConflictChange = {
      _id: 'c2',
      typ: 8,
      value: '2',
      key: 'node-A',
      changeTimestamp: new Date('2026-01-02T00:00:00Z'),
    };
    offline_changes.find.mockReturnValue({
      toArray: vi.fn().mockResolvedValue([change, noConflictChange]),
    });
    const conflictingLock = { key: 'node-B', name: 'Bob' };
    lock_history.findOne
      .mockResolvedValueOnce(conflictingLock)
      .mockResolvedValueOnce(null);
    const lm = new LockManager(db);
    const conflicts = await lm.detectOfflineConflicts('node-A');
    expect(conflicts).toHaveLength(1);
    expect(conflicts[0].change).toBe(change);
    expect(conflicts[0].lock).toBe(conflictingLock);
  });
});

describe('createConflictRecords', () => {
  it('returns 0 for an empty conflict list', async () => {
    const { db } = makeDb();
    const lm = new LockManager(db);
    expect(await lm.createConflictRecords([])).toBe(0);
  });

  it('builds records from current doc and inserts them', async () => {
    const sync_conflicts = makeColl();
    sync_conflicts.insertMany.mockResolvedValue({ insertedCount: 1 });
    const targetColl = makeColl();
    targetColl.findOne.mockResolvedValue({ _id: '1', field: 'remote' });
    const targetDb = { collection: vi.fn().mockReturnValue(targetColl) };
    const client = { db: vi.fn().mockReturnValue(targetDb) };
    const { db } = makeDb({ sync_conflicts }, client);
    const lm = new LockManager(db);

    const conflict = {
      change: {
        _id: 'c1',
        typ: 8,
        value: '1',
        key: 'node-A',
        changeTimestamp: new Date('2026-01-02T00:00:00Z'),
        changeData: { x: 1 },
        collection: 'coll',
        database: 'dbx',
      } as any,
      lock: {
        key: 'node-B',
        name: 'Bob',
        acquiredAt: new Date('2026-01-01T00:00:00Z'),
        releasedAt: new Date('2026-01-03T00:00:00Z'),
      } as any,
    };
    const count = await lm.createConflictRecords([conflict]);
    expect(count).toBe(1);
    expect(client.db).toHaveBeenCalledWith('dbx');
    expect(targetColl.findOne).toHaveBeenCalledWith({ _id: '1' });
    const rec = sync_conflicts.insertMany.mock.calls[0][0][0];
    expect(rec.conflictId).toBe('offline-c1');
    expect(rec.versions[1].data).toEqual({ _id: '1', field: 'remote' });
  });

  it('falls back to {} when the current doc is missing', async () => {
    const sync_conflicts = makeColl();
    sync_conflicts.insertMany.mockResolvedValue({ insertedCount: 1 });
    const targetColl = makeColl();
    targetColl.findOne.mockResolvedValue(null);
    const targetDb = { collection: vi.fn().mockReturnValue(targetColl) };
    const client = { db: vi.fn().mockReturnValue(targetDb) };
    const { db } = makeDb({ sync_conflicts }, client);
    const lm = new LockManager(db);

    const conflict = {
      change: {
        _id: 'c1',
        typ: 8,
        value: '1',
        key: 'node-A',
        changeTimestamp: new Date(),
        changeData: {},
        collection: 'coll',
        database: 'dbx',
      } as any,
      lock: {
        key: 'node-B',
        name: 'Bob',
        acquiredAt: new Date(),
        releasedAt: new Date(),
      } as any,
    };
    await lm.createConflictRecords([conflict]);
    const rec = sync_conflicts.insertMany.mock.calls[0][0][0];
    expect(rec.versions[1].data).toEqual({});
  });
});

describe('cleanLockHistory', () => {
  it('deletes history older than cutoff and returns the count', async () => {
    const { db, lock_history } = makeDb();
    lock_history.deleteMany.mockResolvedValue({ deletedCount: 7 });
    const lm = new LockManager(db);
    expect(await lm.cleanLockHistory(1000)).toBe(7);
    const filter = lock_history.deleteMany.mock.calls[0][0];
    expect(filter.releasedAt.$lt).toBeInstanceOf(Date);
  });
});
