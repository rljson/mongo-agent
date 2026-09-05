// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.

import { describe, expect, it, vi } from 'vitest';

import {
  clearDirtyForCollection,
  clearDirtyPartitions,
  ensureDirtyIndexes,
  listDirtyForCollection,
  markCollectionFullDirty,
  markDirtyById,
} from '../../src/hashing/state-dirty.ts';

/**
 * Real-logic tests for state-dirty using fake Mongo collections.
 * No live Mongo / network — every collection is a canned stub that records
 * the calls made against it so we can assert which marker path was taken.
 */

interface Recorded {
  filter?: any;
  update?: any;
  options?: any;
}

/**
 * Build a fake collection. `findOneResults` is consumed in order — each call to
 * findOne returns (and removes) the next queued result. updateOne / deleteMany /
 * find calls are recorded on the returned `calls` object.
 */
function fakeColl(opts: {
  findOne?: (filter: any, options: any) => any;
  findArray?: any[];
  createIndex?: () => any;
} = {}) {
  const calls = {
    updateOne: [] as Recorded[],
    deleteMany: [] as Recorded[],
    createIndex: [] as any[],
    findProjectArgs: undefined as any,
  };
  const coll: any = {
    createIndex: vi.fn(async (spec: any) => {
      calls.createIndex.push(spec);
    }),
    findOne: vi.fn(async (filter: any, options: any) =>
      opts.findOne ? opts.findOne(filter, options) : null,
    ),
    updateOne: vi.fn(async (filter: any, update: any, options: any) => {
      calls.updateOne.push({ filter, update, options });
      return { acknowledged: true };
    }),
    deleteMany: vi.fn(async (filter: any) => {
      calls.deleteMany.push({ filter });
      return { deletedCount: 0 };
    }),
    find: vi.fn(() => ({
      project: (p: any) => {
        calls.findProjectArgs = p;
        return { toArray: async () => opts.findArray ?? [] };
      },
    })),
  };
  return { coll, calls };
}

/**
 * Build a fake Db that dispatches collection(name) to a per-name stub.
 * `byName` maps the collection name to a { coll, calls } from fakeColl.
 */
function fakeDb(byName: Record<string, { coll: any; calls: any }>) {
  return {
    collection: vi.fn((name: string) => {
      const entry = byName[name];
      if (!entry) throw new Error(`unexpected collection ${name}`);
      return entry.coll;
    }),
  } as any;
}

describe('ensureDirtyIndexes', () => {
  it('creates both indexes on state_dirty', async () => {
    const dirty = fakeColl();
    const db = fakeDb({ state_dirty: dirty });
    await ensureDirtyIndexes(db);
    expect(dirty.calls.createIndex).toEqual([
      { coll: 1, partition: 1 },
      { dirtyAt: 1 },
    ]);
  });
});

describe('markDirtyById', () => {
  it('returns early (no-op) when collName is null/undefined', async () => {
    const db = fakeDb({});
    await markDirtyById(db, null, 'x');
    await markDirtyById(db, undefined, 'x');
    expect(db.collection).not.toHaveBeenCalled();
  });

  it('marks the matching partition dirty when merkle meta is found', async () => {
    const merkle = fakeColl({ findOne: () => ({ idx: 7 }) });
    const dirty = fakeColl();
    const db = fakeDb({ state_merkle: merkle, state_dirty: dirty });

    await markDirtyById(db, 'articles', 'abc');

    expect(dirty.calls.updateOne).toHaveLength(1);
    const c = dirty.calls.updateOne[0];
    expect(c.filter).toEqual({ _id: 'articles::p7' });
    expect(c.update.$set.partition).toBe(7);
    expect(c.update.$set.coll).toBe('articles');
    expect(c.options).toEqual({ upsert: true });
  });

  it('after-max append: no blocking partition → marks the LAST partition dirty', async () => {
    // findPartitionForId → null (no covering partition)
    // blocking query → null (nothing has maxId >= docId)
    // lastPart query → idx 4
    const responses = [
      null, // findPartitionForId
      null, // blocking
      { idx: 4 }, // lastPart
    ];
    const merkle = fakeColl({ findOne: () => responses.shift() });
    const dirty = fakeColl();
    const db = fakeDb({ state_merkle: merkle, state_dirty: dirty });

    await markDirtyById(db, 'articles', 'zzz');

    expect(dirty.calls.updateOne).toHaveLength(1);
    expect(dirty.calls.updateOne[0].filter).toEqual({ _id: 'articles::p4' });
    expect(dirty.calls.updateOne[0].update.$set.partition).toBe(4);
  });

  it('gap case: blocking partition exists → falls back to FULL marker', async () => {
    const responses = [
      null, // findPartitionForId → no cover
      { _id: 'blockerDoc' }, // blocking exists → gap, not after-max
    ];
    const merkle = fakeColl({ findOne: () => responses.shift() });
    const dirty = fakeColl();
    const db = fakeDb({ state_merkle: merkle, state_dirty: dirty });

    await markDirtyById(db, 'articles', 'mid', { reason: 'gap-test' });

    expect(dirty.calls.updateOne).toHaveLength(1);
    const c = dirty.calls.updateOne[0];
    expect(c.filter).toEqual({ _id: 'articles::FULL' });
    expect(c.update.$set.full).toBe(true);
    expect(c.update.$set.reason).toBe('gap-test');
  });

  it('no cache: no blocking and no lastPart → FULL with default reason', async () => {
    const responses = [
      null, // findPartitionForId
      null, // blocking
      null, // lastPart
    ];
    const merkle = fakeColl({ findOne: () => responses.shift() });
    const dirty = fakeColl();
    const db = fakeDb({ state_merkle: merkle, state_dirty: dirty });

    await markDirtyById(db, 'articles', 'x');

    const c = dirty.calls.updateOne[0];
    expect(c.filter).toEqual({ _id: 'articles::FULL' });
    expect(c.update.$set.reason).toBe('partition_not_found');
  });

  it('lastPart present but idx is not a number → FULL fallback', async () => {
    const responses = [
      null, // findPartitionForId
      null, // blocking
      { idx: 'oops' }, // lastPart with bad idx
    ];
    const merkle = fakeColl({ findOne: () => responses.shift() });
    const dirty = fakeColl();
    const db = fakeDb({ state_merkle: merkle, state_dirty: dirty });

    await markDirtyById(db, 'articles', 'x');

    expect(dirty.calls.updateOne[0].filter).toEqual({ _id: 'articles::FULL' });
  });

  it('findPartitionForId throws → logs, treats as no meta, continues to FULL', async () => {
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    let call = 0;
    const merkle = fakeColl({
      findOne: () => {
        call++;
        if (call === 1) throw new Error('boom'); // findPartitionForId
        return null; // blocking, lastPart
      },
    });
    const dirty = fakeColl();
    const db = fakeDb({ state_merkle: merkle, state_dirty: dirty });

    await markDirtyById(db, 'articles', 'x');

    expect(logSpy).toHaveBeenCalledWith(
      expect.stringContaining('[markDirtyById] ERROR finding partition'),
    );
    expect(dirty.calls.updateOne[0].filter).toEqual({ _id: 'articles::FULL' });
    logSpy.mockRestore();
  });

  it('after-max branch: state_merkle query throws → swallowed, falls through to FULL', async () => {
    let call = 0;
    const merkle = fakeColl({
      findOne: () => {
        call++;
        if (call === 1) return null; // findPartitionForId → no cover
        throw new Error('merkle down'); // blocking query throws
      },
    });
    const dirty = fakeColl();
    const db = fakeDb({ state_merkle: merkle, state_dirty: dirty });

    await markDirtyById(db, 'articles', 'x');

    expect(dirty.calls.updateOne[0].filter).toEqual({ _id: 'articles::FULL' });
  });

  it('uses empty options default and reason falls back to partition_not_found', async () => {
    const responses = [null, null, null];
    const merkle = fakeColl({ findOne: () => responses.shift() });
    const dirty = fakeColl();
    const db = fakeDb({ state_merkle: merkle, state_dirty: dirty });

    // call WITHOUT options argument to exercise the default param
    await markDirtyById(db, 'articles', 'x');
    expect(dirty.calls.updateOne[0].update.$set.reason).toBe('partition_not_found');
  });
});

describe('markCollectionFullDirty', () => {
  it('upserts a FULL marker with the given reason', async () => {
    const dirty = fakeColl();
    const db = fakeDb({ state_dirty: dirty });

    await markCollectionFullDirty(db, 'articles', 'bulk-import');

    const c = dirty.calls.updateOne[0];
    expect(c.filter).toEqual({ _id: 'articles::FULL' });
    expect(c.update.$set).toMatchObject({
      coll: 'articles',
      full: true,
      reason: 'bulk-import',
    });
    expect(c.options).toEqual({ upsert: true });
  });

  it('defaults reason to "bulk"', async () => {
    const dirty = fakeColl();
    const db = fakeDb({ state_dirty: dirty });
    await markCollectionFullDirty(db, 'articles');
    expect(dirty.calls.updateOne[0].update.$set.reason).toBe('bulk');
  });
});

describe('listDirtyForCollection', () => {
  it('reports full=true and sorts partition numbers ascending', async () => {
    const dirty = fakeColl({
      findArray: [
        { _id: 'a::p3', partition: 3 },
        { _id: 'a::FULL', full: true },
        { _id: 'a::p1', partition: 1 },
        { _id: 'a::weird' }, // no partition / no full → ignored
      ],
    });
    const db = fakeDb({ state_dirty: dirty });

    const res = await listDirtyForCollection(db, 'a');
    expect(res).toEqual({ full: true, partitions: [1, 3] });
  });

  it('reports full=false when no full marker present', async () => {
    const dirty = fakeColl({
      findArray: [{ _id: 'a::p2', partition: 2 }],
    });
    const db = fakeDb({ state_dirty: dirty });

    const res = await listDirtyForCollection(db, 'a');
    expect(res).toEqual({ full: false, partitions: [2] });
  });
});

describe('clearDirtyForCollection', () => {
  it('deletes all markers for the collection', async () => {
    const dirty = fakeColl();
    const db = fakeDb({ state_dirty: dirty });
    await clearDirtyForCollection(db, 'a');
    expect(dirty.calls.deleteMany[0].filter).toEqual({ coll: 'a' });
  });
});

describe('clearDirtyPartitions', () => {
  it('returns early for empty array (no deleteMany)', async () => {
    const dirty = fakeColl();
    const db = fakeDb({ state_dirty: dirty });
    await clearDirtyPartitions(db, 'a', []);
    expect(dirty.calls.deleteMany).toHaveLength(0);
  });

  it('returns early for undefined partitions', async () => {
    const dirty = fakeColl();
    const db = fakeDb({ state_dirty: dirty });
    await clearDirtyPartitions(db, 'a', undefined as any);
    expect(dirty.calls.deleteMany).toHaveLength(0);
  });

  it('deletes the mapped partition ids', async () => {
    const dirty = fakeColl();
    const db = fakeDb({ state_dirty: dirty });
    await clearDirtyPartitions(db, 'a', [2, 5]);
    expect(dirty.calls.deleteMany[0].filter).toEqual({
      _id: { $in: ['a::p2', 'a::p5'] },
    });
  });
});
