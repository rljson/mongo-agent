// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.

import { BsMem } from '@rljson/bs';
import { EventEmitter } from 'node:events';
import { describe, expect, it, vi } from 'vitest';

import {
  createSuppressor,
  isInternalCollection,
  startDbChangeStream,
  SYNC_OPS_TABLE_CFG,
} from '../src/watch-changes.ts';

/**
 * Unit tests for the change-stream watcher. No real MongoDB: we drive the code
 * with a fake `Db` whose collections are in-memory Maps, and a fake change
 * stream that is a plain EventEmitter (matching the `cs.on('change'|'error')`
 * surface the producer uses). The serial queue inside startDbChangeStream is
 * driven via flushing microtasks after emitting events.
 */

interface FakeDoc {
  _id: unknown;
  [k: string]: unknown;
}

/**
 * A minimal in-memory collection supporting the subset of the driver API that
 * watch-changes.ts touches: findOne, insertOne, updateOne (with $set + upsert),
 * deleteOne. Keyed by `_id`.
 */
const makeColl = () => {
  const store = new Map<string, FakeDoc>();
  const key = (id: unknown) => String(id);
  return {
    store,
    async findOne(filter: { _id?: unknown }) {
      if (filter && '_id' in filter) {
        return store.get(key(filter._id)) ?? null;
      }
      // first doc
      const first = store.values().next();
      return first.done ? null : first.value;
    },
    async insertOne(doc: FakeDoc) {
      const k = key(doc._id);
      if (store.has(k)) {
        const e: any = new Error('duplicate key');
        e.code = 11000;
        throw e;
      }
      store.set(k, doc);
      return { insertedId: doc._id };
    },
    async updateOne(
      filter: { _id?: unknown },
      update: { $set?: Record<string, unknown> },
      opts?: { upsert?: boolean },
    ) {
      const k = key(filter._id);
      const existing = store.get(k);
      if (existing) {
        Object.assign(existing, update.$set ?? {});
        return { matchedCount: 1, modifiedCount: 1, upsertedCount: 0 };
      }
      if (opts?.upsert) {
        store.set(k, { _id: filter._id, ...(update.$set ?? {}) } as FakeDoc);
        return { matchedCount: 0, modifiedCount: 0, upsertedCount: 1 };
      }
      return { matchedCount: 0, modifiedCount: 0, upsertedCount: 0 };
    },
    async deleteOne(filter: { _id?: unknown }) {
      const k = key(filter._id);
      const had = store.delete(k);
      return { deletedCount: had ? 1 : 0 };
    },
  };
};

/**
 * Fake Db. `watch` returns a controllable EventEmitter so we can emit
 * 'change' / 'error' events at will. `collection(name)` returns a stable
 * per-name in-memory collection.
 */
const makeDb = (
  opts: {
    watchImpl?: (pipeline: unknown, options: unknown) => any;
    /** Names returned by listCollections (for computeStateCheckpoint). */
    listCollections?: string[];
    /** If true, listCollections().toArray() rejects (force checkpoint failure). */
    listCollectionsThrows?: boolean;
  } = {},
) => {
  const colls = new Map<string, ReturnType<typeof makeColl>>();
  const collection = (name: string) => {
    let c = colls.get(name);
    if (!c) {
      c = makeColl();
      colls.set(name, c);
    }
    return c as any;
  };
  const cs = new EventEmitter() as any;
  expect(typeof cs.removeAllListeners).toBe('function'); // present via EventEmitter
  const watchCalls: Array<{ pipeline: unknown; options: unknown }> = [];
  const db = {
    collection,
    watch(pipeline: unknown, options: unknown) {
      watchCalls.push({ pipeline, options });
      if (opts.watchImpl) return opts.watchImpl(pipeline, options);
      return cs;
    },
    // computeStateCheckpoint calls db.listCollections({}, {nameOnly:true}).toArray()
    listCollections() {
      return {
        toArray: async () => {
          if (opts.listCollectionsThrows) {
            throw new Error('listCollections failed');
          }
          return (opts.listCollections ?? []).map((name) => ({ name }));
        },
      };
    },
  } as any;
  return { db, cs, colls, collection, watchCalls };
};

/** Flush pending microtasks so the serial-queue chain settles. */
const flush = async (times = 50) => {
  for (let i = 0; i < times; i++) {
    await Promise.resolve();
  }
};

const emitChange = (cs: any, change: unknown) => cs.emit('change', change);

describe('createSuppressor', () => {
  it('add then has returns true; missing key returns false', () => {
    const s = createSuppressor(1000);
    const ns = { db: 'd', coll: 'c' };
    s.add(ns, 'id1');
    expect(s.has(ns, 'id1')).toBe(true);
    expect(s.has(ns, 'id2')).toBe(false);
  });

  it('builds key from empty db/coll when namespace fields are missing', () => {
    const s = createSuppressor(1000);
    // ns with undefined fields exercises the `|| ''` fallbacks.
    s.add({ db: undefined as any, coll: undefined as any }, 7);
    expect(s.has({ db: undefined as any, coll: undefined as any }, 7)).toBe(
      true,
    );
  });

  it('expires entries past their TTL (cleanup deletes them)', () => {
    let now = 1000;
    const spy = vi.spyOn(Date, 'now').mockImplementation(() => now);
    try {
      const s = createSuppressor(50); // 50ms TTL
      const ns = { db: 'd', coll: 'c' };
      s.add(ns, 'x'); // expiresAt = 1050
      now = 1100; // past expiry -> cleanup() deletes on next has()
      expect(s.has(ns, 'x')).toBe(false);
    } finally {
      spy.mockRestore();
    }
  });

  it('defaults TTL to 30000ms when not provided', () => {
    let now = 0;
    const spy = vi.spyOn(Date, 'now').mockImplementation(() => now);
    try {
      const s = createSuppressor();
      const ns = { db: 'd', coll: 'c' };
      s.add(ns, 'k');
      now = 29_999; // still within default TTL
      expect(s.has(ns, 'k')).toBe(true);
    } finally {
      spy.mockRestore();
    }
  });
});

describe('isInternalCollection', () => {
  it('treats undefined / empty as internal', () => {
    expect(isInternalCollection(undefined)).toBe(true);
    expect(isInternalCollection('')).toBe(true);
  });

  it('matches exact internal names', () => {
    expect(isInternalCollection('sync_ops')).toBe(true);
    expect(isInternalCollection('state_checkpoints')).toBe(true);
    expect(isInternalCollection('sync_head')).toBe(true);
  });

  it('matches internal prefixes', () => {
    expect(isInternalCollection('system.indexes')).toBe(true);
    expect(isInternalCollection('sync_anything')).toBe(true);
    expect(isInternalCollection('state_foo')).toBe(true);
  });

  it('passes through user collections', () => {
    expect(isInternalCollection('cd_articles')).toBe(false);
    expect(isInternalCollection('users')).toBe(false);
  });
});

describe('SYNC_OPS_TABLE_CFG', () => {
  it('is hashed and has the expected key', () => {
    expect(SYNC_OPS_TABLE_CFG.key).toBe('sync_ops');
    expect(typeof SYNC_OPS_TABLE_CFG._hash).toBe('string');
    expect(SYNC_OPS_TABLE_CFG._hash.length).toBeGreaterThan(0);
  });
});

describe('startDbChangeStream: setup', () => {
  it('starts without a resume token and logs info', async () => {
    const { db, watchCalls } = makeDb();
    const info = vi.fn();
    const cs = await startDbChangeStream({
      db,
      nodeId: 'L1',
      logger: { info },
    });
    expect(cs).toBeDefined();
    expect(watchCalls).toHaveLength(1);
    // No resumeAfter when no token present.
    expect((watchCalls[0].options as any).resumeAfter).toBeUndefined();
    expect(info).toHaveBeenCalled();
  });

  it('passes resumeAfter when a stored resume token exists', async () => {
    const { db, watchCalls } = makeDb();
    db.collection('sync_resume').store.set('resume', {
      _id: 'resume',
      token: { _data: 'TKN' },
    });
    await startDbChangeStream({ db, nodeId: 'L1' });
    expect((watchCalls[0].options as any).resumeAfter).toEqual({
      _data: 'TKN',
    });
  });

  it('tolerates a failing resume-token lookup (catch -> null)', async () => {
    const { db, watchCalls } = makeDb();
    const orig = db.collection;
    db.collection = (name: string) => {
      if (name === 'sync_resume') {
        return {
          findOne: async () => {
            throw new Error('boom');
          },
        };
      }
      return orig(name);
    };
    await startDbChangeStream({ db, nodeId: 'L1' });
    // Started anyway, with no resume token.
    expect((watchCalls[0].options as any).resumeAfter).toBeUndefined();
  });
});

describe('startDbChangeStream: invalid resume token recovery', () => {
  it('clears the token and restarts fresh when watch throws a resume error', async () => {
    let call = 0;
    const sharedCs = new EventEmitter() as any;
    const { db } = makeDb({
      watchImpl: () => {
        call++;
        if (call === 1) {
          const e: any = new Error('cannot resume the stream');
          throw e;
        }
        return sharedCs;
      },
    });
    db.collection('sync_resume').store.set('resume', {
      _id: 'resume',
      token: { _data: 'STALE' },
    });
    const warn = vi.fn();
    const info = vi.fn();
    const cs = await startDbChangeStream({
      db,
      nodeId: 'L1',
      logger: { warn, info },
    });
    expect(call).toBe(2); // retried watch
    expect(cs).toBe(sharedCs);
    expect(warn).toHaveBeenCalled();
    // Token was deleted.
    expect(db.collection('sync_resume').store.has('resume')).toBe(false);
  });

  it('tolerates the deleteOne failing while clearing the stale token', async () => {
    let call = 0;
    const sharedCs = new EventEmitter() as any;
    const base = makeDb({
      watchImpl: () => {
        call++;
        if (call === 1) throw new Error('resume token not found');
        return sharedCs;
      },
    });
    const orig = base.db.collection;
    base.db.collection = (name: string) => {
      if (name === 'sync_resume') {
        return {
          findOne: async () => ({ _id: 'resume', token: { _data: 'X' } }),
          deleteOne: async () => {
            throw new Error('delete failed');
          },
        };
      }
      return orig(name);
    };
    const cs = await startDbChangeStream({ db: base.db, nodeId: 'L1' });
    expect(cs).toBe(sharedCs);
    expect(call).toBe(2);
  });

  it('rethrows non-resume watch errors', async () => {
    const { db } = makeDb({
      watchImpl: () => {
        throw new Error('totally different failure');
      },
    });
    await expect(
      startDbChangeStream({ db, nodeId: 'L1' }),
    ).rejects.toThrow('totally different failure');
  });

  it('rethrows a resume-like error when there is no token to clear', async () => {
    // resumeToken is null -> the `resumeToken && ...` guard is false -> rethrow.
    const { db } = makeDb({
      watchImpl: () => {
        throw new Error('cannot resume the stream');
      },
    });
    await expect(
      startDbChangeStream({ db, nodeId: 'L1' }),
    ).rejects.toThrow('cannot resume');
  });
});

describe('startDbChangeStream: change handling', () => {
  it('ignores changes on internal collections', async () => {
    const { db, cs } = makeDb();
    await startDbChangeStream({ db, nodeId: 'L1' });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'sync_ops' },
      operationType: 'insert',
      documentKey: { _id: 1 },
      _id: { _data: 'rt' },
    });
    await flush();
    // Nothing appended.
    expect(db.collection('sync_ops').store.size).toBe(0);
  });

  it('skips when docId cannot be resolved', async () => {
    const { db, cs } = makeDb();
    await startDbChangeStream({ db, nodeId: 'L1' });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'insert',
      // no documentKey, no fullDocument._id
      _id: { _data: 'rt' },
    });
    await flush();
    expect(db.collection('sync_ops').store.size).toBe(0);
  });

  it('skips when namespace is missing', async () => {
    const { db, cs } = makeDb();
    await startDbChangeStream({ db, nodeId: 'L1' });
    emitChange(cs, {
      // ns undefined -> coll undefined -> isInternalCollection true actually.
      // To hit the `!ns` guard specifically, provide a docId but ns must be
      // falsy AND coll resolvable as non-internal. coll is undefined so
      // isInternalCollection short-circuits true first. So this path is
      // covered by the internal-collection guard; assert no append.
      operationType: 'insert',
      documentKey: { _id: 5 },
      _id: { _data: 'rt' },
    });
    await flush();
    expect(db.collection('sync_ops').store.size).toBe(0);
  });

  it('appends a full insert op with fullDocument blob + resume token + sync_local update', async () => {
    const { db, cs } = makeDb();
    const bs = new BsMem();
    await startDbChangeStream({ db, nodeId: 'L1', bs });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'insert',
      documentKey: { _id: 'A1' },
      fullDocument: { _id: 'A1', name: 'widget' },
      _id: { _data: 'resume-1' },
      clusterTime: { $timestamp: { t: 1, i: 1 } },
      wallTime: new Date('2026-01-01T00:00:00.000Z'),
    });
    await flush();
    // One op stored in sync_ops with chain fields.
    const ops = [...db.collection('sync_ops').store.values()];
    expect(ops).toHaveLength(1);
    expect(ops[0]).toMatchObject({
      _id: 'L1_1',
      origin: 'L1',
      seq: 1,
      operationType: 'insert',
    });
    // sync_local advanced.
    const local = db.collection('sync_local').store.get('local');
    expect(local?.seq).toBe(1);
    // resume token persisted.
    expect(db.collection('sync_resume').store.get('resume')?.token).toEqual({
      _data: 'resume-1',
    });
  });

  it('handles update ops with updateDescription and wallTime string', async () => {
    const { db, cs } = makeDb();
    await startDbChangeStream({ db, nodeId: 'L1' });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'update',
      documentKey: { _id: 'A2' },
      updateDescription: { updatedFields: { name: 'new' } },
      wallTime: '2026-02-02T00:00:00.000Z', // string branch
      // no _id -> no resume token write branch
    });
    await flush();
    const ops = [...db.collection('sync_ops').store.values()];
    expect(ops).toHaveLength(1);
    expect(ops[0].operationType).toBe('update');
    // No change._id => resume token not written.
    expect(db.collection('sync_resume').store.has('resume')).toBe(false);
  });

  it('resolves docId from fullDocument._id when documentKey is absent', async () => {
    const { db, cs } = makeDb();
    await startDbChangeStream({ db, nodeId: 'L1' });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'replace',
      fullDocument: { _id: 'F1', v: 1 },
      _id: { _data: 'rt-f' },
      // wallTime absent -> the `?? undefined` branch
    });
    await flush();
    const ops = [...db.collection('sync_ops').store.values()];
    expect(ops).toHaveLength(1);
    expect(ops[0].docId).toBe('F1');
  });

  it('suppresses echo ops added by the suppressor (no append) but still marks dirty', async () => {
    const { db, cs } = makeDb();
    const suppressor = createSuppressor(10_000);
    suppressor.add({ db: 'caratdb', coll: 'cd_articles' }, 'E1');
    await startDbChangeStream({
      db,
      nodeId: 'L1',
      suppressor,
      trackStateHash: true,
    });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'update',
      documentKey: { _id: 'E1' },
      _id: { _data: 'rt' },
    });
    await flush();
    // Suppressed -> no sync_ops append.
    expect(db.collection('sync_ops').store.size).toBe(0);
    // But markDirtyById ran (trackStateHash) -> a state_dirty marker exists.
    expect(db.collection('state_dirty').store.size).toBeGreaterThan(0);
  });

  it('tracks state hash: computes checkpoint and threads prev/current hashes', async () => {
    // Empty listCollections -> computeStateCheckpoint loops zero collections
    // and returns a valid (empty-db) checkpoint, so newStateHash is set.
    const { db, cs } = makeDb({ listCollections: [] });
    db.collection('cd_articles').store.set('S1', { _id: 'S1', n: 1 });
    const error = vi.fn();
    await startDbChangeStream({
      db,
      nodeId: 'L1',
      trackStateHash: true,
      logger: { error },
    });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'insert',
      documentKey: { _id: 'S1' },
      fullDocument: { _id: 'S1', n: 1 },
      _id: { _data: 'rt-s' },
    });
    await flush(40);
    expect(error).not.toHaveBeenCalled();
    const ops = [...db.collection('sync_ops').store.values()];
    expect(ops).toHaveLength(1);
    // currentStateHash should be set (a string) once tracking succeeded.
    expect(typeof ops[0].currentStateHash).toBe('string');
  });

  it('logs a warning when computeStateCheckpoint throws (state-hash failure)', async () => {
    const { db, cs } = makeDb({ listCollectionsThrows: true });
    const warn = vi.fn();
    await startDbChangeStream({
      db,
      nodeId: 'L1',
      trackStateHash: true,
      logger: { warn, error: vi.fn() },
    });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'insert',
      documentKey: { _id: 'W1' },
      fullDocument: { _id: 'W1' },
      _id: { _data: 'rt-w' },
    });
    await flush();
    // computeStateCheckpoint needs db.listCollections which our fake lacks ->
    // it throws -> warn('Failed to compute new state hash').
    expect(warn).toHaveBeenCalledWith('Failed to compute new state hash');
  });

  it('logs an error when a serial-queue task rejects', async () => {
    const { db, cs } = makeDb();
    const error = vi.fn();
    // Force appendOp to throw all retries by making sync_local.findOne reject.
    const orig = db.collection;
    db.collection = (name: string) => {
      if (name === 'sync_local') {
        return {
          findOne: async () => {
            throw new Error('local read fail');
          },
          updateOne: async () => ({}),
        };
      }
      return orig(name);
    };
    await startDbChangeStream({ db, nodeId: 'L1', logger: { error, warn: vi.fn() } });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'insert',
      documentKey: { _id: 'Z1' },
      fullDocument: { _id: 'Z1' },
      _id: { _data: 'rt-z' },
    });
    await flush();
    expect(error).toHaveBeenCalledWith(
      expect.objectContaining({ err: expect.any(String) }),
      'serial queue task failed',
    );
  });

  it('forwards change-stream error events to the logger', async () => {
    const { db, cs } = makeDb();
    const error = vi.fn();
    await startDbChangeStream({ db, nodeId: 'L1', logger: { error } });
    cs.emit('error', new Error('stream broke'));
    expect(error).toHaveBeenCalledWith(
      { err: 'stream broke' },
      'change stream error',
    );
  });
});

describe('appendOp via change stream: retry + table persistence', () => {
  it('reuses an existing sync_ops ComponentsTable across two ops', async () => {
    const { db, cs } = makeDb();
    const bs = new BsMem();
    await startDbChangeStream({ db, nodeId: 'L1', bs });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'insert',
      documentKey: { _id: 'T1' },
      fullDocument: { _id: 'T1' },
      _id: { _data: 'r1' },
    });
    await flush();
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'insert',
      documentKey: { _id: 'T2' },
      fullDocument: { _id: 'T2' },
      _id: { _data: 'r2' },
    });
    await flush();
    const ops = [...db.collection('sync_ops').store.values()];
    expect(ops.map((o) => o._id).sort()).toEqual(['L1_1', 'L1_2']);
    // The sync_ops_meta blob now references a table with 2 rows.
    const meta = db.collection('sync_state').store.get('sync_ops_meta');
    expect(meta?.rowCount).toBe(2);
  });

  it('retries appendOp and logs a warn, then succeeds (saveSyncOpsTable fails once)', async () => {
    const { db, cs } = makeDb();
    const warn = vi.fn();
    let setBlobCalls = 0;
    // Wrap a real BsMem but make the FIRST setBlob (table save) throw to force
    // one retry, then succeed.
    const inner = new BsMem();
    const bs: any = {
      getBlob: (id: string) => inner.getBlob(id),
      setBlob: (buf: any) => {
        setBlobCalls++;
        // The first setBlob call is for the fullDocument blob (in the change
        // handler). The save path also calls setBlob. Throw on the 2nd call
        // (the table save of attempt 1) to trigger a retry.
        if (setBlobCalls === 2) throw new Error('blob save fail');
        return inner.setBlob(buf);
      },
    };
    await startDbChangeStream({ db, nodeId: 'L1', bs, logger: { warn, error: vi.fn() } });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'insert',
      documentKey: { _id: 'R1' },
      fullDocument: { _id: 'R1' },
      _id: { _data: 'rr' },
    });
    await flush();
    expect(warn).toHaveBeenCalledWith(
      expect.objectContaining({ attempt: 1 }),
      'error appending to sync_ops table; retrying',
    );
    // Eventually succeeded.
    const ops = [...db.collection('sync_ops').store.values()];
    expect(ops).toHaveLength(1);
  });

  it('loads a stale-meta table whose blob is missing -> treated as fresh', async () => {
    const { db, cs } = makeDb();
    const bs = new BsMem();
    // Pre-seed sync_ops_meta pointing at a blob that does not exist.
    db.collection('sync_state').store.set('sync_ops_meta', {
      _id: 'sync_ops_meta',
      componentsBlobId: 'does-not-exist',
    });
    await startDbChangeStream({ db, nodeId: 'L1', bs });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'insert',
      documentKey: { _id: 'M1' },
      fullDocument: { _id: 'M1' },
      _id: { _data: 'rm' },
    });
    await flush();
    const ops = [...db.collection('sync_ops').store.values()];
    expect(ops).toHaveLength(1);
  });

  it('exhausts all retries and surfaces the error to the queue catch', async () => {
    // loadSyncOpsTable's sync_state.findOne throws on EVERY attempt -> appendOp
    // retries MAX_RETRIES (5) times, warning on 1..4, then rethrows on attempt
    // 5. The queue catch logs "serial queue task failed".
    const { db, cs } = makeDb();
    const warn = vi.fn();
    const error = vi.fn();
    const orig = db.collection;
    db.collection = (name: string) => {
      if (name === 'sync_state') {
        return {
          findOne: async () => {
            throw new Error('meta read always fails');
          },
        };
      }
      return orig(name);
    };
    await startDbChangeStream({
      db,
      nodeId: 'L1',
      logger: { warn, error },
    });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'update', // no fullDocument -> no setBlob before appendOp
      documentKey: { _id: 'X9' },
      _id: { _data: 'rx' },
    });
    await flush(80);
    // Warned on attempts 1..4 (4 times), then the 5th attempt rethrows.
    expect(warn).toHaveBeenCalledTimes(4);
    expect(warn).toHaveBeenLastCalledWith(
      expect.objectContaining({ attempt: 4 }),
      'error appending to sync_ops table; retrying',
    );
    expect(error).toHaveBeenCalledWith(
      expect.objectContaining({ err: 'meta read always fails' }),
      'serial queue task failed',
    );
  });

  it('ignores a duplicate-_id insert into sync_ops (retry-safe insert .catch)', async () => {
    // Pre-seed sync_ops with the _id the first op will use (L1_1) so its
    // insertOne throws a duplicate-key error -> the .catch swallows it and the
    // op still completes (sync_local advances).
    const { db, cs } = makeDb();
    db.collection('sync_ops').store.set('L1_1', { _id: 'L1_1', stale: true });
    await startDbChangeStream({ db, nodeId: 'L1' });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'update',
      documentKey: { _id: 'D1' },
      _id: { _data: 'rd' },
    });
    await flush();
    // Insert was swallowed; the pre-seeded doc is still there...
    expect(db.collection('sync_ops').store.get('L1_1')).toMatchObject({
      stale: true,
    });
    // ...but sync_local still advanced to seq 1 (op processed without throwing).
    expect(db.collection('sync_local').store.get('local')?.seq).toBe(1);
  });

  it('falls back to GENESIS prevHash when sync_local.headHash is falsy', async () => {
    // Seed sync_local with a present-but-empty headHash to exercise the
    // `local.headHash || "GENESIS"` fallback (right-hand side).
    const { db, cs } = makeDb();
    db.collection('sync_local').store.set('local', {
      _id: 'local',
      seq: 0,
      headHash: '', // falsy -> GENESIS fallback
    });
    await startDbChangeStream({ db, nodeId: 'L1' });
    emitChange(cs, {
      ns: { db: 'caratdb', coll: 'cd_articles' },
      operationType: 'update',
      documentKey: { _id: 'G1' },
      _id: { _data: 'rg' },
    });
    await flush();
    const op = db.collection('sync_ops').store.get('L1_1');
    expect(op).toBeDefined();
    expect((op as any).prevHash).toBe('GENESIS');
  });
});
