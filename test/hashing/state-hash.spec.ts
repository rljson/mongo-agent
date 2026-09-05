// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.

import { describe, expect, it } from 'vitest';

import {
  computeStateCheckpoint,
  docLeafHash,
  getLatestCheckpoint,
  type MerklePartition,
} from '../../src/hashing/state-hash.ts';

/**
 * Tests for the merkle-tree state-hash checkpoint engine.
 *
 * No live MongoDB: we drive `computeStateCheckpoint` / `getLatestCheckpoint`
 * with a hand-rolled in-memory fake `Db` whose collections honour exactly the
 * subset of the driver API the source touches (find/findOne/sort/limit/
 * project/projection/toArray, async iteration, replaceOne+upsert, insertOne,
 * deleteMany, listCollections). That keeps the test deterministic and lets us
 * assert on the resulting partition/checkpoint documents.
 */

// ---------------------------------------------------------------------------
// In-memory fake Mongo
// ---------------------------------------------------------------------------

type AnyDoc = Record<string, unknown> & { _id: any };

function applyProjection(doc: AnyDoc, projection?: Record<string, 0 | 1>): AnyDoc {
  if (!projection) return doc;
  const keys = Object.keys(projection);
  // _id is included by default unless explicitly excluded.
  const out: AnyDoc = { _id: doc._id } as AnyDoc;
  for (const k of keys) {
    if (projection[k] === 1 && k !== '_id' && k in doc) {
      (out as any)[k] = (doc as any)[k];
    }
  }
  if ('_id' in projection && (projection as any)._id === 0) {
    delete (out as any)._id;
  }
  return out;
}

function cmp(a: any, b: any): number {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

function matches(doc: AnyDoc, filter: Record<string, any>): boolean {
  for (const [key, cond] of Object.entries(filter)) {
    const val = (doc as any)[key];
    if (cond !== null && typeof cond === 'object' && !Array.isArray(cond)) {
      for (const [op, operand] of Object.entries(cond)) {
        if (op === '$gte' && !(cmp(val, operand) >= 0)) return false;
        else if (op === '$lte' && !(cmp(val, operand) <= 0)) return false;
        else if (op === '$gt' && !(cmp(val, operand) > 0)) return false;
        else if (op === '$lt' && !(cmp(val, operand) < 0)) return false;
        else if (op === '$exists') {
          const has = (doc as any)[key] !== undefined;
          if (has !== operand) return false;
        } else if (op === '$in') {
          if (!(operand as any[]).some((o) => cmp(o, val) === 0)) return false;
        } else if (op.startsWith('$')) {
          // unknown operator — ignore
        } else {
          // nested equality — not used here
          if (val !== cond) return false;
        }
      }
    } else {
      if (cmp(val, cond) !== 0) return false;
    }
  }
  return true;
}

class FakeCursor {
  private docs: AnyDoc[];
  private _sort: Record<string, 1 | -1> | null = null;
  private _limit: number | null = null;
  private _projection: Record<string, 0 | 1> | undefined;

  constructor(docs: AnyDoc[], opts?: { sort?: any; projection?: any; limit?: number }) {
    this.docs = docs;
    if (opts?.sort) this._sort = opts.sort;
    if (opts?.projection) this._projection = opts.projection;
    if (typeof opts?.limit === 'number') this._limit = opts.limit;
  }

  sort(s: Record<string, 1 | -1>): this {
    this._sort = s;
    return this;
  }
  limit(n: number): this {
    this._limit = n;
    return this;
  }
  project(p: Record<string, 0 | 1>): this {
    this._projection = p;
    return this;
  }

  private resolve(): AnyDoc[] {
    let out = [...this.docs];
    if (this._sort) {
      const entries = Object.entries(this._sort);
      out.sort((a, b) => {
        for (const [k, dir] of entries) {
          const c = cmp((a as any)[k], (b as any)[k]);
          if (c !== 0) return dir === 1 ? c : -c;
        }
        return 0;
      });
    }
    if (this._limit != null) out = out.slice(0, this._limit);
    return out.map((d) => applyProjection(d, this._projection));
  }

  async toArray(): Promise<AnyDoc[]> {
    return this.resolve();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<AnyDoc> {
    for (const d of this.resolve()) {
      yield d;
    }
  }
}

class FakeCollection {
  docs: AnyDoc[] = [];

  find(filter: Record<string, any> = {}, opts?: { sort?: any; projection?: any; batchSize?: number }): FakeCursor {
    const matched = this.docs.filter((d) => matches(d, filter));
    return new FakeCursor(matched, opts);
  }

  async findOne(
    filter: Record<string, any> = {},
    opts?: { projection?: any; sort?: any },
  ): Promise<AnyDoc | null> {
    let matched = this.docs.filter((d) => matches(d, filter));
    if (opts?.sort) {
      const entries = Object.entries(opts.sort) as [string, 1 | -1][];
      matched = [...matched].sort((a, b) => {
        for (const [k, dir] of entries) {
          const c = cmp((a as any)[k], (b as any)[k]);
          if (c !== 0) return dir === 1 ? c : -c;
        }
        return 0;
      });
    }
    const doc = matched[0];
    if (!doc) return null;
    return applyProjection(doc, opts?.projection);
  }

  async replaceOne(
    filter: { _id: any },
    replacement: AnyDoc,
    opts?: { upsert?: boolean },
  ): Promise<{ acknowledged: true }> {
    const i = this.docs.findIndex((d) => cmp(d._id, filter._id) === 0);
    if (i >= 0) this.docs[i] = { ...replacement };
    else if (opts?.upsert) this.docs.push({ ...replacement });
    return { acknowledged: true };
  }

  async insertOne(doc: AnyDoc): Promise<{ acknowledged: true }> {
    this.docs.push({ ...doc });
    return { acknowledged: true };
  }

  async deleteMany(filter: Record<string, any>): Promise<{ deletedCount: number }> {
    const before = this.docs.length;
    this.docs = this.docs.filter((d) => !matches(d, filter));
    return { deletedCount: before - this.docs.length };
  }

  async updateOne(): Promise<{ acknowledged: true }> {
    return { acknowledged: true };
  }
}

class FakeDb {
  private colls = new Map<string, FakeCollection>();

  collection(name: string): any {
    let c = this.colls.get(name);
    if (!c) {
      c = new FakeCollection();
      this.colls.set(name, c);
    }
    return c;
  }

  seed(name: string, docs: AnyDoc[]): void {
    const c = this.collection(name) as FakeCollection;
    c.docs = docs.map((d) => ({ ...d }));
  }

  listCollections(): { toArray: () => Promise<{ name: string }[]> } {
    const names = [...this.colls.keys()];
    return { toArray: async () => names.map((name) => ({ name })) };
  }
}

const newDb = (): FakeDb => new FakeDb();

// ---------------------------------------------------------------------------
// docLeafHash
// ---------------------------------------------------------------------------

describe('docLeafHash', () => {
  it('returns null for null / undefined doc', () => {
    expect(docLeafHash(null)).toBeNull();
    expect(docLeafHash(undefined)).toBeNull();
  });

  it('prefers the stored __h field verbatim', () => {
    expect(docLeafHash({ _id: 'x', __h: 'cafe', a: 1 } as any)).toBe('cafe');
  });

  it('stringifies a non-string __h field', () => {
    expect(docLeafHash({ _id: 'x', __h: 1234 as any })).toBe('1234');
  });

  it('computes a deterministic integrity hash when __h is absent', () => {
    const a = docLeafHash({ _id: 'x', n: 1 });
    const b = docLeafHash({ _id: 'x', n: 1 });
    expect(a).toBe(b);
    expect(a).toMatch(/^[0-9a-f]{64}$/);
  });

  it('ignores __h presence vs absence in the computed hash', () => {
    // __h is stripped before hashing the remaining fields, so a doc that
    // *would* be hashed (no __h) must equal a hash computed from the same
    // payload with a bogus __h removed.
    const withBogus = docLeafHash({ _id: 'x', n: 1, __h: '' } as any);
    const without = docLeafHash({ _id: 'x', n: 1 });
    expect(withBogus).toBe(without);
  });
});

// ---------------------------------------------------------------------------
// getLatestCheckpoint
// ---------------------------------------------------------------------------

describe('getLatestCheckpoint', () => {
  it('returns the most recent checkpoint by ts', async () => {
    const db = newDb();
    db.seed('state_checkpoints', [
      { _id: 'a', ts: 100 },
      { _id: 'b', ts: 300 },
      { _id: 'c', ts: 200 },
    ]);
    const cp = await getLatestCheckpoint(db as any);
    expect(cp?._id).toBe('b');
  });

  it('returns null when there are no checkpoints', async () => {
    const db = newDb();
    const cp = await getLatestCheckpoint(db as any);
    expect(cp).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// computeStateCheckpoint — FULL mode
// ---------------------------------------------------------------------------

describe('computeStateCheckpoint: full mode', () => {
  it('scans collections, partitions docs, writes merkle + checkpoint', async () => {
    const db = newDb();
    // Two data collections out of order — sorted by name => books before users.
    db.seed('users', [
      { _id: 3, name: 'c' },
      { _id: 1, name: 'a' },
      { _id: 2, name: 'b' },
    ]);
    db.seed('books', [{ _id: 'k1', t: 'x' }]);
    // Bookkeeping collections that MUST be skipped by prefix.
    db.seed('sync_recentChanges', [{ _id: 1 }]);
    db.seed('state_merkle', []);
    db.seed('state_dirty', []);
    db.seed('rljson_x', [{ _id: 1 }]);
    db.seed('system.profile', [{ _id: 1 }]);

    const cp = await computeStateCheckpoint({
      db: db as any,
      partitionSize: 2,
      mode: 'full',
    });

    expect(cp.mode).toBe('full');
    expect(cp.partitionSize).toBe(2);
    expect(Object.keys(cp.collections).sort()).toEqual(['books', 'users']);
    // users: 3 docs, partitionSize 2 => 2 partitions; books: 1 doc => 1.
    expect(cp.collections.users.partitions).toBe(2);
    expect(cp.collections.books.partitions).toBe(1);
    expect(cp.dbRoot).toMatch(/^[0-9a-f]{64}$/);

    // Merkle partitions were persisted.
    const merkle = (db.collection('state_merkle') as any).docs as MerklePartition[];
    const userParts = merkle.filter((m) => m.coll === 'users');
    expect(userParts).toHaveLength(2);
    expect(userParts.find((p) => p.idx === 0)?.count).toBe(2);
    expect(userParts.find((p) => p.idx === 1)?.count).toBe(1);

    // Checkpoint was inserted.
    const cps = (db.collection('state_checkpoints') as any).docs;
    expect(cps).toHaveLength(1);
    expect(cps[0]._id).toBe(cp._id);
  });

  it('respects the ignoredColls set', async () => {
    const db = newDb();
    db.seed('users', [{ _id: 1 }]);
    db.seed('secret', [{ _id: 1 }]);
    const cp = await computeStateCheckpoint({
      db: db as any,
      mode: 'full',
      ignoredColls: new Set(['secret']),
    });
    expect(Object.keys(cp.collections)).toEqual(['users']);
  });

  it('uses the {_id,__h} projection fast-path when the sampled doc has __h', async () => {
    const db = newDb();
    // All docs carry __h, so docLeafHash uses the stored value.
    db.seed('cat', [
      { _id: 1, __h: 'h1', big: 'ignored-by-projection' },
      { _id: 2, __h: 'h2', big: 'ignored' },
    ]);
    const cp = await computeStateCheckpoint({ db: db as any, mode: 'full' });
    expect(cp.collections.cat.partitions).toBe(1);
    const merkle = (db.collection('state_merkle') as any).docs as MerklePartition[];
    expect(merkle).toHaveLength(1);
    expect(merkle[0].count).toBe(2);
  });

  it('skips an empty collection partition flush (no merkle rows)', async () => {
    const db = newDb();
    db.seed('empty', []);
    const cp = await computeStateCheckpoint({ db: db as any, mode: 'full' });
    expect(cp.collections.empty.partitions).toBe(0);
    const merkle = (db.collection('state_merkle') as any).docs as MerklePartition[];
    expect(merkle).toHaveLength(0);
  });

  it('deletes stale merkle partitions beyond the new count when the collection shrank', async () => {
    const db = newDb();
    db.seed('users', [{ _id: 1 }]);
    // Pre-existing extra partitions from a larger past scan.
    db.seed('state_merkle', [
      { _id: 'users::p0', coll: 'users', idx: 0, root: 'old' },
      { _id: 'users::p5', coll: 'users', idx: 5, root: 'old' },
      { _id: 'users::p6', coll: 'users', idx: 6, root: 'old' },
    ]);
    await computeStateCheckpoint({ db: db as any, mode: 'full', partitionSize: 50 });
    const merkle = (db.collection('state_merkle') as any).docs as MerklePartition[];
    const users = merkle.filter((m) => m.coll === 'users');
    // Only the freshly-written p0 survives; p5/p6 (idx >= 1) were deleted.
    expect(users.map((m) => m.idx).sort()).toEqual([0]);
  });
});

// ---------------------------------------------------------------------------
// computeStateCheckpoint — INCREMENTAL mode
// ---------------------------------------------------------------------------

describe('computeStateCheckpoint: incremental mode', () => {
  it('falls back to full scan when a FULL dirty marker is present', async () => {
    const db = newDb();
    db.seed('users', [{ _id: 1 }, { _id: 2 }]);
    db.seed('state_dirty', [
      { _id: 'users::FULL', coll: 'users', full: true, dirtyAt: 'x' },
    ]);
    const cp = await computeStateCheckpoint({ db: db as any, mode: 'incremental' });
    expect(cp.mode).toBe('incremental');
    // Full rescan path persisted a partition.
    const merkle = (db.collection('state_merkle') as any).docs as MerklePartition[];
    expect(merkle.filter((m) => m.coll === 'users')).toHaveLength(1);
    // Dirty markers cleared.
    expect((db.collection('state_dirty') as any).docs).toHaveLength(0);
  });

  it('falls back to full scan when there is no cached merkle partition', async () => {
    const db = newDb();
    db.seed('users', [{ _id: 1 }]);
    db.seed('state_dirty', []); // not full-dirty
    db.seed('state_merkle', []); // no cache
    const cp = await computeStateCheckpoint({ db: db as any, mode: 'incremental' });
    expect(cp.collections.users.partitions).toBe(1);
  });

  it('reuses cached roots for clean partitions and recomputes dirty middle partitions', async () => {
    const db = newDb();
    // 3 partitions worth of docs (partitionSize 1): _id 1, 2, 3.
    db.seed('users', [{ _id: 1 }, { _id: 2 }, { _id: 3 }]);
    db.seed('state_merkle', [
      { _id: 'users::p0', coll: 'users', idx: 0, minId: 1, maxId: 1, count: 1, root: 'CACHED0', ts: 1, updatedAt: 'x' },
      { _id: 'users::p1', coll: 'users', idx: 1, minId: 2, maxId: 2, count: 1, root: 'CACHED1', ts: 1, updatedAt: 'x' },
      { _id: 'users::p2', coll: 'users', idx: 2, minId: 3, maxId: 3, count: 1, root: 'CACHED2', ts: 1, updatedAt: 'x' },
    ]);
    // Only the middle partition (idx 1, not the last) is dirty.
    db.seed('state_dirty', [
      { _id: 'users::p1', coll: 'users', partition: 1, dirtyAt: 'x' },
    ]);

    await computeStateCheckpoint({ db: db as any, mode: 'incremental' });

    const merkle = (db.collection('state_merkle') as any).docs as MerklePartition[];
    // p0 + p2 keep their cached roots untouched (not rewritten in this run).
    expect(merkle.find((m) => m.idx === 0)?.root).toBe('CACHED0');
    expect(merkle.find((m) => m.idx === 2)?.root).toBe('CACHED2');
    // p1 was recomputed (root no longer the placeholder).
    const p1 = merkle.find((m) => m.idx === 1)!;
    expect(p1.root).not.toBe('CACHED1');
    expect(p1.root).toMatch(/^[0-9a-f]{64}$/);
    // Dirty cleared.
    expect((db.collection('state_dirty') as any).docs).toHaveLength(0);
  });

  it('recomputes the LAST partition open-ended so appended docs are included', async () => {
    const db = newDb();
    // Cache only knows about _id 1 in the last partition; doc _id 2 was appended.
    db.seed('users', [{ _id: 1 }, { _id: 2 }]);
    db.seed('state_merkle', [
      { _id: 'users::p0', coll: 'users', idx: 0, minId: 1, maxId: 1, count: 1, root: 'CACHED0', ts: 1, updatedAt: 'x' },
    ]);
    db.seed('state_dirty', [
      { _id: 'users::p0', coll: 'users', partition: 0, dirtyAt: 'x' },
    ]);

    await computeStateCheckpoint({ db: db as any, mode: 'incremental' });

    const merkle = (db.collection('state_merkle') as any).docs as MerklePartition[];
    const p0 = merkle.find((m) => m.idx === 0)!;
    // Open-ended rescan ($gte minId, no upper bound) picked up _id 2.
    expect(p0.count).toBe(2);
    expect(p0.maxId).toBe(2);
  });

  it('keeps the cached root when a dirty partition becomes empty', async () => {
    const db = newDb();
    // Partition p0 covers _id 1; p1 is dirty but its id range now has no docs.
    db.seed('users', [{ _id: 1 }]);
    db.seed('state_merkle', [
      { _id: 'users::p0', coll: 'users', idx: 0, minId: 1, maxId: 1, count: 1, root: 'CACHED0', ts: 1, updatedAt: 'x' },
      { _id: 'users::p1', coll: 'users', idx: 1, minId: 100, maxId: 200, count: 1, root: 'CACHED1', ts: 1, updatedAt: 'x' },
    ]);
    // Mark the NON-last partition p0 dirty (range 1..1, still has doc) AND p1.
    // p1 is the last partition (open-ended >= 100) → no docs → empty branch.
    db.seed('state_dirty', [
      { _id: 'users::p1', coll: 'users', partition: 1, dirtyAt: 'x' },
    ]);

    await computeStateCheckpoint({ db: db as any, mode: 'incremental' });

    const merkle = (db.collection('state_merkle') as any).docs as MerklePartition[];
    // p1 stayed at its cached root because the recompute found zero docs.
    expect(merkle.find((m) => m.idx === 1)?.root).toBe('CACHED1');
  });
});
