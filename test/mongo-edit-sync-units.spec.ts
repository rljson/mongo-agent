// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Db } from '@rljson/db';
import { IoMem } from '@rljson/io';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { docHash } from '../src/mongo-component-codec.ts';
import { bucketOf } from '../src/mongo-manifest-hash.ts';
import type { EditSyncConnector } from '../src/mongo-edit-sync.ts';
import { MongoEditSync } from '../src/mongo-edit-sync.ts';

/**
 * DETERMINISTIC unit cover for the anti-entropy host surface.
 *
 * The mesh specs drive these same methods, but only through real timers and a
 * whole backfill round — so whether a given branch runs at all depends on how
 * the round happens to be paced on the day. That made coverage of this file
 * swing by a full point between identical runs and sit right on the gate.
 * These tests call each method directly with the state it branches on, so the
 * branches are covered by construction rather than by timing.
 */

/** A Mongo collection fake that also answers the reads the AE host makes. */
class Coll {
  stream = { on: () => this, close: vi.fn(async () => {}) };
  bulkWrite = vi.fn(async () => ({}));
  updateOne = vi.fn(async () => ({}));
  deleteMany = vi.fn(async () => ({ deletedCount: 0 }));
  constructor(public docs: Array<Record<string, unknown>> = []) {}
  /** Snapshot read (async-iterated) AND `$in` read (`toArray`). */
  find(filter?: { _id?: { $in?: unknown[] } }): AsyncIterable<
    Record<string, unknown>
  > & { toArray: () => Promise<Array<Record<string, unknown>>> } {
    const ids = filter?._id?.$in;
    const docs = ids
      ? this.docs.filter((d) => ids.some((i) => String(i) === String(d['_id'])))
      : this.docs;
    return {
      toArray: async () => docs,
      async *[Symbol.asyncIterator]() {
        for (const d of docs) yield d;
      },
    };
  }
  watch(): { on: () => unknown; close: () => Promise<void> } {
    return this.stream as never;
  }
}

class MongoDbFake {
  constructor(public cols: Record<string, Coll>) {}
  collection(name: string): Coll {
    return (this.cols[name] ??= new Coll());
  }
}

const mkConnector = (): EditSyncConnector & { send: ReturnType<typeof vi.fn> } =>
  ({
    send: vi.fn(),
    reannounce: vi.fn(),
    invalidateReceived: vi.fn(),
    listen: () => {},
  }) as never;

const COLL = 'customers';

/** A started sync over `docs`, with its privates reachable. */
const mkSync = async (
  docs: Array<Record<string, unknown>> = [],
): Promise<{
  sync: MongoEditSync;
  priv: Record<string, never>;
  mongo: MongoDbFake;
  conn: ReturnType<typeof mkConnector>;
}> => {
  const mongo = new MongoDbFake({ [COLL]: new Coll(docs) });
  const io = new IoMem();
  await io.init();
  await io.isReady();
  const conn = mkConnector();
  const sync = new MongoEditSync(
    mongo as never,
    new Db(io),
    conn,
    [COLL],
    'p',
  );
  await sync.start();
  return {
    sync,
    priv: sync as unknown as Record<string, never>,
    mongo,
    conn,
  };
};

describe('MongoEditSync — anti-entropy host surface', () => {
  let stop: (() => Promise<void>) | undefined;
  beforeEach(() => {
    process.env['SL_EDIT_ROOT_DEBOUNCE_MS'] = '5';
  });
  afterEach(async () => {
    await stop?.();
    stop = undefined;
    vi.restoreAllMocks();
    delete process.env['SL_EDIT_ROOT_DEBOUNCE_MS'];
    delete process.env['SL_EDIT_ANTIENTROPY'];
    delete process.env['SL_EDIT_TOMBSTONE_LOG'];
  });

  describe('_bucketEntries', () => {
    it('advertises a tombstone as an empty-hash entry, and a live doc still wins', async () => {
      const { sync, priv } = await mkSync();
      stop = () => sync.stop();
      const p = priv as unknown as {
        _manifestOf: (c: string) => Map<string, string>;
        _tombstones: Map<string, Map<string, unknown>>;
        _bucketEntries: (
          c: string,
          b: number[],
        ) => Map<number, Array<[string, string]>>;
      };
      p._manifestOf(COLL).set('live', 'h-live');
      // 'live' is tombstoned AND live — the live doc must win; 'dead' is only
      // tombstoned, so it goes out as the empty-hash entry that converges a
      // peer still holding it.
      p._tombstones.set(
        COLL,
        new Map<string, unknown>([
          ['dead', 1],
          ['live', 2],
        ]),
      );
      const buckets = [bucketOf('live'), bucketOf('dead')];
      const out = p._bucketEntries(COLL, buckets);
      const all = [...out.values()].flat();
      expect(all).toContainEqual(['dead', '']);
      expect(all).toContainEqual(['live', 'h-live']);
      expect(all).not.toContainEqual(['live', '']);
    });

    it('leaves out a tombstone whose bucket the peer did not ask for', async () => {
      const { sync, priv } = await mkSync();
      stop = () => sync.stop();
      const p = priv as unknown as {
        _tombstones: Map<string, Map<string, unknown>>;
        _bucketEntries: (
          c: string,
          b: number[],
        ) => Map<number, Array<[string, string]>>;
      };
      p._tombstones.set(COLL, new Map<string, unknown>([['dead', 1]]));
      // Any bucket but the one 'dead' hashes into.
      const other = (bucketOf('dead') + 1) % 4096;
      const out = p._bucketEntries(COLL, [other]);
      expect([...out.values()].flat()).toEqual([]);
    });
  });

  describe('_serveComponents', () => {
    it('returns nothing when Mongo holds none of the asked ids', async () => {
      const { sync, priv } = await mkSync();
      stop = () => sync.stop();
      const p = priv as unknown as {
        _serveComponents: (c: string, ids: string[]) => Promise<string[]>;
      };
      expect(await p._serveComponents(COLL, ['404'])).toEqual([]);
    });

    it('publishes the asked docs as components and hands back their hashes', async () => {
      const { sync, priv } = await mkSync([{ _id: 1, name: 'A' }]);
      stop = () => sync.stop();
      const p = priv as unknown as {
        _serveComponents: (c: string, ids: string[]) => Promise<string[]>;
      };
      const hashes = await p._serveComponents(COLL, ['1']);
      expect(hashes).toHaveLength(1);
      expect(hashes[0]).toMatch(/^[A-Za-z0-9_-]{22}$/);
    });
  });

  describe('_pullAndApply', () => {
    it('skips a doc whose exact content the manifest already holds', async () => {
      const doc = { _id: 1, name: 'A' };
      const { sync, priv, mongo } = await mkSync([doc]);
      stop = () => sync.stop();
      const p = priv as unknown as {
        _serveComponents: (c: string, ids: string[]) => Promise<string[]>;
        _pullAndApply: (c: string, h: string[]) => Promise<number>;
        _manifestOf: (c: string) => Map<string, string>;
      };
      const hashes = await p._serveComponents(COLL, ['1']);
      p._manifestOf(COLL).set('1', docHash(doc as never));
      mongo.cols[COLL].bulkWrite.mockClear();
      // Resolved over the read path, but nothing to write: an idempotent pull
      // must not re-upsert (and so must not drive the change stream).
      expect(await p._pullAndApply(COLL, hashes)).toBe(1);
      expect(mongo.cols[COLL].bulkWrite).not.toHaveBeenCalled();
    });
  });

  describe('_pushTombstones', () => {
    it('re-asserts the ids it has a tombstone for and ignores the rest', async () => {
      const { sync, priv, conn } = await mkSync();
      stop = () => sync.stop();
      const p = priv as unknown as {
        _tombstones: Map<string, Map<string, unknown>>;
        _pushTombstones: (c: string, ids: string[]) => Promise<void>;
      };
      p._tombstones.set(COLL, new Map<string, unknown>([['7', 7]]));
      conn.send.mockClear();
      await p._pushTombstones(COLL, ['7', 'not-deleted-here']);
      const refs = conn.send.mock.calls.map((c) => c[0] as string);
      expect(refs.some((r) => r.startsWith(`${COLL}:`))).toBe(true);
    });

    it('sends nothing when it holds no tombstone for any asked id', async () => {
      const { sync, priv, conn } = await mkSync();
      stop = () => sync.stop();
      const p = priv as unknown as {
        _pushTombstones: (c: string, ids: string[]) => Promise<void>;
      };
      conn.send.mockClear();
      await p._pushTombstones(COLL, ['nope']);
      expect(
        conn.send.mock.calls.filter((c) =>
          (c[0] as string).startsWith(`${COLL}:`),
        ),
      ).toEqual([]);
    });
  });

  describe('_maybeTriggerAe', () => {
    it('does not arm a round while the cold start is still running', async () => {
      const { sync, priv } = await mkSync();
      stop = () => sync.stop();
      const p = priv as unknown as {
        _coldStartComplete: boolean;
        _aeCooldownUntil: Map<string, number>;
        _maybeTriggerAe: (c: string) => void;
      };
      p._coldStartComplete = false;
      p._aeCooldownUntil.clear();
      p._maybeTriggerAe(COLL);
      expect(p._aeCooldownUntil.size).toBe(0);
    });

    it('does not arm a round for a collection whose baseline is not ready', async () => {
      const { sync, priv } = await mkSync();
      stop = () => sync.stop();
      const p = priv as unknown as {
        _coldStartComplete: boolean;
        _baselineReady: Set<string>;
        _aeCooldownUntil: Map<string, number>;
        _maybeTriggerAe: (c: string) => void;
      };
      p._coldStartComplete = true;
      p._baselineReady.delete(COLL);
      p._aeCooldownUntil.clear();
      p._maybeTriggerAe(COLL);
      expect(p._aeCooldownUntil.size).toBe(0);
    });
  });

  describe('delete clears echo suppression', () => {
    it('re-inserting the SAME id with the SAME content still broadcasts', async () => {
      // The live failure, in order: a peer's doc is applied here, the doc is
      // deleted locally, and the next run re-inserts the identical document.
      // The stale suppression entry made `_onChange` call that insert an echo,
      // so the head was never sent and the doc existed on one node only —
      // `doc e2eProbe/990900002 not found on [every peer]`, every run.
      const doc = { _id: 1, name: 'x', v: 'v1' };
      const { sync, priv, conn } = await mkSync([]);
      stop = () => sync.stop();
      const p = priv as unknown as {
        _appliedHash: Map<string, string>;
        _key: (c: string, id: unknown) => string;
        _onDelete: (c: string, change: Record<string, unknown>) => void;
        _onChange: (c: string, change: Record<string, unknown>) => Promise<void>;
      };
      const key = p._key(COLL, 1);

      // 1. applied from a peer
      p._appliedHash.set(key, docHash(doc as never));
      // 2. deleted locally (NOT an echo — no peer tombstone was applied)
      p._onDelete(COLL, { documentKey: { _id: 1 } });
      expect(p._appliedHash.has(key)).toBe(false);

      // 3. re-inserted with byte-identical content
      conn.send.mockClear();
      await p._onChange(COLL, { operationType: 'insert', fullDocument: doc });

      // 4. the head goes out, so peers can converge
      const refs = conn.send.mock.calls.map((c) => c[0] as string);
      expect(refs.some((r) => r.startsWith(`${COLL}:`))).toBe(true);
    });

    it('still treats a real peer-delete echo as an echo', async () => {
      const { sync, priv, conn } = await mkSync([]);
      stop = () => sync.stop();
      const p = priv as unknown as {
        _appliedHash: Map<string, string>;
        _key: (c: string, id: unknown) => string;
        _tombstone: (id: unknown) => Record<string, unknown>;
        _pendingDeletes: Map<string, Set<unknown>>;
        _onDelete: (c: string, change: Record<string, unknown>) => void;
      };
      const key = p._key(COLL, 2);
      // A delete we applied because a PEER deleted it must still not be
      // re-propagated — pruning the entry must not cost us that.
      p._appliedHash.set(key, docHash(p._tombstone(2) as never));
      conn.send.mockClear();
      p._onDelete(COLL, { documentKey: { _id: 2 } });
      expect(p._appliedHash.has(key)).toBe(false);
      // Echoes return before the delete is queued for propagation.
      expect(p._pendingDeletes.get(COLL)?.has(2) ?? false).toBe(false);
    });
  });

  describe('_loadTombstones', () => {
    it('is a no-op when the tombstone log is switched off', async () => {
      process.env['SL_EDIT_TOMBSTONE_LOG'] = '0';
      const { sync, priv } = await mkSync();
      stop = () => sync.stop();
      const p = priv as unknown as {
        _tombstones: Map<string, Map<string, unknown>>;
        _loadTombstones: (c: string) => Promise<void>;
      };
      p._tombstones.delete(COLL);
      await p._loadTombstones(COLL);
      expect(p._tombstones.has(COLL)).toBe(false);
    });

    it('creates the map when there is none yet and skips a row without an id', async () => {
      const { sync, priv, mongo } = await mkSync();
      stop = () => sync.stop();
      const p = priv as unknown as {
        _tombstones: Map<string, Map<string, unknown>>;
        _loadTombstones: (c: string) => Promise<void>;
      };
      mongo.collection('sl_edit_tombstones').docs = [
        { collection: COLL, id: 5 },
        { collection: COLL }, // a malformed row must not become a tombstone
      ];
      p._tombstones.delete(COLL);
      await p._loadTombstones(COLL);
      expect([...(p._tombstones.get(COLL) as Map<string, unknown>).keys()]).toEqual(
        ['5'],
      );
    });
  });
});
