// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { MongoScanner } from '../src/mongo-scanner.ts';

/**
 * Coverage spec for `MongoScanner`.
 *
 * All tests run against a lightweight fake `Db` and a stubbed `_converter` /
 * `_bs` so no real MongoDB, network, or filesystem is touched. Private
 * methods are reached via an `(instance as any)` cast, matching the style of
 * the existing scanner / agent specs.
 */
describe('MongoScanner (coverage)', () => {
  let warnSpy: ReturnType<typeof vi.spyOn>;
  let logSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    // Reset the once-per-process GC log guard so every test that hits
    // `_scanCollection` exercises the logging branch deterministically.
    (MongoScanner as any)._gcLogged = false;
  });

  afterEach(() => {
    warnSpy.mockRestore();
    logSpy.mockRestore();
    delete (globalThis as any).gc;
  });

  // ---------------------------------------------------------------------------
  // Fakes
  // ---------------------------------------------------------------------------

  /** Minimal fake Mongo Db. `collFactory` builds per-collection stubs. */
  const makeDb = (
    collFactory: (name: string) => any = () => ({}),
    listed: Array<{ name: string }> = [],
  ) =>
    ({
      databaseName: 'caratdb',
      collection: (n: string) => collFactory(n),
      listCollections: () => ({ toArray: async () => listed }),
    }) as any;

  /** Default converter stub: single-shot path returns a tiny table. */
  const smallConverter = () => ({
    discoverSchema: async () => ({ columns: [{ key: '_hash' }], _hash: 'h1' }),
    mergeTableCfg: (a: any) => a,
    convertCollection: async () => ({ _data: [{ _id: 1 }], _hash: 'c0' }),
  });

  // ---------------------------------------------------------------------------
  // constructor + accessors
  // ---------------------------------------------------------------------------

  it('constructor registers sync_ops and exposes bs / tree getters', () => {
    const s = new MongoScanner(makeDb());
    expect(s.bs).toBeTruthy();
    expect(s.tree).toBeNull();
    expect(s.getTableCfg('sync_ops')).toBeTruthy();
  });

  it('uses the injected bs when supplied via options', () => {
    const bs = { setBlob: async () => ({ blobId: 'x' }) } as any;
    const s = new MongoScanner(makeDb(), { bs });
    expect(s.bs).toBe(bs);
  });

  it('onChange registers a callback', () => {
    const s = new MongoScanner(makeDb()) as any;
    const cb = () => {};
    s.onChange(cb);
    expect(s._changeCallbacks).toContain(cb);
  });

  // ---------------------------------------------------------------------------
  // _shouldIgnore
  // ---------------------------------------------------------------------------

  it('_shouldIgnore: system + internal collections are ignored', () => {
    const s = new MongoScanner(makeDb()) as any;
    expect(s._shouldIgnore('system.profile')).toBe(true);
    expect(s._shouldIgnore('sync_ops')).toBe(true);
    expect(s._shouldIgnore('sync_recentChanges')).toBe(true);
  });

  it('_shouldIgnore: sync_tombstones is always scanned even with filters', () => {
    const s = new MongoScanner(makeDb(), {
      ignore: ['*'],
      include: ['nothing'],
    }) as any;
    expect(s._shouldIgnore('sync_tombstones')).toBe(false);
  });

  it('_shouldIgnore: ignore pattern matches', () => {
    const s = new MongoScanner(makeDb(), { ignore: ['tmp_*'] }) as any;
    expect(s._shouldIgnore('tmp_cache')).toBe(true);
    expect(s._shouldIgnore('cd_articles')).toBe(false);
  });

  it('_shouldIgnore: include filter excludes non-matching collections', () => {
    const s = new MongoScanner(makeDb(), { include: ['cd_*'] }) as any;
    expect(s._shouldIgnore('cd_articles')).toBe(false);
    expect(s._shouldIgnore('other')).toBe(true);
  });

  it('_shouldIgnore: no filters → kept', () => {
    const s = new MongoScanner(makeDb()) as any;
    expect(s._shouldIgnore('anything')).toBe(false);
  });

  // ---------------------------------------------------------------------------
  // _matchPattern
  // ---------------------------------------------------------------------------

  it('_matchPattern handles * and ? wildcards', () => {
    const s = new MongoScanner(makeDb()) as any;
    expect(s._matchPattern('cd_articles', 'cd_*')).toBe(true);
    expect(s._matchPattern('ab', 'a?')).toBe(true);
    expect(s._matchPattern('abc', 'a?')).toBe(false);
  });

  // ---------------------------------------------------------------------------
  // _scanCollection — small / single-shot path
  // ---------------------------------------------------------------------------

  it('_scanCollection small: emits one tree, with GC-unavailable log', async () => {
    const db = makeDb(() => ({
      estimatedDocumentCount: async () => 1,
    }));
    const s = new MongoScanner(db) as any;
    s._converter = smallConverter();
    s._bs = { setBlob: async () => ({ blobId: 'blob-A' }) };
    const trees = await s._scanCollection('sd_options', new Map());
    expect(trees).toHaveLength(1);
    expect(trees[0].meta.componentsBlobId).toBe('blob-A');
    // The once-per-process GC availability line was logged.
    expect(
      logSpy.mock.calls.some((c) => String(c[0]).includes('NOT AVAILABLE')),
    ).toBe(true);
  });

  it('_scanCollection: GC-available log branch when globalThis.gc is a fn', async () => {
    (globalThis as any).gc = () => {};
    const db = makeDb(() => ({ estimatedDocumentCount: async () => 1 }));
    const s = new MongoScanner(db) as any;
    s._converter = smallConverter();
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    await s._scanCollection('c', new Map());
    expect(
      logSpy.mock.calls.some((c) =>
        String(c[0]).includes('available (--expose-gc)'),
      ),
    ).toBe(true);
  });

  it('_scanCollection: GC log only happens once across calls', async () => {
    const db = makeDb(() => ({ estimatedDocumentCount: async () => 1 }));
    const s = new MongoScanner(db) as any;
    s._converter = smallConverter();
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    await s._scanCollection('a', new Map());
    const after1 = logSpy.mock.calls.filter((c) =>
      String(c[0]).includes('explicit GC'),
    ).length;
    await s._scanCollection('b', new Map());
    const after2 = logSpy.mock.calls.filter((c) =>
      String(c[0]).includes('explicit GC'),
    ).length;
    expect(after1).toBe(1);
    expect(after2).toBe(1);
  });

  it('_scanCollection small: JSON.stringify RangeError falls through to streaming', async () => {
    const db = makeDb(() => ({ estimatedDocumentCount: async () => 1 }));
    const s = new MongoScanner(db) as any;
    let streamed = false;
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    s._converter = {
      discoverSchema: async () => ({ columns: [{ key: '_hash' }], _hash: 'h1' }),
      mergeTableCfg: (a: any) => a,
      // A value whose JSON.stringify throws RangeError in the single-shot path.
      convertCollection: async () => ({
        _data: [{ _id: 1 }],
        _hash: 'c0',
        bad: {
          toJSON: () => {
            throw new RangeError('Invalid string length');
          },
        },
      }),
      convertCollectionStreaming: async function* () {
        streamed = true;
        yield { _data: [{ _id: 9 }], _hash: 'cstream' };
      },
    };
    const trees = await s._scanCollection('cd_articles', new Map());
    expect(streamed).toBe(true);
    expect(trees).toHaveLength(1);
    expect(warnSpy).toHaveBeenCalled();
  });

  it('_scanCollection small: non-RangeError from stringify is rethrown', async () => {
    const db = makeDb(() => ({ estimatedDocumentCount: async () => 1 }));
    const s = new MongoScanner(db) as any;
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    s._converter = {
      discoverSchema: async () => ({ columns: [{ key: '_hash' }], _hash: 'h1' }),
      mergeTableCfg: (a: any) => a,
      convertCollection: async () => ({
        _data: [{ _id: 1 }],
        _hash: 'c0',
        bad: {
          toJSON: () => {
            throw new TypeError('boom single-shot');
          },
        },
      }),
    };
    await expect(s._scanCollection('c', new Map())).rejects.toThrow(
      /boom single-shot/,
    );
  });

  // ---------------------------------------------------------------------------
  // _scanCollection — schema merge branches
  // ---------------------------------------------------------------------------

  it('_scanCollection: merges schema when discovered columns differ from cache', async () => {
    const db = makeDb(() => ({ estimatedDocumentCount: async () => 1 }));
    const s = new MongoScanner(db) as any;
    // Pre-seed a cached cfg with fewer columns to trigger the merge branch.
    s._tableConfigs.set('cd_x', { columns: [{ key: '_hash' }], _hash: 'old' });
    let merged = false;
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    s._converter = {
      discoverSchema: async () => ({
        columns: [{ key: '_hash' }, { key: 'name' }],
        _hash: 'new',
      }),
      mergeTableCfg: () => {
        merged = true;
        return { columns: [{ key: '_hash' }, { key: 'name' }], _hash: 'merged' };
      },
      convertCollection: async () => ({ _data: [{ _id: 1 }], _hash: 'c0' }),
    };
    await s._scanCollection('cd_x', new Map());
    expect(merged).toBe(true);
    expect(s.getTableCfg('cd_x')._hash).toBe('merged');
  });

  it('_scanCollection: merges when same column count but a key differs', async () => {
    const db = makeDb(() => ({ estimatedDocumentCount: async () => 1 }));
    const s = new MongoScanner(db) as any;
    s._tableConfigs.set('cd_z', { columns: [{ key: '_hash' }], _hash: 'old' });
    let merged = false;
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    s._converter = {
      // Same length (1) but a different key → some() predicate is true.
      discoverSchema: async () => ({ columns: [{ key: 'other' }], _hash: 'new' }),
      mergeTableCfg: () => {
        merged = true;
        return { columns: [{ key: '_hash' }], _hash: 'merged' };
      },
      convertCollection: async () => ({ _data: [{ _id: 1 }], _hash: 'c0' }),
    };
    await s._scanCollection('cd_z', new Map());
    expect(merged).toBe(true);
  });

  it('_scanCollection: uses cached cfg unchanged when schema matches', async () => {
    const db = makeDb(() => ({ estimatedDocumentCount: async () => 1 }));
    const s = new MongoScanner(db) as any;
    s._tableConfigs.set('cd_y', { columns: [{ key: '_hash' }], _hash: 'old' });
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    s._converter = {
      discoverSchema: async () => ({ columns: [{ key: '_hash' }], _hash: 'new' }),
      mergeTableCfg: () => {
        throw new Error('merge must not be called when schema matches');
      },
      convertCollection: async () => ({ _data: [{ _id: 1 }], _hash: 'c0' }),
    };
    await s._scanCollection('cd_y', new Map());
    expect(s.getTableCfg('cd_y')._hash).toBe('old');
  });

  // ---------------------------------------------------------------------------
  // _scanCollection — tombstone GC + estimatedCount failure
  // ---------------------------------------------------------------------------

  it('_scanCollection: sync_tombstones triggers a GC deleteMany', async () => {
    let deleteCalled = false;
    const db = makeDb(() => ({
      estimatedDocumentCount: async () => 1,
      deleteMany: async () => {
        deleteCalled = true;
        return {};
      },
    }));
    const s = new MongoScanner(db) as any;
    s._converter = smallConverter();
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    await s._scanCollection('sync_tombstones', new Map());
    expect(deleteCalled).toBe(true);
  });

  it('_scanCollection: tombstone GC swallows deleteMany errors (best-effort)', async () => {
    const db = makeDb(() => ({
      estimatedDocumentCount: async () => 1,
      deleteMany: async () => {
        throw new Error('gc failed');
      },
    }));
    const s = new MongoScanner(db) as any;
    s._converter = smallConverter();
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    const trees = await s._scanCollection('sync_tombstones', new Map());
    expect(trees).toHaveLength(1);
  });

  it('_scanCollection: estimatedDocumentCount failure forces streaming', async () => {
    const db = makeDb(() => ({
      estimatedDocumentCount: async () => {
        throw new Error('not authorized');
      },
    }));
    const s = new MongoScanner(db) as any;
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    let streamed = false;
    s._converter = {
      discoverSchema: async () => ({ columns: [], _hash: 'h1' }),
      mergeTableCfg: (a: any) => a,
      convertCollectionStreaming: async function* () {
        streamed = true;
        yield { _data: [{ _id: 1 }], _hash: 'c0' };
      },
    };
    const trees = await s._scanCollection('uncertain', new Map());
    expect(streamed).toBe(true);
    expect(trees).toHaveLength(1);
  });

  // ---------------------------------------------------------------------------
  // _scanCollection — streaming path branches
  // ---------------------------------------------------------------------------

  it('_scanCollection streaming: multiple chunks + final log', async () => {
    const db = makeDb(() => ({ estimatedDocumentCount: async () => 100_000 }));
    const s = new MongoScanner(db) as any;
    let n = 0;
    s._bs = { setBlob: async () => ({ blobId: `cb${n++}` }) };
    s._converter = {
      discoverSchema: async () => ({ columns: [], _hash: 'h1' }),
      mergeTableCfg: (a: any) => a,
      convertCollectionStreaming: async function* () {
        for (let i = 0; i < 3; i++) {
          yield { _data: [{ _id: i }], _hash: `c${i}` };
        }
      },
    };
    const trees = await s._scanCollection('cd_articles', new Map());
    expect(trees).toHaveLength(3);
    expect(trees.map((t: any) => t.meta.chunkIndex)).toEqual([0, 1, 2]);
    expect(
      logSpy.mock.calls.some((c) => String(c[0]).includes('streamed')),
    ).toBe(true);
  });

  it('_scanCollection streaming: GC hint invoked when globalThis.gc present', async () => {
    (globalThis as any).gc = vi.fn();
    const db = makeDb(() => ({ estimatedDocumentCount: async () => 100_000 }));
    const s = new MongoScanner(db) as any;
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    s._converter = {
      discoverSchema: async () => ({ columns: [], _hash: 'h1' }),
      mergeTableCfg: (a: any) => a,
      convertCollectionStreaming: async function* () {
        yield { _data: [{ _id: 1 }], _hash: 'c0' };
      },
    };
    await s._scanCollection('cd_articles', new Map());
    expect((globalThis as any).gc).toHaveBeenCalled();
  });

  it('_scanCollection streaming: oversized chunk RangeError dropped, continues', async () => {
    const db = makeDb(() => ({ estimatedDocumentCount: async () => 100_000 }));
    const s = new MongoScanner(db) as any;
    let n = 0;
    s._bs = { setBlob: async () => ({ blobId: `b${n++}` }) };
    s._converter = {
      discoverSchema: async () => ({ columns: [], _hash: 'h1' }),
      mergeTableCfg: (a: any) => a,
      convertCollectionStreaming: async function* () {
        yield { _data: [{ _id: 'a' }], _hash: 'c0' };
        yield {
          _data: [{ _id: 'b' }],
          _hash: 'c1',
          bad: {
            toJSON: () => {
              throw new RangeError('Invalid string length');
            },
          },
        };
        yield { _data: [{ _id: 'c' }], _hash: 'c2' };
      },
    };
    const trees = await s._scanCollection('cd_models', new Map());
    expect(trees).toHaveLength(2);
    expect(warnSpy).toHaveBeenCalledTimes(1);
  });

  it('_scanCollection streaming: non-RangeError chunk error rethrown', async () => {
    const db = makeDb(() => ({ estimatedDocumentCount: async () => 100_000 }));
    const s = new MongoScanner(db) as any;
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    s._converter = {
      discoverSchema: async () => ({ columns: [], _hash: 'h1' }),
      mergeTableCfg: (a: any) => a,
      convertCollectionStreaming: async function* () {
        yield {
          _data: [{ _id: 1 }],
          _hash: 'c0',
          bad: {
            toJSON: () => {
              throw new TypeError('boom stream');
            },
          },
        };
      },
    };
    await expect(s._scanCollection('c', new Map())).rejects.toThrow(
      /boom stream/,
    );
  });

  it('_scanCollection streaming: empty stream returns []', async () => {
    const db = makeDb(() => ({ estimatedDocumentCount: async () => 100_000 }));
    const s = new MongoScanner(db) as any;
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    s._converter = {
      discoverSchema: async () => ({ columns: [], _hash: 'h1' }),
      mergeTableCfg: (a: any) => a,
      // Yields nothing → chunkTrees stays empty.
      convertCollectionStreaming: async function* () {},
    };
    const trees = await s._scanCollection('empty', new Map());
    expect(trees).toEqual([]);
  });

  // ---------------------------------------------------------------------------
  // scan() end-to-end
  // ---------------------------------------------------------------------------

  it('scan: builds a root tree, skips ignored collections', async () => {
    const db = makeDb(
      () => ({ estimatedDocumentCount: async () => 1 }),
      [{ name: 'system.views' }, { name: 'cd_articles' }],
    );
    const s = new MongoScanner(db) as any;
    s._converter = smallConverter();
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    const tree = await s.scan();
    expect(tree.rootHash).toBeTruthy();
    const root = s.getRootTree();
    expect(root.meta.type).toBe('database');
    expect(root.children).toHaveLength(1); // system.views skipped
    expect(s.tree).toBe(tree);
  });

  it('scan(dirty): re-reads only dirty collections, reuses the cache for the rest', async () => {
    const db = makeDb(
      () => ({ estimatedDocumentCount: async () => 1 }),
      [{ name: 'customers' }, { name: 'items' }],
    );
    const s = new MongoScanner(db) as any;
    s._converter = smallConverter();
    s._bs = { setBlob: async () => ({ blobId: 'b' }) };
    // Full scan first → populates the per-collection cache for both.
    const full = await s.scan();
    expect(full.rootHash).toBeTruthy();
    // Incremental: only 'customers' is dirty → 'items' served from the cache,
    // so _scanCollection runs for 'customers' but NOT 'items'.
    const spy = vi.spyOn(s, '_scanCollection');
    const inc = await s.scan(new Set(['customers']));
    expect(inc.rootHash).toBeTruthy();
    const scanned = spy.mock.calls.map((c: any[]) => c[0]);
    expect(scanned).toContain('customers');
    expect(scanned).not.toContain('items');
    // A full scan (no dirty arg) re-reads everything again — backward compat.
    spy.mockClear();
    await s.scan();
    const scannedFull = spy.mock.calls.map((c: any[]) => c[0]);
    expect(scannedFull).toContain('customers');
    expect(scannedFull).toContain('items');
    spy.mockRestore();
  });

  // ---------------------------------------------------------------------------
  // blob + table cfg helpers
  // ---------------------------------------------------------------------------

  it('getComponentsTable round-trips through bs', async () => {
    const s = new MongoScanner(makeDb()) as any;
    const payload = { _data: [{ _id: 1 }], _hash: 'h' };
    s._bs = {
      getBlob: async () => ({
        content: Buffer.from(JSON.stringify(payload), 'utf-8'),
      }),
    };
    const got = await s.getComponentsTable('blob-1');
    expect(got).toEqual(payload);
  });

  it('createTablesCfgTable + save + load + getTableCfgByHash', async () => {
    const s = new MongoScanner(makeDb()) as any;
    const table = s.createTablesCfgTable();
    expect(Array.isArray(table._data)).toBe(true);
    // sync_ops was registered in the constructor.
    expect(table._data.length).toBeGreaterThan(0);

    const store = new Map<string, Buffer>();
    s._bs = {
      setBlob: async (buf: Buffer) => {
        store.set('id1', buf);
        return { blobId: 'id1' };
      },
      getBlob: async (id: string) => ({ content: store.get(id) }),
    };
    const blobId = await s.saveTablesCfgTable(table);
    expect(blobId).toBe('id1');
    const loaded = await s.loadTablesCfgTable(blobId);
    expect(loaded._data.length).toBe(table._data.length);

    const someHash = table._data[0]._hash;
    expect(s.getTableCfgByHash(loaded, someHash)).toBeTruthy();
    expect(s.getTableCfgByHash(loaded, 'no-such-hash')).toBeUndefined();
  });

  it('getAllTableCfgs returns a copy; addTableCfg mutates the cache', () => {
    const s = new MongoScanner(makeDb()) as any;
    const before = s.getAllTableCfgs();
    expect(before.has('sync_ops')).toBe(true);
    s.addTableCfg('newcoll', { columns: [], _hash: 'z' });
    expect(s.getTableCfg('newcoll')._hash).toBe('z');
    // The previously-returned map must be an independent copy.
    expect(before.has('newcoll')).toBe(false);
  });

  it('getRootTree returns null before scan', () => {
    const s = new MongoScanner(makeDb()) as any;
    expect(s.getRootTree()).toBeNull();
  });

  it('getRootTree returns null when rootHash missing from the tree map', () => {
    const s = new MongoScanner(makeDb()) as any;
    // _tree set but the rootHash points at an entry that is not present →
    // the `|| null` defensive fallback fires.
    s._tree = { rootHash: 'ghost', trees: new Map() };
    expect(s.getRootTree()).toBeNull();
  });

  it('getTableCfg returns undefined for unknown collection', () => {
    const s = new MongoScanner(makeDb()) as any;
    expect(s.getTableCfg('never-seen')).toBeUndefined();
  });
});
