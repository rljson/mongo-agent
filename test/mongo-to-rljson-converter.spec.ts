// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.

import { describe, expect, it, vi } from 'vitest';

import type { TableCfg } from '@rljson/rljson';

import { MongoToRljsonConverter } from '../src/mongo-to-rljson-converter.ts';

/**
 * Coverage spec for MongoToRljsonConverter. No live MongoDB: we feed the
 * converter lightweight fake `Collection` objects whose `find()` returns a
 * canned cursor. All four metrics on the source file are exercised with real
 * assertions (no behavioural change to the source).
 */

// A minimal TableCfg the converter only reads `_hash`/`columns`/flags off of.
const makeCfg = (overrides: Partial<TableCfg> = {}): TableCfg =>
  ({
    key: 'things',
    type: 'components',
    columns: [{ key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'Hash' }],
    isHead: false,
    isRoot: false,
    isShared: true,
    _hash: 'cfg-hash',
    ...overrides,
  } as TableCfg);

// Fake cursor for the `find().limit().toArray()` (convertCollection) path.
const makeArrayCollection = (docs: any[], collectionName = 'things') => {
  let limited: number | undefined;
  const cursor = {
    limit(n: number) {
      limited = n;
      return cursor;
    },
    async toArray() {
      return limited === undefined ? docs : docs.slice(0, limited);
    },
  };
  return {
    collectionName,
    find: vi.fn(() => cursor),
  } as any;
};

// Fake streaming cursor for convertCollectionStreaming (hasNext/next/close).
const makeStreamCollection = (docs: any[], opts: { closeThrows?: boolean } = {}) => {
  let i = 0;
  let closed = false;
  const cursor = {
    async hasNext() {
      return i < docs.length;
    },
    async next() {
      return docs[i++];
    },
    async close() {
      closed = true;
      if (opts.closeThrows) throw new Error('close failed');
    },
    get _closed() {
      return closed;
    },
  };
  return {
    collection: {
      collectionName: 'things',
      find: vi.fn(() => cursor),
    } as any,
    cursor,
  };
};

describe('MongoToRljsonConverter.discoverSchema', () => {
  it('returns a minimal hashed TableCfg keyed by the collection name', async () => {
    const conv = new MongoToRljsonConverter();
    const cfg = await conv.discoverSchema({ collectionName: 'widgets' } as any, 7);
    expect(cfg.key).toBe('widgets');
    expect(cfg.type).toBe('components');
    expect(cfg.columns).toHaveLength(1);
    expect(cfg.columns[0].key).toBe('_hash');
    expect(cfg.isShared).toBe(true);
    expect(typeof cfg._hash).toBe('string');
    expect(cfg._hash).not.toBe(''); // hip() filled it
  });
});

describe('MongoToRljsonConverter.convertCollection', () => {
  it('converts all docs without a limit (single chunk, no yield)', async () => {
    const conv = new MongoToRljsonConverter();
    const coll = makeArrayCollection([{ _id: 'a', n: 1 }, { _id: 'b', n: 2 }]);
    const table = await conv.convertCollection(coll, makeCfg());
    expect(coll.find).toHaveBeenCalledTimes(1);
    expect(table._type).toBe('components');
    expect(table._tableCfg).toBe('cfg-hash');
    expect(table._data).toHaveLength(2);
    expect(table._data[0]._id).toBe('a');
    expect(typeof table._data[0]._hash).toBe('string');
    expect(table._hash).not.toBe('');
  });

  it('applies the limit via cursor.limit()', async () => {
    const conv = new MongoToRljsonConverter();
    const coll = makeArrayCollection([{ _id: 'a' }, { _id: 'b' }, { _id: 'c' }]);
    const table = await conv.convertCollection(coll, makeCfg(), 2);
    expect(table._data).toHaveLength(2);
    expect(table._data.map((d: any) => d._id)).toEqual(['a', 'b']);
  });

  it('chunks large collections and yields to the event loop between chunks', async () => {
    const conv = new MongoToRljsonConverter();
    // 1001 docs forces the CHUNK=500 loop to run >1 iteration and hit the
    // `end < docs.length` setImmediate yield branch (twice).
    const docs = Array.from({ length: 1001 }, (_, k) => ({ _id: `x${k}`, n: k }));
    const coll = makeArrayCollection(docs);
    const setImmediateSpy = vi.spyOn(global, 'setImmediate');
    try {
      const table = await conv.convertCollection(coll, makeCfg());
      expect(table._data).toHaveLength(1001);
      expect(table._data[1000]._id).toBe('x1000');
      // 1001 docs => chunks at 0, 500, 1000; yields after first two only.
      expect(setImmediateSpy).toHaveBeenCalledTimes(2);
    } finally {
      setImmediateSpy.mockRestore();
    }
  });
});

describe('MongoToRljsonConverter.convertCollectionStreaming', () => {
  it('yields a chunk once the buffer fills and a final partial chunk', async () => {
    const conv = new MongoToRljsonConverter();
    const docs = Array.from({ length: 5 }, (_, k) => ({ _id: `d${k}` }));
    const { collection, cursor } = makeStreamCollection(docs);
    const out: any[] = [];
    for await (const t of conv.convertCollectionStreaming(collection, makeCfg(), 2)) {
      out.push(t);
    }
    // chunkSize 2 over 5 docs => [2][2] full chunks + [1] remainder.
    expect(out).toHaveLength(3);
    expect(out[0]._data).toHaveLength(2);
    expect(out[1]._data).toHaveLength(2);
    expect(out[2]._data).toHaveLength(1);
    expect(out[0]._type).toBe('components');
    expect(out[0]._tableCfg).toBe('cfg-hash');
    expect(cursor._closed).toBe(true); // finally ran
  });

  it('emits nothing for an empty collection but still closes the cursor', async () => {
    const conv = new MongoToRljsonConverter();
    const { collection, cursor } = makeStreamCollection([]);
    const out: any[] = [];
    for await (const t of conv.convertCollectionStreaming(collection, makeCfg(), 3)) {
      out.push(t);
    }
    expect(out).toHaveLength(0);
    expect(cursor._closed).toBe(true);
  });

  it('breaks when next() returns a falsy doc', async () => {
    const conv = new MongoToRljsonConverter();
    // hasNext() is true but next() yields null -> the `if (!doc) break` path.
    const cursor = {
      _calls: 0,
      async hasNext() {
        return true;
      },
      async next() {
        return null;
      },
      _closed: false,
      async close() {
        (this as any)._closed = true;
      },
    };
    const collection = { collectionName: 'things', find: vi.fn(() => cursor) } as any;
    const out: any[] = [];
    for await (const t of conv.convertCollectionStreaming(collection, makeCfg(), 2)) {
      out.push(t);
    }
    expect(out).toHaveLength(0);
    expect(cursor._closed).toBe(true);
  });

  it('swallows an error thrown by cursor.close() in the finally block', async () => {
    const conv = new MongoToRljsonConverter();
    const { collection } = makeStreamCollection([{ _id: 'a' }], { closeThrows: true });
    const out: any[] = [];
    // The .catch(() => {}) on cursor.close() must absorb the throw.
    await expect(
      (async () => {
        for await (const t of conv.convertCollectionStreaming(collection, makeCfg(), 5)) {
          out.push(t);
        }
      })(),
    ).resolves.toBeUndefined();
    expect(out).toHaveLength(1);
  });
});

describe('MongoToRljsonConverter.convertDocument', () => {
  it('strips an existing `_hash`, re-hashes, and is order-independent', () => {
    const conv = new MongoToRljsonConverter();
    const a = conv.convertDocument({ _id: 'x', a: 1, b: 2, _hash: 'stale' } as any, makeCfg());
    const b = conv.convertDocument({ b: 2, a: 1, _id: 'x' } as any, makeCfg());
    expect(a._hash).toBe(b._hash); // stale hash ignored, key order ignored
    expect(a._id).toBe('x');
    expect(typeof a._hash).toBe('string');
  });

  it('JSON-roundtrips BSON-ish values (toJSON) so @rljson/hash can walk them', () => {
    const conv = new MongoToRljsonConverter();
    const objectIdLike = { toJSON: () => 'deadbeefdeadbeefdeadbeef' };
    const date = new Date('2026-05-15T10:00:00.000Z');
    const row = conv.convertDocument({ _id: objectIdLike, t: date } as any, makeCfg());
    expect(row._id).toBe('deadbeefdeadbeefdeadbeef'); // collapsed to JSON form
    expect(row.t).toBe('2026-05-15T10:00:00.000Z'); // Date -> ISO string
    expect(typeof row._hash).toBe('string');
  });
});

describe('MongoToRljsonConverter.mergeTableCfg', () => {
  const col = (key: string) =>
    ({ key, type: 'string', titleLong: key, titleShort: key } as any);

  it('unions columns, keeps existing on key clash, and sorts with _hash first', () => {
    const conv = new MongoToRljsonConverter();
    const existing = makeCfg({
      columns: [col('_hash'), { ...col('name'), type: 'string', _existing: true } as any],
      isHead: true,
      isRoot: false,
      isShared: true,
    });
    const incoming = makeCfg({
      // `name` clashes (existing wins); `age` and `addr` are new.
      columns: [{ ...col('name'), type: 'number' } as any, col('age'), col('addr')],
    });
    const merged = conv.mergeTableCfg(existing, incoming);
    const keys = merged.columns.map((c) => c.key);
    expect(keys[0]).toBe('_hash'); // _hash sorts first
    expect(keys).toEqual(['_hash', 'addr', 'age', 'name']); // rest alphabetical
    // existing `name` column type wins over incoming's number type.
    const name = merged.columns.find((c) => c.key === 'name')!;
    expect(name.type).toBe('string');
    // flags carried over from `existing`.
    expect(merged.isHead).toBe(true);
    expect(merged.key).toBe('things');
    expect(merged._hash).not.toBe('');
  });

  it('handles the b._hash branch of the sort comparator', () => {
    const conv = new MongoToRljsonConverter();
    // Put a non-_hash column first and _hash later in `existing` so the
    // comparator must return +1 for the `b.key === '_hash'` case.
    const existing = makeCfg({ columns: [col('zeta'), col('_hash')] });
    const incoming = makeCfg({ columns: [] as any });
    const merged = conv.mergeTableCfg(existing, incoming);
    expect(merged.columns.map((c) => c.key)).toEqual(['_hash', 'zeta']);
  });
});
