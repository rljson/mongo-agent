// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Db } from '@rljson/db';
import { IoMem } from '@rljson/io';
import { Int32 } from 'bson';
import type { Document } from 'mongodb';
import { beforeEach, describe, expect, it } from 'vitest';

import { MongoCakeAdapter } from '../src/mongo-cake-adapter.ts';
import { sliceIdOf } from '../src/mongo-cake-model.ts';
import {
  MongoCakeSync,
  type MongoStore,
} from '../src/mongo-cake-sync.ts';

// .............................................................................

/** A minimal in-memory MongoStore: collection → (sliceId → document). */
class FakeMongo implements MongoStore {
  readonly data = new Map<string, Map<string, Document>>();
  readonly reads: Record<string, number> = {};

  constructor(seed: Record<string, Document[]> = {}) {
    for (const [name, docs] of Object.entries(seed)) {
      const coll = new Map<string, Document>();
      for (const d of docs) coll.set(sliceIdOf(d['_id']), d);
      this.data.set(name, coll);
    }
  }

  async listCollections(): Promise<string[]> {
    return [...this.data.keys()];
  }
  async readCollection(name: string): Promise<Document[]> {
    this.reads[name] = (this.reads[name] ?? 0) + 1;
    return [...(this.data.get(name)?.values() ?? [])];
  }
  async countDocuments(name: string): Promise<number> {
    return this.data.get(name)?.size ?? 0;
  }
  async applyChanges(
    name: string,
    upserts: Document[],
    deletes: unknown[],
  ): Promise<void> {
    let coll = this.data.get(name);
    if (!coll) {
      coll = new Map();
      this.data.set(name, coll);
    }
    for (const d of upserts) coll.set(sliceIdOf(d['_id']), d);
    for (const id of deletes) coll.delete(sliceIdOf(id));
  }

  ids(name: string): unknown[] {
    return [...(this.data.get(name)?.values() ?? [])]
      .map((d) => Number((d['_id'] as Int32).valueOf?.() ?? d['_id']))
      .sort();
  }
}

const newAdapter = async (): Promise<MongoCakeAdapter> => {
  const io = new IoMem();
  await io.init();
  await io.isReady();
  return new MongoCakeAdapter(new Db(io));
};

const lenient = { maxDeleteFraction: 0.95 } as const; // don't trip on tiny colls

// .............................................................................

describe('MongoCakeSync', () => {
  let adapter: MongoCakeAdapter;

  beforeEach(async () => {
    adapter = await newAdapter();
  });

  it('propagates inserts producer → consumer', async () => {
    const a = new FakeMongo({
      customers: [{ _id: new Int32(1), name: 'Ann' }, { _id: new Int32(2), name: 'Bo' }],
    });
    const b = new FakeMongo();
    const producer = new MongoCakeSync(a, adapter, lenient);
    const consumer = new MongoCakeSync(b, adapter, lenient);

    const ref = await producer.pushSnapshot();
    expect(ref).not.toBeNull();
    expect(producer.lastSentHash).toBe(ref);
    const res = await consumer.applyIncoming(ref!);

    expect(res?.upserted).toBe(2);
    expect(res?.deleted).toBe(0);
    expect(b.ids('customers')).toEqual([1, 2]);
  });

  it('propagates a delete by content-absence (no tombstones)', async () => {
    const seed = {
      customers: [{ _id: new Int32(1), name: 'Ann' }, { _id: new Int32(2), name: 'Bo' }],
    };
    const a = new FakeMongo(seed);
    const b = new FakeMongo(seed); // b already in sync with a
    const producer = new MongoCakeSync(a, adapter, lenient);
    const consumer = new MongoCakeSync(b, adapter, lenient);

    // Prime both sides' "previous cake" so the diff is incremental.
    await consumer.applyIncoming((await producer.pushSnapshot())!);

    // Producer deletes _id:2, then pushes.
    a.data.get('customers')!.delete(sliceIdOf(new Int32(2)));
    const ref = await producer.pushSnapshot();
    const res = await consumer.applyIncoming(ref!);

    expect(res?.deleted).toBe(1);
    expect(res?.blockedDeletes).toEqual([]);
    expect(b.ids('customers')).toEqual([1]); // _id:2 gone, no tombstone
  });

  it('suppresses an echo of the node’s own push', async () => {
    const a = new FakeMongo({ customers: [{ _id: new Int32(1) }] });
    const producer = new MongoCakeSync(a, adapter, lenient);
    const ref = await producer.pushSnapshot();
    expect(await producer.applyIncoming(ref!)).toBeNull();
  });

  it('returns null from pushSnapshot when nothing changed', async () => {
    const a = new FakeMongo({ customers: [{ _id: new Int32(1) }] });
    const producer = new MongoCakeSync(a, adapter, lenient);
    expect(await producer.pushSnapshot()).not.toBeNull();
    expect(await producer.pushSnapshot()).toBeNull(); // identical snapshot
  });

  it('only touches the changed collection (incremental apply)', async () => {
    const seed = {
      customers: [{ _id: new Int32(1), name: 'Ann' }],
      items: [{ _id: new Int32(9), sku: 'X' }],
    };
    const a = new FakeMongo(seed);
    const b = new FakeMongo(seed);
    const producer = new MongoCakeSync(a, adapter, lenient);
    const consumer = new MongoCakeSync(b, adapter, lenient);
    await consumer.applyIncoming((await producer.pushSnapshot())!);

    a.data.get('customers')!.set(sliceIdOf(new Int32(2)), { _id: new Int32(2), name: 'Bo' });
    const res = await consumer.applyIncoming((await producer.pushSnapshot())!);

    expect(res?.changed).toEqual(['customers']); // items untouched
    expect(b.ids('customers')).toEqual([1, 2]);
  });

  it('blocks a mass-delete (circuit-breaker) and keeps the data', async () => {
    const docs = Array.from({ length: 10 }, (_v, i) => ({ _id: new Int32(i), n: i }));
    const a = new FakeMongo({ customers: docs });
    const b = new FakeMongo({ customers: docs });
    // Default 30% breaker.
    const producer = new MongoCakeSync(a, adapter);
    const consumer = new MongoCakeSync(b, adapter);
    await consumer.applyIncoming((await producer.pushSnapshot())!);

    // Producer wipes the whole collection, then pushes.
    a.data.get('customers')!.clear();
    const res = await consumer.applyIncoming((await producer.pushSnapshot())!);

    expect(res?.blockedDeletes).toEqual(['customers']);
    expect(res?.deleted).toBe(0);
    expect(b.data.get('customers')!.size).toBe(10); // data preserved
  });

  it('treats a collection that vanished from the cake as a full delete', async () => {
    const seed = {
      customers: [{ _id: new Int32(1) }],
      items: [{ _id: new Int32(9) }, { _id: new Int32(10) }],
    };
    const a = new FakeMongo(seed);
    const b = new FakeMongo(seed);
    // Breaker effectively off (fraction > 1) so the 100% delete is applied —
    // exercising the vanished-collection path itself. (The circuit-breaker test
    // above proves that under normal settings such a wipe is instead blocked.)
    const off = { maxDeleteFraction: 2 } as const;
    const producer = new MongoCakeSync(a, adapter, off);
    const consumer = new MongoCakeSync(b, adapter, off);
    await consumer.applyIncoming((await producer.pushSnapshot())!);

    a.data.delete('items'); // collection removed entirely on the producer
    const res = await consumer.applyIncoming((await producer.pushSnapshot())!);

    expect(res?.changed).toEqual(['items']);
    expect(res?.deleted).toBe(2);
    expect(b.data.get('items')!.size).toBe(0);
  });

  it('returns null for an unresolvable incoming cake hash', async () => {
    const b = new FakeMongo();
    const consumer = new MongoCakeSync(b, adapter, lenient);
    expect(await consumer.applyIncoming('deadbeefdeadbeefdeadbe')).toBeNull();
  });

  it('incremental push rebuilds only the dirty collection, reuses the cache', async () => {
    const a = new FakeMongo({
      customers: [{ _id: new Int32(1) }],
      items: [{ _id: new Int32(9) }],
    });
    const producer = new MongoCakeSync(a, adapter, lenient);
    await producer.pushSnapshot(); // full: reads both once
    expect(a.reads['customers']).toBe(1);
    expect(a.reads['items']).toBe(1);

    // Change customers only; push with dirty={customers}.
    a.data.get('customers')!.set(sliceIdOf(new Int32(2)), { _id: new Int32(2) });
    const ref = await producer.pushSnapshot(new Set(['customers']));
    expect(ref).not.toBeNull();
    expect(a.reads['customers']).toBe(2); // re-read
    expect(a.reads['items']).toBe(1); // reused from cache — NOT re-read
  });

  it('evicts a collection dropped from Mongo out of the layer cache', async () => {
    const seed = {
      customers: [{ _id: new Int32(1) }],
      items: [{ _id: new Int32(9) }, { _id: new Int32(10) }],
    };
    const a = new FakeMongo(seed);
    const b = new FakeMongo(seed);
    const off = { maxDeleteFraction: 2 } as const;
    const producer = new MongoCakeSync(a, adapter, off);
    const consumer = new MongoCakeSync(b, adapter, off);
    await consumer.applyIncoming((await producer.pushSnapshot())!);

    a.data.delete('items'); // collection gone → cache entry must be evicted
    const res = await consumer.applyIncoming((await producer.pushSnapshot())!);
    expect(res?.changed).toEqual(['items']);
    expect(b.data.get('items')!.size).toBe(0);
  });
});
