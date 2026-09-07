// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Db } from '@rljson/db';
import { IoMem } from '@rljson/io';
import { Int32 } from 'bson';
import { describe, expect, it } from 'vitest';

import { MongoEditAdapter } from '../src/mongo-edit-adapter.ts';

const mkDb = async (): Promise<{ io: IoMem; db: Db }> => {
  const io = new IoMem();
  await io.init();
  await io.isReady();
  return { io, db: new Db(io) };
};

// The backfill pull path reads component rows out of the CONSUMER's Db. In live
// sync those rows resolve by hash through IoMulti → relay → the producer that
// published them. Here we model that hop by copying the producer's backfill
// table rows into the consumer's Db, exactly as `pullComponents` will read them.
const copyBackfillRows = async (
  fromIo: IoMem,
  toDb: Db,
  bfKey: string,
): Promise<void> => {
  const dump = (await fromIo.dumpTable({ table: bfKey })) as Record<
    string,
    { _type: string; _data: unknown[] }
  >;
  await toDb.core.import(
    { [bfKey]: { _type: 'components', _data: dump[bfKey]._data as never } },
    { validate: false },
  );
};

describe('MongoEditAdapter — manifest-level backfill (import/pullComponents)', () => {
  const prefix = 'p';
  const bfKey = 'customersBackfill';

  it('round-trips documents by content hash with BSON types intact', async () => {
    const p = await mkDb();
    const producer = new MongoEditAdapter(p.db, prefix);
    await producer.init(['customers']);

    const docs = [
      { _id: new Int32(2400042), name: 'Alice', n: new Int32(7) },
      { _id: 'string-id', name: 'Bob', nested: { when: new Date(0) } },
    ];
    const hashes = await producer.importComponents('customers', docs);
    expect(hashes).toHaveLength(2);
    expect(hashes.every((h) => typeof h === 'string' && h.length > 0)).toBe(
      true,
    );

    // Consumer pulls by hash.
    const c = await mkDb();
    const consumer = new MongoEditAdapter(c.db, prefix);
    await consumer.init(['customers']);
    await copyBackfillRows(p.io, c.db, bfKey);

    const pulled = await consumer.pullComponents('customers', hashes);
    expect(pulled).toHaveLength(2);
    // The Int32 _id survives the EJSON round-trip as an Int32, not a JS number.
    const byId = new Map(pulled.map((d) => [String(d['_id']), d]));
    const alice = byId.get('2400042')!;
    expect(alice['_id']).toBeInstanceOf(Int32);
    expect(alice['name']).toBe('Alice');
    expect(alice['n']).toBeInstanceOf(Int32);
    const bob = byId.get('string-id')!;
    expect((bob['nested'] as { when: Date }).when).toBeInstanceOf(Date);
  });

  it('is content-addressed: identical docs yield identical hashes', async () => {
    const a = await mkDb();
    const b = await mkDb();
    const pa = new MongoEditAdapter(a.db, prefix);
    const pb = new MongoEditAdapter(b.db, prefix);
    await pa.init(['customers']);
    await pb.init(['customers']);
    const doc = { _id: new Int32(1), v: 'same' };
    const [ha] = await pa.importComponents('customers', [doc]);
    const [hb] = await pb.importComponents('customers', [doc]);
    expect(ha).toBe(hb);
  });

  it('skips hashes that do not resolve on the consumer', async () => {
    const p = await mkDb();
    const producer = new MongoEditAdapter(p.db, prefix);
    await producer.init(['customers']);
    const [h] = await producer.importComponents('customers', [
      { _id: new Int32(1), v: 1 },
    ]);

    const c = await mkDb();
    const consumer = new MongoEditAdapter(c.db, prefix);
    await consumer.init(['customers']);
    await copyBackfillRows(p.io, c.db, bfKey);

    // One real hash + one that was never published: only the real one resolves.
    const pulled = await consumer.pullComponents('customers', [
      h,
      'deadbeef'.repeat(8),
    ]);
    expect(pulled).toHaveLength(1);
    expect(Number(pulled[0]['v'])).toBe(1);
  });

  it('returns empty for an unknown collection or empty input', async () => {
    const p = await mkDb();
    const adapter = new MongoEditAdapter(p.db, prefix);
    await adapter.init(['customers']);
    expect(await adapter.importComponents('unknown', [{ _id: 'x' }])).toEqual(
      [],
    );
    expect(await adapter.importComponents('customers', [])).toEqual([]);
    expect(await adapter.pullComponents('unknown', ['h'])).toEqual([]);
    expect(await adapter.pullComponents('customers', [])).toEqual([]);
  });
});
