// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Db } from '@rljson/db';
import { hip } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { EJSON, Int32 } from 'bson';
import { describe, expect, it } from 'vitest';

import { docHash } from '../src/mongo-component-codec.ts';
import {
  compareTimeId,
  MongoEditAdapter,
} from '../src/mongo-edit-adapter.ts';

const mkDb = async (): Promise<{ io: IoMem; db: Db }> => {
  const io = new IoMem();
  await io.init();
  await io.isReady();
  return { io, db: new Db(io) };
};

const editTables = (cakeKey: string): string[] => [
  `${cakeKey}Edits`,
  `${cakeKey}MultiEdits`,
  `${cakeKey}EditHistory`,
];

// Simulate the decentralized pull: copy the edit-chain rows the receiver would
// pull by hash from the producer into the receiver's Db.
const pullEditRows = async (
  fromIo: IoMem,
  toDb: Db,
  tables: string[],
): Promise<void> => {
  const payload: Record<string, { _type: string; _data: unknown[] }> = {};
  for (const t of tables) {
    const dump = (await fromIo.dumpTable({ table: t })) as Record<
      string,
      { _type: string; _data: unknown[] }
    >;
    if (dump?.[t]?._data?.length) {
      payload[t] = { _type: dump[t]._type, _data: dump[t]._data };
    }
  }
  if (Object.keys(payload).length > 0) await toDb.core.import(payload);
};

const canon = (v: unknown): string => EJSON.stringify(v, { relaxed: false });

describe('MongoEditAdapter', () => {
  const prefix = 'testDb';
  const collections = ['customers', 'items'];

  it('round-trips putComponents per collection, byte-perfect', async () => {
    const p = await mkDb();
    const producer = new MongoEditAdapter(p.db, prefix);
    await producer.init(collections);

    const d1 = { _id: new Int32(1), name: 'Alice', age: new Int32(30) };
    const d2 = { _id: 'i1', qty: new Int32(5), tags: ['a', 'b'] };
    const d3 = { _id: new Int32(2), name: 'Bob' };
    await producer.putDoc('customers', d1);
    const items = await producer.putDoc('items', d2);
    const cust = await producer.putDoc('customers', d3);
    const custHead = cust?.head;
    const itemsHead = items?.head;
    expect(custHead).toBeTruthy();
    expect(itemsHead).toBeTruthy();
    // Every edit carries the timeId the whole fleet orders it by.
    expect(cust?.timeId).toMatch(/^\d+:.{4}$/);
    expect(producer.headRef('customers')).toBe(custHead);
    expect(producer.headRef('items')).toBe(itemsHead);

    const c = await mkDb();
    const consumer = new MongoEditAdapter(c.db, prefix);
    await consumer.init(collections);
    await pullEditRows(p.io, c.db, [
      ...editTables(producer.cakeKey('customers')),
      ...editTables(producer.cakeKey('items')),
    ]);

    const custRes = await consumer.collectPuts('customers', custHead as string);
    const itemRes = await consumer.collectPuts('items', itemsHead as string);
    expect(custRes.complete).toBe(true);
    expect(itemRes.complete).toBe(true);
    const custPuts = custRes.puts;
    const itemPuts = itemRes.puts;
    expect(custPuts.length).toBe(2);
    expect(itemPuts.length).toBe(1);

    const byKey: Record<string, unknown> = {};
    for (const put of [...custPuts, ...itemPuts]) {
      byKey[`${put.collection}/${put.sliceId}`] = put.doc;
    }
    expect(canon(byKey['customers/1'])).toBe(canon(d1));
    expect(canon(byKey['customers/2'])).toBe(canon(d3));
    expect(canon(byKey['items/i1'])).toBe(canon(d2));
  });

  it('collectPuts(collection, headRef, sinceRef) returns only the newer edits', async () => {
    const p = await mkDb();
    const producer = new MongoEditAdapter(p.db, prefix);
    await producer.init(collections);
    const h1 = (await producer.putDoc('customers', { _id: new Int32(1), name: 'A' }))?.head;
    const h2 = (await producer.putDoc('customers', { _id: new Int32(2), name: 'B' }))?.head;

    const c = await mkDb();
    const consumer = new MongoEditAdapter(c.db, prefix);
    await consumer.init(collections);
    await pullEditRows(p.io, c.db, editTables(producer.cakeKey('customers')));

    const incremental = await consumer.collectPuts(
      'customers',
      h2 as string,
      h1 as string,
    );
    expect(incremental.complete).toBe(true);
    expect(incremental.puts.length).toBe(1);
    expect(incremental.puts[0].sliceId).toBe('2');
    expect((incremental.puts[0].doc as { name: string }).name).toBe('B');
    expect(incremental.puts[0].timeId).toMatch(/^\d+:.{4}$/);
    // Only the head is safe to remember: its ancestry ends at the stop ref.
    expect(incremental.sealed).toEqual([h2]);

    // A SET of stop refs works the same way — that is what a receiver holding
    // several peers' lineages passes in.
    const viaSet = await consumer.collectPuts(
      'customers',
      h2 as string,
      new Set([h1 as string]),
    );
    expect(viaSet.puts.map((put) => put.sliceId)).toEqual(['2']);
  });

  it('key helpers are inverse; headRef/putDoc handle unknown collections', async () => {
    const p = await mkDb();
    const adapter = new MongoEditAdapter(p.db, prefix);
    await adapter.init(['customers']);
    expect(adapter.layerKey('customers')).toBe('customersLayer');
    expect(adapter.collectionForLayer('customersLayer')).toBe('customers');
    expect(adapter.collectionForLayer('noSuffix')).toBe('noSuffix');
    // Keys are sanitized to valid lowerCamelCase: prefix lowercased,
    // underscore-separated collection parts camel-joined.
    expect(adapter.cakeKey('customers')).toBe('testdbCustomersCake');
    expect(adapter.layerKey('fv_contact_group')).toBe('fvContactGroupLayer');
    expect(adapter.cakeKey('pd_plannings')).toBe('testdbPdPlanningsCake');
    // An all-symbol prefix sanitizes to the fallback 'c'.
    const symAdapter = new MongoEditAdapter(p.db, '___');
    expect(symAdapter.cakeKey('customers')).toBe('cCustomersCake');
    // unknown collection -> null / empty, never throws
    expect(adapter.headRef('unknown')).toBe(null);
    expect(await adapter.putDoc('unknown', { _id: 1 })).toBe(null);
    // An unknown collection is "complete" (nothing to resolve), not a gap.
    expect(await adapter.collectPuts('unknown', 'x')).toEqual({
      puts: [],
      complete: true,
      sealed: [],
    });
    // Nor does it have anything to seed a timeId map with.
    expect(await adapter.latestTimeIds('unknown', new Map(), 10)).toEqual(
      new Map(),
    );
  });

  it('collectPuts flags an unresolvable head as an incomplete chain', async () => {
    const p = await mkDb();
    const producer = new MongoEditAdapter(p.db, prefix);
    await producer.init(['customers']);
    // The head row itself is missing → the walk cannot see any ancestor, so the
    // result is empty AND partial (complete === false), which the sync layer
    // must treat like an empty pull (re-pull, never latch over the hole).
    const res = await producer.collectPuts('customers', 'doesNotExist00000000');
    expect(res.puts).toEqual([]);
    expect(res.complete).toBe(false);
  });

  it('collectPuts flags a missing multiEdit / edit row as incomplete', async () => {
    const p = await mkDb();
    const producer = new MongoEditAdapter(p.db, prefix);
    await producer.init(['customers']);
    const head = (await producer.putDoc('customers', { _id: new Int32(1), name: 'A' }))
      ?.head;
    const ck = producer.cakeKey('customers');

    // Pulled ONLY the EditHistory rows: the head history row resolves, but its
    // multiEditRef does not → the chain is partial (complete === false).
    const cA = await mkDb();
    const consA = new MongoEditAdapter(cA.db, prefix);
    await consA.init(['customers']);
    await pullEditRows(p.io, cA.db, [`${ck}EditHistory`]);
    const resA = await consA.collectPuts('customers', head as string);
    expect(resA.puts).toEqual([]);
    expect(resA.complete).toBe(false);

    // Pulled EditHistory + MultiEdits but NOT the Edits: the edit ref does not
    // resolve → still partial.
    const cB = await mkDb();
    const consB = new MongoEditAdapter(cB.db, prefix);
    await consB.init(['customers']);
    await pullEditRows(p.io, cB.db, [`${ck}EditHistory`, `${ck}MultiEdits`]);
    const resB = await consB.collectPuts('customers', head as string);
    expect(resB.puts).toEqual([]);
    expect(resB.complete).toBe(false);
  });

  it('compareTimeId orders edits the same way on every node', async () => {
    // `<millis>:<nanoid>` — millis numerically, then the suffix, so the order
    // is total and identical everywhere.
    expect(compareTimeId('100:aaaa', '200:aaaa')).toBe(-1);
    expect(compareTimeId('200:aaaa', '100:zzzz')).toBe(1);
    expect(compareTimeId('100:aaaa', '100:bbbb')).toBe(-1);
    expect(compareTimeId('100:bbbb', '100:aaaa')).toBe(1);
    expect(compareTimeId('100:aaaa', '100:aaaa')).toBe(0);
    // Not comparable → 0, so a peer on an older build (no timeId) still
    // converges the way it used to.
    expect(compareTimeId(undefined, '100:aaaa')).toBe(0);
    expect(compareTimeId('100:aaaa', undefined)).toBe(0);
    expect(compareTimeId('nope:aaaa', '100:aaaa')).toBe(0);
    expect(compareTimeId('100:aaaa', 'nope:aaaa')).toBe(0);
  });

  it('falls back to the per-row read path when the batch read is unavailable', async () => {
    // Both relay paths are tried before a row is called missing: a readable
    // that cannot answer a batch content-hash request must not truncate the
    // chain.
    const p = await mkDb();
    const producer = new MongoEditAdapter(p.db, prefix);
    await producer.init(['customers']);
    const head = (await producer.putDoc('customers', { _id: new Int32(1), name: 'A' }))
      ?.head;

    const c = await mkDb();
    const consumer = new MongoEditAdapter(c.db, prefix);
    await consumer.init(['customers']);
    await pullEditRows(p.io, c.db, editTables(producer.cakeKey('customers')));
    (c.db.core as unknown as { readRowsByHashes: unknown }).readRowsByHashes =
      () => {
        throw new Error('batch reads not supported here');
      };

    const res = await consumer.collectPuts('customers', head as string);
    expect(res.complete).toBe(true);
    expect(res.puts.map((put) => put.sliceId)).toEqual(['1']);

    // And when BOTH paths fail the chain is reported truncated rather than
    // blowing up the apply.
    (c.db as unknown as { get: unknown }).get = () => {
      throw new Error('relay unreachable');
    };
    const blind = await consumer.collectPuts('customers', head as string);
    expect(blind.complete).toBe(false);
    expect(blind.puts).toEqual([]);
  });

  it('stops a runaway walk and reports the chain truncated', async () => {
    process.env['SL_EDIT_MAX_WALK'] = '1';
    try {
      const p = await mkDb();
      const producer = new MongoEditAdapter(p.db, prefix);
      await producer.init(['customers']);
      await producer.putDoc('customers', { _id: new Int32(1), name: 'A' });
      const head = (await producer.putDoc('customers', { _id: new Int32(2), name: 'B' }))
        ?.head;

      // The cap stops the walk after the first level: the newest edit is
      // returned, the rest arrives on the next head.
      const res = await producer.collectPuts('customers', head as string);
      expect(res.complete).toBe(false);
      expect(res.puts.map((put) => put.sliceId)).toEqual(['2']);
      expect(res.sealed).toEqual([]);
    } finally {
      delete process.env['SL_EDIT_MAX_WALK'];
    }
  });

  it('does not seal a ref whose ancestor is still missing', async () => {
    const p = await mkDb();
    const producer = new MongoEditAdapter(p.db, prefix);
    await producer.init(['customers']);
    const ck = producer.cakeKey('customers');
    await producer.putDoc('customers', { _id: new Int32(1), name: 'A' });
    const head = (await producer.putDoc('customers', { _id: new Int32(2), name: 'B' }))
      ?.head;

    // The consumer gets everything EXCEPT the oldest history row, so the head
    // resolves but its ancestry does not.
    const c = await mkDb();
    const consumer = new MongoEditAdapter(c.db, prefix);
    await consumer.init(['customers']);
    await pullEditRows(p.io, c.db, [`${ck}MultiEdits`, `${ck}Edits`]);
    const dump = (await p.io.dumpTable({ table: `${ck}EditHistory` })) as Record<
      string,
      { _type: string; _data: Array<{ _hash: string }> }
    >;
    await c.db.core.import({
      [`${ck}EditHistory`]: {
        _type: dump[`${ck}EditHistory`]._type,
        _data: dump[`${ck}EditHistory`]._data.filter(
          (row) => row._hash === head,
        ),
      },
    } as never);

    const res = await consumer.collectPuts('customers', head as string);
    expect(res.complete).toBe(false);
    // The newest edit still arrives — a partial chain is applied, not dropped.
    expect(res.puts.map((put) => put.sliceId)).toEqual(['2']);
    // …but nothing is sealed, so the next walk still reaches the missing row.
    expect(res.sealed).toEqual([]);
  });

  it('ignores a chain node that is not a putComponent', async () => {
    const p = await mkDb();
    const producer = new MongoEditAdapter(p.db, prefix);
    await producer.init(['customers']);
    const ck = producer.cakeKey('customers');
    const head = (await producer.putDoc('customers', { _id: new Int32(1), name: 'A' }))
      ?.head;

    // Append a well-formed history node carrying a non-putComponent action.
    const ehDump = (await p.io.dumpTable({ table: `${ck}EditHistory` })) as Record<
      string,
      { _data: Array<Record<string, unknown>> }
    >;
    const dataRef = ehDump[`${ck}EditHistory`]._data[0]['dataRef'] as string;
    const edit = hip({ name: 'noop', action: { type: 'noop', data: {} }, _hash: '' });
    const multiEdit = hip({ previous: null, edit: edit._hash as string, _hash: '' });
    const history = hip({
      timeId: '9999999999999:zzzz',
      multiEditRef: multiEdit._hash as string,
      dataRef,
      previous: [head as string],
      _hash: '',
    });
    await p.db.core.import({
      [`${ck}Edits`]: { _type: 'edits', _data: [edit] },
      [`${ck}MultiEdits`]: { _type: 'multiEdits', _data: [multiEdit] },
      [`${ck}EditHistory`]: { _type: 'editHistory', _data: [history] },
    } as never);

    const res = await producer.collectPuts(
      'customers',
      history._hash as string,
    );
    expect(res.complete).toBe(true);
    // The noop node contributes no put, but it IS part of the resolved chain.
    expect(res.puts.map((put) => put.sliceId)).toEqual(['1']);
    expect(res.sealed).toContain(history._hash as string);
  });

  it('latestTimeIds seeds only documents the newest local edit produced', async () => {
    const p = await mkDb();
    const adapter = new MongoEditAdapter(p.db, prefix);
    await adapter.init(['customers']);
    const ck = adapter.cakeKey('customers');

    const docV1 = { _id: new Int32(1), name: 'v1' };
    const docV2 = { _id: new Int32(1), name: 'v2' };
    const put = (sliceId: string, doc: Record<string, unknown>) => ({
      _hash: `ed-${sliceId}-${String(doc['name'])}`,
      action: {
        type: 'putComponent',
        data: {
          layer: 'customersLayer',
          sliceId,
          component: EJSON.serialize(doc, { relaxed: false }),
        },
      },
    });
    const rows: Record<string, Array<Record<string, unknown>>> = {
      [`${ck}Edits`]: [put('1', docV1), put('1', docV2), { _hash: 'ed-empty' }],
      [`${ck}MultiEdits`]: [
        { _hash: 'me-1', edit: 'ed-1-v1' },
        { _hash: 'me-2', edit: 'ed-1-v2' },
        { _hash: 'me-3', edit: 'ed-empty' },
        { _hash: 'me-4' },
      ],
      [`${ck}EditHistory`]: [
        // Newest first, so the older edit for the same document is skipped.
        { _hash: 'eh-2', timeId: '2000:aaaa', multiEditRef: 'me-2' },
        { _hash: 'eh-1', timeId: '1000:aaaa', multiEditRef: 'me-1' },
        // No timeId → not orderable, skipped.
        { _hash: 'eh-3', multiEditRef: 'me-2' },
        // Resolves to an edit without an action → no put.
        { _hash: 'eh-4', timeId: '3000:aaaa', multiEditRef: 'me-3' },
        // Resolves to no edit at all → no put.
        { _hash: 'eh-5', timeId: '4000:aaaa', multiEditRef: 'me-4' },
        // No multiEdit reference at all → no put.
        { _hash: 'eh-6', timeId: '5000:aaaa' },
        // A multiEdit that is not in the local store → no put.
        { _hash: 'eh-7', timeId: '6000:aaaa', multiEditRef: 'me-absent' },
      ],
    };
    (p.db.core as unknown as { dumpTable: unknown }).dumpTable = async (
      table: string,
    ) => (rows[table] ? { [table]: { _data: rows[table] } } : { [table]: {} });

    // The document holds exactly what the NEWEST local edit produced, so that
    // edit is provably applied and seeds the ordering.
    expect(
      await adapter.latestTimeIds(
        'customers',
        new Map([['1', docHash(docV2)]]),
        100,
      ),
    ).toEqual(new Map([['1', '2000:aaaa']]));

    // The live document does NOT match — the newest local edit may have been
    // pulled but never applied, so it must not seed anything.
    expect(
      await adapter.latestTimeIds(
        'customers',
        new Map([['1', docHash(docV1)]]),
        100,
      ),
    ).toEqual(new Map());

    // A chain too large to scan is skipped rather than stalling start-up.
    expect(
      await adapter.latestTimeIds(
        'customers',
        new Map([['1', docHash(docV2)]]),
        1,
      ),
    ).toEqual(new Map());

    // A table the local store cannot dump contributes nothing.
    delete rows[`${ck}Edits`];
    expect(
      await adapter.latestTimeIds(
        'customers',
        new Map([['1', docHash(docV2)]]),
        100,
      ),
    ).toEqual(new Map());
  });
});
