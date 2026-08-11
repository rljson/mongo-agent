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

import {
  MongoCakeAdapter,
  componentsTableOf,
} from '../src/mongo-cake-adapter.ts';
import {
  buildCake,
  componentHashOf,
  componentToDoc,
  computeCollectionPlan,
  parseCake,
  parseLayer,
  sliceIdOf,
  type CollectionDocs,
} from '../src/mongo-cake-model.ts';

// .............................................................................

const snapshot = (map: Record<string, Document[]>): CollectionDocs =>
  new Map(Object.entries(map));

const newDb = async (): Promise<Db> => {
  const io = new IoMem();
  await io.init();
  await io.isReady();
  return new Db(io);
};

// Local state the receiver derives from its own Mongo (sliceId → hash / _id).
const localState = (ds: Document[]) => {
  const state = new Map<string, string>();
  const ids = new Map<string, unknown>();
  for (const d of ds) {
    const sid = sliceIdOf(d['_id']);
    state.set(sid, componentHashOf(d));
    ids.set(sid, d['_id']);
  }
  return { state, ids };
};

// .............................................................................

describe('MongoCakeAdapter', () => {
  let db: Db;
  let adapter: MongoCakeAdapter;

  beforeEach(async () => {
    db = await newDb();
    adapter = new MongoCakeAdapter(db);
  });

  it('round-trips a cake: store → fetch cake → layers → components', async () => {
    const built = buildCake(
      snapshot({
        customers: [{ _id: new Int32(1), name: 'Ann' }, { _id: new Int32(2), name: 'Bo' }],
        items: [{ _id: new Int32(9), sku: 'X' }],
      }),
    );
    const ref = await adapter.storeCake(built);
    expect(ref).toBe(built.cakeHash);

    const cakeRow = await adapter.fetchCake(ref);
    expect(cakeRow).not.toBeNull();
    const cake = parseCake(cakeRow!);
    expect(Object.keys(cake.layers).sort()).toEqual(['customers', 'items']);

    const layers = await adapter.fetchLayers(Object.values(cake.layers));
    const customersLayer = parseLayer(layers.get(cake.layers['customers'])!);
    expect(Object.keys(customersLayer.add)).toEqual([sliceIdOf(new Int32(1)), sliceIdOf(new Int32(2))].sort());

    const compHashes = Object.values(customersLayer.add);
    const comps = await adapter.fetchComponents(customersLayer.componentsTable, compHashes);
    const restored = compHashes.map((h) => componentToDoc(comps.get(h)!));
    const names = restored.map((d) => d['name']).sort();
    expect(names).toEqual(['Ann', 'Bo']);
    // BSON type survived the store/fetch round-trip.
    expect(restored[0]['_id']).toBeInstanceOf(Int32);
  });

  it('propagates a delete by content-absence (no tombstone table)', async () => {
    const before = [{ _id: new Int32(1), name: 'Ann' }, { _id: new Int32(2), name: 'Bo' }];
    // Producer stores the "before" cake, then a new cake with _id:2 removed.
    await adapter.storeCake(buildCake(snapshot({ customers: before })));
    const after = buildCake(snapshot({ customers: [{ _id: new Int32(1), name: 'Ann' }] }));
    const ref = await adapter.storeCake(after);

    // Receiver pulls the new cake and diffs it against its local "before" state.
    const cake = parseCake((await adapter.fetchCake(ref))!);
    const layerRow = (await adapter.fetchLayers([cake.layers['customers']])).get(
      cake.layers['customers'],
    )!;
    const layer = parseLayer(layerRow);
    const comps = await adapter.fetchComponents(
      layer.componentsTable,
      Object.values(layer.add),
    );
    const { state, ids } = localState(before);

    const plan = computeCollectionPlan(layer, comps, state, ids);

    expect(plan.upserts).toEqual([]); // _id:1 unchanged
    expect(plan.deletes).toHaveLength(1);
    expect((plan.deletes[0] as Int32).valueOf()).toBe(2);

    // No tombstone collection is involved — only cakes, layers and the
    // collection's components table exist.
    expect(await db.core.hasTable('sync_tombstones')).toBe(false);
    expect(await db.core.hasTable(componentsTableOf('customers'))).toBe(true);
  });

  it('fetches only the changed layer + only the missing components (incremental)', async () => {
    const v1 = buildCake(
      snapshot({
        customers: [{ _id: new Int32(1), name: 'Ann' }],
        items: [{ _id: new Int32(9), sku: 'X' }],
      }),
    );
    await adapter.storeCake(v1);
    // items unchanged; customers gains a doc.
    const v2 = buildCake(
      snapshot({
        customers: [{ _id: new Int32(1), name: 'Ann' }, { _id: new Int32(2), name: 'Bo' }],
        items: [{ _id: new Int32(9), sku: 'X' }],
      }),
    );
    await adapter.storeCake(v2);

    const c1 = parseCake((await adapter.fetchCake(v1.cakeHash))!);
    const c2 = parseCake((await adapter.fetchCake(v2.cakeHash))!);
    // items layer hash is stable → a receiver would skip it entirely.
    expect(c2.layers['items']).toBe(c1.layers['items']);
    expect(c2.layers['customers']).not.toBe(c1.layers['customers']);

    // The new customers layer: only _id:2's component is missing locally.
    const layer = parseLayer(
      (await adapter.fetchLayers([c2.layers['customers']])).get(c2.layers['customers'])!,
    );
    const localHashes = new Set([componentHashOf({ _id: new Int32(1), name: 'Ann' })]);
    const missing = Object.values(layer.add).filter((h) => !localHashes.has(h));
    expect(missing).toHaveLength(1);
    const comps = await adapter.fetchComponents(layer.componentsTable, missing);
    expect(componentToDoc(comps.get(missing[0])!)['name']).toBe('Bo');
  });

  it('fetchCake returns null for an unknown ref', async () => {
    await adapter.storeCake(buildCake(snapshot({ customers: [{ _id: new Int32(1) }] })));
    expect(await adapter.fetchCake('deadbeefdeadbeefdeadbe')).toBeNull();
  });

  it('handles an empty collection and an empty fetch without error', async () => {
    // An empty collection produces an empty components table (no rows to write).
    const built = buildCake(snapshot({ customers: [{ _id: new Int32(1) }], empties: [] }));
    const ref = await adapter.storeCake(built);
    expect(ref).toBe(built.cakeHash);
    const cake = parseCake((await adapter.fetchCake(ref))!);
    expect(Object.keys(cake.layers).sort()).toEqual(['customers', 'empties']);
    // Fetching zero components short-circuits to an empty map.
    expect((await adapter.fetchComponents('emptiesComponents', [])).size).toBe(0);
    expect((await adapter.fetchLayers([])).size).toBe(0);
  });
});
