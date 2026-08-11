// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Int32 } from 'bson';
import type { Document } from 'mongodb';
import { describe, expect, it } from 'vitest';

import {
  buildCake,
  changedCollections,
  componentsTableFor,
  componentHashOf,
  computeCollectionPlan,
  parseCake,
  parseLayer,
  sliceIdOf,
  type CakeRow,
  type CollectionDocs,
} from '../src/mongo-cake-model.ts';

// .............................................................................

const docs = (...ds: Document[]): Document[] => ds;

const snapshot = (map: Record<string, Document[]>): CollectionDocs =>
  new Map(Object.entries(map));

const layerFor = (built: ReturnType<typeof buildCake>, coll: string) =>
  parseLayer(built.layers.get(built.layerByCollection.get(coll)!)!);

const compsFor = (built: ReturnType<typeof buildCake>, coll: string) =>
  built.components.get(`${coll}Components`) ?? new Map<string, CakeRow>();

// The local state maps a receiver derives from its own Mongo:
// sliceId → component hash, and sliceId → raw `_id`.
const localState = (
  ds: Document[],
): { state: Map<string, string>; ids: Map<string, unknown> } => {
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

describe('mongo-cake-model', () => {
  describe('componentsTableFor', () => {
    it('camel-cases snake_case collection names to a valid rljson table key', () => {
      expect(componentsTableFor('customers')).toBe('customersComponents');
      expect(componentsTableFor('sd_protocols')).toBe('sdProtocolsComponents');
      expect(componentsTableFor('fv_contactGroupToShippingRoute')).toBe(
        'fvContactGroupToShippingRouteComponents',
      );
      expect(componentsTableFor('a__b_')).toBe('aBComponents'); // trailing / doubled
      // Leading separator → camel-case would start uppercase → lower-cased.
      expect(componentsTableFor('__synctest')).toBe('synctestComponents');
    });
  });

  describe('sliceIdOf', () => {
    it('maps a nullish _id to the canonical null slice key', () => {
      expect(sliceIdOf(null)).toBe('null');
      expect(sliceIdOf(undefined)).toBe('null');
    });
  });

  describe('buildCake — change detection via content hashes', () => {
    it('is deterministic: identical snapshots yield the same cake hash', () => {
      const a = buildCake(
        snapshot({ customers: docs({ _id: 1, name: 'Ann' }, { _id: 2, name: 'Bo' }) }),
      );
      const b = buildCake(
        snapshot({ customers: docs({ _id: 2, name: 'Bo' }, { _id: 1, name: 'Ann' }) }),
      );
      expect(a.cakeHash).toBe(b.cakeHash);
    });

    it('changing a document changes its collection layer and the cake hash', () => {
      const before = buildCake(snapshot({ customers: docs({ _id: 1, name: 'Ann' }) }));
      const after = buildCake(snapshot({ customers: docs({ _id: 1, name: 'Anna' }) }));
      expect(after.cakeHash).not.toBe(before.cakeHash);
      expect(after.layerByCollection.get('customers')).not.toBe(
        before.layerByCollection.get('customers'),
      );
    });

    it('removing a document changes the layer hash (delete = absence)', () => {
      const before = buildCake(
        snapshot({ customers: docs({ _id: 1, name: 'Ann' }, { _id: 2, name: 'Bo' }) }),
      );
      const after = buildCake(snapshot({ customers: docs({ _id: 1, name: 'Ann' }) }));
      expect(after.cakeHash).not.toBe(before.cakeHash);
      // The removed _id is simply absent from the new layer's `add` — no
      // tombstone row is produced anywhere.
      const add = layerFor(after, 'customers').add;
      expect(Object.keys(add)).toEqual([sliceIdOf(1)]);
      expect(sliceIdOf(2) in add).toBe(false);
    });

    it('an untouched collection keeps its layer hash across a cake rebuild', () => {
      const before = buildCake(
        snapshot({
          customers: docs({ _id: 1, name: 'Ann' }),
          items: docs({ _id: 9, sku: 'X' }),
        }),
      );
      const after = buildCake(
        snapshot({
          customers: docs({ _id: 1, name: 'Ann' }),
          items: docs({ _id: 9, sku: 'X' }, { _id: 10, sku: 'Y' }),
        }),
      );
      expect(after.layerByCollection.get('customers')).toBe(
        before.layerByCollection.get('customers'),
      );
      expect(after.layerByCollection.get('items')).not.toBe(
        before.layerByCollection.get('items'),
      );
      expect(
        changedCollections(parseCake(before.cake), parseCake(after.cake)),
      ).toEqual(new Set(['items']));
    });

    it('treats every collection as changed on first sync (no previous cake)', () => {
      const cake = buildCake(
        snapshot({ customers: docs({ _id: 1 }), items: docs({ _id: 9 }) }),
      );
      expect(changedCollections(null, parseCake(cake.cake))).toEqual(
        new Set(['customers', 'items']),
      );
    });
  });

  describe('computeCollectionPlan — tombstone-free deletes', () => {
    it('deletes _ids present locally but absent from the incoming layer', () => {
      const local = docs({ _id: 1, name: 'Ann' }, { _id: 2, name: 'Bo' });
      const incoming = buildCake(snapshot({ customers: docs({ _id: 1, name: 'Ann' }) }));
      const { state, ids } = localState(local);

      const plan = computeCollectionPlan(
        layerFor(incoming, 'customers'),
        compsFor(incoming, 'customers'),
        state,
        ids,
      );

      expect(plan.deletes).toEqual([2]);
      expect(plan.upserts).toEqual([]);
    });

    it('upserts new and changed documents, deletes removed ones together', () => {
      const local = docs({ _id: 1, name: 'Ann' }, { _id: 2, name: 'Bo' });
      const incoming = buildCake(
        snapshot({ customers: docs({ _id: 1, name: 'Anna' }, { _id: 3, name: 'Cy' }) }),
      );
      const { state, ids } = localState(local);

      const plan = computeCollectionPlan(
        layerFor(incoming, 'customers'),
        compsFor(incoming, 'customers'),
        state,
        ids,
      );

      expect(plan.deletes).toEqual([2]);
      const upsertedIds = plan.upserts
        .map((d) => Number((d['_id'] as Int32).valueOf()))
        .sort();
      expect(upsertedIds).toEqual([1, 3]);
      const anna = plan.upserts.find((d) => Number((d['_id'] as Int32).valueOf()) === 1);
      expect(anna?.['name']).toBe('Anna');
    });

    it('skips an incoming sliceId whose component body is not present yet', () => {
      const incoming = buildCake(snapshot({ customers: docs({ _id: 1, name: 'Ann' }) }));
      const plan = computeCollectionPlan(
        layerFor(incoming, 'customers'),
        new Map(),
        new Map(),
        new Map(),
      );
      expect(plan.upserts).toEqual([]);
      expect(plan.deletes).toEqual([]);
    });

    it('produces no work when local already equals the incoming layer', () => {
      const local = docs({ _id: 1, name: 'Ann' });
      const incoming = buildCake(snapshot({ customers: docs({ _id: 1, name: 'Ann' }) }));
      const { state, ids } = localState(local);
      const plan = computeCollectionPlan(
        layerFor(incoming, 'customers'),
        compsFor(incoming, 'customers'),
        state,
        ids,
      );
      expect(plan.upserts).toEqual([]);
      expect(plan.deletes).toEqual([]);
    });
  });

  describe('BSON-lossless round-trip through the content model', () => {
    it('preserves the Int32 _id on a content-absence delete', () => {
      const local: Document[] = [{ _id: new Int32(100), qty: new Int32(5) }];
      const incoming = buildCake(snapshot({ items: [] }));
      const { state, ids } = localState(local);

      const plan = computeCollectionPlan(
        layerFor(incoming, 'items'),
        compsFor(incoming, 'items'),
        state,
        ids,
      );
      expect(plan.deletes).toHaveLength(1);
      expect(plan.deletes[0]).toBeInstanceOf(Int32);
      expect((plan.deletes[0] as Int32).valueOf()).toBe(100);
    });

    it('restores upserted docs with their BSON types intact', () => {
      const incoming = buildCake(
        snapshot({ items: [{ _id: new Int32(7), qty: new Int32(42) }] }),
      );
      const plan = computeCollectionPlan(
        layerFor(incoming, 'items'),
        compsFor(incoming, 'items'),
        new Map(),
        new Map(),
      );
      expect(plan.upserts).toHaveLength(1);
      const doc = plan.upserts[0];
      expect(doc['_id']).toBeInstanceOf(Int32);
      expect((doc['qty'] as Int32).valueOf()).toBe(42);
    });
  });
});
