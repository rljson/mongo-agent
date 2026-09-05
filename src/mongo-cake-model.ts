// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hsh } from '@rljson/hash';
import { EJSON } from 'bson';
import type { Document } from 'mongodb';

// .............................................................................
// Native RLJSON Cake / Layer / Component model for MongoDB (review #7).
//
// Replaces the Tree + Bs-Blob snapshot with the native content model, stored as
// uniform content-addressed rows (`{ _hash, doc }`) that a peer pulls by hash —
// exactly the `readRowsByHashes` flow of the migration doc §6, without the
// heavyweight Layer/Cake controllers:
//
//   Mongo document   → Component row  (`doc` = the EJSON document)
//   Mongo collection → Layer row      (`doc` = { componentsTable, add:{ _id→compHash } })
//   Mongo database   → Cake row       (`doc` = { layers:{ collection→layerHash } })
//
// A DELETE is the ABSENCE of an `_id` from a Layer's `add` map — the tree's
// "missing node = deleted" semantic, flat and WITHOUT tombstones. Every row's
// `_hash` is a true content hash, so any change (including a removal) ripples
// layer-hash → cake-hash, giving native change-detection.
//
// This module is pure (no I/O). Persistence / wire transport live in the
// adapter; the receiver's apply plan is computed here by diffing an incoming
// Layer against the current Mongo state.
// .............................................................................

/** A content-addressed row: an opaque `doc` string plus its content hash. */
export interface CakeRow {
  _hash: string;
  doc: string;
}

/** The parsed body of a Layer row. */
export interface LayerBody {
  componentsTable: string;
  add: Record<string, string>;
}

/** The parsed body of a Cake row. */
export interface CakeBody {
  layers: Record<string, string>;
}

/** Live documents per collection name. */
export type CollectionDocs = Map<string, Document[]>;

/** Everything produced when a Mongo snapshot is turned into a Cake. */
export interface BuiltCake {
  /** The Cake row's content hash — the single ref broadcast to peers. */
  cakeHash: string;
  /** The Cake row itself. */
  cake: CakeRow;
  /** Layer rows keyed by their content hash. */
  layers: Map<string, CakeRow>;
  /** Layer hash per collection. */
  layerByCollection: Map<string, string>;
  /** Component rows per components-table key, keyed by component hash. */
  components: Map<string, Map<string, CakeRow>>;
}

/** The receiver's plan: what to upsert and what to delete, per collection. */
export interface ApplyPlan {
  upserts: Map<string, Document[]>;
  deletes: Map<string, unknown[]>;
}

// .............................................................................

/**
 * Sorts an object's keys so its JSON serialization is deterministic.
 * @param obj - The object whose keys are sorted into a new object.
 */
const sortedObject = (obj: Record<string, string>): Record<string, string> => {
  const out: Record<string, string> = {};
  for (const key of Object.keys(obj).sort()) out[key] = obj[key];
  return out;
};

/**
 * Wraps a deterministic `doc` string into a content-addressed row.
 * @param doc - The deterministic document/structure string to content-hash.
 */
const wrap = (doc: string): CakeRow => hsh({ doc, _hash: '' }) as unknown as CakeRow;

/**
 * Maps a Mongo collection name to its rljson components-table key. rljson
 * requires lowerCamelCase table keys, but Mongo collections use snake_case
 * (e.g. `sd_protocols`), so the name is camel-cased before the `Components`
 * suffix (`sd_protocols` → `sdProtocolsComponents`). Deterministic, so both
 * nodes derive the same key; content-addressed rows make any (CARAT-improbable)
 * camel-case collision harmless.
 * @param collection - The Mongo collection name.
 * @returns A lowerCamelCase components-table key.
 */
export const componentsTableFor = (collection: string): string => {
  const camel = collection
    .replace(/[^a-zA-Z0-9]+(.)?/g, (_m: string, c?: string) =>
      c ? c.toUpperCase() : '',
    )
    .replace(/^[A-Z]/, (c) => c.toLowerCase());
  return `${camel}Components`;
};

/**
 * Derives a stable, string sliceId from a document's `_id`. Mongo `_id`s can be
 * ObjectId / Int32 / string / …; the canonical Extended-JSON string is stable
 * across nodes and reversible, so the same document maps to the same sliceId
 * everywhere.
 * @param id - The raw Mongo `_id` value.
 * @returns A canonical, stable string key for the `_id`.
 */
export const sliceIdOf = (id: unknown): string =>
  EJSON.stringify(id ?? null, { relaxed: false });

/**
 * Turns a Mongo document into a content-addressed Component row. The document
 * is serialized with BSON-lossless canonical Extended JSON (Int32/Long/Date/
 * ObjectId preserved), so the row hash is a true content hash of the document.
 * @param doc - The raw Mongo document (its existing `_hash`, if any, is dropped).
 * @returns The Component row `{ _hash, doc }`.
 */
export const componentOf = (doc: Document): CakeRow => {
  const { _hash: _drop, ...rest } = doc as Record<string, unknown>;
  void _drop;
  return wrap(EJSON.stringify(rest, { relaxed: false }));
};

/**
 * The Component hash a document would have — the sliceId→hash value stored in a
 * Layer's `add`. Lets a receiver hash its own Mongo docs to detect no-op syncs.
 * @param doc - The raw Mongo document.
 * @returns The document's Component content hash.
 */
export const componentHashOf = (doc: Document): string => componentOf(doc)._hash;

/**
 * Restores a Mongo document from a Component row, re-materialising BSON types.
 * @param row - The Component row.
 * @returns The Mongo document.
 */
export const componentToDoc = (row: CakeRow): Document =>
  EJSON.parse(row.doc, { relaxed: false }) as Document;

/**
 * Parses a Layer row body.
 * @param row - The Layer row whose `doc` string is parsed.
 */
export const parseLayer = (row: CakeRow): LayerBody => JSON.parse(row.doc) as LayerBody;

/**
 * Parses a Cake row body.
 * @param row - The Cake row whose `doc` string is parsed.
 */
export const parseCake = (row: CakeRow): CakeBody => JSON.parse(row.doc) as CakeBody;

/** A single collection's built Layer row plus its Component rows. */
export interface BuiltLayer {
  /** The Layer row (its `_hash` is the collection's content hash). */
  layer: CakeRow;
  /** Component rows keyed by content hash. */
  components: Map<string, CakeRow>;
}

/**
 * Builds one collection's Layer + Component rows. The unit of incremental work:
 * a producer re-runs this only for the collection that changed and reuses the
 * cached result for the rest.
 * @param name - The collection name.
 * @param docs - The collection's live documents.
 * @returns The collection's Layer row and its Component rows.
 */
export const buildCollectionLayer = (
  name: string,
  docs: Document[],
): BuiltLayer => {
  const componentsTable = componentsTableFor(name);
  const components = new Map<string, CakeRow>();
  const add: Record<string, string> = {};
  for (const doc of docs) {
    const component = componentOf(doc);
    components.set(component._hash, component);
    add[sliceIdOf((doc as Record<string, unknown>)['_id'])] = component._hash;
  }
  const layer = wrap(
    JSON.stringify({ componentsTable, add: sortedObject(add) } as LayerBody),
  );
  return { layer, components };
};

/**
 * Builds the Cake row from a collection → layer-hash map.
 * @param cakeLayers - Map of collection name to its Layer hash.
 * @returns The Cake row (its `_hash` is the whole-DB content hash).
 */
export const cakeFromLayers = (
  cakeLayers: Record<string, string>,
): CakeRow =>
  wrap(JSON.stringify({ layers: sortedObject(cakeLayers) } as CakeBody));

/**
 * Builds the native Cake/Layer/Component rows for a full Mongo snapshot.
 * @param collections - Live documents per collection name.
 * @returns The built Cake plus the layer/component rows to persist.
 */
export const buildCake = (collections: CollectionDocs): BuiltCake => {
  const layers = new Map<string, CakeRow>();
  const layerByCollection = new Map<string, string>();
  const components = new Map<string, Map<string, CakeRow>>();
  const cakeLayers: Record<string, string> = {};

  const entries = Array.from(collections.entries()).sort((a, b) =>
    a[0].localeCompare(b[0]),
  );

  for (const [name, docs] of entries) {
    const built = buildCollectionLayer(name, docs);
    layers.set(built.layer._hash, built.layer);
    layerByCollection.set(name, built.layer._hash);
    components.set(componentsTableFor(name), built.components);
    cakeLayers[name] = built.layer._hash;
  }

  const cake = cakeFromLayers(cakeLayers);

  return {
    cakeHash: cake._hash,
    cake,
    layers,
    layerByCollection,
    components,
  };
};

/**
 * Returns the collection names whose Layer hash differs between two Cakes — the
 * collections that actually changed. A collection present in one Cake but not
 * the other counts as changed.
 * @param previous - The previously-applied Cake body, or null on first sync.
 * @param next - The incoming Cake body.
 * @returns The set of collection names to re-apply.
 */
export const changedCollections = (
  previous: CakeBody | null,
  next: CakeBody,
): Set<string> => {
  const changed = new Set<string>();
  const prevLayers = previous?.layers ?? {};
  for (const name of new Set([...Object.keys(prevLayers), ...Object.keys(next.layers)])) {
    if (prevLayers[name] !== next.layers[name]) changed.add(name);
  }
  return changed;
};

/**
 * Computes the receiver's plan for one collection: which documents to upsert and
 * which `_id`s to delete, by diffing an incoming Layer against local state.
 *
 * - **Upsert:** a sliceId whose incoming component hash differs from (or is
 *   absent in) the local state — the document is new or changed.
 * - **Delete:** a sliceId present locally but **absent** from the incoming
 *   Layer's `add` — the tombstone-free, content-absence delete.
 * @param layer - The incoming Layer body for the collection.
 * @param incomingComponents - Component rows for the layer's components table,
 *   keyed by component hash.
 * @param localState - Current local sliceId → component-hash map.
 * @param localIdBySlice - Current local sliceId → raw Mongo `_id`, so delete
 *   filters carry the correct BSON `_id` type.
 * @returns The upsert documents and delete `_id`s for this collection.
 */
export const computeCollectionPlan = (
  layer: LayerBody,
  incomingComponents: Map<string, CakeRow>,
  localState: Map<string, string>,
  localIdBySlice: Map<string, unknown>,
): { upserts: Document[]; deletes: unknown[] } => {
  const upserts: Document[] = [];
  const deletes: unknown[] = [];

  for (const [sliceId, compHash] of Object.entries(layer.add)) {
    if (localState.get(sliceId) === compHash) continue; // unchanged
    const component = incomingComponents.get(compHash);
    if (!component) continue; // body not pulled yet — skip
    upserts.push(componentToDoc(component));
  }

  for (const [sliceId, rawId] of localIdBySlice) {
    if (!(sliceId in layer.add)) deletes.push(rawId);
  }

  return { upserts, deletes };
};
