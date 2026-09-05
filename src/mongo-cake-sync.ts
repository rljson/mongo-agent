// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import type { Document } from 'mongodb';

import type { MongoCakeAdapter } from './mongo-cake-adapter.ts';
import {
  buildCollectionLayer,
  cakeFromLayers,
  changedCollections,
  componentHashOf,
  componentsTableFor,
  computeCollectionPlan,
  parseCake,
  parseLayer,
  sliceIdOf,
  type BuiltLayer,
  type CakeBody,
  type CakeRow,
} from './mongo-cake-model.ts';

// .............................................................................
// Cake/Layer/Component sync orchestration (#7, increment 3).
//
// Producer: snapshot Mongo → buildCake → adapter.storeCake → broadcast the cake
// hash. Consumer: on an incoming cake hash → fetch the cake, diff its layer map
// against the last applied cake, pull only the changed layers and only the
// missing components by hash, then upsert/delete into Mongo. A DELETE is the
// absence of an `_id` from the incoming layer — no tombstones.
//
// Mongo access sits behind the small {@link MongoStore} interface so the
// orchestration is unit-testable with an in-memory store, and the real binding
// is a thin wrapper over a `mongodb` Db.
// .............................................................................

/** The Mongo operations the cake sync needs, abstracted for testability. */
export interface MongoStore {
  /** Non-ignored collection names to sync. */
  listCollections(): Promise<string[]>;
  /** All documents of a collection. */
  readCollection(name: string): Promise<Document[]>;
  /** Number of documents in a collection (for the delete circuit-breaker). */
  countDocuments(name: string): Promise<number>;
  /** Upsert the given documents and delete the given `_id`s in one collection. */
  applyChanges(
    name: string,
    upserts: Document[],
    deletes: unknown[],
  ): Promise<void>;
}

/** Options controlling the cake sync's safety limits and broadcasting. */
export interface MongoCakeSyncOptions {
  /**
   * Reject a collection's delete batch when it removes at least this fraction of
   * the collection (mass-delete circuit-breaker). Defaults to 0.3 (30%).
   */
  maxDeleteFraction?: number;
  /**
   * Reject a collection's delete batch when it removes at least this many
   * documents outright. Defaults to 300_000.
   */
  maxDeleteAbsolute?: number;
  /** Logger for skipped mass-deletes and diagnostics. */
  log?: (message: string) => void;
}

/** Outcome of applying one incoming cake, for diagnostics/tests. */
export interface ApplyResult {
  /** Collections that were touched. */
  changed: string[];
  /** Total documents upserted. */
  upserted: number;
  /** Total documents deleted. */
  deleted: number;
  /** Collections whose delete batch was blocked by the circuit-breaker. */
  blockedDeletes: string[];
}

// .............................................................................

/**
 * Drives Cake/Layer/Component sync between a Mongo store and an rljson Db.
 */
export class MongoCakeSync {
  private _lastSentHash: string | null = null;
  private _prevCake: CakeBody | null = null;
  // Per-collection built layer + component rows, reused across pushes so only
  // the collection that actually changed is re-read and re-serialised.
  private readonly _layerCache = new Map<string, BuiltLayer>();
  private readonly _maxFraction: number;
  private readonly _maxAbsolute: number;
  private readonly _log: (message: string) => void;

  constructor(
    private readonly _store: MongoStore,
    private readonly _adapter: MongoCakeAdapter,
    options: MongoCakeSyncOptions = {},
  ) {
    this._maxFraction = options.maxDeleteFraction ?? 0.3;
    this._maxAbsolute = options.maxDeleteAbsolute ?? 300_000;
    this._log = options.log ?? (() => {});
  }

  /** The last cake hash this node broadcast (used to suppress echoes). */
  get lastSentHash(): string | null {
    return this._lastSentHash;
  }

  /**
   * Snapshots Mongo and stores the resulting cake, re-reading and re-serialising
   * only the collections named in `dirty` (all of them on the first push or when
   * `dirty` is omitted) and reusing the cached layer for the rest. Persists just
   * the (re)built layers + components + the new cake — unchanged rows are
   * already in the store from earlier pushes (content-addressed). Returns the
   * new cake hash to broadcast, or null when nothing changed.
   * @param dirty - Collections that changed since the last push, or omitted for
   *   a full rebuild.
   * @returns The new cake hash, or null on a no-op.
   */
  async pushSnapshot(dirty?: Set<string>): Promise<string | null> {
    const names = await this._store.listCollections();
    const present = new Set(names);
    for (const cached of [...this._layerCache.keys()]) {
      if (!present.has(cached)) this._layerCache.delete(cached);
    }

    const cakeLayers: Record<string, string> = {};
    const builtLayers = new Map<string, CakeRow>();
    const builtComponents = new Map<string, Map<string, CakeRow>>();

    for (const name of names) {
      let entry = this._layerCache.get(name);
      if (!entry || !dirty || dirty.has(name)) {
        const docs = await this._store.readCollection(name);
        entry = buildCollectionLayer(name, docs);
        this._layerCache.set(name, entry);
        builtLayers.set(entry.layer._hash, entry.layer);
        builtComponents.set(componentsTableFor(name), entry.components);
        // Yield (a macrotask) so the event loop can service inbound applies +
        // the test API during a large (re)build instead of freezing.
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
      cakeLayers[name] = entry.layer._hash;
    }

    const cake = cakeFromLayers(cakeLayers);
    if (cake._hash === this._lastSentHash) return null;

    await this._adapter.storeCake({
      cakeHash: cake._hash,
      cake,
      layers: builtLayers,
      layerByCollection: new Map(Object.entries(cakeLayers)),
      components: builtComponents,
    });
    this._lastSentHash = cake._hash;
    this._prevCake = parseCake(cake);
    return cake._hash;
  }

  /**
   * Applies an incoming cake hash: fetches the cake, diffs it against the last
   * applied cake, pulls only changed layers + missing components, and
   * upserts/deletes into Mongo. Ignores an echo of this node's own push.
   * @param cakeHash - The cake hash announced by a peer.
   * @returns What was applied, or null when the hash was an echo / unresolvable.
   */
  async applyIncoming(cakeHash: string): Promise<ApplyResult | null> {
    if (cakeHash === this._lastSentHash) return null; // echo of our own push
    const cakeRow = await this._adapter.fetchCake(cakeHash);
    if (!cakeRow) return null;
    const incoming = parseCake(cakeRow);

    const result: ApplyResult = {
      changed: [],
      upserted: 0,
      deleted: 0,
      blockedDeletes: [],
    };

    for (const name of changedCollections(this._prevCake, incoming)) {
      const touched = await this._applyCollection(name, incoming.layers[name]);
      result.changed.push(name);
      result.upserted += touched.upserted;
      result.deleted += touched.deleted;
      if (touched.blocked) result.blockedDeletes.push(name);
    }

    this._prevCake = incoming;
    // Adopt the applied hash as our own "last" so the change-stream echo this
    // apply triggers (Mongo writes → rescan → identical cake) is a no-op push,
    // and a re-announcement of the same cake (bootstrap heartbeat) is ignored.
    this._lastSentHash = cakeHash;
    return result;
  }

  // ...........................................................................

  /**
   * Applies one collection's incoming layer against local Mongo state.
   * @param name - The collection name.
   * @param layerHash - The incoming layer hash, or undefined when the collection
   *   vanished from the cake (treated as an empty layer → full, guarded delete).
   * @returns Counts of upserted/deleted docs and whether deletes were blocked.
   */
  private async _applyCollection(
    name: string,
    layerHash: string | undefined,
  ): Promise<{ upserted: number; deleted: number; blocked: boolean }> {
    // A collection that vanished from the cake presents as an empty layer:
    // every local doc is absent from `add`, so all are deletes (guarded below).
    const layer =
      layerHash === undefined
        ? { componentsTable: componentsTableFor(name), add: {} }
        : parseLayer(
            (await this._adapter.fetchLayers([layerHash])).get(layerHash)!,
          );

    const local = await this._store.readCollection(name);
    const localState = new Map<string, string>();
    const localIdBySlice = new Map<string, unknown>();
    const localHashes = new Set<string>();
    for (const doc of local) {
      const sid = sliceIdOf((doc as Record<string, unknown>)['_id']);
      const hash = componentHashOf(doc);
      localState.set(sid, hash);
      localIdBySlice.set(sid, (doc as Record<string, unknown>)['_id']);
      localHashes.add(hash);
    }

    // Pull only the component bodies we do not already have locally.
    const missing = Object.entries(layer.add)
      .map(([, hash]) => hash)
      .filter((hash) => !localHashes.has(hash));
    const components = await this._adapter.fetchComponents(
      layer.componentsTable,
      missing,
    );

    const plan = computeCollectionPlan(
      layer,
      components,
      localState,
      localIdBySlice,
    );

    // Mass-delete circuit-breaker: never let a stale/empty incoming cake wipe a
    // collection. Upserts always flow; only the delete batch is gated.
    let deletes = plan.deletes;
    let blocked = false;
    if (deletes.length > 0) {
      const total = await this._store.countDocuments(name);
      const fractionHit = total > 0 && deletes.length / total >= this._maxFraction;
      if (fractionHit || deletes.length >= this._maxAbsolute) {
        this._log(
          `[mongo-cake] BLOCKED mass-delete on ${name}: ${deletes.length}/${total} ` +
            `docs (>= ${Math.round(this._maxFraction * 100)}% or ${this._maxAbsolute}); deletes skipped`,
        );
        deletes = [];
        blocked = true;
      }
    }

    if (plan.upserts.length > 0 || deletes.length > 0) {
      await this._store.applyChanges(name, plan.upserts, deletes);
    }
    return { upserted: plan.upserts.length, deleted: deletes.length, blocked };
  }
}
