// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Db, MultiEditManager } from '@rljson/db';
import { hip } from '@rljson/hash';
import type { Json } from '@rljson/json';
import {
  createCakeTableCfg,
  createEditHistoryTableCfg,
  createEditTableCfg,
  createLayerTableCfg,
  createMultiEditTableCfg,
  createSliceIdsTableCfg,
  Route,
} from '@rljson/rljson';
import type { Edit, TableCfg } from '@rljson/rljson';
import type { Document } from 'mongodb';

import {
  componentToDoc,
  docHash,
  docToComponent,
} from './mongo-component-codec.ts';

/**
 * Maps a MongoDB database onto the RLJSON components/edits model — the
 * document-native replacement for the tree+blob sync (review #7).
 *
 * One RLJSON **cake per collection** (each cake has exactly one Layer): a
 * document is a Component, and every insert/update is a `putComponent`
 * EditAction appended to that collection's append-only edit chain. The head
 * (`EditHistory` ref) is the single hash broadcast per changed collection.
 * (Per-collection cakes rather than one shared cake because putComponent needs
 * the Join to already reference the target layer; a single-layer cake makes
 * the first putComponent self-bootstrapping, keeping the model doc-native.)
 *
 * Decentrality: only head refs (hashes) are broadcast. A receiver PULLS every
 * editHistory/multiEdit/edit row it misses by hash through the client's
 * `IoMulti` (→ `IoPeer` → relay → origin); nothing is pushed, the relay stores
 * no bodies. See the pull-architecture memory.
 */

const LAYER_SUFFIX = 'Layer';
const COMP_SUFFIX = 'Comp';
const SLICE_SUFFIX = 'Slices';

// .............................................................................
/**
 * Converts an arbitrary string into a valid lowerCamelCase RLJSON table key.
 * RLJSON table keys must start with a lowercase letter and contain only
 * alphanumerics — but Mongo collection names use `_` (e.g.
 * `fv_contactGroupToShippingRoute`). Underscore-separated parts are camel-joined
 * (`fv_contact` → `fvContact`) so a distinct collection maps to a distinct key
 * (CARAT collection names don't collide once camel-joined).
 * @param s - The raw string (collection name).
 * @returns A lowerCamelCase key.
 */
const toLowerCamelKey = (s: string): string => {
  const parts = s.split(/[^a-zA-Z0-9]+/).filter(Boolean);
  /* v8 ignore next -- @preserve defensive: a name with no alphanumerics */
  if (parts.length === 0) return 'c';
  let out = parts[0].charAt(0).toLowerCase() + parts[0].slice(1);
  for (let i = 1; i < parts.length; i++) {
    out += parts[i].charAt(0).toUpperCase() + parts[i].slice(1);
  }
  /* v8 ignore next -- @preserve defensive: a name starting with a digit */
  if (!/^[a-z]/.test(out)) out = `c${out}`;
  return out;
};

// .............................................................................
/**
 * Sanitizes the cake-key prefix into a lowercase alphanumeric token.
 * @param s - The raw prefix (e.g. a db name like `CARATDB`).
 * @returns A lowercase alphanumeric prefix.
 */
const sanitizePrefix = (s: string): string =>
  s.toLowerCase().replace(/[^a-z0-9]/g, '') || 'c';

/** A whole-document upsert reconstructed from a `putComponent` edit. */
export interface CollectedPut {
  /** Collection the document belongs to. */
  collection: string;
  /** RLJSON slice id (the stringified Mongo `_id`). */
  sliceId: string;
  /** The reconstructed Mongo document (BSON types restored). */
  doc: Document;
  /**
   * The `timeId` of the `EditHistory` row this put comes from
   * (`<millis>:<nanoid>`), or `undefined` for a row that carries none. It is
   * minted once, by the node that made the edit, and travels with the row — so
   * every node reads the SAME value for the same edit. That makes it a total
   * order all nodes agree on, which is what lets a receiver reject an edit
   * older than the one it already applied to that document
   * ({@link compareTimeId}).
   */
  timeId?: string;
}

/**
 * Result of {@link MongoEditAdapter.collectPuts}. `complete` is `false` when the
 * `EditHistory.previous` walk hit a row that could not be resolved through the
 * hub — the chain is then truncated and `puts` is only a partial view. A caller
 * MUST NOT treat a partial result as authoritative (must not latch the head):
 * ancestors beyond the missing row would otherwise be lost forever. See
 * `MongoEditSync._applyHead`, which treats `!complete` like an empty pull.
 */
export interface CollectPutsResult {
  /** The upserts resolvable from the walked chain, oldest first. */
  puts: CollectedPut[];
  /** `false` if any walked edit-history / multi-edit / edit row was missing. */
  complete: boolean;
  /**
   * The edit-history refs whose row AND whole ancestry down to an already
   * applied ref resolved. Only these may be remembered as applied: recording a
   * ref whose ancestor is still missing would stop every later walk at it and
   * lose that ancestor forever.
   */
  sealed: string[];
}

/** The head a local edit produced, with the `timeId` that edit was minted with. */
export interface PutDocResult {
  /** The new head editHistory ref to broadcast. */
  head: string;
  /** The `timeId` of the edit's EditHistory row, if it carries one. */
  timeId?: string;
}

// .............................................................................
/**
 * Orders two `timeId`s (`<millis>:<nanoid>`). Compares the millisecond part
 * numerically and breaks ties on the random suffix, so the order is total and
 * identical on every node. A missing or malformed id orders as "unknown".
 * @param a - The first timeId (or `undefined`).
 * @param b - The second timeId (or `undefined`).
 * @returns `-1` if `a` is older, `1` if newer, `0` when equal or not
 *   comparable.
 */
export const compareTimeId = (
  a: string | undefined,
  b: string | undefined,
): number => {
  if (!a || !b) return 0;
  if (a === b) return 0;
  const [aMillis, aTail] = a.split(':');
  const [bMillis, bTail] = b.split(':');
  const aNum = Number(aMillis);
  const bNum = Number(bMillis);
  if (Number.isNaN(aNum) || Number.isNaN(bNum)) return 0;
  if (aNum !== bNum) return aNum < bNum ? -1 : 1;
  /* v8 ignore next -- @preserve equal millis with equal tails is `a === b` above */
  return (aTail ?? '') < (bTail ?? '') ? -1 : 1;
};

interface CakeMeta {
  manager: MultiEditManager;
  cakeKey: string;
  layerKey: string;
  compKey: string;
  cakeRef: string;
}

// .............................................................................
/**
 * Builds the (agnostic) components table config. A components table needs at
 * least two columns; the row carries the whole document, so extra (undeclared)
 * fields are accepted.
 * @param key - The components table key.
 * @returns The table configuration.
 */
const componentsTableCfg = (key: string): TableCfg =>
  hip({
    key,
    type: 'components',
    columns: [
      { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'Hash' },
      { key: '_id', type: 'string', titleLong: 'Id', titleShort: 'Id' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
    _hash: '',
  }) as TableCfg;

// .............................................................................
/** Adapter between a MongoDB database and the RLJSON components/edits model. */
export class MongoEditAdapter {
  private readonly _cakes = new Map<string, CakeMeta>();

  /**
   * Hard cap on how many edit-history rows one walk may visit. A walk that
   * deep is a cold replay of a whole lineage rather than an incremental
   * catch-up; stopping keeps a mega chain from pinning a core, and the
   * remainder arrives on the next head.
   */
  /* v8 ignore next -- @preserve walk cap, env-overridable */
  private readonly _maxWalk = Number(process.env['SL_EDIT_MAX_WALK']) || 20_000;

  /**
   * Creates an adapter over the given RLJSON Db. Call {@link init} before use.
   * @param _db - The RLJSON `Db` (built from the client's `IoMulti`).
   * @param _prefix - Prefix for the per-collection cake keys (e.g. `caratdb`).
   */
  constructor(
    private readonly _db: Db,
    private readonly _prefix: string,
  ) {}

  /**
   * The layer (table) key for a collection.
   * @param collection - Mongo collection name.
   * @returns The layer table key.
   */
  layerKey(collection: string): string {
    return `${toLowerCamelKey(collection)}${LAYER_SUFFIX}`;
  }

  /**
   * The collection name for a layer key (inverse of {@link layerKey}).
   * @param layerKey - The layer table key.
   * @returns The collection name.
   */
  collectionForLayer(layerKey: string): string {
    return layerKey.endsWith(LAYER_SUFFIX)
      ? layerKey.slice(0, -LAYER_SUFFIX.length)
      : layerKey;
  }

  /**
   * The cake key for a collection (must end in `Cake`).
   * @param collection - Mongo collection name.
   * @returns The cake table key.
   */
  cakeKey(collection: string): string {
    const safe = toLowerCamelKey(collection);
    const cap = safe.charAt(0).toUpperCase() + safe.slice(1);
    return `${sanitizePrefix(this._prefix)}${cap}${'Cake'}`;
  }

  /**
   * Creates the per-collection cake + tables and an initial empty cake for
   * each collection. Call once on a fresh Db.
   * @param collections - The collections to sync (each becomes its own cake).
   */
  async init(collections: string[]): Promise<void> {
    for (const collection of collections) {
      await this._initCollection(collection);
    }
  }

  // ...........................................................................
  /**
   * Sets up one collection's cake (edit + cake + layer + component + sliceIds
   * tables and an empty cake) and its MultiEditManager.
   * @param collection - The collection to set up.
   */
  private async _initCollection(collection: string): Promise<void> {
    const core = this._db.core;
    const safe = toLowerCamelKey(collection);
    const cakeKey = this.cakeKey(collection);
    const layerKey = this.layerKey(collection);
    const compKey = `${safe}${COMP_SUFFIX}`;
    const sliceKey = `${safe}${SLICE_SUFFIX}`;

    await core.createTable(createEditTableCfg(cakeKey));
    await core.createTable(createMultiEditTableCfg(cakeKey));
    await core.createTable(createEditHistoryTableCfg(cakeKey));
    await core.createTable(createCakeTableCfg(cakeKey));
    await core.createTable(componentsTableCfg(compKey));
    await core.createTable(createSliceIdsTableCfg(sliceKey));
    await core.createTable(createLayerTableCfg(layerKey));

    const emptySlices = hip({ add: [] as string[], _hash: '' });
    const layer = hip({
      componentsTable: compKey,
      sliceIdsTable: sliceKey,
      sliceIdsTableRow: emptySlices._hash as string,
      add: {},
      _hash: '',
    });
    const cake = hip({
      sliceIdsTable: sliceKey,
      sliceIdsRow: emptySlices._hash as string,
      layers: { [layerKey]: layer._hash as string },
      _hash: '',
    });
    // One import so per-import validation sees the whole graph.
    await core.import({
      [compKey]: { _type: 'components', _data: [] },
      [sliceKey]: { _type: 'sliceIds', _data: [emptySlices] },
      [layerKey]: { _type: 'layers', _data: [layer] },
      [cakeKey]: { _type: 'cakes', _data: [cake] },
    });

    const manager = new MultiEditManager(cakeKey, this._db);
    manager.init();
    this._cakes.set(collection, {
      manager,
      cakeKey,
      layerKey,
      compKey,
      cakeRef: cake._hash as string,
    });
  }

  /**
   * Producer: records a whole-document upsert as a `putComponent` edit on the
   * collection's cake and returns the new head `EditHistory` ref to broadcast.
   * @param collection - The collection the document belongs to.
   * @param doc - The Mongo document to upsert.
   * @returns The new head plus the edit's `timeId`, or `null` if the
   *   collection is unknown.
   */
  async putDoc(
    collection: string,
    doc: Document,
  ): Promise<PutDocResult | null> {
    const meta = this._cakes.get(collection);
    if (!meta) return null;
    const component = docToComponent(doc);
    const edit = hip({
      name: `put ${collection}/${String(doc['_id'])}`,
      action: {
        type: 'putComponent',
        data: {
          layer: meta.layerKey,
          sliceId: String(doc['_id']),
          component: component as unknown as Json,
        },
      },
      _hash: '',
    }) as unknown as Edit;
    // cakeRef seeds only the FIRST edit; afterwards the manager chains.
    const cakeRef = meta.manager.head ? undefined : meta.cakeRef;
    await meta.manager.edit(edit, cakeRef);
    /* v8 ignore next -- @preserve head is always set after a successful edit */
    const head = meta.manager.head?.editHistoryRef;
    /* v8 ignore next -- @preserve head is always set after a successful edit */
    if (head === undefined) return null;
    // Read back the `timeId` the manager minted for this edit. The row was
    // just written locally, so this is a local (row-cached) read — and it must
    // be the row's OWN value, not an approximation: every node orders this
    // edit by exactly this string, so a locally invented one would make this
    // node resolve a conflict differently from the rest of the fleet.
    const row = await this._row(`${meta.cakeKey}EditHistory`, head);
    return { head, timeId: row?.['timeId'] as string | undefined };
  }

  /**
   * The current head editHistory ref for a collection, or `null`.
   * @param collection - The collection.
   * @returns The head ref or `null`.
   */
  headRef(collection: string): string | null {
    return this._cakes.get(collection)?.manager.head?.editHistoryRef ?? null;
  }

  // ...........................................................................
  /**
   * Reads one row of a table by its hash. In live sync this pulls the row
   * through the client's `IoMulti` from whichever peer holds it.
   * @param table - The table key.
   * @param hash - The row hash.
   * @returns The row, or `undefined` if not resolvable.
   */
  private async _row(
    table: string,
    hash: string,
  ): Promise<Record<string, unknown> | undefined> {
    const got = await (
      this._db as unknown as {
        get: (
          r: Route,
          w: object,
        ) => Promise<{ rljson?: Record<string, { _data?: unknown[] }> }>;
      }
    ).get(Route.fromFlat(table), { _hash: hash });
    return got?.rljson?.[table]?._data?.[0] as
      | Record<string, unknown>
      | undefined;
  }

  // ...........................................................................
  /**
   * Reads many rows by content hash, trying BOTH relay paths before calling a
   * row missing.
   *
   * `readRowsByHashes` is the batch path: one round trip for the whole set
   * instead of one per row, and it is the path a relay forwards for a content
   * hash. The per-row `Db.get` path is kept as a fallback because a readable
   * may not implement the batch method. Trying both is the difference between
   * "this row is nowhere in the fleet" and "the path I happened to pick could
   * not deliver it" — the latter used to truncate the chain walk and get the
   * whole head discarded.
   * @param table - The table key.
   * @param hashes - The row hashes to read.
   * @returns Hash → row, for every hash that resolved.
   */
  private async _rows(
    table: string,
    hashes: string[],
  ): Promise<Map<string, Record<string, unknown>>> {
    const out = new Map<string, Record<string, unknown>>();
    if (hashes.length === 0) return out;

    try {
      const batch = await this._db.core.readRowsByHashes(table, hashes);
      for (const [hash, row] of batch) {
        out.set(hash, row as unknown as Record<string, unknown>);
      }
    } catch {
      // The batch path is unavailable for this table right now — fall through
      // to the per-row path rather than declaring the rows missing.
    }

    const missing = hashes.filter((hash) => !out.has(hash));
    if (missing.length === 0) return out;

    const rows = await Promise.all(
      missing.map((hash) => this._row(table, hash).catch(() => undefined)),
    );
    for (let i = 0; i < missing.length; i++) {
      const row = rows[i];
      if (row) out.set(missing[i], row);
    }
    return out;
  }

  /**
   * The chain table keys of a collection's cake.
   * @param meta - The collection's cake metadata.
   * @returns The editHistory, multiEdit and edit table keys.
   */
  private static _tables(meta: CakeMeta): {
    eh: string;
    me: string;
    ed: string;
  } {
    return {
      eh: `${meta.cakeKey}EditHistory`,
      me: `${meta.cakeKey}MultiEdits`,
      ed: `${meta.cakeKey}Edits`,
    };
  }

  /**
   * Turns a `putComponent` edit row into a {@link CollectedPut}.
   * @param edit - The edit row.
   * @param timeId - The `timeId` of the EditHistory row that carries it.
   * @returns The put, or `undefined` when the row is not a `putComponent`.
   */
  private _putOf(
    edit: Record<string, unknown> | undefined,
    timeId: string | undefined,
  ): CollectedPut | undefined {
    const action = edit?.['action'] as
      | {
          type?: string;
          data?: {
            layer: string;
            sliceId: string;
            component: Record<string, unknown>;
          };
        }
      | undefined;
    if (action?.type !== 'putComponent' || !action.data) return undefined;
    return {
      collection: this.collectionForLayer(action.data.layer),
      sliceId: action.data.sliceId,
      doc: componentToDoc(action.data.component as never),
      timeId,
    };
  }

  /**
   * Consumer: walks a collection cake's `EditHistory.previous` chain back from
   * `headRef` until it reaches an already-applied ref, returning the
   * `putComponent` upserts in apply order (oldest first). Each edit carries the
   * whole component, so no cake/join reconstruction is needed.
   *
   * `stopAt` is a SET of applied refs, not a single one. Every node keeps its
   * own edit chain — chains are never merged — so a fleet of n nodes has n
   * independent lineages, and a receiver applies heads from all of them. With
   * a single stop ref the walk could only ever terminate on the lineage that
   * spoke last: a head from any other lineage was walked to its ROOT and
   * replayed in full, writing that node's stale document versions over newer
   * ones and re-creating documents another node had deleted. Stopping at ANY
   * applied ancestor keeps every lineage incremental.
   *
   * The walk is level-order and BATCHED: one `readRowsByHashes` per level and
   * per table rather than three per chain link, which is what keeps a long
   * chain from turning into thousands of sequential relay round trips.
   * @param collection - The collection whose cake the head belongs to.
   * @param headRef - The incoming head editHistory ref.
   * @param stopAt - Already-applied ref(s) to stop the walk at (exclusive);
   *   omit for a fresh receiver.
   * @returns The upserts to apply (oldest first), whether the walk resolved
   *   completely, and the refs that are safe to remember as applied — see
   *   {@link CollectPutsResult}.
   */
  async collectPuts(
    collection: string,
    headRef: string,
    stopAt?: string | ReadonlySet<string>,
  ): Promise<CollectPutsResult> {
    const meta = this._cakes.get(collection);
    if (!meta) return { puts: [], complete: true, sealed: [] };
    const { eh: ehTable, me: meTable, ed: edTable } =
      MongoEditAdapter._tables(meta);

    const stop: ReadonlySet<string> =
      typeof stopAt === 'string'
        ? new Set([stopAt])
        : (stopAt ?? new Set<string>());

    let complete = true;
    // Discovery order, newest first (level order from the head).
    const order: string[] = [];
    const rows = new Map<string, Record<string, unknown>>();
    const seen = new Set<string>();
    let frontier = [headRef];

    while (frontier.length > 0) {
      const wanted = frontier.filter(
        (ref) => !stop.has(ref) && !seen.has(ref) && !!seen.add(ref),
      );
      if (wanted.length === 0) break;
      if (order.length + wanted.length > this._maxWalk) {
        // A walk this deep is a cold replay of a whole lineage, not an
        // incremental catch-up. Stop and report the chain truncated: the
        // resolvable part is still applied, and the rest arrives on the next
        // head. Left unbounded this pins a core for minutes on a mega chain.
        complete = false;
        break;
      }
      const fetched = await this._rows(ehTable, wanted);
      const next: string[] = [];
      for (const ref of wanted) {
        const row = fetched.get(ref);
        if (!row) {
          // A history row we needed is not resolvable through the hub, on
          // either read path: its `previous` — and every ancestor beyond it —
          // is unknown. Flag the walk truncated; the rows that DID resolve are
          // still returned, and applying them cannot do harm because an apply
          // never moves a document backwards.
          complete = false;
          continue;
        }
        rows.set(ref, row);
        order.push(ref);
        for (const previous of (row['previous'] ?? []) as string[]) {
          next.push(previous);
        }
      }
      frontier = next;
    }

    // Resolve the multiEdit and edit rows of the whole walk in ONE batch each —
    // three round trips for the whole chain instead of three per link, which is
    // what kept a long chain from turning into thousands of sequential relay
    // reads. Collecting into a Set also drops the `undefined` a malformed row
    // would contribute, and dedupes refs shared by several nodes.
    const meRefs = new Set<string>();
    for (const ref of order) {
      meRefs.add((rows.get(ref) as Record<string, unknown>)[
        'multiEditRef'
      ] as string);
    }
    meRefs.delete(undefined as unknown as string);
    const multiEdits = await this._rows(meTable, [...meRefs]);
    const edRefs = new Set<string>();
    for (const multiEdit of multiEdits.values()) {
      edRefs.add(multiEdit['edit'] as string);
    }
    edRefs.delete(undefined as unknown as string);
    const edits = await this._rows(edTable, [...edRefs]);

    // Oldest first: `previous` links point backwards, so reversing the
    // level-order discovery yields apply order.
    const applyOrder = [...order].reverse();
    const puts: CollectedPut[] = [];
    const resolved = new Set<string>();
    for (const ehRef of applyOrder) {
      const eh = rows.get(ehRef) as Record<string, unknown>;
      // A history row whose multiEdit or edit did not resolve is a hole in the
      // chain just like a missing history row: referenced, but not delivered by
      // any read path.
      const multiEdit = multiEdits.get(eh['multiEditRef'] as string);
      const edit = edits.get(multiEdit?.['edit'] as string);
      if (!edit) {
        complete = false;
        continue;
      }
      resolved.add(ehRef);
      const put = this._putOf(edit, eh['timeId'] as string | undefined);
      if (put) puts.push(put);
    }

    // A ref may be remembered as applied only once its whole ancestry is
    // accounted for. `applyOrder` runs oldest first and `previous` holds at
    // most one ref, so one pass decides every node; anything undecided stays
    // unsealed, which is the safe direction.
    const sealed: string[] = [];
    const sealedSet = new Set<string>();
    for (const ehRef of applyOrder) {
      if (!resolved.has(ehRef)) continue;
      const previous = (rows.get(ehRef)?.['previous'] ?? []) as string[];
      if (!previous.every((ref) => stop.has(ref) || sealedSet.has(ref))) {
        continue;
      }
      sealedSet.add(ehRef);
      sealed.push(ehRef);
    }

    return { puts, complete, sealed };
  }

  // ...........................................................................
  /**
   * The newest edit `timeId` per document, derived from the chain rows this
   * node holds LOCALLY, restricted to documents whose current content that
   * edit actually produced.
   *
   * A restarted node has no memory of what it applied, so the first head from
   * any lineage is walked to its root and replayed in full. Without an ordering
   * seed that replay writes a peer's stale versions over documents another peer
   * has since moved on — the divergence survives the restart, because the
   * lineage that spoke last decides. Seeding from the local chain restores the
   * ordering for free.
   *
   * The `manifest` filter is what makes it SOUND: the local store also holds
   * rows that were pulled but never applied (a truncated pull, a head that was
   * dropped). Only a document whose live content equals what the newest local
   * edit produced is proof that this node applied that edit, so only those are
   * seeded; every other document keeps no entry and is applied normally.
   * @param collection - The collection to seed.
   * @param manifest - Live `sliceId → document content hash` for the
   *   collection.
   * @param maxRows - Give up (returning an empty map) above this many chain
   *   rows, so a mega chain cannot stall start-up.
   * @returns `sliceId → timeId` for the documents that could be attributed.
   */
  async latestTimeIds(
    collection: string,
    manifest: ReadonlyMap<string, string>,
    maxRows: number,
  ): Promise<Map<string, string>> {
    const out = new Map<string, string>();
    const meta = this._cakes.get(collection);
    if (!meta) return out;
    const { eh: ehTable, me: meTable, ed: edTable } =
      MongoEditAdapter._tables(meta);

    const dump = async (
      table: string,
    ): Promise<Array<Record<string, unknown>> | null> => {
      try {
        const rljson = (await this._db.core.dumpTable(table)) as Record<
          string,
          { _data?: Array<Record<string, unknown>> }
        >;
        const data = rljson?.[table]?._data ?? [];
        return data.length > maxRows ? null : data;
      } catch {
        /* v8 ignore next -- @preserve a table that does not exist locally */
        return [];
      }
    };

    const ehRows = await dump(ehTable);
    const meRows = ehRows ? await dump(meTable) : null;
    const edRows = meRows ? await dump(edTable) : null;
    if (!ehRows || !meRows || !edRows) return out;

    const multiEdits = new Map<string, Record<string, unknown>>();
    for (const row of meRows) {
      multiEdits.set(row['_hash'] as string, row);
    }
    const edits = new Map<string, Record<string, unknown>>();
    for (const row of edRows) edits.set(row['_hash'] as string, row);

    // Newest edit per document across everything this node holds locally.
    const newest = new Map<string, { timeId: string; put: CollectedPut }>();
    for (const eh of ehRows) {
      const timeId = eh['timeId'] as string | undefined;
      if (!timeId) continue;
      const meRef = eh['multiEditRef'] as string | undefined;
      const edRef = meRef
        ? (multiEdits.get(meRef)?.['edit'] as string | undefined)
        : undefined;
      const put = this._putOf(edRef ? edits.get(edRef) : undefined, timeId);
      if (!put) continue;
      const previous = newest.get(put.sliceId);
      if (previous && compareTimeId(timeId, previous.timeId) <= 0) continue;
      newest.set(put.sliceId, { timeId, put });
    }

    for (const [sliceId, { timeId, put }] of newest) {
      if (manifest.get(sliceId) === docHash(put.doc)) out.set(sliceId, timeId);
    }
    return out;
  }
}
