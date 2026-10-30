// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

// .............................................................................
// The fs-agent 0.2 architecture, ported 1:1 to Mongo collections instead of
// filesystem folders — per Maximilian's direction (mongo-agent#53 review, and
// the follow-up message that scoped this out): "eigene Kette mit einem
// Snapshot der Collection pro Eintrag (wie treeRef)... keine Zwischenschritte
// mehr auf dem alten Modell."
//
// WHAT THIS RECORDS
// One entry per batch of document changes: the collection's resulting content
// root (`snapshotRef` — mongo-agent already computes this incrementally as
// `_contentRoot`, the XOR-folded entry-digest accumulator; it is exactly
// fs-agent's `treeRef`, just for a flat id→hash map instead of a path tree),
// which sliceIds (stringified `_id`s) it added or modified, which it REMOVED,
// and a fleet-wide `timeId`. A removal is the part no content hash alone can
// express — a deleted document is simply absent from the next snapshot, and
// nothing says the absence was deliberate unless an entry here states it.
//
// WHY THE ROWS ARE WRITTEN BY HAND
// `MultiEditManager` (`@rljson/db`) is the ordinary way to append an edit in
// this codebase's OLD model, and it is not usable here, for the same two
// reasons fs-agent's own `FsEditChain` gives:
//
//   - it refuses more than one `previous` ("has multiple previous refs. Not
//     supported"), and a MERGE entry has two parents by design (two forked
//     lineages reconciled); and
//   - its head tracks ONE node's lineage only, continuously, which is fine
//     for fs-agent's single-folder-per-route model but was exactly the
//     property the OLD mongo-agent model had too — confirmed by reading
//     `MultiEditManager.edit()`: a node's first-ever local edit always writes
//     `previous: null`, a disconnected root. Nodes never adopted a peer's
//     head into their own lineage, which is why `relate()` answered `fork`
//     to EVERY cross-node comparison in the old model — not rarely, which is
//     exactly what broke the first attempt at porting fs-agent's C4 rule
//     straight onto it.
//
// `EditHistory.previous` is `string[]` in the type, so a two-parent merge
// entry is representable — it is only `MultiEditManager` that will not write
// one. Writing the three chain tables by hand, exactly like `FsEditChain`, is
// what makes the receive()/adopt/merge model (ported next on this branch)
// possible at all.
//
// WHY A ROOT IS IDENTIFIED BY CONTENT, NOT BY WHO BOOTED FIRST
// An entry with no parents and nothing changed or removed is not an edit — it
// is the name a node gives the state it already holds when it starts. Every
// node that starts from the SAME baseline content computes the SAME
// `snapshotRef`, and with a content-derived `timeId` (`0:${snapshotRef}`,
// never a minted one) every field of the row is a function of that content —
// so the row, and therefore its `_hash` (its ref), is IDENTICAL on every node
// that starts from the same collection state. Two nodes that have never
// exchanged a single ref now share a genuine common ancestor the moment they
// are asked to `relate()`, which is the one thing the old per-node-minted-root
// model could never give them.
// .............................................................................

import type { Db } from '@rljson/db';
import { hip } from '@rljson/hash';
import type { Json } from '@rljson/json';
import {
  createEditHistoryTableCfg,
  createEditTableCfg,
  createMultiEditTableCfg,
  timeId,
  type Edit,
  type EditHistory,
} from '@rljson/rljson';
import type { Document } from 'mongodb';

import { bodyToDoc, docToBody, mongoCanonical } from './mongo-component-codec.ts';

/** The `EditAction.type` every mongo chain entry carries. */
export const MONGO_EDIT_ACTION = 'putSnapshot';

/**
 * How far back a walk goes before it gives up.
 *
 * A walk this deep is a cold replay of a whole lineage rather than a
 * catch-up, and left unbounded it pins a core on a long chain.
 *
 * Exhausting it is NOT the same as failing to read a row — see
 * {@link MongoEditChain.relate}. Conflating the two made any node more than
 * this many pushes into its own history stop repairing, permanently and
 * silently.
 */
export const DEFAULT_MAX_WALK = 500;

/** What one chain entry says happened. */
export interface MongoEditData extends Json {
  /** The collection's content root after this entry. */
  snapshotRef: string;
  /** sliceId (stringified `_id`) → the changed document's component hash. */
  changed: Record<string, string>;
  /** sliceIds (stringified `_id`s) REMOVED, sorted. */
  removed: string[];
}

/** What {@link MongoEditChain.append} records. */
export interface MongoAppendOptions {
  /** The collection's content root this entry results in. */
  snapshotRef: string;
  /** sliceId → the document, for every id added or modified. */
  changed?: ReadonlyMap<string, Document>;
  /** sliceIds removed. Default: none. */
  removed?: readonly string[];
  /**
   * The entry's `timeId`, when it has to be a function of its content: two
   * nodes that start from the same baseline write the same root entry.
   * Minted otherwise.
   */
  timeId?: string;
  /**
   * What this entry was made from: none for a root, this node's own head for
   * an ordinary edit, both sides' heads for a merge — the one shape
   * `MultiEditManager` refuses and the reason these rows are written by hand.
   */
  previous: readonly string[];
}

/** One entry of the chain, as read back. */
export interface MongoChainEntry {
  /** The `EditHistory` ref — this entry's identity, and the head to announce. */
  head: string;
  /**
   * Fleet-wide order. Minted once, by the node that made the change. Not a
   * conflict resolver any more (see `README.architecture.md`) — only a
   * tie-break, for the rare case even the chain and the hub's stamp cannot
   * order two entries.
   */
  timeId: string;
  /** The collection's content root this entry results in. */
  snapshotRef: string;
  /** The entries this one was made from. Empty for a lineage root. */
  previous: string[];
  /** sliceId → the changed document's component hash. */
  changed: Record<string, string>;
  /** sliceIds removed. */
  removed: string[];
}

/** What {@link MongoEditChain.relate} answers. */
export type ChainRelation =
  | { verdict: 'same' | 'behind' | 'ahead' | 'incomplete' }
  | { verdict: 'fork'; base: string | undefined };

/**
 * Builds the (agnostic) components table config for one collection's document
 * store. The whole document rides in one canonical-EJSON `e` string column
 * (BSON types preserved, deterministic across nodes) — a document's arbitrary
 * fields never need their own declared columns, which the generic table-data
 * validator would otherwise reject outright. Exactly the old model's
 * `backfillTableCfg` (`mongo-edit-adapter.ts`), proven at CARAT's scale.
 * @param key - The components table key.
 * @returns The table configuration.
 */
const componentsTableCfg = (key: string): Json =>
  hip({
    key,
    type: 'components',
    columns: [
      { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'Hash' },
      { key: '_id', type: 'string', titleLong: 'Id', titleShort: 'Id' },
      { key: 'e', type: 'string', titleLong: 'Ejson', titleShort: 'Ejson' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
    _hash: '',
  }) as unknown as Json;

/**
 * The append-only chain for one collection, plus its content-addressed
 * document store.
 *
 * One lineage per NODE, same as fs-agent's per-folder chain: nodes never
 * rewrite or prune an entry, and the chain grows for the life of the
 * collection. What stops two nodes' lineages from staying disjoint forever is
 * NOT this class — it is `receive()`'s `adopt`, which repoints a node's own
 * next `append()` at a peer's head once the two are known to relate.
 */
export class MongoEditChain {
  /** collection → its chain base key (`Db.add*`'s `cakeKey` argument). */
  private readonly _keys = new Map<string, string>();

  /** The last own-lineage walk, keyed by the head it was computed for. */
  private readonly _lineageCache = new Map<
    string,
    { head: string; refs: ReadonlySet<string> }
  >();

  constructor(
    private readonly _db: Db,
    private readonly _prefix: string,
  ) {}

  /**
   * The chain base key for a collection — `Db.addEdit`/`addMultiEdit`/
   * `addEditHistory`'s `cakeKey` argument, which each derives its own table
   * name from (`${key}Edits`, `${key}MultiEdits`, `${key}EditHistory`).
   * @param collection - The Mongo collection name.
   * @returns The base key.
   */
  chainKey(collection: string): string {
    let key = this._keys.get(collection);
    if (!key) {
      const safe = collection.replace(/[^a-zA-Z0-9]/g, '');
      const cap = safe.charAt(0).toUpperCase() + safe.slice(1);
      key = `${this._prefix}Chain${cap}`;
      this._keys.set(collection, key);
    }
    return key;
  }

  /**
   * The components table key for a collection's document store — separate
   * from {@link chainKey} because it is not one of `Db`'s three fixed
   * `add*`-suffixed tables.
   * @param collection - The Mongo collection name.
   * @returns The components table key.
   */
  private _compKey(collection: string): string {
    return `${this.chainKey(collection)}Components`;
  }

  /**
   * Creates one collection's chain + components tables, idempotently.
   *
   * `createEditHistoryTableCfg`'s `dataRef` column is a foreign key into a
   * table named exactly `key` (the OLD model's cake table) — a bare
   * `components` table of our own, holding one tiny marker row per distinct
   * `snapshotRef` ever appended, is what satisfies that reference. The
   * `snapshotRef` ITSELF still travels in the edit's own data (see
   * {@link append}/{@link entry}): this table exists to be validly
   * referenced, not to be read from.
   * @param collection - The Mongo collection name.
   */
  async init(collection: string): Promise<void> {
    const key = this.chainKey(collection);
    const core = this._db.core;
    await core.createTable(createEditTableCfg(key));
    await core.createTable(createMultiEditTableCfg(key));
    await core.createTable(createEditHistoryTableCfg(key));
    await core.createTable(componentsTableCfg(key) as never);
    await core.createTable(componentsTableCfg(this._compKey(collection)) as never);
  }

  /**
   * Marks a `snapshotRef` as known, idempotently, and returns the marker
   * row's hash — what `EditHistory.dataRef` must be set to.
   * @param collection - The collection.
   * @param snapshotRef - The content root this entry results in.
   * @returns The marker row's hash.
   */
  private async _markSnapshot(
    collection: string,
    snapshotRef: string,
  ): Promise<string> {
    const key = this.chainKey(collection);
    // `_id` (not a made-up column) carries the value: the marker table's
    // config declares only `_hash`/`_id` (see `componentsTableCfg`), and the
    // generic table-data validator rejects an undeclared column outright.
    const marker = hip({ _id: snapshotRef, _hash: '' });
    await this._db.core.import({
      [key]: { _type: 'components', _data: [marker] },
    } as never);
    return marker._hash as string;
  }

  /**
   * Stores a document content-addressed, idempotently, and returns its
   * component hash — what a {@link MongoAppendOptions.changed} entry's value
   * must be.
   * @param collection - The collection the document belongs to.
   * @param doc - The Mongo document.
   * @returns The document's component hash.
   */
  async putComponent(collection: string, doc: Document): Promise<string> {
    const comp = this._compKey(collection);
    const row = hip({
      _id: String(doc['_id']),
      e: JSON.stringify(docToBody(doc)),
      _hash: '',
    });
    await this._db.core.import({
      [comp]: { _type: 'components', _data: [row] },
    } as never);
    return row._hash as string;
  }

  /**
   * Reads a document back by its component hash.
   * @param collection - The collection the document belongs to.
   * @param hash - The component hash ({@link putComponent}'s return value).
   * @returns The document, or `undefined` when this node cannot resolve it.
   */
  async getComponent(
    collection: string,
    hash: string,
  ): Promise<Document | undefined> {
    const comp = this._compKey(collection);
    try {
      const result = await this._db.core.readRows(comp, { _hash: hash });
      const row = (result as Record<string, { _data?: Array<Record<string, unknown>> }>)[
        comp
      ]?._data?.[0];
      const e = row?.['e'];
      if (typeof e !== 'string') return undefined;
      // Pin to the exact shape Mongo returns on a read, so this pulled doc's
      // hash equals the hash of the same doc read natively on every peer —
      // see `mongo-edit-adapter.ts`'s `pullComponents`, which this mirrors.
      return mongoCanonical(bodyToDoc(JSON.parse(e) as Record<string, unknown>));
    } catch {
      return undefined;
    }
  }

  /**
   * Appends one entry.
   * @param collection - The collection the entry belongs to.
   * @param opts - What the change was; see {@link MongoAppendOptions}.
   * @returns The entry that was written.
   */
  async append(
    collection: string,
    opts: MongoAppendOptions,
  ): Promise<MongoChainEntry> {
    const key = this.chainKey(collection);
    const previous = [...opts.previous];
    const removed = [...(opts.removed ?? [])].sort();

    const changed: Record<string, string> = {};
    for (const [sliceId, doc] of opts.changed ?? []) {
      changed[sliceId] = await this.putComponent(collection, doc);
    }

    const data: MongoEditData = { snapshotRef: opts.snapshotRef, changed, removed };
    const edit = {
      name: `${MONGO_EDIT_ACTION} ${opts.snapshotRef}`,
      action: {
        name: MONGO_EDIT_ACTION,
        type: MONGO_EDIT_ACTION,
        data: data as unknown as Json,
        _hash: '',
      },
      _hash: '',
    } as unknown as Edit;

    const editRef = this._refOf(await this._db.addEdit(key, edit), key, 'Edits');
    const multiEditRef = this._refOf(
      await this._db.addMultiEdit(key, {
        // A single-entry multiEdit per chain link — mongo has no notion of
        // several edits landing as one unit, and inventing one here would be
        // a shape nothing produces.
        previous: null,
        edit: editRef,
        _hash: '',
      }),
      key,
      'MultiEdits',
    );

    const isRoot =
      previous.length === 0 &&
      Object.keys(changed).length === 0 &&
      removed.length === 0;
    const stamp = opts.timeId ?? (isRoot ? `0:${opts.snapshotRef}` : timeId());
    const dataRef = await this._markSnapshot(collection, opts.snapshotRef);
    const head = this._refOf(
      await this._db.addEditHistory(key, {
        timeId: stamp,
        multiEditRef,
        dataRef,
        previous,
        _hash: '',
      } as EditHistory),
      key,
      'EditHistory',
    );

    // EXTENDED, not invalidated, for the ordinary case of appending onto our
    // own head — see `FsEditChain.append`'s identical reasoning: a merge, or
    // an entry parented on an older head, may reach only a strict subset of
    // what is cached, and a stop set that is too LARGE stops a later walk
    // short.
    const cached = this._lineageCache.get(collection);
    if (cached !== undefined && previous.length === 1 && previous[0] === cached.head) {
      const refs = new Set(cached.refs);
      refs.add(head);
      this._lineageCache.set(collection, { head, refs });
    }

    return {
      head,
      timeId: stamp,
      snapshotRef: opts.snapshotRef,
      previous,
      changed,
      removed,
    };
  }

  /**
   * Reads one entry back.
   * @param collection - The collection the entry belongs to.
   * @param head - The `EditHistory` ref to read.
   * @returns The entry, or `undefined` when this node cannot resolve it — a
   *   ref a peer holds and we do not is the ordinary case, not an error.
   */
  async entry(
    collection: string,
    head: string,
  ): Promise<MongoChainEntry | undefined> {
    const key = this.chainKey(collection);
    const histories = await this._db.getEditHistories(key, head);
    if (histories.length === 0) return undefined;
    const history = histories[0];

    const multiEdits = await this._db.getMultiEdits(key, history.multiEditRef);
    if (multiEdits.length === 0) return undefined;
    const edits = await this._db.getEdits(key, multiEdits[0].edit);
    if (edits.length === 0) return undefined;

    const data = edits[0].action?.data as unknown as MongoEditData | undefined;
    // The recursive content-hasher that stores every nested object attaches
    // its own `_hash` to `changed` (a plain object, unlike `removed`, an
    // array) — not a sliceId, and never one `putComponent` could mint.
    const changed: Record<string, string> = {};
    for (const [sliceId, hash] of Object.entries(data?.changed ?? {})) {
      if (sliceId !== '_hash') changed[sliceId] = hash;
    }
    return {
      head,
      timeId: history.timeId,
      // Not `history.dataRef` — that is a foreign key into the marker table
      // `init()` creates purely to satisfy `createEditHistoryTableCfg`'s
      // schema (see {@link _markSnapshot}). The real value travels in the
      // edit's own data, alongside `changed`/`removed`.
      snapshotRef: data?.snapshotRef as string,
      previous: [...(history.previous ?? [])],
      changed,
      removed: [...(data?.removed ?? [])],
    };
  }

  /**
   * How two heads stand, and for a fork the ancestor to merge against.
   *
   * `same` — the one head. `behind` — theirs descends from ours. `ahead` —
   * ours descends from theirs. A descent found is proof, whatever else in
   * either history cannot be read. `fork` — neither descends from the other:
   * `base` is a common ancestor no other common ancestor descends from, the
   * smallest ref of them when there are several, so every node merging the
   * same two heads merges against the same base. `undefined` when the two
   * share no history within the walk. `incomplete` — no descent was found and
   * an entry could not be read, so a fork cannot be told from a lag.
   *
   * A walk that exhausts `maxWalk` with everything readable answers `fork`:
   * less precise than the truth, never destructive.
   * @param collection - The collection both heads belong to.
   * @param ours - This node's head.
   * @param theirs - The head that arrived.
   * @param maxWalk - Give up past this many entries per side.
   * @returns The relation, and the merge base of a fork.
   */
  async relate(
    collection: string,
    ours: string,
    theirs: string,
    maxWalk = DEFAULT_MAX_WALK,
  ): Promise<ChainRelation> {
    if (ours === theirs) return { verdict: 'same' };

    const parentsOf = async (
      head: string,
    ): Promise<{ parents: Map<string, string[]>; holed: boolean }> => {
      const parents = new Map<string, string[]>();
      let holed = false;
      let frontier = [head];
      while (frontier.length > 0 && parents.size < maxWalk) {
        const wanted = frontier.filter((ref) => !parents.has(ref));
        if (wanted.length === 0) break;
        const next: string[] = [];
        for (const ref of new Set(wanted)) {
          const entry = await this.entry(collection, ref);
          if (!entry) {
            holed = true;
            continue;
          }
          parents.set(ref, entry.previous);
          next.push(...entry.previous);
        }
        frontier = next;
      }
      return { parents, holed };
    };

    const fromTheirs = await parentsOf(theirs);
    if (fromTheirs.parents.has(ours)) return { verdict: 'behind' };
    const fromOurs = await parentsOf(ours);
    if (fromOurs.parents.has(theirs)) return { verdict: 'ahead' };
    if (fromTheirs.holed || fromOurs.holed) return { verdict: 'incomplete' };

    const common = [...fromOurs.parents.keys()].filter((ref) =>
      fromTheirs.parents.has(ref),
    );
    const dominated = new Set<string>();
    for (const ref of common) {
      const stack = [...(fromOurs.parents.get(ref) as string[])];
      while (stack.length > 0) {
        const below = stack.pop() as string;
        if (dominated.has(below)) continue;
        dominated.add(below);
        stack.push(...(fromOurs.parents.get(below) ?? []));
      }
    }
    const best = common.filter((ref) => !dominated.has(ref)).sort();
    return { verdict: 'fork', base: best[0] };
  }

  /**
   * The entries between an ancestor and a descendant, oldest first — what a
   * `behind` apply (see `mongo-apply-behind.ts`) must replay to turn the
   * ancestor's state into the descendant's.
   *
   * Collects every entry `descendant` descends from, down to (but not
   * including) `ancestor`, then orders them so each entry follows every ref
   * in its own `previous` — the order a replay must write them in, so a
   * later entry's `changed`/`removed` for the same id is what ends up
   * applied, matching the chain's own history. A merge entry whose other
   * parent lies outside what was collected can never become ready, so the
   * order is also a completeness check: {@link relate}'s `behind` only
   * proves ONE path exists, not that every entry it passes through was
   * reached inside `maxWalk`.
   * @param collection - The collection.
   * @param ancestor - The head already applied; not included in the result.
   * @param descendant - The head to reach.
   * @param maxWalk - Give up past this many entries.
   * @returns The entries in replay order, or `undefined` when `descendant`
   *   cannot be shown to descend from `ancestor` within `maxWalk`, or an
   *   entry along the way could not be read.
   */
  async path(
    collection: string,
    ancestor: string,
    descendant: string,
    maxWalk = DEFAULT_MAX_WALK,
  ): Promise<MongoChainEntry[] | undefined> {
    if (ancestor === descendant) return [];

    const collected = new Map<string, MongoChainEntry>();
    let reached = false;
    let frontier = [descendant];
    while (frontier.length > 0 && collected.size < maxWalk) {
      const wanted = [...new Set(frontier)].filter(
        (ref) => ref !== ancestor && !collected.has(ref),
      );
      if (wanted.length === 0) break;
      const next: string[] = [];
      for (const ref of wanted) {
        const entry = await this.entry(collection, ref);
        if (!entry) return undefined;
        collected.set(ref, entry);
        for (const p of entry.previous) {
          if (p === ancestor) reached = true;
          else next.push(p);
        }
      }
      frontier = next;
    }
    if (!reached) return undefined;

    const placed = new Set<string>([ancestor]);
    const ordered: MongoChainEntry[] = [];
    const remaining = new Map(collected);
    while (remaining.size > 0) {
      const ready = [...remaining.values()]
        .filter((e) => e.previous.every((p) => placed.has(p)))
        .sort((a, b) => a.head.localeCompare(b.head));
      if (ready.length === 0) return undefined;
      for (const entry of ready) {
        ordered.push(entry);
        placed.add(entry.head);
        remaining.delete(entry.head);
      }
    }
    return ordered;
  }

  /**
   * Every entry this node's own head descends from, itself included.
   *
   * Cached on the head it was computed for. Announcements arrive in bursts
   * and this node's head changes only when it records an entry, so the walk
   * is done once per state rather than once per announcement.
   * @param collection - The collection.
   * @param ourHead - This node's head, or `undefined` when it has none.
   * @param maxWalk - Give up past this many entries.
   * @returns The refs reached, or `undefined` when the walk could not be
   *   trusted — an unreadable entry means the stop set is unknown, not empty.
   */
  async ownLineage(
    collection: string,
    ourHead: string | undefined,
    maxWalk = DEFAULT_MAX_WALK,
  ): Promise<ReadonlySet<string> | undefined> {
    if (ourHead === undefined) return new Set<string>();
    const cached = this._lineageCache.get(collection);
    if (cached?.head === ourHead) return cached.refs;

    const refs = new Set<string>();
    let holed = false;
    let frontier = [ourHead];
    while (frontier.length > 0) {
      const wanted = frontier.filter((ref) => !refs.has(ref) && !!refs.add(ref));
      if (wanted.length === 0) break;
      if (refs.size > maxWalk) break;
      const next: string[] = [];
      for (const ref of wanted) {
        const entry = await this.entry(collection, ref);
        if (!entry) {
          holed = true;
          continue;
        }
        next.push(...entry.previous);
      }
      frontier = next;
    }
    if (holed) return undefined;
    this._lineageCache.set(collection, { head: ourHead, refs });
    return refs;
  }

  /**
   * Pulls the generated ref out of an insert result.
   * @param result - What `db.add*` returned.
   * @param key - The chain base key.
   * @param suffix - The table suffix, e.g. `Edits`.
   * @returns The ref.
   */
  private _refOf(result: unknown, key: string, suffix: string): string {
    const rows = result as Array<Record<string, string>>;
    const ref = rows?.[0]?.[`${key}${suffix}Ref`];
    /* v8 ignore next -- @preserve a successful insert always returns its ref */
    if (!ref) throw new Error(`MongoEditChain: no ref for ${suffix}`);
    return ref;
  }
}
