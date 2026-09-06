// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import type { Db } from '@rljson/db';
import { createHash } from 'node:crypto';
import type { Db as MongoDb, Document } from 'mongodb';
import { ObjectId } from 'mongodb';

import { AE_BUCKET_COUNT, MongoAntiEntropy } from './mongo-anti-entropy.ts';
import type { AntiEntropyHost } from './mongo-anti-entropy.ts';
import { docHash } from './mongo-component-codec.ts';
import type { CollectPutsResult } from './mongo-edit-adapter.ts';
import { compareTimeId, MongoEditAdapter } from './mongo-edit-adapter.ts';
import type { EditCheckpoint } from './mongo-edit-checkpoint.ts';
import { bucketOf as sharedBucketOf } from './mongo-manifest-hash.ts';

/**
 * Marks a component as a delete tombstone. A delete is modeled as a
 * `putComponent` of `{ _id, [TOMBSTONE_FIELD]: true }` (append-only, content-
 * addressed, same edit chain as an upsert); the consumer turns it into a
 * `deleteOne`. The field never lands in Mongo — a tombstone is deleted, not
 * written.
 */
const TOMBSTONE_FIELD = '__slTombstone';

/**
 * The Mongo collection holding this node's persistent tombstone log: one row
 * per `<collection>|<sliceId>` this node deleted, re-loaded on adopt so the
 * manifest-diff backfill never resurrects a locally-deleted doc after a restart.
 */
const TOMBSTONE_LOG = 'sl_edit_tombstones';

/**
 * Prefix marking a broadcast ref as a CONTENT ROOT
 * (`~R~<collection>:<root>`) rather than an editHistory head
 * (`<collection>:<head>`). CARAT collection names never start with `~R~`, so
 * the two ref kinds never collide.
 */
const ROOT_PREFIX = '~R~';

/**
 * Separator appended to a head ref to carry the content root that head
 * produces: `<collection>:<head>|<root>`. The receiver skips the pull only when
 * that root already equals its own — a self-describing check that needs no peer
 * identity and cannot be confused by a third node's root. Neither an
 * editHistory ref (base64url) nor a 64-hex root contains `|`, so splitting at
 * the last `|` is unambiguous. A ref without the suffix (older peer) simply
 * yields no root, and the receiver pulls instead of skipping.
 */
const ROOT_SEP = '|';

/**
 * Above this manifest size a collection is NOT checkpointed: serializing a
 * multi-million-entry manifest to disk on every change (debounced) blocks the
 * event loop as badly as the old full root re-hash did. Such a collection
 * simply rebuilds its manifest by a full scan on restart (correct, just not
 * instant). Smaller collections still checkpoint so they resume without a scan.
 */
const CHECKPOINT_MAX_ENTRIES = 500_000;

/** The subset of the rljson Connector this sync needs. */
export interface EditSyncConnector {
  /** Broadcasts a ref to peers. Dedups: an already-sent ref is a no-op. */
  send(ref: string): void;
  /** Registers a callback for refs arriving from peers. */
  listen(cb: (ref: string) => void | Promise<void>): void;
  /**
   * Re-broadcasts a ref bypassing the send-dedup, so a peer that joined after
   * the ref was first sent still receives it. The relay only replays a single
   * latest ref to a late joiner, and {@link send} drops re-sends — this raw
   * emit is what actually reaches a late peer. Optional so unit tests can use a
   * plain `{ send, listen }` stub.
   */
  reannounce?(ref: string): void;
  /**
   * Clears a ref from the connector's received-dedup set so a later
   * re-announce of the same ref is delivered again. Used to recover from an
   * empty pull (the origin's rows weren't resolvable yet): without this the
   * head ref is deduped forever and never re-pulled.
   */
  invalidateReceived?(ref: string): void;
}

/** The minimal MongoDB change-stream surface this module uses. */
interface MongoChangeStream {
  on(event: string, listener: (arg: unknown) => void): void;
  close(): Promise<void> | void;
}

/**
 * Live MongoDB ↔ RLJSON components/edits sync over a route.
 *
 * Producer: a change on collection C emits a `putComponent` via
 * {@link MongoEditAdapter} and broadcasts the collection's new head as the ref
 * `"<collection>:<editHistoryRef>"`. Consumer: an incoming ref is split, the
 * new edits since the last-applied head are collected and upserted into Mongo.
 *
 * Echo suppression: every document written from a peer is remembered by its
 * content hash; when the resulting change-stream event fires with that same
 * content it is NOT re-broadcast, breaking the apply→change→broadcast loop.
 * Deletes propagate as a tombstone `putComponent` (a component carrying
 * {@link TOMBSTONE_FIELD}); the consumer turns it into a `deleteOne`. A burst
 * of deletes above the mass-delete threshold is NOT propagated (circuit
 * breaker) so a full restore / empty-DB load on one node can't wipe the peer.
 */
export class MongoEditSync {
  private readonly _adapter: MongoEditAdapter;
  /**
   * collection → every editHistory ref already applied (insertion ordered,
   * FIFO-bounded).
   *
   * This used to be a single "last applied head" per collection, and that is
   * what broke convergence under churn. Chains are never merged: each node
   * appends only its OWN writes to its OWN chain, so a fleet of n nodes has n
   * independent lineages and every receiver applies heads from all of them.
   * A single slot can only ever hold one lineage's tip, so a head from any
   * other lineage had no common ancestor with it, was walked to its ROOT and
   * replayed whole — writing that node's stale versions over newer ones from a
   * third node and re-creating documents someone had deleted. The roots then
   * disagreed, the root handler re-drove the other lineage, and the mesh either
   * oscillated or sat permanently split.
   *
   * Holding the applied refs of ALL lineages makes every walk stop at the
   * first ancestor we already have, whichever lineage the head belongs to.
   */
  private readonly _applied = new Map<string, Set<string>>();
  /**
   * collection → (sliceId → `timeId` of the newest edit applied to it).
   *
   * The convergence guarantee. A `timeId` is minted once by the node that made
   * the edit and travels with the row, so every node orders the same two edits
   * the same way. Applying a put only when it is NEWER than the one a document
   * already carries makes every apply monotonic per document: a replay — a cold
   * start, a truncated pull, an evicted ref — can no longer move a document
   * backwards, and two nodes that saw the same edits in different orders end up
   * with the same document.
   *
   * Only documents that have actually been edited get an entry (the baseline
   * snapshot adds none), so this grows with churn, not with collection size.
   */
  private readonly _appliedTimeId = new Map<string, Map<string, string>>();
  /** `${collection}:${_id}` → component hash last written (peer-applied). */
  private readonly _appliedHash = new Map<string, string>();
  /** collection → serialized apply chain (in-flight guard). */
  private readonly _applyChain = new Map<string, Promise<void>>();
  /**
   * collection → (sliceId → doc content-hash). A CONTENT-DETERMINISTIC
   * snapshot of the collection: two nodes holding the same data have identical
   * manifests (the doc hashes are content-addressed, no time component). The
   * hash of this manifest is the collection's "content root".
   */
  private readonly _manifest = new Map<string, Map<string, string>>();
  /** collection → cached content root (invalidated on a manifest change). */
  private readonly _rootCache = new Map<string, string>();
  /**
   * collection → 32-byte XOR accumulator of every entry digest
   * `sha256(sliceId | docHash)`. XOR is commutative and self-inverse, so the
   * root is maintained in O(1) per change (add a doc: XOR its digest in; remove
   * it: XOR the same digest out; change it: XOR old out then new in) instead of
   * re-sorting and re-hashing the whole manifest. That full re-hash over a mega
   * collection (cd_models ~13M docs) is what wedged the hub: the 10s heartbeat
   * recomputed it for every collection, pegging a core forever. XOR-of-hashes is
   * order-independent, so two nodes with identical data still derive the same
   * root; {@link _contentRoot} just hex-encodes this buffer.
   */
  private readonly _rootAcc = new Map<string, Buffer>();
  /**
   * collection → a single Buffer of `AE_BUCKET_COUNT × 32` bytes: the per-bucket
   * XOR accumulators used by the manifest-diff backfill. Bucket `b` occupies
   * `[b*32, b*32+32)` and holds the XOR of the entry digests of every sliceId
   * whose {@link _bucketOf} is `b`. Maintained incrementally alongside the root
   * accumulator (their full XOR is identical), so two nodes compare
   * `AE_BUCKET_COUNT` cheap per-bucket roots to locate exactly which buckets
   * hold a discrepancy without shipping a multi-million-entry manifest.
   */
  private readonly _bucketAcc = new Map<string, Buffer>();
  /**
   * collection → (sliceId → original typed `_id`) of docs deleted on THIS node,
   * consulted synchronously by the backfill so it never resurrects a locally-
   * deleted doc; the typed `_id` lets the delete be re-materialised against a
   * peer whose `_id` is not a plain string (ObjectId / Int32).
   */
  private readonly _tombstones = new Map<string, Map<string, unknown>>();
  /** collection → earliest time a new backfill round may start (cooldown). */
  private readonly _aeCooldownUntil = new Map<string, number>();
  /**
   * Collections whose cold-start baseline is fully built. The backfill is gated
   * on this: a manifest still being scanned would compare as "everything
   * differs" against a peer and trigger a useless full exchange.
   */
  private readonly _baselineReady = new Set<string>();
  /**
   * `true` once the INITIAL cold-start (adopting every collection present at
   * start-up) has finished. Anti-entropy is gated on this GLOBALLY — while the
   * mega collections are still hashing, a small collection must NOT start
   * pulling disjoint docs (that concurrent load wedged a node mid-cold-start).
   * After cold-start completes, AE runs freely and reconciles large deltas.
   */
  private _coldStartComplete = false;
  /* v8 ignore next -- @preserve backfill on/off, env-overridable */
  private readonly _antiEntropyOn = process.env['SL_EDIT_ANTIENTROPY'] !== '0';
  /* v8 ignore next -- @preserve tombstone-log kill switch, env-overridable */
  private readonly _tombstoneLogOn =
    process.env['SL_EDIT_TOMBSTONE_LOG'] !== '0';
  /* v8 ignore next -- @preserve backfill cooldown ms, env-overridable */
  private readonly _aeCooldownMs =
    Number(process.env['SL_EDIT_AE_COOLDOWN_MS']) || 2_000;
  /* v8 ignore next -- @preserve backfill round timeout ms, env-overridable */
  private readonly _aeRoundTimeoutMs =
    Number(process.env['SL_EDIT_AE_ROUND_TIMEOUT_MS']) || 30_000;
  /** collection → in-flight backfill-round abort timer. */
  private readonly _aeRoundTimers = new Map<
    string,
    ReturnType<typeof setTimeout>
  >();
  /** The manifest-diff backfill engine (bucketed anti-entropy). */
  private readonly _ae: MongoAntiEntropy;
  /**
   * collection → the last head a peer announced, with the content root that
   * head produces and the ref exactly as received. Kept so a later `recv root`
   * that reveals divergence can re-drive `_applyHead` for it — a head whose pull
   * came back empty/partial (the origin's rows were not resolvable yet) is
   * retried this way as well as by the producer's heartbeat re-announce.
   *
   * There is deliberately no `_peerRoot` companion any more. It held one
   * last-writer-wins root shared by every peer and gated the apply short-circuit
   * — so on three or more nodes an unrelated node's root could equal ours and
   * silently swallow this peer's head. The skip now compares against the root
   * carried by the head ref itself (see {@link _headRef}), which is
   * self-describing and peer-count independent.
   */
  private readonly _lastPeerHead = new Map<
    string,
    { head: string; root: string | undefined; ref: string }
  >();
  /** Collections whose next delete burst is deliberate, not an accident. */
  private readonly _expectedMassDelete = new Set<string>();

  /**
   * Announce that the next delete burst on `collection` is intentional, so the
   * mass-delete guard lets it propagate instead of blocking it.
   *
   * The guard is a circuit breaker for an accidental wipe — a restore, an
   * empty-DB load — and it cannot tell one from a deliberate reset. The E2E
   * lab lowers the threshold to 5 so its guard recipe can trip it, which also
   * made every probe-collection reset trip it: one node blocked the reset,
   * kept its documents, and the mesh sat on divergent roots for the rest of
   * the run while four other recipes reported missing documents.
   *
   * Consumed by the next burst on that collection, so it cannot leave the
   * guard disarmed.
   * @param collection - The collection about to be wiped deliberately.
   */
  expectMassDelete(collection: string): void {
    this._expectedMassDelete.add(collection);
  }

  /** collection → the root most recently written to the trace log. */
  private readonly _lastLoggedRoot = new Map<string, string>();

  /** collection → pending debounced root recompute+broadcast timer. */
  private readonly _rootTimers = new Map<
    string,
    ReturnType<typeof setTimeout>
  >();
  private readonly _stop: Array<() => void | Promise<void>> = [];
  /**
   * The collections currently being synced. Mutable: {@link _reconcile} adopts
   * collections created after start-up (see the discovery interval below).
   */
  private readonly _collections: Set<string>;
  /** Collections being adopted because a peer announced them (in-flight guard). */
  private readonly _adoptingOnRef = new Set<string>();
  /* v8 ignore next -- @preserve root-broadcast debounce, env-overridable */
  private readonly _rootDebounceMs =
    Number(process.env['SL_EDIT_ROOT_DEBOUNCE_MS']) || 500;
  /* v8 ignore next -- @preserve heartbeat interval, env-overridable */
  private readonly _heartbeatMs =
    Number(process.env['SL_EDIT_HEARTBEAT_MS']) || 10_000;
  /* v8 ignore next -- @preserve collection-discovery interval, env-overridable */
  private readonly _discoverMs =
    Number(process.env['SL_EDIT_DISCOVER_MS']) || 15_000;
  /* v8 ignore start -- @preserve delete-guard knobs, env-overridable */
  private readonly _deleteDebounceMs =
    Number(process.env['SL_EDIT_DELETE_DEBOUNCE_MS']) || 400;
  private readonly _deleteAbsMax =
    Number(process.env['SL_EDIT_DELETE_ABS_MAX']) || 300_000;
  private readonly _deleteFraction =
    Number(process.env['SL_EDIT_DELETE_FRACTION']) || 0.3;
  /* v8 ignore stop */
  /** collection → doc count at snapshot time (mass-delete-guard baseline). */
  private readonly _baselineCount = new Map<string, number>();
  /** collection → ids pending a debounced delete-burst decision. */
  private readonly _pendingDeletes = new Map<string, Set<unknown>>();
  /** collection → pending delete-flush timer. */
  private readonly _deleteTimers = new Map<
    string,
    ReturnType<typeof setTimeout>
  >();
  /* v8 ignore start -- @preserve pull-retry + checkpoint knobs, env-overridable */
  private readonly _pullRetries =
    Number(process.env['SL_EDIT_PULL_RETRIES']) || 5;
  private readonly _pullBackoffMs =
    Number(process.env['SL_EDIT_PULL_BACKOFF_MS']) || 200;
  private readonly _saveDebounceMs =
    Number(process.env['SL_EDIT_SAVE_DEBOUNCE_MS']) || 1000;
  private readonly _checkpointMaxEntries =
    Number(process.env['SL_EDIT_CHECKPOINT_MAX_ENTRIES']) ||
    CHECKPOINT_MAX_ENTRIES;
  private readonly _maxAppliedRefs =
    Number(process.env['SL_EDIT_APPLIED_MAX']) || 50_000;
  private readonly _maxLwwEntries =
    Number(process.env['SL_EDIT_LWW_MAX']) || 200_000;
  private readonly _seedMaxRows =
    Number(process.env['SL_EDIT_SEED_MAX_ROWS']) || 200_000;
  /* v8 ignore stop */
  /** collection → latest change-stream resume token (for the checkpoint). */
  private readonly _lastToken = new Map<string, unknown>();
  /** collection → pending debounced checkpoint-save timer. */
  private readonly _saveTimers = new Map<
    string,
    ReturnType<typeof setTimeout>
  >();
  /* v8 ignore next -- @preserve diagnostic gate, off in tests */
  private readonly _trace = process.env['SL_EDIT_TRACE'] === '1';

  /* v8 ignore start -- @preserve diagnostic logging only */
  private _log(msg: string): void {
    if (this._trace) console.error(`[edit-sync] ${msg}`);
  }
  /* v8 ignore stop */

  /**
   * Creates a live MongoDB ↔ RLJSON components/edits sync. Call {@link start}
   * to begin the snapshot + change-stream + incoming-ref processing.
   * @param _mongoDb - The MongoDB database.
   * @param db - The RLJSON `Db` (from the client's `IoMulti`).
   * @param _connector - The route connector (send/listen).
   * @param collections - Collections to sync at start-up.
   * @param prefix - Cake-key prefix (e.g. the db name).
   * @param _discover - Optional supplier of the currently-desired collection
   *   set (ignore/include already applied). When given, collections created
   *   after start-up are adopted live on a `SL_EDIT_DISCOVER_MS` interval.
   * @param _checkpoint - Optional on-disk checkpoint store. When given, a
   *   restart resumes the change stream from the saved resume token and
   *   restores the manifest instead of re-scanning the whole collection. Omit
   *   it (the default) for the stateless full-snapshot-on-every-start behavior.
   * @param _shouldSync - Predicate deciding whether a collection this node
   * does NOT have locally should be adopted when a peer announces it.
   */
  constructor(
    private readonly _mongoDb: MongoDb,
    db: Db,
    private readonly _connector: EditSyncConnector,
    collections: string[],
    prefix: string,
    private readonly _discover?: () => Promise<string[]>,
    private readonly _checkpoint?: EditCheckpoint,
    private readonly _shouldSync?: (collection: string) => boolean,
  ) {
    this._adapter = new MongoEditAdapter(db, prefix);
    this._collections = new Set(collections);
    const host: AntiEntropyHost = {
      // Route anti-entropy frames through the dedup/coalesce-BYPASSING raw emit.
      // The ordinary broadcast relay collapses a rapid burst of distinct refs to
      // the single latest one — harmless for idempotent head re-announces, fatal
      // for this protocol, whose every bucket-entry and doc frame must arrive
      // (measured live: the data-source hub's AEE/AED bursts were coalesced away
      // and receivers learned nothing was missing). `reannounce` emits each
      // frame straight on the socket; fall back to `send` only where a stub
      // connector has no reannounce.
      send: (r) =>
        this._connector.reannounce
          ? this._connector.reannounce(r)
          : this._connector.send(r),
      bucketRoots: (c) => this._bucketRoots(c),
      bucketEntries: (c, bs) => this._bucketEntries(c, bs),
      manifestHash: (c, id) => this._manifestOf(c).get(id),
      hasTombstone: (c, id) => !!this._tombstones.get(c)?.has(id),
      pushTombstones: (c, ids) => this._pushTombstones(c, ids),
      serveComponents: (c, ids) => this._serveComponents(c, ids),
      pullAndApply: (c, hs) => this._pullAndApply(c, hs),
      syncs: (c) => this._collections.has(c),
      ready: (c) => this._baselineReady.has(c),
      onRoundComplete: (c) => this._onAeRoundComplete(c),
      log: (m) => this._log(m),
    };
    this._ae = new MongoAntiEntropy(host);
  }

  private _key(collection: string, id: unknown): string {
    return `${collection}:${String(id)}`;
  }

  /**
   * The manifest map for a collection, created on first use.
   * @param collection - The collection.
   * @returns The collection's manifest (sliceId → doc content-hash).
   */
  private _manifestOf(collection: string): Map<string, string> {
    let m = this._manifest.get(collection);
    if (!m) {
      m = new Map();
      this._manifest.set(collection, m);
    }
    return m;
  }

  /**
   * The applied-ref set of a collection, created on first use.
   * @param collection - The collection.
   * @returns The mutable set of applied editHistory refs.
   */
  private _appliedOf(collection: string): Set<string> {
    let refs = this._applied.get(collection);
    if (!refs) {
      refs = new Set();
      this._applied.set(collection, refs);
    }
    return refs;
  }

  /**
   * Remembers editHistory refs as applied, evicting the oldest once the set is
   * full. Eviction only costs a longer walk later — never correctness, because
   * a re-walked edit is still rejected by the per-document `timeId` guard.
   * @param collection - The collection the refs belong to.
   * @param refs - The refs to remember.
   */
  private _markApplied(collection: string, refs: readonly string[]): void {
    const applied = this._appliedOf(collection);
    for (const ref of refs) {
      applied.delete(ref);
      applied.add(ref);
    }
    while (applied.size > this._maxAppliedRefs) {
      const oldest = applied.values().next().value as string;
      applied.delete(oldest);
    }
  }

  /**
   * The applied-`timeId` map of a collection, created on first use.
   * @param collection - The collection.
   * @returns The mutable `sliceId → timeId` map.
   */
  private _timeIdsOf(collection: string): Map<string, string> {
    let timeIds = this._appliedTimeId.get(collection);
    if (!timeIds) {
      timeIds = new Map();
      this._appliedTimeId.set(collection, timeIds);
    }
    return timeIds;
  }

  /**
   * Records the `timeId` of the newest edit applied to a document, evicting
   * the oldest entry once the map is full.
   * @param collection - The collection the document belongs to.
   * @param id - The document `_id`.
   * @param timeId - The edit's `timeId`; ignored when absent.
   */
  private _setAppliedTimeId(
    collection: string,
    id: unknown,
    timeId: string | undefined,
  ): void {
    if (!timeId) return;
    const timeIds = this._timeIdsOf(collection);
    const key = String(id);
    timeIds.delete(key);
    timeIds.set(key, timeId);
    while (timeIds.size > this._maxLwwEntries) {
      const oldest = timeIds.keys().next().value as string;
      timeIds.delete(oldest);
    }
  }

  /**
   * Whether an incoming edit is newer than the one a document already carries.
   * An unknown `timeId` on either side is not comparable and counts as newer,
   * so a peer running an older build still converges the way it used to.
   * @param collection - The collection the document belongs to.
   * @param sliceId - The document's slice id.
   * @param timeId - The incoming edit's `timeId`.
   * @returns `true` when the edit may be applied.
   */
  private _isNewer(
    collection: string,
    sliceId: string,
    timeId: string | undefined,
  ): boolean {
    const current = this._appliedTimeId.get(collection)?.get(sliceId);
    return compareTimeId(timeId, current) >= 0;
  }

  /**
   * The XOR root accumulator for a collection (32 zero bytes on first use).
   * @param collection - The collection.
   * @returns The mutable 32-byte accumulator buffer.
   */
  private _accOf(collection: string): Buffer {
    let a = this._rootAcc.get(collection);
    if (!a) {
      a = Buffer.alloc(32);
      this._rootAcc.set(collection, a);
    }
    return a;
  }

  /**
   * The 32-byte entry digest `sha256(sliceId | docHash)` XOR-combined into the
   * root accumulator. Injective per (sliceId, docHash): the docHash is a
   * fixed-length 64-hex suffix, so the `|`-joined byte stream cannot alias.
   * @param key - The stringified `_id` (sliceId).
   * @param hash - The document content hash.
   * @returns The 32-byte digest.
   */
  private _entryDigest(key: string, hash: string): Buffer {
    return createHash('sha256').update(key).update('|').update(hash).digest();
  }

  /**
   * XORs a 32-byte digest into an accumulator in place and drops the cached
   * hex root (recomputed lazily from the accumulator).
   * @param collection - The collection whose accumulator to update.
   * @param digest - The 32-byte entry digest to fold in (add or, identically,
   *   remove — XOR is self-inverse).
   * @param bucket - The manifest bucket the entry belongs to (its per-bucket
   *   accumulator is updated alongside the whole-collection one).
   */
  private _xorEntry(collection: string, digest: Buffer, bucket: number): void {
    const acc = this._accOf(collection);
    for (let i = 0; i < 32; i++) acc[i] ^= digest[i];
    const bAcc = this._bucketAccOf(collection);
    const off = bucket * 32;
    for (let i = 0; i < 32; i++) bAcc[off + i] ^= digest[i];
    this._rootCache.delete(collection);
  }

  /**
   * The mutable per-bucket XOR accumulator buffer for a collection
   * (`AE_BUCKET_COUNT × 32` zero bytes on first use).
   * @param collection - The collection.
   * @returns The bucket-accumulator buffer.
   */
  private _bucketAccOf(collection: string): Buffer {
    let a = this._bucketAcc.get(collection);
    if (!a) {
      a = Buffer.alloc(AE_BUCKET_COUNT * 32);
      this._bucketAcc.set(collection, a);
    }
    return a;
  }

  /**
   * The manifest bucket for a sliceId (stable FNV-1a fold, identical on every
   * node, keyed on the sliceId alone so an update keeps a doc in its bucket).
   * @param key - The sliceId (stringified `_id`).
   * @returns The bucket index in `[0, AE_BUCKET_COUNT)`.
   */
  private _bucketOf(key: string): number {
    return sharedBucketOf(key);
  }

  // ------ anti-entropy (manifest-diff backfill) host surface ------

  /**
   * The collection's `AE_BUCKET_COUNT` per-bucket roots as 64-hex strings, read
   * straight off the incrementally-maintained bucket accumulator.
   * @param collection - The collection.
   * @returns One 64-hex root per bucket.
   */
  private _bucketRoots(collection: string): string[] {
    const buf = this._bucketAccOf(collection);
    const out = new Array<string>(AE_BUCKET_COUNT);
    for (let b = 0; b < AE_BUCKET_COUNT; b++) {
      out[b] = buf.toString('hex', b * 32, b * 32 + 32);
    }
    return out;
  }

  /**
   * The manifest entries in each requested bucket, in ONE manifest pass (so
   * serving a mega collection's reconciliation is O(size), not O(size × asked)).
   * @param collection - The collection.
   * @param buckets - The bucket indices to return entries for.
   * @returns bucket → its `[sliceId, docHash]` entries.
   */
  private _bucketEntries(
    collection: string,
    buckets: number[],
  ): Map<number, Array<[string, string]>> {
    const want = new Set(buckets);
    const out = new Map<number, Array<[string, string]>>();
    for (const b of buckets) out.set(b, []);
    for (const [key, hash] of this._manifestOf(collection)) {
      const b = this._bucketOf(key);
      if (!want.has(b)) continue;
      (out.get(b) as Array<[string, string]>).push([key, hash]);
    }
    return out;
  }

  /**
   * Candidate typed `_id`s for a sliceId (the string; a 24-hex ObjectId; an
   * all-digit number) so a Mongo `_id: {$in: …}` resolves the common CARAT `_id`
   * shapes (ObjectId / Int32 / string). Over-matching only ever replays a
   * harmless idempotent extra upsert.
   * @param sliceId - The stringified `_id`.
   * @returns The candidate typed `_id`s.
   */
  private _typedIdCandidates(sliceId: string): unknown[] {
    const out: unknown[] = [sliceId];
    if (/^[0-9a-fA-F]{24}$/.test(sliceId) && ObjectId.isValid(sliceId)) {
      out.push(new ObjectId(sliceId));
    }
    if (/^-?\d{1,15}$/.test(sliceId)) out.push(Number(sliceId));
    return out;
  }

  /**
   * Responder half of a backfill: read the requested docs from Mongo and replay
   * each as an ordinary `putDoc` edit, then broadcast the head so the requester's
   * existing head-pull path upserts them. Reusing the live edit path keeps BSON
   * types exact and mints a `timeId`, so the requester applies under per-document
   * last-writer-wins — idempotent, and it can never move a document backwards.
   * @param collection - The collection to replay from.
   * @param sliceIds - The stringified `_id`s the peer is missing.
   */
  private async _serveComponents(
    collection: string,
    sliceIds: string[],
  ): Promise<string[]> {
    const candidates = sliceIds.flatMap((s) => this._typedIdCandidates(s));
    const docs = await this._mongoDb
      .collection(collection)
      .find({ _id: { $in: candidates } } as never)
      .toArray();
    if (docs.length === 0) return [];
    // Publish the docs as content-addressed components (no chain, no head) and
    // hand back only their hashes — the peer pulls the bodies over the flow-
    // controlled read path. The codec is canonical-EJSON, so BSON types survive
    // and every node derives the same component hash for the same doc.
    return this._adapter.importComponents(collection, docs as Document[]);
  }

  /**
   * Requester side of the manifest-level backfill: PULL the offered components
   * by hash (flow-controlled — the hub relays + caches one copy) and upsert each
   * decoded doc, mirroring {@link _applyHead}'s echo suppression (`_appliedHash`)
   * so the change stream folds it into the content root WITHOUT re-broadcasting
   * a fresh edit. No chain is walked and nothing is pushed over the wire in bulk
   * — this is the O(size), scale-independent path a large baseline import needs.
   * Additive only: a sliceId whose content we already hold is skipped, so it can
   * never move a document backwards or overwrite a concurrent local edit.
   * @param collection - The collection to upsert into.
   * @param hashes - The component row hashes to pull.
   */
  private async _pullAndApply(
    collection: string,
    hashes: string[],
  ): Promise<void> {
    const docs = await this._adapter.pullComponents(collection, hashes);
    const ops: Array<Record<string, unknown>> = [];
    for (const doc of docs) {
      const d = doc as Record<string, unknown> & { _id: unknown };
      const hash = docHash(d);
      // Idempotent: we already hold this exact content. Skipping keeps the
      // upsert count — and the change stream it drives — bounded to real change.
      if (this._manifestOf(collection).get(String(d._id)) === hash) continue;
      // Suppress the re-broadcast of the echo this write is about to emit; the
      // change stream then folds each upserted doc into the manifest/content root.
      this._appliedHash.set(this._key(collection, d._id), hash);
      ops.push({
        replaceOne: { filter: { _id: d._id }, replacement: d, upsert: true },
      });
    }
    // ONE round trip for the whole batch — a per-doc `replaceOne` await made the
    // backfill Mongo-round-trip-bound (~100 docs/s); `bulkWrite` moves thousands
    // per call, so the pull rate is what actually limits throughput.
    if (ops.length > 0) {
      await this._mongoDb
        .collection(collection)
        .bulkWrite(ops as never, { ordered: false });
    }
  }

  /**
   * Requester half of a delete-wins reconciliation: a peer still holds docs THIS
   * node deleted, so re-materialise their tombstones (original typed `_id` from
   * the tombstone log) and broadcast the head — the peer's pull applies the
   * delete. Never resurrects; only re-asserts our delete.
   * @param collection - The collection.
   * @param sliceIds - The stringified `_id`s to re-delete on the peer.
   */
  private async _pushTombstones(
    collection: string,
    sliceIds: string[],
  ): Promise<void> {
    const log = this._tombstones.get(collection);
    let head: string | null = null;
    for (const sliceId of sliceIds) {
      const id = log?.get(sliceId);
      if (id === undefined) continue;
      const put = await this._adapter.putDoc(collection, this._tombstone(id));
      head = put?.head ?? null;
      this._setAppliedTimeId(collection, id, put?.timeId);
    }
    if (head) this._connector.send(this._headRef(collection, head));
  }

  /**
   * Triggers a manifest-diff backfill round for a collection, guarded by a
   * cooldown and an abort timer that recovers a round which never completes.
   * No-op while the backfill is off, the GLOBAL cold-start is not yet done, or
   * this collection's baseline is not yet built.
   * @param collection - The collection to reconcile.
   */
  private _maybeTriggerAe(collection: string): void {
    if (!this._antiEntropyOn || !this._coldStartComplete) return;
    if (!this._baselineReady.has(collection)) return;
    const now = Date.now();
    if (now < (this._aeCooldownUntil.get(collection) ?? 0)) return;
    this._aeCooldownUntil.set(collection, now + this._aeCooldownMs);
    // Only arm the abort timer when a NEW round actually starts. `trigger`
    // no-ops while a round is already in flight; re-arming the timer on every
    // heartbeat-driven call (as this used to) pushed the abort deadline forever
    // into the future, so a round that got stuck (a lost entry batch leaving
    // `pending` non-empty) never aborted and that collection never reconciled
    // again — the exact stall observed live. A stuck round now aborts on its own
    // deadline and the next divergence re-triggers it fresh.
    if (!this._ae.trigger(collection)) return;
    const prev = this._aeRoundTimers.get(collection);
    if (prev) clearTimeout(prev);
    const t = setTimeout(() => {
      this._aeRoundTimers.delete(collection);
      this._ae.abort(collection);
    }, this._aeRoundTimeoutMs);
    /* v8 ignore next -- @preserve unref keeps the timer from blocking exit */
    (t as unknown as { unref?: () => void }).unref?.();
    this._aeRoundTimers.set(collection, t);
  }

  /**
   * CHAIN the next backfill round. Called by the anti-entropy engine when a
   * round finishes: if the collection is STILL diverged from the peer root we
   * last saw, drive the next round right away instead of waiting to re-receive
   * the peer's root heartbeat. A live bulk import delivers only one capped chunk
   * per round; the relay does not reliably re-broadcast the (now-changed) root,
   * so without this the pull stalls after the first chunk. Chaining is scoped to
   * exactly the still-diverged collection (not a per-heartbeat scan over all
   * collections, which floods the connector) and is cooldown-gated in
   * {@link _maybeTriggerAe}, so a converged collection stops the chain and a
   * chunk that made no progress cannot spin.
   * @param collection - The collection whose round just finished.
   */
  private _onAeRoundComplete(collection: string): void {
    const peer = this._lastPeerHead.get(collection);
    if (!peer || peer.root === this._contentRoot(collection)) return; // converged
    // `_maybeTriggerAe` is cooldown-gated, and the cooldown was armed when THIS
    // round started; a round that finished faster than the cooldown would be
    // silently dropped and break the chain. Schedule the next round for exactly
    // when the cooldown lapses so the chain always continues (delay 0 when it
    // has already lapsed).
    const delay = Math.max(
      0,
      (this._aeCooldownUntil.get(collection) ?? 0) - Date.now(),
    );
    const t = setTimeout(() => this._maybeTriggerAe(collection), delay);
    /* v8 ignore next -- @preserve unref keeps the timer from blocking exit */
    (t as unknown as { unref?: () => void }).unref?.();
  }

  /**
   * Records a deleted `_id` in the persistent tombstone log (in-memory +
   * {@link TOMBSTONE_LOG}) so the backfill will not resurrect it. Best-effort.
   * @param collection - The collection the delete happened in.
   * @param id - The original typed `_id` that was deleted.
   */
  private _recordTombstone(collection: string, id: unknown): void {
    if (!this._tombstoneLogOn) return;
    let log = this._tombstones.get(collection);
    if (!log) {
      log = new Map();
      this._tombstones.set(collection, log);
    }
    const sliceId = String(id);
    if (log.has(sliceId)) return;
    log.set(sliceId, id);
    // Best-effort persistence: this runs INSIDE `_onDelete`, before the delete's
    // root re-broadcast, so it must never throw — a synchronous failure to reach
    // the tombstone collection (e.g. it does not exist yet) would otherwise abort
    // the whole delete propagation. The in-memory guard above is already set, so
    // a missed persist only costs durability across a restart, not correctness.
    try {
      void this._mongoDb
        .collection(TOMBSTONE_LOG)
        .updateOne(
          { _id: `${collection}${ROOT_SEP}${sliceId}` } as never,
          { $set: { collection, id, at: Date.now() } },
          { upsert: true },
        )
        .catch((e) => this._log(`tombstone persist failed: ${String(e)}`));
    } catch (e) {
      this._log(`tombstone persist threw: ${String(e)}`);
    }
  }

  /**
   * Loads a collection's persisted tombstones into the in-memory guard so a doc
   * deleted before a restart is still not resurrected afterwards.
   * @param collection - The collection to load tombstones for.
   */
  private async _loadTombstones(collection: string): Promise<void> {
    if (!this._tombstoneLogOn) return;
    let log = this._tombstones.get(collection);
    if (!log) {
      log = new Map();
      this._tombstones.set(collection, log);
    }
    try {
      const rows = await this._mongoDb
        .collection(TOMBSTONE_LOG)
        .find({ collection } as never)
        .toArray();
      for (const row of rows) {
        const r = row as { id?: unknown };
        if (r.id !== undefined) log.set(String(r.id), r.id);
      }
    } catch (e) {
      this._log(`tombstone load failed: ${String(e)}`);
    }
  }

  /**
   * Recomputes a collection's accumulator from its full manifest (O(size), used
   * once when a manifest is restored from a checkpoint, not on the change path).
   * @param collection - The collection to rebuild.
   */
  private _rebuildAcc(collection: string): void {
    const acc = Buffer.alloc(32);
    const bAcc = Buffer.alloc(AE_BUCKET_COUNT * 32);
    for (const [key, hash] of this._manifestOf(collection)) {
      const d = this._entryDigest(key, hash);
      for (let i = 0; i < 32; i++) acc[i] ^= d[i];
      const off = this._bucketOf(key) * 32;
      for (let i = 0; i < 32; i++) bAcc[off + i] ^= d[i];
    }
    this._rootAcc.set(collection, acc);
    this._bucketAcc.set(collection, bAcc);
    this._rootCache.delete(collection);
  }

  /**
   * Records (or clears) a document's content hash in the collection manifest
   * and invalidates the cached content root. Keeps the manifest a faithful,
   * content-deterministic mirror of the live collection.
   * @param collection - The collection.
   * @param id - The document `_id`.
   * @param hash - The doc content hash, or `null` to remove (deleted).
   */
  private _setManifest(
    collection: string,
    id: unknown,
    hash: string | null,
  ): void {
    const m = this._manifestOf(collection);
    const key = String(id);
    const bucket = this._bucketOf(key);
    const old = m.get(key);
    if (hash === null) {
      // Delete: nothing to do if it was never here; else XOR its digest out.
      if (old === undefined) return;
      this._xorEntry(collection, this._entryDigest(key, old), bucket);
      m.delete(key);
      return;
    }
    // Upsert: no-op if unchanged; else XOR out the old digest (if any) and XOR
    // in the new — the root moves in O(1), never re-hashing the whole manifest.
    if (old === hash) return;
    if (old !== undefined) {
      this._xorEntry(collection, this._entryDigest(key, old), bucket);
    }
    this._xorEntry(collection, this._entryDigest(key, hash), bucket);
    m.set(key, hash);
  }

  /**
   * The content root of a collection: the hex of its XOR entry-digest
   * accumulator. Content-deterministic and order-independent — two nodes with
   * identical data compute an identical root (no time component), unlike the
   * editHistory head. O(1): the accumulator is maintained incrementally on every
   * manifest change (see {@link _setManifest}), so this never walks the manifest
   * — the property that keeps a mega collection from wedging the hub.
   * @param collection - The collection.
   * @returns The 64-hex content root (64 zeros for an empty collection).
   */
  private _contentRoot(collection: string): string {
    const cached = this._rootCache.get(collection);
    if (cached !== undefined) return cached;
    const root = this._accOf(collection).toString('hex');
    this._rootCache.set(collection, root);
    return root;
  }

  /**
   * Broadcasts a collection's current content root (idempotent, dedup-bypass).
   * @param collection - The collection.
   */
  private _broadcastRoot(collection: string): void {
    const root = this._contentRoot(collection);
    const ref = `${ROOT_PREFIX}${collection}:${root}`;
    // Log only when the root actually MOVES. The reconcile loop re-announces
    // every collection every few seconds whether or not anything changed, so
    // logging each one buried every other line: on the lab, all 500 buffered
    // entries on all four nodes were unchanged-root announcements, and not one
    // `recv` survived to explain why the nodes were not converging.
    if (this._lastLoggedRoot.get(collection) !== root) {
      this._lastLoggedRoot.set(collection, root);
      this._log(`root ${collection} = ${root.slice(0, 12)} -> send`);
    }
    if (this._connector.reannounce) this._connector.reannounce(ref);
    else this._connector.send(ref);
  }

  /**
   * Builds the ref announcing a head, tagged with the content root that head
   * produces. Call AFTER the manifest has been updated for the change, so the
   * tagged root is the post-change one the receiver must end up with.
   * @param collection - The collection the head belongs to.
   * @param head - The editHistory head ref.
   * @returns The wire ref `<collection>:<head>|<root>`.
   */
  private _headRef(collection: string, head: string): string {
    return `${collection}:${head}${ROOT_SEP}${this._contentRoot(collection)}`;
  }

  /**
   * Debounced content-root recompute+broadcast (off the change hot path).
   * @param collection - The collection whose root to re-broadcast.
   */
  private _scheduleRoot(collection: string): void {
    const existing = this._rootTimers.get(collection);
    if (existing) clearTimeout(existing);
    const t = setTimeout(() => {
      this._rootTimers.delete(collection);
      this._broadcastRoot(collection);
    }, this._rootDebounceMs);
    /* v8 ignore next -- @preserve unref keeps the timer from blocking exit/tests */
    (t as unknown as { unref?: () => void }).unref?.();
    this._rootTimers.set(collection, t);
  }

  /**
   * Records the latest change-stream resume token and schedules a debounced
   * checkpoint write (manifest + token) for the collection. A no-op when no
   * checkpoint store is configured.
   * @param collection - The collection the change belongs to.
   * @param change - The change-stream event (`_id` is the resume token).
   */
  private _checkpointAfter(
    collection: string,
    change: Record<string, unknown>,
  ): void {
    if (!this._checkpoint) return;
    this._lastToken.set(collection, change['_id']);
    if (this._saveTimers.has(collection)) return;
    const t = setTimeout(() => {
      this._saveTimers.delete(collection);
      void this._saveCheckpoint(collection);
    }, this._saveDebounceMs);
    /* v8 ignore next -- @preserve unref keeps the timer from blocking exit/tests */
    (t as unknown as { unref?: () => void }).unref?.();
    this._saveTimers.set(collection, t);
  }

  /**
   * Writes the current manifest + latest resume token for a collection.
   * @param collection - The collection to checkpoint.
   */
  private async _saveCheckpoint(collection: string): Promise<void> {
    /* v8 ignore next -- @preserve guarded by callers; defensive */
    if (!this._checkpoint) return;
    const manifest = this._manifestOf(collection);
    // A mega collection would serialize millions of entries to disk on every
    // debounced change — the same event-loop stall the root re-hash caused. Skip
    // it: such a collection rebuilds its manifest by a full scan on restart.
    if (manifest.size > this._checkpointMaxEntries) {
      this._log(
        `checkpoint ${collection} skipped (${manifest.size} > ${this._checkpointMaxEntries}, rebuild-on-restart)`,
      );
      return;
    }
    // `save` normalizes an undefined token to null, so no `?? null` here.
    await this._checkpoint.save(
      collection,
      manifest,
      this._lastToken.get(collection),
    );
    this._log(`checkpoint ${collection} saved`);
  }

  /**
   * Starts the sync: sets up cakes, emits the initial snapshot, then watches
   * change streams (producer) and incoming refs (consumer).
   */
  /**
   * Brings one collection under sync: registers its cake, opens the change
   * stream, emits the snapshot, then goes live. Idempotent — a collection
   * already in {@link _collections} is skipped, so the discovery loop can call
   * this freely.
   * @param collection - The collection to adopt.
   */
  private async _adoptCollection(collection: string): Promise<void> {
    if (this._collections.has(collection)) return;
    await this._adapter.init([collection]);
    this._collections.add(collection);
    // Open the change stream BEFORE the snapshot/restore so no write is lost in
    // the gap: events queue and drain (in order, serially) once we go live. The
    // driver's cursor resume point is fixed at this `watch()`, so a document
    // written during the snapshot is captured here; re-emitting an already-seen
    // doc is a harmless idempotent upsert.
    const queue: Array<Record<string, unknown>> = [];
    let snapshotDone = false;
    let pumping = false;
    const pump = async (): Promise<void> => {
      if (pumping) return;
      pumping = true;
      try {
        while (queue.length > 0) {
          const change = queue.shift() as Record<string, unknown>;
          try {
            await this._onChange(collection, change);
          } catch (e) {
            // A single failing change must not wedge the producer side. Without
            // the catch the rejection would escape the `void pump()` call sites
            // as an unhandled rejection AND leave `pumping` latched, so no local
            // write on this collection would ever be broadcast again for the
            // lifetime of the process. `_onChange` rolls its manifest entry back
            // before rethrowing, so the content root stays an honest mirror of
            // what we actually published.
            this._log(
              `pump ${collection} change failed, skipped: ${String(e)}`,
            );
          }
        }
      } finally {
        pumping = false;
      }
    };
    const open = (resumeAfter?: unknown): MongoChangeStream => {
      const opts = { fullDocument: 'updateLookup' } as Record<string, unknown>;
      if (resumeAfter != null) opts['resumeAfter'] = resumeAfter;
      const stream = this._mongoDb
        .collection(collection)
        .watch([], opts) as unknown as MongoChangeStream;
      stream.on('change', (change: unknown) => {
        queue.push(change as Record<string, unknown>);
        if (snapshotDone) void pump();
      });
      this._stop.push(() => stream.close());
      return stream;
    };
    // Full snapshot — MANIFEST ONLY. Reads every existing document, records its
    // content hash (seeding the manifest → the content root, and the mass-delete
    // guard baseline), and broadcasts the content root. It deliberately does NOT
    // `putDoc` the baseline into the cake: building an N-edit chain over millions
    // of catalog rows is the super-linear cold-start wall (measured ~O(N^1.4)).
    // The baseline converges via the CONTENT ROOT — two nodes with identical
    // data derive the same root and short-circuit with no transfer at all. Only
    // live changes (below) append to the cake, so the edit chain stays small and
    // incremental. (A fresh/divergent node is bootstrapped by seeding its data,
    // e.g. mongodump/restore, so its root matches — not by replaying a baseline
    // cake.)
    const fullSnapshot = async (): Promise<void> => {
      this._manifestOf(collection).clear();
      this._accOf(collection).fill(0);
      this._rootCache.delete(collection);
      const cursor = this._mongoDb.collection(collection).find({});
      let baseline = 0;
      for await (const doc of cursor) {
        baseline++;
        this._setManifest(
          collection,
          (doc as { _id: unknown })._id,
          docHash(doc),
        );
      }
      this._baselineCount.set(collection, baseline);
      this._broadcastRoot(collection);
      this._log(`snapshot ${collection} manifest-only ${baseline} docs -> root sent`);
    };

    const cp = this._checkpoint
      ? await this._checkpoint.load(collection)
      : undefined;
    if (cp && cp.token != null) {
      // Resume: restore the manifest (→ content root immediately known) and
      // reopen the stream from the saved token — no full scan. A token too old
      // for Mongo to resume (rotated oplog) surfaces as a stream error; fall
      // back once to a full snapshot so we still converge.
      const restored = new Map(Object.entries(cp.manifest));
      this._manifest.set(collection, restored);
      this._rebuildAcc(collection);
      this._baselineCount.set(collection, restored.size);
      let fellBack = false;
      const stream = open(cp.token);
      stream.on('error', (err: unknown) => {
        if (fellBack) return;
        fellBack = true;
        this._log(`resume ${collection} failed (${String(err)}) -> snapshot`);
        // The errored stream is dead but still closed on sync.stop (via _stop).
        // Open a fresh stream + run a full snapshot so we still converge.
        void (async () => {
          open();
          await fullSnapshot();
        })();
      });
      this._log(`resume ${collection} from checkpoint (${restored.size} docs)`);
      this._broadcastRoot(collection);
    } else {
      open();
      await fullSnapshot();
    }
    // Restore the per-document edit ordering from the chain rows this node
    // already holds locally. Without it a restarted node has no memory of what
    // it applied, so the first head from any lineage is replayed from its root
    // and writes that peer's stale versions over documents another peer has
    // since moved on. Free on a fresh node — there are no chain rows yet.
    await this._seedTimeIds(collection);
    // The collection's baseline manifest is now built (fresh snapshot or resumed
    // checkpoint) → the manifest-diff backfill may reconcile it, and docs this
    // node deleted before a restart are re-guarded against resurrection.
    await this._loadTombstones(collection);
    this._baselineReady.add(collection);
    // Go live: drain everything captured during the snapshot/restore, in order.
    snapshotDone = true;
    void pump();
  }

  /**
   * Seeds the per-document `timeId` map for a collection from the edit-chain
   * rows in this node's LOCAL store, so a restart resumes with the ordering it
   * had. Best effort: a failure (or a chain too large to scan) simply leaves
   * the collection unseeded.
   * @param collection - The collection to seed.
   */
  private async _seedTimeIds(collection: string): Promise<void> {
    try {
      const seeded = await this._adapter.latestTimeIds(
        collection,
        this._manifestOf(collection),
        this._seedMaxRows,
      );
      if (seeded.size === 0) return;
      for (const [sliceId, timeId] of seeded) {
        this._setAppliedTimeId(collection, sliceId, timeId);
      }
      this._log(
        `seed ${collection} ${seeded.size} document timeId(s) from the local chain`,
      );
      /* v8 ignore start -- @preserve defensive: seeding must never stop a start-up */
    } catch (e) {
      this._log(`seed ${collection} failed: ${String(e)}`);
    }
    /* v8 ignore stop */
  }

  /**
   * Adopts collections created since the last pass. The components/edits sync
   * opens ONE change stream per collection, so a collection that did not exist
   * when the agent started would otherwise never be watched — silently, and
   * forever. The supplier returns the currently-desired set (ignore/include
   * already applied by the caller); anything new is adopted live.
   * @param discover - Supplier of the currently-desired collection set.
   */
  private async _reconcile(discover: () => Promise<string[]>): Promise<void> {
    const desired = await discover();
    for (const collection of desired) {
      if (this._collections.has(collection)) continue;
      this._log(`discovered new collection ${collection} -> adopt`);
      await this._adoptCollection(collection);
    }
  }

  async start(): Promise<void> {
    this._connector.listen((ref) => this._onRef(ref));

    for (const collection of [...this._collections]) {
      // `_adoptCollection` re-adds it; start from a snapshot of the initial set
      // so the membership check inside does not skip the first pass.
      this._collections.delete(collection);
      await this._adoptCollection(collection);
    }
    // The initial cold-start is done: every start-up collection now has its
    // baseline manifest. Anti-entropy may run from here — until this point it
    // stays gated so a small collection's backfill cannot starve a mega
    // collection still hashing its baseline.
    this._coldStartComplete = true;

    // Head re-announce (gossip): the relay does not replay past refs to a
    // late-joining peer, so a node that connects after our snapshot broadcast
    // would never learn our heads. Periodically re-send every collection's
    // current head. It is idempotent — a receiver whose last-applied head
    // already equals ours does nothing — and cheap (only hashes travel). This
    // is what converges two nodes regardless of who started first.
    // Collection discovery: adopt collections created after start-up.
    if (this._discover) {
      const discover = this._discover;
      const disc = setInterval(() => {
        void this._reconcile(discover).catch((e) =>
          this._log(`reconcile failed: ${String(e)}`),
        );
      }, this._discoverMs);
      /* v8 ignore next -- @preserve unref keeps the timer from blocking exit/tests */
      (disc as unknown as { unref?: () => void }).unref?.();
      this._stop.push(() => clearInterval(disc));
    }

    const hb = setInterval(() => {
      for (const collection of this._collections) {
        // Re-announce the content root (drives the no-op convergence check).
        // Anti-entropy is NOT self-triggered from here: with the connector's
        // sequenced re-announce (causalOrdering) the peer's `~R~` root heartbeat
        // is delivered reliably even when the hash is unchanged, so the `_onRef`
        // ROOT branch fires the reconciliation on receipt. Driving a round for
        // every diverged collection on every heartbeat instead flooded the
        // connector (dozens of AEQ + their AER per tick × every node), which
        // starved head propagation and dropped cross-subnet peers.
        this._broadcastRoot(collection);
        const head = this._adapter.headRef(collection);
        if (!head) continue;
        const ref = this._headRef(collection, head);
        // Prefer the dedup-bypassing raw re-emit; fall back to send (tests).
        if (this._connector.reannounce) this._connector.reannounce(ref);
        else this._connector.send(ref);
      }
    }, this._heartbeatMs);
    /* v8 ignore next -- @preserve unref keeps the timer from blocking exit/tests */
    (hb as unknown as { unref?: () => void }).unref?.();
    this._stop.push(() => clearInterval(hb));
  }

  private async _onChange(
    collection: string,
    change: Record<string, unknown>,
  ): Promise<void> {
    const op = change['operationType'];
    if (op === 'delete') {
      this._onDelete(collection, change);
      return;
    }
    if (op !== 'insert' && op !== 'update' && op !== 'replace') return;
    const doc = change['fullDocument'] as
      | (Record<string, unknown> & { _id: unknown })
      | undefined;
    if (!doc) return;
    const hash = docHash(doc);
    // Keep the manifest a faithful mirror of Mongo (even for peer-applied echoes)
    // and (debounced) re-broadcast the content root so peers can converge.
    const previous = this._manifestOf(collection).get(String(doc._id)) ?? null;
    this._setManifest(collection, doc._id, hash);
    this._scheduleRoot(collection);
    this._checkpointAfter(collection, change);
    // Echo of a peer-applied write -> do not re-broadcast the edit head.
    if (this._appliedHash.get(this._key(collection, doc._id)) === hash) {
      this._log(`change ${collection}/${String(doc._id)} ${op} = echo, skip`);
      return;
    }
    let head: string | null;
    try {
      const put = await this._adapter.putDoc(collection, doc);
      head = put?.head ?? null;
      // Our own write is the newest edit this document has, so record its
      // timeId: without it an older put from a peer would be applied over the
      // value we just wrote, and this node would disagree with the rest of the
      // fleet about which edit won.
      this._setAppliedTimeId(collection, doc._id, put?.timeId);
    } catch (e) {
      // The doc never made it into the edit chain, so the manifest must not
      // claim it either — otherwise our broadcast content root would advertise
      // data no peer can pull, and the roots would never converge again.
      this._setManifest(collection, doc._id, previous);
      this._scheduleRoot(collection);
      throw e;
    }
    this._log(`change ${collection}/${String(doc._id)} ${op} -> head=${head} send`);
    if (head) this._connector.send(this._headRef(collection, head));
  }

  /**
   * Builds the tombstone document for a deleted `_id`.
   * @param id - The deleted document's `_id`.
   * @returns The tombstone document.
   */
  private _tombstone(id: unknown): Record<string, unknown> & { _id: unknown } {
    return { _id: id, [TOMBSTONE_FIELD]: true };
  }

  /**
   * Buffers a delete for the mass-delete guard. A single/small set of deletes
   * is propagated as tombstones after a short debounce; a burst that would wipe
   * a large fraction (or absolute count) of the collection is dropped entirely
   * so an accidental `deleteMany`/restore on one node can't wipe the peer — the
   * peer keeps the data and heals this node back via its head re-announce.
   * @param collection - The collection the delete happened in.
   * @param change - The change-stream delete event.
   */
  private _onDelete(
    collection: string,
    change: Record<string, unknown>,
  ): void {
    const id = (change['documentKey'] as { _id?: unknown } | undefined)?._id;
    if (id === undefined) return;
    // The doc is gone from Mongo → drop it from the manifest + refresh the root.
    this._setManifest(collection, id, null);
    // Persist a tombstone so the manifest-diff backfill re-asserts this delete
    // against a peer that still holds the doc, rather than resurrecting it.
    this._recordTombstone(collection, id);
    this._scheduleRoot(collection);
    this._checkpointAfter(collection, change);
    // Echo of a peer-applied delete -> do not re-propagate. Prune the
    // echo-suppression entry now that its one delete echo has been consumed, so
    // the map does not retain a slot per deleted _id forever.
    const key = this._key(collection, id);
    const tombHash = docHash(this._tombstone(id));
    if (this._appliedHash.get(key) === tombHash) {
      this._appliedHash.delete(key);
      this._log(`delete ${collection}/${String(id)} = echo, skip`);
      return;
    }
    let pending = this._pendingDeletes.get(collection);
    if (!pending) {
      pending = new Set();
      this._pendingDeletes.set(collection, pending);
    }
    pending.add(id);
    const existing = this._deleteTimers.get(collection);
    if (existing) clearTimeout(existing);
    const timer = setTimeout(
      () => void this._flushDeletes(collection),
      this._deleteDebounceMs,
    );
    /* v8 ignore next -- @preserve unref keeps the timer from blocking exit/tests */
    (timer as unknown as { unref?: () => void }).unref?.();
    this._deleteTimers.set(collection, timer);
  }

  /**
   * Flushes a collection's buffered deletes, applying the mass-delete guard.
   * @param collection - The collection whose buffered deletes to flush.
   */
  private async _flushDeletes(collection: string): Promise<void> {
    this._deleteTimers.delete(collection);
    const pending = this._pendingDeletes.get(collection);
    if (!pending || pending.size === 0) return;
    this._pendingDeletes.set(collection, new Set());

    // The manifest is a live mirror of the collection, and `_onDelete` has
    // already removed the pending ids from it — so `manifest + pending` is the
    // doc count as it stood immediately BEFORE this burst. Deriving the
    // baseline here (rather than latching it at snapshot time) keeps the guard
    // correct for a collection that has grown or shrunk since start-up: a
    // snapshot-time baseline made the fraction guard silently inapplicable to
    // any collection that started small and later grew large.
    const baseline = this._manifestOf(collection).size + pending.size;
    // Fraction guard only for collections that are non-trivial; otherwise just
    // the absolute cap (don't block small deletes).
    const limit =
      baseline >= 10
        ? Math.min(this._deleteAbsMax, Math.ceil(baseline * this._deleteFraction))
        : this._deleteAbsMax;
    if (pending.size >= limit && !this._expectedMassDelete.delete(collection)) {
      this._log(
        `delete BURST ${collection} size=${pending.size} >= limit=${limit} BLOCKED (mass-delete guard)`,
      );
      return;
    }

    let head: string | null = null;
    for (const id of pending) {
      const put = await this._adapter.putDoc(collection, this._tombstone(id));
      head = put?.head ?? null;
      this._setAppliedTimeId(collection, id, put?.timeId);
    }
    this._log(`delete flush ${collection} ${pending.size} tombstone(s) -> head=${head}`);
    if (head) this._connector.send(this._headRef(collection, head));
  }

  private _onRef(ref: string): void {
    // Anti-entropy protocol traffic (manifest-diff backfill) — route to the
    // engine and stop; these refs are neither roots nor heads. Ignored until our
    // own cold-start is complete: a node still hashing its baselines must neither
    // pull nor serve, or the extra load wedges it — a peer's round simply times
    // out and retries once we are ready.
    if (MongoAntiEntropy.owns(ref)) {
      if (this._antiEntropyOn && this._coldStartComplete) {
        void this._ae.onMessage(ref);
      }
      return;
    }
    // Root ref (`~R~<collection>:<root>`): a pure convergence signal — never a
    // pull, and never a reason to skip anything.
    if (ref.startsWith(ROOT_PREFIX)) {
      const body = ref.slice(ROOT_PREFIX.length);
      const i = body.indexOf(':');
      if (i < 0) return;
      const collection = body.slice(0, i);
      const root = body.slice(i + 1);
      if (!this._collections.has(collection)) return;
      this._log(`recv root ${collection} = ${root.slice(0, 12)}`);
      // A peer reporting a root different from ours means someone is ahead of
      // (or behind) us. Re-drive the last head we saw: its pull may have come
      // back empty/partial because the origin's rows were not resolvable yet.
      // `_applyHead` no-ops when the head was already applied, and skips when
      // its tagged root is already ours — so this is cheap when converged.
      const diverged = root !== this._contentRoot(collection);
      const pending = this._lastPeerHead.get(collection);
      if (pending && diverged) {
        this._scheduleApply(
          collection,
          pending.head,
          pending.root,
          pending.ref,
        );
      }
      // Keep the retry loop alive while we are diverged. The heartbeat re-emits
      // the SAME root hash every tick, so after the first delivery the
      // connector's received-dedup swallows it and this re-drive never fires
      // again — a reconnecting node whose initial pull came back empty (the
      // origin's rows were not yet resolvable in the reconnect race) then sits
      // on stale data until some UNRELATED new write mints a fresh hash. Clear
      // the root ref from the received-dedup so the next identical heartbeat is
      // delivered again and re-drives the pending head, giving a deterministic
      // ~heartbeat-interval retry until we converge. When roots already match we
      // let the dedup swallow it — no work, no chatter on a healthy cluster.
      if (diverged) this._connector.invalidateReceived?.(ref);
      // Backfill: a persistent root mismatch the head re-drive cannot close —
      // the divergent docs live only in a peer's manifest baseline, never in an
      // edit chain (a bulk import, a cold-start delta) — is healed by the
      // bucketed manifest-diff backfill.
      if (diverged) this._maybeTriggerAe(collection);
      return;
    }
    const idx = ref.indexOf(':');
    if (idx < 0) return;
    const collection = ref.slice(0, idx);
    // `<head>` or `<head>|<root>` — the root tag is optional so a peer running
    // an older build still interoperates (it just cannot be short-circuited).
    const rest = ref.slice(idx + 1);
    const sep = rest.lastIndexOf(ROOT_SEP);
    const head = sep < 0 ? rest : rest.slice(0, sep);
    const targetRoot = sep < 0 ? undefined : rest.slice(sep + 1);
    this._log(`recv ref ${collection}:${head}`);
    if (!this._collections.has(collection)) {
      // A collection that exists only on the PEER. Dropping the ref here made
      // such a collection permanently unreachable: this node's synced set is
      // built from its OWN collections, and the discovery loop only adopts
      // collections that appear in its OWN mongo — which this one never would,
      // because the very sync that would create it is the one being dropped.
      // The producer side of that deadlock was fixed earlier; this is the
      // consumer side.
      if (!this._shouldSync?.(collection)) {
        this._log(`recv ref ${collection} NOT syncable, drop`);
        return;
      }
      if (!this._adoptingOnRef.has(collection)) {
        this._adoptingOnRef.add(collection);
        this._log(`recv ref ${collection} unknown here -> adopting on demand`);
        void this._adoptCollection(collection)
          .then(() => {
            this._lastPeerHead.set(collection, { head, root: targetRoot, ref });
            this._scheduleApply(collection, head, targetRoot, ref);
          })
          .catch((e) =>
            this._log(`adopt-on-ref ${collection} failed: ${String(e)}`),
          )
          .finally(() => this._adoptingOnRef.delete(collection));
      }
      return;
    }
    this._lastPeerHead.set(collection, { head, root: targetRoot, ref });
    this._scheduleApply(collection, head, targetRoot, ref);
  }

  /**
   * Serializes `_applyHead` calls per collection behind an in-flight guard so a
   * head ref and a root-triggered retry never pull the same collection
   * concurrently.
   * @param collection - The collection to apply into.
   * @param head - The head editHistory ref to apply.
   * @param targetRoot - The content root that head produces, when the sender
   *   tagged it; `undefined` forces a pull (no short-circuit).
   * @param rawRef - The ref exactly as received, for the received-dedup
   *   invalidation after a failed pull.
   */
  private _scheduleApply(
    collection: string,
    head: string,
    targetRoot: string | undefined,
    rawRef: string,
  ): void {
    const prev = this._applyChain.get(collection) ?? Promise.resolve();
    const next = prev
      .catch(() => {})
      .then(() => this._applyHead(collection, head, targetRoot, rawRef));
    this._applyChain.set(collection, next);
  }

  /**
   * Pulls the edits for a head, retrying with exponential backoff while the
   * chain does not resolve completely. An incomplete result means some row is
   * not (yet) reachable through the hub — the origin's `IoPeer` may not be
   * registered, or its rows not yet visible — which is a transient race on a
   * fresh connection, so it is worth retrying before settling for what we got.
   * @param collection - The collection whose cake the head belongs to.
   * @param head - The incoming head editHistory ref.
   * @param applied - The refs already applied for this collection.
   * @returns The collected puts, the completeness flag and the refs that are
   *   safe to remember as applied.
   */
  private async _collectWithRetry(
    collection: string,
    head: string,
    applied: ReadonlySet<string>,
  ): Promise<CollectPutsResult> {
    let last: CollectPutsResult = { puts: [], complete: false, sealed: [] };
    for (let attempt = 0; ; attempt++) {
      try {
        last = await this._adapter.collectPuts(collection, head, applied);
        if (last.complete || attempt >= this._pullRetries) return last;
      } catch (e) {
        this._log(`applyHead ${collection} pull attempt ${attempt} threw: ${String(e)}`);
        if (attempt >= this._pullRetries) return last;
      }
      const backoff = Math.min(this._pullBackoffMs * 2 ** attempt, 5000);
      await new Promise((r) => setTimeout(r, backoff));
    }
  }

  private async _applyHead(
    collection: string,
    head: string,
    targetRoot: string | undefined,
    rawRef: string,
  ): Promise<void> {
    const applied = this._appliedOf(collection);
    if (applied.has(head)) return;
    // Content-root short-circuit: skip the O(n) pull only when the root THIS
    // head produces is already our own — i.e. we demonstrably hold the exact
    // content it would deliver. This is what makes a reconnect / hub change on
    // converged data seamless instead of a full cold-start (the editHistory head
    // can never match across nodes because it carries a time-based id; the
    // content root can and does).
    //
    // It deliberately does NOT consult a per-peer root: a single
    // last-writer-wins slot shared by every peer means that on three or more
    // nodes the root of an unrelated node could equal ours and silently swallow
    // this head — losing inserts and deletes, and leaving the fleet permanently
    // diverged with no repair path (verified live on four nodes, 2026-08-17).
    if (targetRoot !== undefined && targetRoot === this._contentRoot(collection)) {
      this._log(`applyHead ${collection} SKIP — head's root already ours`);
      return;
    }
    const { puts, complete, sealed } = await this._collectWithRetry(
      collection,
      head,
      applied,
    );

    if (!complete) {
      // The walk hit a row no read path could resolve, so `puts` is only part
      // of the chain. That is NOT a reason to throw the pull away, which is
      // what used to happen: the head was invalidated, and because the content
      // root never matched again the node sat on the old state until it was
      // restarted. Apply what did resolve — an apply can no longer move a
      // document backwards, so a partial chain is safe — remember only the refs
      // whose whole ancestry resolved, and clear the received-dedup so a later
      // re-announce delivers the head again and completes the rest.
      this._log(
        `applyHead ${collection} head=${head} PARTIAL -> applying ` +
          `${puts.length} resolvable put(s), re-arming the ref`,
      );
      this._connector.invalidateReceived?.(rawRef);
    }

    if (puts.length === 0) {
      // Either everything this head carries is already applied (a re-announce)
      // or nothing resolved at all. Sealing whatever did resolve still moves
      // the walk floor forward.
      this._markApplied(collection, sealed);
      return;
    }

    this._log(
      `applyHead ${collection} head=${head} -> ${puts.length} puts` +
        `${complete ? '' : ' (partial)'}`,
    );
    for (const put of puts) {
      const doc = put.doc as Record<string, unknown> & { _id: unknown };
      // Last writer wins, by the edit's own timeId. Every node reads the same
      // timeId for the same edit, so every node resolves the same way — that is
      // what makes convergence a guarantee rather than a matter of who spoke
      // last. It is also what makes a replay harmless.
      if (!this._isNewer(collection, put.sliceId, put.timeId)) {
        this._log(
          `applyHead ${collection} skip stale _id=${String(doc._id)} ` +
            `(${String(put.timeId)} not newer)`,
        );
        continue;
      }
      this._setAppliedTimeId(collection, doc._id, put.timeId);
      // Remember the applied content hash so the change-stream event this write
      // is about to emit is recognized as an echo and not re-broadcast. For a
      // tombstone the entry is one-shot: `_onDelete` prunes it once it has
      // consumed the delete echo, so it does not leak a slot per deleted _id.
      this._appliedHash.set(
        this._key(collection, doc._id),
        docHash(doc),
      );
      if (doc[TOMBSTONE_FIELD] === true) {
        await this._mongoDb
          .collection(collection)
          .deleteOne({ _id: doc._id } as never);
        // Applied a peer's delete → record our own tombstone so the backfill
        // never resurrects it from a peer that has not seen the delete yet.
        this._recordTombstone(collection, doc._id);
        this._log(`applyHead ${collection} deleted _id=${String(doc._id)}`);
      } else {
        await this._mongoDb
          .collection(collection)
          .replaceOne({ _id: doc._id } as never, doc, { upsert: true });
        this._log(`applyHead ${collection} upserted _id=${String(doc._id)}`);
      }
    }
    this._markApplied(collection, sealed);
  }

  /** Stops all change streams and cancels pending delete/root timers. */
  async stop(): Promise<void> {
    for (const timer of this._deleteTimers.values()) clearTimeout(timer);
    this._deleteTimers.clear();
    for (const timer of this._rootTimers.values()) clearTimeout(timer);
    this._rootTimers.clear();
    // Flush any pending debounced checkpoint so the latest resume token is not
    // lost on a clean shutdown (the next start resumes from exactly here).
    for (const timer of this._saveTimers.values()) clearTimeout(timer);
    this._saveTimers.clear();
    if (this._checkpoint) {
      for (const collection of this._lastToken.keys()) {
        await this._saveCheckpoint(collection);
      }
    }
    for (const fn of this._stop) await fn();
  }
}
