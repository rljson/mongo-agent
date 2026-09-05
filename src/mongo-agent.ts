// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { BsMem } from '@rljson/bs';
import { Route, syncEvents } from '@rljson/rljson';

import { MongoScanner, type MongoTree } from './mongo-scanner.ts';
import { MongoDbTreeAdapter } from './mongo-db-tree-adapter.ts';
import {
  ensureDirtyIndexes,
  markCollectionFullDirty,
  markDirtyById,
} from './hashing/state-dirty.ts';

import type { Bs } from '@rljson/bs';
import type { Db } from '@rljson/db';
import type { SyncConfig } from '@rljson/rljson';
import type { ChangeStream, ChangeStreamDocument, Db as MongoDb } from 'mongodb';

// .............................................................................

export interface MongoAgentOptions {
  /** Collection-name patterns to ignore (glob: `*`, `?`). */
  ignore?: string[];
  /** Whitelist of collection names. */
  include?: string[];
  /** Sync protocol configuration forwarded to the Connector. */
  syncConfig?: SyncConfig;
  /** Stable client identity forwarded to the Connector. */
  clientIdentity?: unknown;
  /** Debounce delay between change-stream events (ms). Default 300. */
  debounceMs?: number;
  /** Optional pre-existing blob storage. Defaults to a new BsMem. */
  bs?: Bs;
  /**
   * Raw Socket.IO socket — required for the sendRef watchdog. When the
   * watchdog detects too many consecutive sendRef timeouts it calls
   * `socket.disconnect()` followed by `socket.connect()` to force a fresh
   * transport. Without this, the agent has no way to recover from a stuck
   * application-layer state where TCP is alive but the protocol is dead.
   */
  rawSocket?: any;
  /** Origin tag written into pushed ops (echo-break). Defaults to hostname. */
  nodeName?: string;
}

// .............................................................................

/**
 * MongoAgent — counterpart of FsAgent, but for MongoDB.
 *
 * On `syncToDb`: watches MongoDB change streams, extracts an RLJSON tree
 * snapshot of the database, stores it via `db.insertTrees()` (which broadcasts
 * the ref through the Connector to the sl-server multicast bus).
 *
 * On `syncFromDb`: listens for incoming refs from the Connector, fetches the
 * tree from the rljson DB, and applies it to the local MongoDB (upserting
 * documents, deleting orphans).
 */
export class MongoAgent {
  private readonly _mongoDb: MongoDb;
  private readonly _bs: Bs;
  private readonly _scanner: MongoScanner;
  private readonly _options: Required<
    Pick<MongoAgentOptions, 'debounceMs'>
  > &
    MongoAgentOptions;
  private _changeStream: ChangeStream | null = null;
  /**
   * Change-stream resilience state. Mongo's change-streams can silently
   * close after long idles, replica-set elections, transient network
   * blips, or even just rolling restarts on the customer's cluster. If
   * we don't auto-reopen, the agent goes mute for INSERTS/UPDATES/DELETES
   * — exactly the failure mode that turned up after L2 sat untouched for
   * 71 hours. These fields track the lifecycle for `_openChangeStream`
   * and surface health to `getSyncHealth`.
   */
  private _changeStreamAlive = false;
  private _changeStreamLastEventAt: number | null = null;
  private _changeStreamReopenAttempts = 0;
  private _changeStreamResumeToken: unknown = null;
  private _changeStreamStopRequested = false;
  private _changeStreamReopenTimer: NodeJS.Timeout | null = null;
  private _changeStreamOnChange: ((c: ChangeStreamDocument) => void) | null = null;
  private _lastSentRef: string | undefined;
  /**
   * Epoch-ms cutoff during which the change-stream's debounced scan is
   * suppressed. Set by `_forcePushImpl` (and by restoreToRef's force-push)
   * so the change-stream events that mirror the writes we just pushed
   * don't trigger a second, redundant scan + push. Without this every
   * test-api insert ends up in InsertHistory twice — once from the
   * explicit `_afterWrite` → forcePush, once from the change-stream
   * debounce. rljson's `storeTree` includes a fresh `timeId` per call
   * so the rootHash check can't dedupe these.
   */
  private _suppressDebounceUntil = 0;
  /** Periodic GC of internal bookkeeping collections (never grow large). */
  private _internalGcTimer: NodeJS.Timeout | null = null;
  /**
   * BATCHED dirty-partition tracking for the incremental state hash. Every
   * non-internal doc change adds its `_id` to an in-memory per-collection set
   * (O(1), no mongo op on the write path) instead of doing a `markDirtyById`
   * round-trip per write — the latter would queue MILLIONS of fire-and-forget
   * mongo ops during a bulk import and blow the heap. The sets are flushed to
   * `state_dirty` every {@link _dirtyFlushMs}. If a collection accumulates more
   * than {@link _dirtyCap} changed ids in one window it's a bulk load → we drop
   * the per-id set and mark the whole collection FULL-dirty (one marker; the
   * whole new data set has to be hashed once anyway).
   */
  private _dirtyIds = new Map<string, Set<unknown>>();
  private _dirtyFullColls = new Set<string>();
  private readonly _dirtyCap = 50_000;
  private readonly _dirtyFlushMs = 1_500;
  private _dirtyFlushTimer: NodeJS.Timeout | null = null;
  /** Bridge / socket forwarded from fromClient (used for bootstrap subscription). */
  private _bridge: any | null = null;
  /** Promise that resolves once the initial bootstrap-on-connect pull completed. */
  private _bootstrapDone: Promise<void> | null = null;
  /**
   * Installed by `syncToDb` — re-scans MongoDB and pushes the resulting tree
   * via the connector even if nothing in mongo changed.
   */
  private _forcePushImpl: (() => Promise<string | null>) | null = null;
  /**
   * Health metrics maintained by the sendRef-with-timeout wrapper. The test
   * API exposes them via `/test/health` so the UI can show "sync stuck"
   * before the user has to read logs.
   */
  private _consecutiveSendTimeouts = 0;
  private _lastSendOkAt: number | null = null;
  private _lastSendErrorAt: number | null = null;
  private _lastSendErrorMsg: string | null = null;
  private _totalSendOk = 0;
  private _totalSendTimeouts = 0;
  /** Underlying Socket.IO client — used to force-reconnect after stuck pushes. */
  private _socket: any | null = null;
  /**
   * References stashed at sync-start so admin actions (restoreToRef, etc.)
   * can reach the same rljson Db + connector + adapter as the live sync.
   * Set by `syncToDb` / `syncFromDb`; cleared on teardown.
   */
  private _db: any | null = null;
  // Kept for potential future use (e.g. directly invoking connector.send
  // from restoreToRef) — currently we go through _forcePushImpl which
  // already captures the connector closure.
  private _treeKey: string | null = null;
  private _adapter: MongoDbTreeAdapter | null = null;
  /**
   * Optional NetworkManager reference — set externally by sl-auto so the
   * test API can expose /test/network/* read + action endpoints.
   * The mongo-agent itself never uses it; it's purely a passthrough so
   * one and the same TestHttpServer instance can surface both Mongo-sync
   * and Network-topology state.
   */
  private _networkManager: unknown | null = null;
  setNetworkManager(nm: unknown): void {
    this._networkManager = nm;
  }
  getNetworkManager(): unknown | null {
    return this._networkManager;
  }

  /**
   * Optional handle to the in-process file-sync client running alongside
   * this mongo-agent (set by sl-auto when fileSync is enabled). Pure
   * passthrough — the mongo-agent itself doesn't use it; the test API
   * reads it to surface fs status / browse the watched folder / run
   * file-sync baseline tests.
   */
  private _fsAgentHandle:
    | { agent: unknown; folder: string; treeKey: string }
    | null = null;
  setFsAgent(handle: { agent: unknown; folder: string; treeKey: string } | null): void {
    this._fsAgentHandle = handle;
  }
  getFsAgent(): { agent: unknown; folder: string; treeKey: string } | null {
    return this._fsAgentHandle;
  }

  /**
   * Optional handle to the sl-auto orchestrator, exposing live add/remove
   * for mongo.additional sync targets and mongo.mirrors. Lets POST
   * /test/settings hot-apply config changes without an exe restart.
   * Pure passthrough — the mongo-agent itself never invokes this.
   */
  private _orchestrator: unknown | null = null;
  setOrchestrator(o: unknown): void {
    this._orchestrator = o;
  }
  getOrchestrator(): unknown | null {
    return this._orchestrator;
  }
  /** Threshold of consecutive timeouts after which we hard-reconnect the socket. */
  private readonly _reconnectAfterTimeouts = 3;
  /** Default send-ref timeout (ms). */
  private readonly _sendRefTimeoutMs = 5_000;
  /**
   * Ring buffer of the last N tree-ref operations the agent has done. The
   * test API exposes this as the "Sync-Op Chain" so the user has something
   * meaningful to look at — the legacy `sync_ops` collection is no longer
   * written by the tree-snapshot sync model.
   */
  private readonly _refHistory: Array<{
    ts: number;
    ref: string;
    direction: 'sent' | 'received';
    operationType?: string;
  }> = [];
  private readonly _refHistoryMax = 200;
  /**
   * Earliest time the watchdog is allowed to trigger another auto-reconnect.
   * Prevents reconnect storms while the new transport is still warming up
   * (during which the very first sendRefs naturally time out).
   */
  private _watchdogCooldownUntil = 0;

  constructor(mongoDb: MongoDb, bs?: Bs, options: MongoAgentOptions = {}) {
    this._mongoDb = mongoDb;
    this._bs = bs ?? new BsMem();
    this._options = {
      ignore: options.ignore,
      include: options.include,
      syncConfig: options.syncConfig,
      clientIdentity: options.clientIdentity,
      bs: this._bs,
      // Default debounce raised from 300ms → 1000ms because on a burst of
      // change-stream events (e.g. bulk-insert of 10 000 docs) the 300ms
      // window still re-triggered scans during the same insert salve.
      // 1s gives the burst time to settle and yields ONE scan+push for
      // the whole batch instead of dozens.
      debounceMs: options.debounceMs ?? 1000,
      // Origin tag (echo-break).
      nodeName: options.nodeName,
      rawSocket: options.rawSocket,
    };
    this._scanner = new MongoScanner(mongoDb, {
      ignore: this._options.ignore,
      include: this._options.include,
      bs: this._bs,
    });
  }

  /** MongoDB database handle. */
  get mongoDb(): MongoDb {
    return this._mongoDb;
  }

  /** Blob storage (shared with the `@rljson/server` Client). */
  get bs(): Bs {
    return this._bs;
  }

  /** Internal scanner used to extract RLJSON trees. */
  get scanner(): MongoScanner {
    return this._scanner;
  }

  /**
   * Extracts the current MongoDB state as an RLJSON tree.
   * Documents are packed per-collection into ComponentsTable blobs.
   */
  async extract(): Promise<MongoTree> {
    return this._scanner.scan();
  }

  /**
   * Stores a snapshot of the current MongoDB state in the rljson DB and
   * starts watching for changes. On every change-stream event a new
   * snapshot is extracted, stored, and the resulting ref is broadcast
   * through the Connector.
   * @param db - rljson Db (created from `client.io`)
   * @param connector - Connector wired up to the Socket.IO bridge
   * @param treeKey - Tree-table key (must end with "Tree")
   * @returns Stop function
   */
  async syncToDb(
    db: Db,
    connector: any,
    treeKey: string,
  ): Promise<() => void> {
    const adapter = new MongoDbTreeAdapter(db, treeKey);
    // Stash references so admin actions (restoreToRef, /test/refs/history)
    // can reach the rljson Db + adapter without a new wiring.
    this._db = db;
    this._treeKey = treeKey;
    this._adapter = adapter;
    void connector;

    // ----- initial snapshot -----
    {
      const _dbg = process.env['SL_TREE_SYNC_DEBUG'] === '1';
      const _s0 = Date.now();
      const initialTree = await this._scanner.scan();
      /* v8 ignore start -- diagnostic instrumentation (SL_TREE_SYNC_DEBUG) */
      if (_dbg) {
        console.log(
          `[tree-sync] producer scan done: ${initialTree.trees.size} nodes, root=${String(initialTree.rootHash).slice(0, 8)} (${Date.now() - _s0}ms)`,
        );
      }
      /* v8 ignore stop */
      const _p0 = Date.now();
      const initialRef = await adapter.storeTree(initialTree);
      this._lastSentRef = initialRef;
      /* v8 ignore start -- diagnostic instrumentation (SL_TREE_SYNC_DEBUG) */
      if (_dbg) {
        console.log(
          `[tree-sync] producer storeTree done: ref=${String(initialRef).slice(0, 8)} (${Date.now() - _p0}ms, scan+store total ${Date.now() - _s0}ms)`,
        );
      }
      /* v8 ignore stop */
      if (initialRef) {
        // Initial send is best-effort — if the server isn't ready yet, the
        // first change-stream event (or a later force-push) re-tries.
        await this._safeSendRef(connector, initialRef);
      }
    }

    // Capture closures for a "force push" that the test API can call
    // (bypasses the change-stream debounce, ignores rootHash-equality check
    // so it works even when nothing has changed locally).
    this._forcePushImpl = async () => {
      const withFpTimeout = <T>(p: Promise<T>, ms: number, what: string): Promise<T> =>
        Promise.race([
          p,
          new Promise<T>((_, rej) =>
            setTimeout(() => rej(new Error(`${what} timed out after ${ms}ms`)), ms),
          ),
        ]);

      // Set the suppress window UP FRONT — `_scanner.scan()` +
      // `adapter.storeTree(snapshot)` on a non-trivial DB can take 2+
      // seconds, during which the change-stream debounce timer fires
      // and starts its OWN parallel scan + push. Without an early
      // window setting that scan races to completion (it's reading the
      // same Mongo state) and produces a second ref before forcePush
      // finishes. We extend the window once we know storeTree's
      // duration so post-forcePush change-stream events are also
      // covered.
      // Internal timeouts for the two slow steps of forcePush.
      //
      // Calibration: 5_000 ms was the original bound but turned out to be
      // too aggressive in real deployments. On a freshly-bootstrapped
      // node with even 100–500 user docs the rljson connector's storeTree
      // can take 2–8 s on the first push (cache-cold paths through the
      // connector + initial tree-row inserts). At 5 s every other push
      // timed out, the suppress-debounce window cleared, the change-stream
      // refired the SAME scan, and the agent ended up in a permanent
      // retry-and-timeout loop where no sync ever actually landed.
      //
      // 30_000 ms gives the library room to complete legitimate slow
      // operations while still bounding a truly wedged call. The earlier
      // suppress window (10_000) is also extended below.
      this._suppressDebounceUntil = Date.now() + 35_000;
      try {
        const snapshot = await withFpTimeout(
          this._scanner.scan(),
          30_000,
          'forcePush scan',
        );
        const ref = await withFpTimeout(
          adapter.storeTree(snapshot),
          30_000,
          'forcePush storeTree',
        );
        if (!ref) {
          // Reset the window so we don't suppress legitimate future pushes.
          this._suppressDebounceUntil = 0;
          return null;
        }
        this._lastSentRef = ref;
        // Tighten the window now that storeTree is done: only need a
        // short tail to swallow the change-stream events for the writes
        // we just emitted.
        this._suppressDebounceUntil = Date.now() + 2000;
        await this._safeSendRef(connector, ref);
        return ref;
      } catch (err) {
        // Wedge-state escape hatch: a hung rljson connector used to leave
        // the suppress window set to "now + 10s" indefinitely, blocking
        // the change-stream debounce from catching up. Clear the window
        // so the next change-stream event can trigger a fresh attempt.
        this._suppressDebounceUntil = 0;
        console.warn(
          `[mongo-agent] forcePush failed (${(err as Error).message}); ` +
            `suppress window cleared so change-stream can retry`,
        );
        return null;
      }
    };

    // ----- live updates via change streams (debounced) -----
    let debounceTimer: NodeJS.Timeout | null = null;
    let scanInFlight = false;
    let scanQueued = false;
    // Collections changed since the last tree scan. Passed to the INCREMENTAL
    // scanner.scan(dirty) so a per-change scan re-reads only these collections
    // and reuses the cached chunk trees for everything else (kills the
    // O(whole-DB) re-scan that wedged the loop → peer bootstrap rows=0).
    const dirtyColls = new Set<string>();
    const onChange = (change: ChangeStreamDocument): void => {
      const ns = (change as any).ns ?? {};
      const collName: string | undefined = ns.coll;
      const docId = (change as any).documentKey?._id;
      const isInternal =
        !collName ||
        collName.startsWith('sync_') ||
        collName.startsWith('state_') ||
        collName.startsWith('system.') ||
        collName.startsWith('rljson_') ||
        collName === 'sync_recentChanges';

      // ----- DIRTY-PARTITION tracking for the INCREMENTAL state hash -----
      // ANY change to a non-internal doc (local edit OR a peer tree applied via
      // _applyTreeToMongo — both fire this change stream) makes the partition that
      // covers this _id dirty, so the next hash only rescans dirty partitions.
      // Recorded in-memory and flushed in batches (see _flushDirty) — NOT a
      // mongo op per write, which would queue millions of ops in a bulk import.
      if (collName && docId !== undefined && !isInternal) {
        this._recordDirty(collName, docId);
        // Mark this collection dirty for the next incremental tree scan.
        dirtyColls.add(collName);
      }

      // NOTE: `__h` (per-doc integrity hash for the fast hash projection) is
      // NOT maintained inline here. An earlier version stamped __h on every
      // write via an extra updateOne — that DOUBLED the write + change-stream
      // load and crippled bulk imports (a 8.8 M-doc catalog load fired 8.8 M
      // extra writes). __h is now populated only by the on-demand backfill
      // script when the DB is idle; the incremental hash stays correct without
      // it because dirty-partition rescans read FULL documents (see
      // state-hash.ts) and docLeafHash falls back to computeIntegrityHash for
      // any doc lacking __h. The cold full-build uses the __h projection (run
      // the backfill first for a huge collection so that pass stays bounded).

      // RLJSON tree-sync bookkeeping: a delete writes a tombstone (so the
      // delete propagates through the tree); an insert/update marks a recent
      // change (conflict detection reads it to tell a real concurrent edit
      // from a plain fast-forward).
      if (collName && docId !== undefined && !isInternal) {
        if (change.operationType === 'delete') {
          this._writeTombstone(collName, docId).catch((err) =>
            console.warn(
              '[mongo-agent] tombstone write failed:',
              (err as Error).message,
            ),
          );
        } else if (
          change.operationType === 'insert' ||
          change.operationType === 'update' ||
          change.operationType === 'replace'
        ) {
          this._writeRecentChange(collName, docId).catch(() => {
            /* best-effort */
          });
        }
      }

      // Internal-collection writes (rljson_*, sync_*, state_*, system.*)
      // are *our own* side effects of storeTree / tombstone / hash bookkeeping.
      // Letting them trigger another debounced scan means every user write
      // produces TWO refs in InsertHistory:
      //   1. user write → scan → storeTree (writes rljson_*) → ref #1
      //   2. rljson_* change-stream events → scan → storeTree again → ref #2
      // Skip the debounce trigger entirely for internal events.
      if (isInternal) return;

      // Suppress the change-stream-triggered scan if we *just* did an
      // explicit force-push (test-api `_afterWrite` or restoreToRef).
      // Those writes are already captured in the freshly-pushed tree;
      // running a second scan now would produce a second, content-equal
      // tree with a different rootHash (rljson stamps a fresh `timeId`
      // on every storeTree call) — appearing as duplicate entries in
      // the Sync-tab ref history.
      if (Date.now() < this._suppressDebounceUntil) return;

      // Anti-overlap: if a scan is already running, queue a follow-up
      // rather than starting a parallel scan. Pre-fix the agent could
      // start dozens of overlapping scans during a bulk-insert salve,
      // each holding the items collection + a fresh ComponentsTable
      // blob in memory — that's where the 1 GB RSS came from.
      if (debounceTimer) clearTimeout(debounceTimer);
      const runScan = async (): Promise<void> => {
        // Re-check the suppress window here: the change-stream event
        // could have queued this scan BEFORE `_forcePushImpl` got around
        // to setting `_suppressDebounceUntil` (the events fire as soon
        // as Mongo commits the write; forcePush only sets the flag after
        // its own scan+storeTree finishes ~tens of ms later). Without
        // this second check the debounce timer still fires and produces
        // a duplicate ref.
        if (Date.now() < this._suppressDebounceUntil) return;
        if (scanInFlight) {
          scanQueued = true;
          return;
        }
        scanInFlight = true;
        // Snapshot + drain the dirty set: this scan re-reads exactly these
        // collections, everything else comes from the scanner's cache. New
        // changes arriving during the scan re-populate the set for the next
        // run. On failure we put them back so they are retried, not lost.
        const dirty = new Set(dirtyColls);
        dirtyColls.clear();
        try {
          const tmo = <T>(p: Promise<T>, ms: number, what: string): Promise<T> =>
            Promise.race([
              p,
              new Promise<T>((_, rej) =>
                setTimeout(
                  () => rej(new Error(`${what} timed out after ${ms}ms`)),
                  ms,
                ),
              ),
            ]);
          // 30_000 ms — same calibration as the forcePush path above.
          // 5_000 ms was too aggressive; first-push storeTree on a
          // freshly-bootstrapped node legitimately took 2–8 s and every
          // run timed out, leaving the agent in a permanent retry loop.
          const snapshot = await tmo(this._scanner.scan(dirty), 30_000, 'scan');
          if (snapshot.rootHash === this._lastSentRef) return;
          // storeTree is the rljson Db write that occasionally wedges
          // after peer-churn. Bounding it lets the next change-stream
          // event try again instead of pinning scanInFlight forever.
          const ref = await tmo(
            adapter.storeTree(snapshot),
            30_000,
            'storeTree',
          );
          if (!ref || ref === this._lastSentRef) return;
          this._lastSentRef = ref;
          await this._safeSendRef(connector, ref);
        } catch (err) {
          // Put the drained collections back so the next scan retries them
          // (they were NOT re-read into the tree this round).
          for (const c of dirty) dirtyColls.add(c);
          console.error('[mongo-agent] syncToDb error:', (err as Error).message);
        } finally {
          scanInFlight = false;
          if (scanQueued) {
            scanQueued = false;
            // Run the queued scan via a debounce window so further events
            // can still coalesce into it.
            debounceTimer = setTimeout(() => {
              debounceTimer = null;
              void runScan();
            }, this._options.debounceMs);
          }
        }
      };
      debounceTimer = setTimeout(() => {
        debounceTimer = null;
        void runScan();
      }, this._options.debounceMs);
    };

    // Open the change-stream with the resilient auto-reopen loop. Stores
    // `onChange` so a reopen after error/close can re-subscribe the same
    // handler. From this point on, the stream is self-healing for the
    // life of the agent — required for the customer's "runs for months
    // uninterrupted" scenario.
    this._changeStreamOnChange = onChange;
    this._changeStreamStopRequested = false;
    this._openChangeStream();

    // Keep the internal bookkeeping collections from ever growing large.
    // sync_tombstones / sync_recentChanges are only consumed by the
    // snapshot-pull fallback within seconds; under change-push they're not
    // consumed at all. GC anything older than 2 min every 60 s so a bulk
    // delete can never leave hundreds of thousands of rows behind.
    this._startInternalGc();

    // Indexes for the incremental-hash dirty tracking: state_dirty lookups and
    // the state_merkle range-scan that markDirtyById uses to map a changed _id
    // to its partition. Best-effort, one-shot.
    ensureDirtyIndexes(this._mongoDb).catch(() => {});
    this._mongoDb
      .collection('state_merkle')
      .createIndex({ coll: 1, minId: 1, maxId: 1 })
      .catch(() => {});
    // GC range-scan indexes. Without these the periodic _startInternalGc
    // sweep deletes via COLLSCAN, which is what crashed mongod during a bulk
    // import. With them every `deleteMany({changedAt|deletedAt:{$lt}})` is a
    // bounded indexed range delete.
    this._mongoDb
      .collection('sync_recentChanges')
      .createIndex({ changedAt: 1 })
      .catch(() => {});
    this._mongoDb
      .collection('sync_tombstones')
      .createIndex({ deletedAt: 1 })
      .catch(() => {});

    return () => {
      if (debounceTimer) clearTimeout(debounceTimer);
      this._forcePushImpl = null;
      this._db = null;
      this._treeKey = null;
      this._adapter = null;
      this._changeStreamStopRequested = true;
      if (this._changeStreamReopenTimer) {
        clearTimeout(this._changeStreamReopenTimer);
        this._changeStreamReopenTimer = null;
      }
      if (this._changeStream) {
        this._changeStream.removeAllListeners();
        this._changeStream.close().catch(() => {});
        this._changeStream = null;
        this._changeStreamAlive = false;
      }
    };
  }

  /**
   * Opens the MongoDB change-stream and wires it up with full lifecycle
   * handlers (`change` / `error` / `close` / `end`). On any non-clean
   * termination it schedules a backoff-reopen that resumes from the last
   * known resume-token so no events are lost across the outage. Idempotent
   * — safe to call repeatedly.
   */
  private _openChangeStream(): void {
    if (this._changeStreamStopRequested) return;
    if (!this._changeStreamOnChange) return;
    const handler = this._changeStreamOnChange;

    const watchOpts: any = { fullDocument: 'updateLookup' };
    if (this._changeStreamResumeToken) {
      watchOpts.resumeAfter = this._changeStreamResumeToken;
    }

    let stream: ChangeStream;
    try {
      stream = this._mongoDb.watch([], watchOpts);
    } catch (err) {
      console.error(
        '[mongo-agent] change-stream open failed:',
        (err as Error).message,
      );
      this._changeStreamAlive = false;
      this._scheduleChangeStreamReopen();
      return;
    }

    this._changeStream = stream;
    this._changeStreamAlive = true;
    this._changeStreamReopenAttempts = 0;

    stream.on('change', (event: any) => {
      this._changeStreamLastEventAt = Date.now();
      if (event?._id) this._changeStreamResumeToken = event._id;
      try {
        handler(event);
      } catch (err) {
        console.error(
          '[mongo-agent] onChange threw:',
          (err as Error).message,
        );
      }
    });

    stream.on('error', (err) => {
      console.warn(
        '[mongo-agent] change-stream error — will reopen:',
        (err as Error).message,
      );
      this._changeStreamAlive = false;
      this._scheduleChangeStreamReopen();
    });

    stream.on('close', () => {
      this._changeStreamAlive = false;
      if (!this._changeStreamStopRequested) {
        console.warn('[mongo-agent] change-stream closed — will reopen');
        this._scheduleChangeStreamReopen();
      }
    });

    stream.on('end', () => {
      this._changeStreamAlive = false;
      if (!this._changeStreamStopRequested) {
        console.warn('[mongo-agent] change-stream ended — will reopen');
        this._scheduleChangeStreamReopen();
      }
    });
  }

  private _scheduleChangeStreamReopen(): void {
    if (this._changeStreamStopRequested) return;
    if (this._changeStreamReopenTimer) return; // already pending
    // Exponential backoff capped at 5s. So Mongo can be down for any
    // amount of time and we'll reconnect within ~5s after it returns.
    const attempt = ++this._changeStreamReopenAttempts;
    const delayMs = Math.min(5_000, 500 * Math.pow(2, Math.min(attempt, 4)));
    this._changeStreamReopenTimer = setTimeout(() => {
      this._changeStreamReopenTimer = null;
      console.log(
        `[mongo-agent] reopening change-stream (attempt ${attempt}, resumeToken=${this._changeStreamResumeToken ? 'yes' : 'no'})`,
      );
      // Close the old stream defensively if still around
      if (this._changeStream) {
        try {
          this._changeStream.removeAllListeners();
          this._changeStream.close().catch(() => {});
        } catch {
          /* ignore */
        }
        this._changeStream = null;
      }
      this._openChangeStream();
    }, delayMs);
  }

  /**
   * Forces a fresh scan + push, bypassing change-stream debounce. Returns
   * the new tree ref (or `null` if syncToDb hasn't been started yet).
   *
   * Useful from the Test UI when MongoDB had data before the agent started
   * and no subsequent change event has fired — that's the scenario where
   * pre-existing docs stay invisible to peers until something writes.
   */
  async forcePush(): Promise<string | null> {
    if (!this._forcePushImpl) return null;
    return this._forcePushImpl();
  }

  /**
   * Fetch the FULL tree-ref history from the rljson Db's InsertHistory
   * (sorted newest-first). Returns whatever the server-side IoMem still
   * holds — usually a long backlog of recent snapshots. Used by the
   * Sync tab to populate the "Restore to ref" list with stable, agent-
   * restart-surviving entries (the in-memory _refHistory is per-process).
   */
  /**
   * Pull a timestamp out of one InsertHistory row. rljson writes `timeId`
   * as `"<epochMs>:<random>"` (the colon-prefixed suffix exists to keep
   * concurrent inserts ordered when two refs land in the same ms). Older
   * writers used `ts` / `timestamp` directly, so try those first for
   * compatibility.
   * @param r - A single InsertHistory row; inspected for `ts`, `timestamp`,
   *   or a `"<epochMs>:<random>"` `timeId` to recover the write time.
   * @returns The epoch-ms timestamp of the row, or `null` if none could be
   *   derived.
   */
  private _extractTs(r: Record<string, unknown>): number | null {
    if (typeof r['ts'] === 'number') return r['ts'] as number;
    if (typeof r['timestamp'] === 'number') return r['timestamp'] as number;
    const timeId = r['timeId'];
    if (typeof timeId === 'string') {
      const head = timeId.split(':')[0];
      const n = Number(head);
      if (Number.isFinite(n) && n > 0) return n;
    }
    return null;
  }

  async getInsertHistory(limit = 100): Promise<
    Array<{ ref: string; ts: number | null; raw: Record<string, unknown> }>
  > {
    if (!this._db || !this._treeKey) return [];
    try {
      const history = (await (this._db as any).getInsertHistory(
        this._treeKey,
        { sorted: true, ascending: false },
      )) as any;
      const tableName = this._treeKey + 'InsertHistory';
      const rows = (history?.[tableName]?._data ?? []) as Array<any>;
      const slice = rows.slice(0, limit);
      return slice.map((r: any) => ({
        ref: r[this._treeKey + 'Ref'] ?? r.ref ?? '',
        ts: this._extractTs(r),
        raw: r,
      }));
    } catch (err) {
      console.warn(
        '[mongo-agent] getInsertHistory failed:',
        (err as Error).message,
      );
      return [];
    }
  }

  /**
   * Restore the local MongoDB to the state captured by a previous tree ref.
   *
   * Steps:
   *   1. fetchTree(ref) (with the same fallback logic syncFromDb uses)
   *   2. Apply via the standard `_applyTreeToMongo` path — tombstones from
   *      that snapshot remove docs that were alive then but gone now;
   *      Pass 2 upserts the doc states the way they were at that ref.
   *   3. force-push so peers see the rolled-back state immediately.
   *
   * Caveats: the apply path is conflict-aware, so anything you've created
   * since the snapshot that didn't exist back then will produce conflict
   * records. Pure "go back to T0" works cleanly when no one else has
   * touched the DB since.
   * @param ref - The tree ref (root hash) of a previously-stored snapshot to
   *   roll the local MongoDB back to.
   * @returns A summary of the restore: the `ref`, whether the tree was
   *   `fetched` and `applied`, and the `pushedRef` propagated to peers.
   */
  async restoreToRef(ref: string): Promise<{
    ref: string;
    fetched: boolean;
    applied: boolean;
    pushedRef: string | null;
  }> {
    if (!ref) throw new Error('ref is required');
    if (!this._db || !this._adapter) {
      throw new Error('restoreToRef called before syncToDb finished');
    }

    // 1) fetch the tree — with the same getInsertHistory fallback we use
    //    on the live sync path, so historical refs that aren't currently
    //    in the local IoMem still resolve.
    const withTimeout = <T>(p: Promise<T>, ms: number, what: string): Promise<T> =>
      Promise.race([
        p,
        new Promise<T>((_, rej) =>
          setTimeout(() => rej(new Error(`${what} timed out after ${ms}ms`)), ms),
        ),
      ]);

    console.log(`[mongo-agent] restoreToRef start ref=${ref.slice(0, 12)}…`);
    let incoming: MongoTree | null = null;
    try {
      // Bound the first fetch too — the rljson connector can stay pending
      // forever if the hub channel is wedged, which used to make the whole
      // restore HTTP request hang past any reasonable client timeout.
      incoming = await withTimeout(
        this._adapter.fetchTree(ref),
        10_000,
        'fetchTree (restore)',
      );
    } catch (err) {
      console.log(
        `[mongo-agent] restoreToRef fetchTree first try failed: ${(err as Error).message} — retrying via getInsertHistory`,
      );
      try {
        await withTimeout(
          (this._db as any).getInsertHistory(this._treeKey, {
            sorted: true,
            ascending: false,
          }),
          5_000,
          'getInsertHistory (restore)',
        );
        incoming = await withTimeout(
          this._adapter.fetchTree(ref),
          5_000,
          'fetchTree retry (restore)',
        );
      } catch (err2) {
        throw new Error(
          `restoreToRef: ref ${ref.slice(0, 12)}… not available: ${(err2 as Error).message}`,
        );
      }
    }
    if (!incoming) {
      throw new Error(`restoreToRef: empty tree for ref ${ref}`);
    }
    console.log(`[mongo-agent] restoreToRef fetched — applying tree…`);

    // 2) apply via the standard path with restoreMode — overwrites local
    //    state to match the snapshot exactly: orphan docs (inserted after
    //    the snapshot) get deleted, divergent docs get replaced without
    //    recording a conflict, and tombstones bypass the 60s age check.
    {
      const treeNodes = incoming.trees.size;
      const collNodes: Array<{ name: string; docCount: any; blob: string }> = [];
      for (const node of incoming.trees.values()) {
        const meta = (node as any).meta;
        if (meta?.type === 'collection') {
          collNodes.push({
            name: meta.collection ?? meta.name ?? '?',
            docCount: meta.docCount,
            blob: (meta.componentsBlobId ?? '').slice(0, 12) + '…',
          });
        }
      }
      console.log(
        `[mongo-agent] restoreToRef incoming tree: ${treeNodes} nodes, ${collNodes.length} collection(s): ` +
          collNodes
            .map((c) => `${c.name}(docCount=${c.docCount}, blob=${c.blob})`)
            .join(', '),
      );
    }
    await this._applyTreeToMongo(incoming, { restoreMode: true });
    console.log(`[mongo-agent] restoreToRef applied — force-pushing to peers`);

    // 3) propagate to peers so the rolled-back state isn't immediately
    //    overwritten by a peer's "current" tree on the next change-stream
    //    event. Time-bound it so a hung connector can't trap the restore.
    let pushedRef: string | null = null;
    if (this._forcePushImpl) {
      try {
        pushedRef = await withTimeout(
          this._forcePushImpl(),
          10_000,
          'forcePush (restore)',
        );
      } catch (err) {
        console.warn(
          `[mongo-agent] restoreToRef force-push failed (continuing): ${(err as Error).message}`,
        );
      }
    }
    console.log(
      `[mongo-agent] restoreToRef done — pushedRef=${pushedRef ? pushedRef.slice(0, 12) + '…' : 'null'}`,
    );

    return { ref, fetched: true, applied: true, pushedRef };
  }

  /**
   * Last N tree-ref operations seen by this agent (capped at _refHistoryMax).
   * Used by `/test/refs` to drive the UI's "Sync-Op Chain" table — the
   * legacy `sync_ops` collection is empty under the tree-snapshot model.
   * @param limit - Maximum number of most-recent ref operations to return.
   * @returns The last `limit` ref operations, newest-first.
   */
  getRefHistory(limit = 50): Array<{
    ts: number;
    ref: string;
    direction: 'sent' | 'received';
    operationType?: string;
  }> {
    return this._refHistory.slice(-limit).reverse();
  }

  private _pushRefHistory(entry: {
    ts: number;
    ref: string;
    direction: 'sent' | 'received';
    operationType?: string;
  }): void {
    this._refHistory.push(entry);
    if (this._refHistory.length > this._refHistoryMax) {
      this._refHistory.splice(0, this._refHistory.length - this._refHistoryMax);
    }
  }

  /**
   * Sync-pipeline health snapshot for the test API.
   */
  getSyncHealth(): {
    lastSendOkAt: number | null;
    lastSendErrorAt: number | null;
    lastSendErrorMsg: string | null;
    consecutiveTimeouts: number;
    totalSendOk: number;
    totalSendTimeouts: number;
    socketConnected: boolean;
    forcePushReady: boolean;
    changeStreamAlive: boolean;
    changeStreamLastEventAt: number | null;
    changeStreamReopenAttempts: number;
  } {
    return {
      lastSendOkAt: this._lastSendOkAt,
      lastSendErrorAt: this._lastSendErrorAt,
      lastSendErrorMsg: this._lastSendErrorMsg,
      consecutiveTimeouts: this._consecutiveSendTimeouts,
      totalSendOk: this._totalSendOk,
      totalSendTimeouts: this._totalSendTimeouts,
      socketConnected: !!this._socket?.connected,
      forcePushReady: !!this._forcePushImpl,
      changeStreamAlive: this._changeStreamAlive,
      changeStreamLastEventAt: this._changeStreamLastEventAt,
      changeStreamReopenAttempts: this._changeStreamReopenAttempts,
    };
  }

  /**
   * Hard-reset the connection: disconnect the underlying Socket.IO socket
   * (Socket.IO will automatically reconnect with a fresh transport), reset
   * timeout counters, and clear `_lastSentRef` so the next change-stream
   * event will push a full snapshot. Called by `POST /test/reset-sync`.
   */
  resetSync(): { socketDisconnected: boolean } {
    this._consecutiveSendTimeouts = 0;
    this._lastSentRef = undefined;
    if (this._socket?.connected && typeof this._socket.disconnect === 'function') {
      try {
        this._socket.disconnect();
        // Reconnect immediately — Socket.IO's auto-reconnect honours it
        if (typeof this._socket.connect === 'function') this._socket.connect();
        return { socketDisconnected: true };
      } catch {
        return { socketDisconnected: false };
      }
    }
    return { socketDisconnected: false };
  }

  /**
   * Wraps a `sendRef` call with a timeout and updates health metrics.
   * If too many timeouts pile up in a row, force-disconnects the socket so
   * Socket.IO reconnects from scratch — the next change-stream event will
   * re-push the full tree.
   * @param connector - The rljson Connector used to broadcast the ref.
   * @param ref - The tree root hash to send to peers.
   * @returns `true` if the send completed within the timeout, `false` on
   *   timeout/error.
   */
  private async _safeSendRef(connector: any, ref: string): Promise<boolean> {
    const timeoutMs = this._sendRefTimeoutMs;
    /* v8 ignore next 5 -- diagnostic instrumentation (SL_TREE_SYNC_DEBUG) */
    if (process.env['SL_TREE_SYNC_DEBUG'] === '1') {
      console.log(
        `[tree-sync] hub BROADCAST ref=${String(ref).slice(0, 12)}… via connector.send (treeKey=${this._treeKey})`,
      );
    }
    try {
      await Promise.race([
        sendRef(connector, ref),
        new Promise<void>((_resolve, reject) =>
          setTimeout(
            () => reject(new Error(`sendRef timed out after ${timeoutMs}ms`)),
            timeoutMs,
          ),
        ),
      ]);
      this._consecutiveSendTimeouts = 0;
      this._lastSendOkAt = Date.now();
      this._totalSendOk++;
      this._pushRefHistory({ ts: Date.now(), ref, direction: 'sent' });
      return true;
    } catch (err) {
      this._consecutiveSendTimeouts++;
      this._lastSendErrorAt = Date.now();
      this._lastSendErrorMsg = (err as Error).message;
      this._totalSendTimeouts++;
      console.warn(
        `[mongo-agent] sendRef failed (consecutive=${this._consecutiveSendTimeouts}): ${
          (err as Error).message
        }`,
      );
      const now = Date.now();
      const cooldownActive = now < this._watchdogCooldownUntil;
      if (
        !cooldownActive &&
        this._consecutiveSendTimeouts >= this._reconnectAfterTimeouts &&
        this._socket?.connected
      ) {
        console.warn(
          `[mongo-agent] ${this._consecutiveSendTimeouts} consecutive sendRef timeouts — forcing socket reconnect (next reconnect blocked for 30s)`,
        );
        try {
          this._socket.disconnect();
          if (typeof this._socket.connect === 'function') this._socket.connect();
        } catch {
          /* best-effort */
        }
        this._consecutiveSendTimeouts = 0;
        // 30s cooldown so the freshly-rebuilt transport gets a chance to
        // settle before the watchdog can yank it again. Without this, the
        // first few sendRefs after reconnect (which often time out while
        // the engine warms up) immediately trigger ANOTHER disconnect →
        // peer-joined/peer-left storm.
        this._watchdogCooldownUntil = now + 30_000;
      }
      return false;
    }
  }

  /* eslint-disable tsdoc/syntax -- jsdoc/require-param mandates the dotted
     `@param opts.bootstrapTimeoutMs` member doc, which the TSDoc parser then
     rejects as an invalid (dotted) identifier; the two rules are mutually
     exclusive for an inline-object param, so TSDoc is suppressed here. */
  /**
   * Listens for incoming tree refs and applies them to the local MongoDB.
   *
   * Bootstrap-on-connect:
   *   When the rljson server welcomes a new client it emits a `bootstrap`
   *   event on the bridge with the current root ref. We subscribe to that
   *   event in addition to the regular `ref` event and apply both, so a
   *   freshly-connected agent receives the server's current state *before*
   *   it pushes its own local state. Combined with the conflict-aware
   *   apply path, this is what guarantees that concurrent edits made
   *   offline on multiple peers get detected on reconnect.
   *
   *   `syncFromDb` resolves only after either a bootstrap/ref has been
   *   applied OR a short timeout (`bootstrapTimeoutMs`) elapses. Callers
   *   should run `syncFromDbSimple()` BEFORE `syncToDbSimple()` so the
   *   pull-before-push order is preserved.
   * @param db - rljson Db (created from `client.io`)
   * @param connector - Connector for incoming ref notifications
   * @param treeKey - Tree-table key
   * @param opts - Tuning options for the bootstrap-on-connect pull.
   * @param opts.bootstrapTimeoutMs - How long (ms) to wait for the initial
   *   bootstrap/ref before resolving anyway; defaults to 3000.
   * @returns Stop function
   */
  /* eslint-enable tsdoc/syntax */
  async syncFromDb(
    db: Db,
    connector: any,
    treeKey: string,
    opts: { bootstrapTimeoutMs?: number } = {},
  ): Promise<() => void> {
    const adapter = new MongoDbTreeAdapter(db, treeKey);
    const route = Route.fromFlat(`/${treeKey}`);
    void route;
    const bootstrapTimeoutMs = opts.bootstrapTimeoutMs ?? 3000;

    let pendingRef: string | null = null;
    let timer: NodeJS.Timeout | null = null;
    let lastAppliedRef: string | undefined;
    let bootstrapResolved = false;
    /* v8 ignore next -- @preserve placeholder overwritten synchronously by the
       Promise executor below; the initial no-op is never actually invoked. */
    let bootstrapResolve: () => void = () => {};
    const bootstrapPromise = new Promise<void>((res) => {
      bootstrapResolve = () => {
        if (!bootstrapResolved) {
          bootstrapResolved = true;
          res();
        }
      };
    });

    const apply = async (rootRef: string): Promise<void> => {
      if (!rootRef) return;
      if (rootRef === this._lastSentRef) {
        // Echo of our own push — ignore but count as 'connected'
        bootstrapResolve();
        return;
      }
      if (rootRef === lastAppliedRef) {
        bootstrapResolve();
        return;
      }
      lastAppliedRef = rootRef;
      this._pushRefHistory({ ts: Date.now(), ref: rootRef, direction: 'received' });
      try {
        // Primary path: read the tree from the local rljson Db (IoMem).
        let incoming: MongoTree | null = null;
        try {
          incoming = await adapter.fetchTree(rootRef);
        } catch (err) {
          // The remote pushed a ref but the local IoMem doesn't have the
          // tree nodes yet — only the bootstrap-on-connect pull primes
          // those. Pull the freshest insert-history once, then retry.
          // Both calls are wrapped in a 4s timeout so a stuck hub-call
          // can't pile up and stall the event loop for the whole agent.
          console.warn(
            `[mongo-agent] fetchTree(${rootRef.slice(0, 12)}…) failed: ${(err as Error).message}. Falling back to getInsertHistory pull.`,
          );
          const withTimeout = <T>(p: Promise<T>, ms: number, what: string): Promise<T> =>
            Promise.race([
              p,
              new Promise<T>((_, rej) =>
                setTimeout(() => rej(new Error(`${what} timed out after ${ms}ms`)), ms),
              ),
            ]);
          // Bootstrap LIVENESS: retry the pull with backoff instead of giving
          // up after one try. A producer whose initial scan+store is still
          // running cannot serve the tree rows yet — the single-shot give-up
          // was exactly the "rows=0" bootstrap failure the live repro showed.
          // Retrying lets a not-yet-primed producer catch up (~22s window).
          let lastErr: Error | null = null;
          const maxAttempts = 6;
          for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
              await withTimeout(
                (db as any).getInsertHistory(treeKey, {
                  sorted: true,
                  ascending: false,
                }),
                4_000,
                'getInsertHistory',
              );
              incoming = await withTimeout(
                adapter.fetchTree(rootRef),
                4_000,
                'fetchTree retry',
              );
              console.log(
                `[mongo-agent] fallback pull succeeded for ref ${rootRef.slice(0, 12)}… (attempt ${attempt}/${maxAttempts})`,
              );
              lastErr = null;
              break;
            } catch (err2) {
              lastErr = err2 as Error;
              if (attempt < maxAttempts) {
                await new Promise((r) => setTimeout(r, attempt * 1_500));
              }
            }
          }
          if (lastErr) {
            console.error(
              `[mongo-agent] fallback pull failed after ${maxAttempts} attempts for ref ${rootRef.slice(0, 12)}: ${lastErr.message}`,
            );
            return;
          }
        }
        if (!incoming) return;
        // Suppress the next debounced push BEFORE we start writing — the
        // Mongo writes we're about to do (Pass 1 deletes + Pass 2 upserts)
        // will themselves fire change-stream events on user collections,
        // and without this every received ref would echo back to the
        // sender as a new (content-equal but timeId-different) ref.
        // Window covers the apply itself plus the debounce that would
        // otherwise fire afterwards.
        this._suppressDebounceUntil = Date.now() + 5000;
        await this._applyTreeToMongo(incoming);
        // Extend the window past the debounce that would fire just after
        // the apply finishes (apply can take seconds on big trees).
        this._suppressDebounceUntil = Date.now() + 2000;
      } catch (err) {
        console.error('[mongo-agent] syncFromDb apply error:', err);
      } finally {
        bootstrapResolve();
      }
    };

    const handler = async (ref: unknown): Promise<void> => {
      if (typeof ref !== 'string' || !ref) return;
      pendingRef = ref;
      if (timer) clearTimeout(timer);
      timer = setTimeout(async () => {
        timer = null;
        const r = pendingRef;
        pendingRef = null;
        /* v8 ignore next -- @preserve pendingRef is only ever set to a non-empty
           string (the handler rejects falsy refs up front); the `else` is dead. */
        if (r) await apply(r);
      }, this._options.debounceMs);
    };

    // ---- Bridge-level subscription (covers bootstrap + ref) ----
    const bridgeListeners: Array<{ event: string; handler: (p: any) => void }> = [];
    const _dbgRx = process.env['SL_TREE_SYNC_DEBUG'] === '1';
    if (this._bridge && typeof this._bridge.on === 'function') {
      const events = syncEvents(`/${treeKey}`);
      /* v8 ignore next 5 -- diagnostic instrumentation (SL_TREE_SYNC_DEBUG) */
      if (_dbgRx) {
        console.log(
          `[tree-sync] client SUBSCRIBE bridge events: ref='${events.ref}' bootstrap='${events.bootstrap}' (treeKey=${treeKey})`,
        );
      }
      const onBootstrap = (p: any): void => {
        /* v8 ignore next 3 -- diagnostic instrumentation */
        if (_dbgRx)
          console.log(
            `[tree-sync] client RECV bootstrap ref=${String(p?.r ?? p?.ref).slice(0, 12)}…`,
          );
        const r = p?.r ?? p?.ref;
        if (typeof r === 'string') void apply(r);
        else bootstrapResolve();
      };
      const onRef = (p: any): void => {
        /* v8 ignore next 3 -- diagnostic instrumentation */
        if (_dbgRx)
          console.log(
            `[tree-sync] client RECV ref=${String(p?.r ?? p?.ref).slice(0, 12)}… via bridge`,
          );
        const r = p?.r ?? p?.ref;
        if (typeof r === 'string') void handler(r);
      };
      this._bridge.on(events.bootstrap, onBootstrap);
      this._bridge.on(events.ref, onRef);
      bridgeListeners.push({ event: events.bootstrap, handler: onBootstrap });
      bridgeListeners.push({ event: events.ref, handler: onRef });
    }

    // ---- Connector-level subscription (fallback) ----
    if (typeof connector.listen === 'function') {
      connector.listen((ref: unknown) => {
        /* v8 ignore next 3 -- diagnostic instrumentation */
        if (_dbgRx)
          console.log(
            `[tree-sync] client RECV ref=${String(ref).slice(0, 12)}… via connector.listen`,
          );
        return handler(ref);
      });
    }

    // ---- Explicit pull-on-connect ----
    // The server emits a `bootstrap` event when a client joins, but by the
    // time MongoAgent.fromClient registers the bridge handler, that event
    // has already fired (it happens during Client.init() before we even
    // construct MongoAgent). To compensate we query the local Db's
    // insert-history for the latest ref of treeKey and apply that.
    // The local Db's IoMem has been populated by Client.init()'s bootstrap.
    // Give bootstrap a moment to arrive in the local Db (Client.init() may
    // return before the server's bootstrap event has been fully processed
    // by the local IoMem).
    await new Promise((r) => setTimeout(r, 500));

    const tryExplicitPull = async (): Promise<boolean> => {
      try {
        const history = (await (db as any).getInsertHistory(treeKey, {
          sorted: true,
          ascending: false,
        })) as any;
        const tableName = treeKey + 'InsertHistory';
        const rows = history?.[tableName]?._data ?? [];
        console.log(
          `[mongo-agent] explicit bootstrap pull: history rows=${rows.length} (table=${tableName})`,
        );
        const latestRef = rows[0]?.[treeKey + 'Ref'];
        if (typeof latestRef === 'string' && latestRef) {
          console.log(
            `[mongo-agent] explicit bootstrap pull: latestRef=${latestRef.slice(0, 12)}…`,
          );
          await apply(latestRef);
          return true;
        }
      } catch (err) {
        console.warn(
          `[mongo-agent] explicit bootstrap pull failed: ${(err as Error).message}`,
        );
      }
      return false;
    };

    if (!(await tryExplicitPull())) {
      console.log(
        '[mongo-agent] no insert-history on first attempt — will retry after 1500ms',
      );
      // Retry once after a longer delay — bootstrap might still be arriving.
      setTimeout(() => {
        void tryExplicitPull().then(() => bootstrapResolve());
      }, 1500);
    }

    // ---- Wait for bootstrap OR timeout ----
    const timeoutHandle = setTimeout(() => {
      /* v8 ignore next -- @preserve the bootstrapPromise's .finally clears this
         timeout on resolve, so when it fires bootstrap is always still
         unresolved; the already-resolved else is a defensive race guard. */
      if (!bootstrapResolved) {
        console.warn(
          `[mongo-agent] bootstrap-on-connect timed out after ${bootstrapTimeoutMs}ms — proceeding without remote state`,
        );
      }
      bootstrapResolve();
    }, bootstrapTimeoutMs);
    this._bootstrapDone = bootstrapPromise.finally(() => clearTimeout(timeoutHandle));
    await this._bootstrapDone;

    return () => {
      if (timer) clearTimeout(timer);
      for (const { event, handler: h } of bridgeListeners) {
        if (typeof this._bridge?.off === 'function') this._bridge.off(event, h);
      }
      if (typeof connector.tearDown === 'function') {
        connector.tearDown();
      }
    };
  }

  /**
   * Stable JSON-stringify with sorted keys (excluding rljson-internal `_hash`).
   * Used to compute content-equality between a local and an incoming document.
   *
   * IMPORTANT: BSON Date values must be normalised to their numeric
   * millisecond timestamp BEFORE stringify. A doc that lives locally in
   * Mongo carries `createdAt` as a BSON Date (which JSON.stringify renders
   * as an ISO string); the same doc arriving from rljson carries it as a
   * raw number (rljson schema discovery maps Date → number). Without
   * normalisation, the two render to different JSON and every sync ends
   * up logging a phantom "concurrent-update" conflict for every Date-bearing
   * field on every doc.
   * @param doc - The document to fingerprint; normalised (Dates → ms, keys
   *   sorted, `_hash`/`__h` dropped) before hashing.
   * @returns A stable hex content hash, equal for two content-identical docs.
   */
  private _contentHash(doc: Record<string, unknown>): string {
    const normalised = this._normaliseForHash(doc);
    const json = JSON.stringify(normalised);
    let h = 0;
    for (let i = 0; i < json.length; i++) {
      h = ((h << 5) - h + json.charCodeAt(i)) | 0;
    }
    return h.toString(16);
  }

  /**
   * Recursive normalisation used by `_contentHash`:
   *   - drops the rljson-internal `_hash` field
   *   - converts BSON Date / Date instances to their numeric ms timestamp
   *   - sorts object keys so output is canonical
   * @param value - The value (doc, array, scalar, or BSON wrapper) to
   *   normalise recursively into a canonical, hashable form.
   * @returns The canonicalised value with Dates as ms numbers, ObjectIds as
   *   strings, and `_hash`/`__h` stripped.
   */
  private _normaliseForHash(value: unknown): unknown {
    if (value === null || value === undefined) return value;
    if (value instanceof Date) return value.getTime();
    if (Array.isArray(value)) {
      return value.map((v) => this._normaliseForHash(v));
    }
    if (typeof value === 'object') {
      // ObjectId-like / Buffer-like: fall back to a stringified form so
      // both Mongo-native and rljson-roundtripped variants hash identically.
      const v = value as any;
      if (v._bsontype === 'ObjectId' && typeof v.toString === 'function') {
        return v.toString();
      }
      if (v?.type === 'Buffer' && Array.isArray(v.data)) {
        return v.data; // already a plain array of ints
      }
      const out: Record<string, unknown> = {};
      for (const k of Object.keys(v as Record<string, unknown>).sort()) {
        // `_hash` = rljson-internal; `__h` = the per-doc integrity hash kept
        // for the incremental state-hash. Both are DERIVED node-local metadata,
        // never part of the logical content — dropping them keeps conflict
        // detection, echo suppression and the content fingerprint stable when
        // only `__h` is (re)stamped.
        if (k === '_hash' || k === '__h') continue;
        out[k] = this._normaliseForHash((v as any)[k]);
      }
      return out;
    }
    return value;
  }

  /**
   * Records a conflict into the `sync_conflicts` collection. Schema is
   * compatible with the rljson conflict-resolver UI: a single document per
   * (collection, documentId) holding both versions until manually resolved.
   * @param collection - Name of the MongoDB collection the conflicted doc
   *   belongs to.
   * @param documentId - The conflicted document's `_id`.
   * @param incomingDoc - The remote/peer version of the document.
   * @param localDoc - The current local version of the document.
   * @param conflictType - Whether the conflict is a concurrent update of an
   *   existing doc or a concurrent insert of the same id; defaults to
   *   `'concurrent-update'`.
   */
  private async _recordConflict(
    collection: string,
    documentId: unknown,
    incomingDoc: Record<string, unknown>,
    localDoc: Record<string, unknown>,
    conflictType: 'concurrent-update' | 'concurrent-insert' = 'concurrent-update',
  ): Promise<void> {
    try {
      const conflicts = this._mongoDb.collection('sync_conflicts');
      const key = `${collection}::${String(documentId)}`;
      await conflicts.updateOne(
        { _id: key as any },
        {
          $set: {
            _id: key as any,
            conflictType,
            collection,
            documentId: String(documentId),
            detectedAt: new Date(),
            status: 'pending',
            versions: [
              {
                source: 'local',
                hash: this._contentHash(localDoc),
                data: localDoc,
              },
              {
                source: 'remote',
                hash: this._contentHash(incomingDoc),
                data: incomingDoc,
              },
            ],
          },
        },
        { upsert: true },
      );
    } catch (err) {
      console.error(
        '[mongo-agent] failed to record conflict for',
        collection,
        documentId,
        (err as Error).message,
      );
    }
  }

  /**
   * Writes a tombstone marker so the next scan can propagate the delete.
   * Tombstones live in the `sync_tombstones` collection and are scanned as
   * a regular table — peers reading the tree apply them via
   * {@link _applyTreeToMongo}.
   * @param collection - Name of the collection the deleted doc belonged to.
   * @param documentId - The deleted document's `_id`, recorded so peers can
   *   delete their copy.
   */
  private async _writeTombstone(
    collection: string,
    documentId: unknown,
  ): Promise<void> {
    const tombstones = this._mongoDb.collection('sync_tombstones');
    const key = `${collection}::${String(documentId)}`;
    await tombstones.updateOne(
      { _id: key as any },
      {
        $set: {
          _id: key as any,
          collection,
          documentId,
          deletedAt: new Date(),
        },
      },
      { upsert: true },
    );
  }

  // _removeTombstone was used per-doc in the change-stream path; we
  // dropped that call because it caused 10k Mongo deleteOne calls during
  // a bulk-insert burst. Tombstone cleanup is now batched in Pass 2
  // (deleteMany) and in the test-API afterWrite hook.

  /**
   * Writes a "this doc was just touched locally" marker so the
   * snapshot-pull fallback knows to skip it for a short window. Without
   * this an INSERT or UPDATE made on one peer gets reverted the next time
   * the still-stale snapshot of the OTHER peer is pulled back. See the
   * pull-side filter in sl-auto.ts for the consumer side.
   * @param collection - Name of the collection that was touched locally.
   * @param documentId - The `_id` of the locally inserted/updated document.
   */
  private async _writeRecentChange(
    collection: string,
    documentId: unknown,
  ): Promise<void> {
    const recent = this._mongoDb.collection('sync_recentChanges');
    const key = `${collection}::${String(documentId)}`;
    await recent.updateOne(
      { _id: key as any },
      {
        $set: {
          _id: key as any,
          collection,
          documentId,
          changedAt: Date.now(),
        },
      },
      { upsert: true },
    );
    // NO per-write GC here. The previous `deleteMany({changedAt:{$lt:..}})`
    // on every write was a COLLSCAN (no index on changedAt) and, fired once
    // per doc during a bulk import, became a thundering herd of full-scan
    // deletes that overwhelmed mongod (observed: hundreds of concurrent
    // COLLSCAN deletes → "immediate exit due to unhandled exception").
    // The periodic `_startInternalGc` sweep (every 60 s, now indexed by
    // changedAt) is the single, coordinated GC for this collection.
  }

  /**
   * Loads a ComponentsTable blob and returns its `_data` rows.
   * @param blobId - Blob-store id of the ComponentsTable JSON blob to load.
   * @returns The blob's `_data` rows, or an empty array if it has none.
   */
  private async _loadRows(
    blobId: string,
  ): Promise<Array<Record<string, unknown>>> {
    const blob = await this._bs.getBlob(blobId);
    const componentsTable = JSON.parse(blob.content.toString('utf-8')) as {
      _data?: Array<Record<string, unknown>>;
    };
    return componentsTable._data ?? [];
  }

  /* eslint-disable tsdoc/syntax -- jsdoc/require-param mandates the dotted
     `@param opts.restoreMode` member doc, which the TSDoc parser then rejects
     as an invalid (dotted) identifier; the two rules conflict for an
     inline-object param, so TSDoc is suppressed here. */
  /**
   * Applies a MongoTree to the local MongoDB.
   *
   * Two passes:
   *   1. Tombstones (from `sync_tombstones`) — for every
   *      `{collection, documentId}` tombstone, delete the matching local doc. The
   *      tombstone itself is upserted into the local `sync_tombstones`
   *      collection so it propagates further.
   *   2. Document upserts — for every row in every other collection, look
   *      up the local doc; if content differs, record a conflict and
   *      last-writer-wins. Local-only docs are kept (no orphan-delete) —
   *      deletes are now communicated explicitly via tombstones.
   *
   * Tombstones run BEFORE upserts so that a delete-then-reinsert sequence
   * on a peer arrives as `delete(X) + upsert(X)` and ends with X present.
   *
   * `restoreMode` switches the semantics from "incremental sync" (default)
   * to "snapshot restore":
   *   - per snapshot collection, local docs whose `_id` isn't in the
   *     snapshot get DELETED before the upsert pass, and a fresh
   *     `sync_tombstones` row is written for each so peers also delete
   *     them on the next push. Without these tombstones the peer would
   *     keep its copy (live-sync semantics) and then re-push it, causing
   *     the deleted doc to reappear on the restoring node.
   *   - conflict records are skipped (the user explicitly asked to
   *     overwrite with the snapshot version).
   *   - the Pass 1 60s tombstone-age check is kept (NOT bypassed) — a
   *     snapshot can carry tens of thousands of historical tombstones
   *     and applying each one is two sequential Mongo round-trips →
   *     the restore would hang for minutes. Pass 2's orphan-delete is
   *     sufficient to achieve "DB matches snapshot exactly."
   * @param tree - The RLJSON MongoTree snapshot to apply to the local MongoDB.
   * @param opts - Apply-mode options.
   * @param opts.restoreMode - When `true`, switches from incremental sync to
   *   snapshot-restore semantics (orphan-delete + skip conflict recording).
   */
  /* eslint-enable tsdoc/syntax */
  private async _applyTreeToMongo(
    tree: MongoTree,
    opts: { restoreMode?: boolean } = {},
  ): Promise<void> {
    const restoreMode = opts.restoreMode === true;
    const root = tree.trees.get(tree.rootHash);
    if (!root) return;

    // ---- Collect collection nodes (flat list, with name) ----
    const collections: Array<{ name: string; blobId: string }> = [];
    const collect = (nodeHash: string): void => {
      const node = tree.trees.get(nodeHash);
      if (!node) return;
      const meta = (node.meta ?? {}) as Record<string, any>;
      if (meta.type === 'collection' && meta.componentsBlobId) {
        const collName = meta.collection ?? meta.name;
        if (collName) {
          collections.push({ name: collName, blobId: meta.componentsBlobId });
        }
      } else if (Array.isArray((node as any).children)) {
        for (const childHash of (node as any).children) {
          collect(childHash as string);
        }
      }
    };
    collect(tree.rootHash);

    // Pre-build the set of incoming doc-ids per (collection, _id). A doc
    // appearing in the incoming tree means the sender considers it alive —
    // even if there's also a tombstone for the same id.
    const incomingAlive = new Set<string>();
    const collectionsToUpsert: Array<{ name: string; blobId: string }> = [];
    for (const { name, blobId } of collections) {
      if (name === 'sync_conflicts' || name === 'sync_tombstones') continue;
      collectionsToUpsert.push({ name, blobId });
      const rows = await this._loadRows(blobId);
      for (const row of rows) {
        const id = (row as any)._id;
        if (id !== undefined) incomingAlive.add(`${name}::${String(id)}`);
      }
    }

    // ---- Pass 1: apply tombstones (Last-Write-Wins with 60s window) ----
    const TOMBSTONE_MAX_AGE_MS = 60_000;
    // Tracks `<coll>::<docId>` strings for sync_conflicts tombstones we
    // applied this round. Pass 2 uses this to suppress fresh
    // _recordConflict calls for the same docId — without it the peer's
    // resolution push generates a NEW conflict on this node when its
    // local items doc differs from the incoming "winner" version.
    const resolvedConflictKeys = new Set<string>();
    const tombstoneNode = collections.find((c) => c.name === 'sync_tombstones');
    if (tombstoneNode) {
      const rows = await this._loadRows(tombstoneNode.blobId);
      const localTombstones = this._mongoDb.collection('sync_tombstones');
      const now = Date.now();
      for (const row of rows) {
        const tombstone = { ...row };
        delete (tombstone as Record<string, unknown>)['_hash'];
        const targetColl = (tombstone as any).collection as string | undefined;
        const targetId = (tombstone as any).documentId;
        if (!targetColl || targetId === undefined) continue;
        // Track sync_conflicts tombstones BEFORE the age check: even
        // ancient resolutions should still suppress fresh
        // _recordConflict on the same docId during this apply.
        //
        // The suppression lookup at Pass 2 (line ~1611) uses the logical
        // key "<collName>::<docId>". New tombstones carry
        // `targetCollection` + `documentId` (= conflicted doc's id), so
        // we reconstruct that key. Legacy tombstones used the conflict
        // row's own `_id` as `documentId`; for the "<coll>::<docId>"
        // _id scheme that's already the logical key, so adding the raw
        // string as a fallback covers them too.
        if (targetColl === 'sync_conflicts') {
          const targetCollection = (tombstone as any).targetCollection as
            | string
            | undefined;
          if (targetCollection) {
            resolvedConflictKeys.add(`${targetCollection}::${String(targetId)}`);
          } else {
            resolvedConflictKeys.add(String(targetId));
          }
        }
        // Don't apply tombstones that target sync_tombstones itself
        // (recursive delete would race the GC). sync_conflicts USED to be
        // in this skip too, but that prevented conflict-resolution from
        // propagating: when one node resolved a conflict it deleted the
        // local row, but the peer kept its own row. Now that the resolve
        // handler explicitly writes a tombstone for the conflict id, the
        // peer's Pass 1 deletes its matching sync_conflicts row.
        if (targetColl === 'sync_tombstones') {
          continue;
        }

        const key = `${targetColl}::${String(targetId)}`;

        // Rule 1: sender also says "alive" → delete-then-reinsert; keep.
        if (incomingAlive.has(key)) continue;

        // Rule 2: tombstone too old → ignore. A fresh delete propagates;
        // an ancient marker should not keep killing reinserts forever.
        // Scan-time GC also drops these from sync_tombstones after 60s, so
        // in steady state they never appear here.
        // We keep this check even in restoreMode: a snapshot can contain
        // tens of thousands of historical tombstones, and applying each
        // would do two sequential Mongo round-trips → minutes of hang.
        // Pass 2's orphan-delete (below) already handles "doc inserted
        // after the snapshot must disappear," so Pass 1 only needs to
        // catch the rare fresh-tombstone race.
        //
        // EXCEPTION for sync_conflicts: resolution tombstones MUST apply
        // even when ancient. A conflict isn't a doc that gets "reinserted"
        // — once resolved on one peer, the other peer must drop its row,
        // however late the tombstone arrives. Without this exception, if
        // tree propagation is delayed > 60 s (peer offline, storeTree
        // wedge, slow link), the resolved conflict stays visible forever
        // on the non-resolving peer.
        const rawDeletedAt = (tombstone as any).deletedAt;
        let deletedAtMs: number | null = null;
        if (typeof rawDeletedAt === 'number') deletedAtMs = rawDeletedAt;
        // Non-numeric deletedAt: tombstones arrive via a JSON blob so a BSON
        // Date is already an ISO string (the Date arm is unreachable), and
        // malformed date strings don't occur in practice. These defensive arms
        // aren't exercised identically across platforms — excluded from coverage.
        /* v8 ignore start */
        else if (rawDeletedAt instanceof Date) deletedAtMs = rawDeletedAt.getTime();
        else if (typeof rawDeletedAt === 'string') {
          const d = new Date(rawDeletedAt).getTime();
          if (!isNaN(d)) deletedAtMs = d;
        }
        /* v8 ignore stop */
        if (
          targetColl !== 'sync_conflicts' &&
          deletedAtMs !== null &&
          now - deletedAtMs > TOMBSTONE_MAX_AGE_MS
        ) {
          continue;
        }

        // No "local-alive wins" override: a fresh tombstone is THE delete
        // signal; if we trust it, we must apply it even though we still
        // hold a local copy of the doc. Previously this rule ignored real
        // deletes from peers and the deleted doc kept getting re-pushed.

        // Apply the tombstone: delete local doc + mirror tombstone.
        try {
          if (targetColl === 'sync_conflicts') {
            // sync_conflicts has two writer paths with different `_id`
            // schemes (stable "<coll>::<docId>" vs auto ObjectId). Resolve
            // tombstones carry `targetCollection` so we can match by the
            // LOGICAL conflict key {collection, documentId} regardless of
            // which scheme the peer used. Fall back to legacy _id match
            // for older tombstones that don't carry targetCollection.
            const targetCollection = (tombstone as any).targetCollection as
              | string
              | undefined;
            if (targetCollection) {
              const r = await this._mongoDb
                .collection('sync_conflicts')
                .deleteMany({
                  $or: [
                    { _id: targetId as any },
                    {
                      collection: targetCollection,
                      documentId: String(targetId),
                    },
                  ],
                });
              if (r.deletedCount) {
                console.log(
                  `[mongo-agent] tombstone applied (logical): sync_conflicts ${targetCollection}::${String(targetId)} deletedCount=${r.deletedCount}`,
                );
              }
            } else {
              await this._mongoDb
                .collection('sync_conflicts')
                .deleteOne({ _id: targetId as any });
            }
          } else {
            await this._mongoDb
              .collection(targetColl)
              .deleteOne({ _id: targetId as any });
          }
        } catch (err) {
          console.warn(
            '[mongo-agent] tombstone delete failed:',
            (err as Error).message,
          );
        }
        const localId = (tombstone as any)._id;
        if (localId !== undefined) {
          await localTombstones.replaceOne(
            { _id: localId as any },
            tombstone as any,
            { upsert: true },
          );
        }
      }
    }

    // ---- Pass 2: collection upserts (BATCHED) ----
    // Previously this loop did 3 Mongo round-trips per incoming doc
    // (`deleteOne(tombstone)` + `findOne(local)` + `replaceOne`). On a
    // 10 000-doc tree that's 30 000 sequential round-trips → tens of
    // seconds even on localhost, and the agent appears "frozen" because
    // the event loop is queued full of Mongo I/O. We now:
    //   1. bulk-fetch all current locals in one find({_id: {$in}})
    //   2. bulk-delete tombstones for revived ids in one deleteMany
    //   3. bulk-write all upserts via bulkWrite({ordered: false})
    //   4. queue conflict-records for the (rare) diffs and write them
    //      with a single insertMany at the end of the loop.
    // Net: 4 round-trips per collection regardless of doc count.
    const BATCH_SIZE = 1000;

    // Pre-aggregate snapshot-ids per collection NAME when restoring. Big
    // collections come in as N chunk entries with the same name; orphan
    // deletion must compare against the UNION of all chunks, not against
    // a single chunk — otherwise chunk 2 deletes everything that chunk 1
    // restored and vice versa. Computed once up-front so the per-blob
    // upsert loop below stays single-pass.
    const restoreSnapshotIdsByColl = new Map<string, Set<string>>();
    if (restoreMode) {
      for (const { name, blobId } of collectionsToUpsert) {
        let set = restoreSnapshotIdsByColl.get(name);
        if (!set) {
          set = new Set<string>();
          restoreSnapshotIdsByColl.set(name, set);
        }
        const chunkRows = await this._loadRows(blobId);
        for (const r of chunkRows) {
          const id = (r as any)._id;
          if (id !== undefined) set.add(String(id));
        }
      }
    }
    // Track which collection names already had their orphan-delete pass so
    // a multi-chunk collection only triggers the (expensive) deleteMany once.
    const restoreOrphansHandled = new Set<string>();

    for (const { name: collName, blobId } of collectionsToUpsert) {
      const rows = await this._loadRows(blobId);
      if (restoreMode) {
        console.log(
          `[mongo-agent] restore: loading ${collName} from blob=${blobId.slice(0, 12)}… rows=${rows.length}`,
        );
      }
      const coll = this._mongoDb.collection(collName);

      // In restore mode: delete local docs whose _id isn't in the snapshot
      // for this collection — that's how a doc inserted after the snapshot
      // disappears when the user rolls back. Live sync skips this so an
      // older peer's tree never wipes the local's newer state.
      if (restoreMode && !restoreOrphansHandled.has(collName)) {
        restoreOrphansHandled.add(collName);
        // restoreMode always pre-populates the map for every collection above,
        // so the `?? new Set` fallback is dead.
        /* v8 ignore start */
        const snapshotIds =
          restoreSnapshotIdsByColl.get(collName) ?? new Set<string>();
        /* v8 ignore stop */
        try {
          const localProj = await coll
            .find({}, { projection: { _id: 1 } })
            .toArray();
          const orphanIds = localProj
            .map((d) => (d as any)._id)
            .filter((id) => !snapshotIds.has(String(id)));
          console.log(
            `[mongo-agent] restore: ${collName} snapshot=${snapshotIds.size} local=${localProj.length} orphans=${orphanIds.length}`,
          );
          if (orphanIds.length > 0) {
            // Write tombstones FIRST, before the deleteMany. Without these,
            // the force-push that follows restoreToRef sends a tree that
            // looks like the snapshot — but peers apply it without
            // restoreMode (since they receive it via live sync), so they
            // KEEP their copies of the deleted docs. The peer's next
            // change-stream event then pushes those docs back to us and
            // the restore visually "fails." Tombstones make the deletes
            // propagate the same way live deletes do (Pass 1 in peers).
            // Note: we write tombstones synchronously here rather than
            // relying on the change-stream's _writeTombstone hook because
            // the force-push that follows is racing the change-stream
            // delivery — if the push runs first, the tombstones aren't
            // in the scan yet and peers see no delete signal.
            const tombstones = this._mongoDb.collection('sync_tombstones');
            const now = new Date();
            const tombstoneOps = orphanIds.map((id) => ({
              updateOne: {
                filter: { _id: `${collName}::${String(id)}` as any },
                update: {
                  $set: {
                    _id: `${collName}::${String(id)}` as any,
                    collection: collName,
                    documentId: id,
                    deletedAt: now,
                  },
                },
                upsert: true,
              },
            }));
            try {
              await tombstones.bulkWrite(tombstoneOps, { ordered: false });
            } catch (err) {
              console.warn(
                `[mongo-agent] restore: tombstone write failed for ${collName}: ${(err as Error).message}`,
              );
            }
            await coll.deleteMany({ _id: { $in: orphanIds } as any });
            console.log(
              `[mongo-agent] restore: deleted ${orphanIds.length} orphan(s) in ${collName} (tombstones recorded)`,
            );
          }
        } catch (err) {
          console.warn(
            `[mongo-agent] restore: orphan-delete failed for ${collName}: ${(err as Error).message}`,
          );
        }
      }

      if (rows.length === 0) continue;

      for (let start = 0; start < rows.length; start += BATCH_SIZE) {
        const batch = rows.slice(start, start + BATCH_SIZE);

        // Strip rljson _hash, collect by docId
        const incoming = new Map<unknown, Record<string, unknown>>();
        const docIds: unknown[] = [];
        const tombKeysToClear: string[] = [];
        for (const row of batch) {
          const doc = { ...row };
          delete (doc as Record<string, unknown>)['_hash'];
          const id = (doc as any)._id;
          if (id === undefined) continue;
          incoming.set(id, doc);
          docIds.push(id);
          tombKeysToClear.push(`${collName}::${String(id)}`);
        }
        if (docIds.length === 0) continue;

        // 1 round-trip: fetch all current locals into a map
        const locals = await coll
          .find({ _id: { $in: docIds } as any })
          .toArray();
        const localMap = new Map<unknown, Record<string, unknown>>();
        for (const d of locals) localMap.set((d as any)._id, d as any);

        // 1 round-trip: drop tombstones for all alive ids
        try {
          await this._mongoDb
            .collection('sync_tombstones')
            .deleteMany({ _id: { $in: tombKeysToClear } as any });
        } catch {
          /* best-effort */
        }

        // Build conflict + upsert work
        const conflictsToRecord: Array<{
          docId: unknown;
          incoming: Record<string, unknown>;
          local: Record<string, unknown>;
        }> = [];
        const bulkOps: any[] = [];
        for (const [docId, doc] of incoming) {
          const local = localMap.get(docId);
          if (local) {
            const localHash = this._contentHash(local);
            const incomingHash = this._contentHash(doc);
            if (localHash === incomingHash) continue;
            // Restore mode is an explicit user-initiated overwrite — skip
            // conflict recording, the user already chose the snapshot version.
            // Resolved-by-peer suppression — if Pass 1 just applied a
            // sync_conflicts tombstone for this exact `<coll>::<docId>`,
            // it means the peer resolved this conflict and pushed the
            // winner version. Recording a new conflict here would resurrect
            // the conflict on this node and keep the cycle alive.
            const conflictKey = `${collName}::${String(docId)}`;
            const peerResolved = resolvedConflictKeys.has(conflictKey);
            if (!restoreMode && !peerResolved) {
              conflictsToRecord.push({ docId, incoming: doc, local });
            }
          }
          bulkOps.push({
            replaceOne: {
              filter: { _id: docId as any },
              replacement: doc as any,
              upsert: true,
            },
          });
        }

        // 1 round-trip: bulk upsert
        if (bulkOps.length > 0) {
          try {
            await coll.bulkWrite(bulkOps, { ordered: false });
          } catch (err) {
            console.warn(
              `[mongo-agent] bulkWrite upsert failed (${(err as Error).message}); falling back to per-doc`,
            );
            for (const op of bulkOps) {
              try {
                await coll.replaceOne(
                  op.replaceOne.filter,
                  op.replaceOne.replacement,
                  { upsert: true },
                );
              } catch {
                /* skip */
              }
            }
          }
        }

        // Conflict records — usually empty, so cheap. Use sequential
        // _recordConflict (it does upsert with a stable id) since this
        // path is rare.
        for (const c of conflictsToRecord) {
          await this._recordConflict(collName, c.docId, c.incoming, c.local);
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Internal bookkeeping — GC of the sync_* collections + dirty-partition
  // tracking that feeds the incremental state hash.
  // ---------------------------------------------------------------------------

  /**
   * Periodically trims the internal bookkeeping collections so they can
   * never balloon (the 577 k sync_tombstones storm that pinned the db-lock).
   * Runs every 60 s, deletes anything older than 2 min. Idempotent; safe to
   * call once per syncToDb.
   */
  private _startInternalGc(): void {
    if (this._internalGcTimer) return;
    const sweep = async (): Promise<void> => {
      const cutoffDate = new Date(Date.now() - 120_000);
      const cutoffMs = Date.now() - 120_000;
      try {
        await this._mongoDb
          .collection('sync_tombstones')
          .deleteMany({ deletedAt: { $lt: cutoffDate } as any });
      } catch {
        /* best-effort */
      }
      try {
        await this._mongoDb
          .collection('sync_recentChanges')
          .deleteMany({ changedAt: { $lt: cutoffMs } as any });
      } catch {
        /* best-effort */
      }
    };
    void sweep();
    this._internalGcTimer = setInterval(() => void sweep(), 60_000);
    (this._internalGcTimer as unknown as { unref?: () => void }).unref?.();
  }

  /**
   * Record that `id` in `coll` changed — for the incremental state hash. O(1),
   * in-memory only. Bulk imports overflow the per-collection cap and degrade to
   * a single FULL marker instead of tracking millions of ids.
   * @param coll - Name of the collection whose document changed.
   * @param id - The `_id` of the changed document to mark dirty.
   */
  private _recordDirty(coll: string, id: unknown): void {
    if (this._dirtyFullColls.has(coll)) return;
    let set = this._dirtyIds.get(coll);
    if (!set) {
      set = new Set<unknown>();
      this._dirtyIds.set(coll, set);
    }
    set.add(id);
    if (set.size > this._dirtyCap) {
      // Bulk load — stop tracking individual ids, mark the whole collection.
      this._dirtyFullColls.add(coll);
      this._dirtyIds.delete(coll);
    }
    if (!this._dirtyFlushTimer) {
      this._dirtyFlushTimer = setTimeout(() => {
        this._dirtyFlushTimer = null;
        void this._flushDirty();
      }, this._dirtyFlushMs);
      (this._dirtyFlushTimer as unknown as { unref?: () => void }).unref?.();
    }
  }

  /** Persist the batched dirty markers to `state_dirty`. */
  private async _flushDirty(): Promise<void> {
    const fullColls = [...this._dirtyFullColls];
    this._dirtyFullColls.clear();
    const idBatches = [...this._dirtyIds.entries()];
    this._dirtyIds.clear();
    for (const coll of fullColls) {
      try {
        await markCollectionFullDirty(this._mongoDb, coll);
      } catch {
        /* best-effort */
      }
    }
    for (const [coll, ids] of idBatches) {
      for (const id of ids) {
        try {
          await markDirtyById(this._mongoDb, coll, id as never);
        } catch {
          /* best-effort */
        }
      }
    }
  }

  /**
   * Stops change streams and releases resources.
   */
  dispose(): void {
    if (this._changeStream) {
      this._changeStream.removeAllListeners();
      this._changeStream.close().catch(() => {});
      this._changeStream = null;
    }
    if (this._internalGcTimer) {
      clearInterval(this._internalGcTimer);
      this._internalGcTimer = null;
    }
    if (this._dirtyFlushTimer) {
      clearTimeout(this._dirtyFlushTimer);
      this._dirtyFlushTimer = null;
    }
    // Persist whatever dirty markers are still buffered so a restart doesn't
    // lose them (the next hash would otherwise miss those changes).
    void this._flushDirty();
  }

  // .............................................................................
  // Static factories
  // .............................................................................

  /**
   * Creates a fully-configured MongoAgent from an `@rljson/server` Client.
   * Mirrors {@link FsAgent.fromClient} from `@rljson/fs-agent` so a single
   * call wires up the Connector, the rljson Db, and the simplified sync
   * methods.
   * @param mongoDb - The MongoDB database handle to sync
   * @param treeKey - Tree-table key (must match the server's treeKey)
   * @param client - `@rljson/server` Client (provides .io and .bs)
   * @param socket - Bridge / socket used by the Connector
   * @param options - Optional MongoAgent options
   * @returns Agent instance enriched with `syncToDbSimple` / `syncFromDbSimple`
   */
  static async fromClient(
    mongoDb: MongoDb,
    treeKey: string,
    client: any,
    socket: any,
    options: MongoAgentOptions = {},
  ): Promise<
    MongoAgent & {
      syncToDbSimple: () => Promise<() => void>;
      syncFromDbSimple: () => Promise<() => void>;
    }
  > {
    if (!client?.io) {
      throw new Error('Client.io is not initialized');
    }
    if (!client?.bs) {
      throw new Error('Client.bs is not initialized');
    }

    // Reuse the Client's Bs so blobs end up in the same store the server reads.
    const opts: MongoAgentOptions = { ...options, bs: client.bs as Bs };

    const { Db, Connector } = (await import('@rljson/db')) as any;
    const db = new Db(client.io);
    const route = Route.fromFlat(`/${treeKey}`);
    const connector = new Connector(
      db,
      route,
      socket,
      options.syncConfig,
      options.clientIdentity,
    );

    const agent = new MongoAgent(mongoDb, opts.bs, opts);
    // Store the bridge/socket so syncFromDb can subscribe to bootstrap events
    agent._bridge = socket;
    // Underlying Socket.IO socket — used by the safeSendRef watchdog to
    // force-reconnect on a stuck stream. Caller passes it via opts.rawSocket;
    // we fall back to the bridge (some builds expose .disconnect on it too).
    agent._socket = opts.rawSocket ?? (socket as any)?.socket ?? socket;

    const enhanced = agent as MongoAgent & {
      syncToDbSimple: () => Promise<() => void>;
      syncFromDbSimple: () => Promise<() => void>;
    };

    // syncFromDb resolves only after the initial bootstrap (or its timeout),
    // so callers can `await syncFromDbSimple()` before `syncToDbSimple()` to
    // guarantee pull-before-push ordering.
    enhanced.syncToDbSimple = async () => {
      // Block until bootstrap-on-connect has happened, so we don't overwrite
      // the server's existing state with our stale local state.
      if (agent._bootstrapDone) {
        try {
          await agent._bootstrapDone;
        } catch {
          /* best-effort */
        }
      }
      return agent.syncToDb(db, connector, treeKey);
    };
    enhanced.syncFromDbSimple = () => agent.syncFromDb(db, connector, treeKey);

    if (typeof client.onDisconnect === 'function') {
      client.onDisconnect(() => {
        agent.dispose();
      });
    }

    return enhanced;
  }
}

// .............................................................................

/**
 * Sends a ref through a Connector, with optional ack support.
 * @param connector - The rljson Connector to send through; uses `sendWithAck`
 *   when the sync config requires acknowledgement, otherwise plain `send`.
 * @param ref - The tree root hash to broadcast to peers.
 */
async function sendRef(connector: any, ref: string): Promise<void> {
  if (connector?.syncConfig?.requireAck && typeof connector.sendWithAck === 'function') {
    await connector.sendWithAck(ref);
    return;
  }
  if (typeof connector.send === 'function') {
    connector.send(ref);
  }
}
