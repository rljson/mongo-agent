// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Route, syncEvents } from '@rljson/rljson';
import type { ChangeStream, Db as MongoDb, Document } from 'mongodb';

import { MongoCakeAdapter } from './mongo-cake-adapter.ts';
import {
  MongoCakeSync,
  type MongoCakeSyncOptions,
  type MongoStore,
} from './mongo-cake-sync.ts';

// .............................................................................
// Runtime wiring for the Cake/Layer/Component sync (#7, increment 3.5).
//
// Binds MongoCakeSync to a live `mongodb` database (MongoDbStore) and to the
// existing rljson Client/Connector: the cake hash travels as the broadcast ref
// on the treeKey route exactly like the tree root hash did, while the cake /
// layer / component rows are pulled by hash (readRowsByHashes) — cascading to
// the peer independently of the route. Reuses the server's bootstrap heartbeat
// so a (re)joining node converges on the current cake.
// .............................................................................

/**
 * Compiles a `*`-glob (e.g. `sync_*`) to an anchored RegExp.
 * @param glob - The `*`-glob pattern.
 * @returns An anchored RegExp matching the whole collection name.
 */
const globToRe = (glob: string): RegExp =>
  new RegExp(
    '^' +
      glob.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*') +
      '$',
  );

/** A {@link MongoStore} backed by a real `mongodb` database. */
export class MongoDbStore implements MongoStore {
  private readonly _ignore: RegExp[];

  constructor(
    private readonly _db: MongoDb,
    ignore: string[] = [],
  ) {
    this._ignore = ['system.*', 'sync_*', 'state_*', 'rljson_*', ...ignore].map(
      globToRe,
    );
  }

  /**
   * Whether a collection is excluded from sync (internal or ignore-listed).
   * @param name - The collection name.
   * @returns True when the collection should not be synced.
   */
  isIgnored(name: string): boolean {
    return this._ignore.some((re) => re.test(name));
  }

  async listCollections(): Promise<string[]> {
    const infos = await this._db.listCollections({}, { nameOnly: true }).toArray();
    return infos.map((i) => i.name).filter((n) => !this.isIgnored(n)).sort();
  }

  async readCollection(name: string): Promise<Document[]> {
    return this._db.collection(name).find({}).toArray();
  }

  async countDocuments(name: string): Promise<number> {
    return this._db.collection(name).estimatedDocumentCount();
  }

  async applyChanges(
    name: string,
    upserts: Document[],
    deletes: unknown[],
  ): Promise<void> {
    const coll = this._db.collection(name);
    if (upserts.length > 0) {
      await coll.bulkWrite(
        upserts.map((doc) => ({
          replaceOne: {
            filter: { _id: (doc as Record<string, unknown>)['_id'] as never },
            replacement: doc,
            upsert: true,
          },
        })),
        { ordered: false },
      );
    }
    if (deletes.length > 0) {
      await coll.deleteMany({ _id: { $in: deletes as never[] } });
    }
  }
}

/** The minimal initialised rljson Client surface the cake agent needs. */
export interface CakeClient {
  /** The Client's rljson Io (its read cascade reaches the cloud peer). */
  io: unknown;
}

/** Options for {@link MongoCakeAgent.fromClient}. */
export interface MongoCakeAgentOptions extends MongoCakeSyncOptions {
  /** Collection glob patterns to exclude from sync. */
  ignore?: string[];
  /** Debounce window (ms) between a Mongo change and the resulting push. */
  debounceMs?: number;
  /** rljson sync config forwarded to the Connector. */
  syncConfig?: unknown;
  /** Stable client identity forwarded to the Connector. */
  clientIdentity?: unknown;
}

/**
 * The tombstone-free Cake/Layer/Component agent: watches a Mongo database and
 * bidirectionally syncs it with peers over an rljson Client, using content
 * hashes for change-detection and layer-absence for deletes.
 */
export class MongoCakeAgent {
  private _changeStream: ChangeStream | null = null;
  private _debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private _pushing = false;
  private _pushAgain = false;
  private _stopped = false;
  // Collections changed since the last push (the incremental work-list). When
  // `_dirtyAll` is set (a change with no namespace), the next push rebuilds all.
  private readonly _dirty = new Set<string>();
  private _dirtyAll = false;

  private constructor(
    private readonly _mongoDb: MongoDb,
    private readonly _treeKey: string,
    private readonly _store: MongoDbStore,
    private readonly _sync: MongoCakeSync,
    private readonly _connector: { send: (ref: string) => void },
    private readonly _bridge: {
      on: (event: string, handler: (payload: unknown) => void) => void;
    },
    private readonly _debounceMs: number,
    private readonly _log: (message: string) => void,
  ) {}

  /**
   * Builds a cake agent from an initialised rljson Client.
   * @param mongoDb - The live `mongodb` database to sync.
   * @param treeKey - The route/table key used as the broadcast channel.
   * @param client - An initialised `@rljson/server` Client (provides `io`).
   * @param socket - The Client's bridge/socket (Connector transport + events).
   * @param options - Ignore list, debounce, sync config and safety limits.
   * @returns An agent exposing `syncFromDbSimple()` / `syncToDbSimple()`.
   */
  static async fromClient(
    mongoDb: MongoDb,
    treeKey: string,
    client: CakeClient,
    socket: unknown,
    options: MongoCakeAgentOptions = {},
  ): Promise<MongoCakeAgent> {
    const { Db, Connector } = (await import('@rljson/db')) as {
      Db: new (io: unknown) => unknown;
      Connector: new (...args: unknown[]) => { send: (ref: string) => void };
    };
    const db = new Db(client.io);
    const route = Route.fromFlat(`/${treeKey}`);
    const connector = new Connector(
      db,
      route,
      socket,
      options.syncConfig,
      options.clientIdentity,
    );

    const log = options.log ?? (() => {});
    const store = new MongoDbStore(mongoDb, options.ignore);
    const adapter = new MongoCakeAdapter(db as never);
    const sync = new MongoCakeSync(store, adapter, { ...options, log });

    return new MongoCakeAgent(
      mongoDb,
      treeKey,
      store,
      sync,
      connector,
      socket as never,
      options.debounceMs ?? 800,
      log,
    );
  }

  /**
   * Subscribes to inbound cake refs (bootstrap + live) and applies them to
   * Mongo. Resolves immediately; convergence is driven by the server's
   * bootstrap heartbeat re-announcing the current cake to this late joiner.
   * @returns A stop function that unsubscribes.
   */
  async syncFromDb(): Promise<() => void> {
    const events = syncEvents(`/${this._treeKey}`) as {
      bootstrap: string;
      ref: string;
    };
    const onRef = (payload: unknown): void => {
      const p = payload as { r?: string; ref?: string };
      const ref = p?.r ?? p?.ref;
      if (typeof ref === 'string' && ref) void this._applyRef(ref);
    };
    this._bridge.on(events.bootstrap, onRef);
    this._bridge.on(events.ref, onRef);
    return () => {
      this._stopped = true;
    };
  }

  /**
   * Pushes the current Mongo state, then watches for changes and re-pushes
   * (debounced). The initial push seeds peers; the change stream keeps them
   * current.
   * @returns A stop function that closes the change stream.
   */
  async syncToDb(): Promise<() => void> {
    await this._pushSnapshot();
    this._openChangeStream();
    return () => {
      this._stopped = true;
      if (this._debounceTimer) clearTimeout(this._debounceTimer);
      this._safeClose();
      this._changeStream = null;
    };
  }

  /**
   * Alias for {@link syncFromDb}, matching the MongoAgent surface so the shared
   * sl-mongo-agent runtime can drive either agent uniformly.
   * @returns A stop function.
   */
  syncFromDbSimple(): Promise<() => void> {
    return this.syncFromDb();
  }

  /**
   * Alias for {@link syncToDb}, matching the MongoAgent surface.
   * @returns A stop function.
   */
  syncToDbSimple(): Promise<() => void> {
    return this.syncToDb();
  }

  /**
   * Forces an immediate snapshot + broadcast, bypassing the change-stream
   * debounce (used by the Test API's force-sync).
   * @returns The pushed cake hash, or null when nothing changed.
   */
  async forcePush(): Promise<string | null> {
    await this._pushSnapshot();
    return this._sync.lastSentHash;
  }

  /** Stops the agent: halts pushes/applies and closes the change stream. */
  dispose(): void {
    this._stopped = true;
    if (this._debounceTimer) clearTimeout(this._debounceTimer);
    this._safeClose();
    this._changeStream = null;
  }

  // ...........................................................................

  /** Closes the current change stream, swallowing a rejecting close. */
  private _safeClose(): void {
    const stream = this._changeStream;
    if (!stream) return;
    /* v8 ignore next -- a rejecting close() is swallowed by design */
    void stream.close().catch(() => {});
  }

  /**
   * Applies one inbound cake ref to Mongo, tolerating transient errors.
   * @param cakeHash - The cake hash announced by a peer.
   */
  private async _applyRef(cakeHash: string): Promise<void> {
    if (this._stopped) return;
    try {
      const res = await this._sync.applyIncoming(cakeHash);
      if (res && (res.upserted > 0 || res.deleted > 0 || res.blockedDeletes.length > 0)) {
        this._log(
          `[mongo-cake] applied ${cakeHash.slice(0, 8)}: +${res.upserted} -${res.deleted}` +
            (res.blockedDeletes.length
              ? ` (blocked deletes: ${res.blockedDeletes.join(',')})`
              : ''),
        );
      }
    } catch (err) {
      this._log(`[mongo-cake] apply failed for ${cakeHash.slice(0, 8)}: ${(err as Error).message}`);
    }
  }

  /**
   * Builds + stores the snapshot and broadcasts its cake hash. Rebuilds only the
   * given changed collections; omitted `dirty` forces a full rebuild (initial
   * push and force-sync).
   * @param dirty - Collections that changed, or undefined for a full rebuild.
   */
  private async _pushSnapshot(dirty?: Set<string>): Promise<void> {
    if (this._stopped) return;
    try {
      const ref = await this._sync.pushSnapshot(dirty);
      if (ref) {
        this._connector.send(ref);
        this._log(`[mongo-cake] pushed cake ${ref.slice(0, 8)}`);
      }
    } catch (err) {
      this._log(`[mongo-cake] push failed: ${(err as Error).message}`);
    }
  }

  /** Opens the Mongo change stream and schedules debounced re-pushes. */
  private _openChangeStream(): void {
    if (this._stopped) return;
    const stream = this._mongoDb.watch([], { fullDocument: 'updateLookup' });
    this._changeStream = stream;
    stream.on('change', (change: unknown) => {
      const ns = (change as { ns?: { coll?: string } }).ns;
      const coll = ns?.coll;
      // Ignore churn on non-synced (internal / ignore-listed) collections.
      if (coll && this._store.isIgnored(coll)) return;
      // Record which collection changed so the next push rebuilds only that one.
      if (coll) this._dirty.add(coll);
      else this._dirtyAll = true;
      this._schedulePush();
    });
    stream.on('error', () => this._reopenChangeStream());
    stream.on('close', () => {
      if (!this._stopped) this._reopenChangeStream();
    });
  }

  /** Reopens the change stream after an error/close (unless stopped). */
  private _reopenChangeStream(): void {
    if (this._stopped) return;
    this._safeClose();
    this._changeStream = null;
    setTimeout(() => this._openChangeStream(), 1000);
  }

  /** Debounces pushes and coalesces a change that arrives mid-push. */
  private _schedulePush(): void {
    if (this._debounceTimer) clearTimeout(this._debounceTimer);
    this._debounceTimer = setTimeout(() => {
      this._debounceTimer = null;
      if (this._pushing) {
        this._pushAgain = true;
        return;
      }
      void this._runPush();
    }, this._debounceMs);
  }

  /** Runs a push, then repeats once if a change arrived while it was running. */
  private async _runPush(): Promise<void> {
    // Drain the accumulated changed-collection set for this push; a change that
    // lands mid-push re-populates it and triggers the coalesced follow-up.
    const dirty = this._dirtyAll ? undefined : new Set(this._dirty);
    this._dirty.clear();
    this._dirtyAll = false;
    this._pushing = true;
    try {
      await this._pushSnapshot(dirty);
    } finally {
      this._pushing = false;
    }
    if (this._pushAgain) {
      this._pushAgain = false;
      this._schedulePush();
    }
  }
}
