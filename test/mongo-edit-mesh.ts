// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Db } from '@rljson/db';
import type { Io } from '@rljson/io';
import { IoMem, IoMulti } from '@rljson/io';
import type { Json, JsonValue } from '@rljson/json';
import type { ContentType, Rljson, TableCfg, TableKey } from '@rljson/rljson';
import type { Document } from 'mongodb';

import { MongoEditSync } from '../src/mongo-edit-sync.ts';

/**
 * A multi-node in-process mesh for the components/edits sync.
 *
 * It is a faithful model of the production topology, not a mock of it:
 *
 * - Each node has its OWN MongoDB (documents + a live change stream that fires
 *   on every write, including the writes the sync itself applies — that is what
 *   exercises echo suppression).
 * - Each node has its OWN rljson `Db` over an `IoMulti` shaped exactly like the
 *   client's: a local, dumpable, writable `IoMem` at priority 1 and a
 *   READ-ONLY, NON-DUMPABLE peer at priority 2 (`@rljson/server` `Client`
 *   registers `IoPeer` with `dump: false, write: false, priority: 2`). Only the
 *   `readRows` / `readRowsByHashes` paths reach a peer; `dumpTable` cannot see
 *   a row that lives on another node.
 * - Refs travel over a bus with the same send-/received-dedup semantics as
 *   `Connector` (`@rljson/db`), including `reannounce` (dedup-bypassing
 *   re-emit) and `invalidateReceived`.
 */

// .............................................................................
/** A hand-driven stand-in for a MongoDB change stream. */
class MeshChangeStream {
  private readonly _handlers = new Map<string, Array<(a: unknown) => void>>();
  closed = false;

  on(event: string, listener: (arg: unknown) => void): this {
    const list = this._handlers.get(event) ?? [];
    list.push(listener);
    this._handlers.set(event, list);
    return this;
  }

  emit(event: string, arg: unknown): void {
    if (this.closed) return;
    for (const handler of this._handlers.get(event) ?? []) handler(arg);
  }

  close(): void {
    this.closed = true;
  }
}

// .............................................................................
/**
 * A MongoDB collection with real write semantics: a write mutates the store AND
 * emits a change-stream event (asynchronously, as the driver does).
 */
export class MeshCollection {
  readonly docs = new Map<string, Document>();
  private readonly _streams = new Set<MeshChangeStream>();
  private _seq = 0;

  /** Every change event this collection ever emitted (for diagnostics). */
  readonly emitted: Array<Record<string, unknown>> = [];
  /**
   * Every write this collection ever saw, in order, tagged with its source.
   * `sync` writes come from `MongoEditSync._applyHead`; `app` writes come from
   * the node's own application. The history is what makes a REGRESSION
   * (a document going backwards) observable even when the end state happens to
   * look right — the field's "it moves between nodes".
   */
  readonly history: Array<{
    source: 'app' | 'sync';
    op: 'put' | 'del';
    id: string;
    doc?: Document;
  }> = [];
  /** Writes applied by the sync (not by the application). */
  syncWrites = 0;

  find(): AsyncIterable<Document> {
    const snapshot = [...this.docs.values()];
    return {
      async *[Symbol.asyncIterator]() {
        for (const doc of snapshot) yield doc;
      },
    };
  }

  watch(): MeshChangeStream {
    const stream = new MeshChangeStream();
    this._streams.add(stream);
    return stream;
  }

  /** Emits a change event on the next microtask, like the real driver. */
  private _emit(change: Record<string, unknown>): void {
    const full = { ...change, _id: { _data: `t${++this._seq}` } };
    this.emitted.push(full);
    queueMicrotask(() => {
      for (const stream of this._streams) stream.emit('change', full);
    });
  }

  // -- the driver surface `MongoEditSync._applyHead` uses -------------------

  async replaceOne(
    _filter: { _id: unknown },
    doc: Document,
  ): Promise<unknown> {
    const existed = this.docs.has(String(doc['_id']));
    this.syncWrites++;
    this.history.push({
      source: 'sync',
      op: 'put',
      id: String(doc['_id']),
      doc,
    });
    this.docs.set(String(doc['_id']), doc);
    this._emit({
      operationType: existed ? 'replace' : 'insert',
      documentKey: { _id: doc['_id'] },
      fullDocument: doc,
    });
    return {};
  }

  async deleteOne(filter: { _id: unknown }): Promise<unknown> {
    this.syncWrites++;
    this.history.push({ source: 'sync', op: 'del', id: String(filter._id) });
    if (!this.docs.delete(String(filter._id))) return {};
    this._emit({
      operationType: 'delete',
      documentKey: { _id: filter._id },
    });
    return {};
  }

  // -- the surface the "application" on a node uses --------------------------

  /** An application-side insert/update (`replaceOne` with an explicit op). */
  write(doc: Document): void {
    const existed = this.docs.has(String(doc['_id']));
    this.history.push({ source: 'app', op: 'put', id: String(doc['_id']), doc });
    this.docs.set(String(doc['_id']), doc);
    this._emit({
      operationType: existed ? 'update' : 'insert',
      documentKey: { _id: doc['_id'] },
      fullDocument: doc,
    });
  }

  /** An application-side delete. */
  remove(id: unknown): void {
    this.history.push({ source: 'app', op: 'del', id: String(id) });
    if (!this.docs.delete(String(id))) return;
    this._emit({ operationType: 'delete', documentKey: { _id: id } });
  }

  /** The collection as a plain `_id → doc` object, for convergence asserts. */
  snapshot(): Record<string, Document> {
    return Object.fromEntries([...this.docs.entries()].sort());
  }
}

// .............................................................................
/** A MongoDB database made of {@link MeshCollection}s. */
export class MeshMongoDb {
  private readonly _cols = new Map<string, MeshCollection>();

  collection(name: string): MeshCollection {
    let col = this._cols.get(name);
    if (!col) {
      col = new MeshCollection();
      this._cols.set(name, col);
    }
    return col;
  }
}

// .............................................................................
/**
 * A read-only, NON-DUMPABLE view onto the other nodes' local stores — the
 * in-process equivalent of `IoPeer` behind the relay. Reads cascade to the
 * peers; `dump`/`dumpTable`/`write` reject, exactly as they are unavailable
 * (or not registered) across the relay.
 */
export class MeshPeerIo implements Io {
  isOpen = true;
  /** Number of `readRows` round trips served (a proxy for socket traffic). */
  readRowCalls = 0;
  /** Number of `readRowsByHashes` round trips served. */
  batchReadCalls = 0;
  /** Hashes requested per batch read, so a test can assert batching. */
  batchSizes: number[] = [];
  /**
   * Row hashes the SINGLE-ROW read path refuses to serve, modelling a row that
   * is referenced by a head but not resolvable through the ordinary relay read
   * (the field's "partial pull"). The batch content-hash path still serves it.
   */
  readonly unresolvableBySingleRead = new Set<string>();
  /**
   * Tables whose rows the SINGLE-ROW read path refuses entirely. Models the
   * production asymmetry the field report names: a row referenced by a head is
   * not delivered by the ordinary read path, while the content-hash batch path
   * (`readRowsByHashes`) still resolves it.
   */
  readonly singleReadBlockedTables = new Set<string>();
  /**
   * When true, BOTH read paths (`readRows` and `readRowsByHashes`) fail, so a
   * pull comes back empty — models a reconnecting node whose origin rows are not
   * yet resolvable in the reconnect race. Flip back to false to let the retry
   * succeed.
   */
  blockReads = false;

  constructor(private readonly _peers: () => Io[]) {}

  /** Resets the round-trip counters (call before a measured phase). */
  resetCounters(): void {
    this.readRowCalls = 0;
    this.batchReadCalls = 0;
    this.batchSizes = [];
  }

  async init(): Promise<void> {}
  async close(): Promise<void> {
    this.isOpen = false;
  }
  async isReady(): Promise<void> {}

  async dump(): Promise<Rljson> {
    throw new Error('The relay does not dump');
  }

  async dumpTable(): Promise<Rljson> {
    throw new Error('The relay does not dump');
  }

  async contentType(request: { table: string }): Promise<ContentType> {
    for (const peer of this._peers()) {
      try {
        return await peer.contentType(request);
      } catch {
        continue;
      }
    }
    throw new Error(`Table "${request.table}" not found`);
  }

  async tableExists(tableKey: TableKey): Promise<boolean> {
    for (const peer of this._peers()) {
      if (await peer.tableExists(tableKey)) return true;
    }
    return false;
  }

  async createOrExtendTable(): Promise<void> {
    throw new Error('The relay is read-only');
  }

  async rawTableCfgs(): Promise<TableCfg[]> {
    const cfgs = new Map<string, TableCfg>();
    for (const peer of this._peers()) {
      for (const cfg of await peer.rawTableCfgs()) {
        if (!cfgs.has(cfg.key)) cfgs.set(cfg.key, cfg);
      }
    }
    return [...cfgs.values()];
  }

  async write(): Promise<void> {
    throw new Error('The relay is read-only');
  }

  async readRows(request: {
    table: string;
    where: { [column: string]: JsonValue | null };
  }): Promise<Rljson> {
    this.readRowCalls++;
    if (this.blockReads) throw new Error(`Timeout after 30000ms: readRows`);
    const wanted = request.where['_hash'];
    if (
      this.singleReadBlockedTables.has(request.table) ||
      (typeof wanted === 'string' &&
        this.unresolvableBySingleRead.has(wanted))
    ) {
      // Exactly what IoMulti sees when a peer read fails: the table exists
      // (locally) but the row comes back absent, silently.
      throw new Error(`Timeout after 30000ms: readRows`);
    }
    let found = false;
    const rows = new Map<string, Json>();
    let type: ContentType | undefined;
    for (const peer of this._peers()) {
      try {
        const result = await peer.readRows(request);
        const table = result[request.table] as {
          _data: Json[];
          _type: ContentType;
        };
        found = true;
        type ??= table._type;
        for (const row of table._data) {
          rows.set((row as { _hash?: string })._hash as string, row);
        }
      } catch {
        continue;
      }
    }
    if (!found) throw new Error(`Table "${request.table}" not found`);
    return {
      [request.table]: { _data: [...rows.values()], _type: type },
    } as unknown as Rljson;
  }

  async readRowsByHashes(request: {
    table: string;
    hashes: string[];
  }): Promise<Rljson> {
    this.batchReadCalls++;
    this.batchSizes.push(request.hashes.length);
    if (this.blockReads) {
      throw new Error(`Timeout after 30000ms: readRowsByHashes`);
    }
    let found = false;
    const rows = new Map<string, Json>();
    let type: ContentType | undefined;
    for (const peer of this._peers()) {
      try {
        const result = peer.readRowsByHashes
          ? await peer.readRowsByHashes(request)
          : await peer.readRows({
              table: request.table,
              where: { _hash: request.hashes[0] },
            });
        const table = result[request.table] as {
          _data: Json[];
          _type: ContentType;
        };
        found = true;
        type ??= table._type;
        for (const row of table._data) {
          rows.set((row as { _hash?: string })._hash as string, row);
        }
      } catch {
        continue;
      }
    }
    if (!found) throw new Error(`Table "${request.table}" not found`);
    return {
      [request.table]: { _data: [...rows.values()], _type: type },
    } as unknown as Rljson;
  }

  async rowCount(): Promise<number> {
    throw new Error('The relay does not count');
  }
}

// .............................................................................
/**
 * The ref bus: every node's connector, wired to every other node's, with the
 * dedup semantics of `Connector` (`@rljson/db`).
 */
export class MeshBus {
  private readonly _nodes: MeshConnector[] = [];
  /** Every ref ever delivered, in order (`from → ref`), for diagnostics. */
  readonly traffic: Array<{ from: string; ref: string }> = [];

  connector(id: string): MeshConnector {
    const connector = new MeshConnector(id, this);
    this._nodes.push(connector);
    return connector;
  }

  deliver(from: string, ref: string): void {
    this.traffic.push({ from, ref });
    for (const node of this._nodes) {
      if (node.id === from) continue;
      node.receive(ref);
    }
  }
}

// .............................................................................
/** One node's connector: send-dedup out, received-dedup in. */
export class MeshConnector {
  private _cb: ((ref: string) => void | Promise<void>) | undefined;
  private readonly _sent = new Set<string>();
  private readonly _received = new Set<string>();

  constructor(
    readonly id: string,
    private readonly _bus: MeshBus,
  ) {}

  send(ref: string): void {
    if (this._sent.has(ref)) return;
    this._sent.add(ref);
    this._bus.deliver(this.id, ref);
  }

  reannounce(ref: string): void {
    this._bus.deliver(this.id, ref);
  }

  invalidateReceived(ref: string): void {
    this._received.delete(ref);
  }

  listen(cb: (ref: string) => void | Promise<void>): void {
    this._cb = cb;
  }

  receive(ref: string): void {
    if (this._received.has(ref)) return;
    this._received.add(ref);
    void this._cb?.(ref);
  }
}

// .............................................................................
/** One mesh node: mongo + rljson db + connector + the sync under test. */
export interface MeshNode {
  id: string;
  mongo: MeshMongoDb;
  local: IoMem;
  peer: MeshPeerIo;
  db: Db;
  connector: MeshConnector;
  sync: MongoEditSync;
  /** Applies an application-side upsert on this node. */
  put(collection: string, doc: Document): void;
  /** Applies an application-side delete on this node. */
  del(collection: string, id: unknown): void;
}

// .............................................................................
/**
 * Builds an n-node mesh, each node syncing `collections`.
 * @param count - Number of nodes.
 * @param collections - The collections every node syncs.
 * @returns The started nodes and a `stop()` that shuts them all down.
 */
export const buildMesh = async (
  count: number,
  collections: string[],
): Promise<{ nodes: MeshNode[]; stop: () => Promise<void> }> => {
  const locals: IoMem[] = [];
  const nodes: MeshNode[] = [];
  const bus = new MeshBus();

  for (let i = 0; i < count; i++) {
    const local = new IoMem();
    await local.init();
    await local.isReady();
    locals.push(local);
  }

  for (let i = 0; i < count; i++) {
    const id = String.fromCharCode(65 + i); // A, B, C, …
    const local = locals[i];
    const peer = new MeshPeerIo(() => locals.filter((_, j) => j !== i));
    await peer.init();
    const io = new IoMulti([
      { io: local, priority: 1, read: true, write: true, dump: true },
      { io: peer, priority: 2, read: true, write: false, dump: false },
    ]);
    await io.init();
    const db = new Db(io);
    const mongo = new MeshMongoDb();
    const connector = bus.connector(id);
    const sync = new MongoEditSync(
      mongo as never,
      db,
      connector,
      collections,
      'p',
    );
    nodes.push({
      id,
      mongo,
      local,
      peer,
      db,
      connector,
      sync,
      put: (collection, doc) => mongo.collection(collection).write(doc),
      del: (collection, docId) => mongo.collection(collection).remove(docId),
    });
  }

  for (const node of nodes) await node.sync.start();

  return {
    nodes,
    stop: async () => {
      for (const node of nodes) await node.sync.stop();
    },
  };
};

// .............................................................................
/** Resolves after `ms` milliseconds. */
export const wait = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

// .............................................................................
/**
 * Lets the mesh settle: waits until no node has an in-flight apply and the ref
 * bus has been quiet for a whole poll interval, or until `timeoutMs` elapses.
 * @param nodes - The mesh nodes.
 * @param timeoutMs - Give-up budget.
 * @param quietMs - How long the bus must be quiet to count as settled.
 */
export const settle = async (
  nodes: MeshNode[],
  timeoutMs = 4000,
  quietMs = 60,
): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  let quietSince = Date.now();
  let lastSeen = -1;
  while (Date.now() < deadline) {
    await wait(15);
    for (const node of nodes) {
      const chains = (
        node.sync as unknown as { _applyChain: Map<string, Promise<void>> }
      )._applyChain;
      await Promise.all([...chains.values()].map((p) => p.catch(() => {})));
    }
    const seen = nodes.reduce(
      (sum, node) =>
        sum +
        (
          node.connector as unknown as { _received: Set<string> }
        )._received.size,
      0,
    );
    if (seen !== lastSeen) {
      lastSeen = seen;
      quietSince = Date.now();
      continue;
    }
    if (Date.now() - quietSince >= quietMs) return;
  }
};

// .............................................................................
/**
 * The documents of a collection on a node, as comparable plain JSON.
 * @param node - The node to read.
 * @param collection - The collection to read.
 * @returns `_id → document` with BSON values stringified.
 */
export const docsOf = (
  node: MeshNode,
  collection: string,
): Record<string, unknown> => {
  const out: Record<string, unknown> = {};
  for (const [id, doc] of node.mongo.collection(collection).docs) {
    out[id] = JSON.parse(JSON.stringify(doc));
  }
  return Object.fromEntries(Object.entries(out).sort());
};

// .............................................................................
/**
 * Restarts a node's sync in place: stops it and starts a fresh
 * {@link MongoEditSync} over the SAME mongo, rljson db and connector. Models a
 * process restart — the durable state (mongo + local io) survives, all
 * in-memory sync state does not.
 * @param node - The node to restart.
 * @param collections - The collections the restarted sync watches.
 */
export const restartNode = async (
  node: MeshNode,
  collections: string[],
): Promise<void> => {
  await node.sync.stop();
  node.sync = new MongoEditSync(
    node.mongo as never,
    node.db,
    node.connector,
    collections,
    'p',
  );
  await node.sync.start();
};

// .............................................................................
/**
 * Waits until every node holds the SAME documents for a collection and HOLDS
 * that state for `stableMs`. A mesh that keeps flipping between two peers'
 * lineages agrees only momentarily, so a single snapshot comparison can pass
 * by luck; requiring the agreement to persist is what makes "10/10
 * convergence" testable.
 * @param nodes - The mesh nodes.
 * @param collection - The collection to compare.
 * @param budgetMs - Give-up budget.
 * @param stableMs - How long the agreement must hold.
 * @returns The agreed documents.
 * @throws If the nodes never agree, or never hold their agreement.
 */
export const converge = async (
  nodes: MeshNode[],
  collection: string,
  budgetMs = 8000,
  stableMs = 400,
): Promise<Record<string, unknown>> => {
  const deadline = Date.now() + budgetMs;
  let agreed: string | undefined;
  let agreedSince = 0;
  let flips = 0;
  let last = '';
  while (Date.now() < deadline) {
    await wait(20);
    const snapshots = nodes.map((node) =>
      JSON.stringify(docsOf(node, collection)),
    );
    last = nodes
      .map((node, i) => `  ${node.id}: ${snapshots[i]}`)
      .join('\n');
    const allEqual = snapshots.every((s) => s === snapshots[0]);
    if (allEqual && snapshots[0] === agreed) {
      if (Date.now() - agreedSince >= stableMs) {
        return JSON.parse(snapshots[0]) as Record<string, unknown>;
      }
      continue;
    }
    if (agreed !== undefined) flips++;
    agreed = allEqual ? snapshots[0] : undefined;
    agreedSince = Date.now();
  }
  throw new Error(
    `nodes did not converge and hold for ${stableMs}ms within ${budgetMs}ms ` +
      `(${flips} state changes while waiting). Last state:\n${last}`,
  );
};

// .............................................................................
/**
 * Asserts no node ever moved a document BACKWARDS: for every document, the
 * value of `versionField` never decreases over that node's write history, and
 * a document is never re-created after that node deleted it.
 *
 * This is the invariant the field report is about. A mesh that replays a
 * peer's whole lineage writes stale versions over newer ones; whether the
 * damage is still visible at the end depends on who spoke last, so only the
 * history makes the defect deterministic.
 * @param nodes - The mesh nodes.
 * @param collection - The collection to check.
 * @param versionField - The monotonically increasing field on each document.
 */
export const expectNoRegression = (
  nodes: MeshNode[],
  collection: string,
  versionField = 'v',
): void => {
  const problems: string[] = [];
  for (const node of nodes) {
    const seen = new Map<string, number>();
    const deleted = new Set<string>();
    for (const entry of node.mongo.collection(collection).history) {
      if (entry.op === 'del') {
        deleted.add(entry.id);
        continue;
      }
      const version = Number(entry.doc?.[versionField] ?? NaN);
      if (Number.isNaN(version)) continue;
      const previous = seen.get(entry.id);
      if (previous !== undefined && version < previous) {
        problems.push(
          `node ${node.id}: ${entry.id} went ${previous} → ${version} ` +
            `(${entry.source} write)`,
        );
      }
      if (deleted.has(entry.id) && entry.source === 'sync') {
        problems.push(
          `node ${node.id}: ${entry.id} was resurrected after a delete`,
        );
        deleted.delete(entry.id);
      }
      seen.set(entry.id, Math.max(previous ?? version, version));
    }
  }
  if (problems.length > 0) {
    throw new Error(
      `${problems.length} document regression(s):\n  ${problems.join('\n  ')}`,
    );
  }
};

// .............................................................................
/**
 * Total writes the SYNC applied into a node's mongo (application writes
 * excluded), and resets the counter.
 * @param node - The node to read.
 * @param collection - The collection to read.
 * @returns The number of sync-applied writes since the last reset.
 */
export const takeSyncWrites = (node: MeshNode, collection: string): number => {
  const col = node.mongo.collection(collection);
  const count = col.syncWrites;
  col.syncWrites = 0;
  return count;
};
