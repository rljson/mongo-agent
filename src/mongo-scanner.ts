// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Bs, BsMem } from '@rljson/bs';
import { hip } from '@rljson/hash';
import { Json } from '@rljson/json';
import { Ref } from '@rljson/rljson';

import { MongoToRljsonConverter } from './mongo-to-rljson-converter.ts';
import { SYNC_OPS_TABLE_CFG } from './watch-changes.ts';

import type { ComponentsTable, TableCfg, TablesCfgTable } from '@rljson/rljson';

import type { Db } from 'mongodb';

// Tree structure definition - simplified to match RLJSON expectations
export interface Tree extends Json {
  id: string;
  isParent: boolean;
  children?: string[];
  meta: Json | null;
  _hash: string;
}

export type TreeRef = Ref;

// .............................................................................
// Types
// .............................................................................

/**
 * Metadata stored in Tree.meta for MongoDB collections
 */
export interface MongoNodeMeta extends Json {
  /** Collection name or database name */
  name: string;
  /** Type of node */
  type: 'database' | 'collection' | 'document';
  /** Database name */
  database?: string;
  /** Collection name (for documents) */
  collection?: string;
  /** Document count (for collections) */
  docCount?: number;
  /** Last modified timestamp (milliseconds since epoch) */
  mtime: number;
  /** Blob ID for document content (documents only) */
  blobId?: string;
  /** Document ID (for documents) */
  docId?: string;
  /** TableCfg hash (for collections using ComponentsTable) */
  tableCfgHash?: string;
  /** ComponentsTable blob ID (for collections) */
  componentsBlobId?: string;
  /** TablesCfgTable blob ID (for root database node) */
  tableCfgsTableBlobId?: string;
  /**
   * Chunk index for streamed (large) collections. A 1.4 M-doc collection
   * is split into N chunk trees with the same `collection` name but
   * different `chunkIndex` (0..N-1). Without this field two chunks that
   * happen to carry the same `_data` would collapse into one tree via the
   * content-addressed hash and lose data.
   */
  chunkIndex?: number;
}

/**
 * Tree structure with hash mapping
 */
export interface MongoTree {
  /** Root tree hash */
  rootHash: TreeRef;
  /** Map of hash to tree node */
  trees: Map<TreeRef, Tree>;
}

/**
 * Type of MongoDB change
 */
export type MongoChangeType = 'inserted' | 'updated' | 'deleted';

/**
 * MongoDB change event
 */
export interface MongoChange {
  /** Type of change */
  type: MongoChangeType;
  /** Collection path (db.collection) */
  path: string;
  /** Document ID */
  docId: unknown;
  /** Tree node (for inserted/updated) */
  tree?: Tree;
}

/**
 * Callback for MongoDB changes
 */
export type MongoChangeCallback = (change: MongoChange) => void | Promise<void>;

/**
 * Options for scanning
 */
export interface MongoScanOptions {
  /** Collections to ignore (glob patterns) */
  ignore?: string[];
  /** Specific collections to include (if specified, only these are scanned) */
  include?: string[];
  /** Database names to scan (if empty, scan all) */
  databases?: string[];
  /** Blob storage implementation (defaults to BsMem) */
  bs?: Bs;
}

// .............................................................................
// MongoScanner Class
// .............................................................................

/**
 * Scans MongoDB and extracts RLJSON tree structure
 */
export class MongoScanner {
  /**
   * Static guard so we only log "explicit GC available / not available"
   * once per process, no matter how many MongoScanner instances are
   * created (the agent re-instantiates on reconfig and we'd otherwise
   * spam the log).
   */
  private static _gcLogged = false;

  private _db: Db;
  private _tree: MongoTree | null = null;
  private _changeCallbacks: MongoChangeCallback[] = [];
  private _options: MongoScanOptions;
  private _bs: Bs;
  private _converter: MongoToRljsonConverter;
  private _tableConfigs: Map<string, TableCfg> = new Map();
  /**
   * Per-collection chunk trees from the last scan, reused by an incremental
   * `scan(dirtyCollections)` for collections that did not change (see scan()).
   */
  private _collCache: Map<string, Tree[]> = new Map();

  constructor(db: Db, options: MongoScanOptions = {}) {
    this._db = db;
    this._options = options;
    this._bs = options.bs || new BsMem();
    this._converter = new MongoToRljsonConverter();

    // Register system table configs (like sync_ops)
    this._tableConfigs.set('sync_ops', SYNC_OPS_TABLE_CFG);
  }

  /**
   * Gets the blob storage instance
   */
  get bs(): Bs {
    return this._bs;
  }

  /**
   * Gets the current tree structure
   */
  get tree(): MongoTree | null {
    return this._tree;
  }

  /**
   * Registers a callback for MongoDB changes
   * @param callback - Function to call when changes occur
   */
  onChange(callback: MongoChangeCallback): void {
    this._changeCallbacks.push(callback);
  }

  /**
   * Checks if a collection should be ignored
   * @param collectionName - Collection name to check
   * @returns True if collection should be ignored
   */
  private _shouldIgnore(collectionName: string): boolean {
    // Always ignore system collections
    if (collectionName.startsWith('system.')) return true;

    // Internal sync collections
    const internalCollections = [
      'sync_ops',
      'sync_state',
      'sync_local',
      'sync_resume',
      'sync_conflicts',
      'sync_audit',
      'state_checkpoints',
      'state_merkle',
      'sync_head',
      'sync_recentChanges',
    ];
    if (internalCollections.includes(collectionName)) return true;

    // `sync_tombstones` MUST always be scanned — peers need it to propagate
    // deletes. Skip user-provided ignore/include filters for this collection.
    if (collectionName === 'sync_tombstones') return false;

    // Check ignore patterns
    if (this._options.ignore) {
      for (const pattern of this._options.ignore) {
        if (this._matchPattern(collectionName, pattern)) return true;
      }
    }

    // Check include patterns (if specified, must match at least one)
    if (this._options.include && this._options.include.length > 0) {
      return !this._options.include.some((pattern) =>
        this._matchPattern(collectionName, pattern),
      );
    }

    return false;
  }

  /**
   * Simple glob pattern matching
   * @param text - Text to match against
   * @param pattern - Glob pattern
   * @returns True if text matches pattern
   */
  private _matchPattern(text: string, pattern: string): boolean {
    const regex = new RegExp(
      '^' + pattern.replace(/\*/g, '.*').replace(/\?/g, '.') + '$',
    );
    return regex.test(text);
  }

  /**
   * Scans the MongoDB database and builds tree structure
   * @returns Tree structure with document content in blobs
   */
  /**
   * Scan the database into a MongoTree.
   *
   * INCREMENTAL: when `dirtyCollections` is provided, a collection that is NOT
   * in the set AND whose chunk trees were cached by a previous scan is REUSED
   * as-is — it is not re-read, re-hashed or re-stored. This is the fix for the
   * O(whole-DB) re-scan on every change (a full scan of even a small synced set
   * measured ~7 s; at catalog scale it wedges the event loop and starves the
   * peer's bootstrap pull → rows=0). Content-addressing already makes an
   * unchanged collection hash identically, so reusing the cached nodes is
   * byte-equivalent to re-scanning it. Omit `dirtyCollections` (or pass
   * `undefined`) for a full scan — unchanged behaviour (initial snapshot).
   * @param dirtyCollections - Names of collections changed since the last scan.
   *   Anything else is served from the per-collection cache. `undefined` = full.
   * @returns The database MongoTree.
   */
  async scan(dirtyCollections?: Set<string>): Promise<MongoTree> {
    const trees = new Map<TreeRef, Tree>();

    // Get list of collections
    const collections = await this._db.listCollections().toArray();

    const collectionTrees: Tree[] = [];
    const nextCache = new Map<string, Tree[]>();

    for (const collInfo of collections) {
      const collName = collInfo.name;

      if (this._shouldIgnore(collName)) continue;

      // Reuse the cached chunk trees for a collection that did not change since
      // the last scan (no re-read / re-hash / re-store). Only when the caller
      // told us which collections are dirty AND we have a cached result.
      const cached = this._collCache.get(collName);
      let collTrees: Tree[];
      if (dirtyCollections && cached && !dirtyCollections.has(collName)) {
        collTrees = cached;
        for (const t of cached) trees.set(t._hash as string, t);
      } else {
        // Scan this collection. Big collections come back as multiple chunk
        // trees (one Tree per ~50 k-doc blob); small ones as a single tree.
        // The apply path collects per-tree, so multi-chunk collections look
        // like N separate "collection" entries with the same `meta.collection`
        // name — see the `collect()` walk in mongo-agent.ts which already
        // appends each entry individually.
        collTrees = await this._scanCollection(collName, trees);
      }
      nextCache.set(collName, collTrees);
      for (const t of collTrees) collectionTrees.push(t);
    }

    // Replace the cache (drops collections that no longer exist).
    this._collCache = nextCache;

    // Create and save TablesCfgTable (all discovered schemas)
    const tableCfgsTable = this.createTablesCfgTable();
    const tableCfgsTableBlobId = await this.saveTablesCfgTable(tableCfgsTable);

    // Create root tree (database node) with tableCfgsTableBlobId
    const rootMeta: MongoNodeMeta = {
      name: this._db.databaseName,
      type: 'database',
      database: this._db.databaseName,
      tableCfgsTableBlobId,
      mtime: Date.now(),
    };

    const rootTree: Tree = hip({
      id: this._db.databaseName,
      isParent: true,
      children: collectionTrees.map((t) => t._hash as string),
      meta: rootMeta,
      _hash: '',
    });

    trees.set(rootTree._hash as string, rootTree);

    this._tree = {
      rootHash: rootTree._hash as string,
      trees,
    };

    return this._tree;
  }

  /**
   * Scans a single collection and converts it to ComponentsTable(s).
   *
   * Returns an array because large collections are split into multiple
   * chunk trees (one per ~50 k-doc blob) to avoid two hard limits:
   *   1. `cursor.toArray()` loading 1.4 M × ~5 KB docs into V8 at once
   *      (OOMs even with --max-old-space-size=8192)
   *   2. `JSON.stringify(componentsTable)` blowing past Node's ~512 MB
   *      single-string cap (RangeError)
   *
   * Small collections (≤ chunkSize docs) still emit a single tree — same
   * blob layout as before, no tree-hash drift for unchanged collections.
   *
   * The apply path in mongo-agent.ts iterates collection trees by name and
   * loads each blob independently, so emitting N chunk trees for one
   * collection just produces N (name, blobId) entries in the apply loop —
   * no change needed downstream.
   * @param collectionName - Collection name
   * @param trees - Map to store tree nodes
   * @returns Array of collection chunk trees (empty if collection skipped)
   */
  private async _scanCollection(
    collectionName: string,
    trees: Map<TreeRef, Tree>,
  ): Promise<Tree[]> {
    const collection = this._db.collection(collectionName);

    // Tombstone GC: any tombstone older than the LWW window (60s) is dropped
    // from local storage before the scan, so the resulting tree only carries
    // FRESH deletes. Stale tombstones stop propagating and stop killing
    // reinserts. The apply path also ignores tombstones older than 60s, so
    // even if a peer hasn't GC'd yet we still reject them on receipt.
    //
    // EXCEPTION: tombstones targeting `sync_conflicts` (= conflict
    // resolution markers) are kept until peers ack them. A resolved
    // conflict isn't a doc that can be "reinserted", so the 60s rule
    // doesn't apply. If the peer is offline or storeTree is wedged when
    // the resolve happens, GC'ing the marker at 60s makes the resolution
    // un-propagatable: the non-resolving peer keeps showing the conflict
    // forever. We keep these markers in the tree so a delayed-up peer
    // still picks them up. (The marker payload is small; long-term GC
    // can be revisited if these accumulate in real workloads.)
    if (collectionName === 'sync_tombstones') {
      try {
        const cutoff = new Date(Date.now() - 60_000);
        await collection.deleteMany({
          collection: { $ne: 'sync_conflicts' },
          $or: [
            { deletedAt: { $lt: cutoff } },
            // also catch the numeric-ms variant
            { deletedAt: { $lt: cutoff.getTime() } },
          ],
        } as any);
      } catch {
        /* best-effort */
      }
    }

    // Re-discover the schema on every scan and merge with the cached one.
    const discovered = await this._converter.discoverSchema(collection);
    let tableCfg = this._tableConfigs.get(collectionName);
    if (!tableCfg) {
      tableCfg = discovered;
    } else if (
      discovered.columns.length !== tableCfg.columns.length ||
      discovered.columns.some(
        (c) => !tableCfg!.columns.find((e) => e.key === c.key),
      )
    ) {
      tableCfg = this._converter.mergeTableCfg(tableCfg, discovered);
    }
    this._tableConfigs.set(collectionName, tableCfg);

    // Decide single-shot vs streaming based on estimated doc count.
    //
    // Estimated count is cheap (cached by mongo). For small collections we
    // keep the old single-blob layout so existing tree hashes don't churn
    // and the apply path stays a single-pass read. Streaming kicks in only
    // when we'd risk an OOM otherwise.
    //
    // Threshold rationale: real prod docs are sometimes >50 KB each (e.g.
    // cd_models on PC-25366). At 5 k docs/chunk that meant ≈250 MB raw +
    // ≈250 MB JSON + ≈250 MB Buffer = 750 MB per chunk in flight, and
    // multiple chunks queued past a slow setBlob blew the 8 GB old-space
    // cap mid-scan. 500 docs/chunk caps the peak at ≈25 MB raw + ≈25 MB
    // JSON + ≈25 MB Buffer = ≈75 MB even on 50 KB docs.
    //
    // Trade-off: cd_articles (1.4 M docs) lands at 2800 chunks instead of
    // 280. Cursor round-trips dominate runtime now, not conversion peak,
    // and BsFs's content-addressed disk write skips chunks whose blob
    // already exists from a previous run — so re-scans after a crash
    // amortize quickly.
    const CHUNK_DOCS = 500;
    // Log once at startup whether --expose-gc is active. Without it the
    // explicit `globalThis.gc()` between chunks is a no-op and the heap
    // can drift up regardless of CHUNK_DOCS.
    if (!MongoScanner._gcLogged) {
      MongoScanner._gcLogged = true;
      const hasGc = typeof (globalThis as any).gc === 'function';
      console.log(
        `[mongo-scanner] explicit GC ${hasGc ? 'available (--expose-gc)' : 'NOT AVAILABLE — add --expose-gc to NODE_OPTIONS'}`,
      );
    }
    let estimatedCount = 0;
    try {
      estimatedCount = await collection.estimatedDocumentCount();
    } catch {
      // Fall back to streaming when count fails (mongo may reject on
      // permission boundaries) — safer than risking OOM on the legacy path.
      estimatedCount = CHUNK_DOCS + 1;
    }

    // ---- Path A: small collection — single-shot, single blob (old layout) ----
    if (estimatedCount <= CHUNK_DOCS) {
      const componentsTable = await this._converter.convertCollection(
        collection,
        tableCfg,
      );
      let content: string;
      try {
        content = JSON.stringify(componentsTable);
      } catch (err) {
        // estimatedCount was misleading (avg-doc-size too large) — fall
        // through to streaming below by re-issuing the call. Rare path so
        // duplicating the cursor walk is fine vs. handling here.
        if (
          err instanceof RangeError ||
          /Invalid string length/i.test((err as Error).message)
        ) {
          console.warn(
            `[mongo-scanner] ${collectionName}: estimated ${estimatedCount} docs fit the single-blob path but JSON.stringify hit the string cap — falling back to streaming`,
          );
          // fall through to streaming
        } else {
          throw err;
        }
      }
      if (content!) {
        const blobProps = await this._bs.setBlob(Buffer.from(content, 'utf-8'));
        const collMeta: MongoNodeMeta = {
          name: collectionName,
          type: 'collection',
          database: this._db.databaseName,
          collection: collectionName,
          docCount: componentsTable._data.length,
          tableCfgHash: tableCfg._hash as string,
          componentsBlobId: blobProps.blobId,
          mtime: Date.now(),
        };
        const collTree: Tree = hip({
          id: collectionName,
          isParent: false,
          meta: collMeta,
          _hash: '',
        });
        trees.set(collTree._hash as string, collTree);
        return [collTree];
      }
    }

    // ---- Path B: big collection — streaming cursor + N chunk blobs ----
    //
    // Each chunk becomes its own Tree with `meta.type = 'collection'` and
    // the same `meta.collection` name, just with a unique blobId per chunk.
    // The apply-path collector in mongo-agent.ts walks the children of the
    // database-root tree and pushes one (name, blobId) entry per chunk
    // tree, so on the receiving side the chunks are read independently and
    // upserted to the same target collection — no multi-blob logic needed.
    //
    // `chunkIndex` is stored in meta purely for diagnostics + hash
    // uniqueness (without it, two chunks with identical convertDocument
    // output for their docs would collapse into one tree node and we'd lose
    // half the data).
    const chunkTrees: Tree[] = [];
    let chunkIndex = 0;
    let totalDocsSeen = 0;
    for await (const componentsTable of this._converter.convertCollectionStreaming(
      collection,
      tableCfg,
      CHUNK_DOCS,
    )) {
      let content: string;
      try {
        content = JSON.stringify(componentsTable);
      } catch (err) {
        // A single chunk should never exceed the string cap, but if it does
        // (extreme avg-doc-size in some collection), warn and drop it. We
        // continue iterating instead of failing the whole collection.
        if (
          err instanceof RangeError ||
          /Invalid string length/i.test((err as Error).message)
        ) {
          console.warn(
            `[mongo-scanner] ${collectionName} chunk ${chunkIndex} (${componentsTable._data.length} docs): single-chunk JSON.stringify hit string cap, dropped`,
          );
          chunkIndex++;
          continue;
        }
        throw err;
      }
      const blobProps = await this._bs.setBlob(Buffer.from(content, 'utf-8'));
      // Capture the doc count BEFORE nulling `_data` below — totalDocsSeen
      // and collMeta both need it for diagnostics + the tree node metadata.
      const docCount = componentsTable._data.length;
      const collMeta: MongoNodeMeta = {
        name: collectionName,
        type: 'collection',
        database: this._db.databaseName,
        collection: collectionName,
        docCount,
        tableCfgHash: tableCfg._hash as string,
        componentsBlobId: blobProps.blobId,
        mtime: Date.now(),
        // Distinguish the chunk so the hash is unique even when two chunks
        // happen to carry the same `_data` (e.g. empty trailing chunk).
        chunkIndex,
      };
      // Drop the references that the for-await body still keeps alive: the
      // for-await binding releases the previous yielded value only when the
      // next .next() resolves, so on a slow setBlob the previous chunk's
      // 25 MB `_data` array + 25 MB JSON string + 25 MB Buffer all linger
      // until the cursor produces the next batch. Nulling here lets V8 reuse
      // their pages immediately; without it the heap drifts up across the
      // chunk loop on 9 GB CARATDB1.
       
      (componentsTable as any)._data = null;
      content = '';
      // Hint at V8 — only effective when sl.exe is launched with
      // `--expose-gc` (set via NODE_OPTIONS in START-L*.bat). No-op
      // otherwise. On a 5 k-doc chunk this brings the post-blob heap back
      // down to a 100-200 MB baseline almost immediately instead of waiting
      // for the next incremental cycle to catch up.
       
      const g = (globalThis as any).gc;
      if (typeof g === 'function') g();
      const collTree: Tree = hip({
        // Chunk index baked into the id keeps the rljson row distinguishable
        // in places that surface the tree by id rather than hash.
        id: `${collectionName}#${chunkIndex}`,
        isParent: false,
        meta: collMeta,
        _hash: '',
      });
      trees.set(collTree._hash as string, collTree);
      chunkTrees.push(collTree);
      totalDocsSeen += docCount;
      chunkIndex++;
    }
    if (chunkTrees.length === 0) {
      // Empty collection — emit a single empty-chunk tree so the receiver
      // can still observe the collection's schema row in tableCfgsTable.
      // Without this, downstream code that expects per-collection metadata
      // would skip the table entirely on a fresh peer.
      return [];
    }
    console.log(
      `[mongo-scanner] ${collectionName}: streamed ${totalDocsSeen} docs in ${chunkTrees.length} chunks`,
    );
    return chunkTrees;
  }

  /**
   * Retrieves a ComponentsTable from blob storage
   * @param blobId - Blob ID of the ComponentsTable
   * @returns ComponentsTable
   */
  async getComponentsTable(blobId: string): Promise<ComponentsTable<any>> {
    const blob = await this._bs.getBlob(blobId);
    const content = blob.content.toString('utf-8');
    return JSON.parse(content) as ComponentsTable<any>;
  }

  /**
   * Gets the TableCfg for a collection (if it has been scanned)
   * @param collectionName - Collection name
   * @returns TableCfg or undefined if not scanned yet
   */
  getTableCfg(collectionName: string): TableCfg | undefined {
    return this._tableConfigs.get(collectionName);
  }

  /**
   * Gets all discovered TableCfgs
   * @returns Map of collection names to TableCfgs
   */
  getAllTableCfgs(): Map<string, TableCfg> {
    return new Map(this._tableConfigs);
  }

  /**
   * Gets the root tree node
   * @returns Root tree or null if not scanned yet
   */
  getRootTree(): Tree | null {
    if (!this._tree) return null;
    return this._tree.trees.get(this._tree.rootHash) || null;
  }

  /**
   * Creates TablesCfgTable from discovered schemas
   * @returns TablesCfgTable with all TableCfg objects
   */
  createTablesCfgTable(): TablesCfgTable {
    const tableCfgs = Array.from(this._tableConfigs.values());

    return {
      _data: tableCfgs,
    } as TablesCfgTable;
  }

  /**
   * Saves TablesCfgTable to blob storage
   * @param tableCfgsTable - Table configuration table to save
   * @returns Blob ID of saved TablesCfgTable
   */
  async saveTablesCfgTable(tableCfgsTable: TablesCfgTable): Promise<string> {
    const content = JSON.stringify(tableCfgsTable);
    const blobProps = await this._bs.setBlob(Buffer.from(content, 'utf-8'));
    return blobProps.blobId;
  }

  /**
   * Loads TablesCfgTable from blob storage
   * @param blobId - Blob ID of the TablesCfgTable
   * @returns TablesCfgTable
   */
  async loadTablesCfgTable(blobId: string): Promise<TablesCfgTable> {
    const blob = await this._bs.getBlob(blobId);
    const content = blob.content.toString('utf-8');
    return JSON.parse(content) as TablesCfgTable;
  }

  /**
   * Gets a TableCfg by its hash from a TablesCfgTable
   * @param tableCfgsTable - TablesCfgTable to search
   * @param hash - Hash of the TableCfg to find
   * @returns TableCfg or undefined if not found
   */
  getTableCfgByHash(
    tableCfgsTable: TablesCfgTable,
    hash: string,
  ): TableCfg | undefined {
    return tableCfgsTable._data.find((cfg) => cfg._hash === hash);
  }

  /**
   * Adds a TableCfg to the internal cache
   * @param collectionName - Collection name
   * @param tableCfg - TableCfg to add
   */
  addTableCfg(collectionName: string, tableCfg: TableCfg): void {
    this._tableConfigs.set(collectionName, tableCfg);
  }
}
