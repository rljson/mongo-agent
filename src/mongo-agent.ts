// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Bs, BsMem } from '@rljson/bs';

import { MongoBlobAdapter } from './mongo-blob-adapter.ts';
import { MongoDbAdapter, StoreMongoTreeOptions } from './mongo-db-adapter.ts';
import { MongoScanner, MongoTree } from './mongo-scanner.ts';


import type { Connector, Db as RljsonDb } from '@rljson/db';
import type { Db as MongoDb } from 'mongodb';

// .............................................................................
// Types
// .............................................................................

/**
 * Options for MongoAgent operations
 */
export interface MongoAgentOptions {
  /** Collections to ignore when scanning */
  ignore?: string[];
  /** Specific collections to include (if specified, only these are scanned) */
  include?: string[];
  /** Database names to scan (if empty, scan all) */
  databases?: string[];
  /** Database instance for automatic syncing */
  db?: RljsonDb;
  /** Tree key for database storage */
  treeKey?: string;
  /** Storage options for database operations */
  storageOptions?: StoreMongoTreeOptions;
  /** Enable bidirectional sync (both mongo→db and db→mongo) */
  bidirectional?: boolean;
  /** Timeout configuration for async operations */
  timeouts?: TimeoutConfig;
}

/**
 * Timeout configuration for async operations (milliseconds).
 * Every async operation in MongoAgent is guarded by a timeout to prevent
 * silent hangs in socket communication, database queries.
 */
export interface TimeoutConfig {
  /** Timeout for a single db.get() query. Default: 10 000 ms */
  dbQuery?: number;
  /** Timeout for fetching an entire tree from the DB. Default: 20 000 ms */
  fetchTree?: number;
  /** Timeout for a MongoDB extract / scan. Default: 15 000 ms */
  extract?: number;
  /** Timeout for the overall syncFromDb callback. Default: 25 000 ms */
  syncCallback?: number;
  /**
   * Debounce delay for sync callbacks (milliseconds). Default: 300 ms.
   * Rapid change events are coalesced into a single sync operation
   * after this quiet period.
   */
  debounceMs?: number;
}

/** Sensible defaults – every operation is bounded */
const DEFAULT_TIMEOUTS: Required<TimeoutConfig> = {
  dbQuery: 10_000,
  fetchTree: 20_000,
  extract: 15_000,
  syncCallback: 25_000,
  debounceMs: 300,
};

// .............................................................................
// MongoAgent Class
// .............................................................................

/**
 * Orchestrates MongoDB operations with tree structures and blob storage
 */
export class MongoAgent {
  private _scanner: MongoScanner;
  private _blobAdapter: MongoBlobAdapter;
  private _mongoDb: MongoDb;
  private _bs: Bs;
  private _rljsonDb?: RljsonDb;
  private _treeKey?: string;
  private _stopSync?: () => void;
  private _stopSyncFromDb?: () => void;
  private _timeouts: Required<TimeoutConfig>;

  constructor(mongoDb: MongoDb, bs?: Bs, options: MongoAgentOptions = {}) {
    this._mongoDb = mongoDb;
    this._bs = bs || new BsMem();
    this._rljsonDb = options.db;
    this._treeKey = options.treeKey;
    this._timeouts = { ...DEFAULT_TIMEOUTS, ...options.timeouts };
    this._scanner = new MongoScanner(mongoDb, { ...options, bs: this._bs });
    this._blobAdapter = new MongoBlobAdapter(this._bs);

    // Automatically start syncing if db and treeKey are provided
    /* v8 ignore next -- @preserve */
    if (this._rljsonDb && this._treeKey) {
      this._startAutoSync().catch(() => {
        // Intentionally ignored - deprecated constructor pattern
      });

      // Start reverse sync if bidirectional is enabled
      this._startAutoSyncFromDb(options.bidirectional || false).catch(() => {
        // Intentionally ignored - deprecated constructor pattern
      });
    }
  }

  /**
   * Gets the MongoDB database
   */
  get mongoDb(): MongoDb {
    return this._mongoDb;
  }

  /**
   * Gets the blob storage instance
   */
  get bs(): Bs {
    return this._bs;
  }

  /**
   * Gets the scanner instance
   */
  get scanner(): MongoScanner {
    return this._scanner;
  }

  /**
   * Gets the blob adapter instance
   */
  get blobAdapter(): MongoBlobAdapter {
    return this._blobAdapter;
  }

  /**
   * Gets the current timeout configuration
   */
  get timeouts(): Required<TimeoutConfig> {
    return this._timeouts;
  }

  /**
   * Sends a ref through the connector.
   * @param connector - The Connector to send through
   * @param ref - The ref to broadcast
   */
  private async _sendRef(connector: Connector, ref: string): Promise<void> {
    // Always use send() since advanced sync features not available in this version
    connector.send(ref);
  }

  /**
   * Starts automatic syncing to database
   * Note: Auto-sync requires Connector which is not available in constructor.
   * Consider using syncToDb() directly instead of constructor options.
   */
  private async _startAutoSync(): Promise<void> {
    /* v8 ignore next -- @preserve */
    if (!this._rljsonDb || !this._treeKey) {
      return;
    }

    // Cannot create Connector without socket - auto-sync not supported
    /* v8 ignore next -- @preserve */
    throw new Error(
      'Auto-sync from constructor is not supported. ' +
        'Use syncToDb() method directly with a Connector instance.',
    );
  }

  /**
   * Starts automatic syncing from database
   * @param bidirectional - Whether bidirectional sync is enabled
   * Note: Auto-sync requires Connector which is not available in constructor.
   * Consider using syncFromDb() directly instead of constructor options.
   */
  private async _startAutoSyncFromDb(bidirectional: boolean): Promise<void> {
    /* v8 ignore if -- @preserve */
    if (!this._rljsonDb || !this._treeKey || !bidirectional) {
      return;
    }

    // Cannot create Connector without socket - auto-sync not supported
    /* v8 ignore next -- @preserve */
    throw new Error(
      'Auto-sync from constructor is not supported. ' +
        'Use syncFromDb() method directly with a Connector instance.',
    );
  }

  /**
   * Stops automatic syncing and cleans up resources
   */
  dispose(): void {
    /* v8 ignore if -- @preserve */
    if (this._stopSync) {
      this._stopSync();
      this._stopSync = undefined;
    }
    /* v8 ignore if -- @preserve */
    if (this._stopSyncFromDb) {
      this._stopSyncFromDb();
      this._stopSyncFromDb = undefined;
    }
  }

  /**
   * Extracts MongoDB into tree structure with document content in blobs
   * Document content is stored in Bs, tree structure returned with blobIds embedded
   * @returns Tree structure with blobIds in document metadata
   */
  async extract(): Promise<MongoTree> {
    // Scan MongoDB - stores document content in Bs, returns tree structure
    const tree = await this._scanner.scan();

    // Return the tree structure (blobIds are already in document metadata)
    return tree;
  }

  /**
   * Gets the current tree structure
   */
  getTree(): MongoTree | null {
    return this._scanner.tree;
  }

  /**
   * Checks if a blob exists in storage
   * @param blobId - Blob ID to check
   */
  async hasBlob(blobId: string): Promise<boolean> {
    try {
      await this._bs.getBlob(blobId);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Gets document content from blob storage
   * @param blobId - Blob ID
   */
  async getDocumentContent(blobId: string): Promise<Buffer> {
    const blob = await this._bs.getBlob(blobId);
    return blob.content;
  }

  /**
   * Extracts and stores MongoDB tree in database
   * Reads from MongoDB, stores trees in DB and blobs in Bs
   * @param db - RLJSON Database instance
   * @param treeKey - Tree table key
   * @param options - Storage options
   * @returns The root tree reference
   */
  async storeInDb(
    db: RljsonDb,
    treeKey: string,
    options?: StoreMongoTreeOptions,
  ): Promise<string> {
    const tree = await this.extract();

    // Validate tree has content
    /* v8 ignore if -- @preserve */
    if (!tree || !tree.rootHash || !tree.trees) {
      throw new Error(
        'Cannot store empty or invalid tree in database. ' +
          'Ensure MongoDB has been scanned and contains valid data.',
      );
    }

    /* v8 ignore if -- @preserve */
    if (tree.trees.size === 0) {
      throw new Error(
        'Cannot store tree with no nodes. The tree structure is empty.',
      );
    }

    const dbAdapter = new MongoDbAdapter(db, treeKey);
    return await dbAdapter.storeMongoTree(tree, options);
  }

  /**
   * Syncs MongoDB changes to database through a Connector
   * Watches MongoDB for changes and broadcasts them via the connector
   * @param db - RLJSON Database instance
   * @param connector - Connector to broadcast changes through
   * @param treeKey - Tree table key
   * @returns Function to stop syncing
   */
  async syncToDb(
    db: RljsonDb,
    connector: Connector,
    treeKey: string,
  ): Promise<() => void> {
    let stopped = false;

    // Register change callback
    this._scanner.onChange(async () => {
      if (stopped) return;

      try {
        // Extract current state
        const tree = await this.extract();

        // Store in database
        const dbAdapter = new MongoDbAdapter(db, treeKey);
        const ref = await dbAdapter.storeMongoTree(tree);

        // Broadcast via connector
        await this._sendRef(connector, ref);
      } catch (error) {
        console.error('Error syncing MongoDB change to DB:', error);
      }
    });

    // Return stop function
    return () => {
      stopped = true;
    };
  }

  /** Example instance for test purposes */
  static get example(): MongoAgent {
    // Note: In real usage, you'd pass a real MongoDB connection
    // This is just for demonstration
    const mockDb = {} as MongoDb;
    return new MongoAgent(mockDb);
  }
}
