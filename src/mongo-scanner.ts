// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Bs, BsMem } from '@rljson/bs';
import { hip } from '@rljson/hash';
import { Json } from '@rljson/json';
import { Ref } from '@rljson/rljson';
import type { ComponentsTable, TableCfg, TablesCfgTable } from '@rljson/rljson';

import type { Db } from 'mongodb';

import { MongoToRljsonConverter } from './mongo-to-rljson-converter.ts';
import { SYNC_OPS_TABLE_CFG } from './watch-changes.ts';

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
  private _db: Db;
  private _tree: MongoTree | null = null;
  private _changeCallbacks: MongoChangeCallback[] = [];
  private _options: MongoScanOptions;
  private _bs: Bs;
  private _converter: MongoToRljsonConverter;
  private _tableConfigs: Map<string, TableCfg> = new Map();

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
    ];
    if (internalCollections.includes(collectionName)) return true;

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
  async scan(): Promise<MongoTree> {
    const trees = new Map<TreeRef, Tree>();

    // Get list of collections
    const collections = await this._db.listCollections().toArray();

    const collectionTrees: Tree[] = [];

    for (const collInfo of collections) {
      const collName = collInfo.name;

      if (this._shouldIgnore(collName)) continue;

      // Scan this collection
      const collTree = await this._scanCollection(collName, trees);
      if (collTree) {
        collectionTrees.push(collTree);
      }
    }

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
   * Scans a single collection and converts it to ComponentsTable
   * @param collectionName - Collection name
   * @param trees - Map to store tree nodes
   * @returns Collection tree node
   */
  private async _scanCollection(
    collectionName: string,
    trees: Map<TreeRef, Tree>,
  ): Promise<Tree | null> {
    const collection = this._db.collection(collectionName);

    // Get or discover schema (TableCfg)
    let tableCfg = this._tableConfigs.get(collectionName);
    if (!tableCfg) {
      tableCfg = await this._converter.discoverSchema(collection);
      this._tableConfigs.set(collectionName, tableCfg);
    }

    // Convert entire collection to ComponentsTable
    const componentsTable = await this._converter.convertCollection(
      collection,
      tableCfg,
    );

    // Store ComponentsTable as blob
    const content = JSON.stringify(componentsTable);
    const blobProps = await this._bs.setBlob(Buffer.from(content, 'utf-8'));
    const componentsBlobId = blobProps.blobId;

    // Create collection tree node (no per-document children!)
    const collMeta: MongoNodeMeta = {
      name: collectionName,
      type: 'collection',
      database: this._db.databaseName,
      collection: collectionName,
      docCount: componentsTable._data.length,
      tableCfgHash: tableCfg._hash as string,
      componentsBlobId,
      mtime: Date.now(),
    };

    const collTree: Tree = hip({
      id: collectionName,
      isParent: false,
      meta: collMeta,
      _hash: '',
    });

    trees.set(collTree._hash as string, collTree);

    return collTree;
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
