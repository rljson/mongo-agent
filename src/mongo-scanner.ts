// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Bs, BsMem } from '@rljson/bs';
import { hip } from '@rljson/hash';
import { Json } from '@rljson/json';
import { Ref } from '@rljson/rljson';

import type { Db, Document } from 'mongodb';

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

  constructor(db: Db, options: MongoScanOptions = {}) {
    this._db = db;
    this._options = options;
    this._bs = options.bs || new BsMem();
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

    // Create root tree (database node)
    const rootMeta: MongoNodeMeta = {
      name: this._db.databaseName,
      type: 'database',
      database: this._db.databaseName,
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
   * Scans a single collection
   * @param collectionName - Collection name
   * @param trees - Map to store tree nodes
   * @returns Collection tree node
   */
  private async _scanCollection(
    collectionName: string,
    trees: Map<TreeRef, Tree>,
  ): Promise<Tree | null> {
    const collection = this._db.collection(collectionName);
    const documents = await collection.find({}).toArray();

    const documentTrees: Tree[] = [];

    for (const doc of documents) {
      const docTree = await this._scanDocument(collectionName, doc, trees);
      if (docTree) {
        documentTrees.push(docTree);
      }
    }

    // Create collection tree node
    const collMeta: MongoNodeMeta = {
      name: collectionName,
      type: 'collection',
      database: this._db.databaseName,
      collection: collectionName,
      docCount: documents.length,
      mtime: Date.now(),
    };

    const collTree: Tree = hip({
      id: collectionName,
      isParent: true,
      children: documentTrees.map((t) => t._hash as string),
      meta: collMeta,
      _hash: '',
    });

    trees.set(collTree._hash as string, collTree);

    return collTree;
  }

  /**
   * Scans a single document
   * @param collectionName - Collection name
   * @param doc - MongoDB document
   * @param trees - Map to store tree nodes
   * @returns Document tree node
   */
  private async _scanDocument(
    collectionName: string,
    doc: Document,
    trees: Map<TreeRef, Tree>,
  ): Promise<Tree | null> {
    // Store document content in blob storage
    const content = JSON.stringify(doc);
    const blobProps = await this._bs.setBlob(Buffer.from(content, 'utf-8'));
    const blobId = blobProps.blobId;

    // Create document tree node
    const docMeta: MongoNodeMeta = {
      name: String(doc._id),
      type: 'document',
      database: this._db.databaseName,
      collection: collectionName,
      docId: String(doc._id),
      blobId,
      mtime: Date.now(),
    };

    const docTree: Tree = hip({
      id: String(doc._id),
      isParent: false,
      meta: docMeta,
      _hash: '',
    });

    trees.set(docTree._hash as string, docTree);

    return docTree;
  }

  /**
   * Gets the root tree node
   * @returns Root tree or null if not scanned yet
   */
  getRootTree(): Tree | null {
    if (!this._tree) return null;
    return this._tree.trees.get(this._tree.rootHash) || null;
  }
}
