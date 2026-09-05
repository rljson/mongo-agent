// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import type { Db } from '@rljson/db';

import type { MongoTree } from './mongo-scanner.ts';

/**
 * Options for storing MongoDB trees in database
 */
export interface StoreMongoTreeOptions {
  /**
   * Whether to skip notifications after storing (defaults to false).
   * When false, observers (e.g. Connector) are notified automatically
   * via the standard db.insertTrees() pipeline.
   */
  skipNotification?: boolean;
}

/**
 * Adapter for storing MongoDB trees in a database.
 *
 * Uses `db.insertTrees()` to go through the standard insert pipeline:
 * - TreeController writes each node
 * - InsertHistoryRow is created automatically
 * - `notify.notify()` fires so Connector observers broadcast the ref
 */
export class MongoDbAdapter {
  constructor(
    private db: Db,
    private treeKey: string,
  ) {}

  /**
   * Store a MongoDB tree in the database
   * @param mongoTree - The MongoDB tree to store
   * @param options - Storage options
   * @returns The root tree reference
   */
  async storeMongoTree(
    mongoTree: MongoTree,
    options: StoreMongoTreeOptions = {},
  ): Promise<string> {
    // Validate input
    if (!mongoTree) {
      throw new Error('mongoTree cannot be null or undefined');
    }

    if (!mongoTree.rootHash || typeof mongoTree.rootHash !== 'string') {
      throw new Error(
        `Invalid rootHash: expected non-empty string, got ${typeof mongoTree.rootHash}`,
      );
    }

    if (!mongoTree.trees || !(mongoTree.trees instanceof Map)) {
      throw new Error(
        `Invalid trees: expected Map, got ${typeof mongoTree.trees}`,
      );
    }

    if (mongoTree.trees.size === 0) {
      throw new Error(
        'Cannot store empty tree: trees Map must contain at least one node',
      );
    }

    // Verify root node exists in the tree
    if (!mongoTree.trees.has(mongoTree.rootHash)) {
      throw new Error(
        `Root hash "${mongoTree.rootHash}" not found in trees Map. ` +
          `The tree structure may be corrupted.`,
      );
    }

    // Convert all tree nodes from Map to Array
    // CRITICAL: Root tree MUST be the last element (per @rljson/server pattern)
    const rootTree = mongoTree.trees.get(mongoTree.rootHash)!;
    const trees: Array<any> = Array.from(mongoTree.trees.values()).filter(
      (tree) => tree._hash !== mongoTree.rootHash,
    );
    trees.push(rootTree); // Add root as last element

    // Use db.insertTrees() — goes through the full insert pipeline:
    // 1. TreeController writes each node
    // 2. InsertHistoryRow created automatically
    // 3. notify.notify() fires → Connector observers broadcast ref
    // Cast to any to handle version mismatch between local Tree and @rljson/rljson Tree
    const results = await this.db.insertTrees(this.treeKey, trees as any, {
      skipNotification: options.skipNotification,
    });

    return results[0][
      `${this.treeKey}Ref` as keyof (typeof results)[0]
    ] as string;
  }

  /**
   * Get the tree table key
   */
  getTreeKey(): string {
    return this.treeKey;
  }

  /**
   * Get the database instance
   */
  getDb(): Db {
    return this.db;
  }
}
