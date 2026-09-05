// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Route } from '@rljson/rljson';

import type { Db } from '@rljson/db';

import type { MongoTree, Tree } from './mongo-scanner.ts';

// .............................................................................

/**
 * Adapter for storing/fetching MongoTree structures via the rljson Db
 * (mirror of `FsDbAdapter` from `@rljson/fs-agent` — adapted for Mongo trees).
 *
 * Tree nodes share the shape `{ _hash, id, isParent, children, meta }` so
 * `db.insertTrees(treeKey, trees[])` works directly. The `notify` pipeline
 * triggers connector broadcasts of the root ref.
 */
export class MongoDbTreeAdapter {
  constructor(
    private readonly _db: Db,
    private readonly _treeKey: string,
  ) {}

  /* eslint-disable tsdoc/syntax -- jsdoc/require-param mandates a block tag for
     the nested `options.skipNotification` property, but tsdoc/syntax rejects the
     dotted parameter name; the two rules conflict, so tsdoc/syntax is disabled
     for this documented-but-dotted @param only. */
  /**
   * Stores a MongoTree in the rljson DB.
   * @param tree - MongoTree to store
   * @param options - Store options for the insert; controls observer
   *   notification behaviour for restore-loop bounce-back avoidance.
   * @param options.skipNotification - When true, suppresses observer (e.g.
   *   Connector) notifications so the insert does not echo back through the
   *   restore loop; defaults to undefined (notifications enabled).
   * @returns The root tree reference (treeKey + 'Ref' field of the root row)
   */
  /* eslint-enable tsdoc/syntax */
  async storeTree(
    tree: MongoTree,
    options: { skipNotification?: boolean } = {},
  ): Promise<string> {
    if (!tree) {
      throw new Error('tree cannot be null or undefined');
    }
    if (!tree.rootHash || typeof tree.rootHash !== 'string') {
      throw new Error(
        `Invalid rootHash: expected non-empty string, got ${typeof tree.rootHash}`,
      );
    }
    if (!tree.trees || !(tree.trees instanceof Map)) {
      throw new Error(`Invalid trees: expected Map, got ${typeof tree.trees}`);
    }
    if (tree.trees.size === 0) {
      throw new Error('Cannot store empty tree: at least one node required');
    }
    const rootNode = tree.trees.get(tree.rootHash);
    if (!rootNode) {
      throw new Error(`Root hash "${tree.rootHash}" not found in tree.trees`);
    }

    // Insert all non-root nodes first, root last (matches FsDbAdapter ordering).
    const trees: Tree[] = Array.from(tree.trees.values()).filter(
      (t) => t._hash !== tree.rootHash,
    );
    trees.push(rootNode);

    const results = await (this._db as any).insertTrees(this._treeKey, trees, {
      skipNotification: options.skipNotification,
    });
    return results[0][`${this._treeKey}Ref`];
  }

  /**
   * Fetches a MongoTree by its root reference, following children recursively.
   * Mirrors FsAgent._fetchTreeRecursively from `@rljson/fs-agent`.
   * @param rootRef - Hash of the root tree node to start from; its `children`
   *   are followed transitively until every reachable node is collected.
   * @returns The fully resolved MongoTree rooted at `rootRef`.
   */
  async fetchTree(rootRef: string): Promise<MongoTree> {
    if (!rootRef || rootRef.trim() === '') {
      throw new Error('rootRef cannot be empty');
    }
    const route = Route.fromFlat(this._treeKey);
    const fetched = new Map<string, Tree>();
    const toFetch = new Set<string>([rootRef]);
    const processed = new Set<string>();

    while (toFetch.size > 0) {
      const current = toFetch.values().next().value as string;
      toFetch.delete(current);
      // Defensive re-entry guard: `toFetch` is a Set and `add` below is gated
      // by `!processed.has(...)`, so a hash can never be popped twice — this
      // branch is unreachable in practice but kept for safety.
      /* v8 ignore start */
      if (processed.has(current)) continue;
      /* v8 ignore stop */
      processed.add(current);

      let result: any;
      const _dbg = process.env['SL_TREE_SYNC_DEBUG'] === '1';
      const _isRoot = current === rootRef;
      try {
        const _t0 = Date.now();
        result = await (this._db as any).get(route, { _hash: current });
        /* v8 ignore start -- diagnostic instrumentation (SL_TREE_SYNC_DEBUG) */
        if (_dbg) {
          const td = result?.rljson?.[this._treeKey];
          const n = Array.isArray(td?._data)
            ? td._data.length
            : td?._data
              ? Object.keys(td._data).length
              : 0;
          console.log(
            `[tree-sync] fetchTree ${_isRoot ? 'ROOT ' : 'node '}${current.slice(0, 8)} -> rows=${n} (${Date.now() - _t0}ms)`,
          );
        }
        /* v8 ignore stop */
      } catch (err) {
        /* v8 ignore start -- diagnostic instrumentation (SL_TREE_SYNC_DEBUG) */
        if (_dbg) {
          console.log(
            `[tree-sync] fetchTree ${_isRoot ? 'ROOT ' : 'node '}${current.slice(0, 8)} THREW: ${(err as Error).message}`,
          );
        }
        /* v8 ignore stop */
        continue;
      }

      const treeData = result?.rljson?.[this._treeKey];
      if (!treeData?._data) continue;
      const dataArr: Tree[] = Array.isArray(treeData._data)
        ? treeData._data
        : Object.values(treeData._data);

      for (const node of dataArr) {
        if (!node._hash) continue;
        fetched.set(node._hash as string, node);
        const children = (node as any).children;
        if (Array.isArray(children)) {
          for (const childHash of children) {
            if (typeof childHash === 'string' && !processed.has(childHash)) {
              toFetch.add(childHash);
            }
          }
        }
      }
    }

    if (fetched.size === 0) {
      throw new Error(`No tree nodes found for ${this._treeKey}@${rootRef}.`);
    }
    if (!fetched.has(rootRef)) {
      throw new Error(
        `Root tree node "${rootRef}" not found in fetched data.`,
      );
    }
    return { rootHash: rootRef, trees: fetched };
  }
}
