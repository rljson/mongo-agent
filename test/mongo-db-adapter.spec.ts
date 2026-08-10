// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.

import { describe, expect, it, vi } from 'vitest';

import { MongoDbAdapter } from '../src/mongo-db-adapter.ts';
import type { MongoTree } from '../src/mongo-scanner.ts';

/**
 * Coverage tests for MongoDbAdapter.
 *
 * No live MongoDB: `db.insertTrees` is a lightweight stub returning canned
 * results, so every branch of storeMongoTree (validation + the happy path)
 * is exercised deterministically.
 */
describe('MongoDbAdapter', () => {
  const treeKey = 'caratdb';

  // A fake Db whose insertTrees records its args and returns a canned ref.
  const makeDb = (ref = 'ROOT_REF') => {
    const insertTrees = vi.fn(async () => [{ [`${treeKey}Ref`]: ref }]);
    return { db: { insertTrees } as any, insertTrees };
  };

  const makeAdapter = (ref?: string) => {
    const { db, insertTrees } = makeDb(ref);
    return { adapter: new MongoDbAdapter(db, treeKey), db, insertTrees };
  };

  // Build a minimal valid MongoTree with a root node plus N children.
  const makeTree = (rootHash = 'rootH', childHashes: string[] = []): MongoTree => {
    const trees = new Map<string, any>();
    for (const c of childHashes) trees.set(c, { _hash: c });
    trees.set(rootHash, { _hash: rootHash });
    return { rootHash, trees } as unknown as MongoTree;
  };

  describe('storeMongoTree validation', () => {
    it('throws when mongoTree is null/undefined', async () => {
      const { adapter } = makeAdapter();
      await expect(adapter.storeMongoTree(null as any)).rejects.toThrow(
        'mongoTree cannot be null or undefined',
      );
    });

    it('throws when rootHash is missing', async () => {
      const { adapter } = makeAdapter();
      const tree = makeTree();
      (tree as any).rootHash = '';
      await expect(adapter.storeMongoTree(tree)).rejects.toThrow(
        /Invalid rootHash/,
      );
    });

    it('throws when rootHash is not a string', async () => {
      const { adapter } = makeAdapter();
      const tree = makeTree();
      (tree as any).rootHash = 123;
      await expect(adapter.storeMongoTree(tree)).rejects.toThrow(
        /Invalid rootHash: expected non-empty string, got number/,
      );
    });

    it('throws when trees is not a Map', async () => {
      const { adapter } = makeAdapter();
      const tree = makeTree();
      (tree as any).trees = {};
      await expect(adapter.storeMongoTree(tree)).rejects.toThrow(
        /Invalid trees: expected Map, got object/,
      );
    });

    it('throws when trees is missing', async () => {
      const { adapter } = makeAdapter();
      const tree = makeTree();
      (tree as any).trees = undefined;
      await expect(adapter.storeMongoTree(tree)).rejects.toThrow(
        /Invalid trees/,
      );
    });

    it('throws when trees Map is empty', async () => {
      const { adapter } = makeAdapter();
      const tree = { rootHash: 'rootH', trees: new Map() } as unknown as MongoTree;
      await expect(adapter.storeMongoTree(tree)).rejects.toThrow(
        /Cannot store empty tree/,
      );
    });

    it('throws when root hash is not present in trees Map', async () => {
      const { adapter } = makeAdapter();
      const trees = new Map<string, any>();
      trees.set('other', { _hash: 'other' });
      const tree = { rootHash: 'rootH', trees } as unknown as MongoTree;
      await expect(adapter.storeMongoTree(tree)).rejects.toThrow(
        /Root hash "rootH" not found in trees Map/,
      );
    });
  });

  describe('storeMongoTree happy path', () => {
    it('inserts trees with the root as the LAST element and returns the ref', async () => {
      const { adapter, insertTrees } = makeAdapter('THE_REF');
      const tree = makeTree('rootH', ['childA', 'childB']);

      const ref = await adapter.storeMongoTree(tree);

      expect(ref).toBe('THE_REF');
      expect(insertTrees).toHaveBeenCalledTimes(1);
      const [key, treesArg, opts] = insertTrees.mock.calls[0];
      expect(key).toBe(treeKey);
      // Root must be last; children precede it (any order).
      expect(treesArg).toHaveLength(3);
      expect(treesArg[treesArg.length - 1]._hash).toBe('rootH');
      const childHashes = treesArg.slice(0, -1).map((t: any) => t._hash).sort();
      expect(childHashes).toEqual(['childA', 'childB']);
      expect(opts).toEqual({ skipNotification: undefined });
    });

    it('passes skipNotification through to insertTrees', async () => {
      const { adapter, insertTrees } = makeAdapter();
      const tree = makeTree('rootH');
      await adapter.storeMongoTree(tree, { skipNotification: true });
      expect(insertTrees.mock.calls[0][2]).toEqual({ skipNotification: true });
    });

    it('handles a single-node tree (root only)', async () => {
      const { adapter, insertTrees } = makeAdapter('ONLY');
      const tree = makeTree('soloRoot');
      const ref = await adapter.storeMongoTree(tree);
      expect(ref).toBe('ONLY');
      const treesArg = insertTrees.mock.calls[0][1];
      expect(treesArg).toHaveLength(1);
      expect(treesArg[0]._hash).toBe('soloRoot');
    });
  });

  describe('accessors', () => {
    it('getTreeKey returns the configured tree key', () => {
      const { adapter } = makeAdapter();
      expect(adapter.getTreeKey()).toBe(treeKey);
    });

    it('getDb returns the injected db instance', () => {
      const { adapter, db } = makeAdapter();
      expect(adapter.getDb()).toBe(db);
    });
  });
});
