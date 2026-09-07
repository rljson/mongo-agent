// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.

import { describe, expect, it, vi } from 'vitest';

import { MongoDbTreeAdapter } from '../src/mongo-db-tree-adapter.ts';

import type { MongoTree, Tree } from '../src/mongo-scanner.ts';

/**
 * Coverage tests for MongoDbTreeAdapter.
 *
 * Both public methods take a `Db` injected through the constructor, so we
 * never touch a real Mongo / network: a hand-rolled fake `Db` that records
 * calls and returns canned values is enough to exercise every branch.
 */

const node = (hash: string, extra: Partial<Tree> = {}): Tree => ({
  id: hash,
  isParent: false,
  meta: null,
  _hash: hash,
  ...extra,
});

const makeTree = (entries: Tree[], rootHash: string): MongoTree => {
  const trees = new Map<string, Tree>();
  for (const n of entries) trees.set(n._hash, n);
  return { rootHash, trees };
};

describe('MongoDbTreeAdapter.storeTree', () => {
  it('rejects a null/undefined tree', async () => {
    const a = new MongoDbTreeAdapter({} as any, 'mongoTrees');
    await expect(a.storeTree(null as any)).rejects.toThrow(
      'tree cannot be null or undefined',
    );
  });

  it('rejects a tree with a missing/non-string rootHash', async () => {
    const a = new MongoDbTreeAdapter({} as any, 'mongoTrees');
    await expect(
      a.storeTree({ rootHash: 123 as any, trees: new Map() } as any),
    ).rejects.toThrow('Invalid rootHash');
  });

  it('rejects when trees is not a Map', async () => {
    const a = new MongoDbTreeAdapter({} as any, 'mongoTrees');
    await expect(
      a.storeTree({ rootHash: 'r', trees: {} as any } as any),
    ).rejects.toThrow('Invalid trees');
  });

  it('rejects an empty tree map', async () => {
    const a = new MongoDbTreeAdapter({} as any, 'mongoTrees');
    await expect(
      a.storeTree({ rootHash: 'r', trees: new Map() }),
    ).rejects.toThrow('Cannot store empty tree');
  });

  it('rejects when the root hash is absent from trees', async () => {
    const a = new MongoDbTreeAdapter({} as any, 'mongoTrees');
    const tree = makeTree([node('child')], 'missing-root');
    await expect(a.storeTree(tree)).rejects.toThrow(
      'Root hash "missing-root" not found',
    );
  });

  it('inserts non-root nodes first, root last, and returns the root ref', async () => {
    const insertTrees = vi.fn(async () => [{ mongoTreesRef: 'ROOT_REF' }]);
    const db = { insertTrees } as any;
    const a = new MongoDbTreeAdapter(db, 'mongoTrees');

    const root = node('root', { isParent: true, children: ['c1', 'c2'] });
    const tree = makeTree([node('c1'), node('c2'), root], 'root');

    const ref = await a.storeTree(tree);
    expect(ref).toBe('ROOT_REF');

    expect(insertTrees).toHaveBeenCalledTimes(1);
    const [treeKey, trees, opts] = insertTrees.mock.calls[0];
    expect(treeKey).toBe('mongoTrees');
    // Root must be the LAST element (matches FsDbAdapter ordering).
    expect(trees.map((t: Tree) => t._hash)).toEqual(['c1', 'c2', 'root']);
    expect(opts).toEqual({ skipNotification: undefined });
  });

  it('forwards skipNotification to insertTrees', async () => {
    const insertTrees = vi.fn(async () => [{ mongoTreesRef: 'R' }]);
    const a = new MongoDbTreeAdapter({ insertTrees } as any, 'mongoTrees');
    const tree = makeTree([node('root')], 'root');
    await a.storeTree(tree, { skipNotification: true });
    expect(insertTrees.mock.calls[0][2]).toEqual({ skipNotification: true });
  });
});

describe('MongoDbTreeAdapter.fetchTree', () => {
  it('rejects an empty rootRef', async () => {
    const a = new MongoDbTreeAdapter({} as any, 'mongoTrees');
    await expect(a.fetchTree('')).rejects.toThrow('rootRef cannot be empty');
    await expect(a.fetchTree('   ')).rejects.toThrow('rootRef cannot be empty');
  });

  it('walks children recursively (array _data) and returns the full tree', async () => {
    // root -> [a, b]; a -> [c]; b and c are leaves.
    const store: Record<string, Tree> = {
      root: node('root', { isParent: true, children: ['a', 'b'] }),
      a: node('a', { isParent: true, children: ['c'] }),
      b: node('b'),
      c: node('c'),
    };
    const get = vi.fn(async (_route: any, q: { _hash: string }) => ({
      rljson: { mongoTrees: { _data: [store[q._hash]] } },
    }));
    const a = new MongoDbTreeAdapter({ get } as any, 'mongoTrees');

    const tree = await a.fetchTree('root');
    expect(tree.rootHash).toBe('root');
    expect([...tree.trees.keys()].sort()).toEqual(['a', 'b', 'c', 'root']);
  });

  it('handles _data given as an object (not an array)', async () => {
    const get = vi.fn(async (_route: any, q: { _hash: string }) => ({
      rljson: { mongoTrees: { _data: { '0': node(q._hash) } } },
    }));
    const a = new MongoDbTreeAdapter({ get } as any, 'mongoTrees');
    const tree = await a.fetchTree('only');
    expect([...tree.trees.keys()]).toEqual(['only']);
  });

  it('skips nodes without a _hash and non-string child refs', async () => {
    const store: Record<string, Tree[]> = {
      root: [
        node('root', { isParent: true, children: ['real', 42 as any] }),
        { id: 'x', isParent: false, meta: null } as any, // no _hash -> skipped
      ],
      real: [node('real')],
    };
    const get = vi.fn(async (_r: any, q: { _hash: string }) => ({
      rljson: { mongoTrees: { _data: store[q._hash] } },
    }));
    const a = new MongoDbTreeAdapter({ get } as any, 'mongoTrees');
    const tree = await a.fetchTree('root');
    expect([...tree.trees.keys()].sort()).toEqual(['real', 'root']);
  });

  it('continues past a db.get that throws (treats node as absent)', async () => {
    const get = vi.fn(async (_r: any, q: { _hash: string }) => {
      if (q._hash === 'root') {
        return { rljson: { mongoTrees: { _data: [node('root', { children: ['bad'] })] } } };
      }
      throw new Error('boom');
    });
    const a = new MongoDbTreeAdapter({ get } as any, 'mongoTrees');
    const tree = await a.fetchTree('root');
    expect([...tree.trees.keys()]).toEqual(['root']);
  });

  it('emits fetchTree diagnostics under SL_TREE_SYNC_DEBUG (array, object, empty, throw)', async () => {
    process.env['SL_TREE_SYNC_DEBUG'] = '1';
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
    try {
      // root: array _data with three children — object _data, empty _data, and
      // a child whose get throws — so every debug branch (row count via array,
      // via object, via 0, and the THREW path) is exercised.
      const get = vi.fn(async (_r: any, q: { _hash: string }) => {
        if (q._hash === 'root') {
          return {
            rljson: {
              mongoTrees: {
                _data: [
                  node('root', {
                    isParent: true,
                    children: ['obj', 'empty', 'boom'],
                  }),
                ],
              },
            },
          };
        }
        if (q._hash === 'obj') {
          return { rljson: { mongoTrees: { _data: { '0': node('obj') } } } };
        }
        if (q._hash === 'empty') {
          return { rljson: { mongoTrees: {} } };
        }
        throw new Error('boom');
      });
      const a = new MongoDbTreeAdapter({ get } as any, 'mongoTrees');
      const tree = await a.fetchTree('root');
      expect([...tree.trees.keys()].sort()).toEqual(['obj', 'root']);
      expect(spy).toHaveBeenCalled();
    } finally {
      spy.mockRestore();
      delete process.env['SL_TREE_SYNC_DEBUG'];
    }
  });

  it('skips a node whose result has no _data', async () => {
    const get = vi.fn(async (_r: any, q: { _hash: string }) => {
      if (q._hash === 'root') {
        return { rljson: { mongoTrees: { _data: [node('root', { children: ['empty'] })] } } };
      }
      return { rljson: { mongoTrees: {} } }; // no _data
    });
    const a = new MongoDbTreeAdapter({ get } as any, 'mongoTrees');
    const tree = await a.fetchTree('root');
    expect([...tree.trees.keys()]).toEqual(['root']);
  });

  it('fetches a shared child only once in a diamond graph', async () => {
    // Diamond: root -> [a, b]; a -> [shared]; b -> [shared].
    // `toFetch` is a Set and `add` is guarded by `!processed.has`, so a shared
    // child is fetched exactly once even when two parents reference it.
    const store: Record<string, Tree> = {
      root: node('root', { children: ['a', 'b'] }),
      a: node('a', { children: ['shared'] }),
      b: node('b', { children: ['shared'] }),
      shared: node('shared'),
    };
    const get = vi.fn(async (_r: any, q: { _hash: string }) => ({
      rljson: { mongoTrees: { _data: [store[q._hash]] } },
    }));
    const a = new MongoDbTreeAdapter({ get } as any, 'mongoTrees');
    const tree = await a.fetchTree('root');
    expect([...tree.trees.keys()].sort()).toEqual(['a', 'b', 'root', 'shared']);
    const fetched = get.mock.calls.map((c) => c[1]._hash);
    expect(fetched.filter((h) => h === 'shared')).toHaveLength(1);
  });

  it('throws when no nodes are found at all', async () => {
    const get = vi.fn(async () => ({ rljson: { mongoTrees: {} } }));
    const a = new MongoDbTreeAdapter({ get } as any, 'mongoTrees');
    await expect(a.fetchTree('root')).rejects.toThrow(
      'No tree nodes found for mongoTrees@root.',
    );
  });

  it('throws when nodes exist but the root node itself is missing', async () => {
    // root resolves to a node carrying a DIFFERENT _hash, so the rootRef
    // never lands in `fetched`.
    const get = vi.fn(async () => ({
      rljson: { mongoTrees: { _data: [node('other')] } },
    }));
    const a = new MongoDbTreeAdapter({ get } as any, 'mongoTrees');
    await expect(a.fetchTree('root')).rejects.toThrow(
      'Root tree node "root" not found in fetched data.',
    );
  });
});
