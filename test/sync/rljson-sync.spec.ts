// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.

import { BsMem } from '@rljson/bs';
import { describe, expect, it, vi } from 'vitest';

// MongoAgent is mocked so extractRljsonTree never touches a real Mongo / scanner.
// The mock returns a controllable tree and shares the caller-provided `bs`, so
// blob collection runs against a real in-memory BsMem.
const extractMock = vi.fn();
const lastCtorArgs: any[] = [];
vi.mock('../../src/mongo-agent.ts', () => ({
  MongoAgent: class {
    bs: any;
    constructor(_mongoDb: any, bs: any, options: any) {
      this.bs = bs;
      lastCtorArgs.push({ options });
    }
    extract() {
      return extractMock(this.bs);
    }
  },
}));

import {
  applyRljsonTree,
  extractRljsonTree,
  getRljsonSyncState,
} from '../../src/sync/rljson-sync.ts';

// ---------------------------------------------------------------------------
// Fake Mongo helpers
// ---------------------------------------------------------------------------

/** A minimal in-memory collection recording the ops rljson-sync performs. */
function makeCollection(initialDocs: any[] = []) {
  const docs: any[] = [...initialDocs];
  const calls = { replaceOne: [] as any[], deleteOne: [] as any[], updateOne: [] as any[] };
  return {
    docs,
    calls,
    find() {
      return { toArray: async () => docs.map((d) => ({ _id: d._id })) };
    },
    async replaceOne(filter: any, doc: any, opts: any) {
      calls.replaceOne.push({ filter, doc, opts });
      const i = docs.findIndex((d) => d._id === filter._id);
      if (i >= 0) docs[i] = doc;
      else docs.push(doc);
    },
    async deleteOne(filter: any) {
      calls.deleteOne.push({ filter });
      const i = docs.findIndex((d) => d._id === filter._id);
      if (i >= 0) docs.splice(i, 1);
    },
    async updateOne(filter: any, update: any, opts: any) {
      calls.updateOne.push({ filter, update, opts });
    },
    async findOne(filter: any) {
      return docs.find((d) => d.origin === filter.origin) ?? null;
    },
  };
}

/** A fake Db whose collection(name) returns a per-name memoised fake collection. */
function makeDb(seed: Record<string, any[]> = {}) {
  const collections = new Map<string, ReturnType<typeof makeCollection>>();
  for (const [name, docs] of Object.entries(seed)) {
    collections.set(name, makeCollection(docs));
  }
  return {
    collections,
    collection(name: string) {
      if (!collections.has(name)) collections.set(name, makeCollection());
      return collections.get(name)!;
    },
  } as any;
}

const tree = (meta: any, extra: Partial<any> = {}) => ({
  id: extra.id ?? 'n',
  isParent: false,
  meta,
  _hash: 'h',
  ...extra,
});

// ---------------------------------------------------------------------------
// extractRljsonTree
// ---------------------------------------------------------------------------

describe('extractRljsonTree', () => {
  it('collects nodes + dedupes the three blob-id kinds and base64-encodes them', async () => {
    const bs = new BsMem();
    const docBlob = await bs.setBlob(Buffer.from('doc-content'));
    const compBlob = await bs.setBlob(Buffer.from('comp-content'));
    const cfgBlob = await bs.setBlob(Buffer.from('cfg-content'));

    const trees = new Map<string, any>([
      ['rootH', tree({ type: 'database', blobId: docBlob.blobId })],
      ['cH', tree({ type: 'collection', componentsBlobId: compBlob.blobId })],
      ['cfgH', tree({ type: 'database', tableCfgsTableBlobId: cfgBlob.blobId })],
      // node with no relevant meta ids — exercises the falsy branches
      ['plain', tree({ type: 'collection' })],
    ]);

    extractMock.mockResolvedValueOnce({
      rootHash: 'rootH',
      trees,
    });

    const payload = await extractRljsonTree({
      mongoDb: {} as any,
      nodeId: 'node-A',
      bs,
      ignore: ['extra_*'],
      include: ['caratdb'],
    });

    expect(payload.origin).toBe('node-A');
    expect(payload.rootHash).toBe('rootH');
    expect(payload.totalNodes).toBe(4);
    expect(payload.nodes).toHaveLength(4);
    expect(payload.blobs).toHaveLength(3);
    const byId = Object.fromEntries(payload.blobs.map((b) => [b.blobId, b.content]));
    expect(Buffer.from(byId[docBlob.blobId], 'base64').toString()).toBe('doc-content');
    expect(Buffer.from(byId[compBlob.blobId], 'base64').toString()).toBe('comp-content');
    expect(Buffer.from(byId[cfgBlob.blobId], 'base64').toString()).toBe('cfg-content');

    // ignore list is merged with the built-in system prefixes
    expect(lastCtorArgs.at(-1).options.ignore).toEqual(
      expect.arrayContaining(['system.*', 'sync_*', 'extra_*']),
    );
    expect(lastCtorArgs.at(-1).options.include).toEqual(['caratdb']);
  });

  it('logs and skips a blob that cannot be fetched (getBlob throws)', async () => {
    const bs = new BsMem();
    const trees = new Map<string, any>([
      ['rootH', tree({ type: 'document', blobId: 'missing-blob' })],
    ]);
    extractMock.mockResolvedValueOnce({ rootHash: 'rootH', trees });

    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const payload = await extractRljsonTree({
      mongoDb: {} as any,
      nodeId: 'node-B',
      bs,
    });
    expect(payload.blobs).toHaveLength(0);
    expect(errSpy).toHaveBeenCalledWith(
      'Failed to get blob missing-blob:',
      expect.anything(),
    );
    errSpy.mockRestore();
  });

  it('defaults bs to a fresh BsMem when none is provided', async () => {
    const trees = new Map<string, any>([['rootH', tree({ type: 'collection' })]]);
    extractMock.mockResolvedValueOnce({ rootHash: 'rootH', trees });
    const payload = await extractRljsonTree({ mongoDb: {} as any, nodeId: 'node-C' });
    expect(payload.blobs).toHaveLength(0);
    expect(payload.totalNodes).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// applyRljsonTree
// ---------------------------------------------------------------------------

describe('applyRljsonTree', () => {
  it('applies a document node, deletes stale docs, and saves sync state', async () => {
    const bs = new BsMem();
    const doc = { _id: 'keep', val: 1 };
    const docBlob = await bs.setBlob(Buffer.from(JSON.stringify(doc), 'utf-8'));

    const rootNode = tree({ type: 'document', blobId: docBlob.blobId, collection: 'items' });
    // Seed an existing stale doc that should be deleted (not in payload).
    const db = makeDb({ items: [{ _id: 'keep' }, { _id: 'stale' }] });

    const payload = {
      origin: 'src-1',
      rootHash: 'root',
      totalNodes: 1,
      nodes: [{ hash: 'root', node: rootNode as any }],
      blobs: [{ blobId: docBlob.blobId, content: Buffer.from('x').toString('base64') }],
      timestamp: 't',
    };

    const res = await applyRljsonTree({ mongoDb: db, payload, bs });
    expect(res.success).toBe(true);
    expect(res.documentsCreated).toBe(1);
    expect(res.nodesApplied).toBe(1);
    expect(res.blobsReceived).toBe(1);

    const items = db.collections.get('items')!;
    // stale doc deleted
    expect(items.calls.deleteOne).toHaveLength(1);
    expect(items.calls.deleteOne[0].filter._id).toBe('stale');
    // sync state upserted
    const state = db.collections.get('rljson_sync_state')!;
    expect(state.calls.updateOne[0].opts.upsert).toBe(true);
    expect(state.calls.updateOne[0].update.$set.lastRootHash).toBe('root');
  });

  it('applies a ComponentsTable collection node and skips rows without _id', async () => {
    const bs = new BsMem();
    const componentsTable = {
      _data: [
        { _hash: 'a', _id: 'r1', n: 1 },
        { _hash: 'b', n: 2 }, // no _id → skipped
        { _hash: 'c', _id: 'r2', n: 3 },
      ],
    };
    const compBlob = await bs.setBlob(
      Buffer.from(JSON.stringify(componentsTable), 'utf-8'),
    );
    const rootNode = tree({
      type: 'collection',
      componentsBlobId: compBlob.blobId,
      name: 'articles',
    });
    const db = makeDb();
    const payload = {
      origin: 'src-2',
      rootHash: 'root',
      totalNodes: 1,
      nodes: [{ hash: 'root', node: rootNode as any }],
      blobs: [],
      timestamp: 't',
    };
    const res = await applyRljsonTree({ mongoDb: db, payload, bs });
    expect(res.success).toBe(true);
    expect(res.documentsCreated).toBe(2);
    const arts = db.collections.get('articles')!;
    expect(arts.calls.replaceOne.map((c) => c.doc._id)).toEqual(['r1', 'r2']);
    // stripped _hash must NOT be written
    expect(arts.calls.replaceOne[0].doc._hash).toBeUndefined();
  });

  it('handles legacy collection-with-children and database recursion', async () => {
    const bs = new BsMem();
    const doc = { _id: 'd1' };
    const docBlob = await bs.setBlob(Buffer.from(JSON.stringify(doc), 'utf-8'));

    const docNode = tree(
      { type: 'document', blobId: docBlob.blobId, collection: 'col1' },
      { id: 'doc' },
    );
    const colNode = tree({ type: 'collection' }, { id: 'col', children: ['docH', 'missingChild'] });
    const dbNode = tree({ type: 'database' }, { id: 'db', children: ['colH'] });

    const db = makeDb();
    const payload = {
      origin: 'src-3',
      rootHash: 'dbH',
      totalNodes: 3,
      nodes: [
        { hash: 'dbH', node: dbNode as any },
        { hash: 'colH', node: colNode as any },
        { hash: 'docH', node: docNode as any },
      ],
      blobs: [],
      timestamp: 't',
    };
    const res = await applyRljsonTree({ mongoDb: db, payload, bs });
    expect(res.success).toBe(true);
    expect(res.documentsCreated).toBe(1);
  });

  it('returns failure when the root node is missing from the node map', async () => {
    const db = makeDb();
    const payload = {
      origin: 'src-4',
      rootHash: 'absent',
      totalNodes: 0,
      nodes: [],
      blobs: [],
      timestamp: 't',
    };
    const res = await applyRljsonTree({ mongoDb: db, payload });
    expect(res.success).toBe(false);
    expect(res.error).toContain('Root node not found: absent');
    expect(res.documentsCreated).toBe(0);
    expect(res.nodesApplied).toBe(0);
  });

  it('returns failure with String(error) for a non-Error throw', async () => {
    const bs = new BsMem();
    // setBlob that throws a non-Error to exercise the String(error) branch.
    (bs as any).setBlob = async () => {
      throw 'boom-string';
    };
    const payload = {
      origin: 'src-5',
      rootHash: 'root',
      totalNodes: 1,
      nodes: [{ hash: 'root', node: tree({ type: 'database' }) as any }],
      blobs: [{ blobId: 'x', content: Buffer.from('y').toString('base64') }],
      timestamp: 't',
    };
    const res = await applyRljsonTree({ mongoDb: makeDb(), payload, bs });
    expect(res.success).toBe(false);
    expect(res.error).toBe('boom-string');
  });

  it('defaults bs to BsMem and tolerates a node with no meta / unmatched type', async () => {
    // root node has meta:null → applyTreeNode returns 0 immediately.
    const db = makeDb();
    const payload = {
      origin: 'src-6',
      rootHash: 'root',
      totalNodes: 1,
      nodes: [{ hash: 'root', node: tree(null) as any }],
      blobs: [],
      timestamp: 't',
    };
    const res = await applyRljsonTree({ mongoDb: db, payload });
    expect(res.success).toBe(true);
    expect(res.documentsCreated).toBe(0);
  });

  it('returns 0 for a document node missing its collection name', async () => {
    const bs = new BsMem();
    const rootNode = tree({ type: 'document', blobId: 'b' }); // no collection
    const db = makeDb();
    const payload = {
      origin: 'src-7',
      rootHash: 'root',
      totalNodes: 1,
      nodes: [{ hash: 'root', node: rootNode as any }],
      blobs: [],
      timestamp: 't',
    };
    const res = await applyRljsonTree({ mongoDb: db, payload, bs });
    expect(res.success).toBe(true);
    expect(res.documentsCreated).toBe(0);
  });

  it('returns 0 for a ComponentsTable node missing its collection name', async () => {
    const bs = new BsMem();
    const compBlob = await bs.setBlob(Buffer.from(JSON.stringify({ _data: [] }), 'utf-8'));
    // type=collection + componentsBlobId but neither collection nor name set
    const rootNode = { id: 'n', isParent: false, _hash: 'h', meta: { type: 'collection', componentsBlobId: compBlob.blobId } };
    const db = makeDb();
    const payload = {
      origin: 'src-8',
      rootHash: 'root',
      totalNodes: 1,
      nodes: [{ hash: 'root', node: rootNode as any }],
      blobs: [],
      timestamp: 't',
    };
    const res = await applyRljsonTree({ mongoDb: db, payload, bs });
    expect(res.success).toBe(true);
    expect(res.documentsCreated).toBe(0);
  });

  it('handles a ComponentsTable with no _data array (nullish coalesce)', async () => {
    const bs = new BsMem();
    const compBlob = await bs.setBlob(Buffer.from(JSON.stringify({}), 'utf-8'));
    const rootNode = tree({ type: 'collection', componentsBlobId: compBlob.blobId, collection: 'empty' });
    const db = makeDb();
    const payload = {
      origin: 'src-9',
      rootHash: 'root',
      totalNodes: 1,
      nodes: [{ hash: 'root', node: rootNode as any }],
      blobs: [],
      timestamp: 't',
    };
    const res = await applyRljsonTree({ mongoDb: db, payload, bs });
    expect(res.success).toBe(true);
    expect(res.documentsCreated).toBe(0);
    // collection still registered for deletion-detection
    expect(db.collections.has('empty')).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// getRljsonSyncState
// ---------------------------------------------------------------------------

describe('applyRljsonTree branch coverage', () => {
  it('database node with two document children in the same collection (already-present branch)', async () => {
    const bs = new BsMem();
    const d1 = { _id: 'a' };
    const d2 = { _id: 'b' };
    const b1 = await bs.setBlob(Buffer.from(JSON.stringify(d1), 'utf-8'));
    const b2 = await bs.setBlob(Buffer.from(JSON.stringify(d2), 'utf-8'));
    const doc1 = tree({ type: 'document', blobId: b1.blobId, collection: 'same' }, { id: 'd1' });
    const doc2 = tree({ type: 'document', blobId: b2.blobId, collection: 'same' }, { id: 'd2' });
    // database root with both doc children + one missing child hash → exercises
    // both the database recursion AND the `if (childNode)` false branch.
    const dbNode = tree({ type: 'database' }, { id: 'db', children: ['d1H', 'd2H', 'goneH'] });
    const db = makeDb();
    const payload = {
      origin: 'b-1',
      rootHash: 'dbH',
      totalNodes: 3,
      nodes: [
        { hash: 'dbH', node: dbNode as any },
        { hash: 'd1H', node: doc1 as any },
        { hash: 'd2H', node: doc2 as any },
      ],
      blobs: [],
      timestamp: 't',
    };
    const res = await applyRljsonTree({ mongoDb: db, payload, bs });
    expect(res.success).toBe(true);
    expect(res.documentsCreated).toBe(2);
  });

  it('two ComponentsTable nodes for the same collection (already-registered branch)', async () => {
    const bs = new BsMem();
    const ct1 = { _data: [{ _hash: 'x', _id: 'c1' }] };
    const ct2 = { _data: [{ _hash: 'y', _id: 'c2' }] };
    const cb1 = await bs.setBlob(Buffer.from(JSON.stringify(ct1), 'utf-8'));
    const cb2 = await bs.setBlob(Buffer.from(JSON.stringify(ct2), 'utf-8'));
    const col1 = tree({ type: 'collection', componentsBlobId: cb1.blobId, collection: 'dup' }, { id: 'c1' });
    const col2 = tree({ type: 'collection', componentsBlobId: cb2.blobId, collection: 'dup' }, { id: 'c2' });
    const dbNode = tree({ type: 'database' }, { id: 'db', children: ['c1H', 'c2H'] });
    const db = makeDb();
    const payload = {
      origin: 'b-2',
      rootHash: 'dbH',
      totalNodes: 3,
      nodes: [
        { hash: 'dbH', node: dbNode as any },
        { hash: 'c1H', node: col1 as any },
        { hash: 'c2H', node: col2 as any },
      ],
      blobs: [],
      timestamp: 't',
    };
    const res = await applyRljsonTree({ mongoDb: db, payload, bs });
    expect(res.success).toBe(true);
    expect(res.documentsCreated).toBe(2);
  });

  it('database node with no children falls through to return 0', async () => {
    const dbNode = tree({ type: 'database' }, { id: 'db' }); // no children
    const db = makeDb();
    const payload = {
      origin: 'b-4',
      rootHash: 'dbH',
      totalNodes: 1,
      nodes: [{ hash: 'dbH', node: dbNode as any }],
      blobs: [],
      timestamp: 't',
    };
    const res = await applyRljsonTree({ mongoDb: db, payload });
    expect(res.success).toBe(true);
    expect(res.documentsCreated).toBe(0);
  });

  it('legacy collection node with a child hash absent from the map', async () => {
    const colNode = tree({ type: 'collection' }, { id: 'col', children: ['absentH'] });
    const db = makeDb();
    const payload = {
      origin: 'b-3',
      rootHash: 'colH',
      totalNodes: 1,
      nodes: [{ hash: 'colH', node: colNode as any }],
      blobs: [],
      timestamp: 't',
    };
    const res = await applyRljsonTree({ mongoDb: db, payload });
    expect(res.success).toBe(true);
    expect(res.documentsCreated).toBe(0);
  });
});

describe('getRljsonSyncState', () => {
  it('returns the stored state for an origin', async () => {
    const db = makeDb({
      rljson_sync_state: [{ origin: 'o1', lastRootHash: 'r' }],
    });
    const state = await getRljsonSyncState(db, 'o1');
    expect(state).toEqual({ origin: 'o1', lastRootHash: 'r' });
  });

  it('returns null when no state exists', async () => {
    const db = makeDb();
    expect(await getRljsonSyncState(db, 'nope')).toBeNull();
  });
});
