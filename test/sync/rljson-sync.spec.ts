// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { BsMem } from '@rljson/bs';

import { Db as MongoDb, MongoClient } from 'mongodb';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import {
  applyRljsonTree, extractRljsonTree, getRljsonSyncState, RljsonTreePayload
} from '../../src/sync/rljson-sync';


const MONGO_URI =
  process.env.MONGO_URI || 'mongodb://localhost:27017/?directConnection=true';
const TEST_DB_EXTRACT = 'test_rljson_sync_extract';
const TEST_DB_APPLY = 'test_rljson_sync_apply';

describe('rljson-sync', () => {
  let client: MongoClient;
  let mongoDbExtract: MongoDb;
  let mongoDbApply: MongoDb;
  let bs: BsMem;

  beforeEach(async () => {
    client = new MongoClient(MONGO_URI);
    await client.connect();
    mongoDbExtract = client.db(TEST_DB_EXTRACT);
    mongoDbApply = client.db(TEST_DB_APPLY);
    await mongoDbExtract.dropDatabase();
    await mongoDbApply.dropDatabase();
    bs = new BsMem();
  });

  afterEach(async () => {
    if (client) {
      await client.close();
    }
  });

  describe('extractRljsonTree', () => {
    it('should extract empty database', async () => {
      const payload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'node1',
        bs,
      });

      expect(payload).toBeDefined();
      expect(payload.origin).toBe('node1');
      expect(payload.rootHash).toBeDefined();
      expect(payload.totalNodes).toBeGreaterThanOrEqual(1);
      expect(payload.nodes).toBeInstanceOf(Array);
      expect(payload.blobs).toBeInstanceOf(Array);
      expect(payload.timestamp).toBeDefined();
    });

    it('should extract database with single document', async () => {
      await mongoDbExtract.collection('users').insertOne({
        _id: 'user1',
        name: 'Alice',
      });

      const payload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'node1',
        bs,
      });

      expect(payload.totalNodes).toBeGreaterThanOrEqual(3); // db + collection + doc
      expect(payload.blobs.length).toBe(1);

      const blob = payload.blobs[0];
      expect(blob.blobId).toBeDefined();
      expect(blob.content).toBeDefined();

      const decoded = Buffer.from(blob.content, 'base64').toString('utf-8');
      const doc = JSON.parse(decoded);
      expect(doc._id).toBe('user1');
      expect(doc.name).toBe('Alice');
    });

    it('should extract database with multiple collections', async () => {
      await mongoDbExtract.collection('users').insertMany([
        { _id: 'user1', name: 'Alice' },
        { _id: 'user2', name: 'Bob' },
      ]);
      await mongoDbExtract.collection('posts').insertMany([
        { _id: 'post1', title: 'Hello' },
        { _id: 'post2', title: 'World' },
      ]);

      const payload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'node1',
        bs,
      });

      expect(payload.totalNodes).toBeGreaterThanOrEqual(7); // db + 2 collections + 4 docs
      expect(payload.blobs.length).toBe(4);
    });

    it('should ignore specified collections', async () => {
      await mongoDbExtract
        .collection('users')
        .insertOne({ _id: 'user1', name: 'Alice' });
      await mongoDbExtract
        .collection('temp_data')
        .insertOne({ _id: 'temp1', data: 'ignore' });

      const payload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'node1',
        bs,
        ignore: ['temp_*'],
      });

      const nodes = payload.nodes.map((n) => n.node.meta);
      const tempNode = nodes.find((m: any) => m?.name === 'temp_data');
      expect(tempNode).toBeUndefined();
    });

    it('should only include specified collections', async () => {
      await mongoDbExtract
        .collection('users')
        .insertOne({ _id: 'user1', name: 'Alice' });
      await mongoDbExtract
        .collection('posts')
        .insertOne({ _id: 'post1', title: 'Hello' });
      await mongoDbExtract
        .collection('comments')
        .insertOne({ _id: 'comment1', text: 'Nice' });

      const payload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'node1',
        bs,
        include: ['users', 'posts'],
      });

      const nodes = payload.nodes.map((n) => n.node.meta);
      const commentNode = nodes.find((m: any) => m?.name === 'comments');
      expect(commentNode).toBeUndefined();
    });

    it('should generate unique hashes for each node', async () => {
      await mongoDbExtract.collection('users').insertMany([
        { _id: 'user1', name: 'Alice' },
        { _id: 'user2', name: 'Bob' },
      ]);

      const payload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'node1',
        bs,
      });

      const hashes = payload.nodes.map((n) => n.hash);
      const uniqueHashes = new Set(hashes);

      expect(hashes.length).toBe(uniqueHashes.size);
    });

    it('should use custom blob storage', async () => {
      const customBs = new BsMem();
      await mongoDbExtract
        .collection('users')
        .insertOne({ _id: 'user1', name: 'Alice' });

      const payload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'node1',
        bs: customBs,
      });

      expect(payload.blobs.length).toBe(1);

      // Verify blob is in custom storage
      const blob = await customBs.getBlob(payload.blobs[0].blobId);
      expect(blob).toBeDefined();
    });

    it('should include timestamp', async () => {
      const before = Date.now();
      const payload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'node1',
        bs,
      });
      const after = Date.now();

      const timestamp = new Date(payload.timestamp).getTime();
      expect(timestamp).toBeGreaterThanOrEqual(before);
      expect(timestamp).toBeLessThanOrEqual(after);
    });

    it('should handle large documents', async () => {
      const largeDoc = {
        _id: 'large1',
        data: 'x'.repeat(10000),
        nested: { deep: { value: 'test' } },
      };

      await mongoDbExtract.collection('large').insertOne(largeDoc);

      const payload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'node1',
        bs,
      });

      expect(payload.blobs.length).toBe(1);
      const decoded = Buffer.from(payload.blobs[0].content, 'base64').toString(
        'utf-8',
      );
      const doc = JSON.parse(decoded);
      expect(doc._id).toBe('large1');
      expect(doc.data).toBe('x'.repeat(10000));
    });
  });

  describe('applyRljsonTree', () => {
    let payload: RljsonTreePayload;

    beforeEach(async () => {
      // Setup source data
      await mongoDbExtract.collection('users').insertMany([
        { _id: 'user1', name: 'Alice', email: 'alice@example.com' },
        { _id: 'user2', name: 'Bob', email: 'bob@example.com' },
      ]);
      await mongoDbExtract.collection('posts').insertOne({
        _id: 'post1',
        title: 'Hello World',
        author: 'user1',
      });

      // Extract tree
      payload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'sourceNode',
        bs,
      });
    });

    it('should apply tree to empty database', async () => {
      const result = await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload,
        bs,
      });

      expect(result.success).toBe(true);
      expect(result.rootHash).toBe(payload.rootHash);
      expect(result.blobsReceived).toBe(3);
      expect(result.documentsCreated).toBe(3);
    });

    it('should create all collections', async () => {
      await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload,
        bs,
      });

      const collections = await mongoDbApply.listCollections().toArray();
      const collNames = collections
        .map((c) => c.name)
        .filter((n) => n !== 'rljson_sync_state');

      expect(collNames).toContain('users');
      expect(collNames).toContain('posts');
    });

    it('should create all documents', async () => {
      await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload,
        bs,
      });

      const users = await mongoDbApply.collection('users').find().toArray();
      const posts = await mongoDbApply.collection('posts').find().toArray();

      expect(users.length).toBe(2);
      expect(posts.length).toBe(1);

      expect(users.find((u) => u._id === 'user1')).toBeDefined();
      expect(users.find((u) => u._id === 'user2')).toBeDefined();
      expect(posts.find((p) => p._id === 'post1')).toBeDefined();
    });

    it('should preserve document content', async () => {
      await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload,
        bs,
      });

      const user1 = await mongoDbApply
        .collection('users')
        .findOne({ _id: 'user1' });

      expect(user1).toBeDefined();
      expect(user1!.name).toBe('Alice');
      expect(user1!.email).toBe('alice@example.com');
    });

    it('should save sync state', async () => {
      await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload,
        bs,
      });

      const syncState = await mongoDbApply
        .collection('rljson_sync_state')
        .findOne({
          origin: 'sourceNode',
        });

      expect(syncState).toBeDefined();
      expect(syncState!.origin).toBe('sourceNode');
      expect(syncState!.lastRootHash).toBe(payload.rootHash);
      expect(syncState!.totalNodes).toBe(payload.totalNodes);
      expect(syncState!.totalBlobs).toBe(payload.blobs.length);
      expect(syncState!.lastSyncedAt).toBeDefined();
    });

    it('should update existing documents', async () => {
      // First apply
      await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload,
        bs,
      });

      // Modify source and re-extract
      await mongoDbExtract
        .collection('users')
        .updateOne({ _id: 'user1' }, { $set: { name: 'Alice Updated' } });

      const newPayload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'sourceNode',
        bs: new BsMem(), // Use new blob storage
      });

      // Apply updated tree
      await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload: newPayload,
        bs: new BsMem(),
      });

      const user1 = await mongoDbApply
        .collection('users')
        .findOne({ _id: 'user1' });
      expect(user1!.name).toBe('Alice Updated');
    });

    it('should use custom blob storage', async () => {
      const customBs = new BsMem();

      const result = await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload,
        bs: customBs,
      });

      expect(result.success).toBe(true);
    });

    it('should handle complex nested documents', async () => {
      await mongoDbExtract.dropDatabase();
      await mongoDbExtract.collection('orders').insertOne({
        _id: 'order1',
        customer: { name: 'Alice', email: 'alice@example.com' },
        items: [
          { id: 1, qty: 2 },
          { id: 2, qty: 1 },
        ],
      });

      const payload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'node1',
        bs: new BsMem(),
      });

      await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload,
        bs: new BsMem(),
      });

      const order = await mongoDbApply
        .collection('orders')
        .findOne({ _id: 'order1' });
      expect(order!.customer.name).toBe('Alice');
      expect(order!.items).toHaveLength(2);
    });

    it('should handle empty payload gracefully', async () => {
      // Create minimal payload with database node only
      await mongoDbExtract.dropDatabase();

      const emptyPayload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'emptyNode',
        bs: new BsMem(),
      });

      const result = await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload: emptyPayload,
        bs: new BsMem(),
      });

      expect(result.success).toBe(true);
      expect(result.documentsCreated).toBe(0);
    });

    it('should return error on failure', async () => {
      const invalidPayload: RljsonTreePayload = {
        origin: 'invalid',
        rootHash: 'nonexistent',
        totalNodes: 1,
        nodes: [],
        blobs: [],
        timestamp: new Date().toISOString(),
      };

      const result = await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload: invalidPayload,
        bs,
      });

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });
  });

  describe('getRljsonSyncState', () => {
    it('should return null when no sync state exists', async () => {
      const state = await getRljsonSyncState(mongoDbApply, 'unknownNode');

      expect(state).toBeNull();
    });

    it('should return sync state after apply', async () => {
      await mongoDbExtract
        .collection('users')
        .insertOne({ _id: 'user1', name: 'Alice' });

      const payload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'testNode',
        bs,
      });

      await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload,
        bs,
      });

      const state = await getRljsonSyncState(mongoDbApply, 'testNode');

      expect(state).toBeDefined();
      expect(state!.origin).toBe('testNode');
      expect(state!.lastRootHash).toBe(payload.rootHash);
      expect(state!.totalNodes).toBe(payload.totalNodes);
      expect(state!.totalBlobs).toBe(payload.blobs.length);
    });

    it('should update sync state on multiple applies', async () => {
      await mongoDbExtract
        .collection('users')
        .insertOne({ _id: 'user1', name: 'Alice' });

      const payload1 = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'testNode',
        bs: new BsMem(),
      });

      await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload: payload1,
        bs: new BsMem(),
      });

      const state1 = await getRljsonSyncState(mongoDbApply, 'testNode');

      // Add more data
      await mongoDbExtract
        .collection('users')
        .insertOne({ _id: 'user2', name: 'Bob' });

      const payload2 = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'testNode',
        bs: new BsMem(),
      });

      await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload: payload2,
        bs: new BsMem(),
      });

      const state2 = await getRljsonSyncState(mongoDbApply, 'testNode');

      expect(state2!.lastRootHash).not.toBe(state1!.lastRootHash);
      expect(state2!.totalBlobs).toBeGreaterThan(state1!.totalBlobs);
    });
  });

  describe('end-to-end sync', () => {
    it('should sync data between two databases', async () => {
      // Setup source with data
      await mongoDbExtract.collection('users').insertMany([
        { _id: 'user1', name: 'Alice', role: 'admin' },
        { _id: 'user2', name: 'Bob', role: 'user' },
      ]);
      await mongoDbExtract.collection('posts').insertMany([
        { _id: 'post1', title: 'First Post', author: 'user1' },
        { _id: 'post2', title: 'Second Post', author: 'user2' },
      ]);

      // Extract from source
      const payload = await extractRljsonTree({
        mongoDb: mongoDbExtract,
        nodeId: 'sourceNode',
        bs,
      });

      // Apply to target
      const result = await applyRljsonTree({
        mongoDb: mongoDbApply,
        payload,
        bs,
      });

      expect(result.success).toBe(true);

      // Verify data matches
      const sourceUsers = await mongoDbExtract
        .collection('users')
        .find()
        .sort({ _id: 1 })
        .toArray();
      const targetUsers = await mongoDbApply
        .collection('users')
        .find()
        .sort({ _id: 1 })
        .toArray();

      expect(JSON.stringify(targetUsers)).toBe(JSON.stringify(sourceUsers));

      const sourcePosts = await mongoDbExtract
        .collection('posts')
        .find()
        .sort({ _id: 1 })
        .toArray();
      const targetPosts = await mongoDbApply
        .collection('posts')
        .find()
        .sort({ _id: 1 })
        .toArray();

      expect(JSON.stringify(targetPosts)).toBe(JSON.stringify(sourcePosts));
    });
  });
});
