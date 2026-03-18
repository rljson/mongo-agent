// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import { MongoClient, type Db as MongoDb } from 'mongodb';
import { BsMem } from '@rljson/bs';
import { MongoScanner } from '../src/mongo-scanner';

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/?directConnection=true';
const TEST_DB_NAME = 'test_mongo_scanner';

describe('MongoScanner', () => {
  let client: MongoClient;
  let mongoDb: MongoDb;
  let bs: BsMem;

  beforeEach(async () => {
    client = new MongoClient(MONGO_URI);
    await client.connect();
    mongoDb = client.db(TEST_DB_NAME);
    await mongoDb.dropDatabase();
    bs = new BsMem();
  });

  afterEach(async () => {
    if (client) {
      await client.close();
    }
  });

  describe('constructor', () => {
    it('should create scanner with default options', () => {
      const scanner = new MongoScanner(mongoDb);
      expect(scanner).toBeDefined();
      expect(scanner.bs).toBeDefined();
    });

    it('should create scanner with custom blob storage', () => {
      const customBs = new BsMem();
      const scanner = new MongoScanner(mongoDb, { bs: customBs });
      expect(scanner.bs).toBe(customBs);
    });

    it('should create scanner with ignore patterns', () => {
      const scanner = new MongoScanner(mongoDb, {
        bs,
        ignore: ['system.*', 'temp_*'],
      });
      expect(scanner).toBeDefined();
    });

    it('should create scanner with include patterns', () => {
      const scanner = new MongoScanner(mongoDb, {
        bs,
        include: ['users', 'posts'],
      });
      expect(scanner).toBeDefined();
    });
  });

  describe('scan', () => {
    it('should scan empty database', async () => {
      const scanner = new MongoScanner(mongoDb, { bs });
      const tree = await scanner.scan();
      
      expect(tree).toBeDefined();
      expect(tree.rootHash).toBeDefined();
      expect(tree.trees).toBeInstanceOf(Map);
      expect(tree.trees.size).toBeGreaterThanOrEqual(1); // At least database node
    });

    it('should scan database with single collection', async () => {
      await mongoDb.collection('users').insertOne({ _id: 'user1', name: 'Alice' });
      
      const scanner = new MongoScanner(mongoDb, { bs });
      const tree = await scanner.scan();
      
      expect(tree.rootHash).toBeDefined();
      expect(tree.trees.size).toBeGreaterThanOrEqual(3); // database + collection + document
      
      // Verify root node
      const rootNode = tree.trees.get(tree.rootHash);
      expect(rootNode).toBeDefined();
      expect(rootNode?.meta).toBeDefined();
      expect((rootNode?.meta as any).type).toBe('database');
    });

    it('should scan database with multiple collections', async () => {
      await mongoDb.collection('users').insertMany([
        { _id: 'user1', name: 'Alice' },
        { _id: 'user2', name: 'Bob' },
      ]);
      await mongoDb.collection('posts').insertMany([
        { _id: 'post1', title: 'Hello' },
        { _id: 'post2', title: 'World' },
      ]);
      
      const scanner = new MongoScanner(mongoDb, { bs });
      const tree = await scanner.scan();
      
      expect(tree.trees.size).toBeGreaterThanOrEqual(7); // db + 2 collections + 4 documents
    });

    it('should ignore specified collections', async () => {
      await mongoDb.collection('users').insertOne({ _id: 'user1', name: 'Alice' });
      await mongoDb.collection('temp_data').insertOne({ _id: 'temp1', data: 'ignore me' });
      await mongoDb.collection('tempfiles').insertOne({ _id: 'temp2', data: 'ignore me too' });
      
      const scanner = new MongoScanner(mongoDb, {
        bs,
        ignore: ['temp*'],
      });
      const tree = await scanner.scan();
      
      // Should include users
      const nodes = Array.from(tree.trees.values());
      const usersNode = nodes.find(n => (n.meta as any)?.name === 'users');
      expect(usersNode).toBeDefined();
      
      // Should not include temp collections
      const tempDataNode = nodes.find(n => (n.meta as any)?.name === 'temp_data');
      const tempFilesNode = nodes.find(n => (n.meta as any)?.name === 'tempfiles');
      expect(tempDataNode).toBeUndefined();
      expect(tempFilesNode).toBeUndefined();
    });

    it('should only include specified collections', async () => {
      await mongoDb.collection('users').insertOne({ _id: 'user1', name: 'Alice' });
      await mongoDb.collection('posts').insertOne({ _id: 'post1', title: 'Hello' });
      await mongoDb.collection('comments').insertOne({ _id: 'comment1', text: 'Nice' });
      await mongoDb.collection('likes').insertOne({ _id: 'like1', count: 5 });
      
      const scanner = new MongoScanner(mongoDb, {
        bs,
        include: ['users', 'posts'],
      });
      const tree = await scanner.scan();
      
      const nodes = Array.from(tree.trees.values());
      
      // Should include users and posts
      const usersNode = nodes.find(n => (n.meta as any)?.name === 'users');
      const postsNode = nodes.find(n => (n.meta as any)?.name === 'posts');
      expect(usersNode).toBeDefined();
      expect(postsNode).toBeDefined();
      
      // Should not include comments or likes
      const commentsNode = nodes.find(n => (n.meta as any)?.name === 'comments');
      const likesNode = nodes.find(n => (n.meta as any)?.name === 'likes');
      expect(commentsNode).toBeUndefined();
      expect(likesNode).toBeUndefined();
    });

    it('should store documents as blobs', async () => {
      await mongoDb.collection('users').insertOne({ _id: 'user1', name: 'Alice', email: 'alice@example.com' });
      
      const scanner = new MongoScanner(mongoDb, { bs });
      const tree = await scanner.scan();
      
      // Find document node
      const nodes = Array.from(tree.trees.values());
      const docNode = nodes.find(n => (n.meta as any)?.type === 'document');
      
      expect(docNode).toBeDefined();
      expect((docNode?.meta as any).blobId).toBeDefined();
      
      // Verify blob exists in scanner's blob storage
      const blobId = (docNode?.meta as any).blobId;
      const blob = await scanner.bs.getBlob(blobId);
      expect(blob).toBeDefined();
      
      // Verify blob content
      const content = blob.content.toString('utf-8');
      const doc = JSON.parse(content);
      expect(doc._id).toBe('user1');
      expect(doc.name).toBe('Alice');
    });

    it('should generate unique hashes for each node', async () => {
      await mongoDb.collection('users').insertMany([
        { _id: 'user1', name: 'Alice' },
        { _id: 'user2', name: 'Bob' },
      ]);
      
      const scanner = new MongoScanner(mongoDb, { bs });
      const tree = await scanner.scan();
      
      const hashes = Array.from(tree.trees.keys());
      const uniqueHashes = new Set(hashes);
      
      expect(hashes.length).toBe(uniqueHashes.size); // All hashes should be unique
    });

    it('should include metadata in tree nodes', async () => {
      await mongoDb.collection('users').insertOne({ _id: 'user1', name: 'Alice' });
      
      const scanner = new MongoScanner(mongoDb, { bs });
      const tree = await scanner.scan();
      
      const nodes = Array.from(tree.trees.values());
      
      // Database node
      const dbNode = nodes.find(n => (n.meta as any)?.type === 'database');
      expect(dbNode?.meta).toBeDefined();
      expect((dbNode?.meta as any).database).toBe(TEST_DB_NAME);
      expect((dbNode?.meta as any).mtime).toBeGreaterThan(0);
      
      // Collection node
      const collNode = nodes.find(n => (n.meta as any)?.type === 'collection');
      expect(collNode?.meta).toBeDefined();
      expect((collNode?.meta as any).collection).toBe('users');
      expect((collNode?.meta as any).docCount).toBe(1);
      
      // Document node
      const docNode = nodes.find(n => (n.meta as any)?.type === 'document');
      expect(docNode?.meta).toBeDefined();
      expect((docNode?.meta as any).docId).toBe('user1');
      expect((docNode?.meta as any).blobId).toBeDefined();
    });

    it('should handle large collections', async () => {
      const docs = Array.from({ length: 50 }, (_, i) => ({
        _id: `user${i}`,
        name: `User ${i}`,
      }));
      await mongoDb.collection('users').insertMany(docs);
      
      const scanner = new MongoScanner(mongoDb, { bs });
      const tree = await scanner.scan();
      
      const nodes = Array.from(tree.trees.values());
      const docNodes = nodes.filter(n => (n.meta as any)?.type === 'document');
      
      expect(docNodes.length).toBe(50);
    });

    it('should create parent-child relationships', async () => {
      await mongoDb.collection('users').insertMany([
        { _id: 'user1', name: 'Alice' },
        { _id: 'user2', name: 'Bob' },
      ]);
      
      const scanner = new MongoScanner(mongoDb, { bs });
      const tree = await scanner.scan();
      
      // Database node should be parent
      const dbNode = tree.trees.get(tree.rootHash);
      expect(dbNode?.isParent).toBe(true);
      expect(dbNode?.children).toBeDefined();
      expect(dbNode!.children!.length).toBeGreaterThan(0);
      
      // Collection node should be parent
      const collNode = Array.from(tree.trees.values()).find(n => (n.meta as any)?.type === 'collection');
      expect(collNode?.isParent).toBe(true);
      expect(collNode?.children).toBeDefined();
      expect(collNode!.children!.length).toBe(2);
      
      // Document node should not be parent
      const docNode = Array.from(tree.trees.values()).find(n => (n.meta as any)?.type === 'document');
      expect(docNode?.isParent).toBe(false);
    });
  });

  describe('edge cases', () => {
    it('should handle documents with complex nested objects', async () => {
      await mongoDb.collection('users').insertOne({
        _id: 'user1',
        name: 'Alice',
        address: {
          street: '123 Main St',
          city: 'NYC',
          country: 'USA',
        },
        tags: ['admin', 'verified'],
      });
      
      const scanner = new MongoScanner(mongoDb, { bs });
      const tree = await scanner.scan();
      
      const docNode = Array.from(tree.trees.values()).find(n => (n.meta as any)?.docId === 'user1');
      expect(docNode).toBeDefined();
      
      const blobId = (docNode?.meta as any).blobId;
      const blob = await scanner.bs.getBlob(blobId);
      const doc = JSON.parse(blob.content.toString('utf-8'));
      
      expect(doc.address).toEqual({
        street: '123 Main St',
        city: 'NYC',
        country: 'USA',
      });
      expect(doc.tags).toEqual(['admin', 'verified']);
    });

    it('should handle documents with special characters', async () => {
      await mongoDb.collection('users').insertOne({
        _id: 'user1',
        name: 'Alice "The Pro" O\'Brien',
        bio: 'Loves 日本語 and emojis 🎉',
      });
      
      const scanner = new MongoScanner(mongoDb, { bs });
      const tree = await scanner.scan();
      
      const docNode = Array.from(tree.trees.values()).find(n => (n.meta as any)?.docId === 'user1');
      const blobId = (docNode?.meta as any).blobId;
      const blob = await scanner.bs.getBlob(blobId);
      const doc = JSON.parse(blob.content.toString('utf-8'));
      
      expect(doc.name).toBe('Alice "The Pro" O\'Brien');
      expect(doc.bio).toBe('Loves 日本語 and emojis 🎉');
    });

    it('should handle empty collections', async () => {
      await mongoDb.createCollection('empty_collection');
      
      const scanner = new MongoScanner(mongoDb, { bs });
      const tree = await scanner.scan();
      
      const collNode = Array.from(tree.trees.values()).find(n => 
        (n.meta as any)?.type === 'collection' && (n.meta as any)?.name === 'empty_collection'
      );
      
      expect(collNode).toBeDefined();
      expect((collNode?.meta as any).docCount).toBe(0);
      expect(collNode?.children).toEqual([]);
    });
  });
});
