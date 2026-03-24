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
      expect(tree.trees.size).toBe(2); // database + collection (no per-document nodes)
      
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
      
      expect(tree.trees.size).toBe(3); // db + 2 collections (no per-document nodes)
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

    it('should store documents as ComponentsTable blobs', async () => {
      await mongoDb.collection('users').insertOne({ _id: 'user1', name: 'Alice', email: 'alice@example.com' });
      
      const scanner = new MongoScanner(mongoDb, { bs });
      const tree = await scanner.scan();
      
      // Find collection node (not document node - documents are in ComponentsTable)
      const nodes = Array.from(tree.trees.values());
      const collNode = nodes.find(n => (n.meta as any)?.type === 'collection');
      
      expect(collNode).toBeDefined();
      expect((collNode?.meta as any).componentsBlobId).toBeDefined();
      expect((collNode?.meta as any).tableCfgHash).toBeDefined();
      
      // Verify ComponentsTable blob exists
      const componentsBlobId = (collNode?.meta as any).componentsBlobId;
      const componentsTable = await scanner.getComponentsTable(componentsBlobId);
      expect(componentsTable).toBeDefined();
      
      // Verify ComponentsTable structure
      expect(componentsTable._type).toBe('components');
      expect(componentsTable._tableCfg).toBe((collNode?.meta as any).tableCfgHash);
      expect(componentsTable._data.length).toBe(1);
      
      // Verify document data in ComponentsTable
      const row = componentsTable._data[0];
      expect(row._id).toBe('user1');
      expect(row.name).toBe('Alice');
      expect(row.email).toBe('alice@example.com');
      expect(row._hash).toBeDefined();
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
      
      // Collection node with ComponentsTable metadata
      const collNode = nodes.find(n => (n.meta as any)?.type === 'collection');
      expect(collNode?.meta).toBeDefined();
      expect((collNode?.meta as any).collection).toBe('users');
      expect((collNode?.meta as any).docCount).toBe(1);
      expect((collNode?.meta as any).componentsBlobId).toBeDefined();
      expect((collNode?.meta as any).tableCfgHash).toBeDefined();
      
      // No document nodes in new structure - documents are in ComponentsTable
      const docNode = nodes.find(n => (n.meta as any)?.type === 'document');
      expect(docNode).toBeUndefined();
    });

    it('should handle large collections', async () => {
      const docs = Array.from({ length: 50 }, (_, i) => ({
        _id: `user${i}`,
        name: `User ${i}`,
      }));
      await mongoDb.collection('users').insertMany(docs);
      
      const scanner = new MongoScanner(mongoDb, { bs });
      const tree = await scanner.scan();
      
      // No per-document nodes - all documents in one ComponentsTable
      const nodes = Array.from(tree.trees.values());
      const collNode = nodes.find(n => (n.meta as any)?.type === 'collection');
      
      expect(collNode).toBeDefined();
      expect((collNode?.meta as any).docCount).toBe(50);
      
      // Verify ComponentsTable has all 50 documents
      const componentsBlobId = (collNode?.meta as any).componentsBlobId;
      const componentsTable = await scanner.getComponentsTable(componentsBlobId);
      expect(componentsTable._data.length).toBe(50);
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
      
      // Collection node is now a leaf (isParent=false, no children)
      const collNode = Array.from(tree.trees.values()).find(n => (n.meta as any)?.type === 'collection');
      expect(collNode?.isParent).toBe(false);
      expect(collNode?.children).toBeUndefined();
      
      // Verify documents are in ComponentsTable, not as individual nodes
      const componentsBlobId = (collNode?.meta as any).componentsBlobId;
      const componentsTable = await scanner.getComponentsTable(componentsBlobId);
      expect(componentsTable._data.length).toBe(2);
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
      
      const collNode = Array.from(tree.trees.values()).find(n => (n.meta as any)?.type === 'collection');
      expect(collNode).toBeDefined();
      
      // Get document from ComponentsTable
      const componentsBlobId = (collNode?.meta as any).componentsBlobId;
      const componentsTable = await scanner.getComponentsTable(componentsBlobId);
      const doc = componentsTable._data[0];
      
      // Nested objects are hashed by RLJSON, so they have _hash field
      expect(doc.address).toMatchObject({
        street: '123 Main St',
        city: 'NYC',
        country: 'USA',
      });
      expect(doc.address._hash).toBeDefined();
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
      
      const collNode = Array.from(tree.trees.values()).find(n => (n.meta as any)?.type === 'collection');
      const componentsBlobId = (collNode?.meta as any).componentsBlobId;
      const componentsTable = await scanner.getComponentsTable(componentsBlobId);
      const doc = componentsTable._data[0];
      
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
      expect(collNode?.isParent).toBe(false);
      expect(collNode?.children).toBeUndefined();
    });
  });
});
