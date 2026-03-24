// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { MongoClient, ObjectId } from 'mongodb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { MongoToRljsonConverter } from '../src/mongo-to-rljson-converter';

describe('MongoToRljsonConverter', () => {
  let client: MongoClient;
  let converter: MongoToRljsonConverter;

  beforeAll(async () => {
    const mongoUri =
      process.env.MONGO_URI ||
      'mongodb://localhost:27017/?directConnection=true';
    client = new MongoClient(mongoUri);
    await client.connect();
    converter = new MongoToRljsonConverter();
  }, 30000); // 30 second timeout

  afterAll(async () => {
    await client.close();
  });

  describe('discoverSchema', () => {
    it('should discover schema from simple collection', async () => {
      const db = client.db('test_converter');
      const collection = db.collection('users');

      // Clean and insert test data
      await collection.deleteMany({});
      await collection.insertMany([
        { _id: 'user1', name: 'Alice', age: 30, email: 'alice@example.com' },
        { _id: 'user2', name: 'Bob', age: 25, email: 'bob@example.com' },
      ]);

      const tableCfg = await converter.discoverSchema(collection);

      expect(tableCfg.key).toBe('users');
      expect(tableCfg.type).toBe('components');
      expect(tableCfg._hash).toBeTruthy();
      expect(tableCfg.columns).toBeDefined();

      // Should have _hash, _id, name, age, email columns
      const columnKeys = tableCfg.columns.map((c) => c.key);
      expect(columnKeys).toContain('_hash');
      expect(columnKeys).toContain('_id');
      expect(columnKeys).toContain('name');
      expect(columnKeys).toContain('age');
      expect(columnKeys).toContain('email');

      // Check column types
      const nameCol = tableCfg.columns.find((c) => c.key === 'name');
      expect(nameCol?.type).toBe('string');

      const ageCol = tableCfg.columns.find((c) => c.key === 'age');
      expect(ageCol?.type).toBe('number');
    });

    it('should handle ObjectId fields', async () => {
      const db = client.db('test_converter');
      const collection = db.collection('posts');

      await collection.deleteMany({});
      await collection.insertMany([
        { _id: new ObjectId(), title: 'Post 1', authorId: new ObjectId() },
        { _id: new ObjectId(), title: 'Post 2', authorId: new ObjectId() },
      ]);

      const tableCfg = await converter.discoverSchema(collection);

      const idCol = tableCfg.columns.find((c) => c.key === '_id');
      expect(idCol?.type).toBe('string'); // ObjectId converted to string

      const authorIdCol = tableCfg.columns.find((c) => c.key === 'authorId');
      expect(authorIdCol?.type).toBe('string');
    });

    it('should handle Date fields', async () => {
      const db = client.db('test_converter');
      const collection = db.collection('events');

      await collection.deleteMany({});
      await collection.insertMany([
        { _id: 'event1', name: 'Event 1', date: new Date('2026-01-01') },
        { _id: 'event2', name: 'Event 2', date: new Date('2026-02-01') },
      ]);

      const tableCfg = await converter.discoverSchema(collection);

      const dateCol = tableCfg.columns.find((c) => c.key === 'date');
      expect(dateCol?.type).toBe('number'); // Dates stored as timestamps
    });

    it('should handle nested objects', async () => {
      const db = client.db('test_converter');
      const collection = db.collection('profiles');

      await collection.deleteMany({});
      await collection.insertMany([
        {
          _id: 'profile1',
          name: 'Alice',
          address: { city: 'NYC', zip: '10001' },
        },
        {
          _id: 'profile2',
          name: 'Bob',
          address: { city: 'LA', zip: '90001' },
        },
      ]);

      const tableCfg = await converter.discoverSchema(collection);

      // Nested objects should be flattened with dot notation
      const cityCol = tableCfg.columns.find((c) => c.key === 'address.city');
      expect(cityCol).toBeDefined();
      expect(cityCol?.type).toBe('string');

      const zipCol = tableCfg.columns.find((c) => c.key === 'address.zip');
      expect(zipCol).toBeDefined();
    });

    it('should handle arrays as json type', async () => {
      const db = client.db('test_converter');
      const collection = db.collection('tags');

      await collection.deleteMany({});
      await collection.insertMany([
        { _id: 'item1', name: 'Item 1', tags: ['tag1', 'tag2'] },
        { _id: 'item2', name: 'Item 2', tags: ['tag3'] },
      ]);

      const tableCfg = await converter.discoverSchema(collection);

      const tagsCol = tableCfg.columns.find((c) => c.key === 'tags');
      expect(tagsCol?.type).toBe('jsonArray'); // Arrays stored as jsonArray
    });

    it('should handle empty collection', async () => {
      const db = client.db('test_converter');
      const collection = db.collection('empty');

      await collection.deleteMany({});

      const tableCfg = await converter.discoverSchema(collection);

      expect(tableCfg.key).toBe('empty');
      expect(tableCfg.columns).toHaveLength(1); // Only _hash column
      expect(tableCfg.columns[0].key).toBe('_hash');
    });
  });

  describe('convertDocument', () => {
    it('should convert document with all field types', async () => {
      const db = client.db('test_converter');
      const collection = db.collection('mixed');

      await collection.deleteMany({});
      await collection.insertOne({
        _id: 'doc1',
        name: 'Test',
        count: 42,
        active: true,
        createdAt: new Date('2026-01-01'),
        objectId: new ObjectId(),
        tags: ['a', 'b'],
        meta: { key: 'value' },
      });

      const tableCfg = await converter.discoverSchema(collection);
      const doc = await collection.findOne({ _id: 'doc1' });

      const converted = converter.convertDocument(doc!, tableCfg);

      expect(converted._hash).toBeTruthy();
      expect(converted._id).toBe('doc1');
      expect(converted.name).toBe('Test');
      expect(converted.count).toBe(42);
      expect(converted.active).toBe(true);
      expect(typeof converted.createdAt).toBe('number'); // Converted to timestamp
      expect(typeof converted.objectId).toBe('string'); // Converted to string
      expect(Array.isArray(converted.tags)).toBe(true);
      expect(typeof converted.meta).toBe('object');
    });

    it('should handle null values', async () => {
      const db = client.db('test_converter');
      const collection = db.collection('nulls');

      await collection.deleteMany({});
      await collection.insertOne({
        _id: 'doc1',
        name: 'Test',
        optional: null,
      });

      const tableCfg = await converter.discoverSchema(collection);
      const doc = await collection.findOne({ _id: 'doc1' });

      const converted = converter.convertDocument(doc!, tableCfg);

      expect(converted.optional).toBeNull();
    });
  });

  describe('convertCollection', () => {
    it('should convert entire collection to ComponentsTable', async () => {
      const db = client.db('test_converter');
      const collection = db.collection('products');

      await collection.deleteMany({});
      await collection.insertMany([
        { _id: 'p1', name: 'Product 1', price: 10.99 },
        { _id: 'p2', name: 'Product 2', price: 20.99 },
        { _id: 'p3', name: 'Product 3', price: 30.99 },
      ]);

      const tableCfg = await converter.discoverSchema(collection);
      const componentsTable = await converter.convertCollection(
        collection,
        tableCfg,
      );

      expect(componentsTable._type).toBe('components');
      expect(componentsTable._tableCfg).toBe(tableCfg._hash);
      expect(componentsTable._hash).toBeTruthy();
      expect(componentsTable._data).toHaveLength(3);

      // Each row should be hashed
      for (const row of componentsTable._data) {
        expect(row._hash).toBeTruthy();
        expect(row._id).toBeTruthy();
        expect(row.name).toBeTruthy();
        expect(typeof row.price).toBe('number');
      }
    });

    it('should respect limit parameter', async () => {
      const db = client.db('test_converter');
      const collection = db.collection('limited');

      await collection.deleteMany({});
      await collection.insertMany([
        { _id: '1', value: 1 },
        { _id: '2', value: 2 },
        { _id: '3', value: 3 },
        { _id: '4', value: 4 },
        { _id: '5', value: 5 },
      ]);

      const tableCfg = await converter.discoverSchema(collection);
      const componentsTable = await converter.convertCollection(
        collection,
        tableCfg,
        3, // Limit to 3 documents
      );

      expect(componentsTable._data).toHaveLength(3);
    });

    it('should produce valid ComponentsTable structure', async () => {
      const db = client.db('test_converter');
      const collection = db.collection('orders');

      await collection.deleteMany({});
      await collection.insertMany([
        { _id: 'order1', total: 100, status: 'pending' },
        { _id: 'order2', total: 200, status: 'completed' },
      ]);

      const tableCfg = await converter.discoverSchema(collection);
      const componentsTable = await converter.convertCollection(
        collection,
        tableCfg,
      );

      // Verify structure matches ComponentsTable interface
      expect(componentsTable).toHaveProperty('_tableCfg');
      expect(componentsTable).toHaveProperty('_type');
      expect(componentsTable).toHaveProperty('_data');
      expect(componentsTable).toHaveProperty('_hash');

      expect(typeof componentsTable._tableCfg).toBe('string');
      expect(componentsTable._type).toBe('components');
      expect(Array.isArray(componentsTable._data)).toBe(true);
      expect(typeof componentsTable._hash).toBe('string');
    });
  });

  describe('integration test with real MongoDB data', () => {
    it('should handle complex real-world document structure', async () => {
      const db = client.db('test_converter');
      const collection = db.collection('articles');

      await collection.deleteMany({});
      await collection.insertOne({
        _id: new ObjectId(),
        title: 'Sample Article',
        author: {
          name: 'John Doe',
          email: 'john@example.com',
        },
        tags: ['tech', 'nodejs', 'mongodb'],
        metadata: {
          views: 1000,
          likes: 50,
        },
        publishedAt: new Date('2026-03-20'),
        draft: false,
      });

      const tableCfg = await converter.discoverSchema(collection);
      const componentsTable = await converter.convertCollection(
        collection,
        tableCfg,
      );

      expect(componentsTable._data).toHaveLength(1);

      const row = componentsTable._data[0];
      expect(row._hash).toBeTruthy();
      expect(row.title).toBe('Sample Article');
      expect(row['author.name']).toBe('John Doe');
      expect(row['author.email']).toBe('john@example.com');
      expect(row.tags).toEqual(['tech', 'nodejs', 'mongodb']);
      expect(typeof row.publishedAt).toBe('number');
      expect(row.draft).toBe(false);
    });
  });
});
