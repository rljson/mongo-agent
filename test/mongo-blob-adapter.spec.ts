// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { BsMem } from '@rljson/bs';

import { beforeEach, describe, expect, it } from 'vitest';

import { MongoBlobAdapter } from '../src/mongo-blob-adapter';


describe('MongoBlobAdapter', () => {
  let adapter: MongoBlobAdapter;
  let bs: BsMem;

  beforeEach(() => {
    bs = new BsMem();
    adapter = new MongoBlobAdapter(bs);
  });

  describe('constructor', () => {
    it('should create adapter with default blob storage', () => {
      const adapter = new MongoBlobAdapter();
      expect(adapter).toBeDefined();
      expect(adapter.bs).toBeDefined();
    });

    it('should create adapter with custom blob storage', () => {
      const customBs = new BsMem();
      const adapter = new MongoBlobAdapter(customBs);
      expect(adapter.bs).toBe(customBs);
    });
  });

  describe('documentToBlob', () => {
    it('should convert simple document to blob', async () => {
      const doc = { _id: 'user1', name: 'Alice' };
      const meta = await adapter.documentToBlob(doc, 'testdb', 'users');

      expect(meta).toBeDefined();
      expect(meta.docId).toBe('user1');
      expect(meta.collection).toBe('users');
      expect(meta.database).toBe('testdb');
      expect(meta.blobId).toBeDefined();
      expect(meta.size).toBeGreaterThan(0);
      expect(meta.mtime).toBeGreaterThan(0);
    });

    it('should convert complex document to blob', async () => {
      const doc = {
        _id: 'user1',
        name: 'Alice',
        address: {
          street: '123 Main St',
          city: 'NYC',
        },
        tags: ['admin', 'verified'],
      };

      const meta = await adapter.documentToBlob(doc, 'testdb', 'users');

      expect(meta.docId).toBe('user1');
      expect(meta.size).toBeGreaterThan(0);

      // Verify blob content
      const blob = await bs.getBlob(meta.blobId);
      const content = JSON.parse(blob.content.toString('utf-8'));
      expect(content).toEqual(doc);
    });

    it('should handle documents with special characters', async () => {
      const doc = {
        _id: 'user1',
        name: 'Alice "The Pro" O\'Brien',
        bio: 'Loves 日本語 and emojis 🎉',
      };

      const meta = await adapter.documentToBlob(doc, 'testdb', 'users');

      const blob = await bs.getBlob(meta.blobId);
      const content = JSON.parse(blob.content.toString('utf-8'));
      expect(content.name).toBe('Alice "The Pro" O\'Brien');
      expect(content.bio).toBe('Loves 日本語 and emojis 🎉');
    });

    it('should handle documents with numeric IDs', async () => {
      const doc = { _id: 12345, value: 'test' };
      const meta = await adapter.documentToBlob(doc, 'testdb', 'data');

      expect(meta.docId).toBe('12345');
    });

    it('should handle documents with ObjectId-like IDs', async () => {
      const doc = { _id: { $oid: '507f1f77bcf86cd799439011' }, value: 'test' };
      const meta = await adapter.documentToBlob(doc, 'testdb', 'data');

      expect(meta.docId).toBeDefined();
    });

    it('should calculate correct blob size', async () => {
      const doc = { _id: 'user1', name: 'Alice' };
      const meta = await adapter.documentToBlob(doc, 'testdb', 'users');

      const expectedSize = Buffer.byteLength(JSON.stringify(doc), 'utf-8');
      expect(meta.size).toBe(expectedSize);
    });

    it('should use custom blob storage from options', async () => {
      const customBs = new BsMem();
      const doc = { _id: 'user1', name: 'Alice' };

      const meta = await adapter.documentToBlob(doc, 'testdb', 'users', {
        bs: customBs,
      });

      // Blob should be in custom storage, not default
      const blob = await customBs.getBlob(meta.blobId);
      expect(blob).toBeDefined();
    });
  });

  describe('blobToDocument', () => {
    it('should convert blob back to document', async () => {
      const doc = { _id: 'user1', name: 'Alice', email: 'alice@example.com' };
      const meta = await adapter.documentToBlob(doc, 'testdb', 'users');

      const restored = await adapter.blobToDocument(meta);

      expect(restored).toEqual(doc);
    });

    it('should convert complex blob to document', async () => {
      const doc = {
        _id: 'user1',
        name: 'Alice',
        address: {
          street: '123 Main St',
          city: 'NYC',
        },
        tags: ['admin', 'verified'],
      };

      const meta = await adapter.documentToBlob(doc, 'testdb', 'users');
      const restored = await adapter.blobToDocument(meta);

      expect(restored).toEqual(doc);
    });

    it('should handle special characters in blob', async () => {
      const doc = {
        _id: 'user1',
        name: 'Alice "The Pro" O\'Brien',
        bio: 'Loves 日本語 and emojis 🎉',
      };

      const meta = await adapter.documentToBlob(doc, 'testdb', 'users');
      const restored = await adapter.blobToDocument(meta);

      expect(restored).toEqual(doc);
    });

    it('should use custom blob storage from options', async () => {
      const customBs = new BsMem();
      const doc = { _id: 'user1', name: 'Alice' };

      const meta = await adapter.documentToBlob(doc, 'testdb', 'users', {
        bs: customBs,
      });
      const restored = await adapter.blobToDocument(meta, { bs: customBs });

      expect(restored).toEqual(doc);
    });

    it('should throw error for non-existent blob', async () => {
      const fakeMeta = {
        docId: 'fake',
        collection: 'test',
        database: 'test',
        blobId: 'nonexistent-blob-id',
        size: 0,
        mtime: Date.now(),
      };
      await expect(adapter.blobToDocument(fakeMeta)).rejects.toThrow();
    });
  });

  describe('round-trip conversions', () => {
    it('should preserve document through round-trip', async () => {
      const doc = { _id: 'user1', name: 'Alice', age: 30, active: true };

      const meta = await adapter.documentToBlob(doc, 'testdb', 'users');
      const restored = await adapter.blobToDocument(meta);

      expect(restored).toEqual(doc);
    });

    it('should handle multiple documents', async () => {
      const docs = [
        { _id: 'user1', name: 'Alice' },
        { _id: 'user2', name: 'Bob' },
        { _id: 'user3', name: 'Charlie' },
      ];

      const metas = await Promise.all(
        docs.map((doc) => adapter.documentToBlob(doc, 'testdb', 'users')),
      );

      const restored = await Promise.all(
        metas.map((meta) => adapter.blobToDocument(meta)),
      );

      expect(restored).toEqual(docs);
    });

    it('should handle nested objects and arrays', async () => {
      const doc = {
        _id: 'order1',
        customer: {
          name: 'Alice',
          email: 'alice@example.com',
        },
        items: [
          { id: 1, name: 'Widget', quantity: 2 },
          { id: 2, name: 'Gadget', quantity: 1 },
        ],
        metadata: {
          created: new Date().toISOString(),
          status: 'pending',
        },
      };

      const meta = await adapter.documentToBlob(doc, 'testdb', 'orders');
      const restored = await adapter.blobToDocument(meta);

      expect(restored).toEqual(doc);
    });

    it('should handle null and undefined values', async () => {
      const doc = {
        _id: 'user1',
        name: 'Alice',
        nickname: null,
        description: undefined,
      };

      const meta = await adapter.documentToBlob(doc, 'testdb', 'users');
      const restored = await adapter.blobToDocument(meta);

      // undefined values are not preserved in JSON
      expect(restored._id).toBe('user1');
      expect(restored.name).toBe('Alice');
      expect(restored.nickname).toBe(null);
      expect('description' in restored).toBe(false);
    });

    it('should handle boolean values', async () => {
      const doc = {
        _id: 'user1',
        active: true,
        verified: false,
      };

      const meta = await adapter.documentToBlob(doc, 'testdb', 'users');
      const restored = await adapter.blobToDocument(meta);

      expect(restored.active).toBe(true);
      expect(restored.verified).toBe(false);
    });

    it('should handle numeric values', async () => {
      const doc = {
        _id: 'user1',
        age: 30,
        balance: 1234.56,
        count: 0,
      };

      const meta = await adapter.documentToBlob(doc, 'testdb', 'users');
      const restored = await adapter.blobToDocument(meta);

      expect(restored.age).toBe(30);
      expect(restored.balance).toBe(1234.56);
      expect(restored.count).toBe(0);
    });
  });

  describe('metadata accuracy', () => {
    it('should set accurate timestamp', async () => {
      const beforeTime = Date.now();
      const doc = { _id: 'user1', name: 'Alice' };
      const meta = await adapter.documentToBlob(doc, 'testdb', 'users');
      const afterTime = Date.now();

      expect(meta.mtime).toBeGreaterThanOrEqual(beforeTime);
      expect(meta.mtime).toBeLessThanOrEqual(afterTime);
    });

    it('should include all required metadata fields', async () => {
      const doc = { _id: 'user1', name: 'Alice' };
      const meta = await adapter.documentToBlob(doc, 'testdb', 'users');

      expect(meta).toHaveProperty('docId');
      expect(meta).toHaveProperty('collection');
      expect(meta).toHaveProperty('database');
      expect(meta).toHaveProperty('blobId');
      expect(meta).toHaveProperty('size');
      expect(meta).toHaveProperty('mtime');
    });
  });
});
