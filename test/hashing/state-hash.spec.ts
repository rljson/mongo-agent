// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import type { Collection, Db, FindCursor } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';

import {
  computeStateCheckpoint, docLeafHash, DocWithHash, getLatestCheckpoint, MerklePartition,
  StateCheckpoint
} from '../../src/hashing/state-hash.ts';


describe('state-hash', () => {
  describe('docLeafHash', () => {
    it('returns null for null input', () => {
      expect(docLeafHash(null)).toBeNull();
    });

    it('returns null for undefined input', () => {
      expect(docLeafHash(undefined)).toBeNull();
    });

    it('returns stored __h field when present', () => {
      const doc: DocWithHash = {
        _id: 'test123',
        title: 'Test',
        __h: 'stored-hash-value',
      };
      expect(docLeafHash(doc)).toBe('stored-hash-value');
    });

    it('computes hash when __h field is missing', () => {
      const doc: DocWithHash = {
        _id: 'test456',
        title: 'Test Doc',
        count: 42,
      };
      const hash = docLeafHash(doc);
      expect(hash).toBeDefined();
      expect(hash).toHaveLength(64); // SHA-256 hex
    });

    it('produces same hash for same document without __h', () => {
      const doc1: DocWithHash = { _id: 'id1', value: 100 };
      const doc2: DocWithHash = { _id: 'id1', value: 100 };
      expect(docLeafHash(doc1)).toBe(docLeafHash(doc2));
    });

    it('ignores __h when computing hash from scratch', () => {
      const doc1: DocWithHash = { _id: 'id1', name: 'Test' };
      const doc2: DocWithHash = { _id: 'id1', name: 'Test', __h: undefined };
      // When __h is undefined, both should compute the same hash
      const hash1 = docLeafHash(doc1);
      const hash2 = docLeafHash(doc2);
      expect(hash1).toBe(hash2);
    });
  });

  describe('getLatestCheckpoint', () => {
    it('returns latest checkpoint when exists', async () => {
      const mockCheckpoint: StateCheckpoint = {
        _id: 'cp_test',
        ts: Date.now(),
        updatedAt: new Date().toISOString(),
        mode: 'full',
        partitionSize: 50000,
        dbRoot: 'abc123',
        collections: {},
        covers: null,
      };

      const mockCollection = {
        find: vi.fn().mockReturnValue({
          sort: vi.fn().mockReturnValue({
            limit: vi.fn().mockReturnValue({
              toArray: vi.fn().mockResolvedValue([mockCheckpoint]),
            }),
          }),
        }),
      } as unknown as Collection<StateCheckpoint>;

      const mockDb = {
        collection: vi.fn().mockReturnValue(mockCollection),
      } as unknown as Db;

      const result = await getLatestCheckpoint(mockDb);

      expect(result).toEqual(mockCheckpoint);
      expect(mockDb.collection).toHaveBeenCalledWith('state_checkpoints');
    });

    it('returns null when no checkpoints exist', async () => {
      const mockCollection = {
        find: vi.fn().mockReturnValue({
          sort: vi.fn().mockReturnValue({
            limit: vi.fn().mockReturnValue({
              toArray: vi.fn().mockResolvedValue([]),
            }),
          }),
        }),
      } as unknown as Collection<StateCheckpoint>;

      const mockDb = {
        collection: vi.fn().mockReturnValue(mockCollection),
      } as unknown as Db;

      const result = await getLatestCheckpoint(mockDb);

      expect(result).toBeNull();
    });
  });

  describe('computeStateCheckpoint', () => {
    it('computes checkpoint for empty database', async () => {
      const stateCheckpoints: StateCheckpoint[] = [];

      const mockDb = {
        listCollections: vi.fn().mockReturnValue({
          toArray: vi.fn().mockResolvedValue([]),
        }),
        collection: vi.fn((name: string) => {
          if (name === 'state_checkpoints') {
            return {
              insertOne: vi.fn(async (doc: StateCheckpoint) => {
                stateCheckpoints.push(doc);
                return { insertedId: doc._id };
              }),
            } as unknown as Collection<StateCheckpoint>;
          }
          return {} as Collection;
        }),
      } as unknown as Db;

      const result = await computeStateCheckpoint({ db: mockDb });

      expect(result).toBeDefined();
      expect(result.dbRoot).toBeDefined();
      expect(result.collections).toEqual({});
      expect(stateCheckpoints).toHaveLength(1);
      expect(stateCheckpoints[0]).toEqual(result);
    });

    it('computes checkpoint for single collection with documents', async () => {
      const docs: DocWithHash[] = [
        { _id: 'doc1', title: 'Test 1', __h: 'hash1' },
        { _id: 'doc2', title: 'Test 2', __h: 'hash2' },
        { _id: 'doc3', title: 'Test 3', __h: 'hash3' },
      ];

      const merklePartitions: MerklePartition[] = [];
      const stateCheckpoints: StateCheckpoint[] = [];

      const mockCursor = {
        [Symbol.asyncIterator]: async function* () {
          for (const doc of docs) {
            yield doc;
          }
        },
      } as unknown as FindCursor<DocWithHash>;

      const mockDb = {
        listCollections: vi.fn().mockReturnValue({
          toArray: vi.fn().mockResolvedValue([{ name: 'testcoll' }]),
        }),
        collection: vi.fn((name: string) => {
          if (name === 'testcoll') {
            return {
              find: vi.fn().mockReturnValue(mockCursor),
            } as unknown as Collection<DocWithHash>;
          }
          if (name === 'state_merkle') {
            return {
              replaceOne: vi.fn(async (_filter, doc: MerklePartition) => {
                merklePartitions.push(doc);
                return { modifiedCount: 1 };
              }),
            } as unknown as Collection<MerklePartition>;
          }
          if (name === 'state_checkpoints') {
            return {
              insertOne: vi.fn(async (doc: StateCheckpoint) => {
                stateCheckpoints.push(doc);
                return { insertedId: doc._id };
              }),
            } as unknown as Collection<StateCheckpoint>;
          }
          return {} as Collection;
        }),
      } as unknown as Db;

      const result = await computeStateCheckpoint({
        db: mockDb,
        partitionSize: 50000,
      });

      expect(result).toBeDefined();
      expect(result.collections.testcoll).toBeDefined();
      expect(result.collections.testcoll.partitions).toBe(1);
      expect(merklePartitions).toHaveLength(1);
      expect(merklePartitions[0].coll).toBe('testcoll');
      expect(merklePartitions[0].count).toBe(3);
      expect(stateCheckpoints).toHaveLength(1);
    });

    it('creates multiple partitions when partition size exceeded', async () => {
      // Create 5 docs with partition size of 2
      const docs: DocWithHash[] = [
        { _id: 'doc1', title: 'Test 1', __h: 'hash1' },
        { _id: 'doc2', title: 'Test 2', __h: 'hash2' },
        { _id: 'doc3', title: 'Test 3', __h: 'hash3' },
        { _id: 'doc4', title: 'Test 4', __h: 'hash4' },
        { _id: 'doc5', title: 'Test 5', __h: 'hash5' },
      ];

      const merklePartitions: MerklePartition[] = [];
      const stateCheckpoints: StateCheckpoint[] = [];

      const mockCursor = {
        [Symbol.asyncIterator]: async function* () {
          for (const doc of docs) {
            yield doc;
          }
        },
      } as unknown as FindCursor<DocWithHash>;

      const mockDb = {
        listCollections: vi.fn().mockReturnValue({
          toArray: vi.fn().mockResolvedValue([{ name: 'articles' }]),
        }),
        collection: vi.fn((name: string) => {
          if (name === 'articles') {
            return {
              find: vi.fn().mockReturnValue(mockCursor),
            } as unknown as Collection<DocWithHash>;
          }
          if (name === 'state_merkle') {
            return {
              replaceOne: vi.fn(async (_filter, doc: MerklePartition) => {
                merklePartitions.push(doc);
                return { modifiedCount: 1 };
              }),
            } as unknown as Collection<MerklePartition>;
          }
          if (name === 'state_checkpoints') {
            return {
              insertOne: vi.fn(async (doc: StateCheckpoint) => {
                stateCheckpoints.push(doc);
                return { insertedId: doc._id };
              }),
            } as unknown as Collection<StateCheckpoint>;
          }
          return {} as Collection;
        }),
      } as unknown as Db;

      const result = await computeStateCheckpoint({
        db: mockDb,
        partitionSize: 2, // Small partition size to force multiple partitions
      });

      expect(result).toBeDefined();
      expect(result.collections.articles).toBeDefined();
      expect(result.collections.articles.partitions).toBe(3); // 5 docs / 2 per partition = 3 partitions
      expect(merklePartitions).toHaveLength(3);
      expect(merklePartitions[0].count).toBe(2);
      expect(merklePartitions[1].count).toBe(2);
      expect(merklePartitions[2].count).toBe(1);
    });

    it('ignores system collections and specified collections', async () => {
      const mockDb = {
        listCollections: vi.fn().mockReturnValue({
          toArray: vi
            .fn()
            .mockResolvedValue([
              { name: 'users' },
              { name: 'system.indexes' },
              { name: 'sync_ops' },
              { name: 'articles' },
            ]),
        }),
        collection: vi.fn((name: string) => {
          if (name === 'users' || name === 'articles') {
            return {
              find: vi.fn().mockReturnValue({
                [Symbol.asyncIterator]: async function* () {
                  // Empty
                },
              }),
            } as unknown as Collection<DocWithHash>;
          }
          if (name === 'state_merkle') {
            return {
              replaceOne: vi.fn().mockResolvedValue({ modifiedCount: 1 }),
            } as unknown as Collection<MerklePartition>;
          }
          if (name === 'state_checkpoints') {
            return {
              insertOne: vi.fn().mockResolvedValue({ insertedId: 'test' }),
            } as unknown as Collection<StateCheckpoint>;
          }
          return {} as Collection;
        }),
      } as unknown as Db;

      const result = await computeStateCheckpoint({
        db: mockDb,
        ignoredColls: new Set(['sync_ops']),
      });

      expect(result.collections).toHaveProperty('users');
      expect(result.collections).toHaveProperty('articles');
      expect(result.collections).not.toHaveProperty('system.indexes');
      expect(result.collections).not.toHaveProperty('sync_ops');
    });

    it('uses correct mode and partitionSize in checkpoint', async () => {
      const stateCheckpoints: StateCheckpoint[] = [];

      const mockDb = {
        listCollections: vi.fn().mockReturnValue({
          toArray: vi.fn().mockResolvedValue([]),
        }),
        collection: vi.fn((name: string) => {
          if (name === 'state_checkpoints') {
            return {
              insertOne: vi.fn(async (doc: StateCheckpoint) => {
                stateCheckpoints.push(doc);
                return { insertedId: doc._id };
              }),
            } as unknown as Collection<StateCheckpoint>;
          }
          return {} as Collection;
        }),
      } as unknown as Db;

      const result = await computeStateCheckpoint({
        db: mockDb,
        mode: 'full',
        partitionSize: 10000,
      });

      expect(result.mode).toBe('full');
      expect(result.partitionSize).toBe(10000);
      expect(stateCheckpoints[0].mode).toBe('full');
      expect(stateCheckpoints[0].partitionSize).toBe(10000);
    });

    it('computes hashes for documents without __h field', async () => {
      // Documents without pre-computed __h field
      const docs: DocWithHash[] = [
        { _id: 'doc1', title: 'Test 1' }, // No __h field
        { _id: 'doc2', title: 'Test 2' }, // No __h field
      ];

      const merklePartitions: MerklePartition[] = [];
      const stateCheckpoints: StateCheckpoint[] = [];

      const mockCursor = {
        [Symbol.asyncIterator]: async function* () {
          for (const doc of docs) {
            yield doc;
          }
        },
      } as unknown as FindCursor<DocWithHash>;

      const mockDb = {
        listCollections: vi.fn().mockReturnValue({
          toArray: vi.fn().mockResolvedValue([{ name: 'nocol' }]),
        }),
        collection: vi.fn((name: string) => {
          if (name === 'nocol') {
            return {
              find: vi.fn().mockReturnValue(mockCursor),
            } as unknown as Collection<DocWithHash>;
          }
          if (name === 'state_merkle') {
            return {
              replaceOne: vi.fn(async (_filter, doc: MerklePartition) => {
                merklePartitions.push(doc);
                return { modifiedCount: 1 };
              }),
            } as unknown as Collection<MerklePartition>;
          }
          if (name === 'state_checkpoints') {
            return {
              insertOne: vi.fn(async (doc: StateCheckpoint) => {
                stateCheckpoints.push(doc);
                return { insertedId: doc._id };
              }),
            } as unknown as Collection<StateCheckpoint>;
          }
          return {} as Collection;
        }),
      } as unknown as Db;

      const result = await computeStateCheckpoint({ db: mockDb });

      expect(result).toBeDefined();
      expect(result.collections.nocol).toBeDefined();
      expect(merklePartitions[0].count).toBe(2);
      expect(merklePartitions[0].root).toBeDefined();
      expect(merklePartitions[0].root.length).toBe(64); // SHA-256 hex
    });
  });
});
