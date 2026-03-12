// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it, vi } from 'vitest';
import type { Db, Collection } from 'mongodb';
import {
  clearDirtyForCollection,
  clearDirtyPartitions,
  ensureDirtyIndexes,
  listDirtyForCollection,
  markDirtyById,
  type DirtyDoc,
  type DirtyFullDoc,
  type DirtyPartitionDoc,
  type PartitionMeta,
} from '../../src/hashing/state-dirty.ts';

describe('state-dirty', () => {
  describe('ensureDirtyIndexes', () => {
    it('creates required indexes on state_dirty collection', async () => {
      const createIndexMock = vi.fn().mockResolvedValue('index_created');

      const mockCollection = {
        createIndex: createIndexMock,
      } as unknown as Collection;

      const mockDb = {
        collection: vi.fn().mockReturnValue(mockCollection),
      } as unknown as Db;

      await ensureDirtyIndexes(mockDb);

      expect(mockDb.collection).toHaveBeenCalledWith('state_dirty');
      expect(createIndexMock).toHaveBeenCalledTimes(2);
      expect(createIndexMock).toHaveBeenCalledWith({ coll: 1, partition: 1 });
      expect(createIndexMock).toHaveBeenCalledWith({ dirtyAt: 1 });
    });
  });

  describe('markDirtyById', () => {
    it('marks partition dirty when metadata exists', async () => {
      const updateOneCalls: Array<{
        filter: unknown;
        update: unknown;
        options: unknown;
      }> = [];

      const mockPartitionMeta: PartitionMeta = {
        partition: 3,
        minId: 'doc000',
        maxId: 'doc100',
      };

      const mockDb = {
        collection: vi.fn((name: string) => {
          if (name === 'state_merkle') {
            return {
              findOne: vi.fn().mockResolvedValue(mockPartitionMeta),
            } as unknown as Collection<PartitionMeta>;
          }
          if (name === 'state_dirty') {
            return {
              updateOne: vi.fn(async (filter, update, options) => {
                updateOneCalls.push({ filter, update, options });
                return { modifiedCount: 1 };
              }),
            } as unknown as Collection<DirtyPartitionDoc>;
          }
          return {} as Collection;
        }),
      } as unknown as Db;

      await markDirtyById(mockDb, 'articles', 'doc042');

      expect(updateOneCalls).toHaveLength(1);
      expect(updateOneCalls[0].filter).toEqual({ _id: 'articles::p3' });
      expect(updateOneCalls[0].update).toMatchObject({
        $set: {
          coll: 'articles',
          partition: 3,
        },
      });
      expect(updateOneCalls[0].options).toEqual({ upsert: true });
    });

    it('marks full collection dirty when partition not found', async () => {
      const updateOneCalls: Array<{
        filter: unknown;
        update: unknown;
        options: unknown;
      }> = [];

      const mockDb = {
        collection: vi.fn((name: string) => {
          if (name === 'state_merkle') {
            return {
              findOne: vi.fn().mockResolvedValue(null),
            } as unknown as Collection<PartitionMeta>;
          }
          if (name === 'state_dirty') {
            return {
              updateOne: vi.fn(async (filter, update, options) => {
                updateOneCalls.push({ filter, update, options });
                return { modifiedCount: 1 };
              }),
            } as unknown as Collection<DirtyFullDoc>;
          }
          return {} as Collection;
        }),
      } as unknown as Db;

      await markDirtyById(mockDb, 'newcoll', 'newdoc1');

      expect(updateOneCalls).toHaveLength(1);
      expect(updateOneCalls[0].filter).toEqual({ _id: 'newcoll::FULL' });
      expect(updateOneCalls[0].update).toMatchObject({
        $set: {
          coll: 'newcoll',
          full: true,
          reason: 'partition_not_found',
        },
      });
    });

    it('marks full collection dirty with custom reason', async () => {
      const updateOneCalls: Array<{ update: unknown }> = [];

      const mockDb = {
        collection: vi.fn((name: string) => {
          if (name === 'state_merkle') {
            return {
              findOne: vi.fn().mockResolvedValue(null),
            } as unknown as Collection<PartitionMeta>;
          }
          if (name === 'state_dirty') {
            return {
              updateOne: vi.fn(async (filter, update) => {
                updateOneCalls.push({ filter, update });
                return { modifiedCount: 1 };
              }),
            } as unknown as Collection<DirtyFullDoc>;
          }
          return {} as Collection;
        }),
      } as unknown as Db;

      await markDirtyById(mockDb, 'articles', 'doc1', {
        reason: 'manual_invalidation',
      });

      expect(updateOneCalls[0].update).toMatchObject({
        $set: {
          reason: 'manual_invalidation',
        },
      });
    });

    it('does nothing when collName is null', async () => {
      const mockDb = {
        collection: vi.fn(),
      } as unknown as Db;

      await markDirtyById(mockDb, null, 'doc1');

      expect(mockDb.collection).not.toHaveBeenCalled();
    });

    it('does nothing when collName is undefined', async () => {
      const mockDb = {
        collection: vi.fn(),
      } as unknown as Db;

      await markDirtyById(mockDb, undefined, 'doc1');

      expect(mockDb.collection).not.toHaveBeenCalled();
    });

    it('marks full collection dirty when partition field is missing', async () => {
      const updateOneCalls: Array<{ filter: unknown }> = [];

      const mockPartitionMeta = {
        minId: 'doc000',
        maxId: 'doc100',
        // partition field is missing
      };

      const mockDb = {
        collection: vi.fn((name: string) => {
          if (name === 'state_merkle') {
            return {
              findOne: vi.fn().mockResolvedValue(mockPartitionMeta),
            } as unknown as Collection;
          }
          if (name === 'state_dirty') {
            return {
              updateOne: vi.fn(async (filter) => {
                updateOneCalls.push({ filter });
                return { modifiedCount: 1 };
              }),
            } as unknown as Collection<DirtyFullDoc>;
          }
          return {} as Collection;
        }),
      } as unknown as Db;

      await markDirtyById(mockDb, 'articles', 'doc1');

      expect(updateOneCalls[0].filter).toEqual({ _id: 'articles::FULL' });
    });

    it('handles findPartitionForId errors gracefully', async () => {
      const updateOneCalls: Array<{ filter: unknown }> = [];

      const mockDb = {
        collection: vi.fn((name: string) => {
          if (name === 'state_merkle') {
            return {
              findOne: vi
                .fn()
                .mockRejectedValue(new Error('Database connection lost')),
            } as unknown as Collection<PartitionMeta>;
          }
          if (name === 'state_dirty') {
            return {
              updateOne: vi.fn(async (filter) => {
                updateOneCalls.push({ filter });
                return { modifiedCount: 1 };
              }),
            } as unknown as Collection<DirtyFullDoc>;
          }
          return {} as Collection;
        }),
      } as unknown as Db;

      await markDirtyById(mockDb, 'articles', 'doc1');

      // Should fall back to marking full collection dirty
      expect(updateOneCalls[0].filter).toEqual({ _id: 'articles::FULL' });
    });
  });

  describe('listDirtyForCollection', () => {
    it('returns empty status when no dirty markers exist', async () => {
      const mockCollection = {
        find: vi.fn().mockReturnValue({
          project: vi.fn().mockReturnValue({
            toArray: vi.fn().mockResolvedValue([]),
          }),
        }),
      } as unknown as Collection<DirtyDoc>;

      const mockDb = {
        collection: vi.fn().mockReturnValue(mockCollection),
      } as unknown as Db;

      const result = await listDirtyForCollection(mockDb, 'articles');

      expect(result).toEqual({ full: false, partitions: [] });
    });

    it('returns partition list when partitions are dirty', async () => {
      const dirtyDocs: DirtyPartitionDoc[] = [
        { _id: 'articles::p2', coll: 'articles', partition: 2, dirtyAt: '2024-01-01' },
        { _id: 'articles::p5', coll: 'articles', partition: 5, dirtyAt: '2024-01-01' },
        { _id: 'articles::p1', coll: 'articles', partition: 1, dirtyAt: '2024-01-01' },
      ];

      const mockCollection = {
        find: vi.fn().mockReturnValue({
          project: vi.fn().mockReturnValue({
            toArray: vi.fn().mockResolvedValue(dirtyDocs),
          }),
        }),
      } as unknown as Collection<DirtyDoc>;

      const mockDb = {
        collection: vi.fn().mockReturnValue(mockCollection),
      } as unknown as Db;

      const result = await listDirtyForCollection(mockDb, 'articles');

      expect(result.full).toBe(false);
      expect(result.partitions).toEqual([1, 2, 5]); // Sorted
    });

    it('returns full=true when full marker exists', async () => {
      const dirtyDocs: DirtyDoc[] = [
        { _id: 'articles::FULL', coll: 'articles', full: true, dirtyAt: '2024-01-01' },
        { _id: 'articles::p2', coll: 'articles', partition: 2, dirtyAt: '2024-01-01' },
      ];

      const mockCollection = {
        find: vi.fn().mockReturnValue({
          project: vi.fn().mockReturnValue({
            toArray: vi.fn().mockResolvedValue(dirtyDocs),
          }),
        }),
      } as unknown as Collection<DirtyDoc>;

      const mockDb = {
        collection: vi.fn().mockReturnValue(mockCollection),
      } as unknown as Db;

      const result = await listDirtyForCollection(mockDb, 'articles');

      expect(result.full).toBe(true);
      expect(result.partitions).toEqual([2]);
    });

    it('filters out invalid partition numbers', async () => {
      const dirtyDocs = [
        { _id: 'articles::p2', coll: 'articles', partition: 2, dirtyAt: '2024-01-01' },
        { _id: 'articles::invalid', coll: 'articles', partition: 'invalid', dirtyAt: '2024-01-01' },
        { _id: 'articles::p5', coll: 'articles', partition: 5, dirtyAt: '2024-01-01' },
      ];

      const mockCollection = {
        find: vi.fn().mockReturnValue({
          project: vi.fn().mockReturnValue({
            toArray: vi.fn().mockResolvedValue(dirtyDocs),
          }),
        }),
      } as unknown as Collection<DirtyDoc>;

      const mockDb = {
        collection: vi.fn().mockReturnValue(mockCollection),
      } as unknown as Db;

      const result = await listDirtyForCollection(mockDb, 'articles');

      expect(result.partitions).toEqual([2, 5]);
    });
  });

  describe('clearDirtyForCollection', () => {
    it('deletes all dirty markers for collection', async () => {
      const deleteManyCalls: Array<{ filter: unknown }> = [];

      const mockCollection = {
        deleteMany: vi.fn(async (filter) => {
          deleteManyCalls.push({ filter });
          return { deletedCount: 5 };
        }),
      } as unknown as Collection;

      const mockDb = {
        collection: vi.fn().mockReturnValue(mockCollection),
      } as unknown as Db;

      await clearDirtyForCollection(mockDb, 'articles');

      expect(deleteManyCalls).toHaveLength(1);
      expect(deleteManyCalls[0].filter).toEqual({ coll: 'articles' });
    });
  });

  describe('clearDirtyPartitions', () => {
    it('deletes specified partition markers', async () => {
      const deleteManyCalls: Array<{ filter: unknown }> = [];

      const mockCollection = {
        deleteMany: vi.fn(async (filter) => {
          deleteManyCalls.push({ filter });
          return { deletedCount: 3 };
        }),
      } as unknown as Collection;

      const mockDb = {
        collection: vi.fn().mockReturnValue(mockCollection),
      } as unknown as Db;

      await clearDirtyPartitions(mockDb, 'articles', [1, 3, 5]);

      expect(deleteManyCalls).toHaveLength(1);
      expect(deleteManyCalls[0].filter).toEqual({
        _id: { $in: ['articles::p1', 'articles::p3', 'articles::p5'] },
      });
    });

    it('does nothing when partition list is empty', async () => {
      const mockCollection = {
        deleteMany: vi.fn(),
      } as unknown as Collection;

      const mockDb = {
        collection: vi.fn().mockReturnValue(mockCollection),
      } as unknown as Db;

      await clearDirtyPartitions(mockDb, 'articles', []);

      expect(mockCollection.deleteMany).not.toHaveBeenCalled();
    });

    it('does nothing when partition list is null', async () => {
      const mockCollection = {
        deleteMany: vi.fn(),
      } as unknown as Collection;

      const mockDb = {
        collection: vi.fn().mockReturnValue(mockCollection),
      } as unknown as Db;

      await clearDirtyPartitions(mockDb, 'articles', null as never);

      expect(mockCollection.deleteMany).not.toHaveBeenCalled();
    });
  });
});
