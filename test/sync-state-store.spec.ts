// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import type { Db } from 'mongodb';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  getState,
  markApplied,
  markPulled,
  type SyncState,
} from '../src/sync-state-store.ts';

describe('sync-state-store', () => {
  let mockDb: Db;
  let mockSyncStateCollection: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockSyncStateCollection = {
      findOne: vi.fn().mockResolvedValue(null),
      updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
    };

    mockDb = {
      collection: vi.fn((name: string) => {
        if (name === 'sync_state') {
          return mockSyncStateCollection;
        }
        return {
          findOne: vi.fn().mockResolvedValue(null),
        };
      }),
    } as unknown as Db;
  });

  describe('getState', () => {
    it('returns default state when no state exists', async () => {
      mockSyncStateCollection.findOne = vi.fn().mockResolvedValue(null);

      const result = await getState(mockDb, 'nodeA');

      expect(result).toEqual({
        _id: 'nodeA',
        lastSeqPulled: 0,
        lastHashPulled: 'GENESIS',
        lastSeqApplied: 0,
        lastHashApplied: 'GENESIS',
      });

      expect(mockSyncStateCollection.findOne).toHaveBeenCalledWith({
        _id: 'nodeA',
      });
    });

    it('returns existing state when found', async () => {
      const existingState: SyncState = {
        _id: 'nodeB',
        lastSeqPulled: 10,
        lastHashPulled: 'hash10',
        lastSeqApplied: 9,
        lastHashApplied: 'hash9',
        updatedAt: '2024-01-01T00:00:00.000Z',
      };

      mockSyncStateCollection.findOne = vi.fn().mockResolvedValue(existingState);

      const result = await getState(mockDb, 'nodeB');

      expect(result).toEqual(existingState);
      expect(mockSyncStateCollection.findOne).toHaveBeenCalledWith({
        _id: 'nodeB',
      });
    });

    it('handles different origin identifiers', async () => {
      const origins = ['node1', 'node2', 'peer-A', 'cluster-xyz'];

      for (const origin of origins) {
        mockSyncStateCollection.findOne = vi.fn().mockResolvedValue(null);

        const result = await getState(mockDb, origin);

        expect(result._id).toBe(origin);
        expect(mockSyncStateCollection.findOne).toHaveBeenCalledWith({
          _id: origin,
        });
      }
    });
  });

  describe('markPulled', () => {
    it('updates lastSeqPulled and lastHashPulled', async () => {
      await markPulled(mockDb, 'nodeA', 5, 'hash5');

      expect(mockSyncStateCollection.updateOne).toHaveBeenCalledWith(
        { _id: 'nodeA' },
        {
          $max: { lastSeqPulled: 5 },
          $set: {
            lastHashPulled: 'hash5',
            updatedAt: expect.any(String),
          },
        },
        { upsert: true }
      );
    });

    it('uses $max to prevent sequence number from decreasing', async () => {
      await markPulled(mockDb, 'nodeB', 3, 'hash3');

      const call = (mockSyncStateCollection.updateOne as ReturnType<typeof vi.fn>)
        .mock.calls[0];

      expect(call[1]).toHaveProperty('$max', { lastSeqPulled: 3 });
    });

    it('includes updatedAt timestamp', async () => {
      const beforeTime = new Date().toISOString();
      await markPulled(mockDb, 'nodeC', 7, 'hash7');
      const afterTime = new Date().toISOString();

      const call = (mockSyncStateCollection.updateOne as ReturnType<typeof vi.fn>)
        .mock.calls[0];
      const updatedAt = call[1].$set.updatedAt;

      expect(updatedAt).toBeDefined();
      expect(updatedAt >= beforeTime).toBe(true);
      expect(updatedAt <= afterTime).toBe(true);
    });

    it('uses upsert to create document if not exists', async () => {
      await markPulled(mockDb, 'newNode', 1, 'hash1');

      const call = (mockSyncStateCollection.updateOne as ReturnType<typeof vi.fn>)
        .mock.calls[0];

      expect(call[2]).toEqual({ upsert: true });
    });

    it('handles sequence 0', async () => {
      await markPulled(mockDb, 'nodeD', 0, 'GENESIS');

      expect(mockSyncStateCollection.updateOne).toHaveBeenCalledWith(
        { _id: 'nodeD' },
        {
          $max: { lastSeqPulled: 0 },
          $set: {
            lastHashPulled: 'GENESIS',
            updatedAt: expect.any(String),
          },
        },
        { upsert: true }
      );
    });

    it('handles large sequence numbers', async () => {
      const largeSeq = 999999;
      await markPulled(mockDb, 'nodeE', largeSeq, 'hash999999');

      expect(mockSyncStateCollection.updateOne).toHaveBeenCalledWith(
        { _id: 'nodeE' },
        expect.objectContaining({
          $max: { lastSeqPulled: largeSeq },
        }),
        { upsert: true }
      );
    });
  });

  describe('markApplied', () => {
    it('updates lastSeqApplied and lastHashApplied', async () => {
      await markApplied(mockDb, 'nodeA', 4, 'hash4');

      expect(mockSyncStateCollection.updateOne).toHaveBeenCalledWith(
        { _id: 'nodeA' },
        {
          $max: { lastSeqApplied: 4 },
          $set: {
            lastHashApplied: 'hash4',
            updatedAt: expect.any(String),
          },
        },
        { upsert: true }
      );
    });

    it('uses $max to prevent sequence number from decreasing', async () => {
      await markApplied(mockDb, 'nodeB', 2, 'hash2');

      const call = (mockSyncStateCollection.updateOne as ReturnType<typeof vi.fn>)
        .mock.calls[0];

      expect(call[1]).toHaveProperty('$max', { lastSeqApplied: 2 });
    });

    it('includes updatedAt timestamp', async () => {
      const beforeTime = new Date().toISOString();
      await markApplied(mockDb, 'nodeC', 6, 'hash6');
      const afterTime = new Date().toISOString();

      const call = (mockSyncStateCollection.updateOne as ReturnType<typeof vi.fn>)
        .mock.calls[0];
      const updatedAt = call[1].$set.updatedAt;

      expect(updatedAt).toBeDefined();
      expect(updatedAt >= beforeTime).toBe(true);
      expect(updatedAt <= afterTime).toBe(true);
    });

    it('uses upsert to create document if not exists', async () => {
      await markApplied(mockDb, 'newNode', 1, 'hash1');

      const call = (mockSyncStateCollection.updateOne as ReturnType<typeof vi.fn>)
        .mock.calls[0];

      expect(call[2]).toEqual({ upsert: true });
    });

    it('handles sequence 0', async () => {
      await markApplied(mockDb, 'nodeD', 0, 'GENESIS');

      expect(mockSyncStateCollection.updateOne).toHaveBeenCalledWith(
        { _id: 'nodeD' },
        {
          $max: { lastSeqApplied: 0 },
          $set: {
            lastHashApplied: 'GENESIS',
            updatedAt: expect.any(String),
          },
        },
        { upsert: true }
      );
    });

    it('handles large sequence numbers', async () => {
      const largeSeq = 888888;
      await markApplied(mockDb, 'nodeE', largeSeq, 'hash888888');

      expect(mockSyncStateCollection.updateOne).toHaveBeenCalledWith(
        { _id: 'nodeE' },
        expect.objectContaining({
          $max: { lastSeqApplied: largeSeq },
        }),
        { upsert: true }
      );
    });
  });

  describe('integration scenarios', () => {
    it('can mark pulled and applied separately', async () => {
      // Pull first
      await markPulled(mockDb, 'nodeX', 10, 'hash10');
      
      expect(mockSyncStateCollection.updateOne).toHaveBeenCalledWith(
        { _id: 'nodeX' },
        expect.objectContaining({
          $max: { lastSeqPulled: 10 },
          $set: expect.objectContaining({
            lastHashPulled: 'hash10',
          }),
        }),
        { upsert: true }
      );

      // Then mark applied
      await markApplied(mockDb, 'nodeX', 10, 'hash10');

      expect(mockSyncStateCollection.updateOne).toHaveBeenCalledWith(
        { _id: 'nodeX' },
        expect.objectContaining({
          $max: { lastSeqApplied: 10 },
          $set: expect.objectContaining({
            lastHashApplied: 'hash10',
          }),
        }),
        { upsert: true }
      );
    });

    it('handles pulled ahead of applied', async () => {
      // Pull seq 15
      await markPulled(mockDb, 'nodeY', 15, 'hash15');

      // Apply only up to seq 12
      await markApplied(mockDb, 'nodeY', 12, 'hash12');

      expect(mockSyncStateCollection.updateOne).toHaveBeenCalledTimes(2);
    });
  });
});
