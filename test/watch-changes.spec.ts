// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import type { ChangeStreamDocument, Db } from 'mongodb';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
  createSuppressor, isInternalCollection, startDbChangeStream
} from '../src/watch-changes.ts';


describe('watch-changes', () => {
  describe('createSuppressor', () => {
    it('adds and checks entries', () => {
      const suppressor = createSuppressor();
      const ns = { db: 'testdb', coll: 'users' };
      const id = '123';

      expect(suppressor.has(ns, id)).toBe(false);

      suppressor.add(ns, id);
      expect(suppressor.has(ns, id)).toBe(true);
    });

    it('expires entries after TTL', async () => {
      const ttl = 100; // 100ms
      const suppressor = createSuppressor(ttl);
      const ns = { db: 'testdb', coll: 'users' };
      const id = '456';

      suppressor.add(ns, id);
      expect(suppressor.has(ns, id)).toBe(true);

      // Wait for expiration
      await new Promise((resolve) => setTimeout(resolve, ttl + 50));

      expect(suppressor.has(ns, id)).toBe(false);
    });

    it('handles different namespaces separately', () => {
      const suppressor = createSuppressor();
      const ns1 = { db: 'db1', coll: 'coll1' };
      const ns2 = { db: 'db2', coll: 'coll2' };
      const id = 'same-id';

      suppressor.add(ns1, id);

      expect(suppressor.has(ns1, id)).toBe(true);
      expect(suppressor.has(ns2, id)).toBe(false);
    });

    it('handles different IDs separately', () => {
      const suppressor = createSuppressor();
      const ns = { db: 'testdb', coll: 'users' };

      suppressor.add(ns, 'id1');
      suppressor.add(ns, 'id2');

      expect(suppressor.has(ns, 'id1')).toBe(true);
      expect(suppressor.has(ns, 'id2')).toBe(true);
      expect(suppressor.has(ns, 'id3')).toBe(false);
    });

    it('cleans up expired entries on access', async () => {
      const ttl = 50;
      const suppressor = createSuppressor(ttl);
      const ns = { db: 'testdb', coll: 'users' };

      // Add multiple entries
      suppressor.add(ns, 'id1');
      await new Promise((resolve) => setTimeout(resolve, 30));
      suppressor.add(ns, 'id2');

      // Wait for first entry to expire
      await new Promise((resolve) => setTimeout(resolve, 30));

      // Check triggers cleanup - id1 should be removed, id2 still valid
      expect(suppressor.has(ns, 'id2')).toBe(true);
      expect(suppressor.has(ns, 'id1')).toBe(false);
    });

    it('handles namespace with undefined properties', () => {
      const suppressor = createSuppressor();
      const ns1 = { db: undefined, coll: undefined } as unknown as {
        db: string;
        coll: string;
      };
      const ns2 = { db: 'testdb', coll: undefined } as unknown as {
        db: string;
        coll: string;
      };
      const id = 'test-id';

      suppressor.add(ns1, id);
      expect(suppressor.has(ns1, id)).toBe(true);
      expect(suppressor.has(ns2, id)).toBe(false);
    });
  });

  describe('isInternalCollection', () => {
    it('returns true for undefined collection', () => {
      expect(isInternalCollection(undefined)).toBe(true);
    });

    it('returns true for system collections', () => {
      expect(isInternalCollection('system.users')).toBe(true);
      expect(isInternalCollection('system.views')).toBe(true);
    });

    it('returns true for sync_ prefixed collections', () => {
      expect(isInternalCollection('sync_ops')).toBe(true);
      expect(isInternalCollection('sync_state')).toBe(true);
      expect(isInternalCollection('sync_custom')).toBe(true);
    });

    it('returns true for state_ prefixed collections', () => {
      expect(isInternalCollection('state_checkpoints')).toBe(true);
      expect(isInternalCollection('state_merkle')).toBe(true);
    });

    it('returns true for exact match internal collections', () => {
      expect(isInternalCollection('sync_ops')).toBe(true);
      expect(isInternalCollection('sync_resume')).toBe(true);
      expect(isInternalCollection('sync_conflicts')).toBe(true);
    });

    it('returns false for user collections', () => {
      expect(isInternalCollection('users')).toBe(false);
      expect(isInternalCollection('articles')).toBe(false);
      expect(isInternalCollection('products')).toBe(false);
    });
  });

  describe('startDbChangeStream', () => {
    let mockDb: Db;
    let mockChangeStream: {
      on: ReturnType<typeof vi.fn>;
      close: ReturnType<typeof vi.fn>;
    };
    let changeHandlers: Map<string, (data: unknown) => void>;

    beforeEach(() => {
      vi.clearAllMocks();
      changeHandlers = new Map();

      mockChangeStream = {
        on: vi.fn((event: string, handler: (data: unknown) => void) => {
          changeHandlers.set(event, handler);
          return mockChangeStream;
        }),
        close: vi.fn(),
      };

      const mockSyncOps = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        findOne: vi.fn().mockResolvedValue(null),
      };

      const mockSyncLocal = {
        findOne: vi.fn().mockResolvedValue({ seq: 0, headHash: 'GENESIS' }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        deleteOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb = {
        collection: vi.fn((name: string) => {
          if (name === 'sync_ops') return mockSyncOps;
          if (name === 'sync_local') return mockSyncLocal;
          if (name === 'sync_resume') return mockSyncResume;
          return mockSyncOps;
        }),
        watch: vi.fn().mockReturnValue(mockChangeStream),
      } as unknown as Db;
    });

    it('starts change stream without resume token', async () => {
      const logger = {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
        logger,
      });

      expect(mockDb.watch).toHaveBeenCalledWith(
        [
          {
            $match: {
              operationType: {
                $in: ['insert', 'update', 'replace', 'delete'],
              },
            },
          },
        ],
        {
          fullDocument: 'updateLookup',
        },
      );

      expect(logger.info).toHaveBeenCalledWith(
        { resumeAfter: false },
        'DB change stream started (SERIAL queue enabled)',
      );
    });

    it('starts change stream with resume token', async () => {
      const resumeToken = { _data: 'resume123' };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue({ token: resumeToken }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        deleteOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_resume') return mockSyncResume;
        return {
          insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
          findOne: vi.fn().mockResolvedValue({ seq: 0, headHash: 'GENESIS' }),
          updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        };
      }) as unknown as typeof mockDb.collection;

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
      });

      expect(mockDb.watch).toHaveBeenCalledWith(
        expect.any(Array),
        expect.objectContaining({
          resumeAfter: resumeToken,
        }),
      );
    });

    it('handles invalid resume token by starting fresh', async () => {
      const invalidToken = { _data: 'invalid' };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue({ token: invalidToken }),
        deleteOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      let watchCallCount = 0;
      mockDb = {
        collection: vi.fn((name: string) => {
          if (name === 'sync_resume') return mockSyncResume;
          return {
            insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
            findOne: vi.fn().mockResolvedValue({ seq: 0, headHash: 'GENESIS' }),
            updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
          };
        }),
        watch: vi.fn(() => {
          watchCallCount++;
          if (watchCallCount === 1) {
            throw new Error('resume token invalid');
          }
          return mockChangeStream;
        }),
      } as unknown as Db;

      const logger = {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
        logger,
      });

      expect(mockSyncResume.deleteOne).toHaveBeenCalledWith({ _id: 'resume' });
      expect(logger.warn).toHaveBeenCalled();
      expect(mockDb.watch).toHaveBeenCalledTimes(2);
    });

    it('processes insert change event', async () => {
      const mockSyncOps = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncLocal = {
        findOne: vi.fn().mockResolvedValue({ seq: 5, headHash: 'hash123' }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_ops') return mockSyncOps;
        if (name === 'sync_local') return mockSyncLocal;
        if (name === 'sync_resume') return mockSyncResume;
        return mockSyncOps;
      }) as unknown as typeof mockDb.collection;

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
      });

      const changeEvent: ChangeStreamDocument = {
        _id: { _data: 'change1' },
        operationType: 'insert',
        ns: { db: 'testdb', coll: 'users' },
        documentKey: { _id: 'user123' },
        fullDocument: { _id: 'user123', name: 'Alice' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      expect(changeHandler).toBeDefined();

      changeHandler!(changeEvent);

      // Wait for async processing
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockSyncOps.insertOne).toHaveBeenCalled();
      expect(mockSyncResume.updateOne).toHaveBeenCalledWith(
        { _id: 'resume' },
        {
          $set: {
            token: changeEvent._id,
            updatedAt: expect.any(String),
          },
        },
        { upsert: true },
      );
    });

    it('ignores internal collections', async () => {
      const mockCollection = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn(
        () => mockCollection,
      ) as unknown as typeof mockDb.collection;

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
      });

      const changeEvent: ChangeStreamDocument = {
        _id: { _data: 'change2' },
        operationType: 'insert',
        ns: { db: 'testdb', coll: 'sync_ops' },
        documentKey: { _id: 'op123' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockCollection.insertOne).not.toHaveBeenCalled();
    });

    it('ignores changes without document ID', async () => {
      const mockCollection = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn(
        () => mockCollection,
      ) as unknown as typeof mockDb.collection;

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
      });

      const changeEvent = {
        _id: { _data: 'change3' },
        operationType: 'insert',
        ns: { db: 'testdb', coll: 'users' },
        // No documentKey or fullDocument
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockCollection.insertOne).not.toHaveBeenCalled();
    });

    it('uses suppressor to ignore echo changes', async () => {
      const suppressor = createSuppressor();
      const mockCollection = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn(
        () => mockCollection,
      ) as unknown as typeof mockDb.collection;

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
        suppressor,
      });

      const ns = { db: 'testdb', coll: 'users' };
      const docId = 'user456';

      // Add to suppressor
      suppressor.add(ns, docId);

      const changeEvent: ChangeStreamDocument = {
        _id: { _data: 'change4' },
        operationType: 'update',
        ns,
        documentKey: { _id: docId },
        fullDocument: { _id: docId, name: 'Bob' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockCollection.insertOne).not.toHaveBeenCalled();
    });

    it('ignores changes without namespace', async () => {
      const mockCollection = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn(
        () => mockCollection,
      ) as unknown as typeof mockDb.collection;

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
      });

      const changeEvent = {
        _id: { _data: 'change_no_ns' },
        operationType: 'insert',
        // No ns field
        documentKey: { _id: 'doc123' },
        fullDocument: { _id: 'doc123', value: 'test' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockCollection.insertOne).not.toHaveBeenCalled();
    });

    it('handles errors in change processing', async () => {
      const mockSyncOps = {
        insertOne: vi.fn().mockRejectedValue(new Error('Insert failed')),
      };
      const mockSyncLocal = {
        findOne: vi.fn().mockResolvedValue({ seq: 0, headHash: 'GENESIS' }),
        updateOne: vi.fn(),
      };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn(),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_ops') return mockSyncOps;
        if (name === 'sync_local') return mockSyncLocal;
        if (name === 'sync_resume') return mockSyncResume;
        return mockSyncOps;
      }) as unknown as typeof mockDb.collection;

      const logger = {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
        logger,
      });

      const changeEvent: ChangeStreamDocument = {
        _id: { _data: 'change5' },
        operationType: 'insert',
        ns: { db: 'testdb', coll: 'users' },
        documentKey: { _id: 'user789' },
        fullDocument: { _id: 'user789', name: 'Charlie' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 100));

      expect(logger.error).toHaveBeenCalledWith(
        { err: expect.any(String) },
        'serial queue task failed',
      );
    });

    it('handles change stream errors', async () => {
      const logger = {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
        logger,
      });

      const errorHandler = changeHandlers.get('error');
      expect(errorHandler).toBeDefined();

      const error = new Error('Stream error');
      errorHandler!(error);

      expect(logger.error).toHaveBeenCalledWith(
        { err: 'Stream error' },
        'change stream error',
      );
    });

    it('retries on duplicate key error', async () => {
      let insertAttempts = 0;
      const mockSyncOps = {
        insertOne: vi.fn().mockImplementation(() => {
          insertAttempts++;
          if (insertAttempts === 1) {
            const error = new Error('Duplicate key') as Error & {
              code?: number;
            };
            error.code = 11000;
            return Promise.reject(error);
          }
          return Promise.resolve({ acknowledged: true });
        }),
      };
      const mockSyncLocal = {
        findOne: vi
          .fn()
          .mockResolvedValueOnce({ seq: 5, headHash: 'hash1' })
          .mockResolvedValueOnce({ seq: 6, headHash: 'hash2' }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_ops') return mockSyncOps;
        if (name === 'sync_local') return mockSyncLocal;
        if (name === 'sync_resume') return mockSyncResume;
        return mockSyncOps;
      }) as unknown as typeof mockDb.collection;

      const logger = {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
        logger,
      });

      const changeEvent: ChangeStreamDocument = {
        _id: { _data: 'change6' },
        operationType: 'insert',
        ns: { db: 'testdb', coll: 'users' },
        documentKey: { _id: 'user999' },
        fullDocument: { _id: 'user999', name: 'Dave' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 100));

      expect(mockSyncOps.insertOne).toHaveBeenCalledTimes(2);
      expect(logger.warn).toHaveBeenCalledWith(
        expect.objectContaining({
          err: 'Duplicate key',
          attempt: 1,
        }),
        'duplicate key on sync_ops insert; retrying',
      );
    });

    it('fails after max retries on duplicate key', async () => {
      const mockSyncOps = {
        insertOne: vi.fn().mockImplementation(() => {
          const error = new Error('Duplicate key') as Error & { code?: number };
          error.code = 11000;
          return Promise.reject(error);
        }),
      };
      const mockSyncLocal = {
        findOne: vi.fn().mockResolvedValue({ seq: 5, headHash: 'hash1' }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_ops') return mockSyncOps;
        if (name === 'sync_local') return mockSyncLocal;
        if (name === 'sync_resume') return mockSyncResume;
        return mockSyncOps;
      }) as unknown as typeof mockDb.collection;

      const logger = {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
        logger,
      });

      const changeEvent: ChangeStreamDocument = {
        _id: { _data: 'change7' },
        operationType: 'insert',
        ns: { db: 'testdb', coll: 'users' },
        documentKey: { _id: 'user111' },
        fullDocument: { _id: 'user111', name: 'Eve' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 150));

      expect(mockSyncOps.insertOne).toHaveBeenCalledTimes(5); // MAX_RETRIES
      expect(logger.error).toHaveBeenCalledWith(
        { err: 'Duplicate key' },
        'serial queue task failed',
      );
    });

    it('handles error when reading resume token', async () => {
      const mockSyncResume = {
        findOne: vi.fn().mockRejectedValue(new Error('Read error')),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_resume') return mockSyncResume;
        return {
          insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
          findOne: vi.fn().mockResolvedValue({ seq: 0, headHash: 'GENESIS' }),
          updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        };
      }) as unknown as typeof mockDb.collection;

      // Should not throw - error is caught and null is used
      await expect(
        startDbChangeStream({
          db: mockDb,
          nodeId: 'node1',
        }),
      ).resolves.toBeDefined();
    });

    it('rethrows non-resume errors from watch', async () => {
      mockDb.watch = vi.fn(() => {
        throw new Error('Connection failed');
      }) as unknown as typeof mockDb.watch;

      await expect(
        startDbChangeStream({
          db: mockDb,
          nodeId: 'node1',
        }),
      ).rejects.toThrow('Connection failed');
    });

    it('works without logger', async () => {
      const mockCollection = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn(
        () => mockCollection,
      ) as unknown as typeof mockDb.collection;

      // Should not throw without logger
      await expect(
        startDbChangeStream({
          db: mockDb,
          nodeId: 'node1',
          // no logger
        }),
      ).resolves.toBeDefined();
    });

    it('works with partial logger (missing methods)', async () => {
      const mockCollection = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn(
        () => mockCollection,
      ) as unknown as typeof mockDb.collection;

      // Logger with only some methods
      const partialLogger = {
        info: vi.fn(),
        // warn and error are missing
      };

      await expect(
        startDbChangeStream({
          db: mockDb,
          nodeId: 'node1',
          logger: partialLogger,
        }),
      ).resolves.toBeDefined();
    });

    it('extracts docId from fullDocument when documentKey missing _id', async () => {
      const mockSyncOps = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncLocal = {
        findOne: vi.fn().mockResolvedValue({ seq: 0, headHash: 'GENESIS' }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_ops') return mockSyncOps;
        if (name === 'sync_local') return mockSyncLocal;
        if (name === 'sync_resume') return mockSyncResume;
        return mockSyncOps;
      }) as unknown as typeof mockDb.collection;

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
      });

      const changeEvent = {
        _id: { _data: 'change8' },
        operationType: 'insert',
        ns: { db: 'testdb', coll: 'users' },
        documentKey: {}, // No _id in documentKey
        fullDocument: { _id: 'user222', name: 'Frank' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockSyncOps.insertOne).toHaveBeenCalled();
    });

    it('handles changes without fullDocument or updateDescription', async () => {
      const mockSyncOps = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncLocal = {
        findOne: vi.fn().mockResolvedValue({ seq: 0, headHash: 'GENESIS' }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_ops') return mockSyncOps;
        if (name === 'sync_local') return mockSyncLocal;
        if (name === 'sync_resume') return mockSyncResume;
        return mockSyncOps;
      }) as unknown as typeof mockDb.collection;

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
      });

      const changeEvent = {
        _id: { _data: 'change9' },
        operationType: 'delete',
        ns: { db: 'testdb', coll: 'users' },
        documentKey: { _id: 'user333' },
        // No fullDocument or updateDescription
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockSyncOps.insertOne).toHaveBeenCalledWith(
        expect.objectContaining({
          payload: {
            fullDocument: null,
            updateDescription: null,
          },
        }),
      );
    });

    it('handles changes without _id in change event', async () => {
      const mockSyncOps = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncLocal = {
        findOne: vi.fn().mockResolvedValue({ seq: 0, headHash: 'GENESIS' }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_ops') return mockSyncOps;
        if (name === 'sync_local') return mockSyncLocal;
        if (name === 'sync_resume') return mockSyncResume;
        return mockSyncOps;
      }) as unknown as typeof mockDb.collection;

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
      });

      const changeEvent = {
        // No _id field
        operationType: 'insert',
        ns: { db: 'testdb', coll: 'users' },
        documentKey: { _id: 'user444' },
        fullDocument: { _id: 'user444', name: 'Grace' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockSyncOps.insertOne).toHaveBeenCalled();
      // Resume token should NOT be updated because change._id is missing
      expect(mockSyncResume.updateOne).not.toHaveBeenCalled();
    });

    it('handles deleteOne failure when clearing invalid resume token', async () => {
      const resumeToken = { _data: 'invalid' };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue({ token: resumeToken }),
        deleteOne: vi.fn().mockRejectedValue(new Error('Delete failed')),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      let watchCallCount = 0;
      mockDb = {
        collection: vi.fn((name: string) => {
          if (name === 'sync_resume') return mockSyncResume;
          return {
            insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
            findOne: vi.fn().mockResolvedValue({ seq: 0, headHash: 'GENESIS' }),
            updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
          };
        }),
        watch: vi.fn(() => {
          watchCallCount++;
          if (watchCallCount === 1) {
            throw new Error('resume token invalid');
          }
          return mockChangeStream;
        }),
      } as unknown as Db;

      const logger = {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      // Should not throw despite deleteOne failure
      await expect(
        startDbChangeStream({
          db: mockDb,
          nodeId: 'node1',
          logger,
        }),
      ).resolves.toBeDefined();

      expect(mockSyncResume.deleteOne).toHaveBeenCalled();
    });

    it('handles sequential task failures in serial queue', async () => {
      let insertAttempts = 0;
      const mockSyncOps = {
        insertOne: vi.fn().mockImplementation(async () => {
          insertAttempts++;
          if (insertAttempts === 1) {
            throw new Error('First insert failed');
          }
          return { acknowledged: true };
        }),
      };
      const mockSyncLocal = {
        findOne: vi
          .fn()
          .mockResolvedValueOnce({ seq: 0, headHash: 'GENESIS' })
          .mockResolvedValueOnce({ seq: 1, headHash: 'hash1' }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_ops') return mockSyncOps;
        if (name === 'sync_local') return mockSyncLocal;
        if (name === 'sync_resume') return mockSyncResume;
        return mockSyncOps;
      }) as unknown as typeof mockDb.collection;

      const logger = {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
        logger,
      });

      // Send two change events
      const changeEvent1: ChangeStreamDocument = {
        _id: { _data: 'change1' },
        operationType: 'insert',
        ns: { db: 'testdb', coll: 'users' },
        documentKey: { _id: 'user1' },
        fullDocument: { _id: 'user1', name: 'Alice' },
      } as ChangeStreamDocument;

      const changeEvent2: ChangeStreamDocument = {
        _id: { _data: 'change2' },
        operationType: 'insert',
        ns: { db: 'testdb', coll: 'users' },
        documentKey: { _id: 'user2' },
        fullDocument: { _id: 'user2', name: 'Bob' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent1);
      changeHandler!(changeEvent2);

      // Wait for both to process
      await new Promise((resolve) => setTimeout(resolve, 150));

      // First should fail, second should succeed (serial queue continues)
      expect(logger.error).toHaveBeenCalledWith(
        { err: 'First insert failed' },
        'serial queue task failed',
      );
      expect(mockSyncOps.insertOne).toHaveBeenCalledTimes(2);
    });

    it('handles missing seq and headHash in sync_local', async () => {
      const mockSyncOps = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncLocal = {
        findOne: vi.fn().mockResolvedValue({ seq: 0, headHash: null }), // Falsy headHash
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_ops') return mockSyncOps;
        if (name === 'sync_local') return mockSyncLocal;
        if (name === 'sync_resume') return mockSyncResume;
        return mockSyncOps;
      }) as unknown as typeof mockDb.collection;

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
      });

      const changeEvent: ChangeStreamDocument = {
        _id: { _data: 'change10' },
        operationType: 'insert',
        ns: { db: 'testdb', coll: 'users' },
        documentKey: { _id: 'user10' },
        fullDocument: { _id: 'user10', name: 'Test' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockSyncOps.insertOne).toHaveBeenCalledWith(
        expect.objectContaining({
          seq: 1,
          prevHash: 'GENESIS', // Should default to GENESIS when headHash is falsy
        }),
      );
    });

    it('handles null sync_local document', async () => {
      const mockSyncOps = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncLocal = {
        findOne: vi.fn().mockResolvedValue(null), // No local document
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_ops') return mockSyncOps;
        if (name === 'sync_local') return mockSyncLocal;
        if (name === 'sync_resume') return mockSyncResume;
        return mockSyncOps;
      }) as unknown as typeof mockDb.collection;

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
      });

      const changeEvent: ChangeStreamDocument = {
        _id: { _data: 'change11' },
        operationType: 'insert',
        ns: { db: 'testdb', coll: 'users' },
        documentKey: { _id: 'user11' },
        fullDocument: { _id: 'user11', name: 'Test' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockSyncOps.insertOne).toHaveBeenCalledWith(
        expect.objectContaining({
          seq: 1,
          prevHash: 'GENESIS',
        }),
      );
    });

    it('handles change event with empty db and coll strings', async () => {
      const mockSyncOps = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncLocal = {
        findOne: vi.fn().mockResolvedValue({ seq: 0, headHash: 'GENESIS' }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_ops') return mockSyncOps;
        if (name === 'sync_local') return mockSyncLocal;
        if (name === 'sync_resume') return mockSyncResume;
        if (name === '') return mockSyncOps; // Empty coll name
        return mockSyncOps;
      }) as unknown as typeof mockDb.collection;

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
      });

      const changeEvent = {
        _id: { _data: 'change12' },
        operationType: 'insert',
        ns: { db: '', coll: 'validcoll' }, // db is empty string
        documentKey: { _id: 'user12' },
        fullDocument: { _id: 'user12', name: 'Test' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockSyncOps.insertOne).toHaveBeenCalledWith(
        expect.objectContaining({
          ns: { db: '', coll: 'validcoll' }, // Should preserve the values
        }),
      );
    });

    it('handles change event with undefined ns.db', async () => {
      const mockSyncOps = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncLocal = {
        findOne: vi.fn().mockResolvedValue({ seq: 0, headHash: 'GENESIS' }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_ops') return mockSyncOps;
        if (name === 'sync_local') return mockSyncLocal;
        if (name === 'sync_resume') return mockSyncResume;
        return mockSyncOps;
      }) as unknown as typeof mockDb.collection;

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
      });

      const changeEvent = {
        _id: { _data: 'change13' },
        operationType: 'insert',
        ns: { db: undefined, coll: 'mycoll' } as unknown as {
          db: string;
          coll: string;
        },
        documentKey: { _id: 'user13' },
        fullDocument: { _id: 'user13', name: 'Test' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(mockSyncOps.insertOne).toHaveBeenCalledWith(
        expect.objectContaining({
          ns: { db: undefined, coll: 'mycoll' }, // db is undefined
        }),
      );
    });

    it('handles change event with undefined ns.coll', async () => {
      const mockSyncOps = {
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncLocal = {
        findOne: vi.fn().mockResolvedValue({ seq: 0, headHash: 'GENESIS' }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };
      const mockSyncResume = {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      };

      mockDb.collection = vi.fn((name: string) => {
        if (name === 'sync_ops') return mockSyncOps;
        if (name === 'sync_local') return mockSyncLocal;
        if (name === 'sync_resume') return mockSyncResume;
        if (name === undefined) return mockSyncOps; // Handle undefined coll
        return mockSyncOps;
      }) as unknown as typeof mockDb.collection;

      await startDbChangeStream({
        db: mockDb,
        nodeId: 'node1',
      });

      const changeEvent = {
        _id: { _data: 'change14' },
        operationType: 'insert',
        ns: { db: 'mydb', coll: undefined } as unknown as {
          db: string;
          coll: string;
        },
        documentKey: { _id: 'user14' },
        fullDocument: { _id: 'user14', name: 'Test' },
      } as ChangeStreamDocument;

      const changeHandler = changeHandlers.get('change');
      changeHandler!(changeEvent);

      await new Promise((resolve) => setTimeout(resolve, 50));

      // This should be filtered out by isInternalCollection since coll is undefined
      expect(mockSyncOps.insertOne).not.toHaveBeenCalled();
    });
  });
});
