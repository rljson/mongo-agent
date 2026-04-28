// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { ObjectId } from 'bson';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
  applyOneOp, fetchOpsFromHub, SyncOp, syncOriginFromHub
} from '../../src/sync/pull-from-hub.ts';


import type { Db, MongoClient } from 'mongodb';
describe('pull-from-hub', () => {
  describe('fetchOpsFromHub', () => {
    let mockFetch: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      mockFetch = vi.fn();
      global.fetch = mockFetch;
    });

    it('fetches operations from hub and deserializes them', async () => {
      const mockOps = [
        {
          _id: 'nodeA_1',
          origin: 'nodeA',
          seq: 1,
          prevHash: 'GENESIS',
          opHash: 'hash1',
          chainHash: 'chain1',
          ns: { db: 'testdb', coll: 'users' },
          operationType: 'insert',
          docId: { $oid: '507f1f77bcf86cd799439011' },
          payload: {
            fullDocument: {
              _id: { $oid: '507f1f77bcf86cd799439011' },
              name: 'Alice',
            },
          },
          ts: '2024-01-01T00:00:00.000Z',
        },
      ];

      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => ({ ops: mockOps }),
        text: async () => JSON.stringify({ ops: mockOps }),
      });

      const logger = {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      const ops = await fetchOpsFromHub({
        fastify: { log: logger },
        hubUrl: 'http://hub:3000',
        peerClientId: 'nodeA',
        origin: 'nodeA',
        lastSeqSeen: 0,
        lastHashSeen: 'GENESIS',
      });

      expect(mockFetch).toHaveBeenCalledWith(
        'http://hub:3000/hub/relay/nodeA/sync/pull',
        {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
          },
          body: JSON.stringify({
            origin: 'nodeA',
            lastSeqSeen: 0,
            lastHashSeen: 'GENESIS',
          }),
        },
      );

      expect(ops).toHaveLength(1);
      expect(ops[0].origin).toBe('nodeA');
      expect(ops[0].seq).toBe(1);
      // EJSON should deserialize $oid to ObjectId
      expect(ops[0].docId).toBeInstanceOf(ObjectId);
      expect(logger.info).toHaveBeenCalled();
    });

    it('handles array payload format', async () => {
      const mockOps = [
        {
          _id: 'nodeB_1',
          origin: 'nodeB',
          seq: 1,
          ns: { db: 'testdb', coll: 'products' },
          operationType: 'update',
        },
      ];

      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => mockOps,
      });

      const logger = { info: vi.fn() };

      const ops = await fetchOpsFromHub({
        fastify: { log: logger },
        hubUrl: 'http://hub:3000',
        peerClientId: 'nodeB',
        origin: 'nodeB',
        lastSeqSeen: 0,
        lastHashSeen: 'GENESIS',
      });

      expect(ops).toHaveLength(1);
      expect(ops[0].origin).toBe('nodeB');
    });

    it('returns empty array for empty payload', async () => {
      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => ({ ops: [] }),
      });

      const logger = { info: vi.fn() };

      const ops = await fetchOpsFromHub({
        fastify: { log: logger },
        hubUrl: 'http://hub:3000',
        peerClientId: 'nodeC',
        origin: 'nodeC',
        lastSeqSeen: 5,
        lastHashSeen: 'hash5',
      });

      expect(ops).toHaveLength(0);
    });

    it('throws error when fetch fails', async () => {
      mockFetch.mockResolvedValue({
        ok: false,
        status: 500,
        text: async () => 'Internal Server Error',
      });

      const logger = { info: vi.fn() };

      await expect(
        fetchOpsFromHub({
          fastify: { log: logger },
          hubUrl: 'http://hub:3000',
          peerClientId: 'nodeD',
          origin: 'nodeD',
          lastSeqSeen: 0,
          lastHashSeen: 'GENESIS',
        }),
      ).rejects.toThrow('pull failed 500: Internal Server Error');
    });

    it('handles payload without ops property', async () => {
      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => ({ data: 'something' }),
      });

      const logger = { info: vi.fn() };

      const ops = await fetchOpsFromHub({
        fastify: { log: logger },
        hubUrl: 'http://hub:3000',
        peerClientId: 'nodeE',
        origin: 'nodeE',
        lastSeqSeen: 0,
        lastHashSeen: 'GENESIS',
      });

      expect(ops).toHaveLength(0);
    });

    it('works without logger info method', async () => {
      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => ({ ops: [] }),
      });

      const logger = {};

      const ops = await fetchOpsFromHub({
        fastify: { log: logger },
        hubUrl: 'http://hub:3000',
        peerClientId: 'nodeF',
        origin: 'nodeF',
        lastSeqSeen: 0,
        lastHashSeen: 'GENESIS',
      });

      expect(ops).toHaveLength(0);
    });

    it('handles null payload', async () => {
      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => null,
      });

      const logger = { info: vi.fn() };

      const ops = await fetchOpsFromHub({
        fastify: { log: logger },
        hubUrl: 'http://hub:3000',
        peerClientId: 'nodeG',
        origin: 'nodeG',
        lastSeqSeen: 0,
        lastHashSeen: 'GENESIS',
      });

      expect(ops).toHaveLength(0);
      expect(logger.info).toHaveBeenCalledWith(
        expect.objectContaining({
          payloadKeys: null, // Should be null for non-object payload
        }),
        'sync pull payload parsed',
      );
    });

    it('handles string payload', async () => {
      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => 'not an object',
      });

      const logger = { info: vi.fn() };

      const ops = await fetchOpsFromHub({
        fastify: { log: logger },
        hubUrl: 'http://hub:3000',
        peerClientId: 'nodeH',
        origin: 'nodeH',
        lastSeqSeen: 0,
        lastHashSeen: 'GENESIS',
      });

      expect(ops).toHaveLength(0);
      expect(logger.info).toHaveBeenCalledWith(
        expect.objectContaining({
          payloadKeys: null, // Should be null for non-object payload
        }),
        'sync pull payload parsed',
      );
    });
  });

  describe('applyOneOp', () => {
    let mockDb: Db;
    let mockCollections: Map<string, ReturnType<typeof vi.fn>>;

    beforeEach(() => {
      mockCollections = new Map();

      mockDb = {
        collection: vi.fn((name: string) => {
          if (!mockCollections.has(name)) {
            mockCollections.set(name, {
              findOne: vi.fn().mockResolvedValue(null),
              insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
              updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
              replaceOne: vi.fn().mockResolvedValue({ acknowledged: true }),
              deleteOne: vi.fn().mockResolvedValue({ acknowledged: true }),
            });
          }
          return mockCollections.get(name);
        }),
      } as unknown as Db;
    });

    it('applies insert operation', async () => {
      const op: SyncOp = {
        _id: 'nodeA_1',
        origin: 'nodeA',
        seq: 1,
        prevHash: 'GENESIS',
        opHash: 'hash1',
        chainHash: 'chain1',
        ns: { db: 'testdb', coll: 'users' },
        operationType: 'insert',
        docId: new ObjectId('507f1f77bcf86cd799439011'),
        payload: {
          fullDocument: {
            _id: new ObjectId('507f1f77bcf86cd799439011'),
            name: 'Alice',
            age: 30,
          },
        },
        ts: '2024-01-01T00:00:00.000Z',
      };

      const logger = { info: vi.fn(), warn: vi.fn() };

      const result = await applyOneOp({
        db: mockDb,
        op,
        localNodeId: 'nodeB',
        fastify: { log: logger },
      });

      expect(result.applied).toBe(true);

      const usersColl = mockCollections.get('users');
      expect(usersColl?.replaceOne).toHaveBeenCalledWith(
        { _id: op.payload.fullDocument?._id },
        op.payload.fullDocument,
        { upsert: true },
      );

      const syncOpsColl = mockCollections.get('sync_ops');
      expect(syncOpsColl?.insertOne).toHaveBeenCalledWith(op);

      const syncStateColl = mockCollections.get('sync_state');
      expect(syncStateColl?.updateOne).toHaveBeenCalledWith(
        { origin: 'nodeA' },
        {
          $set: {
            origin: 'nodeA',
            lastSeqSeen: 1,
            lastHashSeen: 'chain1',
            applied: {
              lastSeq: 1,
              lastHash: 'chain1',
            },
            updatedAt: expect.any(String),
            updatedBy: 'nodeB',
          },
        },
        { upsert: true },
      );
    });

    it('applies update operation', async () => {
      const op: SyncOp = {
        _id: 'nodeA_2',
        origin: 'nodeA',
        seq: 2,
        prevHash: 'chain1',
        opHash: 'hash2',
        chainHash: 'chain2',
        ns: { db: 'testdb', coll: 'users' },
        operationType: 'update',
        docId: new ObjectId('507f1f77bcf86cd799439011'),
        payload: {
          fullDocument: {
            _id: new ObjectId('507f1f77bcf86cd799439011'),
            name: 'Alice Updated',
            age: 31,
          },
        },
        ts: '2024-01-02T00:00:00.000Z',
      };

      const logger = { info: vi.fn(), warn: vi.fn() };

      const result = await applyOneOp({
        db: mockDb,
        op,
        localNodeId: 'nodeB',
        fastify: { log: logger },
      });

      expect(result.applied).toBe(true);

      const usersColl = mockCollections.get('users');
      expect(usersColl?.replaceOne).toHaveBeenCalledWith(
        { _id: op.payload.fullDocument?._id },
        op.payload.fullDocument,
        { upsert: true },
      );
    });

    it('applies delete operation', async () => {
      const docId = new ObjectId('507f1f77bcf86cd799439011');
      const op: SyncOp = {
        _id: 'nodeA_3',
        origin: 'nodeA',
        seq: 3,
        prevHash: 'chain2',
        opHash: 'hash3',
        chainHash: 'chain3',
        ns: { db: 'testdb', coll: 'users' },
        operationType: 'delete',
        docId,
        payload: null,
        ts: '2024-01-03T00:00:00.000Z',
      };

      const logger = { info: vi.fn(), warn: vi.fn() };

      const result = await applyOneOp({
        db: mockDb,
        op,
        localNodeId: 'nodeB',
        fastify: { log: logger },
      });

      expect(result.applied).toBe(true);

      const usersColl = mockCollections.get('users');
      expect(usersColl?.deleteOne).toHaveBeenCalledWith({ _id: docId });
    });

    it('skips already present operation', async () => {
      const op: SyncOp = {
        _id: 'nodeA_4',
        origin: 'nodeA',
        seq: 4,
        prevHash: 'chain3',
        opHash: 'hash4',
        chainHash: 'chain4',
        ns: { db: 'testdb', coll: 'users' },
        operationType: 'insert',
        docId: new ObjectId(),
        payload: { fullDocument: { _id: new ObjectId(), name: 'Bob' } },
        ts: '2024-01-04T00:00:00.000Z',
      };

      // Set up the mock before calling applyOneOp
      mockCollections.set('sync_ops', {
        findOne: vi.fn().mockResolvedValue({ _id: op._id }),
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        replaceOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        deleteOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      });

      const logger = { info: vi.fn(), warn: vi.fn() };

      const result = await applyOneOp({
        db: mockDb,
        op,
        localNodeId: 'nodeB',
        fastify: { log: logger },
      });

      expect(result.applied).toBe(false);
      expect(result.reason).toBe('already-present');
    });

    it('handles unknown operation type', async () => {
      const op: SyncOp = {
        _id: 'nodeA_5',
        origin: 'nodeA',
        seq: 5,
        prevHash: 'chain4',
        opHash: 'hash5',
        chainHash: 'chain5',
        ns: { db: 'testdb', coll: 'users' },
        operationType: 'unknown',
        docId: new ObjectId(),
        payload: null,
        ts: '2024-01-05T00:00:00.000Z',
      };

      const logger = { info: vi.fn(), warn: vi.fn() };

      const result = await applyOneOp({
        db: mockDb,
        op,
        localNodeId: 'nodeB',
        fastify: { log: logger },
      });

      expect(result.applied).toBe(false);
      expect(result.reason).toBe('unknown-op-type');
      expect(logger.warn).toHaveBeenCalledWith(
        { opId: op._id, type: 'unknown' },
        'unknown op type',
      );
    });

    it('uses suppressor to prevent echo loops', async () => {
      const docId = new ObjectId('507f1f77bcf86cd799439011');
      const op: SyncOp = {
        _id: 'nodeA_8',
        origin: 'nodeA',
        seq: 8,
        prevHash: 'chain7',
        opHash: 'hash8',
        chainHash: 'chain8',
        ns: { db: 'testdb', coll: 'users' },
        operationType: 'insert',
        docId,
        payload: {
          fullDocument: {
            _id: docId,
            name: 'Dave',
          },
        },
        ts: '2024-01-08T00:00:00.000Z',
      };

      const suppressor = {
        add: vi.fn(),
      };

      const logger = { info: vi.fn(), warn: vi.fn() };

      await applyOneOp({
        db: mockDb,
        op,
        localNodeId: 'nodeB',
        fastify: { log: logger },
        suppressor,
      });

      expect(suppressor.add).toHaveBeenCalledWith(op.ns, docId);
    });

    it('uses suppressor for update operations', async () => {
      const docId = new ObjectId('507f1f77bcf86cd799439011');
      const op: SyncOp = {
        _id: 'nodeA_10',
        origin: 'nodeA',
        seq: 10,
        prevHash: 'chain9',
        opHash: 'hash10',
        chainHash: 'chain10',
        ns: { db: 'testdb', coll: 'users' },
        operationType: 'update',
        docId,
        payload: {
          fullDocument: {
            _id: docId,
            name: 'Updated',
          },
        },
        ts: '2024-01-10T00:00:00.000Z',
      };

      const suppressor = {
        add: vi.fn(),
      };

      const logger = { info: vi.fn(), warn: vi.fn() };

      await applyOneOp({
        db: mockDb,
        op,
        localNodeId: 'nodeB',
        fastify: { log: logger },
        suppressor,
      });

      expect(suppressor.add).toHaveBeenCalledWith(op.ns, docId);
    });

    it('uses suppressor for delete operations', async () => {
      const docId = new ObjectId('507f1f77bcf86cd799439011');
      const op: SyncOp = {
        _id: 'nodeA_11',
        origin: 'nodeA',
        seq: 11,
        prevHash: 'chain10',
        opHash: 'hash11',
        chainHash: 'chain11',
        ns: { db: 'testdb', coll: 'users' },
        operationType: 'delete',
        docId,
        payload: null,
        ts: '2024-01-11T00:00:00.000Z',
      };

      const suppressor = {
        add: vi.fn(),
      };

      const logger = { info: vi.fn(), warn: vi.fn() };

      await applyOneOp({
        db: mockDb,
        op,
        localNodeId: 'nodeB',
        fastify: { log: logger },
        suppressor,
      });

      expect(suppressor.add).toHaveBeenCalledWith(op.ns, docId);
    });

    it('works without logger warn method', async () => {
      const op: SyncOp = {
        _id: 'nodeA_9',
        origin: 'nodeA',
        seq: 9,
        prevHash: 'chain8',
        opHash: 'hash9',
        chainHash: 'chain9',
        ns: { db: 'testdb', coll: 'users' },
        operationType: 'unknown',
        docId: new ObjectId(),
        payload: null,
        ts: '2024-01-09T00:00:00.000Z',
      };

      const logger = {};

      const result = await applyOneOp({
        db: mockDb,
        op,
        localNodeId: 'nodeB',
        fastify: { log: logger },
      });

      expect(result.applied).toBe(false);
      expect(result.reason).toBe('unknown-op-type');
    });

    it('records concurrent-update conflict when local has unsynced edit', async () => {
      // L2 receiving an update from laptop1 for a doc L2 already edited.
      const docId = new ObjectId('507f1f77bcf86cd799439011');
      const op: SyncOp = {
        _id: 'laptop1_5',
        origin: 'laptop1',
        seq: 5,
        chainHash: 'chain5',
        ns: { db: 'testdb', coll: 'users' },
        operationType: 'update',
        docId,
        payload: {
          fullDocument: { _id: docId, name: 'Edited on L1' },
        },
        ts: '2024-01-01T00:00:00.000Z',
      };
      const localDoc = { _id: docId, name: 'Edited on L2' };
      const lastLocalEdit = {
        _id: 'laptop2_3',
        origin: 'laptop2',
        seq: 3,
        operationType: 'update',
        chainHash: 'chain3',
        ts: '2024-01-01T00:00:00.000Z',
        payload: { fullDocument: localDoc },
        ns: { db: 'testdb', coll: 'users' },
        docId,
      };

      const usersReplaceOne = vi.fn();
      mockCollections.set('users', {
        findOne: vi.fn().mockResolvedValue(localDoc),
        replaceOne: usersReplaceOne,
      } as never);
      // sync_ops.findOne is called twice: dedupe by _id (returns null), then
      // for lastLocalEdit lookup (returns the local edit).
      const syncOpsFindOne = vi
        .fn()
        .mockImplementation((filter: { _id?: string }) =>
          Promise.resolve(filter._id === op._id ? null : lastLocalEdit),
        );
      mockCollections.set('sync_ops', {
        findOne: syncOpsFindOne,
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      } as never);
      let recorded: { conflictType?: string; versions?: unknown[] } | null = null;
      mockCollections.set('sync_conflicts', {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn(
          async (
            _filter: unknown,
            update: { $setOnInsert?: typeof recorded },
          ) => {
            recorded = update.$setOnInsert ?? null;
            return { acknowledged: true };
          },
        ),
      } as never);
      mockCollections.set('sync_state', {
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      } as never);

      const result = await applyOneOp({
        db: mockDb,
        op,
        localNodeId: 'laptop2',
        fastify: { log: { info: vi.fn(), warn: vi.fn() } },
      });

      expect(result.applied).toBe(false);
      expect(result.reason).toBe('conflict-recorded');
      // Local doc not overwritten — conflict preserves local state.
      expect(usersReplaceOne).not.toHaveBeenCalled();
      expect(recorded).not.toBeNull();
      expect(recorded!.conflictType).toBe('concurrent-update');
      expect(recorded!.versions).toHaveLength(2);
    });

    it('applies remote resolution when pending sync_conflict exists', async () => {
      // Simulates a UI/API resolution on the peer. The incoming op should
      // overwrite local AND mark our local conflict resolved.
      const docId = new ObjectId('507f1f77bcf86cd799439011');
      const op: SyncOp = {
        _id: 'laptop1_8',
        origin: 'laptop1',
        seq: 8,
        chainHash: 'chain8',
        ns: { db: 'testdb', coll: 'users' },
        operationType: 'update',
        docId,
        payload: { fullDocument: { _id: docId, name: 'resolved' } },
        ts: '2024-01-01T00:00:00.000Z',
      };
      const usersReplaceOne = vi.fn().mockResolvedValue({ acknowledged: true });
      mockCollections.set('users', {
        findOne: vi.fn().mockResolvedValue({ _id: docId, name: 'old' }),
        replaceOne: usersReplaceOne,
      } as never);
      const conflictsUpdateOne = vi.fn(async () => ({ acknowledged: true }));
      mockCollections.set('sync_conflicts', {
        findOne: vi.fn().mockResolvedValue({
          _id: 'conflict-1',
          conflictId: 'conflict-1',
          documentId: String(docId),
          collection: 'users',
          status: 'pending',
        }),
        updateOne: conflictsUpdateOne,
      } as never);
      mockCollections.set('sync_ops', {
        findOne: vi.fn().mockResolvedValue(null),
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      } as never);
      mockCollections.set('sync_state', {
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      } as never);

      const result = await applyOneOp({
        db: mockDb,
        op,
        localNodeId: 'laptop2',
        fastify: { log: { info: vi.fn(), warn: vi.fn() } },
      });

      expect(result.applied).toBe(true);
      expect(result.reason).toBe('resolution-applied');
      expect(usersReplaceOne).toHaveBeenCalledWith(
        { _id: docId },
        op.payload!.fullDocument,
        { upsert: true },
      );
      expect(conflictsUpdateOne).toHaveBeenCalledWith(
        { _id: 'conflict-1' },
        { $set: { status: 'resolved' } },
      );
    });

    it('records update-delete conflict (Case A: peer deletes locally-updated doc)', async () => {
      const docId = new ObjectId('507f1f77bcf86cd799439011');
      const op: SyncOp = {
        _id: 'laptop1_9',
        origin: 'laptop1',
        seq: 9,
        chainHash: 'chain9',
        ns: { db: 'testdb', coll: 'users' },
        operationType: 'delete',
        docId,
        payload: null,
        ts: '2024-01-01T00:00:00.000Z',
      };
      const localDoc = { _id: docId, name: 'L2 still has this updated' };
      const lastLocalUpdate = {
        _id: 'laptop2_4',
        origin: 'laptop2',
        seq: 4,
        operationType: 'update',
        chainHash: 'chain4',
        ts: '2024-01-01T00:00:00.000Z',
        payload: { fullDocument: localDoc },
        ns: { db: 'testdb', coll: 'users' },
        docId,
      };
      const usersDeleteOne = vi.fn();
      mockCollections.set('users', {
        findOne: vi.fn().mockResolvedValue(localDoc),
        deleteOne: usersDeleteOne,
      } as never);
      mockCollections.set('sync_ops', {
        findOne: vi
          .fn()
          .mockImplementation((filter: { _id?: string }) =>
            Promise.resolve(filter._id === op._id ? null : lastLocalUpdate),
          ),
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      } as never);
      let recorded: { conflictType?: string } | null = null;
      mockCollections.set('sync_conflicts', {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn(
          async (
            _filter: unknown,
            update: { $setOnInsert?: typeof recorded },
          ) => {
            recorded = update.$setOnInsert ?? null;
            return { acknowledged: true };
          },
        ),
      } as never);
      mockCollections.set('sync_state', {
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      } as never);

      const result = await applyOneOp({
        db: mockDb,
        op,
        localNodeId: 'laptop2',
        fastify: { log: { info: vi.fn(), warn: vi.fn() } },
      });

      expect(result.applied).toBe(false);
      expect(result.reason).toBe('conflict-recorded');
      // Local doc must NOT be deleted — preserve until resolution.
      expect(usersDeleteOne).not.toHaveBeenCalled();
      expect(recorded).not.toBeNull();
      expect(recorded!.conflictType).toBe('update-delete');
    });

    it('records update-delete conflict (Case B: peer updates locally-deleted doc)', async () => {
      const docId = new ObjectId('507f1f77bcf86cd799439011');
      const op: SyncOp = {
        _id: 'laptop1_10',
        origin: 'laptop1',
        seq: 10,
        chainHash: 'chain10',
        ns: { db: 'testdb', coll: 'users' },
        operationType: 'update',
        docId,
        payload: { fullDocument: { _id: docId, name: 'L1 update' } },
        ts: '2024-01-01T00:00:00.000Z',
      };
      const lastLocalDelete = {
        _id: 'laptop2_5',
        origin: 'laptop2',
        seq: 5,
        operationType: 'delete',
        chainHash: 'chain5',
        ts: '2024-01-01T00:00:00.000Z',
        payload: null,
        ns: { db: 'testdb', coll: 'users' },
        docId,
      };
      const usersReplaceOne = vi.fn();
      mockCollections.set('users', {
        findOne: vi.fn().mockResolvedValue(null), // local was deleted
        replaceOne: usersReplaceOne,
      } as never);
      mockCollections.set('sync_ops', {
        findOne: vi
          .fn()
          .mockImplementation((filter: { _id?: string }) =>
            Promise.resolve(filter._id === op._id ? null : lastLocalDelete),
          ),
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      } as never);
      let recorded: { conflictType?: string } | null = null;
      mockCollections.set('sync_conflicts', {
        findOne: vi.fn().mockResolvedValue(null),
        updateOne: vi.fn(
          async (
            _filter: unknown,
            update: { $setOnInsert?: typeof recorded },
          ) => {
            recorded = update.$setOnInsert ?? null;
            return { acknowledged: true };
          },
        ),
      } as never);
      mockCollections.set('sync_state', {
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      } as never);

      const result = await applyOneOp({
        db: mockDb,
        op,
        localNodeId: 'laptop2',
        fastify: { log: { info: vi.fn(), warn: vi.fn() } },
      });

      expect(result.applied).toBe(false);
      expect(result.reason).toBe('conflict-recorded');
      // Don't recreate the locally-deleted doc.
      expect(usersReplaceOne).not.toHaveBeenCalled();
      expect(recorded).not.toBeNull();
      expect(recorded!.conflictType).toBe('update-delete');
    });
  });

  describe('syncOriginFromHub', () => {
    let mockDb: Db;
    let mockMongo: MongoClient;
    let mockFetch: ReturnType<typeof vi.fn>;
    let mockCollections: Map<string, ReturnType<typeof vi.fn>>;

    beforeEach(() => {
      mockCollections = new Map();

      mockDb = {
        collection: vi.fn((name: string) => {
          if (!mockCollections.has(name)) {
            mockCollections.set(name, {
              findOne: vi.fn().mockResolvedValue(null),
              insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
              updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
              replaceOne: vi.fn().mockResolvedValue({ acknowledged: true }),
              deleteOne: vi.fn().mockResolvedValue({ acknowledged: true }),
            });
          }
          return mockCollections.get(name);
        }),
      } as unknown as Db;

      mockMongo = {
        db: vi.fn(() => mockDb),
      } as unknown as MongoClient;

      mockFetch = vi.fn();
      global.fetch = mockFetch;
    });

    it('syncs operations from hub and applies them', async () => {
      const mockOps = [
        {
          _id: 'nodeA_1',
          origin: 'nodeA',
          seq: 1,
          prevHash: 'GENESIS',
          opHash: 'hash1',
          chainHash: 'chain1',
          ns: { db: 'testdb', coll: 'users' },
          operationType: 'insert',
          docId: { $oid: '507f1f77bcf86cd799439011' },
          payload: {
            fullDocument: {
              _id: { $oid: '507f1f77bcf86cd799439011' },
              name: 'Alice',
            },
          },
          ts: '2024-01-01T00:00:00.000Z',
        },
        {
          _id: 'nodeA_2',
          origin: 'nodeA',
          seq: 2,
          prevHash: 'chain1',
          opHash: 'hash2',
          chainHash: 'chain2',
          ns: { db: 'testdb', coll: 'users' },
          operationType: 'update',
          docId: { $oid: '507f1f77bcf86cd799439011' },
          payload: {
            fullDocument: {
              _id: { $oid: '507f1f77bcf86cd799439011' },
              name: 'Alice Updated',
            },
          },
          ts: '2024-01-02T00:00:00.000Z',
        },
      ];

      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => ({ ops: mockOps }),
      });

      const logger = { info: vi.fn(), warn: vi.fn() };

      const result = await syncOriginFromHub({
        fastify: { log: logger },
        mongo: mockMongo,
        dbName: 'testdb',
        localNodeId: 'nodeB',
        hubUrl: 'http://hub:3000',
        peerClientId: 'nodeA',
        origin: 'nodeA',
      });

      expect(result.pulled).toBe(2);
      expect(result.applied).toBe(2);
      expect(result.upToDate).toBe(false);

      expect(logger.info).toHaveBeenCalledWith(
        expect.objectContaining({
          peer: 'nodeA',
          origin: 'nodeA',
          pulled: 2,
          applied: 2,
          upToDate: false,
        }),
        'sync pull via hub done',
      );
    });

    it('resumes from last sync state', async () => {
      // Set up sync_state mock before calling syncOriginFromHub
      mockCollections.set('sync_state', {
        findOne: vi.fn().mockResolvedValue({
          origin: 'nodeA',
          lastSeqSeen: 5,
          lastHashSeen: 'chain5',
        }),
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        replaceOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        deleteOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      });

      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => ({ ops: [] }),
      });

      const logger = { info: vi.fn() };

      const result = await syncOriginFromHub({
        fastify: { log: logger },
        mongo: mockMongo,
        dbName: 'testdb',
        localNodeId: 'nodeB',
        hubUrl: 'http://hub:3000',
        peerClientId: 'nodeA',
        origin: 'nodeA',
      });

      expect(mockFetch).toHaveBeenCalledWith(
        'http://hub:3000/hub/relay/nodeA/sync/pull',
        expect.objectContaining({
          body: JSON.stringify({
            origin: 'nodeA',
            lastSeqSeen: 5,
            lastHashSeen: 'chain5',
          }),
        }),
      );

      expect(result.upToDate).toBe(true);
    });

    it('handles operations that are already present', async () => {
      const mockOps = [
        {
          _id: 'nodeA_1',
          origin: 'nodeA',
          seq: 1,
          ns: { db: 'testdb', coll: 'users' },
          operationType: 'insert',
          docId: { $oid: '507f1f77bcf86cd799439011' },
          payload: {
            fullDocument: {
              _id: { $oid: '507f1f77bcf86cd799439011' },
              name: 'Alice',
            },
          },
        },
      ];

      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => ({ ops: mockOps }),
      });

      // Set up sync_ops mock to return existing document
      mockCollections.set('sync_ops', {
        findOne: vi.fn().mockResolvedValue({ _id: 'nodeA_1' }),
        insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        replaceOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        deleteOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      });

      const logger = { info: vi.fn() };

      const result = await syncOriginFromHub({
        fastify: { log: logger },
        mongo: mockMongo,
        dbName: 'testdb',
        localNodeId: 'nodeB',
        hubUrl: 'http://hub:3000',
        peerClientId: 'nodeA',
        origin: 'nodeA',
      });

      expect(result.pulled).toBe(1);
      expect(result.applied).toBe(0);
    });

    it('passes suppressor to applyOneOp', async () => {
      const mockOps = [
        {
          _id: 'nodeA_1',
          origin: 'nodeA',
          seq: 1,
          ns: { db: 'testdb', coll: 'users' },
          operationType: 'insert',
          docId: { $oid: '507f1f77bcf86cd799439011' },
          payload: {
            fullDocument: {
              _id: { $oid: '507f1f77bcf86cd799439011' },
              name: 'Alice',
            },
          },
        },
      ];

      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => ({ ops: mockOps }),
      });

      const suppressor = {
        add: vi.fn(),
      };

      const logger = { info: vi.fn() };

      await syncOriginFromHub({
        fastify: { log: logger },
        mongo: mockMongo,
        dbName: 'testdb',
        localNodeId: 'nodeB',
        hubUrl: 'http://hub:3000',
        peerClientId: 'nodeA',
        origin: 'nodeA',
        suppressor,
      });

      expect(suppressor.add).toHaveBeenCalled();
    });

    it('works without logger info method', async () => {
      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => ({ ops: [] }),
      });

      const logger = {};

      const result = await syncOriginFromHub({
        fastify: { log: logger },
        mongo: mockMongo,
        dbName: 'testdb',
        localNodeId: 'nodeB',
        hubUrl: 'http://hub:3000',
        peerClientId: 'nodeA',
        origin: 'nodeA',
      });

      expect(result.upToDate).toBe(true);
    });
  });
});
