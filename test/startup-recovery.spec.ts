// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import type { Db } from 'mongodb';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  checkResumeTokenValidity,
  clearResumeToken,
  performStartupRecovery,
  type ResumeTokenDoc,
} from '../src/startup-recovery.ts';

describe('startup-recovery', () => {
  let mockDb: Db;
  let mockSyncResumeCollection: ReturnType<typeof vi.fn>;
  let mockAdminDb: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockSyncResumeCollection = {
      findOne: vi.fn().mockResolvedValue(null),
      deleteOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      insertOne: vi.fn().mockResolvedValue({ acknowledged: true }),
      updateOne: vi.fn().mockResolvedValue({ acknowledged: true }),
    };

    mockAdminDb = {
      command: vi.fn().mockResolvedValue({ optimes: { test: 'data' } }),
    };

    mockDb = {
      collection: vi.fn((name: string) => {
        if (name === 'sync_resume') {
          return mockSyncResumeCollection;
        }
        return {
          findOne: vi.fn().mockResolvedValue(null),
          deleteOne: vi.fn().mockResolvedValue({ acknowledged: true }),
        };
      }),
      admin: vi.fn(() => mockAdminDb),
    } as unknown as Db;
  });

  describe('checkResumeTokenValidity', () => {
    it('returns no token status when resume token does not exist', async () => {
      const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };

      const result = await checkResumeTokenValidity({ db: mockDb, logger });

      expect(result.hasToken).toBe(false);
      expect(result.isValid).toBe(true);
      expect(result.message).toBe(
        'No resume token - will start from current position'
      );
      expect(logger.info).toHaveBeenCalledWith(
        'No resume token found - starting fresh change stream'
      );
    });

    it('returns no token status when token is null', async () => {
      mockSyncResumeCollection.findOne = vi
        .fn()
        .mockResolvedValue({ _id: 'resume', token: null });

      const logger = { info: vi.fn() };

      const result = await checkResumeTokenValidity({ db: mockDb, logger });

      expect(result.hasToken).toBe(false);
      expect(result.isValid).toBe(true);
    });

    it('returns valid status for recent token (< 1 hour)', async () => {
      const now = new Date();
      const recentTime = new Date(now.getTime() - 30 * 60 * 1000); // 30 minutes ago

      const resumeDoc: ResumeTokenDoc = {
        _id: 'resume',
        token: { _data: 'test_token' },
        updatedAt: recentTime.toISOString(),
      };

      mockSyncResumeCollection.findOne = vi.fn().mockResolvedValue(resumeDoc);

      const logger = { info: vi.fn(), warn: vi.fn() };

      const result = await checkResumeTokenValidity({ db: mockDb, logger });

      expect(result.hasToken).toBe(true);
      expect(result.isValid).toBe(true);
      expect(result.ageMinutes).toBeGreaterThanOrEqual(29);
      expect(result.ageMinutes).toBeLessThanOrEqual(31);
      expect(result.message).toBe('Resume token is recent and likely valid');
      expect(logger.info).toHaveBeenCalledWith(
        expect.objectContaining({ tokenAge: expect.any(String) }),
        'Resume token found'
      );
    });

    it('returns warning for old token (>= 1 hour)', async () => {
      const now = new Date();
      const oldTime = new Date(now.getTime() - 90 * 60 * 1000); // 90 minutes ago

      const resumeDoc: ResumeTokenDoc = {
        _id: 'resume',
        token: { _data: 'test_token' },
        updatedAt: oldTime.toISOString(),
      };

      mockSyncResumeCollection.findOne = vi.fn().mockResolvedValue(resumeDoc);

      const logger = { info: vi.fn(), warn: vi.fn() };

      const result = await checkResumeTokenValidity({ db: mockDb, logger });

      expect(result.hasToken).toBe(true);
      expect(result.isValid).toBe('unknown');
      expect(result.ageHours).toBeGreaterThanOrEqual(1);
      expect(result.warning).toContain('Token is old');
      expect(result.recommendation).toContain('state hash comparison');
      expect(logger.warn).toHaveBeenCalledWith(
        expect.objectContaining({ tokenAgeHours: expect.any(Number) }),
        'Resume token is old - consider state hash verification'
      );
    });

    it('queries oplog status when checking token', async () => {
      const recentTime = new Date(Date.now() - 30 * 60 * 1000);

      const resumeDoc: ResumeTokenDoc = {
        _id: 'resume',
        token: { _data: 'test_token' },
        updatedAt: recentTime.toISOString(),
      };

      mockSyncResumeCollection.findOne = vi.fn().mockResolvedValue(resumeDoc);

      const mockCommand = vi
        .fn()
        .mockResolvedValue({ optimes: { test: 'data' } });
      mockAdminDb.command = mockCommand;

      const logger = { info: vi.fn() };

      await checkResumeTokenValidity({ db: mockDb, logger });

      expect(mockDb.admin).toHaveBeenCalled();
      expect(mockCommand).toHaveBeenCalledWith({ replSetGetStatus: 1 });
      expect(logger.info).toHaveBeenCalledWith(
        { oplogInfo: { test: 'data' } },
        'Oplog status checked'
      );
    });

    it('continues if oplog status check fails', async () => {
      const recentTime = new Date(Date.now() - 30 * 60 * 1000);

      const resumeDoc: ResumeTokenDoc = {
        _id: 'resume',
        token: { _data: 'test_token' },
        updatedAt: recentTime.toISOString(),
      };

      mockSyncResumeCollection.findOne = vi.fn().mockResolvedValue(resumeDoc);

      const mockCommand = vi
        .fn()
        .mockRejectedValue(new Error('Not a replica set'));
      mockAdminDb.command = mockCommand;

      const logger = { info: vi.fn() };

      const result = await checkResumeTokenValidity({ db: mockDb, logger });

      expect(result.isValid).toBe(true);
      expect(result.message).toBe('Resume token is recent and likely valid');
    });

    it('returns error when check fails', async () => {
      mockSyncResumeCollection.findOne = vi
        .fn()
        .mockRejectedValue(new Error('Database connection lost'));

      const logger = { error: vi.fn() };

      const result = await checkResumeTokenValidity({ db: mockDb, logger });

      expect(result.hasToken).toBe(false);
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('Database connection lost');
      expect(logger.error).toHaveBeenCalledWith(
        { err: 'Database connection lost' },
        'Error checking resume token validity'
      );
    });

    it('works without logger', async () => {
      const result = await checkResumeTokenValidity({ db: mockDb });

      expect(result.hasToken).toBe(false);
      expect(result.isValid).toBe(true);
    });

    it('handles oplog status without optimes', async () => {
      const recentTime = new Date(Date.now() - 30 * 60 * 1000);

      const resumeDoc: ResumeTokenDoc = {
        _id: 'resume',
        token: { _data: 'test_token' },
        updatedAt: recentTime.toISOString(),
      };

      mockSyncResumeCollection.findOne = vi.fn().mockResolvedValue(resumeDoc);

      const mockCommand = vi.fn().mockResolvedValue({ status: 'ok' });
      mockAdminDb.command = mockCommand;

      const logger = { info: vi.fn() };

      const result = await checkResumeTokenValidity({ db: mockDb, logger });

      expect(result.isValid).toBe(true);
      // Should not log oplog info if optimes is missing
      expect(logger.info).not.toHaveBeenCalledWith(
        expect.objectContaining({ oplogInfo: expect.anything() }),
        'Oplog status checked'
      );
    });
  });

  describe('performStartupRecovery', () => {
    it('performs recovery with no token', async () => {
      const logger = { info: vi.fn(), warn: vi.fn() };

      const result = await performStartupRecovery({
        db: mockDb,
        nodeId: 'node1',
        logger,
      });

      expect(result.hasToken).toBe(false);
      expect(result.isValid).toBe(true);
      expect(logger.info).toHaveBeenCalledWith(
        { nodeId: 'node1' },
        'Starting recovery checks...'
      );
      expect(logger.info).toHaveBeenCalledWith(
        { nodeId: 'node1' },
        'Startup recovery: no issues detected'
      );
    });

    it('performs recovery with valid recent token', async () => {
      const recentTime = new Date(Date.now() - 30 * 60 * 1000);

      const resumeDoc: ResumeTokenDoc = {
        _id: 'resume',
        token: { _data: 'test_token' },
        updatedAt: recentTime.toISOString(),
      };

      mockSyncResumeCollection.findOne = vi.fn().mockResolvedValue(resumeDoc);

      const logger = { info: vi.fn(), warn: vi.fn() };

      const result = await performStartupRecovery({
        db: mockDb,
        nodeId: 'node2',
        logger,
      });

      expect(result.hasToken).toBe(true);
      expect(result.isValid).toBe(true);
      expect(logger.info).toHaveBeenCalledWith(
        { nodeId: 'node2' },
        'Startup recovery: no issues detected'
      );
    });

    it('performs recovery with old token and logs warning', async () => {
      const oldTime = new Date(Date.now() - 90 * 60 * 1000);

      const resumeDoc: ResumeTokenDoc = {
        _id: 'resume',
        token: { _data: 'test_token' },
        updatedAt: oldTime.toISOString(),
      };

      mockSyncResumeCollection.findOne = vi.fn().mockResolvedValue(resumeDoc);

      const logger = { info: vi.fn(), warn: vi.fn() };

      const result = await performStartupRecovery({
        db: mockDb,
        nodeId: 'node3',
        logger,
      });

      expect(result.hasToken).toBe(true);
      expect(result.isValid).toBe('unknown');
      expect(result.warning).toBeDefined();
      expect(logger.warn).toHaveBeenCalledWith(
        expect.objectContaining({
          nodeId: 'node3',
          tokenStatus: expect.any(Object),
          recommendation: expect.stringContaining('state hash comparison'),
        }),
        'Startup recovery: potential sync gap detected'
      );
    });

    it('performs recovery with invalid token', async () => {
      mockSyncResumeCollection.findOne = vi
        .fn()
        .mockRejectedValue(new Error('Database error'));

      const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };

      const result = await performStartupRecovery({
        db: mockDb,
        nodeId: 'node4',
        logger,
      });

      expect(result.hasToken).toBe(false);
      expect(result.isValid).toBe(false);
      expect(logger.warn).toHaveBeenCalledWith(
        expect.objectContaining({
          nodeId: 'node4',
        }),
        'Startup recovery: potential sync gap detected'
      );
    });

    it('works without logger', async () => {
      const result = await performStartupRecovery({
        db: mockDb,
        nodeId: 'node5',
      });

      expect(result.hasToken).toBe(false);
      expect(result.isValid).toBe(true);
    });
  });

  describe('clearResumeToken', () => {
    it('clears resume token', async () => {
      const logger = { info: vi.fn() };

      await clearResumeToken({ db: mockDb, logger });

      expect(mockSyncResumeCollection.deleteOne).toHaveBeenCalledWith({
        _id: 'resume',
      });
      expect(logger.info).toHaveBeenCalledWith('Resume token cleared');
    });

    it('works without logger', async () => {
      await clearResumeToken({ db: mockDb });

      expect(mockSyncResumeCollection.deleteOne).toHaveBeenCalledWith({
        _id: 'resume',
      });
    });
  });
});
