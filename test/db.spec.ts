// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';

// Use vi.hoisted to create mocks that can be used in vi.mock factory
const { mockClient, MockMongoClient } = vi.hoisted(() => {
  const mockDb = {
    collection: vi.fn(),
  };

  const mockClient = {
    connect: vi.fn().mockResolvedValue(undefined),
    db: vi.fn().mockReturnValue(mockDb),
    close: vi.fn().mockResolvedValue(undefined),
  };

  class MockMongoClient {
    constructor() {
      // The constructor should call mockClient.connect, etc.
      return mockClient as never;
    }
  }

  return { mockClient, MockMongoClient };
});

// Mock MongoDB before importing db module
vi.mock('mongodb', () => {
  return {
    MongoClient: MockMongoClient,
  };
});

// Import after mocking
import { close, connect, getDb } from '../../src/db.ts';

describe('db', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Reset mock implementations
    mockClient.connect.mockResolvedValue(undefined);
    mockClient.close.mockResolvedValue(undefined);
  });

  afterEach(async () => {
    // Clean up connection state
    await close().catch(() => {
      /* ignore */
    });
  });

  describe('connect', () => {
    it('connects to MongoDB with provided URI', async () => {
      const uri = 'mongodb://localhost:27017/testdb';

      const db = await connect(uri);

      expect(mockClient.connect).toHaveBeenCalled();
      expect(mockClient.db).toHaveBeenCalled();
      expect(db).toBeDefined();
    });

    it('throws error when URI is empty string', async () => {
      await expect(connect('')).rejects.toThrow('MONGO_URI not set');
    });

    it('successfully creates connection', async () => {
      const uri = 'mongodb://localhost:27017/testdb';

      await connect(uri);

      // Verify connection was established
      expect(mockClient.connect).toHaveBeenCalled();
      expect(mockClient.db).toHaveBeenCalled();
    });
  });

  describe('getDb', () => {
    it('returns database instance after connection', async () => {
      const uri = 'mongodb://localhost:27017/testdb';

      await connect(uri);
      const db = getDb();

      expect(db).toBeDefined();
      expect(db.collection).toBeDefined();
    });

    it('throws error when not connected', () => {
      // Don't connect first

      expect(() => getDb()).toThrow('DB not connected');
    });

    it('returns same instance on multiple calls', async () => {
      const uri = 'mongodb://localhost:27017/testdb';

      await connect(uri);
      const db1 = getDb();
      const db2 = getDb();

      expect(db1).toBe(db2);
    });
  });

  describe('close', () => {
    it('closes client connection', async () => {
      const uri = 'mongodb://localhost:27017/testdb';

      await connect(uri);

      await close();

      expect(mockClient.close).toHaveBeenCalled();
    });

    it('clears connection state after closing', async () => {
      const uri = 'mongodb://localhost:27017/testdb';

      await connect(uri);
      await close();

      expect(() => getDb()).toThrow('DB not connected');
    });

    it('handles close when not connected', async () => {
      // Should not throw
      await expect(close()).resolves.toBeUndefined();
    });

    it('clears state even if close throws', async () => {
      const uri = 'mongodb://localhost:27017/testdb';

      await connect(uri);

      mockClient.close.mockRejectedValueOnce(
        new Error('Connection error')
      );

      // Should not throw, error is swallowed
      await close();

      // State should still be cleared
      expect(() => getDb()).toThrow('DB not connected');
    });
  });
});
