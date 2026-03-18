// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it, beforeEach, vi } from 'vitest';
import { syncRljsonTreeFromHub } from '../../src/sync/rljson-hub-sync';
import type { RljsonTreePayload } from '../../src/sync/rljson-sync';

// Mock fetch
global.fetch = vi.fn();

describe('rljson-hub-sync', () => {
  const hubUrl = 'http://localhost:3000';
  const peerClientId = 'nodeA';
  const localNodeId = 'nodeB';
  let mockFastify: any;

  beforeEach(() => {
    vi.clearAllMocks();
    mockFastify = {
      log: {
        info: vi.fn(),
        error: vi.fn(),
      },
    };
  });

  describe('syncRljsonTreeFromHub', () => {
    it('should fetch and apply tree successfully', async () => {
      const mockPayload: RljsonTreePayload = {
        origin: peerClientId,
        rootHash: 'abc123',
        totalNodes: 3,
        nodes: [],
        blobs: [],
        timestamp: new Date().toISOString(),
      };

      // Mock fetching tree from peer via hub
      (global.fetch as any).mockResolvedValueOnce({
        ok: true,
        json: async () => ({ ok: true, payload: mockPayload }),
      });

      // Mock applying tree locally
      (global.fetch as any).mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ok: true,
          result: {
            success: true,
            rootHash: 'abc123',
            nodesApplied: 3,
            blobsReceived: 0,
          },
        }),
      });

      const result = await syncRljsonTreeFromHub({
        fastify: mockFastify,
        hubUrl,
        peerClientId,
        localNodeId,
      });

      expect(result.success).toBe(true);
      expect(fetch).toHaveBeenCalledTimes(2);
      expect(fetch).toHaveBeenCalledWith(
        `${hubUrl}/hub/relay/${peerClientId}/rljson/tree`,
        expect.objectContaining({ method: 'GET' })
      );
    });

    it('should handle HTTP errors', async () => {
      (global.fetch as any).mockResolvedValueOnce({
        ok: false,
        status: 404,
        statusText: 'Not Found',
        text: async () => 'Not found',
      });

      const result = await syncRljsonTreeFromHub({
        fastify: mockFastify,
        hubUrl,
        peerClientId,
        localNodeId,
      });

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });

    it('should log fetch operations', async () => {
      const mockPayload: RljsonTreePayload = {
        origin: peerClientId,
        rootHash: 'hash123',
        totalNodes: 1,
        nodes: [],
        blobs: [],
        timestamp: new Date().toISOString(),
      };

      (global.fetch as any).mockResolvedValueOnce({
        ok: true,
        json: async () => ({ ok: true, payload: mockPayload }),
      });

      (global.fetch as any).mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ok: true,
          result: {
            success: true,
            rootHash: 'hash123',
            nodesApplied: 1,
            blobsReceived: 0,
          },
        }),
      });

   await syncRljsonTreeFromHub({
        fastify: mockFastify,
        hubUrl,
        peerClientId,
        localNodeId,
      });

      expect(mockFastify.log.info).toHaveBeenCalled();
    });

    it('should handle response without payload', async () => {
      (global.fetch as any).mockResolvedValueOnce({
        ok: true,
        json: async () => ({ ok: false }),
      });

      const result = await syncRljsonTreeFromHub({
        fastify: mockFastify,
        hubUrl,
        peerClientId,
        localNodeId,
      });

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });
  });
});
