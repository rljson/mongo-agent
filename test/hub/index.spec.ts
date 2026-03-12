// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { createHubApp, type RegistryEntry } from '../../src/hub/index.ts';

describe('hub', () => {
  let app: FastifyInstance;
  let registry: Map<string, RegistryEntry>;
  let originalFetch: typeof fetch;

  beforeEach(async () => {
    const hubApp = createHubApp({ logger: false });
    app = hubApp.app;
    registry = hubApp.registry;
    await app.ready();

    originalFetch = global.fetch;
  });

  afterEach(async () => {
    await app.close();
    global.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  describe('POST /hub/register', () => {
    it('registers a new client', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/hub/register',
        payload: {
          clientId: 'nodeA',
          url: 'http://localhost:3000',
        },
      });

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({
        ok: true,
        clientId: 'nodeA',
        url: 'http://localhost:3000',
      });

      const entry = registry.get('nodeA');
      expect(entry).toBeDefined();
      expect(entry?.url).toBe('http://localhost:3000');
      expect(entry?.lastSeenAt).toBeDefined();
    });

    it('returns 400 when clientId is missing', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/hub/register',
        payload: {
          url: 'http://localhost:3000',
        },
      });

      expect(response.statusCode).toBe(400);
      expect(response.json()).toEqual({
        error: 'clientId and url are required',
      });
    });

    it('returns 400 when url is missing', async () => {
      const response = await app.inject({
        method: 'POST',
        url: '/hub/register',
        payload: {
          clientId: 'nodeA',
        },
      });

      expect(response.statusCode).toBe(400);
      expect(response.json()).toEqual({
        error: 'clientId and url are required',
      });
    });

    it('overwrites existing registration', async () => {
      registry.set('nodeA', {
        url: 'http://old-url:3000',
        lastSeenAt: '2020-01-01T00:00:00.000Z',
      });

      const response = await app.inject({
        method: 'POST',
        url: '/hub/register',
        payload: {
          clientId: 'nodeA',
          url: 'http://new-url:3000',
        },
      });

      expect(response.statusCode).toBe(200);
      const entry = registry.get('nodeA');
      expect(entry?.url).toBe('http://new-url:3000');
    });
  });

  describe('DELETE /hub/unregister/:clientId', () => {
    it('unregisters an existing client', async () => {
      registry.set('nodeA', {
        url: 'http://localhost:3000',
        lastSeenAt: '2024-01-01T00:00:00.000Z',
      });

      const response = await app.inject({
        method: 'DELETE',
        url: '/hub/unregister/nodeA',
      });

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ ok: true });
      expect(registry.has('nodeA')).toBe(false);
    });

    it('succeeds even if client does not exist', async () => {
      const response = await app.inject({
        method: 'DELETE',
        url: '/hub/unregister/nonexistent',
      });

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ ok: true });
    });
  });

  describe('GET /hub/clients', () => {
    it('returns empty list when no clients registered', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/hub/clients',
      });

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ clients: [] });
    });

    it('returns list with one client', async () => {
      registry.set('nodeA', {
        url: 'http://localhost:3000',
        lastSeenAt: '2024-01-01T00:00:00.000Z',
      });

      const response = await app.inject({
        method: 'GET',
        url: '/hub/clients',
      });

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({
        clients: [
          {
            clientId: 'nodeA',
            url: 'http://localhost:3000',
            lastSeenAt: '2024-01-01T00:00:00.000Z',
          },
        ],
      });
    });

    it('returns sorted list of multiple clients', async () => {
      registry.set('nodeC', {
        url: 'http://localhost:3002',
        lastSeenAt: '2024-01-03T00:00:00.000Z',
      });
      registry.set('nodeA', {
        url: 'http://localhost:3000',
        lastSeenAt: '2024-01-01T00:00:00.000Z',
      });
      registry.set('nodeB', {
        url: 'http://localhost:3001',
        lastSeenAt: '2024-01-02T00:00:00.000Z',
      });

      const response = await app.inject({
        method: 'GET',
        url: '/hub/clients',
      });

      expect(response.statusCode).toBe(200);
      const data = response.json();
      expect(data.clients).toHaveLength(3);
      expect(data.clients[0].clientId).toBe('nodeA');
      expect(data.clients[1].clientId).toBe('nodeB');
      expect(data.clients[2].clientId).toBe('nodeC');
    });
  });

  describe('GET /health', () => {
    it('returns ok status', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/health',
      });

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ ok: true });
    });
  });

  describe('ALL /hub/relay/:clientId/*', () => {
    it('returns 404 when client not registered', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/hub/relay/nonexistent/some/path',
      });

      expect(response.statusCode).toBe(404);
      expect(response.json()).toEqual({
        error: 'unknown clientId: nonexistent',
      });
    });

    it('relays GET request successfully', async () => {
      registry.set('nodeA', {
        url: 'http://localhost:3000',
        lastSeenAt: '2024-01-01T00:00:00.000Z',
      });

      global.fetch = vi.fn().mockResolvedValue({
        status: 200,
        headers: new Map([['content-type', 'application/json']]),
        text: async () => JSON.stringify({ data: 'test' }),
      } as unknown as Response);

      const response = await app.inject({
        method: 'GET',
        url: '/hub/relay/nodeA/api/test',
      });

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ data: 'test' });
      expect(global.fetch).toHaveBeenCalledWith(
        'http://localhost:3000/api/test',
        {
          method: 'GET',
          headers: {},
        }
      );
    });

    it('relays POST request with body', async () => {
      registry.set('nodeA', {
        url: 'http://localhost:3000',
        lastSeenAt: '2024-01-01T00:00:00.000Z',
      });

      global.fetch = vi.fn().mockResolvedValue({
        status: 201,
        headers: new Map([['content-type', 'application/json']]),
        text: async () => JSON.stringify({ created: true }),
      } as unknown as Response);

      const response = await app.inject({
        method: 'POST',
        url: '/hub/relay/nodeA/api/create',
        payload: { name: 'test' },
        headers: {
          'content-type': 'application/json',
        },
      });

      expect(response.statusCode).toBe(201);
      expect(response.json()).toEqual({ created: true });
      expect(global.fetch).toHaveBeenCalledWith(
        'http://localhost:3000/api/create',
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ name: 'test' }),
        }
      );
    });

    it('preserves query string in relay', async () => {
      registry.set('nodeA', {
        url: 'http://localhost:3000',
        lastSeenAt: '2024-01-01T00:00:00.000Z',
      });

      global.fetch = vi.fn().mockResolvedValue({
        status: 200,
        headers: new Map([['content-type', 'text/plain']]),
        text: async () => 'ok',
      } as unknown as Response);

      const response = await app.inject({
        method: 'GET',
        url: '/hub/relay/nodeA/api/test?foo=bar&baz=qux',
      });

      expect(response.statusCode).toBe(200);
      expect(global.fetch).toHaveBeenCalledWith(
        'http://localhost:3000/api/test?foo=bar&baz=qux',
        {
          method: 'GET',
          headers: {},
        }
      );
    });

    it('updates lastSeenAt on relay', async () => {
      const oldTimestamp = '2024-01-01T00:00:00.000Z';
      registry.set('nodeA', {
        url: 'http://localhost:3000',
        lastSeenAt: oldTimestamp,
      });

      global.fetch = vi.fn().mockResolvedValue({
        status: 200,
        headers: new Map([['content-type', 'text/plain']]),
        text: async () => 'ok',
      } as unknown as Response);

      await app.inject({
        method: 'GET',
        url: '/hub/relay/nodeA/api/test',
      });

      const entry = registry.get('nodeA');
      expect(entry?.lastSeenAt).not.toBe(oldTimestamp);
      expect(new Date(entry!.lastSeenAt).getTime()).toBeGreaterThan(
        new Date(oldTimestamp).getTime()
      );
    });

    it('returns text response when content-type is not JSON', async () => {
      registry.set('nodeA', {
        url: 'http://localhost:3000',
        lastSeenAt: '2024-01-01T00:00:00.000Z',
      });

      global.fetch = vi.fn().mockResolvedValue({
        status: 200,
        headers: new Map([['content-type', 'text/plain']]),
        text: async () => 'plain text',
      } as unknown as Response);

      const response = await app.inject({
        method: 'GET',
        url: '/hub/relay/nodeA/api/test',
      });

      expect(response.statusCode).toBe(200);
      expect(response.body).toBe('plain text');
    });

    it('handles invalid JSON gracefully', async () => {
      registry.set('nodeA', {
        url: 'http://localhost:3000',
        lastSeenAt: '2024-01-01T00:00:00.000Z',
      });

      global.fetch = vi.fn().mockResolvedValue({
        status: 200,
        headers: new Map([['content-type', 'application/json']]),
        text: async () => 'not valid json',
      } as unknown as Response);

      const response = await app.inject({
        method: 'GET',
        url: '/hub/relay/nodeA/api/test',
      });

      expect(response.statusCode).toBe(200);
      expect(response.body).toBe('not valid json');
    });

    it('returns 502 when relay fails', async () => {
      registry.set('nodeA', {
        url: 'http://localhost:3000',
        lastSeenAt: '2024-01-01T00:00:00.000Z',
      });

      global.fetch = vi
        .fn()
        .mockRejectedValue(new Error('Connection refused'));

      const response = await app.inject({
        method: 'GET',
        url: '/hub/relay/nodeA/api/test',
      });

      expect(response.statusCode).toBe(502);
      const data = response.json();
      expect(data.error).toBe('relay failed');
      expect(data.clientId).toBe('nodeA');
      expect(data.message).toBe('Connection refused');
    });

    it('handles empty path', async () => {
      registry.set('nodeA', {
        url: 'http://localhost:3000',
        lastSeenAt: '2024-01-01T00:00:00.000Z',
      });

      global.fetch = vi.fn().mockResolvedValue({
        status: 200,
        headers: new Map([['content-type', 'text/plain']]),
        text: async () => 'ok',
      } as unknown as Response);

      const response = await app.inject({
        method: 'GET',
        url: '/hub/relay/nodeA/',
      });

      expect(response.statusCode).toBe(200);
      expect(global.fetch).toHaveBeenCalledWith('http://localhost:3000/', {
        method: 'GET',
        headers: {},
      });
    });
  });
});
