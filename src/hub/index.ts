// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * Hub server for distributed MongoDB synchronization.
 *
 * Provides a central registry and relay service for distributed agents:
 * - Client registration and discovery
 * - Request relay to registered clients
 * - Health monitoring
 */

import Fastify, { type FastifyInstance } from 'fastify';

/** Default port for the hub server */
const PORT = parseInt(process.env.PORT || '3200', 10);

/**
 * Registry entry for a client node.
 */
export interface RegistryEntry {
  /** Base URL of the client */
  url: string;
  /** ISO timestamp of last activity */
  lastSeenAt: string;
}

/**
 * Request body for client registration.
 */
interface RegisterBody {
  /** Unique identifier for the client */
  clientId: string;
  /** Base URL where the client can be reached */
  url: string;
}

/**
 * Client information returned by the API.
 */
interface ClientInfo {
  /** Unique identifier for the client */
  clientId: string;
  /** Base URL where the client can be reached */
  url: string;
  /** ISO timestamp of last activity */
  lastSeenAt: string;
}

/**
 * Options for creating a hub app.
 */
interface HubAppOptions {
  /** Enable Fastify logging (default: true) */
  logger?: boolean;
}

/**
 * Create and configure a hub server instance.
 * @param options - Configuration options
 * @returns Configured Fastify instance and registry
 */
export function createHubApp(options: HubAppOptions = {}): {
  app: FastifyInstance;
  registry: Map<string, RegistryEntry>;
} {
  const app = Fastify({ logger: options.logger ?? true });
  const registry = new Map<string, RegistryEntry>();

  /**
   * Register a new client node.
   *
   * POST /hub/register
   * @example
   * ```
   * POST /hub/register
   * { "clientId": "nodeA", "url": "http://localhost:3000" }
   * ```
   */
  app.post<{ Body: RegisterBody }>('/hub/register', async (req, reply) => {
    const { clientId, url } = req.body || {};

    if (!clientId || !url) {
      return reply.code(400).send({ error: 'clientId and url are required' });
    }

    registry.set(clientId, {
      url,
      lastSeenAt: new Date().toISOString(),
    });

    app.log.info({ clientId, url }, 'client registered');
    return { ok: true, clientId, url };
  });

  /**
   * Unregister a client node.
   *
   * DELETE /hub/unregister/:clientId
   * @example
   * ```
   * DELETE /hub/unregister/nodeA
   * ```
   */
  app.delete<{ Params: { clientId: string } }>(
    '/hub/unregister/:clientId',
    async (req) => {
      const clientId = req.params.clientId;
      registry.delete(clientId);
      return { ok: true };
    }
  );

  /**
   * List all registered clients.
   *
   * GET /hub/clients
   * @example
   * ```
   * GET /hub/clients
   * { "clients": [{ "clientId": "nodeA", "url": "...", "lastSeenAt": "..." }] }
   * ```
   */
  app.get('/hub/clients', async () => {
    const clients: ClientInfo[] = [];

    for (const [clientId, entry] of registry.entries()) {
      clients.push({
        clientId,
        url: entry.url,
        lastSeenAt: entry.lastSeenAt,
      });
    }

    clients.sort((a, b) => a.clientId.localeCompare(b.clientId));
    return { clients };
  });

  /**
   * Relay any request to a client.
   *
   * ALL /hub/relay/:clientId/*
   * @example
   * ```
   * GET  /hub/relay/nodeA/sync/info
   * POST /hub/relay/nodeB/sync/pull
   * ```
   */
  app.all<{ Params: { clientId: string; '*': string } }>(
    '/hub/relay/:clientId/*',
    async (req, reply) => {
      const clientId = req.params.clientId;
      const entry = registry.get(clientId);

      if (!entry) {
        return reply.code(404).send({ error: `unknown clientId: ${clientId}` });
      }

      const tailPath = req.params['*'] || '';

      // Important: preserve query string from original request
      const rawUrl = req.raw.url || req.url || '';
      const qIndex = rawUrl.indexOf('?');
      const query = qIndex >= 0 ? rawUrl.slice(qIndex) : '';

      const targetUrl = `${entry.url}/${tailPath}${query}`;

      // Update lastSeenAt for this client
      registry.set(clientId, {
        url: entry.url,
        lastSeenAt: new Date().toISOString(),
      });

      const method = req.method;

      let body: string | undefined;
      if (method !== 'GET' && method !== 'HEAD' && req.body !== undefined) {
        body = JSON.stringify(req.body);
      }

      const headers: Record<string, string> = {};
      const contentType = req.headers['content-type'];
      if (contentType) {
        headers['content-type'] = contentType;
      }

      try {
        const resp = await fetch(targetUrl, {
          method,
          headers,
          ...(body ? { body } : {}),
        });

        const respCt = resp.headers.get('content-type') || '';
        const text = await resp.text();

        reply.code(resp.status);
        if (respCt) {
          reply.header('content-type', respCt);
        }

        app.log.info(
          {
            clientId,
            method,
            targetUrl,
            statusCode: resp.status,
          },
          'relay ok'
        );

        // Return JSON as JSON, otherwise text
        if (respCt.includes('application/json')) {
          try {
            return JSON.parse(text);
          } catch (err) {
            app.log.warn(
              {
                clientId,
                targetUrl,
                message: err instanceof Error ? err.message : String(err),
              },
              'relay response claimed JSON but could not be parsed'
            );
            return text;
          }
        }

        return text;
      } catch (err) {
        app.log.warn(
          {
            clientId,
            targetUrl,
            message: err instanceof Error ? err.message : String(err),
          },
          'relay failed'
        );

        return reply.code(502).send({
          error: 'relay failed',
          clientId,
          targetUrl,
          message: err instanceof Error ? err.message : String(err),
        });
      }
    }
  );

  /**
   * Health check endpoint.
   *
   * GET /health
   */
  app.get('/health', async () => ({ ok: true }));

  return { app, registry };
}

/* v8 ignore start */
/**
 * Start the hub server.
 */
async function main(): Promise<void> {
  const { app } = createHubApp();
  await app.listen({ host: '0.0.0.0', port: PORT });
  app.log.info({ port: PORT }, 'hub started');
}

// Only start the server if this module is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
/* v8 ignore stop */

