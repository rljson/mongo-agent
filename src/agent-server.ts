// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * Agent server for distributed MongoDB synchronization.
 *
 * Provides:
 * - Local change capture via change streams
 * - Bi-directional sync with peer nodes via hub
 * - HTTP API for sync operations
 */

import Fastify, { type FastifyInstance } from 'fastify';
import { MongoClient } from 'mongodb';
import { syncOriginFromHub } from './sync/pull-from-hub.ts';
import { createSuppressor, startDbChangeStream } from './watch-changes.ts';
import { performStartupRecovery } from './startup-recovery.ts';
import { getState } from './sync-state-store.ts';

const PORT = parseInt(process.env.PORT || '3001', 10);
const NODE_ID = process.env.NODE_ID || 'nodeA';
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME || 'syncdb';
const HUB_URL = process.env.HUB_URL;
const PEERS = (process.env.PEERS || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
const SYNC_INTERVAL_MS = parseInt(
  process.env.SYNC_INTERVAL_MS || '2000',
  10
);

/**
 * Options for creating an agent app.
 */
interface AgentAppOptions {
  /** Enable Fastify logging (default: true) */
  logger?: boolean;
  /** MongoDB client instance */
  mongo: MongoClient;
  /** Database name */
  dbName: string;
  /** Node identifier */
  nodeId: string;
  /** Hub URL for registration and relay */
  hubUrl?: string;
  /** List of peer node IDs */
  peers: string[];
}

/**
 * Create and configure an agent server instance.
 * @param options - Configuration options
 * @returns Configured Fastify instance
 */
export function createAgentApp(options: AgentAppOptions): FastifyInstance {
  const { logger = true, mongo, dbName, nodeId, hubUrl, peers } = options;

  const app = Fastify({ logger });

  /**
   * Health check endpoint.
   *
   * GET /health
   */
  app.get('/health', async () => ({
    ok: true,
    nodeId,
  }));

  /**
   * Get sync info for this node.
   *
   * GET /sync/info
   */
  app.get('/sync/info', async () => {
    const db = mongo.db(dbName);
    const syncOps = db.collection('sync_ops');

    const last = await syncOps
      .find({ origin: nodeId })
      .sort({ seq: -1 })
      .limit(1)
      .toArray();
    const head = last[0];

    return {
      nodeId,
      headSeq: head?.seq ?? 0,
      headHash: head?.chainHash ?? 'GENESIS',
    };
  });

  /**
   * Pull operations from a specific origin.
   *
   * POST /sync/pull
   */
  app.post<{ Body: { origin?: string; lastSeqSeen?: number } }>(
    '/sync/pull',
    async (req, reply) => {
      const { origin, lastSeqSeen = 0 } = req.body || {};

      if (!origin) {
        return reply.code(400).send({ error: 'origin is required' });
      }

      const db = mongo.db(dbName);
      const syncOps = db.collection('sync_ops');

      const ops = await syncOps
        .find({ origin, seq: { $gt: lastSeqSeen } })
        .sort({ seq: 1 })
        .toArray();

      return { ops };
    }
  );

  /**
   * Get sync state for a specific origin.
   *
   * GET /sync/state/:origin
   */
  app.get<{ Params: { origin: string } }>(
    '/sync/state/:origin',
    async (req) => {
      const db = mongo.db(dbName);
      const state = await getState(db, req.params.origin);

      return {
        ok: true,
        origin: req.params.origin,
        lastSeqPulled: state.lastSeqPulled,
        lastHashPulled: state.lastHashPulled,
        lastSeqApplied: state.lastSeqApplied,
        lastHashApplied: state.lastHashApplied,
      };
    }
  );

  /**
   * Register this node at the hub.
   */
  async function registerAtHub(): Promise<void> {
    if (!hubUrl) {
      app.log.warn('HUB_URL not set, skipping hub registration');
      return;
    }

    const selfUrl = `http://${nodeId === 'nodeA' ? 'agenta' : 'agentb'}:${PORT}`;

    const resp = await fetch(`${hubUrl}/hub/register`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        clientId: nodeId,
        url: selfUrl,
      }),
    });

    if (!resp.ok) {
      throw new Error(
        `hub register failed: ${resp.status} ${await resp.text()}`
      );
    }

    app.log.info({ nodeId, selfUrl }, 'registered at hub');
  }

  /**
   * Poll all peer nodes for new operations.
   */
  async function pollPeers(): Promise<void> {
    if (!hubUrl) {
      app.log.warn('HUB_URL not set, skipping peer polling');
      return;
    }

    for (const peer of peers) {
      if (peer === nodeId) continue;

      try {
        await syncOriginFromHub({
          fastify: app,
          mongo,
          dbName,
          localNodeId: nodeId,
          hubUrl,
          peerClientId: peer,
          origin: peer,
        });
      } catch (err) {
        app.log.warn(
          {
            peer,
            message: err instanceof Error ? err.message : String(err),
          },
          'sync poll failed'
        );
      }
    }
  }

  /**
   * Start the agent's background tasks.
   */
  async function startBackgroundTasks(): Promise<void> {
    const db = mongo.db(dbName);

    // Register at hub
    await registerAtHub();

    // Perform startup recovery checks (resume token validation)
    await performStartupRecovery({
      db,
      nodeId,
      logger: app.log,
    });

    // Start change stream to capture local changes
    const suppressor = createSuppressor();
    startDbChangeStream({
      db,
      nodeId,
      logger: app.log,
      suppressor,
    });

    // Poll peers periodically
    setInterval(() => {
      pollPeers().catch((err) => {
        app.log.warn(
          { message: err instanceof Error ? err.message : String(err) },
          'pollPeers interval failed'
        );
      });
    }, SYNC_INTERVAL_MS);
  }

  // Attach background tasks starter to the app
  (app as FastifyInstance & { startBackgroundTasks?: () => Promise<void> }).startBackgroundTasks = startBackgroundTasks;

  return app;
}

/* v8 ignore start */
/**
 * Start the agent server.
 */
async function main(): Promise<void> {
  if (!MONGO_URI) {
    throw new Error('MONGO_URI is required');
  }

  const mongo = new MongoClient(MONGO_URI);
  await mongo.connect();

  const app = createAgentApp({
    mongo,
    dbName: DB_NAME,
    nodeId: NODE_ID,
    hubUrl: HUB_URL,
    peers: PEERS,
  });

  await app.listen({ host: '0.0.0.0', port: PORT });
  app.log.info({ port: PORT, nodeId: NODE_ID }, 'agent started');

  // Start background tasks
  const startTasks = (app as FastifyInstance & { startBackgroundTasks?: () => Promise<void> }).startBackgroundTasks;
  if (startTasks) {
    await startTasks();
  }
}

// Only start the server if this module is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
/* v8 ignore stop */
