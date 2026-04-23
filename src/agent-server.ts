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

import { EJSON } from 'bson';
import Fastify, { FastifyInstance } from 'fastify';
import { MongoClient } from 'mongodb';
import { pathToFileURL } from 'node:url';

import { createLockManager } from './lock-manager.ts';
import { performStartupRecovery } from './startup-recovery.ts';
import { getState } from './sync-state-store.ts';
import { syncOriginFromHub } from './sync/pull-from-hub.ts';
import { syncRljsonTreeFromHub } from './sync/rljson-hub-sync.ts';
import {
  applyRljsonTree, extractRljsonTree, getRljsonSyncState, RljsonTreePayload
} from './sync/rljson-sync.ts';
import { createSuppressor, startDbChangeStream } from './watch-changes.ts';


const PORT = parseInt(process.env.PORT || '3001', 10);
const NODE_ID = process.env.NODE_ID || 'nodeA';
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME || 'syncdb';
const HUB_URL = process.env.HUB_URL;
const PEERS = (process.env.PEERS || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
const SYNC_INTERVAL_MS = parseInt(process.env.SYNC_INTERVAL_MS || '2000', 10);
// Enable RLJSON mode: sync using hashes and tree structures instead of raw JSON operations
const USE_RLJSON_SYNC = process.env.USE_RLJSON_SYNC === 'true';

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
  /** Use RLJSON sync mode (hash-based) instead of operation-based sync */
  useRljsonSync?: boolean;
  /** Lock manager instance (optional) */
  lockManager?: ReturnType<typeof createLockManager>;
}

/**
 * Create and configure an agent server instance.
 * @param options - Configuration options
 * @returns Configured Fastify instance
 */
export function createAgentApp(options: AgentAppOptions): FastifyInstance {
  const {
    logger = true,
    mongo,
    dbName,
    nodeId,
    hubUrl,
    peers,
    useRljsonSync = false,
  } = options;

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

      // Serialize via canonical EJSON so BSON types (Timestamp, ObjectId,
      // Date, Long) survive JSON transport and can be reconstructed by
      // EJSON.deserialize on the consuming peer.
      return { ops: ops.map((op) => EJSON.serialize(op)) };
    },
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
    },
  );

  // =========================================================================
  // RLJSON Sync Endpoints (Hash-based synchronization)
  // =========================================================================

  /**
   * Get current RLJSON tree representation of this node's database.
   * Returns tree structure with hashes instead of raw JSON data.
   *
   * GET /rljson/tree
   */
  app.get('/rljson/tree', async () => {
    const db = mongo.db(dbName);

    try {
      const payload = await extractRljsonTree({
        mongoDb: db,
        nodeId,
      });

      app.log.info(
        {
          rootHash: payload.rootHash,
          totalNodes: payload.totalNodes,
          totalBlobs: payload.blobs.length,
        },
        'RLJSON tree extracted',
      );

      return {
        ok: true,
        payload,
      };
    } catch (error) {
      app.log.error({ error }, 'Failed to extract RLJSON tree');
      throw error;
    }
  });

  /**
   * Accept and apply RLJSON tree from a peer.
   * This replaces raw JSON sync with hash-based tree sync.
   *
   * POST /rljson/sync
   */
  app.post<{ Body: RljsonTreePayload }>('/rljson/sync', async (req, reply) => {
    const payload = req.body;

    if (!payload || !payload.rootHash) {
      return reply.code(400).send({ error: 'Invalid payload' });
    }

    const db = mongo.db(dbName);

    try {
      const result = await applyRljsonTree({
        mongoDb: db,
        payload,
      });

      app.log.info(
        {
          origin: payload.origin,
          rootHash: result.rootHash,
          nodesApplied: result.nodesApplied,
          blobsReceived: result.blobsReceived,
        },
        'RLJSON tree applied',
      );

      return {
        ok: true,
        result,
      };
    } catch (error) {
      app.log.error({ error }, 'Failed to apply RLJSON tree');
      return reply.code(500).send({
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  });

  /**
   * Get RLJSON sync state for a specific origin.
   * Shows last synced root hash and statistics.
   *
   * GET /rljson/state/:origin
   */
  app.get<{ Params: { origin: string } }>(
    '/rljson/state/:origin',
    async (req) => {
      const db = mongo.db(dbName);
      const state = await getRljsonSyncState(db, req.params.origin);

      if (!state) {
        return {
          ok: true,
          origin: req.params.origin,
          synced: false,
        };
      }

      return {
        ok: true,
        origin: req.params.origin,
        synced: true,
        lastRootHash: state.lastRootHash,
        lastSyncedAt: state.lastSyncedAt,
        totalNodes: state.totalNodes,
        totalBlobs: state.totalBlobs,
      };
    },
  );

  // =========================================================================
  // Lock Management Endpoints (Distributed Record Locking)
  // =========================================================================

  if (options.lockManager) {
    /**
     * Acquire a lock on a record
     *
     * POST /lock/acquire
     * Body: { typ: number, value: string, name?: string, email?: string }
     */
    app.post<{
      Body: {
        typ: number;
        value: string;
        name?: string;
        email?: string;
      };
    }>('/lock/acquire', async (req, reply) => {
      const { typ, value, name, email } = req.body;

      if (typ === undefined || !value) {
        return reply.code(400).send({ error: 'typ and value are required' });
      }

      try {
        const acquired = await options.lockManager!.acquireLock({
          typ,
          value,
          key: nodeId,
          name: name || nodeId,
          compName: process.env.HOSTNAME || 'unknown',
          eMail: email,
        });

        return {
          ok: true,
          acquired,
          lockId: `${typ}-${value}`,
          lockedBy: acquired ? nodeId : undefined,
        };
      } catch (error) {
        app.log.error({ error, typ, value }, 'Failed to acquire lock');
        return reply.code(500).send({
          ok: false,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    });

    /**
     * Release a lock on a record
     *
     * POST /lock/release
     * Body: { typ: number, value: string }
     */
    app.post<{ Body: { typ: number; value: string } }>(
      '/lock/release',
      async (req, reply) => {
        const { typ, value } = req.body;

        if (typ === undefined || !value) {
          return reply.code(400).send({ error: 'typ and value are required' });
        }

        try {
          const released = await options.lockManager!.releaseLock(
            typ,
            value,
            nodeId,
          );

          return {
            ok: true,
            released,
            lockId: `${typ}-${value}`,
          };
        } catch (error) {
          app.log.error({ error, typ, value }, 'Failed to release lock');
          return reply.code(500).send({
            ok: false,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      },
    );

    /**
     * Check if a record is locked
     *
     * GET /lock/status/:typ/:value
     */
    app.get<{ Params: { typ: string; value: string } }>(
      '/lock/status/:typ/:value',
      async (req, reply) => {
        const typ = parseInt(req.params.typ, 10);
        const { value } = req.params;

        if (isNaN(typ) || !value) {
          return reply.code(400).send({ error: 'Invalid typ or value' });
        }

        try {
          const lock = await options.lockManager!.isLocked(typ, value);

          return {
            ok: true,
            locked: !!lock,
            lock: lock
              ? {
                  lockId: lock._id,
                  lockedBy: lock.key,
                  lockedByName: lock.name,
                  acquiredAt: lock.commonFields.createdAt,
                }
              : null,
          };
        } catch (error) {
          app.log.error({ error, typ, value }, 'Failed to check lock status');
          return reply.code(500).send({
            ok: false,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      },
    );

    /**
     * Record an offline change
     *
     * POST /lock/offline-change
     * Body: { typ: number, value: string, changeData: any, collection: string, database: string }
     */
    app.post<{
      Body: {
        typ: number;
        value: string;
        changeData: any;
        collection: string;
        database: string;
      };
    }>('/lock/offline-change', async (req, reply) => {
      const { typ, value, changeData, collection, database } = req.body;

      if (typ === undefined || !value || !changeData || !collection) {
        return reply.code(400).send({
          error: 'typ, value, changeData, and collection are required',
        });
      }

      try {
        await options.lockManager!.recordOfflineChange(
          typ,
          value,
          nodeId,
          changeData,
          collection,
          database || dbName,
        );

        return {
          ok: true,
          recorded: true,
          nodeId,
        };
      } catch (error) {
        app.log.error({ error, typ, value }, 'Failed to record offline change');
        return reply.code(500).send({
          ok: false,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    });

    /**
     * Detect offline conflicts for this node
     *
     * GET /lock/detect-conflicts
     */
    app.get('/lock/detect-conflicts', async (req, reply) => {
      try {
        const conflicts =
          await options.lockManager!.detectOfflineConflicts(nodeId);

        const conflictCount =
          await options.lockManager!.createConflictRecords(conflicts);

        if (conflictCount > 0) {
          await options.lockManager!.clearOfflineChanges(nodeId);
        }

        return {
          ok: true,
          conflictsDetected: conflicts.length,
          conflictsCreated: conflictCount,
        };
      } catch (error) {
        app.log.error({ error }, 'Failed to detect conflicts');
        return reply.code(500).send({
          ok: false,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    });

    /**
     * Get offline changes for this node
     *
     * GET /lock/offline-changes
     */
    app.get('/lock/offline-changes', async (req, reply) => {
      try {
        const changes = await options.lockManager!.getOfflineChanges(nodeId);

        return {
          ok: true,
          nodeId,
          offlineChanges: changes,
          count: changes.length,
        };
      } catch (error) {
        app.log.error({ error }, 'Failed to get offline changes');
        return reply.code(500).send({
          ok: false,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    });

    app.log.info('Lock management endpoints registered');
  }

  /**
   * Register this node at the hub.
   */
  async function registerAtHub(): Promise<void> {
    if (!hubUrl) {
      app.log.warn('HUB_URL not set, skipping hub registration');
      return;
    }

    // Get the actual network IP address from server addresses
    const addresses = app.addresses();
    let selfUrl: string;

    // Explicit overrides first (set in .env when auto-detect picks the wrong NIC,
    // e.g. when a VPN like Barracuda is enumerated before the real LAN interface).
    const explicitUrl = process.env.AGENT_URL;
    const explicitIp = process.env.AGENT_IP;

    if (explicitUrl) {
      selfUrl = explicitUrl;
    } else if (explicitIp) {
      selfUrl = `http://${explicitIp}:${PORT}`;
    } else {
      // app.addresses() returns AddressInfo objects ({ address, port, family }),
      // not URL strings. Find the first non-loopback, non-VPN IPv4 address.
      const networkAddress = addresses.find((addr) => {
        if (typeof addr === 'string') return false;
        const ip = addr.address;
        return (
          (ip.startsWith('192.168.') ||
            ip.startsWith('10.') ||
            (ip.startsWith('172.') &&
              (() => {
                const second = parseInt(ip.split('.')[1] || '0', 10);
                return second >= 16 && second <= 31;
              })())) &&
          ip !== '127.0.0.1'
        );
      });

      if (networkAddress && typeof networkAddress !== 'string') {
        selfUrl = `http://${networkAddress.address}:${networkAddress.port}`;
      } else {
        selfUrl = `http://localhost:${PORT}`;
        app.log.warn(
          { selfUrl },
          'auto-detected selfUrl is localhost; set AGENT_URL or AGENT_IP in .env so peers can reach this agent',
        );
      }
    }

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
        `hub register failed: ${resp.status} ${await resp.text()}`,
      );
    }

    app.log.info({ nodeId, selfUrl }, 'registered at hub');
  }

  /**
   * Poll all peer nodes for new data.
   * Uses either RLJSON mode (hash-based tree sync) or legacy mode (operation-based sync).
   */
  async function pollPeers(): Promise<void> {
    if (!hubUrl) {
      app.log.warn('HUB_URL not set, skipping peer polling');
      return;
    }

    for (const peer of peers) {
      if (peer === nodeId) continue;

      try {
        if (useRljsonSync) {
          // RLJSON mode: Sync using hash-based tree structures
          app.log.info(
            { peer, mode: 'RLJSON' },
            'Syncing from peer (RLJSON mode)',
          );

          await syncRljsonTreeFromHub({
            fastify: app,
            hubUrl,
            peerClientId: peer,
            localNodeId: nodeId,
          });
        } else {
          // Legacy mode: Sync using individual operations
          app.log.info(
            { peer, mode: 'LEGACY' },
            'Syncing from peer (legacy mode)',
          );

          await syncOriginFromHub({
            fastify: app,
            mongo,
            dbName,
            localNodeId: nodeId,
            hubUrl,
            peerClientId: peer,
            origin: peer,
          });
        }
      } catch (err) {
        app.log.warn(
          {
            peer,
            mode: useRljsonSync ? 'RLJSON' : 'LEGACY',
            message: err instanceof Error ? err.message : String(err),
          },
          'sync poll failed',
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
          'pollPeers interval failed',
        );
      });
    }, SYNC_INTERVAL_MS);
  }

  // Attach background tasks starter to the app
  (
    app as FastifyInstance & { startBackgroundTasks?: () => Promise<void> }
  ).startBackgroundTasks = startBackgroundTasks;

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

  // Initialize lock manager for distributed locking and offline conflict detection
  const db = mongo.db(DB_NAME);
  const lockManager = createLockManager(db);
  await lockManager.initialize();
  console.log(
    '✓ Lock manager initialized (collections: locking, lock_history, offline_changes)',
  );

  const app = createAgentApp({
    mongo,
    dbName: DB_NAME,
    nodeId: NODE_ID,
    hubUrl: HUB_URL,
    peers: PEERS,
    useRljsonSync: USE_RLJSON_SYNC,
    lockManager,
  });

  await app.listen({ host: '0.0.0.0', port: PORT });

  const mode = USE_RLJSON_SYNC
    ? 'RLJSON (hash-based)'
    : 'LEGACY (operation-based)';
  app.log.info(
    {
      port: PORT,
      nodeId: NODE_ID,
      syncMode: mode,
      rljsonEnabled: USE_RLJSON_SYNC,
    },
    'agent started',
  );

  // Start background tasks
  const startTasks = (
    app as FastifyInstance & { startBackgroundTasks?: () => Promise<void> }
  ).startBackgroundTasks;
  if (startTasks) {
    await startTasks();
  }
}

// Only start the server if this module is executed directly
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
/* v8 ignore stop */
