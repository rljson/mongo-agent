/**
 * Simple Backend API for Testing Conflict Resolution UI
 * This is a minimal version for demonstration purposes
 */

import { spawn } from 'node:child_process';
import * as fs from 'node:fs';
import * as http from 'node:http';
import * as path from 'node:path';
import * as url from 'node:url';

import cors from 'cors';
import express from 'express';
import { Db, MongoClient, ObjectId } from 'mongodb';

/**
 * Document ids in `sync_conflicts.documentId` are stored as their hex
 * string. The actual collection's `_id` is typically a BSON ObjectId, so
 * matching on the raw string never finds the real doc and (with upsert)
 * inserts a phantom string-keyed copy. Rehydrate to ObjectId when the
 * value looks like a 24-char hex; pass non-hex ids through unchanged.
 */
function resolveDocId(id: unknown): unknown {
  if (typeof id === 'string' && /^[a-f0-9]{24}$/i.test(id)) {
    return new ObjectId(id);
  }
  return id;
}


const app = express();
const port = 3000;

/**
 * Repo root used as cwd when spawning hub/agent processes. Defaults to two
 * levels above this file (project root) but can be overridden via env for
 * portability.
 */
const REPO_ROOT =
  process.env.REPO_ROOT ||
  path.resolve(path.dirname(url.fileURLToPath(import.meta.url)), '..');

const HUB_PORT = 3200;
const L1_AGENT_PORT = 3001;
const HUB_HOST = '127.0.0.1';

// Middleware
app.use(cors());
app.use(express.json());

/**
 * Probe a TCP port over HTTP. Returns true if anything answers within
 * 1 second. Used by the dashboard status endpoint.
 */
function probeHttp(host: string, portNum: number, urlPath: string): Promise<boolean> {
  return new Promise((resolve) => {
    const req = http.get(
      { host, port: portNum, path: urlPath, timeout: 1500 },
      (res) => {
        res.resume();
        resolve((res.statusCode ?? 500) < 500);
      },
    );
    req.on('error', () => resolve(false));
    req.on('timeout', () => {
      req.destroy();
      resolve(false);
    });
  });
}

/**
 * Spawns a Node process detached from this server with stdout/stderr piped
 * to a log file. Returns the PID. The child survives this server stopping.
 */
function spawnDetachedNode(
  args: string[],
  logBasename: string,
  extraEnv: Record<string, string> = {},
): { pid: number | undefined } {
  const outPath = path.join(REPO_ROOT, `${logBasename}.out`);
  const errPath = path.join(REPO_ROOT, `${logBasename}.err`);
  const out = fs.openSync(outPath, 'a');
  const err = fs.openSync(errPath, 'a');
  const child = spawn('node', args, {
    cwd: REPO_ROOT,
    detached: true,
    stdio: ['ignore', out, err],
    env: { ...process.env, ...extraEnv },
    windowsHide: true,
  });
  child.unref();
  return { pid: child.pid };
}

// Track L2 agent's running state — port 3002 is on the remote laptop, can't
// be probed locally without WinRM. We rely on the hub's `lastSeenAt` for it.
async function l2AgentUp(): Promise<boolean> {
  if (!(await probeHttp(HUB_HOST, HUB_PORT, '/hub/clients'))) return false;
  try {
    const resp = await fetch(`http://${HUB_HOST}:${HUB_PORT}/hub/clients`);
    if (!resp.ok) return false;
    const data = (await resp.json()) as {
      clients?: Array<{ clientId: string; lastSeenAt: string }>;
    };
    const l2 = (data.clients || []).find((c) => c.clientId === 'laptop2');
    if (!l2) return false;
    // Stale-tolerant: hub considers a client live if seen within ~10s.
    const seen = Date.parse(l2.lastSeenAt);
    return !Number.isNaN(seen) && Date.now() - seen < 10_000;
  } catch {
    return false;
  }
}

let mongoClient: MongoClient;
let db: Db;

// Mock data for testing
const mockConflicts = new Map();

/**
 * Initialize MongoDB connection
 */
async function initializeMongoDB() {
  const mongoUrl = process.env.MONGO_URI || 'mongodb://mongoa:27017';
  const dbName = process.env.DB_NAME || 'test_offline_persistence';
  mongoClient = new MongoClient(mongoUrl);
  await mongoClient.connect();

  db = mongoClient.db(dbName);

  console.log(`✅ MongoDB connected (db=${dbName})`);

  // Create indexes for better query performance
  await db.collection('sync_conflicts').createIndex({ status: 1 });
  await db.collection('sync_conflicts').createIndex({ detectedAt: -1 });

  // Check for existing conflicts
  const conflictCount = await db.collection('sync_conflicts').countDocuments();
  console.log(`📊 Found ${conflictCount} conflicts in database`);

  // Create some sample conflicts for testing if none exist
  if (conflictCount === 0) {
    createSampleConflicts();
  }
}

/**
 * Create sample conflicts for UI testing
 */
function createSampleConflicts() {
  const conflict1 = {
    conflictId: 'conflict-001',
    documentId: 'user-alice-123',
    collection: 'users',
    database: 'rljson-sync',
    detectedAt: Date.now() - 300000, // 5 minutes ago
    status: 'pending',
    conflictType: 'concurrent-update',
    versions: [
      {
        documentId: 'user-alice-123',
        data: {
          _id: 'user-alice-123',
          name: 'Alice Johnson',
          age: 30,
          email: 'alice@example.com',
          status: 'active',
          department: 'Engineering',
        },
        timestamp: Date.now() - 300000,
        nodeId: 'node-a',
        operationId: 'op-001',
        operationType: 'update',
        stateHash: 'hash-a1b2c3',
        componentsHash: 'comp-d4e5f6',
      },
      {
        documentId: 'user-alice-123',
        data: {
          _id: 'user-alice-123',
          name: 'Alice Smith-Johnson',
          age: 31,
          email: 'alice.johnson@company.com',
          status: 'active',
          department: 'Engineering',
          role: 'Senior Developer',
        },
        timestamp: Date.now() - 299000,
        nodeId: 'node-b',
        operationId: 'op-002',
        operationType: 'update',
        stateHash: 'hash-g7h8i9',
        componentsHash: 'comp-j1k2l3',
      },
    ],
  };

  const conflict2 = {
    conflictId: 'conflict-002',
    documentId: 'user-bob-456',
    collection: 'users',
    database: 'rljson-sync',
    detectedAt: Date.now() - 120000, // 2 minutes ago
    status: 'pending',
    conflictType: 'concurrent-update',
    versions: [
      {
        documentId: 'user-bob-456',
        data: {
          _id: 'user-bob-456',
          name: 'Bob Wilson',
          age: 28,
          email: 'bob@example.com',
          status: 'inactive',
        },
        timestamp: Date.now() - 120000,
        nodeId: 'node-a',
        operationId: 'op-003',
        operationType: 'update',
        stateHash: 'hash-m4n5o6',
        componentsHash: 'comp-p7q8r9',
      },
      {
        documentId: 'user-bob-456',
        data: {
          _id: 'user-bob-456',
          name: 'Bob Wilson',
          age: 28,
          email: 'bob@example.com',
          status: 'active',
          lastLogin: Date.now(),
        },
        timestamp: Date.now() - 118000,
        nodeId: 'node-c',
        operationId: 'op-004',
        operationType: 'update',
        stateHash: 'hash-s1t2u3',
        componentsHash: 'comp-v4w5x6',
      },
    ],
  };

  mockConflicts.set(conflict1.conflictId, conflict1);
  mockConflicts.set(conflict2.conflictId, conflict2);

  console.log(`✅ Created ${mockConflicts.size} sample conflicts for testing`);
}

// ==================== API ENDPOINTS ====================

/**
 * GET /api/conflicts
 */
app.get('/api/conflicts', async (req, res) => {
  try {
    const status = req.query.status as string | undefined;
    const query = status ? { status } : {};

    // Query both mock conflicts and real MongoDB conflicts
    const dbConflicts = await db
      .collection('sync_conflicts')
      .find(query)
      .sort({ detectedAt: -1 })
      .toArray();

    // Also include mock conflicts for demo purposes
    let mockConflictsList = Array.from(mockConflicts.values());
    if (status) {
      mockConflictsList = mockConflictsList.filter((c) => c.status === status);
    }

    // Combine both sources
    const allConflicts = [...dbConflicts, ...mockConflictsList];

    res.json(allConflicts);
  } catch (error) {
    console.error('Error fetching conflicts:', error);
    res.status(500).json({ error: 'Failed to fetch conflicts' });
  }
});

/**
 * GET /api/conflicts/:id
 */
app.get('/api/conflicts/:id', async (req, res) => {
  try {
    // Try to find in MongoDB first
    const dbConflict = await db.collection('sync_conflicts').findOne({
      conflictId: req.params.id,
    });

    if (dbConflict) {
      return res.json(dbConflict);
    }

    // Fall back to mock data
    const mockConflict = mockConflicts.get(req.params.id);

    if (!mockConflict) {
      return res.status(404).json({ error: 'Conflict not found' });
    }

    res.json(mockConflict);
  } catch (error) {
    console.error('Error fetching conflict:', error);
    res.status(500).json({ error: 'Failed to fetch conflict' });
  }
});

/**
 * POST /api/conflicts/resolve
 */
app.post('/api/conflicts/resolve', async (req, res) => {
  try {
    const resolution = req.body;

    // Try to find and update in MongoDB first
    const dbConflict = await db.collection('sync_conflicts').findOne({
      conflictId: resolution.conflictId,
    });

    if (dbConflict) {
      // Apply the resolved document back to the collection. The resolution
      // semantics:
      //   - mergedDocument is an object  → upsert that doc
      //   - mergedDocument is null AND user picked the delete-side of an
      //     update-delete conflict → delete the doc here so the choice
      //     propagates (change-stream → sync_op → other laptop's
      //     applyOneOp resolution branch picks it up by `pendingConflict`).
      const resolvedDoc =
        resolution.mergedDocument !== undefined
          ? resolution.mergedDocument
          : resolution.selectedVersion?.data;
      const pickedDeleteSide =
        resolvedDoc === null || resolvedDoc === undefined;

      const collectionName = dbConflict.collection || 'articles';
      const collection = db.collection(collectionName);
      const matchId = resolveDocId(dbConflict.documentId);

      if (pickedDeleteSide) {
        const r = await collection.deleteOne({ _id: matchId } as Record<
          string,
          unknown
        >);
        console.log(
          `🗑️  Resolved by delete: ${collectionName}._id=${dbConflict.documentId} (matched=${r.deletedCount})`,
        );
      } else if (resolvedDoc) {
        await collection.updateOne(
          { _id: matchId } as Record<string, unknown>,
          { $set: { ...resolvedDoc, _id: matchId } },
          { upsert: true },
        );

        console.log(
          `✅ Applied resolved document to ${collectionName} collection`,
        );
      }

      // Update conflict status. Only the status flips here — any audit
      // trail (who/when/which strategy/etc.) belongs in a separate
      // collection if/when we add one.
      await db.collection('sync_conflicts').updateOne(
        { conflictId: resolution.conflictId },
        { $set: { status: 'resolved' } },
      );

      console.log(
        `✅ Resolved conflict: ${resolution.conflictId} using ${resolution.resolutionType}`,
      );

      return res.json({
        success: true,
        message: 'Conflict resolved and applied successfully',
      });
    }

    // Fall back to mock data
    const conflict = mockConflicts.get(resolution.conflictId);

    if (!conflict) {
      return res.status(404).json({
        success: false,
        error: 'Conflict not found',
      });
    }

    // Mark as resolved
    conflict.status = 'resolved';
    conflict.resolution = resolution;
    mockConflicts.set(conflict.conflictId, conflict);

    console.log(
      `✅ Resolved conflict: ${resolution.conflictId} using ${resolution.resolutionType}`,
    );

    res.json({
      success: true,
      message: 'Conflict resolved successfully',
    });
  } catch (error) {
    console.error('Error resolving conflict:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to resolve conflict',
    });
  }
});

/**
 * GET /api/agents/status
 */
app.get('/api/agents/status', (req, res) => {
  try {
    const statuses = [
      {
        nodeId: 'node-a',
        lastSync: Date.now() - 30000,
        stateHash: 'hash-current-a',
        pendingOperations: 2,
        isOnline: true,
      },
      {
        nodeId: 'node-b',
        lastSync: Date.now() - 45000,
        stateHash: 'hash-current-b',
        pendingOperations: 1,
        isOnline: true,
      },
      {
        nodeId: 'node-c',
        lastSync: Date.now() - 60000,
        stateHash: 'hash-current-c',
        pendingOperations: 0,
        isOnline: true,
      },
    ];

    res.json(statuses);
  } catch (error) {
    console.error('Error fetching agent status:', error);
    res.status(500).json({ error: 'Failed to fetch agent status' });
  }
});

/**
 * GET /api/documents/:id/history
 */
app.get('/api/documents/:id/history', async (req, res) => {
  try {
    const documentId = req.params.id;

    // Return mock history
    const history = [
      {
        documentId,
        operationType: 'insert',
        timestamp: Date.now() - 500000,
        nodeId: 'node-a',
        operationId: 'op-000',
      },
      {
        documentId,
        operationType: 'update',
        timestamp: Date.now() - 300000,
        nodeId: 'node-a',
        operationId: 'op-001',
      },
      {
        documentId,
        operationType: 'update',
        timestamp: Date.now() - 299000,
        nodeId: 'node-b',
        operationId: 'op-002',
      },
    ];

    res.json(history);
  } catch (error) {
    console.error('Error fetching document history:', error);
    res.status(500).json({ error: 'Failed to fetch document history' });
  }
});

/**
 * GET /api/conflicts/:id/verify-chain
 */
app.get('/api/conflicts/:id/verify-chain', (req, res) => {
  try {
    const conflict = mockConflicts.get(req.params.id);

    if (!conflict) {
      return res.status(404).json({ error: 'Conflict not found' });
    }

    // Mock verification - in real implementation, verify hash chain
    const verificationResults = conflict.versions.map((v: any) => ({
      nodeId: v.nodeId,
      operationId: v.operationId,
      valid: true,
    }));

    res.json({
      valid: true,
      details: verificationResults,
    });
  } catch (error) {
    console.error('Error verifying hash chain:', error);
    res.status(500).json({ error: 'Failed to verify hash chain' });
  }
});

/**
 * POST /api/sync/trigger
 */
app.post('/api/sync/trigger', (req, res) => {
  try {
    console.log('🔄 Sync triggered manually');
    res.json({ success: true });
  } catch (error) {
    console.error('Error triggering sync:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to trigger sync',
    });
  }
});

// ==================== SERVICE CONTROL ====================
// Dashboard "Start" buttons hit these. Each start endpoint is idempotent —
// returns the existing service if it's already up, otherwise spawns it
// detached and waits ~3s for it to bind its port before reporting back.

/**
 * GET /api/services/status
 * Returns liveness of hub, L1 agent, and L2 agent for the dashboard pills.
 */
app.get('/api/services/status', async (_req, res) => {
  try {
    const [hub, l1, l2] = await Promise.all([
      probeHttp(HUB_HOST, HUB_PORT, '/hub/clients'),
      probeHttp(HUB_HOST, L1_AGENT_PORT, '/health'),
      l2AgentUp(),
    ]);
    res.json({ hub, l1, l2 });
  } catch (error) {
    console.error('services/status error:', error);
    res.status(500).json({ error: 'status check failed' });
  }
});

async function waitFor(
  predicate: () => Promise<boolean>,
  timeoutMs = 4000,
): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return true;
    await new Promise((r) => setTimeout(r, 250));
  }
  return false;
}

app.post('/api/services/start-hub', async (_req, res) => {
  try {
    if (await probeHttp(HUB_HOST, HUB_PORT, '/hub/clients')) {
      return res.json({ status: 'already-up' });
    }
    const { pid } = spawnDetachedNode(
      ['--import', 'tsx/esm', '_hub-start.mts'],
      'hub.run',
    );
    const up = await waitFor(() =>
      probeHttp(HUB_HOST, HUB_PORT, '/hub/clients'),
    );
    res.json({ pid, status: up ? 'up' : 'starting' });
  } catch (error) {
    console.error('start-hub error:', error);
    res.status(500).json({ error: String(error) });
  }
});

app.post('/api/services/start-agent-l1', async (_req, res) => {
  try {
    if (await probeHttp(HUB_HOST, L1_AGENT_PORT, '/health')) {
      return res.json({ status: 'already-up' });
    }
    const { pid } = spawnDetachedNode(
      [
        '--max-old-space-size=16384',
        '--env-file=.env',
        '--import',
        'tsx/esm',
        'src/agent-server.ts',
      ],
      'agent-l1.run',
    );
    const up = await waitFor(
      () => probeHttp(HUB_HOST, L1_AGENT_PORT, '/health'),
      8000,
    );
    res.json({ pid, status: up ? 'up' : 'starting' });
  } catch (error) {
    console.error('start-agent-l1 error:', error);
    res.status(500).json({ error: String(error) });
  }
});

app.post('/api/services/start-agent-l2', async (_req, res) => {
  try {
    if (await l2AgentUp()) {
      return res.json({ status: 'already-up' });
    }
    // Spawn powershell with the helper script. Detached so this endpoint
    // returns immediately even if WinRM is slow.
    const psScript = path.join(REPO_ROOT, 'scripts', 'start-l2-agent.ps1');
    const out = fs.openSync(path.join(REPO_ROOT, 'agent-l2-launch.out'), 'a');
    const err = fs.openSync(path.join(REPO_ROOT, 'agent-l2-launch.err'), 'a');
    const child = spawn(
      'powershell.exe',
      ['-NoProfile', '-File', psScript],
      {
        cwd: REPO_ROOT,
        detached: true,
        stdio: ['ignore', out, err],
        windowsHide: true,
      },
    );
    child.unref();
    // L2 boot is slower (WinRM hop + Win32_Process.Create + node startup +
    // first poll cycle to register at hub).
    const up = await waitFor(l2AgentUp, 12000);
    res.json({ pid: child.pid, status: up ? 'up' : 'starting' });
  } catch (error) {
    console.error('start-agent-l2 error:', error);
    res.status(500).json({ error: String(error) });
  }
});

// ---- Stop endpoints ------------------------------------------------------

/**
 * Kills whatever process is listening on the given local TCP port.
 * Idempotent: returns `running: false` if nothing was listening.
 */
function stopLocalByPort(portNum: number): Promise<boolean> {
  return new Promise((resolve) => {
    const cmd =
      `$p = Get-NetTCPConnection -LocalPort ${portNum} -State Listen ` +
      `-ErrorAction SilentlyContinue; ` +
      `if ($p) { Stop-Process -Id $p.OwningProcess -Force ` +
      `-ErrorAction SilentlyContinue; exit 0 } else { exit 1 }`;
    const ps = spawn('powershell.exe', ['-NoProfile', '-Command', cmd], {
      windowsHide: true,
    });
    ps.on('close', (code) => resolve(code === 0));
    ps.on('error', () => resolve(false));
  });
}

/**
 * Stops the L2 agent over WinRM.
 */
function stopRemoteAgentL2(): Promise<boolean> {
  return new Promise((resolve) => {
    const cmd =
      `Invoke-Command -ComputerName 192.168.178.64 -ScriptBlock { ` +
      `$p = Get-NetTCPConnection -LocalPort 3002 -State Listen ` +
      `-ErrorAction SilentlyContinue; ` +
      `if ($p) { Stop-Process -Id $p.OwningProcess -Force ` +
      `-ErrorAction SilentlyContinue; 'stopped' } else { 'idle' } }`;
    const ps = spawn('powershell.exe', ['-NoProfile', '-Command', cmd], {
      windowsHide: true,
    });
    ps.on('close', () => resolve(true));
    ps.on('error', () => resolve(false));
  });
}

app.post('/api/services/stop-hub', async (_req, res) => {
  try {
    const stopped = await stopLocalByPort(HUB_PORT);
    res.json({ stopped });
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

app.post('/api/services/stop-agent-l1', async (_req, res) => {
  try {
    const stopped = await stopLocalByPort(L1_AGENT_PORT);
    res.json({ stopped });
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

app.post('/api/services/stop-agent-l2', async (_req, res) => {
  try {
    const stopped = await stopRemoteAgentL2();
    res.json({ stopped });
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

// ==================== REPAIR ====================
// Runs the existing CLI scripts (restore-from-chain, restore-from-peer,
// backfill-hashes) as child processes and streams the resulting summary
// back to the dashboard. Each script is coverage-excluded by design.

interface RepairResult {
  exitCode: number | null;
  stdout: string;
  stderr: string;
}

function runScript(
  scriptRel: string,
  envOverrides: Record<string, string>,
  timeoutMs = 60_000,
): Promise<RepairResult> {
  return new Promise((resolve) => {
    const child = spawn(
      'node',
      ['--import', 'tsx/esm', scriptRel],
      {
        cwd: REPO_ROOT,
        env: { ...process.env, ...envOverrides },
        windowsHide: true,
      },
    );
    let stdout = '';
    let stderr = '';
    let timedOut = false;
    const t = setTimeout(() => {
      timedOut = true;
      child.kill();
    }, timeoutMs);
    child.stdout?.on('data', (b) => {
      stdout += b.toString('utf-8');
      // Keep memory bounded for very long outputs.
      if (stdout.length > 200_000) stdout = stdout.slice(-200_000);
    });
    child.stderr?.on('data', (b) => {
      stderr += b.toString('utf-8');
      if (stderr.length > 200_000) stderr = stderr.slice(-200_000);
    });
    child.on('close', (code) => {
      clearTimeout(t);
      resolve({
        exitCode: timedOut ? -1 : code,
        stdout,
        stderr,
      });
    });
    child.on('error', (err) => {
      clearTimeout(t);
      resolve({ exitCode: -1, stdout, stderr: String(err) });
    });
  });
}

const CARATDB_URI =
  process.env.MONGO_URI ||
  'mongodb://localhost:27017/?replicaSet=rs0&directConnection=true';
const CARATDB_DB = process.env.DB_NAME || 'CARATDB';

/**
 * POST /api/repair/restore-from-chain
 * body: { dryRun?: boolean }
 * Walks the local sync_ops chain and re-applies any op whose effect
 * isn't reflected in the local collection state.
 */
app.post<{ Body?: { dryRun?: boolean } }>(
  '/api/repair/restore-from-chain',
  async (req, res) => {
    try {
      const dry = req.body?.dryRun === true;
      const env: Record<string, string> = {
        MONGO_URI: CARATDB_URI,
        DB_NAME: CARATDB_DB,
      };
      if (dry) env.DRY_RUN = '1';
      const r = await runScript(
        'src/scripts/restore-from-chain.ts',
        env,
        120_000,
      );
      res.json(r);
    } catch (error) {
      res.status(500).json({ error: String(error) });
    }
  },
);

/**
 * POST /api/repair/restore-from-peer
 * body: { coll?: string }
 * Diffs local _ids vs peer's via the agent's /diff endpoints; copies
 * back any docs the local node is missing.
 */
app.post<{ Body?: { coll?: string } }>(
  '/api/repair/restore-from-peer',
  async (req, res) => {
    try {
      const coll = req.body?.coll;
      const env: Record<string, string> = {
        MONGO_URI: CARATDB_URI,
        DB_NAME: CARATDB_DB,
        HUB_URL: `http://${HUB_HOST}:${HUB_PORT}`,
        PEER_NODE_ID: 'laptop2',
      };
      if (coll) env.COLL = coll;
      const r = await runScript(
        'src/scripts/restore-from-peer.ts',
        env,
        300_000,
      );
      res.json(r);
    } catch (error) {
      res.status(500).json({ error: String(error) });
    }
  },
);

/**
 * POST /api/repair/backfill-hashes
 * body: { coll?: string }
 * Populates `__h` on docs that don't have one yet so future state-hash
 * recomputes can use the {_id, __h} projection fast path.
 */
app.post<{ Body?: { coll?: string } }>(
  '/api/repair/backfill-hashes',
  async (req, res) => {
    try {
      const coll = req.body?.coll;
      const env: Record<string, string> = {
        MONGO_URI: CARATDB_URI,
        DB_NAME: CARATDB_DB,
      };
      if (coll) env.COLL = coll;
      const r = await runScript(
        'src/scripts/backfill-hashes.ts',
        env,
        600_000,
      );
      res.json(r);
    } catch (error) {
      res.status(500).json({ error: String(error) });
    }
  },
);

// ==================== HASH STATUS (Tier 3) ====================

/**
 * GET /api/hash-status
 * Returns the most recent state_checkpoints entry from the local DB,
 * giving the dashboard the current dbRoot + per-collection roots without
 * forcing a fresh recompute. Use POST /api/hash-status/recompute to
 * actually run computeStateCheckpoint.
 */
app.get('/api/hash-status', async (_req, res) => {
  try {
    const cp = await db
      .collection('state_checkpoints')
      .find({})
      .sort({ ts: -1 })
      .limit(1)
      .next();
    res.json({ checkpoint: cp });
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

// ==================== CHAIN INSPECTOR (Tier 3) ====================
// Read-only views of sync_ops + a per-origin prevHash→chainHash linkcheck.
// Fields are projected so the dashboard doesn't pull fullDocument blobs.

/**
 * GET /api/chain/origins
 * Distinct list of `origin` values present in sync_ops.
 */
app.get('/api/chain/origins', async (_req, res) => {
  try {
    const origins = await db.collection('sync_ops').distinct('origin');
    res.json({ origins: (origins as string[]).sort() });
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

/**
 * GET /api/chain?origin=...&limit=...&after=...
 * Paginated sync_ops list for a single origin. Returns a UI-friendly
 * projection (no fullDocument).
 */
app.get<{
  Querystring: { origin?: string; limit?: string; after?: string };
}>('/api/chain', async (req, res) => {
  try {
    const origin = req.query.origin;
    if (!origin) return res.status(400).json({ error: 'origin required' });
    const limit = Math.min(
      parseInt(String(req.query.limit ?? '100'), 10) || 100,
      500,
    );
    const after = parseInt(String(req.query.after ?? '0'), 10) || 0;
    const ops = await db
      .collection('sync_ops')
      .find({ origin, seq: { $gt: after } })
      .sort({ seq: 1 })
      .limit(limit)
      .project({
        _id: 1,
        origin: 1,
        seq: 1,
        operationType: 1,
        ns: 1,
        docId: 1,
        prevHash: 1,
        opHash: 1,
        chainHash: 1,
        ts: 1,
      })
      .toArray();
    res.json({
      ops: ops.map((o) => ({
        ...o,
        docId: o.docId === null || o.docId === undefined ? null : String(o.docId),
      })),
      hasMore: ops.length === limit,
    });
  } catch (error) {
    res.status(500).json({ error: String(error) });
  }
});

/**
 * GET /api/chain/verify?origin=...
 * Walks the chain (per-origin prevHash linkcheck) and returns the first
 * break, plus total op count. Origin is optional — empty verifies all.
 */
app.get<{ Querystring: { origin?: string } }>(
  '/api/chain/verify',
  async (req, res) => {
    try {
      const origin = req.query.origin;
      const filter: Record<string, unknown> = origin ? { origin } : {};
      const cursor = db
        .collection('sync_ops')
        .find(filter, {
          projection: { origin: 1, seq: 1, prevHash: 1, chainHash: 1 },
        })
        .sort({ origin: 1, seq: 1 });
      const prevByOrigin = new Map<string, string>();
      let valid = true;
      let firstBreakAt: { origin: string; seq: number } | null = null;
      let total = 0;
      for await (const op of cursor) {
        total += 1;
        const expected = prevByOrigin.get(op.origin) ?? 'GENESIS';
        if (op.prevHash !== expected) {
          valid = false;
          if (!firstBreakAt) {
            firstBreakAt = { origin: op.origin, seq: op.seq };
          }
        }
        prevByOrigin.set(op.origin, op.chainHash || 'INVALID');
      }
      res.json({ valid, firstBreakAt, total });
    } catch (error) {
      res.status(500).json({ error: String(error) });
    }
  },
);

// ==================== PARTITION MAP (Tier 3) ====================

/**
 * GET /api/partitions?coll=...
 * state_merkle entries (the cached partition Merkle nodes), grouped by
 * collection. Optional `coll` filter; without it, returns every entry.
 */
app.get<{ Querystring: { coll?: string } }>(
  '/api/partitions',
  async (req, res) => {
    try {
      const coll = req.query.coll;
      const filter: Record<string, unknown> = coll ? { coll } : {};
      const parts = await db
        .collection('state_merkle')
        .find(filter)
        .sort({ coll: 1, idx: 1 })
        .toArray();
      res.json({
        partitions: parts.map((p) => ({
          coll: (p as unknown as { coll: string }).coll,
          idx: (p as unknown as { idx: number }).idx,
          count: (p as unknown as { count: number }).count,
          root: (p as unknown as { root: string }).root,
          minId:
            (p as unknown as { minId?: unknown }).minId === undefined
              ? null
              : String((p as unknown as { minId: unknown }).minId),
          maxId:
            (p as unknown as { maxId?: unknown }).maxId === undefined
              ? null
              : String((p as unknown as { maxId: unknown }).maxId),
          updatedAt:
            (p as unknown as { updatedAt?: string }).updatedAt ?? null,
        })),
      });
    } catch (error) {
      res.status(500).json({ error: String(error) });
    }
  },
);

// ==================== START SERVER ====================

async function startServer() {
  try {
    await initializeMongoDB();

    app.listen(port, () => {
      console.log('');
      console.log('='.repeat(60));
      console.log(`🚀 RLJSON Conflict Resolution API running`);
      console.log('='.repeat(60));
      console.log(`📍 Server:    http://localhost:${port}`);
      console.log(`📊 MongoDB:   mongodb://localhost:27017`);
      console.log('');
      console.log('📝 Available Endpoints:');
      console.log(`   GET  http://localhost:${port}/api/conflicts`);
      console.log(`   GET  http://localhost:${port}/api/conflicts/:id`);
      console.log(`   POST http://localhost:${port}/api/conflicts/resolve`);
      console.log(`   GET  http://localhost:${port}/api/agents/status`);
      console.log(`   GET  http://localhost:${port}/api/documents/:id/history`);
      console.log(
        `   GET  http://localhost:${port}/api/conflicts/:id/verify-chain`,
      );
      console.log(`   POST http://localhost:${port}/api/sync/trigger`);
      console.log('='.repeat(60));
      console.log('');
      console.log('💡 Tip: Open http://localhost:4200 for the UI');
      console.log('');
    });
  } catch (error) {
    console.error('❌ Failed to start server:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down gracefully...');

  if (mongoClient) {
    await mongoClient.close();
  }

  process.exit(0);
});

startServer();
