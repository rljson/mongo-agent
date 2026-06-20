// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Bs, BsMem } from '@rljson/bs';
import { hip, hsh } from '@rljson/hash';

import { computeOpHash, sha256Hex } from './hashing/integrity-hash.ts';
import { markDirtyById } from './hashing/state-dirty.ts';
import { computeStateCheckpoint } from './hashing/state-hash.ts';

import type { ChangeStream, ChangeStreamDocument, Db, ObjectId } from 'mongodb';

import type { ComponentsTable, TableCfg } from '@rljson/rljson';
/**
 * Logger interface compatible with Fastify/Pino
 */
export interface Logger {
  info?: (obj?: object | string, msg?: string) => void;
  warn?: (obj?: object | string, msg?: string) => void;
  error?: (obj?: object | string, msg?: string) => void;
}

/**
 * Namespace for database and collection
 */
export interface Namespace {
  db: string;
  coll: string;
}

/**
 * Sync operation to be stored
 */
export interface SyncOp {
  ns: Namespace;
  operationType: string;
  docId: unknown;
  payload?: {
    fullDocumentBlobId?: string; // Blob reference (content hash) - producer-local
    updateDescriptionBlobId?: string; // Blob reference (content hash) - producer-local
    /**
     * Inline full document snapshot. Required for cross-process sync because
     * blob storage (e.g. BsMem) is per-agent; the consumer cannot resolve
     * fullDocumentBlobId. Populated for insert/update/replace.
     */
    fullDocument?: Record<string, unknown>;
    /** Inline change-stream updateDescription. */
    updateDescription?: Record<string, unknown>;
  } | null;
  ts?: string;

  // Optional change stream metadata
  changeStreamId?: unknown; // MongoDB resume token (_id from change event)
  clusterTime?: unknown; // MongoDB cluster timestamp
  wallTime?: string; // MongoDB wall time (ISO string)

  // Optional state tracking (for DB synthesis)
  prevStateHash?: string; // State hash before this operation
  currentStateHash?: string; // State hash after this operation
}

/**
 * Stored sync operation document
 */
export interface SyncOpDoc extends SyncOp {
  _id: string;
  origin: string;
  seq: number;
  prevHash: string;
  opHash: string;
  chainHash: string;

  // Change stream metadata (for resume and correlation)
  changeStreamId?: unknown; // MongoDB resume token (_id from change event)
  clusterTime?: unknown; // MongoDB cluster timestamp
  wallTime?: string; // MongoDB wall time (ISO string)
}

/**
 * Echo suppressor interface
 */
export interface Suppressor {
  add: (ns: Namespace, id: unknown) => void;
  has: (ns: Namespace, id: unknown) => boolean;
}

/**
 * Options for starting change stream
 */
export interface StartChangeStreamOptions {
  db: Db;
  nodeId: string;
  logger?: Logger;
  suppressor?: Suppressor;
  bs?: Bs;
  trackStateHash?: boolean; // Enable state hash tracking (slower but complete)
}

/**
 * Creates a suppressor to prevent echo loops.
 * When applyOp writes a document, it adds it to the suppressor
 * so the change stream doesn't treat it as a new local change.
 * @param ttlMs - Time-to-live in milliseconds (default: 30000)
 * @returns Suppressor instance
 */
export function createSuppressor(ttlMs = 30000): Suppressor {
  const m = new Map<string, number>(); // key -> expiresAt

  function keyOf(ns: Namespace, id: unknown): string {
    const db = ns?.db || '';
    const coll = ns?.coll || '';
    return `${db}.${coll}::${String(id)}`;
  }

  function cleanup(): void {
    const now = Date.now();
    for (const [k, exp] of m.entries()) {
      if (exp <= now) m.delete(k);
    }
  }

  return {
    add(ns: Namespace, id: unknown): void {
      cleanup();
      m.set(keyOf(ns, id), Date.now() + ttlMs);
    },
    has(ns: Namespace, id: unknown): boolean {
      cleanup();
      return m.has(keyOf(ns, id));
    },
  };
}

/**
 * TableCfg for sync_ops ComponentsTable
 */
export const SYNC_OPS_TABLE_CFG = hip<TableCfg>({
  key: 'sync_ops',
  type: 'components',
  columns: [
    { key: '_hash', type: 'string', titleShort: 'Hash', titleLong: 'Hash' },
    { key: '_id', type: 'string', titleShort: 'ID', titleLong: 'ID' },
    {
      key: 'origin',
      type: 'string',
      titleShort: 'Origin',
      titleLong: 'Origin Node',
    },
    {
      key: 'seq',
      type: 'number',
      titleShort: 'Seq',
      titleLong: 'Sequence Number',
    },
    {
      key: 'operationType',
      type: 'string',
      titleShort: 'OpType',
      titleLong: 'Operation Type',
    },
    {
      key: 'prevHash',
      type: 'string',
      titleShort: 'PrevHash',
      titleLong: 'Previous Hash',
    },
    {
      key: 'opHash',
      type: 'string',
      titleShort: 'OpHash',
      titleLong: 'Operation Hash',
    },
    {
      key: 'chainHash',
      type: 'string',
      titleShort: 'ChainHash',
      titleLong: 'Chain Hash',
    },
    {
      key: 'ns',
      type: 'json' as any,
      titleShort: 'NS',
      titleLong: 'Namespace',
    },
    {
      key: 'docId',
      type: 'string',
      titleShort: 'DocID',
      titleLong: 'Document ID',
    },
    {
      key: 'payload',
      type: 'json' as any,
      titleShort: 'Payload',
      titleLong: 'Payload (blob references)',
    },
    { key: 'ts', type: 'string', titleShort: 'TS', titleLong: 'Timestamp' },
    {
      key: 'changeStreamId',
      type: 'json' as any,
      titleShort: 'CSId',
      titleLong: 'Change Stream ID',
    },
    {
      key: 'clusterTime',
      type: 'json' as any,
      titleShort: 'ClusterT',
      titleLong: 'Cluster Time',
    },
    {
      key: 'wallTime',
      type: 'string',
      titleShort: 'WallT',
      titleLong: 'Wall Time',
    },
    {
      key: 'prevStateHash',
      type: 'string',
      titleShort: 'PrevState',
      titleLong: 'Previous State Hash',
    },
    {
      key: 'currentStateHash',
      type: 'string',
      titleShort: 'CurrState',
      titleLong: 'Current State Hash',
    },
  ],
  isHead: false,
  isRoot: false,
  isShared: true,
  _hash: '',
});

/**
 * Loads the sync_ops ComponentsTable from blob storage
 * @param db - Database instance
 * @param bs - BlobStorage instance
 * @returns ComponentsTable or null if not exists
 */
async function loadSyncOpsTable(
  db: Db,
  bs: Bs,
): Promise<ComponentsTable<any> | null> {
  // Load metadata from sync_state collection
  const meta = await db
    .collection('sync_state')
    .findOne({ _id: 'sync_ops_meta' } as Record<string, unknown>);

  if (!meta || !(meta as any).componentsBlobId) {
    return null;
  }

  const blobId = (meta as any).componentsBlobId;
  const blob = await bs.getBlob(blobId).catch(() => null);

  if (!blob) {
    // Stale metadata pointing to a blob that no longer exists
    // (e.g. in-memory BsMem after agent restart). Treat as fresh table.
    return null;
  }

  const table = JSON.parse(
    blob.content.toString('utf-8'),
  ) as ComponentsTable<any>;
  return table;
}

/**
 * Saves the sync_ops ComponentsTable to blob storage
 * @param db - Database instance
 * @param bs - BlobStorage instance
 * @param table - ComponentsTable to save
 */
async function saveSyncOpsTable(
  db: Db,
  bs: Bs,
  table: ComponentsTable<any>,
): Promise<void> {
  // Store table as blob
  const content = JSON.stringify(table);
  const blobProps = await bs.setBlob(Buffer.from(content, 'utf-8'));

  // Update metadata
  await db.collection('sync_state').updateOne(
    { _id: 'sync_ops_meta' } as Record<string, unknown>,
    {
      $set: {
        componentsBlobId: blobProps.blobId,
        tableCfgHash: SYNC_OPS_TABLE_CFG._hash,
        rowCount: table._data.length,
        updatedAt: new Date().toISOString(),
      },
    },
    { upsert: true },
  );
}

// Internal collections that should not be watched
const INTERNAL_PREFIXES = ['system.', 'sync_', 'state_'];
const INTERNAL_EXACT = new Set([
  'sync_ops',
  'sync_state',
  'sync_local',
  'sync_resume',
  'sync_conflicts',
  'sync_audit',
  'state_checkpoints',
  'state_merkle',
  'sync_head',
]);

/**
 * Checks if a collection is internal and should not be watched
 * @param collName - Collection name
 * @returns True if collection is internal
 */
export function isInternalCollection(collName: string | undefined): boolean {
  if (!collName) return true;
  if (INTERNAL_EXACT.has(collName)) return true;
  return INTERNAL_PREFIXES.some((p) => collName.startsWith(p));
}

/**
 * Serial queue to ensure operations are processed sequentially
 */
interface SerialQueue {
  enqueue: <T>(fn: () => Promise<T>) => Promise<T>;
}

/**
 * Creates a serial queue that processes tasks one at a time
 * @returns Serial queue instance
 */
function createSerialQueue(): SerialQueue {
  let chain = Promise.resolve();
  return {
    enqueue<T>(fn: () => Promise<T>): Promise<T> {
      const run = async (): Promise<T> => fn();
      const next = chain.then(run, run);
      chain = next.then(
        () => {},
        () => {
          /* swallow to keep chain alive */
        },
      );
      return next;
    },
  };
}

/**
 * Appends an operation to sync_ops ComponentsTable with retry logic
 * @param db - Database instance
 * @param bs - BlobStorage instance
 * @param nodeId - Node identifier
 * @param op - Sync operation to append
 * @param logger - Optional logger
 * @returns Stored sync operation document
 */
async function appendOp(
  db: Db,
  bs: Bs,
  nodeId: string,
  op: SyncOp,
  logger?: Logger,
): Promise<SyncOpDoc> {
  const MAX_RETRIES = 5;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const local = (await db
      .collection('sync_local')
      .findOne({ _id: 'local' } as Record<string, unknown>)) || {
      seq: 0,
      headHash: 'GENESIS',
    };
    const nextSeq = (local.seq || 0) + 1;
    const prevHash = local.headHash || 'GENESIS';

    // The hash chain and the rljson ComponentsTable both rely on
    // `stableStringify`, which can't represent native BSON types (a `Date`
    // becomes `{}`, an `ObjectId` becomes `{}`, …). Flatten *only* for those
    // paths via a JSON round-trip. The native-typed `op` keeps Date/ObjectId
    // instances intact for the raw `sync_ops` insert below — so peers receive
    // EJSON `$date`/`$oid` over the wire and apply with full type fidelity.
    const opPlain = JSON.parse(JSON.stringify(op)) as SyncOp;

    const opHash = computeOpHash(opPlain);
    const chainHash = sha256Hex(prevHash + '|' + opHash);

    // Native-typed doc for the raw sync_ops collection.
    const doc: SyncOpDoc = {
      _id: `${nodeId}_${nextSeq}`,
      origin: nodeId,
      seq: nextSeq,
      prevHash,
      opHash,
      chainHash,
      ns: op.ns,
      operationType: op.operationType,
      docId: op.docId,
      payload: op.payload,
      ts: op.ts,
      changeStreamId: op.changeStreamId,
      clusterTime: op.clusterTime,
      wallTime: op.wallTime,
      prevStateHash: op.prevStateHash,
      currentStateHash: op.currentStateHash,
    };

    // Plain-typed doc for the rljson ComponentsTable hash path only.
    const docPlain: SyncOpDoc = {
      ...doc,
      docId: opPlain.docId,
      payload: opPlain.payload,
    };

    try {
      // Load existing ComponentsTable or create new one
      let table = await loadSyncOpsTable(db, bs);

      if (!table) {
        // Create new ComponentsTable
        table = hip<ComponentsTable<any>>({
          _tableCfg: SYNC_OPS_TABLE_CFG._hash as string,
          _type: 'components',
          _data: [],
          _hash: '',
        });
      }

      // Add hashed operation to table (rljson can't hash native BSON types).
      const hashedDoc = hsh(docPlain as any);
      table._data.push(hashedDoc);

      // Clear hash before rehashing (required for hip() to recompute)
      table._hash = '';

      // Rehash the table
      table = hip(table);

      // Save updated table
      await saveSyncOpsTable(db, bs, table);

      // Also persist into raw sync_ops collection so the legacy
      // /sync/pull endpoint (which queries db.collection('sync_ops'))
      // can serve ops to peers. Without this, pulls always return empty.
      // Uses the native-typed `doc`: types survive EJSON.serialize/deserialize.
      await db
        .collection('sync_ops')
        .insertOne(doc as unknown as Record<string, unknown>)
        .catch(() => {
          /* duplicate _id on retry - ignore */
        });

      // Update local state (still needed for sequence tracking)
      await db.collection('sync_local').updateOne(
        { _id: 'local' } as Record<string, unknown>,
        {
          $set: {
            seq: nextSeq,
            headHash: chainHash,
            updatedAt: new Date().toISOString(),
          },
        },
        { upsert: true },
      );

      return doc;
    } catch (e) {
      // Retry on conflicts
      const error = e as { code?: number; message?: string };
      if (attempt < MAX_RETRIES) {
        logger?.warn?.(
          { err: error.message, attempt, nextSeq },
          'error appending to sync_ops table; retrying',
        );
        continue;
      }
      throw e;
    }
  }

  // If we exhausted all retries without returning, throw an error
  throw new Error(`Failed to append operation after ${MAX_RETRIES} retries`);
}

/**
 * Starts a MongoDB change stream to watch for database changes.
 * Captures changes to user collections and appends them to sync_ops.
 * Handles resume tokens for crash recovery.
 * @param options - Change stream options
 * @returns Change stream instance
 */
export async function startDbChangeStream(
  options: StartChangeStreamOptions,
): Promise<ChangeStream> {
  const { db, nodeId, logger, suppressor, bs, trackStateHash } = options;

  // Create BlobStorage if not provided
  const blobStorage = bs || new BsMem();

  const q = createSerialQueue();

  // Tracks the post-checkpoint dbRoot across change-stream events; only used
  // when `trackStateHash` is enabled. Persists across the change-stream
  // callbacks below — declared here so its writes outlive any single tick.
  let currentDbStateHash: string | undefined;

  // ---- State-hash checkpoint: DEBOUNCED off the change hot-path ----
  // Computing the full dbRoot on every change is O(total-DB): it rolls up
  // every collection + partition and re-hashes any dirty (50k-doc) partition,
  // so a single insert into a DB with large catalogs (cd_models 9M) blocked
  // the serial queue for ~30s. The data sync does NOT need the hash —
  // `appendOp` ships the delta immediately and the apply/pull path never reads
  // prev/currentStateHash (they only feed the convergence chain + restore). So
  // the hot path now just `markDirtyById`s and the checkpoint is recomputed
  // once changes settle, collapsing a whole burst into a single rollup.
  // `currentDbStateHash` converges shortly after the last change; on-demand
  // `/test/hash-recompute` still returns the exact current dbRoot.
  const CHECKPOINT_DEBOUNCE_MS = 750;
  const CHECKPOINT_IGNORED = new Set([
    'state_checkpoints',
    'state_merkle',
    'state_dirty',
    'sync_ops',
    'sync_state',
    'sync_local',
    'sync_resume',
  ]);
  let checkpointTimer: ReturnType<typeof setTimeout> | null = null;
  let checkpointRunning = false;
  let checkpointPendingAgain = false;

  const recomputeCheckpoint = async (): Promise<void> => {
    if (checkpointRunning) {
      // A burst arrived while we were hashing — rerun once we finish so the
      // final dbRoot reflects the latest dirty partitions.
      checkpointPendingAgain = true;
      return;
    }
    checkpointRunning = true;
    try {
      const newState = await computeStateCheckpoint({
        db,
        ignoredColls: CHECKPOINT_IGNORED,
        partitionSize: 50000,
        mode: 'incremental',
      });
      currentDbStateHash = newState.dbRoot;
    } catch {
      logger?.warn?.('Failed to compute state checkpoint (debounced)');
    } finally {
      checkpointRunning = false;
      if (checkpointPendingAgain) {
        checkpointPendingAgain = false;
        scheduleCheckpoint();
      }
    }
  };

  function scheduleCheckpoint(): void {
    if (!trackStateHash) return;
    if (checkpointTimer) clearTimeout(checkpointTimer);
    checkpointTimer = setTimeout(() => {
      checkpointTimer = null;
      void recomputeCheckpoint();
    }, CHECKPOINT_DEBOUNCE_MS);
  }

  // Load resume token
  const resumeDoc = await db
    .collection('sync_resume')
    .findOne({ _id: 'resume' } as Record<string, unknown>)
    .catch(() => null);
  const resumeToken = resumeDoc?.token || null;

  const pipeline = [
    {
      $match: {
        operationType: { $in: ['insert', 'update', 'replace', 'delete'] },
      },
    },
  ];

  let cs: ChangeStream;
  try {
    cs = db.watch(pipeline, {
      fullDocument: 'updateLookup',
      ...(resumeToken ? { resumeAfter: resumeToken } : {}),
    });

    logger?.info?.(
      { resumeAfter: !!resumeToken },
      'DB change stream started (SERIAL queue enabled)',
    );
  } catch (err) {
    // If resume token is invalid (e.g., outside oplog window), start fresh
    const error = err as { message?: string };
    if (resumeToken && error.message?.includes('resume')) {
      logger?.warn?.(
        {
          err: error.message,
          action: 'clearing invalid resume token and starting fresh',
        },
        'Resume token invalid - starting change stream from current position',
      );

      // Clear invalid token
      await db
        .collection('sync_resume')
        .deleteOne({ _id: 'resume' } as Record<string, unknown>)
        .catch(() => {
          /* ignore */
        });

      // Start without resume token
      cs = db.watch(pipeline, {
        fullDocument: 'updateLookup',
      });

      logger?.info?.(
        'DB change stream started from current position (no resume token)',
      );
    } else {
      throw err;
    }
  }

  cs.on('change', (change: ChangeStreamDocument) => {
    q.enqueue(async () => {
      const ns = (change as { ns?: { db: string; coll: string } }).ns;
      const coll = ns?.coll;

      if (isInternalCollection(coll)) return;

      // Extract document ID
      const docId =
        (change as { documentKey?: { _id?: unknown } }).documentKey?._id ??
        (change as { fullDocument?: { _id?: unknown } }).fullDocument?._id ??
        null;

      if (docId === null || docId === undefined) return;

      // Guard against missing namespace
      if (!ns) return;

      // Mark dirty for state hash tracking BEFORE the suppressor check —
      // remote-applied ops (which the suppressor swallows below) still
      // mutate the local collection, so their partitions are genuinely
      // dirty and need recomputing on the next incremental hash.
      if (trackStateHash && ns) {
        await markDirtyById(
          db,
          ns.coll,
          docId as ObjectId | string | number,
          { reason: change.operationType },
        );
      }

      // Suppress echo-loop (from applyOp): everything past this point
      // records the op as locally-originated, which is wrong for ops we
      // just applied from a peer.
      if (suppressor?.has(ns as Namespace, docId)) return;

      // Store fullDocument and updateDescription as blobs (RLJSON pattern)
      let fullDocumentBlobId: string | undefined;
      let updateDescriptionBlobId: string | undefined;

      const fullDoc = (change as { fullDocument?: unknown }).fullDocument;
      if (fullDoc) {
        const docJson = JSON.stringify(fullDoc);
        const blob = await blobStorage.setBlob(Buffer.from(docJson, 'utf-8'));
        fullDocumentBlobId = blob.blobId;
      }

      const updateDesc = (change as { updateDescription?: unknown })
        .updateDescription;
      if (updateDesc) {
        const descJson = JSON.stringify(updateDesc);
        const blob = await blobStorage.setBlob(Buffer.from(descJson, 'utf-8'));
        updateDescriptionBlobId = blob.blobId;
      }

      // State tracking: capture state before operation
      const prevStateHash = trackStateHash ? currentDbStateHash : undefined;

      // State hash is computed OFF the hot path (debounced) — see
      // scheduleCheckpoint above. Tag the op with the last-known dbRoot (cheap)
      // and let the checkpoint converge after the burst settles. The
      // markDirtyById call above already recorded this change's partition, so
      // the deferred recompute will include it.
      const newStateHash: string | undefined = trackStateHash
        ? currentDbStateHash
        : undefined;
      if (trackStateHash) scheduleCheckpoint();

      // Keep `fullDocument`, `updateDescription`, and `docId` as native BSON
      // types (Date, ObjectId, Decimal128, …). The wire-format step in
      // `agent-server.ts` runs `EJSON.serialize(op)` so they survive the JSON
      // hop as `$oid`/`$date`/etc., and the consumer's `EJSON.deserialize`
      // restores them losslessly. Flattening to plain JSON is done *locally*
      // inside appendOp where the hash chain and the rljson ComponentsTable
      // need it — not here, where it would silently drop types end-to-end.
      const op: SyncOp = {
        ns: { db: ns.db, coll: ns.coll },
        operationType: change.operationType,
        docId,
        payload: {
          fullDocumentBlobId,
          updateDescriptionBlobId,
          // Inline payloads so consumers can apply without access to producer's blob store.
          fullDocument: fullDoc
            ? (fullDoc as Record<string, unknown>)
            : undefined,
          updateDescription: updateDesc
            ? (updateDesc as Record<string, unknown>)
            : undefined,
        },
        ts: new Date().toISOString(),
        // Capture change stream metadata (serialize complex MongoDB objects)
        changeStreamId: change._id
          ? JSON.parse(JSON.stringify(change._id))
          : undefined,
        clusterTime: (change as { clusterTime?: unknown }).clusterTime
          ? JSON.parse(
              JSON.stringify((change as { clusterTime?: unknown }).clusterTime),
            )
          : undefined,
        wallTime:
          (change as { wallTime?: Date }).wallTime?.toISOString?.() ??
          (typeof (change as { wallTime?: string }).wallTime === 'string'
            ? (change as { wallTime?: string }).wallTime
            : undefined),
        // State tracking (if enabled)
        prevStateHash,
        currentStateHash: newStateHash,
      };

      // Append operation to ComponentsTable
      await appendOp(db, blobStorage, nodeId, op, logger);

      // Store resume token after successful write
      if (change._id) {
        await db.collection('sync_resume').updateOne(
          { _id: 'resume' } as Record<string, unknown>,
          {
            $set: {
              token: change._id,
              updatedAt: new Date().toISOString(),
            },
          },
          { upsert: true },
        );
      }
    }).catch((e) => {
      const error = e as { message?: string };
      logger?.error?.({ err: error.message }, 'serial queue task failed');
    });
  });

  cs.on('error', (e: Error) => {
    logger?.error?.({ err: e.message }, 'change stream error');
  });

  return cs;
}
