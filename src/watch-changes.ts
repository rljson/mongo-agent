// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Bs, BsMem } from '@rljson/bs';
import { hip, hsh } from '@rljson/hash';
import type { ChangeStream, ChangeStreamDocument, Db } from 'mongodb';

import { computeOpHash, sha256Hex } from './hashing/integrity-hash.ts';
import { computeStateCheckpoint } from './hashing/state-hash.ts';
import { markDirtyById } from './hashing/state-dirty.ts';

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
    fullDocumentBlobId?: string;        // Blob reference (content hash)
    updateDescriptionBlobId?: string;   // Blob reference (content hash)
  } | null;
  ts?: string;
  
  // Optional change stream metadata
  changeStreamId?: unknown;     // MongoDB resume token (_id from change event)
  clusterTime?: unknown;         // MongoDB cluster timestamp
  wallTime?: string;             // MongoDB wall time (ISO string)
  
  // Optional state tracking (for DB synthesis)
  prevStateHash?: string;        // State hash before this operation
  currentStateHash?: string;     // State hash after this operation
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
  changeStreamId?: unknown;     // MongoDB resume token (_id from change event)
  clusterTime?: unknown;         // MongoDB cluster timestamp
  wallTime?: string;             // MongoDB wall time (ISO string)
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
  trackStateHash?: boolean;  // Enable state hash tracking (slower but complete)
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
  const blob = await bs.getBlob(blobId);

  if (!blob) {
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

    const opHash = computeOpHash(op);
    const chainHash = sha256Hex(prevHash + '|' + opHash);

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

      // Add hashed operation to table
      const hashedDoc = hsh(doc as any);
      table._data.push(hashedDoc);

      // Clear hash before rehashing (required for hip() to recompute)
      table._hash = '';

      // Rehash the table
      table = hip(table);

      // Save updated table
      await saveSyncOpsTable(db, bs, table);

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

      // Suppress echo-loop (from applyOp)
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

      const updateDesc = (change as { updateDescription?: unknown }).updateDescription;
      if (updateDesc) {
        const descJson = JSON.stringify(updateDesc);
        const blob = await blobStorage.setBlob(Buffer.from(descJson, 'utf-8'));
        updateDescriptionBlobId = blob.blobId;
      }

      // State tracking: capture state before operation
      const prevStateHash = trackStateHash ? currentDbStateHash : undefined;
      
      // Mark dirty for state hash tracking
      if (trackStateHash && ns) {
        await markDirtyById(db, ns.coll, docId, { reason: change.operationType });
      }
      
      // Compute new state hash after operation (if tracking enabled)
      let newStateHash: string | undefined;
      if (trackStateHash) {
        try {
          const newState = await computeStateCheckpoint({
            db,
            ignoredColls: new Set(['state_checkpoints', 'state_merkle', 'state_dirty', 'sync_ops', 'sync_state', 'sync_local', 'sync_resume']),
            partitionSize: 50000,
            mode: 'incremental',
          });
          newStateHash = newState.dbRoot;
          currentDbStateHash = newStateHash;  // Update tracked state
        } catch (err) {
          logger?.warn?.('Failed to compute new state hash');
        }
      }

      const op: SyncOp = {
        ns: { db: ns.db, coll: ns.coll },
        operationType: change.operationType,
        docId: typeof docId === 'object' && docId !== null ? 
          JSON.parse(JSON.stringify(docId)) : docId,  // Serialize ObjectIds
        payload: {
          fullDocumentBlobId,
          updateDescriptionBlobId,
        },
        ts: new Date().toISOString(),
        // Capture change stream metadata (serialize complex MongoDB objects)
        changeStreamId: change._id ? JSON.parse(JSON.stringify(change._id)) : undefined,
        clusterTime: (change as { clusterTime?: unknown }).clusterTime ? 
          JSON.parse(JSON.stringify((change as { clusterTime?: unknown }).clusterTime)) : undefined,
        wallTime: (change as { wallTime?: Date }).wallTime?.toISOString?.() ?? 
          (typeof (change as { wallTime?: string }).wallTime === 'string' ? 
            (change as { wallTime?: string }).wallTime : undefined),
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
