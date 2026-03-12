// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import type {
  ChangeStream,
  ChangeStreamDocument,
  Db,
  Document,
  OptionalId,
} from 'mongodb';
import { computeOpHash, sha256Hex } from './hashing/integrity-hash.ts';


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
    fullDocument?: unknown;
    updateDescription?: unknown;
  } | null;
  ts?: string;
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
 * Appends an operation to sync_ops collection with retry logic
 * @param db - Database instance
 * @param nodeId - Node identifier
 * @param op - Sync operation to append
 * @param logger - Optional logger
 * @returns Stored sync operation document
 */
async function appendOp(
  db: Db,
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
    };

    try {
      await db
        .collection('sync_ops')
        .insertOne(doc as unknown as OptionalId<Document>);

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
      // Duplicate key: someone wrote this seq already -> reread local and retry
      const error = e as { code?: number; message?: string };
      if (error?.code === 11000 && attempt < MAX_RETRIES) {
        logger?.warn?.(
          { err: error.message, attempt, nextSeq },
          'duplicate key on sync_ops insert; retrying',
        );
        continue;
      }
      throw e;
    }
  }
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
  const { db, nodeId, logger, suppressor } = options;
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

      const op: SyncOp = {
        ns: { db: ns.db, coll: ns.coll },
        operationType: change.operationType,
        docId,
        payload: {
          fullDocument:
            (change as { fullDocument?: unknown }).fullDocument ?? null,
          updateDescription:
            (change as { updateDescription?: unknown }).updateDescription ??
            null,
        },
        ts: new Date().toISOString(),
      };

      // Append operation
      await appendOp(db, nodeId, op, logger);

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
