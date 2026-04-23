// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { EJSON, ObjectId } from 'bson';


import type { Db, Document, MongoClient, OptionalId } from 'mongodb';
import type { Logger, Namespace, Suppressor } from '../watch-changes.ts';

/**
 * Sync operation payload containing document data.
 */
export interface SyncOpPayload {
  /** Full document for insert/update operations */
  fullDocument?: Record<string, unknown>;
  /** Update description for update operations */
  updateDescription?: {
    updatedFields?: Record<string, unknown>;
    removedFields?: string[];
  };
}

/**
 * Sync operation document structure.
 */
export interface SyncOp {
  /** Unique operation identifier (origin_seq) */
  _id: string;
  /** Origin node identifier */
  origin: string;
  /** Sequence number for this origin */
  seq: number;
  /** Previous operation hash in chain */
  prevHash?: string;
  /** Hash of this operation */
  opHash?: string;
  /** Chain hash including this operation */
  chainHash?: string;
  /** Namespace (database and collection) */
  ns: Namespace;
  /** Type of operation */
  operationType: string;
  /** Document ID affected by this operation */
  docId: unknown;
  /** Operation payload */
  payload: SyncOpPayload | null;
  /** Timestamp of operation */
  ts: string;
}

/**
 * Sync state tracking for an origin.
 */
export interface SyncState {
  /** Origin node identifier */
  origin: string;
  /** Last sequence number seen from this origin */
  lastSeqSeen: number;
  /** Last chain hash seen from this origin */
  lastHashSeen: string;
  /** Applied state tracking */
  applied?: {
    lastSeq: number;
    lastHash: string;
  };
  /** When this state was last updated */
  updatedAt?: string;
  /** Which node updated this state */
  updatedBy?: string;
}

/**
 * Options for fetchOpsFromHub function.
 */
export interface FetchOpsFromHubOptions {
  /** Fastify instance with logger */
  fastify: { log: Logger };
  /** Hub URL for relay service */
  hubUrl: string;
  /** Peer client identifier */
  peerClientId: string;
  /** Origin node identifier */
  origin: string;
  /** Last sequence number seen from this origin */
  lastSeqSeen: number;
  /** Last chain hash seen from this origin */
  lastHashSeen: string;
}

/**
 * Options for applyOneOp function.
 */
export interface ApplyOneOpOptions {
  /** MongoDB database instance */
  db: Db;
  /** Operation to apply */
  op: SyncOp;
  /** Local node identifier */
  localNodeId: string;
  /** Fastify instance with logger */
  fastify: { log: Logger };
  /** Optional suppressor to prevent echo loops */
  suppressor?: Suppressor;
}

/**
 * Result of applying an operation.
 */
export interface ApplyOpResult {
  /** Whether the operation was applied */
  applied: boolean;
  /** Reason if not applied */
  reason?: string;
}

/**
 * Options for syncOriginFromHub function.
 */
export interface SyncOriginFromHubOptions {
  /** Fastify instance with logger */
  fastify: { log: Logger };
  /** MongoDB client */
  mongo: MongoClient;
  /** Database name */
  dbName: string;
  /** Local node identifier */
  localNodeId: string;
  /** Hub URL for relay service */
  hubUrl: string;
  /** Peer client identifier */
  peerClientId: string;
  /** Origin node identifier */
  origin: string;
  /** Optional suppressor to prevent echo loops */
  suppressor?: Suppressor;
}

/**
 * Result of syncing from hub.
 */
export interface SyncResult {
  /** Number of operations pulled */
  pulled: number;
  /** Number of operations applied */
  applied: number;
  /** Whether we're up to date (no new ops) */
  upToDate: boolean;
}

/**
 * Converts a string to ObjectId if it looks like a valid ObjectId hex string.
 * This is needed because ObjectIds get stringified during JSON serialization.
 * @param value - Value to potentially convert
 * @returns ObjectId if valid hex string, otherwise original value
 */
export function maybeObjectId(value: unknown): unknown {
  if (typeof value === 'string' && /^[a-f0-9]{24}$/i.test(value)) {
    return new ObjectId(value);
  }
  return value;
}

/**
 * Fetches operations from hub for a specific origin.
 * Expects payload from peer/hub in one of these forms:
 * - `{ ops: [...] }`
 * - `[...]`
 * Operations are normalized with EJSON.deserialize so that
 * $oid / $date / $numberInt become real BSON/JS values.
 * @param options - Fetch options
 * @returns Array of deserialized operations
 */
export async function fetchOpsFromHub(
  options: FetchOpsFromHubOptions,
): Promise<SyncOp[]> {
  const { fastify, hubUrl, peerClientId, origin, lastSeqSeen, lastHashSeen } =
    options;

  const url = `${hubUrl}/hub/relay/${peerClientId}/sync/pull`;

  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      origin,
      lastSeqSeen,
      lastHashSeen,
    }),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`pull failed ${resp.status}: ${text}`);
  }

  const payload = (await resp.json()) as unknown;

  const rawOps = Array.isArray(payload)
    ? payload
    : Array.isArray((payload as { ops?: unknown[] })?.ops)
      ? (payload as { ops: unknown[] }).ops
      : [];

  const ops = rawOps.map((op) => {
    // Drop fields we don't need to apply but which may carry BSON shapes
    // (e.g. Timestamp encoded as { $timestamp: "<decimal>" }) that older
    // EJSON.deserialize versions choke on.
    const o = op as Record<string, unknown>;
    delete o.clusterTime;
    delete o.changeStreamId;
    try {
      return EJSON.deserialize(o) as SyncOp;
    } catch {
      // Fall back to the raw JSON object — applyOneOp only needs basic fields.
      return o as unknown as SyncOp;
    }
  });

  fastify.log.info?.(
    {
      peer: peerClientId,
      origin,
      requestedFromSeq: lastSeqSeen,
      pulled: ops.length,
      payloadKeys:
        payload && typeof payload === 'object'
          ? Object.keys(payload as Record<string, unknown>)
          : null,
    },
    'sync pull payload parsed',
  );

  return ops;
}

/**
 * Applies a single operation to MongoDB.
 * Expects a "sync_ops" collection for deduplication/chain tracking.
 * @param options - Apply options
 * @returns Result indicating whether operation was applied
 */
export async function applyOneOp(
  options: ApplyOneOpOptions,
): Promise<ApplyOpResult> {
  const { db, op, localNodeId, fastify, suppressor } = options;

  const syncOps = db.collection('sync_ops');
  const syncState = db.collection('sync_state');

  const already = await syncOps.findOne({ _id: op._id } as Record<
    string,
    unknown
  >);
  if (already) {
    return { applied: false, reason: 'already-present' };
  }

  const coll = db.collection(op.ns.coll);

  // Use _id from fullDocument to preserve correct type.
  // Convert string IDs back to ObjectId if they look like ObjectIds.
  if (op.operationType === 'insert') {
    const fd = op.payload?.fullDocument;
    if (!fd || typeof fd !== 'object' || fd._id === undefined) {
      fastify.log.warn?.(
        { opId: op._id, type: op.operationType },
        'op missing fullDocument payload; skipping (cannot apply insert)',
      );
      return { applied: false, reason: 'missing-payload' };
    }
    const docId = maybeObjectId(fd._id);
    const fullDoc = { ...fd, _id: docId };
    await coll.replaceOne({ _id: docId } as Record<string, unknown>, fullDoc, {
      upsert: true,
    });

    // Add to suppressor to prevent echo loop
    if (suppressor) {
      suppressor.add(op.ns, docId);
    }
  } else if (op.operationType === 'update' || op.operationType === 'replace') {
    const fd = op.payload?.fullDocument;
    if (!fd || typeof fd !== 'object' || fd._id === undefined) {
      fastify.log.warn?.(
        { opId: op._id, type: op.operationType },
        'op missing fullDocument payload; skipping (cannot apply update)',
      );
      return { applied: false, reason: 'missing-payload' };
    }
    const docId = maybeObjectId(fd._id);
    const fullDoc = { ...fd, _id: docId };
    await coll.replaceOne({ _id: docId } as Record<string, unknown>, fullDoc, {
      upsert: true,
    });

    // Add to suppressor to prevent echo loop
    if (suppressor) {
      suppressor.add(op.ns, docId);
    }
  } else if (op.operationType === 'delete') {
    const docId = maybeObjectId(op.docId);
    await coll.deleteOne({ _id: docId } as Record<string, unknown>);

    // Add to suppressor to prevent echo loop
    if (suppressor) {
      suppressor.add(op.ns, docId);
    }
  } else {
    fastify.log.warn?.(
      { opId: op._id, type: op.operationType },
      'unknown op type',
    );
    return { applied: false, reason: 'unknown-op-type' };
  }

  // Record remote operation locally
  await syncOps.insertOne(op as unknown as OptionalId<Document>);

  await syncState.updateOne(
    { origin: op.origin },
    {
      $set: {
        origin: op.origin,
        lastSeqSeen: op.seq,
        lastHashSeen: op.chainHash,
        applied: {
          lastSeq: op.seq,
          lastHash: op.chainHash,
        },
        updatedAt: new Date().toISOString(),
        updatedBy: localNodeId,
      },
    },
    { upsert: true },
  );

  return { applied: true };
}

/**
 * Pulls all missing operations for an origin and applies them.
 * @param options - Sync options
 * @returns Sync result with counts
 */
export async function syncOriginFromHub(
  options: SyncOriginFromHubOptions,
): Promise<SyncResult> {
  const {
    fastify,
    mongo,
    dbName,
    localNodeId,
    hubUrl,
    peerClientId,
    origin,
    suppressor,
  } = options;

  const db = mongo.db(dbName);
  const syncState = db.collection<SyncState>('sync_state');

  const state = await syncState.findOne({ origin });

  const lastSeqSeen = state?.lastSeqSeen ?? 0;
  const lastHashSeen = state?.lastHashSeen ?? 'GENESIS';

  const ops = await fetchOpsFromHub({
    fastify,
    hubUrl,
    peerClientId,
    origin,
    lastSeqSeen,
    lastHashSeen,
  });

  let appliedCount = 0;

  for (const op of ops) {
    const res = await applyOneOp({
      db,
      op,
      localNodeId,
      fastify,
      suppressor,
    });

    if (res.applied) {
      appliedCount += 1;
    }
  }

  fastify.log.info?.(
    {
      peer: peerClientId,
      origin,
      pulled: ops.length,
      applied: appliedCount,
      upToDate: ops.length === 0,
    },
    'sync pull via hub done',
  );

  return {
    pulled: ops.length,
    applied: appliedCount,
    upToDate: ops.length === 0,
  };
}
