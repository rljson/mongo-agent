// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { EJSON } from 'bson';


import type { Db, Document, MongoClient, OptionalId } from 'mongodb';
import type { Logger, Namespace, Suppressor } from '../watch-changes.ts';
import { docLeafHash, type DocWithHash } from '../hashing/state-hash.ts';

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

  // Set to true if we detect a concurrent-update conflict and decide NOT to
  // overwrite the local doc. We still want to advance sync_ops / sync_state
  // below so the chain stays intact and the UI can resolve.
  let conflictDetected = false;

  // Apply operations with native BSON types. The producer stores ops with
  // native types in `sync_ops`, the wire encoder uses `EJSON.serialize`, and
  // `fetchOpsFromHub` runs `EJSON.deserialize` on each op before we get here
  // — so `fd._id`, `fd.createdAt`, etc. already carry their original BSON
  // types. No per-type restoration needed.
  if (op.operationType === 'insert' || op.operationType === 'update' || op.operationType === 'replace') {
    const fd = op.payload?.fullDocument;
    if (!fd || typeof fd !== 'object' || (fd as Record<string, unknown>)._id === undefined) {
      fastify.log.warn?.(
        { opId: op._id, type: op.operationType },
        'op missing fullDocument payload; skipping',
      );
      return { applied: false, reason: 'missing-payload' };
    }
    const fullDoc = fd as Record<string, unknown>;
    const docId = fullDoc._id;

    // Resolution propagation: if there's already a pending sync_conflicts
    // entry for this docId, this incoming op IS the resolution (some node's
    // UI/API picked a winner and wrote the resolved doc, which generated
    // this op via the change-stream). Apply unconditionally — overriding
    // local — and mark the local conflict resolved. Without this branch the
    // standard conflict detection below would re-flag the resolution as a
    // brand-new conflict and the choice would never propagate.
    const pendingConflict = await db
      .collection('sync_conflicts')
      .findOne({
        documentId: String(docId),
        collection: op.ns.coll,
        status: 'pending',
      });
    if (pendingConflict) {
      if (suppressor) {
        suppressor.add(op.ns, docId);
      }
      await coll.replaceOne(
        { _id: docId } as Record<string, unknown>,
        fullDoc,
        { upsert: true },
      );
      await db.collection('sync_conflicts').updateOne(
        { _id: (pendingConflict as { _id: unknown })._id } as Record<
          string,
          unknown
        >,
        { $set: { status: 'resolved' } },
      );
      fastify.log.info?.(
        {
          docId,
          opId: op._id,
          conflictId: (pendingConflict as { conflictId?: string })
            .conflictId,
        },
        'remote resolution applied; sync_conflicts marked resolved',
      );
      // Skip the rest of the apply branch (we already wrote). Fall through
      // to the sync_ops insert + sync_state update below.
      try {
        await syncOps.insertOne(op as unknown as OptionalId<Document>);
      } catch (err) {
        if ((err as { code?: number } | null)?.code !== 11000) throw err;
      }
      await syncState.updateOne(
        { origin: op.origin },
        {
          $set: {
            origin: op.origin,
            lastSeqSeen: op.seq,
            lastHashSeen: op.chainHash,
            applied: { lastSeq: op.seq, lastHash: op.chainHash },
            updatedAt: new Date().toISOString(),
            updatedBy: localNodeId,
          },
        },
        { upsert: true },
      );
      return { applied: true, reason: 'resolution-applied' };
    }

    // Concurrent-update conflict detection. When (1) the doc already exists
    // locally, (2) its content differs from the peer's payload, and (3) our
    // most recent LOCAL-origin sync_op on this doc still matches the local
    // content — both nodes diverged on this doc (the classic "edited offline
    // on both laptops, then reconnected" case). Record in sync_conflicts and
    // preserve the local edit; the UI resolves which version wins.
    const local = await coll.findOne(
      { _id: docId } as Record<string, unknown>,
    );
    // Update-delete conflict detection (Case B): peer wants to update a doc
    // that we deleted locally. If our most recent local-origin op on this
    // docId is a `delete`, both nodes diverged. Preserve the local
    // "deleted" state and record the conflict.
    if (!local) {
      const lastLocalEdit = await syncOps.findOne(
        {
          'ns.coll': op.ns.coll,
          docId,
          origin: localNodeId,
        } as Record<string, unknown>,
        { sort: { seq: -1 } },
      );
      if (
        lastLocalEdit &&
        (lastLocalEdit as { operationType?: string }).operationType ===
          'delete'
      ) {
        const remoteHash = docLeafHash(fd as DocWithHash);
        await recordConflict({
          db,
          ns: op.ns,
          docId,
          local: null,
          localHash: null,
          lastLocalEdit: lastLocalEdit as unknown as SyncOp,
          remoteOp: op,
          remoteHash,
          localNodeId,
          conflictType: 'update-delete',
        });
        fastify.log.warn?.(
          { docId, opId: op._id, peerOrigin: op.origin },
          'update-delete conflict: peer updated, local deleted; preserving local-deleted',
        );
        conflictDetected = true;
      }
    }
    if (!conflictDetected && local) {
      const remoteHash = docLeafHash(fd as DocWithHash);
      const localHash = docLeafHash(local as DocWithHash);
      if (remoteHash !== null && localHash !== null && remoteHash !== localHash) {
        const lastLocalEdit = await syncOps.findOne(
          {
            'ns.coll': op.ns.coll,
            docId,
            origin: localNodeId,
          } as Record<string, unknown>,
          { sort: { seq: -1 } },
        );
        const lastEditFd = (
          lastLocalEdit as { payload?: SyncOpPayload } | null
        )?.payload?.fullDocument;
        const lastEditHash = lastEditFd
          ? docLeafHash(lastEditFd as DocWithHash)
          : null;
        if (lastEditHash !== null && lastEditHash === localHash) {
          await recordConflict({
            db,
            ns: op.ns,
            docId,
            local,
            localHash,
            lastLocalEdit: lastLocalEdit as unknown as SyncOp,
            remoteOp: op,
            remoteHash,
            localNodeId,
          });
          fastify.log.warn?.(
            {
              docId,
              opId: op._id,
              peerOrigin: op.origin,
              localOpId: (lastLocalEdit as unknown as { _id: string })._id,
            },
            'concurrent-update conflict detected; recorded in sync_conflicts; preserving local',
          );
          conflictDetected = true;
        }
      }
    }

    if (!conflictDetected) {
      // Add to suppressor BEFORE the write so the change-stream callback,
      // which can fire concurrently with await replaceOne, always sees it.
      if (suppressor) {
        suppressor.add(op.ns, docId);
      }
      await coll.replaceOne(
        { _id: docId } as Record<string, unknown>,
        fullDoc,
        { upsert: true },
      );
    }
  } else if (op.operationType === 'delete') {
    const docId = op.docId;
    // Update-delete conflict detection (Case A): peer wants to delete a
    // doc that we still have AND that our most recent local-origin op
    // updated rather than deleted. Both nodes diverged on this doc;
    // preserve the local update.
    const local = await coll.findOne(
      { _id: docId } as Record<string, unknown>,
    );
    if (local) {
      const lastLocalEdit = await syncOps.findOne(
        {
          'ns.coll': op.ns.coll,
          docId,
          origin: localNodeId,
        } as Record<string, unknown>,
        { sort: { seq: -1 } },
      );
      if (
        lastLocalEdit &&
        (lastLocalEdit as { operationType?: string }).operationType !==
          'delete'
      ) {
        const localHash = docLeafHash(local as DocWithHash);
        await recordConflict({
          db,
          ns: op.ns,
          docId,
          local,
          localHash,
          lastLocalEdit: lastLocalEdit as unknown as SyncOp,
          remoteOp: op,
          remoteHash: null,
          localNodeId,
          conflictType: 'update-delete',
        });
        fastify.log.warn?.(
          { docId, opId: op._id, peerOrigin: op.origin },
          'update-delete conflict: peer deleted, local updated; preserving local',
        );
        conflictDetected = true;
      }
    }
    if (!conflictDetected) {
      if (suppressor) {
        suppressor.add(op.ns, docId);
      }
      await coll.deleteOne({ _id: docId } as Record<string, unknown>);
    }
  } else {
    fastify.log.warn?.(
      { opId: op._id, type: op.operationType },
      'unknown op type',
    );
    return { applied: false, reason: 'unknown-op-type' };
  }

  // Record remote operation locally. Use insertOne but tolerate E11000:
  // a duplicate-key here means a concurrent pollPeers cycle (or a previous
  // crash mid-batch) already inserted this op. The doc payload is identical
  // (deterministic _id = origin_seq), so treat it as success and continue.
  try {
    await syncOps.insertOne(op as unknown as OptionalId<Document>);
  } catch (err) {
    const code = (err as { code?: number } | null)?.code;
    if (code === 11000) {
      fastify.log.debug?.(
        { opId: op._id, origin: op.origin, seq: op.seq },
        'sync_ops insert E11000 (already recorded by concurrent batch); continuing',
      );
    } else {
      throw err;
    }
  }

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

  return conflictDetected
    ? { applied: false, reason: 'conflict-recorded' }
    : { applied: true };
}

/**
 * Records a concurrent-update conflict in `sync_conflicts`. Idempotent on
 * `conflictId` — re-running the same op detection on a re-pull won't
 * duplicate. Shape matches what the UI / `conflict-api-simple.ts` expects.
 */
async function recordConflict(opts: {
  db: Db;
  ns: Namespace;
  docId: unknown;
  local: Record<string, unknown> | null;
  localHash: string | null;
  lastLocalEdit: SyncOp;
  remoteOp: SyncOp;
  remoteHash: string | null;
  localNodeId: string;
  conflictType?: 'concurrent-update' | 'update-delete' | 'concurrent-insert';
}): Promise<void> {
  const {
    db,
    ns,
    docId,
    local,
    localHash,
    lastLocalEdit,
    remoteOp,
    remoteHash,
    localNodeId,
    conflictType = 'concurrent-update',
  } = opts;
  const conflictId = `conflict-${ns.coll}-${String(docId)}-${lastLocalEdit._id}-${remoteOp._id}`;
  const tsToMs = (ts: unknown): number => {
    if (typeof ts === 'string') return new Date(ts).getTime();
    if (typeof ts === 'number') return ts;
    return Date.now();
  };
  await db.collection('sync_conflicts').updateOne(
    { conflictId },
    {
      $setOnInsert: {
        conflictId,
        documentId: String(docId),
        collection: ns.coll,
        database: ns.db,
        detectedAt: Date.now(),
        status: 'pending',
        conflictType,
        versions: [
          {
            documentId: String(docId),
            data: local,
            timestamp: tsToMs(lastLocalEdit.ts),
            nodeId: localNodeId,
            operationId: lastLocalEdit._id,
            operationType: lastLocalEdit.operationType,
            stateHash: localHash,
            componentsHash: lastLocalEdit.chainHash,
          },
          {
            documentId: String(docId),
            data: remoteOp.payload?.fullDocument ?? null,
            timestamp: tsToMs(remoteOp.ts),
            nodeId: remoteOp.origin,
            operationId: remoteOp._id,
            operationType: remoteOp.operationType,
            stateHash: remoteHash,
            componentsHash: remoteOp.chainHash,
          },
        ],
      },
    },
    { upsert: true },
  );
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
