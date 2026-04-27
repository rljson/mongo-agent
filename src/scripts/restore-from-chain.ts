// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { MongoClient } from 'mongodb';

import { computeIntegrityHash } from '../hashing/integrity-hash.ts';
import { markDirtyById } from '../hashing/state-dirty.ts';

/**
 * Sync operation document, mirroring the shape stored in `sync_ops`.
 */
interface SyncOpDoc {
  _id: string;
  origin: string;
  seq: number;
  prevHash?: string;
  chainHash?: string;
  ns: { db: string; coll: string };
  operationType: 'insert' | 'update' | 'replace' | 'delete';
  docId: unknown;
  payload?: { fullDocument?: Record<string, unknown> } | null;
  ts: string;
}

/**
 * Per-doc leaf hash: stored `__h` if present, otherwise canonical-JSON
 * integrity hash of the doc minus `__h`.
 */
function leafHashOf(
  doc: Record<string, unknown> | null | undefined,
): string | null {
  if (!doc) return null;
  if (doc.__h) return String(doc.__h);
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { __h, ...rest } = doc;
  return computeIntegrityHash(rest as Record<string, unknown>);
}

interface RestoreStats {
  totalOps: number;
  chainOk: boolean;
  chainBreakAtSeq?: number;
  restoredMissing: number;
  restoredDrifted: number;
  redeleted: number;
  okInserts: number;
  okDeletes: number;
  noPayload: number;
  other: number;
  durationMs: number;
  touchedCollections: string[];
}

/**
 * Restores local Mongo state from the local `sync_ops` chain.
 *
 * Walks every op in seq order and ensures the local collection reflects what
 * the chain says it should. This is the agent equivalent of `git fsck` plus
 * a `git checkout` of every commit's effect — the chain is the recovery log,
 * no peer query required.
 *
 * Three repair behaviors:
 *   - missing doc → `replaceOne(payload.fullDocument, { upsert: true })`
 *   - content drift (doc exists but its leaf hash differs from what the op
 *     payload says) → same `replaceOne`
 *   - re-deletion (op said delete, doc came back somehow) → `deleteOne`
 *
 * Limitations:
 *   - Only repairs docs whose history is in `sync_ops`. Pre-chain baseline
 *     data (e.g. mongorestore'd before the agent started) needs the
 *     hash-tree set-diff approach in the test-restore-from-hash-mismatch
 *     spec — chain replay can't conjure ops that don't exist.
 *   - Linear walk from genesis. For very long chains, anchor on the latest
 *     `state_checkpoints` entry instead of replaying everything.
 *
 * Env:
 *   MONGO_URI         (required)
 *   DB_NAME           (required)
 *   DRY_RUN=1         report what would change, don't write
 *   SKIP_CHAIN_VERIFY=1  skip the chainHash recompute (faster, less safe)
 *
 * Idempotent — running twice on a healthy DB is a no-op.
 */
async function main(): Promise<void> {
  const uri = process.env.MONGO_URI;
  if (!uri) throw new Error('MONGO_URI required');
  const dbName = process.env.DB_NAME;
  if (!dbName) throw new Error('DB_NAME required');
  const dryRun =
    process.env.DRY_RUN === '1' || process.env.DRY_RUN === 'true';
  const skipChainVerify = process.env.SKIP_CHAIN_VERIFY === '1';

  const c = new MongoClient(uri);
  await c.connect();
  try {
    const db = c.db(dbName);
    const syncOps = db.collection<SyncOpDoc>('sync_ops');
    const total = await syncOps.countDocuments();
    console.log(`restore-from-chain: walking ${total} sync_ops...`);
    if (dryRun) console.log('  (DRY_RUN — no writes will be performed)');

    const stats: RestoreStats = {
      totalOps: total,
      chainOk: true,
      restoredMissing: 0,
      restoredDrifted: 0,
      redeleted: 0,
      okInserts: 0,
      okDeletes: 0,
      noPayload: 0,
      other: 0,
      durationMs: 0,
      touchedCollections: [],
    };
    const t0 = Date.now();
    // Track all docs we modify so we can dirty-mark them at the end and the
    // next `computeStateCheckpoint` recompute picks up the changes without
    // requiring the agent's change-stream to have observed them.
    const touched = new Map<string, Array<unknown>>();
    function touch(coll: string, id: unknown): void {
      let arr = touched.get(coll);
      if (!arr) {
        arr = [];
        touched.set(coll, arr);
      }
      arr.push(id);
    }

    let prevChain = 'GENESIS';
    const cursor = syncOps.find({}).sort({ seq: 1 });
    for await (const op of cursor) {
      // Phase 1: chain integrity link check (cheap — string compare).
      if (!skipChainVerify) {
        if (op.prevHash !== prevChain) {
          console.error(
            `  ⚠️ chain break at seq ${op.seq}: expected prevHash=${prevChain}, got ${op.prevHash}`,
          );
          stats.chainOk = false;
          stats.chainBreakAtSeq = stats.chainBreakAtSeq ?? op.seq;
        }
        prevChain = op.chainHash || 'INVALID';
      }

      const coll = db.collection(op.ns.coll);

      // Phase 2: replay this op's effect.
      if (
        op.operationType === 'insert' ||
        op.operationType === 'update' ||
        op.operationType === 'replace'
      ) {
        const fd = op.payload?.fullDocument;
        if (!fd) {
          stats.noPayload++;
          continue;
        }
        const docId = (fd as { _id?: unknown })._id;
        const local = await coll.findOne(
          { _id: docId } as Record<string, unknown>,
          { projection: { _id: 1, __h: 1 } as Record<string, 0 | 1> },
        );

        if (!local) {
          if (!dryRun) {
            await coll.replaceOne(
              { _id: docId } as Record<string, unknown>,
              fd,
              { upsert: true },
            );
          }
          console.log(
            `  [seq ${op.seq}] RESTORE   ${op.ns.coll}._id=${docId} (missing)`,
          );
          stats.restoredMissing++;
          touch(op.ns.coll, docId);
          continue;
        }

        // Doc exists. Compare leaf hashes to catch content drift.
        const expected = leafHashOf(fd);
        const localProjHash = (local as { __h?: unknown }).__h;
        let localHash: string | null;
        if (localProjHash != null) {
          localHash = String(localProjHash);
        } else {
          // Local doc has no `__h` — compute on the fly. Slow path; one
          // findOne per such doc. After running backfill-hashes once,
          // this branch becomes unreachable.
          const full = await coll.findOne(
            { _id: docId } as Record<string, unknown>,
          );
          localHash = leafHashOf(full as Record<string, unknown> | null);
        }

        if (expected != null && localHash != null && expected !== localHash) {
          if (!dryRun) {
            await coll.replaceOne(
              { _id: docId } as Record<string, unknown>,
              fd,
              { upsert: true },
            );
          }
          console.log(
            `  [seq ${op.seq}] RESTORE   ${op.ns.coll}._id=${docId} (drifted: ${localHash.slice(0, 8)} → ${expected.slice(0, 8)})`,
          );
          stats.restoredDrifted++;
          touch(op.ns.coll, docId);
        } else {
          stats.okInserts++;
        }
      } else if (op.operationType === 'delete') {
        const docId = op.docId;
        const local = await coll.findOne(
          { _id: docId } as Record<string, unknown>,
          { projection: { _id: 1 } as Record<string, 0 | 1> },
        );
        if (local) {
          if (!dryRun) {
            await coll.deleteOne({ _id: docId } as Record<string, unknown>);
          }
          console.log(
            `  [seq ${op.seq}] RE-DELETE ${op.ns.coll}._id=${docId}`,
          );
          stats.redeleted++;
          touch(op.ns.coll, docId);
        } else {
          stats.okDeletes++;
        }
      } else {
        stats.other++;
      }
    }

    // Phase 3: mark dirty so the next state hash recompute reflects the
    // repair. Without this, an `incremental` rerun would happily reuse the
    // pre-repair partition cache and report a stale dbRoot.
    if (!dryRun && touched.size > 0) {
      const summary = Array.from(touched.entries())
        .map(([coll, ids]) => `${coll}=${ids.length}`)
        .join(', ');
      console.log(`marking dirty: ${summary}`);
      for (const [collName, ids] of touched.entries()) {
        for (const id of ids) {
          await markDirtyById(
            db,
            collName,
            id as Parameters<typeof markDirtyById>[2],
            { reason: 'restore-from-chain' },
          );
        }
      }
    }

    stats.durationMs = Date.now() - t0;
    stats.touchedCollections = Array.from(touched.keys()).sort();
    console.log(JSON.stringify(stats, null, 2));

    if (!stats.chainOk) {
      console.error(
        '\n⚠️ CHAIN INTEGRITY VIOLATION — local sync_ops chain has broken links.',
      );
      console.error(
        '   The walk continued, but the underlying chain damage needs investigation.',
      );
      console.error(
        '   Consider re-pulling from a healthy peer: drop sync_ops + sync_state,',
      );
      console.error('   restart the agent so the missing ops re-flow in.');
      process.exit(2);
    }
  } finally {
    await c.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
