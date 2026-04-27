// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import type { Db, Document, ObjectId } from 'mongodb';
import { createHash } from 'node:crypto';

import { computeIntegrityHash } from './integrity-hash.ts';
import {
  clearDirtyForCollection,
  listDirtyForCollection,
} from './state-dirty.ts';

/**
 * Computes SHA-256 hash of a string
 * @param str - String to hash
 * @returns 64-character hexadecimal hash
 */
function sha256Hex(str: string): string {
  return createHash('sha256').update(str).digest('hex');
}

/**
 * Document with optional integrity hash field
 */
export interface DocWithHash extends Document {
  _id: ObjectId | string | number;
  __h?: string;
}

/**
 * Merkle tree partition metadata
 */
export interface MerklePartition {
  _id: string;
  coll: string;
  idx: number;
  minId: ObjectId | string | number;
  maxId: ObjectId | string | number;
  count: number;
  root: string;
  ts: number;
  updatedAt: string;
}

/**
 * State checkpoint document
 */
export interface StateCheckpoint {
  _id: string;
  ts: number;
  updatedAt: string;
  mode: 'incremental' | 'full';
  partitionSize: number;
  dbRoot: string;
  collections: Record<string, { root: string; partitions: number }>;
  covers: unknown | null;
}

/**
 * Computes stable "leaf" hash for a document.
 * Prefers stored __h field, otherwise computes integrity hash.
 * @param doc - Document to hash
 * @returns Hash string or null if doc is null/undefined
 */
export function docLeafHash(
  doc: DocWithHash | null | undefined,
): string | null {
  if (!doc) return null;
  if (doc.__h) return String(doc.__h);

  // computeIntegrityHash should ignore __h anyway,
  // but we remove it to be safe.
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { __h, ...rest } = doc;
  return computeIntegrityHash(rest as Record<string, unknown>);
}

/**
 * Retrieves the most recent state checkpoint from database
 * @param db - MongoDB database instance
 * @returns Latest checkpoint or null if none exists
 */
export async function getLatestCheckpoint(
  db: Db,
): Promise<StateCheckpoint | null> {
  const last = await db
    .collection<StateCheckpoint>('state_checkpoints')
    .find({})
    .sort({ ts: -1 })
    .limit(1)
    .toArray();

  return last[0] || null;
}

/**
 * Options for computing state checkpoint
 */
export interface ComputeStateCheckpointOptions {
  /** MongoDB database instance */
  db: Db;
  /** Collections to ignore during checkpoint computation */
  ignoredColls?: Set<string>;
  /** Number of documents per partition */
  partitionSize?: number;
  /** Whether to store leaf hashes (unused in current implementation) */
  storeLeaves?: boolean;
  /** Checkpoint mode */
  mode?: 'incremental' | 'full';
}

/**
 * Computes deterministic state checkpoint using merkle tree partitioning.
 * Process:
 * - Iterate collections sorted by name
 * - Iterate docs sorted by _id ASC
 * - Partition by fixed partitionSize in deterministic order
 * - Compute partition root from ordered leaves
 * - Compute collection root from ordered partition roots
 * - Compute db root from ordered collection roots
 * @param options - Checkpoint computation options
 * @returns Created state checkpoint document
 */
export async function computeStateCheckpoint(
  options: ComputeStateCheckpointOptions,
): Promise<StateCheckpoint> {
  const {
    db,
    ignoredColls = new Set(),
    partitionSize = 50000,
    mode = 'incremental',
  } = options;
  const nowIso = new Date().toISOString();
  const ts = Date.now();

  // Pick collections deterministically
  const all = (await db.listCollections({}, { nameOnly: true }).toArray())
    .map((x: { name: string }) => x.name)
    .filter(
      (name: string) =>
        name && !name.startsWith('system.') && !ignoredColls.has(name),
    )
    .sort((a: string, b: string) => a.localeCompare(b));

  const collections: Record<string, { root: string; partitions: number }> = {};
  const dbPieces: string[] = [];

  for (const collName of all) {
    const coll = db.collection<DocWithHash>(collName);

    // Fast-path eligibility: when the first sampled doc already has its
    // integrity hash stored (collection has been run through the backfill
    // script), project only `_id` + `__h` from the cursor below. Sample
    // approach is O(1) — `findOne({ __h: $exists: false })` would scan
    // the collection looking for a non-match and become the bottleneck on
    // large backfilled collections. If a later doc unexpectedly lacks
    // `__h`, `docLeafHash` returns null and the partition root reflects
    // that — caller should run backfill again.
    const sample = await coll.findOne(
      {},
      { projection: { __h: 1 }, sort: { _id: 1 } },
    );
    const useHashProjection = sample !== null && sample.__h != null;

    // Check if we can use incremental mode for this collection
    let useIncremental = mode === 'incremental';
    let dirtyPartitions: Set<number> = new Set();
    let cachedPartitions: MerklePartition[] = [];

    if (useIncremental) {
      // Get dirty status
      const dirtyStatus = await listDirtyForCollection(db, collName);

      if (dirtyStatus.full) {
        // Full rescan required for this collection
        console.log(
          `   [${collName}] Full rescan required (dirty status: FULL)`,
        );
        useIncremental = false;
      } else {
        // Load cached partitions
        cachedPartitions = await db
          .collection<MerklePartition>('state_merkle')
          .find({ coll: collName })
          .sort({ idx: 1 })
          .toArray();

        if (cachedPartitions.length === 0) {
          // No cache available, must do full scan
          console.log(`   [${collName}] No cache available - full scan needed`);
          useIncremental = false;
        } else {
          // Use incremental mode with these dirty partitions
          dirtyPartitions = new Set(dirtyStatus.partitions);
          console.log(
            `   [${collName}] Incremental mode: ${dirtyPartitions.size} dirty partitions out of ${cachedPartitions.length}`,
          );
        }
      }
    }

    const partRoots: string[] = [];

    if (useIncremental && cachedPartitions.length > 0) {
      // INCREMENTAL MODE: Reuse cached hashes, only recompute dirty partitions
      console.log(
        `   [${collName}] Using incremental mode with ${cachedPartitions.length} cached partitions`,
      );

      // The last partition is allowed to grow on append-after-max — when
      // `markDirtyById` flags it for an _id past every known maxId, we
      // recompute open-ended ($gte minId, no upper bound) so the new docs
      // are included and `maxId` is refreshed.
      const lastIdx = cachedPartitions[cachedPartitions.length - 1].idx;

      for (const cached of cachedPartitions) {
        if (dirtyPartitions.has(cached.idx)) {
          // Recompute this dirty partition
          const isLast = cached.idx === lastIdx;
          console.log(
            `   [${collName}] Recomputing dirty partition ${cached.idx}${isLast ? ' (open-ended, may have grown)' : ''}`,
          );
          const filter: Record<string, unknown> = isLast
            ? { _id: { $gte: cached.minId } }
            : { _id: { $gte: cached.minId, $lte: cached.maxId } };
          // Fast path: when every doc has __h (collection is backfilled),
          // project only `{_id, __h}` so the cursor sends ~80 bytes/doc
          // instead of the full document, and we skip the per-doc
          // canonical-JSON walk inside docLeafHash.
          const cursor = coll.find(filter, {
            sort: { _id: 1 },
            batchSize: 5000,
            ...(useHashProjection
              ? { projection: { _id: 1, __h: 1 } }
              : {}),
          });

          const partLines: string[] = [];
          let partCount = 0;
          let partMinId: ObjectId | string | number | null = null;
          let partMaxId: ObjectId | string | number | null = null;

          for await (const doc of cursor) {
            const idStr = String(doc._id);
            const h = docLeafHash(doc);

            if (partCount === 0) partMinId = doc._id;
            partMaxId = doc._id;

            partLines.push(`${idStr}:${h}`);
            partCount += 1;
          }

          if (partCount > 0) {
            const payload = partLines.join('\n');
            const root = sha256Hex(payload);

            // Update cached partition
            const metaDoc: MerklePartition = {
              _id: `${collName}::p${cached.idx}`,
              coll: collName,
              idx: cached.idx,
              minId: partMinId!,
              maxId: partMaxId!,
              count: partCount,
              root,
              ts,
              updatedAt: nowIso,
            };

            await db
              .collection<MerklePartition>('state_merkle')
              .replaceOne({ _id: metaDoc._id }, metaDoc, { upsert: true });

            partRoots.push(root);
          } else {
            // Partition became empty, use cached hash but mark for potential cleanup
            partRoots.push(cached.root);
          }
        } else {
          // Use cached hash - no need to scan documents!
          partRoots.push(cached.root);
        }
      }

      // Clear dirty markers for this collection
      await clearDirtyForCollection(db, collName);

      // Collection root from ordered partition roots
      const collRoot = sha256Hex(
        partRoots.map((r, i) => `${i}:${r}`).join('\n'),
      );
      collections[collName] = {
        root: collRoot,
        partitions: cachedPartitions.length,
      };

      dbPieces.push(`${collName}:${collRoot}`);
    } else {
      // FULL MODE: Scan all documents and compute all partitions.
      // Same `{_id, __h}` projection fast path as the incremental branch
      // when the collection is fully backfilled.
      const cursor = coll.find(
        {},
        {
          sort: { _id: 1 },
          batchSize: 5000,
          ...(useHashProjection ? { projection: { _id: 1, __h: 1 } } : {}),
        },
      );

      let partIdx = 0;
      let partCount = 0;
      let partMinId: ObjectId | string | number | null = null;
      let partMaxId: ObjectId | string | number | null = null;

      let partLines: string[] = [];

      async function flushPartition(): Promise<void> {
        if (partCount === 0) return;

        const payload = partLines.join('\n');
        const root = sha256Hex(payload);

        const metaDoc: MerklePartition = {
          _id: `${collName}::p${partIdx}`,
          coll: collName,
          idx: partIdx,
          minId: partMinId!,
          maxId: partMaxId!,
          count: partCount,
          root,
          ts,
          updatedAt: nowIso,
        };

        await db
          .collection<MerklePartition>('state_merkle')
          .replaceOne({ _id: metaDoc._id }, metaDoc, { upsert: true });

        partRoots.push(root);

        // Next partition
        partIdx += 1;
        partCount = 0;
        partMinId = null;
        partMaxId = null;
        partLines = [];
      }

      for await (const doc of cursor) {
        const idStr = String(doc._id);
        const h = docLeafHash(doc);

        if (partCount === 0) partMinId = doc._id;
        partMaxId = doc._id;

        // IMPORTANT: keep order stable
        partLines.push(`${idStr}:${h}`);
        partCount += 1;

        if (partCount >= partitionSize) {
          await flushPartition();
        }
      }

      // Flush tail
      await flushPartition();

      // Collection root from ordered partition roots
      const collRoot = sha256Hex(
        partRoots.map((r, i) => `${i}:${r}`).join('\n'),
      );
      collections[collName] = { root: collRoot, partitions: partIdx };

      dbPieces.push(`${collName}:${collRoot}`);
    }
  }

  const dbRoot = sha256Hex(dbPieces.join('\n'));

  // NOTE: covers headSeq/headHash is filled by caller or left null here
  const cp: StateCheckpoint = {
    _id: `cp_${nowIso}_${sha256Hex(dbRoot).slice(0, 12)}`,
    ts,
    updatedAt: nowIso,
    mode,
    partitionSize,
    dbRoot,
    collections,
    covers: null,
  };

  await db.collection<StateCheckpoint>('state_checkpoints').insertOne(cp);

  return cp;
}
