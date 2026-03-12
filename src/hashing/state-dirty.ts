// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import type { Db, ObjectId } from 'mongodb';

/**
 * Dirty partition tracking.
 *
 * state_dirty documents:
 * - partition dirty: \{ _id: "\<coll\>::p\<idx\>", coll, partition, dirtyAt \}
 * - full dirty:      \{ _id: "\<coll\>::FULL", coll, full: true, dirtyAt, reason \}
 */

/**
 * Dirty partition document
 */
export interface DirtyPartitionDoc {
  _id: string;
  coll: string;
  partition: number;
  dirtyAt: string;
}

/**
 * Full dirty marker document
 */
export interface DirtyFullDoc {
  _id: string;
  coll: string;
  full: true;
  dirtyAt: string;
  reason?: string;
}

/**
 * Union type for dirty documents
 */
export type DirtyDoc = DirtyPartitionDoc | DirtyFullDoc;

/**
 * Partition metadata from state_merkle
 */
export interface PartitionMeta {
  partition?: number;
  minId?: ObjectId | string | number;
  maxId?: ObjectId | string | number;
}

/**
 * Dirty collection status
 */
export interface DirtyStatus {
  full: boolean;
  partitions: number[];
}

/**
 * Options for marking dirty
 */
export interface MarkDirtyOptions {
  reason?: string;
}

/**
 * Creates partition dirty document ID
 * @param coll - Collection name
 * @param idx - Partition index
 * @returns Document ID string
 */
function dirtyPartId(coll: string, idx: number): string {
  return `${coll}::p${idx}`;
}

/**
 * Creates full dirty document ID
 * @param coll - Collection name
 * @returns Document ID string
 */
function dirtyFullId(coll: string): string {
  return `${coll}::FULL`;
}

/**
 * Creates indexes on state_dirty collection for efficient queries
 * @param db - MongoDB database instance
 */
export async function ensureDirtyIndexes(db: Db): Promise<void> {
  await db.collection('state_dirty').createIndex({ coll: 1, partition: 1 });
  await db.collection('state_dirty').createIndex({ dirtyAt: 1 });
}

/**
 * Finds partition metadata in state_merkle that covers docId by minId/maxId range.
 * @param db - MongoDB database instance
 * @param collName - Collection name
 * @param docId - Document ID to find partition for
 * @returns Partition metadata or null if not found
 */
async function findPartitionForId(
  db: Db,
  collName: string,
  docId: ObjectId | string | number
): Promise<PartitionMeta | null> {
  // Needs state_merkle to exist from a previous full scan.
  return db
    .collection<PartitionMeta>('state_merkle')
    .findOne(
      {
        coll: collName,
        minId: { $lte: docId },
        maxId: { $gte: docId },
      },
      { projection: { partition: 1, minId: 1, maxId: 1 } }
    );
}

/**
 * Marks a document as dirty by ID, updating the appropriate partition or full collection marker.
 * If partition metadata is not found, marks entire collection as dirty.
 * @param db - MongoDB database instance
 * @param collName - Collection name
 * @param docId - Document ID that changed
 * @param options - Optional reason for marking dirty
 */
export async function markDirtyById(
  db: Db,
  collName: string | null | undefined,
  docId: ObjectId | string | number,
  options: MarkDirtyOptions = {}
): Promise<void> {
  if (!collName) return;

  const { reason } = options;

  // If no merkle meta yet, force full scan next time
  const meta = await findPartitionForId(db, collName, docId).catch(() => null);

  const dirtyAt = new Date().toISOString();

  if (!meta || typeof meta.partition !== 'number') {
    // Could be: new docs in a gap / no checkpoint yet
    await db.collection<DirtyFullDoc>('state_dirty').updateOne(
      { _id: dirtyFullId(collName) },
      {
        $set: {
          coll: collName,
          full: true,
          dirtyAt,
          reason: reason || 'partition_not_found',
        },
      },
      { upsert: true }
    );
    return;
  }

  await db.collection<DirtyPartitionDoc>('state_dirty').updateOne(
    { _id: dirtyPartId(collName, meta.partition) },
    { $set: { coll: collName, partition: meta.partition, dirtyAt } },
    { upsert: true }
  );
}

/**
 * Lists dirty partitions for a collection
 * @param db - MongoDB database instance
 * @param collName - Collection name
 * @returns Dirty status with full flag and partition numbers
 */
export async function listDirtyForCollection(
  db: Db,
  collName: string
): Promise<DirtyStatus> {
  const docs = await db
    .collection<DirtyDoc>('state_dirty')
    .find({ coll: collName })
    .project({ _id: 1, full: 1, partition: 1 })
    .toArray();

  const full = docs.some((d) => 'full' in d && d.full === true);
  const parts = docs
    .filter((d): d is DirtyPartitionDoc => 'partition' in d && typeof d.partition === 'number')
    .map((d) => d.partition)
    .sort((a, b) => a - b);

  return { full, partitions: parts };
}

/**
 * Clears all dirty markers for a collection
 * @param db - MongoDB database instance
 * @param collName - Collection name
 */
export async function clearDirtyForCollection(
  db: Db,
  collName: string
): Promise<void> {
  await db.collection('state_dirty').deleteMany({ coll: collName });
}

/**
 * Clears specific dirty partition markers
 * @param db - MongoDB database instance
 * @param collName - Collection name
 * @param partitions - Array of partition indexes to clear
 */
export async function clearDirtyPartitions(
  db: Db,
  collName: string,
  partitions: number[]
): Promise<void> {
  if (!partitions || partitions.length === 0) return;
  const ids = partitions.map((p) => dirtyPartId(collName, p));
  await db.collection('state_dirty').deleteMany({ _id: { $in: ids } as never });
}
