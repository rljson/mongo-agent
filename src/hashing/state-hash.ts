// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import type { Db, Document, ObjectId } from 'mongodb';
import { createHash } from 'node:crypto';

import { computeIntegrityHash } from './integrity-hash.ts';


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

    // Deterministic cursor
    const cursor = coll.find({}, { sort: { _id: 1 }, batchSize: 5000 });

    let partIdx = 0;
    let partCount = 0;
    let partMinId: ObjectId | string | number | null = null;
    let partMaxId: ObjectId | string | number | null = null;

    const partRoots: string[] = [];
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
    const collRoot = sha256Hex(partRoots.map((r, i) => `${i}:${r}`).join('\n'));
    collections[collName] = { root: collRoot, partitions: partIdx };

    dbPieces.push(`${collName}:${collRoot}`);
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
