// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import type { Db } from 'mongodb';

/**
 * Sync state document stored in sync_state collection.
 */
export interface SyncState {
  /** Origin identifier (document ID) */
  _id: string;
  /** Last sequence number pulled from this origin */
  lastSeqPulled: number;
  /** Last hash pulled from this origin */
  lastHashPulled: string;
  /** Last sequence number applied from this origin */
  lastSeqApplied: number;
  /** Last hash applied from this origin */
  lastHashApplied: string;
  /** When this state was last updated */
  updatedAt?: string;
}

/**
 * Get sync state for an origin.
 * Returns default state if not found.
 * @param db - MongoDB database instance
 * @param origin - Origin identifier
 * @returns Sync state document
 */
export async function getState(db: Db, origin: string): Promise<SyncState> {
  const s = await db
    .collection<SyncState>('sync_state')
    .findOne({ _id: origin });
  return (
    s || {
      _id: origin,
      lastSeqPulled: 0,
      lastHashPulled: 'GENESIS',
      lastSeqApplied: 0,
      lastHashApplied: 'GENESIS',
    }
  );
}

/**
 * Mark sequence and hash as pulled from an origin.
 * Uses $max to ensure seq only increases.
 * @param db - MongoDB database instance
 * @param origin - Origin identifier
 * @param seq - Sequence number
 * @param hash - Hash value
 * @returns Promise that resolves when update completes
 */
export async function markPulled(
  db: Db,
  origin: string,
  seq: number,
  hash: string
): Promise<void> {
  await db.collection('sync_state').updateOne(
    { _id: origin } as Record<string, unknown>,
    {
      $max: { lastSeqPulled: seq },
      $set: { lastHashPulled: hash, updatedAt: new Date().toISOString() },
    },
    { upsert: true }
  );
}

/**
 * Mark sequence and hash as applied from an origin.
 * Uses $max to ensure seq only increases.
 * @param db - MongoDB database instance
 * @param origin - Origin identifier
 * @param seq - Sequence number
 * @param hash - Hash value
 * @returns Promise that resolves when update completes
 */
export async function markApplied(
  db: Db,
  origin: string,
  seq: number,
  hash: string
): Promise<void> {
  await db.collection('sync_state').updateOne(
    { _id: origin } as Record<string, unknown>,
    {
      $max: { lastSeqApplied: seq },
      $set: { lastHashApplied: hash, updatedAt: new Date().toISOString() },
    },
    { upsert: true }
  );
}
