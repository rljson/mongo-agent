// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { BsInterface } from '@rljson/bs';
import { hip, hsh } from '@rljson/hash';
import type { Db } from 'mongodb';

import {
  computeStateCheckpoint,
  getLatestCheckpoint,
} from './hashing/state-hash.ts';

/**
 * Simple RLJSON state change entry
 * Captures entire DB state before/after changes
 */
export interface SimpleStateChange {
  id: string;
  hash: string;
  type: string;
  json: {
    prevStateHash: string | null;
    currentStateHash: string;
    timestamp: number;
    operation: string;
    description?: string;
  };
  _hash?: string;
}

/**
 * Simple state log using RLJSON format
 * Tracks entire DB state changes over time
 */
export class SimpleStateLog {
  private db: Db;
  private bs?: BsInterface;
  private changeLog: SimpleStateChange[] = [];
  private prevStateHash: string | null = null;

  constructor(db: Db, bs?: BsInterface) {
    this.db = db;
    this.bs = bs;
  }

  /**
   * Initialize with current DB state
   */
  async initialize(): Promise<void> {
    const checkpoint = await getLatestCheckpoint(this.db);
    this.prevStateHash = checkpoint?.dbRoot || null;
  }

  /**
   * Capture current DB state and create change entry
   * @param operation - Operation type (e.g., 'insert', 'update', 'delete', 'manual')
   * @param description - Optional description
   * @returns Created state change entry
   */
  async captureStateChange(
    operation: string,
    description?: string,
  ): Promise<SimpleStateChange> {
    // Compute current state
    const checkpoint = await computeStateCheckpoint({
      db: this.db,
      ignoredColls: new Set([
        'state_checkpoints',
        'state_merkle',
        'sync_ops',
        'state_changelog',
      ]),
      partitionSize: 50000,
      mode: 'incremental',
    });

    const currentStateHash = checkpoint.dbRoot;

    // Create simple RLJSON entry
    const entry: SimpleStateChange = {
      id: `change_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      hash: '', // Will be computed
      type: 'state_change',
      json: {
        prevStateHash: this.prevStateHash,
        currentStateHash,
        timestamp: Date.now(),
        operation,
        description,
      },
    };

    // Compute hash for the entry content (json field)
    const hashedJson = hsh(entry.json);
    entry.hash = hashedJson._hash as string;

    // Compute hash for entire entry
    hip(entry);

    // Store in blob storage if available
    if (this.bs) {
      const blobHash = await this.bs.store(JSON.stringify(entry));
    }

    // Add to in-memory log
    this.changeLog.push(entry);

    // Update prev state for next capture
    this.prevStateHash = currentStateHash;

    return entry;
  }

  /**
   * Get all captured state changes
   */
  getChangeLog(): SimpleStateChange[] {
    return [...this.changeLog];
  }

  /**
   * Compare two states
   * @param hash1 - First state hash
   * @param hash2 - Second state hash
   * @returns True if states are identical
   */
  compareStates(hash1: string, hash2: string): boolean {
    return hash1 === hash2;
  }

  /**
   * Find state change by state hash
   * @param stateHash - State hash to search for
   * @returns Matching entries
   */
  findByStateHash(stateHash: string): SimpleStateChange[] {
    return this.changeLog.filter(
      (entry) =>
        entry.json.prevStateHash === stateHash ||
        entry.json.currentStateHash === stateHash,
    );
  }

  /**
   * Get state transition chain
   * Shows how DB state evolved over time
   */
  getStateChain(): Array<{
    from: string | null;
    to: string;
    operation: string;
    timestamp: number;
  }> {
    return this.changeLog.map((entry) => ({
      from: entry.json.prevStateHash,
      to: entry.json.currentStateHash,
      operation: entry.json.operation,
      timestamp: entry.json.timestamp,
    }));
  }

  /**
   * Persist change log to MongoDB collection
   * @param collectionName - Collection name (default: 'state_changelog')
   */
  async persist(collectionName = 'state_changelog'): Promise<void> {
    const coll = this.db.collection(collectionName);
    if (this.changeLog.length > 0) {
      await coll.insertMany(this.changeLog);
    }
  }

  /**
   * Load change log from MongoDB collection
   * @param collectionName - Collection name (default: 'state_changelog')
   */
  async load(collectionName = 'state_changelog'): Promise<void> {
    const coll = this.db.collection<SimpleStateChange>(collectionName);
    const entries = await coll.find({}).sort({ 'json.timestamp': 1 }).toArray();
    this.changeLog = entries;

    // Set prev state to latest
    if (entries.length > 0) {
      const last = entries[entries.length - 1];
      this.prevStateHash = last.json.currentStateHash;
    }
  }
}
