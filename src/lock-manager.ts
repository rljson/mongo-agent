// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * Lock Manager for distributed record locking
 *
 * Prevents concurrent edits across multiple nodes by maintaining
 * a locking collection in MongoDB. When a record is locked by one
 * node, other nodes cannot modify it until the lock is released.
 */

import { Collection, Db } from 'mongodb';


/**
 * Common fields for all database records
 */
interface CommonFields {
  createdBy: string;
  updatedBy: string;
  status: number;
  version: number;
  recordNo: number;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * Lock record stored in the locking collection
 */
export interface LockRecord {
  _id: string; // Format: "typ-value" e.g., "8-127731"
  typ: number; // Type of the locked entity (e.g., 8 for users)
  value: string; // ID of the locked record
  key: string; // User/node key doing the locking
  name: string; // User/node name
  telefone?: string; // Optional phone number
  eMail?: string; // Optional email
  compName: string; // Computer/node name
  clientName?: string; // Optional client name
  commonFields: CommonFields;
}

/**
 * Lock history record - keeps track of released locks
 */
export interface LockHistoryRecord extends LockRecord {
  acquiredAt: Date;
  releasedAt: Date;
}

/**
 * Offline change record
 */
export interface OfflineChange {
  _id: string;
  typ: number;
  value: string; // record ID
  key: string; // who made the change
  changeTimestamp: Date;
  changeData: any; // the actual change data
  collection: string;
  database: string;
}

/**
 * Entity type mapping
 */
export const EntityType = {
  USERS: 8,
  ORDERS: 9,
  PRODUCTS: 10,
  ARTICLES: 11,
  // Add more entity types as needed
} as const;

export type EntityTypeValue = (typeof EntityType)[keyof typeof EntityType];

/**
 * Options for acquiring a lock
 */
export interface LockOptions {
  typ: number;
  value: string;
  key: string; // Who is locking (node/user ID)
  name: string; // Display name of locker
  compName: string; // Computer/node name
  telefone?: string;
  eMail?: string;
  clientName?: string;
}

/**
 * Lock Manager class
 */
export class LockManager {
  private lockCollection: Collection<LockRecord>;
  private lockHistoryCollection: Collection<LockHistoryRecord>;
  private offlineChangesCollection: Collection<OfflineChange>;

  constructor(private db: Db) {
    this.lockCollection = db.collection<LockRecord>('locking');
    this.lockHistoryCollection =
      db.collection<LockHistoryRecord>('lock_history');
    this.offlineChangesCollection =
      db.collection<OfflineChange>('offline_changes');
  }

  /**
   * Initialize the lock collection with indexes
   */
  async initialize(): Promise<void> {
    // Create indexes for efficient queries
    await this.lockCollection.createIndex(
      { typ: 1, value: 1 },
      { unique: false },
    );
    await this.lockCollection.createIndex({ key: 1 }, { unique: false });
    await this.lockCollection.createIndex(
      { 'commonFields.createdAt': 1 },
      { unique: false },
    );

    // Create indexes for lock history
    await this.lockHistoryCollection.createIndex(
      { typ: 1, value: 1 },
      { unique: false },
    );
    await this.lockHistoryCollection.createIndex(
      { acquiredAt: 1, releasedAt: 1 },
      { unique: false },
    );

    // Create indexes for offline changes
    await this.offlineChangesCollection.createIndex(
      { typ: 1, value: 1 },
      { unique: false },
    );
    await this.offlineChangesCollection.createIndex(
      { key: 1, changeTimestamp: 1 },
      { unique: false },
    );
  }

  /**
   * @param typ
   * @param value
   * Generate lock ID from type and value
   */
  private generateLockId(typ: number, value: string): string {
    return `${typ}-${value}`;
  }

  /**
   * Acquire a lock on a record
   * @param options
   * Returns true if lock was acquired, false if already locked
   */
  async acquireLock(options: LockOptions): Promise<boolean> {
    const lockId = this.generateLockId(options.typ, options.value);

    // Check if lock already exists
    const existingLock = await this.lockCollection.findOne({ _id: lockId });

    if (existingLock) {
      // Lock exists - check if it's by the same key (re-entrant lock)
      if (existingLock.key === options.key) {
        // Update the existing lock timestamp
        await this.lockCollection.updateOne(
          { _id: lockId },
          {
            $set: {
              'commonFields.updatedAt': new Date(),
              'commonFields.updatedBy': options.key,
            },
          },
        );
        return true;
      }
      // Lock held by another node/user
      return false;
    }

    // Create new lock
    const now = new Date();
    const lockRecord: LockRecord = {
      _id: lockId,
      typ: options.typ,
      value: options.value,
      key: options.key,
      name: options.name,
      telefone: options.telefone || '',
      eMail: options.eMail || '',
      compName: options.compName,
      clientName: options.clientName || '',
      commonFields: {
        createdBy: options.key,
        updatedBy: options.key,
        status: 0,
        version: 1,
        recordNo: 0,
        createdAt: now,
        updatedAt: now,
      },
    };

    try {
      await this.lockCollection.insertOne(lockRecord);
      return true;
    } catch (err: any) {
      // Handle race condition where another node acquired lock simultaneously
      if (err.code === 11000) {
        // Duplicate key error
        return false;
      }
      throw err;
    }
  }

  /**
   * Release a lock on a record
   * Only the lock owner can release their lock
   * @param typ
   * @param value
   * @param key
   * Also saves to lock history for offline conflict detection
   */
  async releaseLock(typ: number, value: string, key: string): Promise<boolean> {
    const lockId = this.generateLockId(typ, value);

    // Get the lock before deleting to save history
    const lock = await this.lockCollection.findOne({
      _id: lockId,
      key: key,
    });

    if (!lock) {
      return false;
    }

    // Save to lock history
    const historyRecord: LockHistoryRecord = {
      ...lock,
      acquiredAt: lock.commonFields.createdAt,
      releasedAt: new Date(),
    };

    await this.lockHistoryCollection.insertOne(historyRecord);

    // Delete the active lock
    const result = await this.lockCollection.deleteOne({
      _id: lockId,
      key: key,
    });

    return result.deletedCount === 1;
  }

  /**
   * Check if a record is lockede
   * @param typ
   * @param valu
   * Returns the lock record if locked, null otherwise
   */
  async isLocked(typ: number, value: string): Promise<LockRecord | null> {
    const lockId = this.generateLockId(typ, value);
    return await this.lockCollection.findOne({ _id: lockId });
  }

  /** key
   * @param typ
   * @param value
   * @param
   * Check if a record is locked by a specific key
   */
  async isLockedBy(typ: number, value: string, key: string): Promise<boolean> {
    const lock = await this.isLocked(typ, value);
    return lock !== null && lock.key === key;
  }

  /**
   * @param key
   * Get all locks held by a specific key (node/user)
   */
  async getLocksBy(key: string): Promise<LockRecord[]> {
    return await this.lockCollection.find({ key }).toArray();
  }

  /**
   * Release all locks held by a specific key
   * @param key
   * Useful for cleanup when a node disconnects
   */
  async releaseAllLocks(key: string): Promise<number> {
    const result = await this.lockCollection.deleteMany({ key });
    return result.deletedCount;
  }

  /**
   * @param maxAgeMs
   * Remove stale locks older than the specified age in milliseconds
   */
  async removeOldLocks(maxAgeMs: number): Promise<number> {
    const cutoffDate = new Date(Date.now() - maxAgeMs);

    const result = await this.lockCollection.deleteMany({
      'commonFields.updatedAt': { $lt: cutoffDate },
    });

    return result.deletedCount;
  }

  /**
   * Verify if an operation is allowed on a record
   * @param typ
   * @param value
   * @param key
   * Returns true if no lock exists or if locked by the same key
   */
  async canModify(typ: number, value: string, key: string): Promise<boolean> {
    const lock = await this.isLocked(typ, value);

    // No lock exists - modification allowed
    if (!lock) {
      return true;
    }

    // Lock exists but owned by the same key - modification allowed
    return lock.key === key;
  }

  /**
   * Attempt to acquire lock with retry
   * @param options
   * @param maxRetries
   * @param retryDelayMs
   */
  async acquireLockWithRetry(
    options: LockOptions,
    maxRetries = 3,
    retryDelayMs = 100,
  ): Promise<boolean> {
    for (let i = 0; i < maxRetries; i++) {
      const acquired = await this.acquireLock(options);
      if (acquired) {
        return true;
      }

      if (i < maxRetries - 1) {
        await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
      }
    }

    return false;
  }

  /**
   * Record an offline change made by a node
   * @param typ
   * @param value
   * @param key
   * @param changeData
   * @param collection
   * @param database
   * This will be checked against lock history when the node comes back online
   */
  async recordOfflineChange(
    typ: number,
    value: string,
    key: string,
    changeData: any,
    collection: string,
    database: string,
  ): Promise<void> {
    const changeId = `${typ}-${value}-${Date.now()}`;
    const offlineChange: OfflineChange = {
      _id: changeId,
      typ,
      value,
      key,
      changeTimestamp: new Date(),
      changeData,
      collection,
      database,
    };

    await this.offlineChangesCollection.insertOne(offlineChange);
  }

  /**
   * Detect conflicts between offline changes and lock history
   * Returns list of conflicts where offline changes were made
   * @param key to records
   * that were locked by other nodes during the offline period
   */
  async detectOfflineConflicts(
    key: string,
  ): Promise<Array<{ change: OfflineChange; lock: LockHistoryRecord }>> {
    const conflicts: Array<{ change: OfflineChange; lock: LockHistoryRecord }> =
      [];

    // Get all offline changes for this node
    const offlineChanges = await this.offlineChangesCollection
      .find({ key })
      .toArray();

    for (const change of offlineChanges) {
      // Check if there was a lock on this record during the offline change time
      // by another node
      const conflictingLock = await this.lockHistoryCollection.findOne({
        typ: change.typ,
        value: change.value,
        key: { $ne: change.key }, // Different node had the lock
        acquiredAt: { $lte: change.changeTimestamp },
        releasedAt: { $gte: change.changeTimestamp },
      });

      if (conflictingLock) {
        conflicts.push({
          change,
          lock: conflictingLock,
        });
      }
    }

    return conflicts;
  }

  /**
   * Create conflict records in the sync_conflicts collection
   */
  async createConflictRecords(
    conflicts: Array<{ change: OfflineChange; lock: LockHistoryRecord }>,
  ): Promise<number> {
    if (conflicts.length === 0) {
      return 0;
    }

    const conflictsCollection = this.db.collection('sync_conflicts');

    // Build conflict records with both versions (lock holder + offline change)
    const conflictRecords = await Promise.all(
      conflicts.map(async (conflict) => {
        // Fetch the current document state from the collection (lock holder's version)
        const targetDb = this.db.client.db(conflict.change.database);
        const targetCollection = targetDb.collection(
          conflict.change.collection,
        );
        const currentDoc = await targetCollection.findOne({
          _id: conflict.change.value,
        });

        return {
          conflictId: `offline-${conflict.change._id}`,
          documentId: conflict.change.value,
          collection: conflict.change.collection,
          database: conflict.change.database,
          detectedAt: Date.now(),
          status: 'pending',
          conflictType: 'offline-lock-conflict',
          offlineChange: {
            nodeId: conflict.change.key,
            timestamp: conflict.change.changeTimestamp.getTime(),
            data: conflict.change.changeData,
          },
          lockInfo: {
            lockedBy: conflict.lock.key,
            lockedByName: conflict.lock.name,
            acquiredAt: conflict.lock.acquiredAt.getTime(),
            releasedAt: conflict.lock.releasedAt.getTime(),
          },
          versions: [
            // Version 0: Offline node's local changes (what was changed offline)
            {
              documentId: conflict.change.value,
              data: conflict.change.changeData,
              timestamp: conflict.change.changeTimestamp.getTime(),
              nodeId: conflict.change.key,
              operationType: 'offline-update',
            },
            // Version 1: Lock holder's remote changes (what changed on the server)
            {
              documentId: conflict.change.value,
              data: currentDoc || {},
              timestamp: conflict.lock.releasedAt.getTime(),
              nodeId: conflict.lock.key,
              operationType: 'locked-update',
            },
          ],
        };
      }),
    );

    const result = await conflictsCollection.insertMany(conflictRecords);
    return result.insertedCount;
  }

  /**
   * Clear offline changes for a node after conflict detection
   * @param key - The node key
   */
  async clearOfflineChanges(key: string): Promise<number> {
    const result = await this.offlineChangesCollection.deleteMany({ key });
    return result.deletedCount;
  }

  /**
   * Get all offline changes for a node
   * @param key - The node key
   */
  async getOfflineChanges(key: string): Promise<OfflineChange[]> {
    return await this.offlineChangesCollection.find({ key }).toArray();
  }

  /**
   * Clean old lock history records
   * @param maxAgeMs - Maximum age in milliseconds
   */
  async cleanLockHistory(maxAgeMs: number): Promise<number> {
    const cutoffDate = new Date(Date.now() - maxAgeMs);
    const result = await this.lockHistoryCollection.deleteMany({
      releasedAt: { $lt: cutoffDate },
    });
    return result.deletedCount;
  }
}

/**
 * Create a lock manager instance
 * @param db
 */
export function createLockManager(db: Db): LockManager {
  return new LockManager(db);
}
