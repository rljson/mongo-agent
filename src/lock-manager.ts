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

import { Collection, Db, ObjectId } from 'mongodb';


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
   * Generate the deterministic lock ID from an entity type and value.
   *
   * The ID has the form `"typ-value"` (e.g. `"8-127731"`) and is used as the
   * `_id` of the lock record so that concurrent acquisitions collide on the
   * unique index.
   * @param typ - Numeric entity type of the locked record (see {@link EntityType}).
   * @param value - Identifier of the locked record within its entity type.
   */
  private generateLockId(typ: number, value: string): string {
    return `${typ}-${value}`;
  }

  /**
   * Acquire a lock on a record.
   *
   * Re-entrant for the same key (refreshes the lock's timestamp); returns
   * `true` if the lock was acquired or already held by the same key, and
   * `false` if it is currently held by another node/user.
   * @param options - Lock target and owner metadata (entity type, value, key, name, etc.).
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
   * Release a lock on a record.
   *
   * Only the lock owner (matching key) can release their lock. The released
   * lock is also copied into the lock-history collection so that later offline
   * conflict detection can tell when the record was locked and by whom.
   * @param typ - Numeric entity type of the locked record.
   * @param value - Identifier of the locked record within its entity type.
   * @param key - Owner key that must match the existing lock to release it.
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

    // Save to lock history. The same record can be locked+released repeatedly,
    // so the history `_id` must be UNIQUE per release episode — reusing the
    // lock's own `_id` (the `typ-value` lockId) collided on the second release
    // (E11000). Conflict detection queries by typ/value/key/time, never by _id,
    // so a synthetic unique id is safe.
    const historyRecord: LockHistoryRecord = {
      ...lock,
      _id: `${lock._id}-${new ObjectId().toHexString()}`,
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
   * Check whether a record is currently locked.
   *
   * Returns the active lock record if one exists, or `null` when the record is
   * not locked.
   * @param typ - Numeric entity type of the record to check.
   * @param value - Identifier of the record within its entity type.
   */
  async isLocked(typ: number, value: string): Promise<LockRecord | null> {
    const lockId = this.generateLockId(typ, value);
    return await this.lockCollection.findOne({ _id: lockId });
  }

  /**
   * Check whether a record is locked by one specific key (node/user).
   *
   * Returns `true` only when an active lock exists and is owned by `key`.
   * @param typ - Numeric entity type of the record to check.
   * @param value - Identifier of the record within its entity type.
   * @param key - Owner key to compare against the active lock's holder.
   */
  async isLockedBy(typ: number, value: string, key: string): Promise<boolean> {
    const lock = await this.isLocked(typ, value);
    return lock !== null && lock.key === key;
  }

  /**
   * Get all active locks held by a specific key (node/user).
   * @param key - Owner key whose held locks should be returned.
   */
  async getLocksBy(key: string): Promise<LockRecord[]> {
    return await this.lockCollection.find({ key }).toArray();
  }

  /**
   * Release all locks held by a specific key.
   *
   * Useful for cleanup when a node disconnects. Returns the number of locks
   * removed.
   * @param key - Owner key whose held locks should all be released.
   */
  async releaseAllLocks(key: string): Promise<number> {
    const result = await this.lockCollection.deleteMany({ key });
    return result.deletedCount;
  }

  /**
   * Remove stale locks whose last update is older than the given age.
   *
   * Returns the number of stale locks deleted.
   * @param maxAgeMs - Maximum allowed lock age in milliseconds; older locks are removed.
   */
  async removeOldLocks(maxAgeMs: number): Promise<number> {
    const cutoffDate = new Date(Date.now() - maxAgeMs);

    const result = await this.lockCollection.deleteMany({
      'commonFields.updatedAt': { $lt: cutoffDate },
    });

    return result.deletedCount;
  }

  /**
   * Verify whether a modifying operation is allowed on a record.
   *
   * Returns `true` when no lock exists or when the existing lock is owned by
   * the same key, and `false` when another key holds the lock.
   * @param typ - Numeric entity type of the record to check.
   * @param value - Identifier of the record within its entity type.
   * @param key - Owner key requesting the modification.
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
   * Attempt to acquire a lock, retrying a bounded number of times.
   *
   * Returns `true` as soon as the lock is acquired, or `false` after all
   * attempts have been exhausted.
   * @param options - Lock target and owner metadata passed to {@link acquireLock}.
   * @param maxRetries - Maximum number of acquisition attempts before giving up.
   * @param retryDelayMs - Delay in milliseconds to wait between failed attempts.
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
   * Record an offline change made by a node while disconnected.
   *
   * The stored change is later checked against the lock history when the node
   * comes back online, so conflicts with locks held by other nodes can be
   * detected.
   * @param typ - Numeric entity type of the changed record.
   * @param value - Identifier of the changed record within its entity type.
   * @param key - Node/user key that made the offline change.
   * @param changeData - Payload describing the change that was applied offline.
   * @param collection - Name of the collection the change targets.
   * @param database - Name of the database the change targets.
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
   * Detect conflicts between a node's offline changes and the lock history.
   *
   * Returns the list of offline changes that were made to records which were
   * locked by other nodes during the offline period.
   * @param key - Node/user key whose offline changes should be examined.
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
   * Create conflict records in the `sync_conflicts` collection.
   *
   * For each detected conflict, writes a record holding both the offline
   * node's version and the lock holder's current server version, and returns
   * the number of conflict records inserted.
   * @param conflicts - Detected offline/lock conflicts to persist, as produced by {@link detectOfflineConflicts}.
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
          _id: conflict.change.value as any,
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
 * Create a {@link LockManager} instance bound to the given database.
 * @param db - MongoDB database handle whose locking collections the manager operates on.
 */
export function createLockManager(db: Db): LockManager {
  return new LockManager(db);
}
