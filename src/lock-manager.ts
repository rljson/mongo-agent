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
  _id: string;           // Format: "typ-value" e.g., "8-127731"
  typ: number;           // Type of the locked entity (e.g., 8 for users)
  value: string;         // ID of the locked record
  key: string;           // User/node key doing the locking
  name: string;          // User/node name
  telefone?: string;     // Optional phone number
  eMail?: string;        // Optional email
  compName: string;      // Computer/node name
  clientName?: string;   // Optional client name
  commonFields: CommonFields;
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

export type EntityTypeValue = typeof EntityType[keyof typeof EntityType];

/**
 * Options for acquiring a lock
 */
export interface LockOptions {
  typ: number;
  value: string;
  key: string;          // Who is locking (node/user ID)
  name: string;         // Display name of locker
  compName: string;     // Computer/node name
  telefone?: string;
  eMail?: string;
  clientName?: string;
}

/**
 * Lock Manager class
 */
export class LockManager {
  private lockCollection: Collection<LockRecord>;
  
  constructor(private db: Db) {
    this.lockCollection = db.collection<LockRecord>('locking');
  }

  /**
   * Initialize the lock collection with indexes
   */
  async initialize(): Promise<void> {
    // Create indexes for efficient queries
    await this.lockCollection.createIndex({ typ: 1, value: 1 }, { unique: false });
    await this.lockCollection.createIndex({ key: 1 }, { unique: false });
    await this.lockCollection.createIndex({ 'commonFields.createdAt': 1 }, { unique: false });
  }

  /**
   * Generate lock ID from type and value
   */
  private generateLockId(typ: number, value: string): string {
    return `${typ}-${value}`;
  }

  /**
   * Acquire a lock on a record
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
              'commonFields.updatedBy': options.key
            }
          }
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
        updatedAt: now
      }
    };

    try {
      await this.lockCollection.insertOne(lockRecord);
      return true;
    } catch (err: any) {
      // Handle race condition where another node acquired lock simultaneously
      if (err.code === 11000) { // Duplicate key error
        return false;
      }
      throw err;
    }
  }

  /**
   * Release a lock on a record
   * Only the lock owner can release their lock
   */
  async releaseLock(typ: number, value: string, key: string): Promise<boolean> {
    const lockId = this.generateLockId(typ, value);
    
    const result = await this.lockCollection.deleteOne({
      _id: lockId,
      key: key  // Ensure only the owner can release
    });

    return result.deletedCount === 1;
  }

  /**
   * Check if a record is locked
   * Returns the lock record if locked, null otherwise
   */
  async isLocked(typ: number, value: string): Promise<LockRecord | null> {
    const lockId = this.generateLockId(typ, value);
    return await this.lockCollection.findOne({ _id: lockId });
  }

  /**
   * Check if a record is locked by a specific key
   */
  async isLockedBy(typ: number, value: string, key: string): Promise<boolean> {
    const lock = await this.isLocked(typ, value);
    return lock !== null && lock.key === key;
  }

  /**
   * Get all locks held by a specific key (node/user)
   */
  async getLocksBy(key: string): Promise<LockRecord[]> {
    return await this.lockCollection.find({ key }).toArray();
  }

  /**
   * Release all locks held by a specific key
   * Useful for cleanup when a node disconnects
   */
  async releaseAllLocks(key: string): Promise<number> {
    const result = await this.lockCollection.deleteMany({ key });
    return result.deletedCount;
  }

  /**
   * Remove stale locks older than the specified age in milliseconds
   */
  async removeOldLocks(maxAgeMs: number): Promise<number> {
    const cutoffDate = new Date(Date.now() - maxAgeMs);
    
    const result = await this.lockCollection.deleteMany({
      'commonFields.updatedAt': { $lt: cutoffDate }
    });

    return result.deletedCount;
  }

  /**
   * Verify if an operation is allowed on a record
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
   */
  async acquireLockWithRetry(
    options: LockOptions,
    maxRetries = 3,
    retryDelayMs = 100
  ): Promise<boolean> {
    for (let i = 0; i < maxRetries; i++) {
      const acquired = await this.acquireLock(options);
      if (acquired) {
        return true;
      }

      if (i < maxRetries - 1) {
        await new Promise(resolve => setTimeout(resolve, retryDelayMs));
      }
    }

    return false;
  }
}

/**
 * Create a lock manager instance
 */
export function createLockManager(db: Db): LockManager {
  return new LockManager(db);
}
