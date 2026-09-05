// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { MongoClient, type Db } from 'mongodb';

let client: MongoClient | null = null;
let db: Db | null = null;

/**
 * Connects to MongoDB using the provided URI
 * @param uri - MongoDB connection URI
 * @returns Database instance
 */
export async function connect(uri: string): Promise<Db> {
  if (!uri) throw new Error('MONGO_URI not set');

  client = new MongoClient(uri, {
    // Keep it boring & stable
    maxPoolSize: 20,
    minPoolSize: 0,
  });

  await client.connect();
  db = client.db(); // db from URI
  return db;
}

/**
 * Gets the current database instance.
 * Throws error if not connected.
 * @returns Database instance
 */
export function getDb(): Db {
  if (!db) throw new Error('DB not connected');
  return db;
}

/**
 * Closes the database connection
 */
export async function close(): Promise<void> {
  try {
    await client?.close();
  } catch {
    // Swallow errors - we're closing anyway
  } finally {
    client = null;
    db = null;
  }
}
