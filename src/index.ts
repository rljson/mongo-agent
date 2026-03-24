// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

// Main exports
export { MongoAgent } from './mongo-agent.ts';
export type { MongoAgentOptions, TimeoutConfig } from './mongo-agent.ts';

// Scanner exports
export { MongoScanner } from './mongo-scanner.ts';
export type {
  MongoChange,
  MongoChangeCallback,
  MongoChangeType,
  MongoNodeMeta,
  MongoScanOptions,
  MongoTree,
} from './mongo-scanner.ts';

// Adapter exports
export { MongoDbAdapter } from './mongo-db-adapter.ts';
export type { StoreMongoTreeOptions } from './mongo-db-adapter.ts';

export { MongoBlobAdapter } from './mongo-blob-adapter.ts';
export type {
  BlobToDocumentOptions,
  DocumentBlobMeta,
  DocumentToBlobOptions,
} from './mongo-blob-adapter.ts';

// Converter exports
export { MongoToRljsonConverter } from './mongo-to-rljson-converter.ts';

// Example
export { example } from './example.ts';

// Sync exports
export {
  applyRljsonTree,
  extractRljsonTree,
  getRljsonSyncState,
  type ApplyRljsonTreeOptions,
  type ExtractRljsonTreeOptions,
  type RljsonSyncResult,
  type RljsonSyncState,
  type RljsonTreePayload,
} from './sync/rljson-sync.ts';

export {
  syncRljsonTreeFromHub,
  type RljsonSyncFromHubResult,
  type SyncRljsonFromHubOptions,
} from './sync/rljson-hub-sync.ts';

// Export hashing utilities for E2E tests
export {
  computeIntegrityHash,
  computeOpHash,
  sha256Hex,
} from './hashing/integrity-hash.ts';
export {
  clearDirtyForCollection,
  clearDirtyPartitions,
  listDirtyForCollection,
  markDirtyById,
} from './hashing/state-dirty.ts';
export {
  computeStateCheckpoint,
  getLatestCheckpoint,
  type MerklePartition,
  type StateCheckpoint,
} from './hashing/state-hash.ts';
