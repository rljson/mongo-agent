// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

export { MongoAgent } from './mongo-agent.ts';
export type { MongoAgentOptions } from './mongo-agent.ts';
export { MongoScanner } from './mongo-scanner.ts';
export type {
  MongoChange,
  MongoChangeCallback,
  MongoChangeType,
  MongoNodeMeta,
  MongoScanOptions,
  TreeRef,
} from './mongo-scanner.ts';
export { MongoBlobAdapter } from './mongo-blob-adapter.ts';
export { applyRljsonTree, extractRljsonTree } from './sync/rljson-sync.ts';
export type { RljsonTreePayload } from './sync/rljson-sync.ts';

// Edits-Chain components/edits mongo sync — the production sync engine for the
// CARAT fleet: per-lineage incremental walk over the set of applied refs +
// per-document timeId ordering (guaranteed convergence under concurrent
// multi-writer load), content-root heartbeat, mass-delete circuit breaker.
export { MongoEditSync } from './mongo-edit-sync.ts';
export type { EditSyncConnector } from './mongo-edit-sync.ts';
export { MongoEditAdapter, compareTimeId } from './mongo-edit-adapter.ts';
export type { CollectPutsResult } from './mongo-edit-adapter.ts';
export { EditCheckpoint } from './mongo-edit-checkpoint.ts';

// Adapters and helpers the consuming app builds on. They were part of the
// package's source from the first port but not of its public surface, so
// cos-one-client could not drop its own copy of them.
export { MongoDbAdapter } from './mongo-db-adapter.ts';
export { MongoDbTreeAdapter } from './mongo-db-tree-adapter.ts';
export { MongoToRljsonConverter } from './mongo-to-rljson-converter.ts';
export type { MongoTree, Tree } from './mongo-scanner.ts';
export {
  computeIntegrityHash,
  computeOpHash,
  sha256Hex,
} from './hashing/integrity-hash.ts';
export {
  computeStateCheckpoint,
  docLeafHash,
  getLatestCheckpoint,
} from './hashing/state-hash.ts';
export type {
  ComputeStateCheckpointOptions,
  DocWithHash,
  MerklePartition,
  StateCheckpoint,
} from './hashing/state-hash.ts';
