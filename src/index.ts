// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

export { MongoAgent } from './mongo-agent.ts';

// Export hashing utilities for E2E tests
export { computeIntegrityHash, computeOpHash, sha256Hex } from './hashing/integrity-hash.ts';
export { 
  computeStateCheckpoint, 
  getLatestCheckpoint,
  type StateCheckpoint,
  type MerklePartition 
} from './hashing/state-hash.ts';
export {
  markDirtyById,
  listDirtyForCollection,
  clearDirtyForCollection,
  clearDirtyPartitions
} from './hashing/state-dirty.ts';
