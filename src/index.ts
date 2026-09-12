// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

// The components/edits mongo sync — and, since 0.0.35, the only one.
//
// Three generations lived here at once: a tree+blob sync, an operation log
// with Merkle state checkpoints, and this. Two of them were unreachable from
// the package's single entry point and from every consumer, and they were
// still being read by anyone trying to understand how mongo syncs.
//
// What remains is what runs: a per-lineage incremental walk over the set of
// applied refs, per-document `timeId` ordering (which is what makes concurrent
// multi-writer load converge), a content-root heartbeat, and a mass-delete
// circuit breaker.
export { MongoEditSync } from './mongo-edit-sync.ts';
export type {
  EditSyncConnector,
  MongoEditSyncHealth,
} from './mongo-edit-sync.ts';
export { MongoEditAdapter, compareTimeId } from './mongo-edit-adapter.ts';
export type { CollectPutsResult } from './mongo-edit-adapter.ts';
export { EditCheckpoint } from './mongo-edit-checkpoint.ts';
export type { EditCheckpointState } from './mongo-edit-checkpoint.ts';
