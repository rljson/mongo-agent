// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

export { MongoAgent } from './mongo-agent.ts';
export { MongoScanner } from './mongo-scanner.ts';
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
