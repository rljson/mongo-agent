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

// Native RLJSON Cake/Layer/Component mongo sync (review #7, tombstone-free).
export {
  buildCake,
  buildCollectionLayer,
  cakeFromLayers,
  changedCollections,
  componentHashOf,
  componentOf,
  componentToDoc,
  componentsTableFor,
  computeCollectionPlan,
  parseCake,
  parseLayer,
  sliceIdOf,
} from './mongo-cake-model.ts';
export type {
  BuiltCake,
  BuiltLayer,
  CakeBody,
  CakeRow,
  CollectionDocs,
  LayerBody,
} from './mongo-cake-model.ts';
export {
  CAKE_TABLE,
  LAYER_TABLE,
  MongoCakeAdapter,
  componentsTableOf,
} from './mongo-cake-adapter.ts';
export { MongoCakeSync } from './mongo-cake-sync.ts';
export type { MongoStore, MongoCakeSyncOptions, ApplyResult } from './mongo-cake-sync.ts';
export { MongoCakeAgent, MongoDbStore } from './mongo-cake-agent.ts';
export type { MongoCakeAgentOptions } from './mongo-cake-agent.ts';

// Edits-Chain components/edits mongo sync (convergence engine: per-lineage
// incremental walk + per-document timeId ordering, deterministic late-joiner
// catch-up). This is the variant running in production on the CARAT fleet.
export { MongoEditSync } from './mongo-edit-sync.ts';
export type { EditSyncConnector } from './mongo-edit-sync.ts';
export { MongoEditAdapter, compareTimeId } from './mongo-edit-adapter.ts';
export type { CollectPutsResult } from './mongo-edit-adapter.ts';
export { EditCheckpoint } from './mongo-edit-checkpoint.ts';
