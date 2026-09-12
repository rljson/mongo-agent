<!--
@license
Copyright (c) 2025 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Architecture

**See [doc/sync-architecture.md](doc/sync-architecture.md).** That is the
engine this package is.

## What this page used to be

Four hundred lines describing the **operation-log** subsystem — `sync_ops`,
`sync_local`, `sync_state`, the hub HTTP endpoints, hash chains and Merkle
state checkpoints — as though it were current. It had not been current for a
long time. The page carried a scope note admitting as much and then went on
for another four hundred lines, which is a worse arrangement than either
telling the truth or deleting the page.

As of **0.0.35** that subsystem is gone from the source, along with the
tree+blob generation beside it: `mongo-agent.ts`, `mongo-scanner.ts`,
`mongo-db-adapter.ts`, `mongo-db-tree-adapter.ts`, `mongo-to-rljson-converter.ts`,
`watch-changes.ts`, `db.ts`, `sync-state-store.ts`, `startup-recovery.ts`,
`lock-manager.ts`, `src/hashing/` and `src/scripts/` — about 6 600 lines. None
of it was reachable from the package's single entry point, so no consumer could
import it; it was reachable only from its own specs, and from anyone trying to
work out how a MongoDB actually syncs.

Three generations lived here at once, and a reader had no way to tell which one
ran. That is the cost this removal was paying down.

## What remains

One engine, exported from `src/index.ts`:

| | |
| --- | --- |
| `MongoEditSync` | The sync. A per-lineage incremental walk over applied refs, a content-root heartbeat, live collection discovery, and a mass-delete circuit breaker |
| `MongoEditAdapter` | Documents to and from the edit chain, ordered by per-document `timeId` — which is what makes concurrent multi-writer load converge |
| `EditCheckpoint` | The small mutable per-collection pointer a restart needs: the content-root manifest, the change-stream resume token, and the cake head |

The documents travel **inline inside the edits**. There is no tree here and no
blob store — which is why a mongo route needs neither, on a node or in the
cloud.
