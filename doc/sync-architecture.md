<!--
@license
Copyright (c) 2025 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Mongo sync architecture

How this package keeps a MongoDB in sync across nodes. One engine ships:
the **components/edits chain**. Two earlier ones were removed — see
[Removed generations](#removed-generations) at the end, because their names
still appear in older notes and in field configs.

## What selects the engine

The consuming app (`sl-mongo-agent` in `cos-one-client`) picks the sync
engine from the route name:

```ts
const isComponentsRoute = !config.treeKey.endsWith('Tree');
```

A Mongo route must therefore **not** end in `Tree` — `mongoCaratOne`, not
`mongoCaratOneTree`. The `Tree` suffix is reserved for file sync, whose hub
route really does carry a trees table and blob bodies.

## Tables, per synced collection

`MongoEditAdapter` derives every table name from a cake key,
`<prefix><Collection>Cake`, and creates them on demand — the route itself is
a bare relay, nothing is provisioned hub-side.

| Table | Holds |
| --- | --- |
| `<Collection>Comp` | one content-addressed row per Mongo document |
| `<Collection>Slices` | the `_id` set of the collection |
| `<Collection>Layer` | `_id → component hash`; an `_id` absent from the layer is deleted |
| `<Collection>Cake` | the collection's state as a single hash |
| `<Collection>CakeEdits` | individual edits |
| `<Collection>CakeMultiEdits` | edits grouped into one applied step |
| `<Collection>CakeEditHistory` | the ordered chain; its head is what gets broadcast |

Documents are Io rows, never blobs. There is no tree walk: a consumer pulls
the component hashes it is missing in one `readRowsByHashes` batch.

## The flow

**Producer** — a change-stream event writes the changed documents as
component rows, appends an edit, and broadcasts the new `EditHistory` head
ref for that collection. One ref per changed collection, nothing else on the
wire.

**Consumer** — on an incoming head it walks the chain from its own last
applied ref per sender lineage, pulls the missing component rows by hash, and
upserts them into Mongo. Deletes travel as tombstones.

Convergence under concurrent multi-writer load rests on two things: a
per-document `timeId` ordering (`compareTimeId`), and a per-sender lineage of
applied refs rather than one global "last applied" slot. A single slot loses
updates whenever two nodes write between heartbeats.

## Supporting parts

- **`EditCheckpoint`** persists a collection's content-hash manifest plus the
  change-stream resume token, streamed line by line so a manifest of millions
  of entries costs constant memory. Without it every restart re-scans in full.
- **`MongoAntiEntropy`** reconciles what the live chain missed. Manifests are
  bucketed; peers exchange bucket roots (`AEQ`/`AER`), then entries of the
  differing buckets (`AEG`/`AEE`), then the documents themselves
  (`AEW`/`AEH`). Rounds are loss-tolerant: each arriving batch makes progress
  on its own, and still-differing buckets simply re-trigger next round.
- **Tombstone log** (`sl_edit_tombstones`) keeps a delete from being
  resurrected by a backfill. Persistence is best-effort by design — it runs
  inside the delete path and must never abort propagation.
- **`MongoEditSync.health()`** reports what this sync is doing and holds:
  `watching` / `open` (responsible-for vs. actually-streaming — the gap is the
  useful part), `lastChangeAt`, a node-level `stateRef`, and `roots` per
  collection. The `stateRef` is an order-independent XOR fold over each
  collection's content root with the collection name mixed in, so two nodes
  derive the same value without exchanging anything, and a document that moved
  between collections still shows. `null` means *not worked out yet*; 64 zeros
  means *holds nothing* — which two nodes can genuinely agree on.
  It exists because the legacy tree sync's `changeStreamAlive` belongs to a path
  that never runs here, so every node reported a dead change stream while the
  sync was working.
- **Mass-delete circuit breaker** refuses to propagate a delete burst above a
  configured size, so one node's accident does not empty the fleet.

## Removed generations

Both were removed in `drop-tree-and-cake-generations`. Neither has a cloud
path and neither should be reintroduced.

**Tree + blobs.** Each collection was serialised to one components table,
`JSON.stringify`'d into a Bs blob (chunked at ~50k documents to dodge
`cursor.toArray()` OOM and Node's ~512 MB single-string cap), with a
two-level tree of database → collection nodes above it. Copied from the
FsAgent, where a real folder hierarchy justifies a tree; Mongo data is flat,
so the hierarchy was artificial and the node walk was pure overhead — and it
was the walk that the hub read-amplification bug hung on.

**Cake sync.** A `MongoCakeAgent` / `MongoCakeSync` pair, the intended
replacement for the tree, never wired into any consumer. The cake *table*
survives — it is part of the edits engine above — but the standalone cake
sync engine is gone.

An operation-log generation (`sync_ops` / `sync_local` / `sync_state`,
described in [README.architecture.md](../README.architecture.md)) still has
source in the package (`db.ts`, `sync-state-store.ts`, `startup-recovery.ts`,
`lock-manager.ts`) but is reachable from its own specs only — nothing exports
it, so no consumer can call it.
