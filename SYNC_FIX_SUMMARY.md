# Bidirectional Sync Fix — Summary

**Branch**: `migrating-sources`
**Final commit**: `926ac6c`
**Verified**: 2026-04-23, two laptops (NB-2666 / NB-2750) over Wi-Fi via hub-relay, converged in ~2–3 s with no echo loop.

---

## Symptom

Inserts on one laptop never appeared on the other, even though the hub-relay HTTP path was healthy (`/hub/clients` showed both agents, `/sync/pull` returned 200).

---

## Root Cause

Five chained bugs in the **change-stream → ops-log → peer-apply** pipeline. Each one masked the next, so fixing the first only revealed the second, etc.

### Bug 1 — Change-stream events never written to raw `sync_ops` collection
The watcher appended ops only to the rljson `sync_ops` ComponentsTable wrapper. But `/sync/pull` reads from the **raw** `sync_ops` collection. Result: peers always got an empty op list.

### Bug 2 — `/sync/pull` returned BSON shapes as plain JSON
`ObjectId` and `Date` were lost in transit, so the consumer crashed/silently dropped ops when trying to deserialize.

### Bug 3 — `fullDocument` only stored as a `BsMem` blob ID, not inlined
`BsMem` is per-process. The peer had no way to resolve `fullDocumentBlobId` because the blob existed only in the producer's memory. Applied ops therefore had no payload.

### Bug 4 — rljson `ComponentsTable` silently dropped ops with non-primitive fields
Nested objects and `Date` instances inside `fullDocument` made `ComponentsTable` reject the row ("Unsupported type: object"). The op vanished. Then `applyOneOp` later tried `replaceOne({_id: undefined}, …)` and corrupted the destination collection.

### Bug 5 — Echo loop + race condition in suppressor
Ops applied from a peer were re-captured by the local change stream and bounced back. The `Suppressor` (which marks `(ns, _id)` as "just applied — ignore the next change-stream event") was:
- (a) **never wired** into the hub-pull path (`syncOriginFromHub` didn't receive it), and
- (b) even when wired, called *after* `await replaceOne(…)` — the change-stream callback fired first and won the race.

---

## Fixes

| # | Commit  | File(s)                              | Change |
|---|---------|--------------------------------------|--------|
| 1 | `0c1ce0b` | `src/watch-changes.ts`               | Mirror-write every captured op into the raw `sync_ops` collection. Tolerate missing blob on restart (`bs.getBlob(id).catch(() => null)`). |
| 2 | `9ceeb88` | `src/agent-server.ts`, `src/sync/pull-from-hub.ts` | `/sync/pull` uses `EJSON.serialize`. Consumer strips `clusterTime` / `changeStreamId` and uses `EJSON.deserialize` to restore BSON types. |
| 3 | `55252b6` | `src/watch-changes.ts`               | Inline `fullDocument` and `updateDescription` into `op.payload` (no more cross-process blob lookups). |
| 4 | `0e88515` | `src/watch-changes.ts`, `src/sync/pull-from-hub.ts` | Normalize payloads via `JSON.parse(JSON.stringify(fullDoc))` before insert so rljson accepts them. Add missing-payload / missing-`_id` guards in `applyOneOp` (returns `{applied: false, reason: 'missing-payload'}` instead of corrupting the collection). |
| 5 | `926ac6c` | `src/agent-server.ts`, `src/sync/pull-from-hub.ts` | Hoist a single shared `Suppressor` instance to module scope. Pass it to **both** `startDbChangeStream` and `syncOriginFromHub` (was missing on the hub-pull path). Call `suppressor.add(ns, _id)` **before** the mongo write to close the change-stream race. |

### Bonus fix
`DB_NAME` was drifting between laptops (`CARATDB` vs `testdb`). Both `.env` files now pinned to `CARATDB`. The hub does not validate that peers agree on a database name — silent data divergence if they don't.

---

## Architectural Lessons (recorded in repo memory)

- **`BsMem` is per-process.** Don't ship blob IDs across the wire — peers can't resolve them. Inline the payload.
- **rljson `ComponentsTable` rejects non-primitives silently.** Always serialize through `JSON.parse(JSON.stringify(...))` (or equivalent) before inserting Mongo documents into it.
- **Change-stream callbacks race with `await`.** Anything you want suppressed must be added to the suppressor *before* the mongo write that triggers the event, not after.
- **Hub does not validate DB-name agreement** between peers — easy footgun.
- **Per-origin `lastSeqSeen` rewinds aren't auto-detected.** If a peer rebuilds its `sync_ops`, downstream peers won't catch up without manual reset.
- **EJSON everywhere** for any change-stream / BSON payload that crosses an HTTP boundary.

---

## End-to-End Flow (post-fix)

```
Laptop A insert
  └─► A.mongo change stream
        └─► watcher: build SyncOp, inline fullDocument (plain JSON),
             append to rljson sync_ops AND raw sync_ops collection
              └─► A.agent /sync/pull (EJSON.serialize)
                   └─► hub /hub/relay/A/sync/pull → B.agent
                        └─► B.consumer:
                             strip clusterTime/changeStreamId,
                             EJSON.deserialize,
                             suppressor.add(ns, _id),    ← BEFORE write
                             replaceOne / deleteOne / insertOne
                              └─► B.mongo change stream fires
                                   └─► watcher sees suppressed (ns, _id), skips
                                        └─► no echo back to A ✅
```

Result: bidirectional convergence in ~2–3 s, `sync_state.lastSeqSeen` advances cleanly, no echo loop.
