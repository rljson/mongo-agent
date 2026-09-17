# Changelog

## [0.0.48]

**`collectionStats()` — what each synced collection holds, counted on request.**

`health()` reports the content roots continuously, because they are a
by-product of syncing and cost nothing to read. A document COUNT is not: it is
a database operation, and a sync agent running one on a loop competes with the
application it is syncing for.

So this is on request and only on request. Nothing calls it on a timer, nothing
caches it, and it is not in the change stream's path — the caller asks when
somebody is looking, and the answer carries `measuredAt`, because a count is a
measurement and a stale one has to be able to say so.

A collection that cannot be counted reports `documents: null`, never `0`: a
count that failed and a collection that is empty are opposite findings, and `0`
is the one an operator acts on. One collection the server refuses does not make
the others unreadable.

Each entry also carries the same `root` `health()` reports, so the count and
the hash beside it cannot come from two different reads.

## [0.0.47]

**Sync progress is no longer reported as an error.**

Every line `MongoEditSync` emitted went to `console.error`, so a healthy
node describing itself — `recv root kunden = bbec…`, `checkpoint saved`,
`applyHead SKIP` — arrived in an operator's log panel as ERROR. On the testlab
that was 96 errors on a node with nothing wrong.

The noise was not the cost. **Ten of those call sites report a real failure**
— a tombstone that could not be persisted, a resume that fell back to a full
snapshot, a seed that failed — and they were indistinguishable from the other
thirty-three. A wall of red teaches people to stop reading it, which is
precisely when the one line that mattered goes past.

- Progress goes to `console.log`, still behind `SL_EDIT_TRACE=1`.
- Failures go to `console.error` and are **no longer behind the trace gate**:
  they are rare by construction, and one of them on a node nobody happened to
  start with tracing on is exactly the case worth catching.
- The tag is now `[sl-mongo]`, not `[edit-sync]`. Every operator-facing
  surface — the process list, the config, the route — says `sl-mongo`;
  `edit-sync` is the engine's internal name and appears nowhere a reader has
  seen before.

## [0.0.1]

Initial commit.
