# Changelog

## [0.0.54]

**Declare what the client actually forces.** 0.0.53 declared `fs-agent` 0.0.80
and `rljson` 0.0.83 while the One Client's `pnpm.overrides` install 0.0.81 and
0.0.84 on top of it. So the suite that gated the release ran against a stack
nobody ships — the same trap this repo already has on record from PR #2, where
green tests against `db` 0.0.28 / `server` 0.0.42 proved nothing about what
shipped.

Lifted to the full current set, and the suite re-run on it: `db` 0.0.47, `io`
0.0.80, `rljson` 0.0.84, `server` 0.0.68, `fs-agent` 0.0.81, `bs` 0.0.26,
`hash` 0.0.19, `json` 0.0.23 — every one of them npm-latest, and every rljson
repo's `main` currently equals its published version, so there is no newer fixed
state sitting unpublished anywhere.

`rljson` 0.0.84 adds cake layer/slice-id validation, which is the kind of change
that rejects data that used to pass. 249 tests say it does not reject anything
this package produces.

249 tests, 99.55 / 98.04 / 100 / 99.83.

## [0.0.53]

**Quiet about a route it does not serve.** The other half of what a MIXED fleet
looks like from the new side, and the direct consequence of 0.0.52's companion
change in the One Client: the E2E sandbox route deliberately stops existing
between runs, while older peers keep talking about it.

A peer that syncs a collection this node does not announces its root on every
heartbeat — and while diverged it clears its own dedup, so every one of those
really does arrive. Both drop sites were correct and both said so every time.
Now: once, then every `NOT_SYNCABLE_EVERY`th with the count, which is the only
part that carries information. Same rule `recv ref` and `recv root` already
follow.

### Pulled into pure functions, because branches buried in a method are branches
### nobody tests

`notSyncableLine`, `stuckDecision`, `noProgressDelayMs` and `pairLabel` are now
exported functions beside `headReceiptLine` and `rootReceiptLine`, for the
reason those two are: every path is directly testable. 0.0.52 shipped the bound
with three of its own decision branches uncovered — the cap boundary, the
announce-once suppression, and the back-off growth — which is exactly where a
later change would break it silently.

Branch coverage went UP doing this, 97.68 → 98.04, and the new code has no
uncovered branch of its own.

### One correctness fix inside 0.0.52's own work

The pause key briefly carried 12-character roots, because the log line wanted
short ones. A key and a label want different things: the key decides whether a
pause still applies, and two different roots sharing a prefix must not read as
the same divergence. The key holds the FULL roots; `pairLabel` trims for
display, and names an untagged peer root rather than printing a blank.

249 tests, 99.55 / 98.04 / 100 / 99.83.

## [0.0.52]

**A divergence anti-entropy cannot resolve retried every five seconds for
ever.** That is the mechanism behind a workstation at 3.5 GB and a console
scrolling one collection's name past everything else — and the collection it
happened to be was `e2eProbe`, which nothing was using.

### What was unbounded

The no-progress back-off has been there a while: a round that finds divergence
but moves no documents waits 5 s instead of re-chaining immediately. Flat, and
**with no end**. So a divergence that can be *detected* but never *closed*
retried at that rate for the life of the process. Two ways to get one, both
already documented in this package:

- the phantom-differing bucket — equal content, unequal hash (see
  `mongoCanonical`), the case that produced `ae … want+=0 drop+=0` looping live
  on 2026-09-09;
- a document outside what the codec can round-trip, which the same comment
  records as a known limit.

Each retry broadcasts `AEQ`, and every peer answers with a full `AER` manifest
of `AE_BUCKET_COUNT` × 64 hex ≈ **256 kB**. Every one of those messages is
nonce-stamped, deliberately, so the connector's ref-dedup cannot swallow it.
Nothing in that loop is wrong per round. What was wrong is that it had no end.

### The bound

Consecutive rounds that move nothing while the roots still differ now double
the back-off (capped at 5 min) and, after `SL_EDIT_AE_STUCK_CAP` of them,
**pause** the collection's backfill. One log line says so, naming both roots.

The pause is keyed to the root PAIR that got stuck, not to the collection, so
it lifts itself: any change on either side is new information and the backfill
resumes on its own. A pause needing an operator to clear it would be a worse
bug than the loop.

While paused, the receive side also stops clearing the diverged root from the
received-dedup. That deliberate dedup defeat exists to re-drive a TRANSIENT
divergence on the next heartbeat; on a permanent one it is what put the same
line on the console for ever.

Same shape as the `_headRearmCap` directly above it in the file, for the same
reason: re-arming work that provably cannot progress is not resilience.

### Where the accounting had to go, and why the first attempt did nothing

Bounding the round-completion chain alone changed **nothing measurable** — the
test still counted 33 broadcasts where it expected under 12. The accounting sat
below `_onAeRoundComplete`'s "no peer head recorded" early return, and a
collection whose documents live only in a peer's cold-start baseline has no peer
head. That is *precisely* the collection anti-entropy exists to serve. So the
rounds that could never progress were the ones never counted, and the retries
were arriving from the heartbeat path the whole time.

The peer root therefore cannot be read from `_lastPeerHead`, which is about edit
chains; it is tracked where the divergence is actually measured.

### Measured, not argued

`test/mongo-edit-ae-backfill.spec.ts` asserts on the TRAFFIC, not on a log line:
reads that never succeed, then count the `AEQ` broadcasts. With the cap lifted
the control run reproduces the loop in two seconds — 34 and climbing. With the
bound: a handful, then a flat line across the next window. A second test proves
a paused collection resumes and converges the moment a root changes.

### Also

Dependencies lifted to the set the One Client now runs — `db` 0.0.47, `io`
0.0.80, `rljson` 0.0.83, `server` 0.0.68, `fs-agent` 0.0.80 — so the suite
tests the stack that ships.

## [0.0.51]

**`recv ref` said the same thing thousands of times and buried everything
else.** The other half of the flood 0.0.50 fixed for `recv root`.

A head that applies only partially re-arms by clearing the connector's
received-dedup, so the next announcement delivers it again — and a peer
announces on every heartbeat regardless. The line was written on every receipt.

On 2026-09-28 a lab console was nothing but two `e2eProbe` heads alternating,
for minutes, while the node worked itself up to `rss=7087MB … ab=5542MB` and
died on `Ineffective mark-compacts near heap limit`. The one thing the console
could not be used for was working out why.

So the same rule as the root: a head this node has not traced, or every
twentieth identical repeat, **with the count** — because a head re-delivered
two hundred times without converging is a finding rather than progress, and
only the count says which.

The decision is `headReceiptLine`, a pure function beside `rootReceiptLine`,
so the rule is testable without a mongo, a peer or a socket.

### Considered and withdrawn

A responder-side window on `_onQuery`, suppressing an unchanged bucket manifest
for a couple of seconds. The intent was to bound the anti-entropy amplification
— N peers, N distinct asks (every AE message carries a nonce so the ref-dedup
cannot drop a retry), every node answering every ask, every answer broadcast to
every peer: N² manifests at ~256 KB each.

**It broke chained backfill, and the test caught it.** A large baseline delta
converges by chaining rounds, and each round re-asks the same question and needs
the same unchanged answer; suppressing the repeat stalled the chain after one
capped chunk. The amplification is real and still unbounded — but the fix is not
this one, and shipping it would have traded a noisy console for a sync that
stops halfway.

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
