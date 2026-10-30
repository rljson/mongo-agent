# CLAUDE.md — @rljson/mongo-agent

MongoDB sync agent for the RLJSON ecosystem — the components/edits engine that keeps a MongoDB in sync across nodes. Depends on `@rljson/db`, `@rljson/io`, `@rljson/server` and `@rljson/bs`.

## The Sync Engine (read before touching sync code)

One engine ships: the **components/edits chain** (`MongoEditSync`,
`MongoEditAdapter`, `EditCheckpoint`, `MongoAntiEntropy`). Documents are
content-addressed Io rows; the head of a collection's `EditHistory` is the
only thing broadcast. Full picture:
[doc/sync-architecture.md](doc/sync-architecture.md).

- **A Mongo route must NOT end in `Tree`.** The consuming app selects the
  engine by that suffix (`!config.treeKey.endsWith('Tree')` in
  `sl-mongo-agent`), so `mongoCaratOneTree` would ask for an engine that no
  longer exists. Name it `mongoCaratOne`.
- The `Cake` **table** is part of this engine. The removed thing was the
  standalone `MongoCakeAgent`/`MongoCakeSync`, not cakes.
- Tree + blob Mongo sync is gone. Do not reintroduce a tree walk for flat
  document data.
- **Anti-entropy rules (ONE-443):**
  - A document both sides hold at different content is **decided** by
    `peerVersionWins` (newer edit `timeId` → edited over loaded → greater
    hash). It must stay symmetric: both nodes reach the same answer from the
    same two versions, or they overwrite each other in turn.
  - An `AER`/`AEE` answer that **agrees** must never end a round or complete
    a bucket — every query reaches every peer, and the agreeing answer is
    usually first.
  - Protocol nonces must be unique **across nodes** (random start), not only
    per node: identical frames from two nodes are dropped as repeats.
  - Always test the anti-entropy with **three** nodes as well as two. Both of
    the last two defects were invisible with two.

<!-- helix:claude_md:start -->

# gg workflow

This repo is developed ticket by ticket with the `gg` CLI. Follow the
development guide, it tells you when to ask the user and which command
comes next:

@doc/guides/for-ai/ai-dev-guide.md

The steps are also available as skills: `/gg-ticket`, `/gg-commit`,
`/gg-push`, `/gg-review`, `/gg-publish`, `/gg-cleanup`. `/gg` lists them
and says which one comes next.

# rljson additions to the gg workflow

- **Pull requests:** CI on a pull request checks that every commit went
  through gg's local checks (`gg one did commit`). The full tests run on
  `main` after the merge: watch them after every publish.

<!-- helix:claude_md:end -->
