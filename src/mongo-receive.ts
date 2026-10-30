// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

// .............................................................................
// Ticket 2 of the fs-agent 0.2 port (see `mongo-edit-chain.ts`): the five-case
// decision a node makes about a head that arrived, ported from fs-agent's
// `fs-receive.ts` near verbatim. The chain (ticket 1) made `relate()` meaningful
// across nodes; this is the policy that acts on what it answers.
//
// This module is deliberately mechanism-free, exactly like its fs-agent
// original: `receive()` only decides, through the injected `MongoReceiveDeps`,
// what should happen. Nothing here touches `MongoEditChain`, a Mongo
// collection, or the sync engine directly — that wiring (the concrete
// `applyBehind`/`adopt`/`merge`, and hooking `receive()` into `MongoEditSync`'s
// head-arrival path) is the next stage, once this decision logic is proven on
// its own.
// .............................................................................

import type { ChainRelation } from './mongo-edit-chain.ts';

/** What {@link receive} needs from the agent, for one collection. */
export interface MongoReceiveDeps {
  /** This node's head for the collection, or `undefined` before it has one. */
  ownHead(): string | undefined;
  /** Authors whatever local work is pending, first. */
  flushLocal(): Promise<void>;
  /** How the own head and an arrived head stand. */
  relate(ours: string, theirs: string): Promise<ChainRelation>;
  /** The snapshot ref a head names, or `undefined` when it cannot be read. */
  snapshotRefOf(head: string): Promise<string | undefined>;
  /**
   * Writes a head's snapshot into the collection, deleting what it lacks, and
   * makes it this node's head. `refused` when it could not — nothing was
   * adopted then.
   */
  applyBehind(head: string): Promise<'applied' | 'refused'>;
  /** Makes `head` — one state under another name — this node's head. */
  adopt(head: string): Promise<void>;
  /** Announces this node's head again. */
  announceOwn(): void;
  /** Merges a forked head into this node's; see the C1–C6 table (ticket 3). */
  merge(
    theirs: string,
    base: string | undefined,
  ): Promise<'merged' | 'deferred' | 'refused'>;
  /** Says that nothing could be decided about `head`. */
  blocked(head: string, reason: string): void;
}

/** What {@link receive} did. */
export type MongoReceiveOutcome =
  | 'no-head'
  | 'in-sync'
  | 'applied'
  | 'refused'
  | 'announced'
  | 'adopted'
  | 'merged'
  | 'deferred'
  | 'blocked';

// .............................................................................
/**
 * What a node does with a head that arrived for one collection.
 *
 * The chain is the only order:
 *
 * | relation | action |
 * | --- | --- |
 * | `behind` — theirs descends from ours | apply it, deleting what it lacks, and adopt it |
 * | `ahead` — ours descends from theirs | announce ours |
 * | `fork`, same content | converge on the smaller ref, on every node |
 * | `fork` | merge against the base |
 * | `incomplete` | nothing; say so |
 *
 * Deleting on `behind` is correct by construction: whoever made the head held
 * this node's state and chose not to have the document. Local work this node
 * has not authored into the chain yet is authored first, so it can never be
 * silently overwritten by a head that never saw it — it turns a `behind` into
 * a `fork` instead.
 * @param head - The head that arrived.
 * @param deps - The agent's side of the decision, for one collection.
 * @returns What was done.
 */
export const receive = async (
  head: string,
  deps: MongoReceiveDeps,
): Promise<MongoReceiveOutcome> => {
  if (deps.ownHead() === undefined) return 'no-head';
  await deps.flushLocal();
  const own = deps.ownHead() as string;
  if (head === own) return 'in-sync';

  const relation = await deps.relate(own, head);
  switch (relation.verdict) {
    case 'same':
      return 'in-sync';
    case 'behind':
      return deps.applyBehind(head);
    case 'ahead':
      deps.announceOwn();
      return 'announced';
    case 'incomplete':
      deps.blocked(head, 'the history between the two heads cannot be read');
      return 'blocked';
  }

  const [ours, theirs] = await Promise.all([
    deps.snapshotRefOf(own),
    deps.snapshotRefOf(head),
  ]);
  if (ours === undefined || theirs === undefined) {
    deps.blocked(head, 'a forked head names a snapshot that cannot be read');
    return 'blocked';
  }
  if (ours === theirs) {
    // One state under two names. Every node keeps the smaller, so they agree
    // without a merge entry and without a round of announcements.
    if (head < own) {
      await deps.adopt(head);
      return 'adopted';
    }
    deps.announceOwn();
    return 'announced';
  }
  return deps.merge(head, relation.base);
};
