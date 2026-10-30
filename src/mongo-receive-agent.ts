// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

// .............................................................................
// Ticket 4 of the fs-agent 0.2 port (see `mongo-edit-chain.ts`, `mongo-receive.ts`,
// `mongo-apply-behind.ts`): the concrete `MongoReceiveDeps` a real node needs —
// an own head, a running content-root accumulator for local changes, and the
// five-case `receive()` loop wired against a real `MongoEditChain` and a real
// Mongo collection.
//
// STILL STANDALONE. This is not `MongoEditSync` with its change-stream swapped
// out — it is the piece that CAN be wired into it, once a decision is made
// about how the old model (anti-entropy, manifest buckets, tombstone log) and
// this one coexist in the same running class, or whether the old model is
// retired outright first. That decision, and the actual change-stream
// wiring, is its own next stage.
//
// WHY A LOCAL sliceId -> docHash MANIFEST IS KEPT HERE TOO
// `MongoEditChain.append` takes the resulting `snapshotRef` as given — it
// does not derive it from a diff, because it has no notion of "the previous
// content" beyond what its own `changed`/`removed` say. Computing the next
// root incrementally (XOR out a changed sliceId's OLD digest, XOR in its
// new one) needs that old digest, so this agent keeps the same
// `sliceId -> docHash` manifest the old model keeps in `mongo-edit-sync.ts`,
// built on the same shared primitives (`mongo-manifest-hash.ts`) rather than
// a copy of the old model's own bookkeeping.
// .............................................................................

import type { Document } from 'mongodb';

import {
  applyBehind as applyBehindToTarget,
  type ApplyBehindTarget,
} from './mongo-apply-behind.ts';
import { docHash } from './mongo-component-codec.ts';
import type { MongoChainEntry, MongoEditChain } from './mongo-edit-chain.ts';
import { entryDigest } from './mongo-manifest-hash.ts';
import {
  receive,
  type MongoReceiveDeps,
  type MongoReceiveOutcome,
} from './mongo-receive.ts';

/** What {@link MongoReceiveAgent} needs to run one collection. */
export interface MongoReceiveAgentOptions {
  /** The collection's chain (ticket 1). */
  chain: MongoEditChain;
  /** The collection name — the chain's own key for it. */
  collection: string;
  /** The real Mongo collection to write applied content into. */
  target: ApplyBehindTarget;
  /** Announces this node's own head again (e.g. over the existing connector). */
  onAnnounce: (head: string) => void;
  /** Called when `receive()` could decide nothing about an arrived head. */
  onBlocked?: (head: string, reason: string) => void;
  /** Give up a chain walk past this many entries; see `MongoEditChain.path`. */
  maxWalk?: number;
}

/**
 * XORs `digest` into `acc` in place. Self-inverse: folding the same digest
 * in twice cancels it, which is how a changed/removed sliceId's old entry is
 * un-folded.
 * @param acc - The 32-byte accumulator, modified in place.
 * @param digest - The 32-byte entry digest to fold in.
 */
const fold = (acc: Buffer, digest: Buffer): void => {
  for (let i = 0; i < digest.length; i++) acc[i] ^= digest[i];
};

/**
 * One collection's node state for the fs-agent 0.2 model: an own head, the
 * content-root accumulator that head names, and the `receive()` loop that
 * keeps it honest against a peer's.
 */
export class MongoReceiveAgent {
  private readonly _chain: MongoEditChain;
  private readonly _collection: string;
  private readonly _target: ApplyBehindTarget;
  private readonly _onAnnounce: (head: string) => void;
  private readonly _onBlocked: (head: string, reason: string) => void;
  private readonly _maxWalk: number | undefined;

  private _ownHead: string | undefined;
  private _acc = Buffer.alloc(32);
  private readonly _manifest = new Map<string, string>();
  private readonly _pendingChanged = new Map<string, Document>();
  private readonly _pendingRemoved = new Set<string>();

  constructor(opts: MongoReceiveAgentOptions) {
    this._chain = opts.chain;
    this._collection = opts.collection;
    this._target = opts.target;
    this._onAnnounce = opts.onAnnounce;
    this._onBlocked = opts.onBlocked ?? (() => {});
    this._maxWalk = opts.maxWalk;
  }

  /** This node's head for the collection, or `undefined` before {@link bootstrap}. */
  ownHead(): string | undefined {
    return this._ownHead;
  }

  /**
   * Names the state this node already holds when it starts — a root entry,
   * content-derived so every node starting from the same baseline writes the
   * identical one. Cold-start reconciliation beyond this (a node that already
   * has a DIFFERENT baseline than its peers) is `planJoin`, a later ticket.
   * @param initial - sliceId → document, for every document already in the
   *   collection. Empty (the default) for a genuinely empty collection.
   * @returns The root head.
   */
  async bootstrap(initial?: ReadonlyMap<string, Document>): Promise<string> {
    for (const [sliceId, doc] of initial ?? []) {
      const hash = docHash(doc);
      this._manifest.set(sliceId, hash);
      fold(this._acc, entryDigest(sliceId, hash));
    }
    const root = await this._chain.append(this._collection, {
      snapshotRef: this._acc.toString('hex'),
      previous: [],
    });
    this._ownHead = root.head;
    return root.head;
  }

  /**
   * Queues a local change, and folds it into the running content-root
   * immediately — the root a concurrent `receive()` sees is always current,
   * whether or not {@link flushLocal} has run yet.
   * @param sliceId - The stringified `_id`.
   * @param doc - The document's new content, or `undefined` for a deletion.
   */
  recordChange(sliceId: string, doc: Document | undefined): void {
    const previousHash = this._manifest.get(sliceId);
    if (previousHash !== undefined) fold(this._acc, entryDigest(sliceId, previousHash));

    if (doc === undefined) {
      this._manifest.delete(sliceId);
      this._pendingRemoved.add(sliceId);
      this._pendingChanged.delete(sliceId);
      return;
    }
    const hash = docHash(doc);
    this._manifest.set(sliceId, hash);
    fold(this._acc, entryDigest(sliceId, hash));
    this._pendingChanged.set(sliceId, doc);
    this._pendingRemoved.delete(sliceId);
  }

  /**
   * Authors every {@link recordChange} queued since the last flush as one
   * chain entry. A no-op when nothing is pending.
   */
  async flushLocal(): Promise<void> {
    if (this._pendingChanged.size === 0 && this._pendingRemoved.size === 0) return;
    const entry = await this._chain.append(this._collection, {
      snapshotRef: this._acc.toString('hex'),
      previous: this._ownHead !== undefined ? [this._ownHead] : [],
      changed: new Map(this._pendingChanged),
      removed: [...this._pendingRemoved],
    });
    this._ownHead = entry.head;
    this._pendingChanged.clear();
    this._pendingRemoved.clear();
  }

  /**
   * Replays every entry a successful {@link applyBehindToTarget} just wrote
   * into the manifest this node keeps for its OWN future local edits — a
   * sliceId `applyBehind` just set must fold out to its new digest, not the
   * one `recordChange` would otherwise assume.
   * @param steps - The entries `applyBehindToTarget` replayed, oldest first.
   */
  private async _absorb(steps: readonly MongoChainEntry[]): Promise<void> {
    for (const entry of steps) {
      for (const [sliceId, hash] of Object.entries(entry.changed)) {
        const doc = await this._chain.getComponent(this._collection, hash);
        /* v8 ignore next -- @preserve applyBehindToTarget already proved every changed hash in this path resolves */
        if (doc) this._manifest.set(sliceId, docHash(doc));
      }
      for (const sliceId of entry.removed) this._manifest.delete(sliceId);
    }
    const last = steps[steps.length - 1];
    /* v8 ignore next -- @preserve a 'behind' relation always yields at least one step */
    if (last) this._acc = Buffer.from(last.snapshotRef, 'hex');
  }

  /**
   * This collection's side of `receive()` — see `mongo-receive.ts`.
   * @returns The deps `receive()` needs.
   */
  private _deps(): MongoReceiveDeps {
    return {
      ownHead: () => this._ownHead,
      flushLocal: () => this.flushLocal(),
      relate: (ours, theirs) =>
        this._chain.relate(this._collection, ours, theirs, this._maxWalk),
      snapshotRefOf: async (head) =>
        (await this._chain.entry(this._collection, head))?.snapshotRef,
      applyBehind: async (head) => {
        const ours = this._ownHead as string;
        const outcome = await applyBehindToTarget(
          this._chain,
          this._collection,
          this._target,
          ours,
          head,
          this._maxWalk,
        );
        if (outcome !== 'applied') return outcome;
        const steps = await this._chain.path(
          this._collection,
          ours,
          head,
          this._maxWalk,
        );
        /* v8 ignore next -- @preserve applyBehindToTarget already proved this path resolves */
        if (steps) await this._absorb(steps);
        this._ownHead = head;
        return outcome;
      },
      adopt: async (head) => {
        this._ownHead = head;
      },
      announceOwn: () => this._onAnnounce(this._ownHead as string),
      // `merge3` (the C1–C6 conflict table) is a later ticket; a genuine fork
      // is deferred, never guessed at, until it exists.
      merge: async () => 'deferred',
      blocked: (head, reason) => this._onBlocked(head, reason),
    };
  }

  /**
   * Receives a head that arrived for this collection.
   * @param head - The head.
   * @returns What was done; see `mongo-receive.ts`.
   */
  async receiveHead(head: string): Promise<MongoReceiveOutcome> {
    return receive(head, this._deps());
  }
}
