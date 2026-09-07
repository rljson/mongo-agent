// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

// .............................................................................
// Manifest-diff BACKFILL ("anti-entropy") for the components/edits sync.
//
// WHY THIS EXISTS
// The cake cold-start is manifest-only: it records every doc's content hash but
// does NOT put the baseline bodies into the edit chain (that N-edit chain is the
// super-linear cold-start wall). Live changes converge via head-pull, and two
// nodes with identical data derive the same content root and short-circuit. But
// a node that was DOWN while a peer inserted/updated docs never receives those
// heads, and after its own restart those docs live only in the peer's BASELINE
// (manifest), not in any edit chain — so `collectPuts` can never deliver them.
// The two nodes then sit at different content roots FOREVER with no repair path.
// That is the "a laptop should sync until it is on the same state" gap.
//
// WHAT THIS DOES
// When a node sees a peer's content root differ from its own for a collection
// (and the ordinary head-retry did not close it), it runs a bucketed
// manifest reconciliation:
//   1. AEQ  → ask the peer for its per-bucket manifest roots.
//   2. AER  ← the peer's `bucketCount` roots (XOR of each bucket's entry
//             digests). Compare to our own; the buckets whose root differs are
//             the only ones that can hold a discrepancy.
//   3. AEG  → ask the peer for the raw entries (sliceId → docHash) of exactly
//             those buckets.
//   4. AEE  ← the peer's entries for a bucket. Any sliceId the peer has that we
//             LACK is a doc we are missing (or that we deleted).
//   5. AEW  → ask the peer for the docs we are missing.
//   6. AEH  ← the peer stores those docs as content-addressed components and
//             sends back just their ROW HASHES. We then PULL the bodies by hash
//             over the ordinary flow-controlled `readRowsByHashes` path and
//             upsert them straight into Mongo (manifest-level, chain-free).
//
// WHY PULL, NOT PUSH (this is the scaling property)
// An earlier version broadcast the doc BODIES (AED). That does not scale: the
// hub relays a full copy of the payload to EVERY peer (7 receivers → ~7×,
// 100 → ~100×), and, with the coalesce-bypassing raw emit the protocol needs,
// buffers them unbounded — a 453 MB backfill ballooned the hub to 3.4 GB and
// crashed it. Sending only the hashes (tiny) and letting each peer pull the
// bodies moves the bulk over the request/response read path instead: the hub
// caches ONE copy and serves it to all, flow-controlled, so 200 MB stays 200 MB
// on the wire whether the cluster is 3 nodes or 100. We still deliberately do
// NOT route a baseline backfill through the edit chain — replaying tens of
// thousands of docs as edits rebuilds the super-linear cold-start wall
// (`applyHead … PARTIAL, 0 resolvable`); the components are stored chain-free
// and the change-stream echo (suppressed via the host's applied-hash) folds
// each upserted doc into the content root.
//
// SAFETY
// - ADDITIVE ONLY: we only ever pull docs the peer has and we lack; we never
//   delete a peer's doc from here. Bulk deletes stay the job of the tombstone
//   edit path + its mass-delete circuit breaker.
// - NO RESURRECTION: before asking to replay a missing doc we consult a
//   persistent local tombstone log. A doc we deliberately deleted is NOT pulled
//   back; instead we re-broadcast its tombstone so the delete wins on the peer.
//   This is the exact failure the demo hit ("deleted customer came back"), so it
//   is guarded explicitly.
// .............................................................................

/** Message prefixes. A CARAT collection name never starts with `~`. */
const AEQ = '~AEQ~'; // request peer bucket roots
const AER = '~AER~'; // peer bucket roots
const AEG = '~AEG~'; // request entries of listed buckets
const AEE = '~AEE~'; // entries of one bucket
const AEW = '~AEW~'; // request the docs of listed sliceIds
const AEH = '~AEH~'; // component row hashes a peer can pull the docs by

/** Field separator inside a message body (never appears in a collection name). */
const SEP = '|';

/**
 * Number of manifest buckets. MUST be identical on every node for the per-bucket
 * roots to be comparable, so it is a fixed constant (not scaled to size). 4096
 * roots serialise to ~256 KB — one Socket.IO frame — and keep a mega
 * collection's per-bucket entry payload to `size/4096` rows.
 */
export const AE_BUCKET_COUNT = 4096;

/**
 * The host surface {@link MongoAntiEntropy} drives. Implemented by
 * {@link MongoEditSync}; injected so the protocol is unit-testable against a
 * plain stub with no Mongo, cake, or sockets.
 */
export interface AntiEntropyHost {
  /** Broadcast a protocol ref to peers (same channel as heads/roots). */
  send(ref: string): void;
  /** Our `AE_BUCKET_COUNT` per-bucket roots (each 64 hex) for a collection. */
  bucketRoots(collection: string): string[];
  /**
   * The `[sliceId, docHash]` entries our manifest holds, grouped by bucket, for
   * the requested buckets. Batched (one manifest pass for all buckets) so
   * serving a reconciliation of a mega collection is O(size), not O(size ×
   * buckets).
   */
  bucketEntries(
    collection: string,
    buckets: number[],
  ): Map<number, Array<[string, string]>>;
  /** Our manifest's docHash for a sliceId, or `undefined` if we lack it. */
  manifestHash(collection: string, sliceId: string): string | undefined;
  /** Whether we hold a persistent tombstone for a sliceId (we deleted it). */
  hasTombstone(collection: string, sliceId: string): boolean;
  /** Re-broadcast tombstones for sliceIds we deleted (make delete win). */
  pushTombstones(collection: string, sliceIds: string[]): Promise<void>;
  /**
   * Read these sliceIds from Mongo, publish them as content-addressed components
   * for pull, and return the component row hashes the requester can pull by.
   */
  serveComponents(collection: string, sliceIds: string[]): Promise<string[]>;
  /**
   * Pull these component hashes over the flow-controlled read path and upsert
   * the decoded docs into Mongo (manifest-level, change-stream echo suppressed).
   */
  pullAndApply(collection: string, hashes: string[]): Promise<void>;
  /** Whether a collection is one we sync (drop protocol refs for others). */
  syncs(collection: string): boolean;
  /**
   * Whether a collection's cold-start baseline is complete. Until it is, our
   * bucket roots are partial and would make a peer see spurious differences, so
   * we neither answer AEQ for it nor (the host also gates `trigger`) start a
   * round for it.
   */
  ready(collection: string): boolean;
  /**
   * Called when a reconciliation round for a collection finishes (drained,
   * all-equal, or aborted). Lets the host CHAIN the next round immediately when
   * the collection is still diverged, instead of waiting to RE-RECEIVE the
   * peer's root heartbeat — a live bulk import delivers one capped chunk per
   * round, and if the relay does not re-deliver the (now-changed) root the pull
   * stalls after the first chunk. Chaining self-drives the backfill to
   * completion for exactly the still-diverged collection (no per-heartbeat scan
   * over all collections, which floods the connector).
   * @param collection - The collection whose round just finished.
   */
  onRoundComplete?(collection: string): void;
  /** Diagnostic log (gated by SL_EDIT_TRACE in the host). */
  log(msg: string): void;
}

/** A requester-side reconciliation session for one collection. */
interface Session {
  /**
   * Buckets whose root differed from the peer; awaiting their entries. Drained
   * as entry batches arrive — the round finishes when it empties (or the host's
   * abort timer fires). Docs are requested per-batch (loss-tolerant), so this is
   * only a completion marker, not a staging buffer.
   */
  pending: Set<number>;
}

/**
 * Bucketed manifest reconciliation ("anti-entropy") for one node. One instance
 * per {@link MongoEditSync}; both the requester and the responder halves of the
 * protocol live here and are selected by the incoming message kind.
 */
export class MongoAntiEntropy {
  private readonly _sessions = new Map<string, Session>();
  /** Collections currently mid-round (suppresses re-trigger churn). */
  private readonly _busy = new Set<string>();
  /**
   * Monotonic per-message nonce. The sync connector DEDUPS identical ref
   * strings (both the send side and the receiver's seen-set — the mechanism
   * head refs rely on for idempotency). An anti-entropy message, however, is a
   * REQUEST that must be delivered every time it is sent: a retried
   * `~AEQ~<collection>` is byte-identical to the first and was silently dropped,
   * so a round whose first message was lost could never make progress. Stamping
   * every message with an increasing nonce makes it unique and immune to the
   * dedup. The receiver strips the nonce before parsing (see {@link _body}).
   */
  private _seq = 0;

  constructor(private readonly _host: AntiEntropyHost) {}

  /**
   * Send a nonce-stamped protocol message so the connector's ref-dedup cannot
   * drop a retry.
   * @param prefix - The message-kind prefix (AEQ/AER/AEG/AEE/AEW).
   * @param body - The message body (already `|`-joined).
   */
  private _send(prefix: string, body: string): void {
    this._host.send(`${prefix}${this._seq++}${SEP}${body}`);
  }

  /**
   * Strip the leading nonce field a sender prepended, yielding the body the
   * handlers expect. Only an all-DIGIT first field is treated as a nonce — a
   * collection name (which always leads a body) is never all digits — so this
   * also tolerates a message with no nonce.
   * @param rest - The ref with its kind-prefix already removed.
   * @returns The body without the nonce.
   */
  private _body(rest: string): string {
    const i = rest.indexOf(SEP);
    if (i > 0 && /^\d+$/.test(rest.slice(0, i))) return rest.slice(i + 1);
    return rest;
  }

  /**
   * Whether a ref is an anti-entropy protocol message this class owns.
   * @param ref - The raw ref off the sync channel.
   * @returns True for an AE protocol message, false for a head/root/other ref.
   */
  static owns(ref: string): boolean {
    return (
      ref.startsWith(AEQ) ||
      ref.startsWith(AER) ||
      ref.startsWith(AEG) ||
      ref.startsWith(AEE) ||
      ref.startsWith(AEW) ||
      ref.startsWith(AEH)
    );
  }

  /**
   * Kick off a reconciliation round for a collection whose root we just saw
   * differ from a peer's. A no-op if a round for it is already in flight.
   * @param collection - The collection to reconcile.
   */
  trigger(collection: string): boolean {
    if (this._busy.has(collection)) return false;
    this._busy.add(collection);
    this._sessions.set(collection, { pending: new Set() });
    this._host.log(`ae ${collection} trigger -> request bucket roots`);
    this._send(AEQ, collection);
    return true;
  }

  /**
   * Abandon an in-flight round: its session and busy flag are cleared so a
   * later mismatch can start fresh. Called on a round timeout by the host so a
   * lost message, an empty pull, or a peer whose collection was not yet ready
   * (it does not answer AEQ) cannot wedge a collection's backfill forever.
   * @param collection - The collection whose round to abandon.
   */
  abort(collection: string): void {
    if (this._busy.has(collection)) this._finish(collection);
  }

  /**
   * Handle an incoming protocol message. Returns a promise only for the
   * message kinds that do async Mongo work (AEW replay); the rest are sync.
   * @param ref - The raw protocol ref.
   * @returns A promise that resolves when any async handling is done.
   */
  async onMessage(ref: string): Promise<void> {
    if (ref.startsWith(AEQ)) return this._onQuery(this._body(ref.slice(AEQ.length)));
    if (ref.startsWith(AER)) return this._onRoots(this._body(ref.slice(AER.length)));
    if (ref.startsWith(AEG)) return this._onGet(this._body(ref.slice(AEG.length)));
    if (ref.startsWith(AEE)) return this._onEntries(this._body(ref.slice(AEE.length)));
    if (ref.startsWith(AEW)) return this._onReplay(this._body(ref.slice(AEW.length)));
    if (ref.startsWith(AEH)) return this._onHashes(this._body(ref.slice(AEH.length)));
  }

  // ------ responder half ------

  /**
   * AEQ: a peer asked for our bucket roots. Reply with all of them.
   * @param collection - The collection requested.
   */
  private _onQuery(collection: string): void {
    if (!this._host.syncs(collection) || !this._host.ready(collection)) return;
    const roots = this._host.bucketRoots(collection).join('');
    this._host.log(`ae ${collection} <- AEQ, sending roots`);
    this._send(AER, `${collection}${SEP}${roots}`);
  }

  /**
   * AEG: a peer asked for the entries of specific buckets. Reply per bucket.
   * @param body - The message body `<collection>|<b,b,…>`.
   */
  private async _onGet(body: string): Promise<void> {
    const [collection, list] = this._split2(body);
    if (!this._host.syncs(collection)) return;
    const buckets = list ? list.split(',').map((n) => Number(n)) : [];
    if (buckets.length === 0) return;
    const byBucket = this._host.bucketEntries(collection, buckets);
    // Reply in a FEW size-bounded batches, NOT one frame per bucket. The
    // transport reliably delivers a single sizeable ref (this is exactly how
    // the ~256 KB AER arrives), but a synchronous burst of 128 tiny AEE frames
    // — times several mega collections reconciling at once — is coalesced/
    // dropped by the relay, so the requester received ZERO of them and no round
    // ever completed. Pack many buckets into each message, keep every message
    // under the socket frame limit, and pace them across ticks so none is lost.
    const LIMIT = Number(process.env['SL_EDIT_AE_MSG_BYTES']) || 200_000;
    let batch: Array<[number, Array<[string, string]>]> = [];
    let size = 0;
    let msgs = 0;
    const flush = (): void => {
      if (batch.length === 0) return;
      this._send(AEE, `${collection}${SEP}${JSON.stringify(batch)}`);
      batch = [];
      size = 0;
      msgs++;
    };
    for (const b of buckets) {
      const entries = byBucket.get(b) ?? [];
      const est = entries.length * 80 + 16;
      if (size > 0 && size + est > LIMIT) {
        flush();
        await new Promise((r) => setImmediate(r));
      }
      batch.push([b, entries]);
      size += est;
    }
    flush();
    this._host.log(`ae ${collection} <- AEG ${buckets.length}b -> sent ${msgs} AEE msg`);
  }

  /**
   * AEW: a peer asked for the docs it is missing. Publish them as content-
   * addressed components and reply with just their ROW HASHES (small), which the
   * peer pulls the bodies by. The hashes are sent in count-bounded batches so
   * each control message stays tiny; the bulk moves over the pull path, not here.
   * @param body - The message body `<collection>|<json sliceId array>`.
   */
  private async _onReplay(body: string): Promise<void> {
    const [collection, json] = this._split2(body);
    if (!this._host.syncs(collection)) return;
    const ids = JSON.parse(json) as string[];
    if (ids.length === 0) return;
    const hashes = await this._host.serveComponents(collection, ids);
    if (hashes.length === 0) return;
    this._host.log(
      `ae ${collection} <- AEW ${ids.length} -> offer ${hashes.length} hash(es)`,
    );
    const per = Number(process.env['SL_EDIT_AE_HASHES_PER_MSG']) || 1000;
    for (let i = 0; i < hashes.length; i += per) {
      this._send(
        AEH,
        `${collection}${SEP}${JSON.stringify(hashes.slice(i, i + per))}`,
      );
    }
  }

  /**
   * AEH: a peer offered the component hashes for docs we asked for. Pull the
   * bodies by hash over the flow-controlled read path and upsert them.
   * @param body - The message body `<collection>|<json hash array>`.
   */
  private async _onHashes(body: string): Promise<void> {
    const [collection, json] = this._split2(body);
    if (!this._host.syncs(collection)) return;
    const hashes = JSON.parse(json) as string[];
    if (hashes.length === 0) return;
    await this._host.pullAndApply(collection, hashes);
    this._host.log(`ae ${collection} <- AEH pull ${hashes.length}`);
  }

  // ------ requester half ------

  /**
   * AER: the peer's bucket roots. Compare, then ask for differing buckets.
   * @param body - The message body `<collection>|<joined 64-hex roots>`.
   */
  private _onRoots(body: string): void {
    const [collection, roots] = this._split2(body);
    const session = this._sessions.get(collection);
    if (!session || !this._host.syncs(collection)) return;
    const mine = this._host.bucketRoots(collection);
    const differing: number[] = [];
    for (let b = 0; b < AE_BUCKET_COUNT; b++) {
      const peer = roots.slice(b * 64, b * 64 + 64);
      if (peer.length === 64 && peer !== mine[b]) differing.push(b);
    }
    if (differing.length === 0) {
      this._finish(collection);
      return;
    }
    // Cap the buckets requested per round. A near-total divergence (a fresh
    // catalog/baseline import) makes almost every one of the 4096 buckets
    // differ. Requesting them ALL in one AEG means `_complete` only fires once
    // every bucket's entries have arrived — a multi-megabyte, multi-thousand-
    // frame exchange that the host's round-timeout aborts long before it
    // finishes, so the round never reaches AEW and NOTHING is ever pulled. Ask
    // for a bounded slice instead: the round completes quickly, replays a
    // capped chunk, and the still-differing remainder re-triggers next round,
    // converging incrementally (mirrors the per-round doc cap in `_complete`).
    const cap = Number(process.env['SL_EDIT_AE_MAX_BUCKETS']) || 128;
    const chunk = differing.length > cap ? differing.slice(0, cap) : differing;
    for (const b of chunk) session.pending.add(b);
    this._host.log(
      `ae ${collection} <- AER: ${differing.length} differing bucket(s) -> AEG ${chunk.length}`,
    );
    this._send(AEG, `${collection}${SEP}${chunk.join(',')}`);
  }

  /**
   * AEE: the peer's entries for one bucket. Any sliceId the peer has that we
   * lack is missing here; route it to replay (or to re-delete if tombstoned).
   * @param body - The message body `<collection>|<bucket>|<json entries>`.
   */
  private _onEntries(body: string): void {
    const [collection, json] = this._split2(body);
    const session = this._sessions.get(collection);
    if (!session || !this._host.syncs(collection)) return;
    const batch = JSON.parse(json) as Array<
      [number, Array<[string, string]>]
    >;
    const want: string[] = [];
    const redelete: string[] = [];
    for (const [bucket, entries] of batch) {
      for (const [sliceId, hash] of entries) {
        const ours = this._host.manifestHash(collection, sliceId);
        // Only ADD what we are missing. A sliceId we already hold (even at a
        // different hash — that is a concurrent-edit conflict handled
        // elsewhere) is left alone: backfill never overwrites live content.
        if (ours !== undefined) continue;
        if (hash === '') continue;
        if (this._host.hasTombstone(collection, sliceId)) redelete.push(sliceId);
        else want.push(sliceId);
      }
      session.pending.delete(bucket);
    }
    // LOSS-TOLERANT: act on THIS batch immediately instead of waiting for every
    // requested bucket's entries to arrive. Over a lossy transport a single
    // dropped entry message must not stall the whole round — the still-differing
    // buckets simply re-trigger next round. Each arriving batch makes forward
    // progress on its own.
    this._host.log(
      `ae ${collection} <- AEE ${batch.length}b pending=${session.pending.size} want+=${want.length}`,
    );
    if (redelete.length > 0) {
      void this._host.pushTombstones(collection, redelete);
    }
    if (want.length > 0) {
      const cap = Number(process.env['SL_EDIT_AE_MAX_DOCS']) || 20000;
      for (let i = 0; i < want.length; i += cap) {
        this._send(
          AEW,
          `${collection}${SEP}${JSON.stringify(want.slice(i, i + cap))}`,
        );
      }
    }
    if (session.pending.size === 0) this._finish(collection);
  }

  /**
   * Clears the in-flight session so the next mismatch can re-trigger.
   * @param collection - The collection whose round is done.
   */
  private _finish(collection: string): void {
    this._sessions.delete(collection);
    this._busy.delete(collection);
    // Defer so the host re-triggers on a clean stack (the busy flag is already
    // cleared, so `trigger` can start the next round) rather than re-entering
    // mid-message-handling.
    const host = this._host;
    if (host.onRoundComplete) {
      queueMicrotask(() => host.onRoundComplete!(collection));
    }
  }

  // ------ parsing helpers ------

  /**
   * Split on the FIRST `|` into `[head, rest]` (rest may itself contain `|`).
   * @param body - The message body to split.
   * @returns The `[head, rest]` pair (`rest` is `''` when there is no `|`).
   */
  private _split2(body: string): [string, string] {
    const i = body.indexOf(SEP);
    if (i < 0) return [body, ''];
    return [body.slice(0, i), body.slice(i + 1)];
  }
}
