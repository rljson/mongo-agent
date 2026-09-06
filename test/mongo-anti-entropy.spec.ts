// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { beforeEach, describe, expect, it } from 'vitest';

import {
  AE_BUCKET_COUNT,
  AntiEntropyHost,
  MongoAntiEntropy,
} from '../src/mongo-anti-entropy';
import { bucketOf } from '../src/mongo-manifest-hash';

// A fully in-memory AntiEntropyHost: it records every ref the protocol sends and
// lets each test drive the responder/requester halves deterministically with no
// Mongo, cake, or sockets.
class FakeHost implements AntiEntropyHost {
  sent: string[] = [];
  roots: string[] = new Array(AE_BUCKET_COUNT).fill('0'.repeat(64));
  manifest = new Map<string, string>();
  tombstones = new Set<string>();
  entriesByBucket = new Map<number, Array<[string, string]>>();
  served: Array<{ collection: string; ids: string[] }> = [];
  pulled: Array<{ collection: string; hashes: string[] }> = [];
  pushedTombstones: Array<{ collection: string; ids: string[] }> = [];
  serveResult: string[] = [];
  syncable = new Set<string>(['customers']);
  readyCollections = new Set<string>(['customers']);
  roundsCompleted: string[] = [];
  logs: string[] = [];

  send(ref: string): void {
    this.sent.push(ref);
  }
  bucketRoots(): string[] {
    return this.roots;
  }
  bucketEntries(
    _collection: string,
    buckets: number[],
  ): Map<number, Array<[string, string]>> {
    const out = new Map<number, Array<[string, string]>>();
    for (const b of buckets) out.set(b, this.entriesByBucket.get(b) ?? []);
    return out;
  }
  manifestHash(_collection: string, sliceId: string): string | undefined {
    return this.manifest.get(sliceId);
  }
  hasTombstone(_collection: string, sliceId: string): boolean {
    return this.tombstones.has(sliceId);
  }
  async pushTombstones(collection: string, ids: string[]): Promise<void> {
    this.pushedTombstones.push({ collection, ids });
  }
  async serveComponents(collection: string, ids: string[]): Promise<string[]> {
    this.served.push({ collection, ids });
    return this.serveResult;
  }
  async pullAndApply(collection: string, hashes: string[]): Promise<void> {
    this.pulled.push({ collection, hashes });
  }
  syncs(collection: string): boolean {
    return this.syncable.has(collection);
  }
  ready(collection: string): boolean {
    return this.readyCollections.has(collection);
  }
  onRoundComplete(collection: string): void {
    this.roundsCompleted.push(collection);
  }
  log(msg: string): void {
    this.logs.push(msg);
  }

  // Test helper: the payload of the last sent ref whose kind-prefix matches.
  lastBodyOf(prefix: string): string | undefined {
    for (let i = this.sent.length - 1; i >= 0; i--) {
      const r = this.sent[i];
      if (r.startsWith(prefix)) {
        // strip `<prefix><nonce>|`
        const rest = r.slice(prefix.length);
        const bar = rest.indexOf('|');
        return rest.slice(bar + 1);
      }
    }
    return undefined;
  }
  countOf(prefix: string): number {
    return this.sent.filter((r) => r.startsWith(prefix)).length;
  }
}

const AEQ = '~AEQ~';
const AER = '~AER~';
const AEG = '~AEG~';
const AEE = '~AEE~';
const AEW = '~AEW~';
const AEH = '~AEH~';
const COLL = 'customers';

// Build a stamped incoming ref exactly as a peer would (prefix + nonce + body).
const msg = (prefix: string, body: string): string => `${prefix}9|${body}`;

describe('MongoAntiEntropy', () => {
  let host: FakeHost;
  let ae: MongoAntiEntropy;

  beforeEach(() => {
    host = new FakeHost();
    ae = new MongoAntiEntropy(host);
    delete process.env['SL_EDIT_AE_MAX_BUCKETS'];
    delete process.env['SL_EDIT_AE_MAX_DOCS'];
    delete process.env['SL_EDIT_AE_MSG_BYTES'];
    delete process.env['SL_EDIT_AE_HASHES_PER_MSG'];
  });

  describe('static owns()', () => {
    it('recognises every AE kind and nothing else', () => {
      for (const p of [AEQ, AER, AEG, AEE, AEW, AEH]) {
        expect(MongoAntiEntropy.owns(`${p}0|${COLL}`)).toBe(true);
      }
      expect(MongoAntiEntropy.owns(`${COLL}|deadbeef`)).toBe(false);
      expect(MongoAntiEntropy.owns('~R~something')).toBe(false);
    });
  });

  describe('trigger / abort', () => {
    it('sends a nonce-stamped AEQ and no-ops while a round is in flight', () => {
      expect(ae.trigger(COLL)).toBe(true);
      expect(host.countOf(AEQ)).toBe(1);
      // A second trigger while busy is a no-op (returns false, sends nothing).
      expect(ae.trigger(COLL)).toBe(false);
      expect(host.countOf(AEQ)).toBe(1);
      // After abort the collection can start a fresh round.
      ae.abort(COLL);
      expect(ae.trigger(COLL)).toBe(true);
      expect(host.countOf(AEQ)).toBe(2);
    });

    it('abort on a collection with no in-flight round is a no-op', () => {
      expect(() => ae.abort('nope')).not.toThrow();
    });

    it('stamps each message with a distinct nonce so ref-dedup cannot drop it', () => {
      ae.trigger(COLL);
      ae.abort(COLL);
      ae.trigger(COLL);
      const aeqs = host.sent.filter((r) => r.startsWith(AEQ));
      expect(aeqs[0]).not.toBe(aeqs[1]);
    });
  });

  describe('responder half', () => {
    it('AEQ -> replies AER with all bucket roots when synced + ready', async () => {
      await ae.onMessage(msg(AEQ, COLL));
      expect(host.countOf(AER)).toBe(1);
      const body = host.lastBodyOf(AER)!;
      const [coll, roots] = [body.slice(0, body.indexOf('|')), body.slice(body.indexOf('|') + 1)];
      expect(coll).toBe(COLL);
      expect(roots.length).toBe(AE_BUCKET_COUNT * 64);
    });

    it('AEQ -> silent when the collection is not synced or not ready', async () => {
      await ae.onMessage(msg(AEQ, 'unsynced'));
      host.readyCollections.delete(COLL);
      await ae.onMessage(msg(AEQ, COLL));
      expect(host.countOf(AER)).toBe(0);
    });

    it('AEG -> replies AEE batches for the requested buckets', async () => {
      host.entriesByBucket.set(1, [['a', 'h1']]);
      host.entriesByBucket.set(2, [['b', 'h2']]);
      await ae.onMessage(msg(AEG, `${COLL}|1,2`));
      expect(host.countOf(AEE)).toBeGreaterThanOrEqual(1);
      const body = host.lastBodyOf(AEE)!;
      expect(body.startsWith(`${COLL}|`)).toBe(true);
    });

    it('AEG -> splits into multiple AEE messages past the byte budget', async () => {
      process.env['SL_EDIT_AE_MSG_BYTES'] = '200';
      const big = (n: number): Array<[string, string]> =>
        Array.from({ length: 20 }, (_, i) => [`k${n}-${i}`, 'h'.repeat(8)]);
      host.entriesByBucket.set(1, big(1));
      host.entriesByBucket.set(2, big(2));
      host.entriesByBucket.set(3, big(3));
      await ae.onMessage(msg(AEG, `${COLL}|1,2,3`));
      expect(host.countOf(AEE)).toBeGreaterThan(1);
    });

    it('AEG -> silent for empty bucket list or unsynced collection', async () => {
      await ae.onMessage(msg(AEG, `${COLL}|`));
      await ae.onMessage(msg(AEG, `unsynced|1,2`));
      expect(host.countOf(AEE)).toBe(0);
    });

    it('AEW -> serves components and offers their hashes in capped AEH batches', async () => {
      process.env['SL_EDIT_AE_HASHES_PER_MSG'] = '2';
      host.serveResult = ['x1', 'x2', 'x3'];
      await ae.onMessage(msg(AEW, `${COLL}|${JSON.stringify(['a', 'b'])}`));
      expect(host.served).toHaveLength(1);
      // 3 hashes / 2 per msg = 2 AEH messages.
      expect(host.countOf(AEH)).toBe(2);
    });

    it('AEW -> silent for empty ids, no served hashes, or unsynced', async () => {
      await ae.onMessage(msg(AEW, `${COLL}|[]`));
      host.serveResult = [];
      await ae.onMessage(msg(AEW, `${COLL}|${JSON.stringify(['a'])}`));
      await ae.onMessage(msg(AEW, `unsynced|${JSON.stringify(['a'])}`));
      expect(host.countOf(AEH)).toBe(0);
    });

    it('AEH -> pulls the offered component bodies by hash', async () => {
      await ae.onMessage(msg(AEH, `${COLL}|${JSON.stringify(['x1', 'x2'])}`));
      expect(host.pulled).toEqual([{ collection: COLL, hashes: ['x1', 'x2'] }]);
    });

    it('AEH -> silent for empty hash list or unsynced collection', async () => {
      await ae.onMessage(msg(AEH, `${COLL}|[]`));
      await ae.onMessage(msg(AEH, `unsynced|${JSON.stringify(['x'])}`));
      expect(host.pulled).toHaveLength(0);
    });
  });

  describe('requester half', () => {
    // Drives a full round from the requester side: trigger (AEQ), then feed the
    // peer's AER / AEE back in and assert the AEG / AEW it produces.
    const differAt = (bucket: number): void => {
      host.roots[bucket] = 'f'.repeat(64); // our root for this bucket
    };

    it('AER all-equal -> finishes the round without asking for entries', async () => {
      ae.trigger(COLL);
      const roots = host.roots.join('');
      await ae.onMessage(msg(AER, `${COLL}|${roots}`));
      expect(host.countOf(AEG)).toBe(0);
      // Round finished: a fresh trigger is accepted.
      expect(ae.trigger(COLL)).toBe(true);
    });

    it('AER with differing buckets -> asks for exactly those buckets via AEG', async () => {
      ae.trigger(COLL);
      // Peer roots: all zero except bucket 5 differs from ours.
      differAt(5);
      const peerRoots = new Array(AE_BUCKET_COUNT).fill('0'.repeat(64)).join('');
      await ae.onMessage(msg(AER, `${COLL}|${peerRoots}`));
      expect(host.countOf(AEG)).toBe(1);
      expect(host.lastBodyOf(AEG)).toBe(`${COLL}|5`);
    });

    it('AER caps the buckets requested per round', async () => {
      process.env['SL_EDIT_AE_MAX_BUCKETS'] = '4';
      ae.trigger(COLL);
      for (let b = 0; b < 10; b++) host.roots[b] = 'f'.repeat(64);
      const peerRoots = new Array(AE_BUCKET_COUNT).fill('0'.repeat(64)).join('');
      await ae.onMessage(msg(AER, `${COLL}|${peerRoots}`));
      const asked = host.lastBodyOf(AEG)!.split('|')[1].split(',');
      expect(asked).toHaveLength(4);
    });

    it('AER with no session (never triggered) is ignored', async () => {
      await ae.onMessage(msg(AER, `${COLL}|${host.roots.join('')}`));
      expect(host.countOf(AEG)).toBe(0);
    });

    it('AEE -> requests missing docs (AEW) and finishes when pending drains', async () => {
      ae.trigger(COLL);
      differAt(bucketOf('missing'));
      const peerRoots = new Array(AE_BUCKET_COUNT).fill('0'.repeat(64)).join('');
      await ae.onMessage(msg(AER, `${COLL}|${peerRoots}`));
      const bucket = bucketOf('missing');
      // We hold 'have' already; 'missing' we lack -> only 'missing' is wanted.
      host.manifest.set('have', 'hh');
      const entries: Array<[number, Array<[string, string]>]> = [
        [bucket, [['missing', 'mh'], ['have', 'hh']]],
      ];
      await ae.onMessage(msg(AEE, `${COLL}|${JSON.stringify(entries)}`));
      expect(host.countOf(AEW)).toBe(1);
      expect(JSON.parse(host.lastBodyOf(AEW)!.split('|')[1])).toEqual([
        'missing',
      ]);
      // pending drained -> round finished -> re-trigger accepted.
      expect(ae.trigger(COLL)).toBe(true);
    });

    it('AEE -> re-broadcasts tombstones for a deleted doc instead of pulling it', async () => {
      ae.trigger(COLL);
      const bucket = bucketOf('deleted');
      host.roots[bucket] = 'f'.repeat(64);
      const peerRoots = new Array(AE_BUCKET_COUNT).fill('0'.repeat(64)).join('');
      await ae.onMessage(msg(AER, `${COLL}|${peerRoots}`));
      host.tombstones.add('deleted');
      const entries: Array<[number, Array<[string, string]>]> = [
        [bucket, [['deleted', 'dh']]],
      ];
      await ae.onMessage(msg(AEE, `${COLL}|${JSON.stringify(entries)}`));
      expect(host.pushedTombstones).toEqual([
        { collection: COLL, ids: ['deleted'] },
      ]);
      expect(host.countOf(AEW)).toBe(0);
    });

    it('AEE -> ignores an empty-hash entry (a manifest tombstone marker)', async () => {
      ae.trigger(COLL);
      const bucket = bucketOf('gone');
      host.roots[bucket] = 'f'.repeat(64);
      const peerRoots = new Array(AE_BUCKET_COUNT).fill('0'.repeat(64)).join('');
      await ae.onMessage(msg(AER, `${COLL}|${peerRoots}`));
      const entries: Array<[number, Array<[string, string]>]> = [
        [bucket, [['gone', '']]],
      ];
      await ae.onMessage(msg(AEE, `${COLL}|${JSON.stringify(entries)}`));
      expect(host.countOf(AEW)).toBe(0);
    });

    it('AEE with no session is ignored', async () => {
      const entries: Array<[number, Array<[string, string]>]> = [[0, []]];
      await ae.onMessage(msg(AEE, `${COLL}|${JSON.stringify(entries)}`));
      expect(host.countOf(AEW)).toBe(0);
    });

    it('AEE caps the docs requested per AEW message', async () => {
      process.env['SL_EDIT_AE_MAX_DOCS'] = '2';
      ae.trigger(COLL);
      // Make five distinct missing ids that all fall in buckets we mark differing.
      const ids = ['m0', 'm1', 'm2', 'm3', 'm4'];
      for (const id of ids) host.roots[bucketOf(id)] = 'f'.repeat(64);
      const peerRoots = new Array(AE_BUCKET_COUNT).fill('0'.repeat(64)).join('');
      await ae.onMessage(msg(AER, `${COLL}|${peerRoots}`));
      const byBucket = new Map<number, Array<[string, string]>>();
      for (const id of ids) {
        const b = bucketOf(id);
        const list = byBucket.get(b) ?? [];
        list.push([id, `${id}h`]);
        byBucket.set(b, list);
      }
      const entries = [...byBucket.entries()];
      await ae.onMessage(msg(AEE, `${COLL}|${JSON.stringify(entries)}`));
      // 5 wanted / cap 2 -> at least 3 AEW messages.
      expect(host.countOf(AEW)).toBeGreaterThanOrEqual(3);
    });
  });

  describe('round-completion chaining hook', () => {
    const flush = (): Promise<void> =>
      new Promise((r) => queueMicrotask(() => r()));

    it('calls onRoundComplete when a round finishes all-equal', async () => {
      ae.trigger(COLL);
      await ae.onMessage(msg(AER, `${COLL}|${host.roots.join('')}`));
      await flush();
      expect(host.roundsCompleted).toContain(COLL);
    });

    it('calls onRoundComplete when a round is aborted', async () => {
      ae.trigger(COLL);
      ae.abort(COLL);
      await flush();
      expect(host.roundsCompleted).toContain(COLL);
    });

    it('tolerates a host that does not implement onRoundComplete', async () => {
      const bare = new FakeHost();
      (bare as { onRoundComplete?: unknown }).onRoundComplete = undefined;
      const ae2 = new MongoAntiEntropy(bare);
      ae2.trigger(COLL);
      // all-equal round -> _finish, must not throw with the hook absent
      await ae2.onMessage(msg(AER, `${COLL}|${bare.roots.join('')}`));
      await flush();
      expect(bare.roundsCompleted).toEqual([]);
    });
  });

  describe('nonce handling', () => {
    it('tolerates a message with no nonce prefix', async () => {
      // No leading digits+`|`: the body is used as-is.
      await ae.onMessage(`${AEH}${COLL}|${JSON.stringify(['h'])}`);
      expect(host.pulled).toEqual([{ collection: COLL, hashes: ['h'] }]);
    });

    it('onMessage ignores a non-AE ref', async () => {
      await ae.onMessage(`~R~${COLL}|root`);
      expect(host.sent).toHaveLength(0);
    });
  });
});
