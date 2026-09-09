// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { bucketOf } from '../src/mongo-manifest-hash.ts';
import { buildMesh, converge, docsOf, settle } from './mongo-edit-mesh.ts';

/**
 * Manifest-diff BACKFILL ("anti-entropy") over the real `MongoEditSync`.
 *
 * The scenario head-pull CANNOT fix: a peer holds documents only in its
 * cold-start BASELINE (manifest) — they live in no edit chain, so no head
 * carries them. Two nodes then sit at different content roots forever unless the
 * bucketed manifest reconciliation pulls the missing docs by content hash. These
 * tests seed a node's mongo BEFORE start (so the snapshot folds the docs into
 * the manifest chain-free) and assert the lagging peer converges via the
 * backfill.
 */

const COLLECTION = 'customers';

describe('MongoEditSync — anti-entropy backfill', () => {
  let stopMesh: (() => Promise<void>) | undefined;

  beforeEach(() => {
    process.env['SL_EDIT_TRACE'] = '1';
    process.env['SL_EDIT_PULL_RETRIES'] = '1';
    process.env['SL_EDIT_PULL_BACKOFF_MS'] = '1';
    process.env['SL_EDIT_ROOT_DEBOUNCE_MS'] = '5';
    process.env['SL_EDIT_HEARTBEAT_MS'] = '30';
    process.env['SL_EDIT_AE_COOLDOWN_MS'] = '1';
    // A requester's first AEQ can race ahead of the responder's cold-start (the
    // peer is not `ready` yet and drops it). A short round timeout aborts that
    // stuck round quickly so the next heartbeat re-triggers it once the peer is
    // ready — the same self-healing the 30 s production default gives, sped up
    // so the round completes inside the test budget.
    process.env['SL_EDIT_AE_ROUND_TIMEOUT_MS'] = '500';
  });

  afterEach(async () => {
    await stopMesh?.();
    stopMesh = undefined;
    for (const k of [
      'SL_EDIT_TRACE',
      'SL_EDIT_PULL_RETRIES',
      'SL_EDIT_PULL_BACKOFF_MS',
      'SL_EDIT_ROOT_DEBOUNCE_MS',
      'SL_EDIT_HEARTBEAT_MS',
      'SL_EDIT_AE_COOLDOWN_MS',
      'SL_EDIT_AE_ROUND_TIMEOUT_MS',
      'SL_EDIT_AE_MAX_BUCKETS',
      'SL_EDIT_AE_NOPROGRESS_BACKOFF_MS',
    ]) {
      delete process.env[k];
    }
  }, 40_000);

  it('backfills baseline-only docs of every CARAT id shape into a lagging node', async () => {
    // The three `_id` shapes the backfill's typed-id resolver must handle:
    // a plain string, a numeric id (CARAT catalog keys), and an ObjectId hex.
    const oid = 'a1b2c3d4e5f6a1b2c3d4e5f6';
    const { nodes, stop } = await buildMesh(2, [COLLECTION], {
      // B's mongo already holds these before it starts. Cold-start records them
      // in B's manifest but NOT in any edit chain, so A can only get them via
      // the backfill — there is no head to pull.
      seed: (ns) => {
        const col = ns[1].mongo.collection(COLLECTION);
        col.docs.set('base1', { _id: 'base1', name: 'Baseline', v: 1 });
        col.docs.set('2400042', { _id: '2400042', name: 'Numeric' });
        col.docs.set(oid, { _id: oid, name: 'ObjectIdish' });
      },
    });
    stopMesh = stop;

    const state = await converge(nodes, COLLECTION);
    expect(
      state['base1'],
      'the lagging node never backfilled the baseline-only doc',
    ).toMatchObject({ _id: 'base1', name: 'Baseline', v: 1 });
    expect(state['2400042']).toMatchObject({ name: 'Numeric' });
    expect(state[oid]).toMatchObject({ name: 'ObjectIdish' });
  }, 40_000);

  it('backfills many baseline docs and still converges on later live edits', async () => {
    const seeded: Record<string, unknown> = {};
    const { nodes, stop } = await buildMesh(2, [COLLECTION], {
      seed: (ns) => {
        for (let i = 0; i < 50; i++) {
          const id = `seed-${i}`;
          const doc = { _id: id, v: i };
          ns[1].mongo.collection(COLLECTION).docs.set(id, doc);
          seeded[id] = doc;
        }
      },
    });
    stopMesh = stop;
    const [a] = nodes;

    // A live edit on A on TOP of the backfill: both must land everywhere.
    a.put(COLLECTION, { _id: 'live', v: 99 });

    const state = await converge(nodes, COLLECTION);
    for (let i = 0; i < 50; i++) {
      expect(state[`seed-${i}`]).toMatchObject({ v: i });
    }
    expect(state['live']).toMatchObject({ v: 99 });
  }, 40_000);

  it('chains many capped rounds to converge a large baseline delta', async () => {
    // One bucket per round (SL_EDIT_AE_MAX_BUCKETS=1) forces the backfill to
    // take MANY rounds. Convergence then depends on the round-completion chain
    // re-driving each next round for the still-diverged collection, not on
    // re-receiving the peer root. The doc count exceeds one round's capacity.
    process.env['SL_EDIT_AE_MAX_BUCKETS'] = '1';
    const { nodes, stop } = await buildMesh(2, [COLLECTION], {
      seed: (ns) => {
        const col = ns[1].mongo.collection(COLLECTION);
        for (let i = 0; i < 40; i++) col.docs.set(`c${i}`, { _id: `c${i}`, v: i });
      },
    });
    stopMesh = stop;

    const state = await converge(nodes, COLLECTION);
    for (let i = 0; i < 40; i++) {
      expect(state[`c${i}`], `doc c${i} never backfilled`).toMatchObject({ v: i });
    }
  }, 40_000);

  it('backs off a no-progress round then converges once the pull works again', async () => {
    // A round that finds divergence but pulls NOTHING (a slow/unservable read)
    // must not spin the chain; it backs off. Once the pull works, the backfill
    // resumes and converges. Short backoff so the test does not wait 5s.
    process.env['SL_EDIT_AE_NOPROGRESS_BACKOFF_MS'] = '30';
    const { nodes, stop } = await buildMesh(2, [COLLECTION], {
      seed: (ns) => {
        const col = ns[1].mongo.collection(COLLECTION);
        for (let i = 0; i < 20; i++) col.docs.set(`b${i}`, { _id: `b${i}`, v: i });
      },
    });
    stopMesh = stop;
    const [a] = nodes;

    // A's pulls return empty at first -> its rounds make no progress -> backoff.
    a.peer.blockReads = true;
    await settle(nodes, 1200);
    expect(docsOf(a, COLLECTION)['b0']).toBeUndefined(); // nothing pulled yet
    // Pull works again -> the backfill resumes and converges.
    a.peer.blockReads = false;
    const state = await converge(nodes, COLLECTION);
    for (let i = 0; i < 20; i++) expect(state[`b${i}`]).toMatchObject({ v: i });
  }, 40_000);

  it('does not resurrect a doc the lagging node deleted (tombstone wins)', async () => {
    // Both nodes know 'shared'; A then deletes it. B still carries it in its
    // baseline. The backfill must NOT pull the deleted doc back onto A — instead
    // A re-drives the tombstone so the delete wins on B too.
    const { nodes, stop } = await buildMesh(2, [COLLECTION]);
    stopMesh = stop;
    const [a] = nodes;

    a.put(COLLECTION, { _id: 'shared', v: 1 });
    await converge(nodes, COLLECTION);

    a.del(COLLECTION, 'shared');
    await settle(nodes, 3000);

    // Give the backfill ample time to (wrongly) resurrect it, then assert it did
    // not — on A it stays deleted and B converges to deleted as well.
    await settle(nodes, 2000);
    expect(docsOf(a, COLLECTION)['shared']).toBeUndefined();
    const state = await converge(nodes, COLLECTION);
    expect(state['shared']).toBeUndefined();
  }, 40_000);

  it('re-asserts a persisted tombstone against a peer holding the doc in its baseline', async () => {
    // The resurrection failure the demo hit, driven entirely through the
    // backfill: B carries 'ghost' only in its cold-start baseline (no edit chain
    // head), and A restarts holding a PERSISTED tombstone for it. When A sees
    // B's 'ghost' in the manifest diff it must NOT pull it back — it re-drives
    // the delete so 'ghost' disappears on B too.
    const { nodes, stop } = await buildMesh(2, [COLLECTION], {
      seed: (ns) => {
        // B holds the doc only in its baseline.
        ns[1].mongo
          .collection(COLLECTION)
          .docs.set('ghost', { _id: 'ghost', v: 1 });
        // A restarts with a persisted tombstone for it (what _loadTombstones
        // reads back on start).
        ns[0].mongo.collection('sl_edit_tombstones').docs.set(
          `${COLLECTION}|ghost`,
          { _id: `${COLLECTION}|ghost`, collection: COLLECTION, id: 'ghost' },
        );
      },
    });
    stopMesh = stop;
    const [, b] = nodes;

    const state = await converge(nodes, COLLECTION);
    expect(
      state['ghost'],
      'the tombstone was ignored and the deleted doc came back',
    ).toBeUndefined();
    // The delete reached the peer that still held it in its baseline.
    expect(docsOf(b, COLLECTION)['ghost']).toBeUndefined();
  }, 40_000);

  it('advertises a persisted tombstone as an empty-hash bucket entry (a live doc still wins)', async () => {
    // A holds a persisted tombstone for 'orphan' (deleted, not in its mongo) and
    // separately still holds 'reborn' live even though a stale tombstone for it
    // lingers. `_bucketEntries` must advertise 'orphan' as a tombstone so a
    // superset peer can drop it, but never shadow the live 'reborn'.
    const { nodes, stop } = await buildMesh(2, [COLLECTION], {
      seed: (ns) => {
        const tomb = ns[0].mongo.collection('sl_edit_tombstones');
        for (const id of ['orphan', 'reborn']) {
          tomb.docs.set(`${COLLECTION}|${id}`, {
            _id: `${COLLECTION}|${id}`,
            collection: COLLECTION,
            id,
          });
        }
        // 'reborn' is live again in A's mongo despite the lingering tombstone.
        ns[0].mongo
          .collection(COLLECTION)
          .docs.set('reborn', { _id: 'reborn', v: 2 });
      },
    });
    stopMesh = stop;
    const [a] = nodes;
    await settle(nodes, 200);

    const internals = a.sync as unknown as {
      _bucketEntries: (
        c: string,
        b: number[],
      ) => Map<number, Array<[string, string]>>;
    };
    const ob = bucketOf('orphan');
    const rb = bucketOf('reborn');
    const got = internals._bucketEntries(COLLECTION, [ob, rb]);
    expect(got.get(ob)).toContainEqual(['orphan', '']);
    // The live doc wins: 'reborn' appears with its real hash, never as a tombstone.
    const rebornEntries = got.get(rb) ?? [];
    expect(rebornEntries.some(([id]) => id === 'reborn')).toBe(true);
    expect(rebornEntries).not.toContainEqual(['reborn', '']);
  }, 40_000);

  it('drops a locally-held doc when a peer tombstone arrives through the engine (superset convergence)', async () => {
    // A single node holds 'orphan' live (a delete head it missed). Driven
    // end-to-end through its real anti-entropy engine: a controlled round feeds
    // a peer's roots (only orphan's bucket differs) and that peer's entries
    // (orphan advertised as a tombstone). The node must drop its own copy — the
    // only path that converges a SUPERSET node down — exercising the real host
    // arrow. A lone node has no peer to resurrect it, so the result is exact.
    const { nodes, stop } = await buildMesh(1, [COLLECTION], {
      seed: (ns) => {
        ns[0].mongo
          .collection(COLLECTION)
          .docs.set('orphan', { _id: 'orphan', v: 1 });
      },
    });
    stopMesh = stop;
    const [a] = nodes;
    await settle(nodes, 200);
    expect(docsOf(a, COLLECTION)['orphan']).toMatchObject({ v: 1 });

    const ai = a.sync as unknown as {
      _ae: {
        trigger: (c: string) => boolean;
        onMessage: (ref: string) => Promise<void>;
        _busy: Set<string>;
        _sessions: Map<string, unknown>;
      };
      _bucketRoots: (c: string) => string[];
    };
    // Start a clean round for the collection, then feed the peer's view.
    ai._ae._busy.delete(COLLECTION);
    ai._ae._sessions.delete(COLLECTION);
    ai._ae.trigger(COLLECTION);
    const mine = ai._bucketRoots(COLLECTION);
    const ob = bucketOf('orphan');
    const peer = [...mine];
    peer[ob] = '0'.repeat(64); // only orphan's bucket differs
    await ai._ae.onMessage(`~AER~9|${COLLECTION}|${peer.join('')}`);
    await ai._ae.onMessage(
      `~AEE~9|${COLLECTION}|${JSON.stringify([[ob, [['orphan', '']]]])}`,
    );
    await settle(nodes, 200);
    expect(docsOf(a, COLLECTION)['orphan']).toBeUndefined();
  }, 40_000);
  it('a round that pulled documents chains on the cooldown, not the back-off', async () => {
    // The other side of the gate. A productive round must resume as soon as the
    // cooldown it armed has elapsed — waiting out the no-progress back-off
    // instead would turn a fast backfill into a trickle.
    process.env['SL_EDIT_AE_NOPROGRESS_BACKOFF_MS'] = '5000';
    const { nodes, stop } = await buildMesh(2, [COLLECTION]);
    stopMesh = stop;
    const [a] = nodes;
    const internals = a.sync as unknown as {
      _aeRoundProgress: Map<string, boolean>;
      _aeCooldownUntil: Map<string, number>;
      _lastPeerHead: Map<string, { head: string; root?: string; ref: string }>;
      _onAeRoundComplete: (c: string) => void;
      _maybeTriggerAe: (c: string) => void;
    };

    // Diverged from the peer, and the round we just finished applied documents.
    internals._lastPeerHead.set(COLLECTION, {
      head: 'H',
      root: 'f'.repeat(64),
      ref: `${COLLECTION}:H`,
    });
    internals._aeRoundProgress.set(COLLECTION, true);
    internals._aeCooldownUntil.set(COLLECTION, Date.now() + 40);

    let triggered = 0;
    internals._maybeTriggerAe = () => {
      triggered++;
    };
    internals._onAeRoundComplete(COLLECTION);

    // Chains on the cooldown remainder (~40ms), long before the 5s back-off.
    await new Promise((r) => setTimeout(r, 300));
    expect(triggered, 'a productive round waited out the no-progress back-off').toBe(1);
    // The progress flag is consumed, so the next round judges itself afresh.
    expect(internals._aeRoundProgress.has(COLLECTION)).toBe(false);
  }, 40_000);
});
