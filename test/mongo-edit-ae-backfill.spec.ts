// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { afterEach, beforeEach, describe, expect, it } from 'vitest';

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
});
