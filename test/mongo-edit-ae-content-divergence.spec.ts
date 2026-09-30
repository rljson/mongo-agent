// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { buildMesh, converge } from './mongo-edit-mesh.ts';

/**
 * ONE-443: a document both nodes hold, with DIFFERENT content.
 *
 * The anti-entropy backfilled only what a node lacked entirely. A sliceId both
 * sides held at different hashes was skipped ("a concurrent-edit conflict
 * handled elsewhere") — but for a document that lives only in a baseline
 * (manifest, no edit chain), there IS no elsewhere. Two nodes loaded from
 * different backups then sat on two contents for good, every screen reporting
 * the sync as healthy, because the only thing compared was presence.
 *
 * The rule both nodes now apply, identically: the newer edit wins (the
 * edit chain's own per-document `timeId`); an edited document beats one that
 * was only ever loaded; two loads decide by content hash — arbitrary, but the
 * same answer on every node, which is what makes it converge.
 */

const COLLECTION = 'customers';

describe('MongoEditSync — anti-entropy decides on differing content', () => {
  let stopMesh: (() => Promise<void>) | undefined;

  beforeEach(() => {
    process.env['SL_EDIT_TRACE'] = '1';
    process.env['SL_EDIT_PULL_RETRIES'] = '1';
    process.env['SL_EDIT_PULL_BACKOFF_MS'] = '1';
    process.env['SL_EDIT_ROOT_DEBOUNCE_MS'] = '5';
    process.env['SL_EDIT_HEARTBEAT_MS'] = '30';
    process.env['SL_EDIT_AE_COOLDOWN_MS'] = '1';
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
    ]) {
      delete process.env[k];
    }
  }, 40_000);

  // The ticket's test: seed two machines from different backups.
  it('two-different-initial-loads: nodes seeded from different backups converge', async () => {
    const fromA = { _id: 'k1', name: 'Backup A', street: 'Hauptstr. 1' };
    const fromB = { _id: 'k1', name: 'Backup B', street: 'Nebenweg 7' };
    const { nodes, stop } = await buildMesh(2, [COLLECTION], {
      seed: (ns) => {
        const a = ns[0].mongo.collection(COLLECTION);
        const b = ns[1].mongo.collection(COLLECTION);
        a.docs.set('k1', fromA);
        b.docs.set('k1', fromB);
        // Plus what only one backup has, so presence and content are tested
        // in the same run.
        a.docs.set('only-a', { _id: 'only-a', v: 1 });
        b.docs.set('only-b', { _id: 'only-b', v: 1 });
      },
    });
    stopMesh = stop;

    const state = await converge(nodes, COLLECTION, 15_000);

    expect(state['only-a']).toMatchObject({ v: 1 });
    expect(state['only-b']).toMatchObject({ v: 1 });
    // Either backup may win — both nodes must hold the SAME one.
    expect([fromA, fromB]).toContainEqual(state['k1']);
  }, 40_000);

  it('converges three nodes that each loaded a different backup', async () => {
    const { nodes, stop } = await buildMesh(3, [COLLECTION], {
      seed: (ns) => {
        ns.forEach((n, i) =>
          n.mongo
            .collection(COLLECTION)
            .docs.set('k1', { _id: 'k1', from: `backup-${i}` }),
        );
      },
    });
    stopMesh = stop;

    const state = await converge(nodes, COLLECTION, 20_000);
    expect(['backup-0', 'backup-1', 'backup-2']).toContain(
      (state['k1'] as { from: string }).from,
    );
  }, 40_000);

  // heals-after-forced-divergence, mongo side: the edit's head is LOST, so
  // the only way it can arrive is the anti-entropy — and there it must win
  // over the other node's loaded version rather than be skipped.
  it('heals-after-forced-divergence: an edit whose head was lost still wins', async () => {
    const { nodes, bus, stop } = await buildMesh(2, [COLLECTION], {
      seed: (ns) => {
        for (const n of ns) {
          n.mongo
            .collection(COLLECTION)
            .docs.set('k1', { _id: 'k1', v: 1, name: 'loaded' });
        }
      },
    });
    stopMesh = stop;
    const [a] = nodes;
    await converge(nodes, COLLECTION, 15_000);

    // Every head A announces for this collection is lost from now on.
    bus.drop = (from, ref) => from === 'A' && ref.startsWith(`${COLLECTION}:`);
    a.put(COLLECTION, { _id: 'k1', v: 2, name: 'edited on A' });

    const state = await converge(nodes, COLLECTION, 15_000);
    expect(state['k1']).toMatchObject({ v: 2, name: 'edited on A' });
  }, 40_000);

  it('an edit made after the loads beats the other node’s loaded version', async () => {
    const { nodes, bus, stop } = await buildMesh(2, [COLLECTION], {
      seed: (ns) => {
        ns[0].mongo
          .collection(COLLECTION)
          .docs.set('k1', { _id: 'k1', v: 1, from: 'backup A' });
        ns[1].mongo
          .collection(COLLECTION)
          .docs.set('k1', { _id: 'k1', v: 1, from: 'backup B' });
      },
    });
    stopMesh = stop;
    const [, b] = nodes;

    // B edits its copy, and the head is lost: B's edited version carries a
    // timeId, A's loaded one does not — the edit must win, whichever backup
    // the hash tiebreak would have picked.
    bus.drop = (from, ref) => from === 'B' && ref.startsWith(`${COLLECTION}:`);
    b.put(COLLECTION, { _id: 'k1', v: 2, from: 'edited on B' });

    const state = await converge(nodes, COLLECTION, 15_000);
    expect(state['k1']).toMatchObject({ v: 2, from: 'edited on B' });
  }, 40_000);
});

// The decision is made a round trip before the pull lands. Between the two, a
// local edit can happen — and it must not be overwritten by a version that
// won against what this node held BEFORE the edit.
describe('MongoEditSync — the pull re-checks a contested decision', () => {
  let stopMesh: (() => Promise<void>) | undefined;

  // No anti-entropy of its own: only this test may touch k1.
  beforeEach(() => {
    process.env['SL_EDIT_HEARTBEAT_MS'] = '600000';
    process.env['SL_EDIT_AE_COOLDOWN_MS'] = '600000';
  });

  afterEach(async () => {
    await stopMesh?.();
    stopMesh = undefined;
    delete process.env['SL_EDIT_HEARTBEAT_MS'];
    delete process.env['SL_EDIT_AE_COOLDOWN_MS'];
  });

  type Internals = {
    _serveComponents: (c: string, ids: string[]) => Promise<string[]>;
    _pullAndApply: (
      c: string,
      hashes: string[],
      contested?: Map<string, string | undefined>,
    ) => Promise<number>;
    _setAppliedTimeId: (c: string, id: unknown, t: string | undefined) => void;
    _timeIdsOf: (c: string) => Map<string, string>;
  };

  /** Two nodes holding k1 at different content, and the winner's hash. */
  const setup = async () => {
    const { nodes, stop } = await buildMesh(2, [COLLECTION], {
      seed: (ns) => {
        ns[0].mongo.collection(COLLECTION).docs.set('k1', { _id: 'k1', v: 'mine' });
        ns[1].mongo.collection(COLLECTION).docs.set('k1', { _id: 'k1', v: 'theirs' });
      },
    });
    stopMesh = stop;
    const a = nodes[0].sync as unknown as Internals;
    const b = nodes[1].sync as unknown as Internals;
    const hashes = await b._serveComponents(COLLECTION, ['k1']);
    return { nodes, a, hashes };
  };

  it('keeps a local edit made since the decision', async () => {
    const { nodes, a, hashes } = await setup();
    a._setAppliedTimeId(COLLECTION, 'k1', '9000000000000:local');
    const contested = new Map([['k1', '1000000000000:peer']]);
    await a._pullAndApply(COLLECTION, hashes, contested);
    expect(nodes[0].mongo.collection(COLLECTION).docs.get('k1')).toMatchObject({ v: 'mine' });
    expect(contested.size).toBe(0);
  }, 40_000);

  it('keeps a local edit against a winner that was never edited', async () => {
    const { nodes, a, hashes } = await setup();
    a._setAppliedTimeId(COLLECTION, 'k1', '9000000000000:local');
    await a._pullAndApply(COLLECTION, hashes, new Map([['k1', undefined]]));
    expect(nodes[0].mongo.collection(COLLECTION).docs.get('k1')).toMatchObject({ v: 'mine' });
  }, 40_000);

  it('applies a newer winner, and adopts its timeId', async () => {
    const { nodes, a, hashes } = await setup();
    a._setAppliedTimeId(COLLECTION, 'k1', '1000000000000:local');
    await a._pullAndApply(COLLECTION, hashes, new Map([['k1', '9000000000000:peer']]));
    expect(nodes[0].mongo.collection(COLLECTION).docs.get('k1')).toMatchObject({ v: 'theirs' });
    expect(a._timeIdsOf(COLLECTION).get('k1')).toBe('9000000000000:peer');
  }, 40_000);
});
