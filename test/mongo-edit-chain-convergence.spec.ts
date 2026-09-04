// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import {
  buildMesh,
  converge,
  docsOf,
  expectNoRegression,
  restartNode,
  settle,
  takeSyncWrites,
} from './mongo-edit-mesh.ts';

/**
 * Convergence of the components/edits sync under churn — the field report:
 *
 *   "Not every update/delete converges on all nodes under load. Symptom:
 *   `applyHead <coll> head=… PARTIAL after retries -> invalidate`. A receiver
 *   pulls a peer's head, but collectPuts does not return all rows of the edit
 *   chain → after retries the head is invalidated and discarded → that node
 *   stays on the old state. CREATEs always arrive; it hits updates/deletes as
 *   soon as a collection's edit chain gets long (lots of churn). It moves
 *   between nodes/collections depending on chain length. No data loss, but no
 *   guaranteed 10/10 convergence."
 *
 * These run the real `MongoEditSync` on a faithful model of the production
 * topology (see `mongo-edit-mesh.ts`): one mongo + one rljson `Db` per node,
 * a read-only NON-DUMPABLE relay between them, and refs over a bus with the
 * production dedup semantics. Reproduction is "many fast insert/update/delete
 * cycles on one collection across several nodes", exactly as reported.
 */

const COLLECTION = 'customers';
const EH_TABLE = 'pCustomersCakeEditHistory';
const ME_TABLE = 'pCustomersCakeMultiEdits';
const ED_TABLE = 'pCustomersCakeEdits';

describe('MongoEditSync — edit-chain convergence', () => {
  let stopMesh: (() => Promise<void>) | undefined;

  beforeEach(() => {
    process.env['SL_EDIT_PULL_RETRIES'] = '1';
    process.env['SL_EDIT_PULL_BACKOFF_MS'] = '1';
    process.env['SL_EDIT_ROOT_DEBOUNCE_MS'] = '5';
    process.env['SL_EDIT_DELETE_DEBOUNCE_MS'] = '10';
    process.env['SL_EDIT_HEARTBEAT_MS'] = '50';
  });

  afterEach(async () => {
    await stopMesh?.();
    stopMesh = undefined;
    delete process.env['SL_EDIT_PULL_RETRIES'];
    delete process.env['SL_EDIT_PULL_BACKOFF_MS'];
    delete process.env['SL_EDIT_ROOT_DEBOUNCE_MS'];
    delete process.env['SL_EDIT_DELETE_DEBOUNCE_MS'];
    delete process.env['SL_EDIT_HEARTBEAT_MS'];
  }, 40_000);

  // ...........................................................................
  it('two nodes converge on insert, update and delete', async () => {
    const { nodes, stop } = await buildMesh(2, [COLLECTION]);
    stopMesh = stop;
    const [a, b] = nodes;

    a.put(COLLECTION, { _id: 'c1', name: 'Alice', v: 1 });
    expect((await converge(nodes, COLLECTION))['c1']).toEqual({
      _id: 'c1',
      name: 'Alice',
      v: 1,
    });

    a.put(COLLECTION, { _id: 'c1', name: 'Alice', v: 2 });
    expect((await converge(nodes, COLLECTION))['c1']).toMatchObject({ v: 2 });

    b.del(COLLECTION, 'c1');
    expect(await converge(nodes, COLLECTION)).toEqual({});
    expectNoRegression(nodes, COLLECTION);
  }, 40_000);

  // ...........................................................................
  it('three nodes converge when each writes its own document', async () => {
    const { nodes, stop } = await buildMesh(3, [COLLECTION]);
    stopMesh = stop;

    for (let round = 1; round <= 4; round++) {
      for (let i = 0; i < nodes.length; i++) {
        nodes[i].put(COLLECTION, {
          _id: `c${i}`,
          owner: nodes[i].id,
          v: round,
        });
        await settle(nodes);
      }
    }

    const state = await converge(nodes, COLLECTION);
    for (let i = 0; i < nodes.length; i++) {
      expect(state[`c${i}`]).toMatchObject({ v: 4 });
    }
    expectNoRegression(nodes, COLLECTION);
  }, 40_000);

  // ...........................................................................
  it('three nodes converge under interleaved insert/update/delete churn', async () => {
    const { nodes, stop } = await buildMesh(3, [COLLECTION]);
    stopMesh = stop;

    const docCount = 6;
    for (let i = 0; i < docCount; i++) {
      nodes[i % nodes.length].put(COLLECTION, {
        _id: `d${i}`,
        v: 0,
        by: nodes[i % nodes.length].id,
      });
    }
    await converge(nodes, COLLECTION);

    // Churn: every document is updated from a rotating node, so each
    // receiver's applies keep jumping between two foreign lineages.
    for (let round = 1; round <= 4; round++) {
      for (let i = 0; i < docCount; i++) {
        const writer = nodes[(i + round) % nodes.length];
        writer.put(COLLECTION, { _id: `d${i}`, v: round, by: writer.id });
        await settle(nodes);
      }
    }

    const afterChurn = await converge(nodes, COLLECTION);
    for (let i = 0; i < docCount; i++) {
      expect(afterChurn[`d${i}`]).toMatchObject({ v: 4 });
    }

    // Deletes, issued after the chains have grown.
    for (let i = 0; i < docCount; i += 2) {
      nodes[2].del(COLLECTION, `d${i}`);
      await settle(nodes);
    }

    expect(Object.keys(await converge(nodes, COLLECTION)).sort()).toEqual([
      'd1',
      'd3',
      'd5',
    ]);
    expectNoRegression(nodes, COLLECTION);
  }, 40_000);

  // ...........................................................................
  it('a delete is not resurrected by a head from another lineage', async () => {
    const { nodes, stop } = await buildMesh(3, [COLLECTION]);
    stopMesh = stop;
    const [a, b] = nodes;

    // A owns the document and edits it repeatedly, so its lineage carries
    // several puts for `gone`.
    for (let v = 1; v <= 3; v++) {
      a.put(COLLECTION, { _id: 'gone', v });
      await settle(nodes);
    }
    await converge(nodes, COLLECTION);

    // B deletes it — the tombstone is the newest edit on that document.
    b.del(COLLECTION, 'gone');
    expect(await converge(nodes, COLLECTION)).toEqual({});

    // A now edits an UNRELATED document. Its head still descends from the
    // three `gone` puts — replaying that lineage must not bring `gone` back.
    a.put(COLLECTION, { _id: 'other', v: 1 });
    const state = await converge(nodes, COLLECTION);

    expect(state['gone'], 'a deleted document was resurrected').toBeUndefined();
    expect(state['other']).toMatchObject({ v: 1 });
    expectNoRegression(nodes, COLLECTION);
  }, 40_000);

  // ...........................................................................
  it('a restarted node does not regress documents its peers own', async () => {
    const { nodes, stop } = await buildMesh(3, [COLLECTION]);
    stopMesh = stop;
    const [a, b, c] = nodes;

    // A builds a lineage holding v1 of both documents.
    a.put(COLLECTION, { _id: 'x', v: 1, by: 'A' });
    await settle(nodes);
    a.put(COLLECTION, { _id: 'y', v: 1, by: 'A' });
    await settle(nodes);

    // B takes both documents forward.
    b.put(COLLECTION, { _id: 'x', v: 2, by: 'B' });
    await settle(nodes);
    b.put(COLLECTION, { _id: 'y', v: 2, by: 'B' });
    await converge(nodes, COLLECTION);

    // C restarts: all in-memory sync state is gone, mongo and the local
    // rljson store survive.
    await restartNode(c, [COLLECTION]);
    await settle(nodes);

    // A now writes a third document. Its head still descends from its own
    // stale v1 puts for x and y, and C — with no applied state left — must not
    // replay that lineage over B's newer documents.
    a.put(COLLECTION, { _id: 'z', v: 1, by: 'A' });
    const state = await converge(nodes, COLLECTION);

    expect(state['x']).toMatchObject({ v: 2, by: 'B' });
    expect(state['y']).toMatchObject({ v: 2, by: 'B' });
    expect(state['z']).toMatchObject({ v: 1, by: 'A' });
    expectNoRegression(nodes, COLLECTION);
  }, 40_000);

  // ...........................................................................
  it('one new edit costs one applied write per peer, however long the chain', async () => {
    const { nodes, stop } = await buildMesh(3, [COLLECTION]);
    stopMesh = stop;

    // Grow the chain: 12 edits spread over all three nodes.
    for (let i = 0; i < 12; i++) {
      nodes[i % nodes.length].put(COLLECTION, { _id: `k${i % 4}`, v: i });
      await settle(nodes);
    }
    await converge(nodes, COLLECTION);
    for (const node of nodes) takeSyncWrites(node, COLLECTION);

    // One further edit. Each other node must apply exactly that one document,
    // not replay the lineage it belongs to.
    nodes[0].put(COLLECTION, { _id: 'k0', v: 99 });
    await converge(nodes, COLLECTION);

    for (const node of nodes.slice(1)) {
      const applied = takeSyncWrites(node, COLLECTION);
      expect(
        applied,
        `node ${node.id} applied ${applied} writes for a single new edit`,
      ).toBeLessThanOrEqual(2);
    }
  }, 40_000);

  // ...........................................................................
  it('chain rows the single-row read path cannot serve are pulled by content hash', async () => {
    const { nodes, stop } = await buildMesh(2, [COLLECTION]);
    stopMesh = stop;
    const [a, b] = nodes;

    // The relay cannot serve the edit-chain tables through the single-row read
    // path — the production hole the report names ("rows referenced only via a
    // path that does not relay are missing from the pull"). The content-hash
    // batch path (`readRowsByHashes`, relay-capable) still resolves them, so a
    // pull that re-fetches missing rows by hash converges; one that walks the
    // chain row by row goes PARTIAL and discards the head.
    for (const table of [EH_TABLE, ME_TABLE, ED_TABLE]) {
      b.peer.singleReadBlockedTables.add(table);
    }

    a.put(COLLECTION, { _id: 'p1', v: 1 });
    await settle(nodes);
    a.put(COLLECTION, { _id: 'p2', v: 1 });

    const state = await converge(nodes, COLLECTION);
    expect(
      state['p2'],
      'B discarded the head instead of completing the pull by content hash',
    ).toMatchObject({ v: 1 });
    expectNoRegression(nodes, COLLECTION);
  }, 40_000);

  // ...........................................................................
  it('a late joiner catches up after its first pull came back empty', async () => {
    const { nodes, stop } = await buildMesh(2, [COLLECTION]);
    stopMesh = stop;
    const [a, b] = nodes;

    // Baseline: both nodes converge on v1.
    a.put(COLLECTION, { _id: 'x', v: 1 });
    await converge(nodes, COLLECTION);

    // Reconnect race: B's origin rows are momentarily unresolvable. It still
    // receives A's next head + root over the bus, but every pull comes back
    // empty, so those refs are consumed by its received-dedup without applying
    // anything — B is left holding a pending head whose content root it lacks.
    b.peer.blockReads = true;
    a.put(COLLECTION, { _id: 'x', v: 2 });
    await settle([a]);
    expect(
      docsOf(b, COLLECTION)['x'],
      'B applied v2 even though its pull was blocked',
    ).toMatchObject({ v: 1 });

    // Rows resolve again. Nothing new is written — only the periodic root
    // heartbeat re-announces the UNCHANGED head/root. The late joiner must
    // re-drive the pending head off that heartbeat and converge, rather than
    // sit on v1 until some unrelated write mints a fresh hash.
    b.peer.blockReads = false;
    const state = await converge(nodes, COLLECTION);
    expect(
      state['x'],
      'late joiner never caught up after its rows became resolvable',
    ).toMatchObject({ v: 2 });
    expectNoRegression(nodes, COLLECTION);
  }, 40_000);
});
