// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Db } from '@rljson/db';
import { IoMem } from '@rljson/io';
import { EJSON, Int32 } from 'bson';
import { describe, expect, it } from 'vitest';

import { MongoEditChain } from '../src/mongo-edit-chain.ts';

const mkDb = async (): Promise<{ io: IoMem; db: Db }> => {
  const io = new IoMem();
  await io.init();
  await io.isReady();
  return { io, db: new Db(io) };
};

const canon = (v: unknown): string => EJSON.stringify(v, { relaxed: false });

describe('MongoEditChain', () => {
  const prefix = 'testDb';
  const collection = 'customers';

  it('round-trips a document through putComponent/getComponent, byte-perfect', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);

    const doc = { _id: new Int32(1), name: 'Alice', age: new Int32(30) };
    const hash = await chain.putComponent(collection, doc);
    expect(hash).toMatch(/^[0-9a-zA-Z_-]+$/);

    const back = await chain.getComponent(collection, hash);
    expect(canon(back)).toBe(canon(doc));

    // Idempotent: storing the same content again is a no-op, same hash.
    const hash2 = await chain.putComponent(collection, doc);
    expect(hash2).toBe(hash);
  });

  it('answers undefined for a component this node does not have', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    expect(await chain.getComponent(collection, 'NOT_STORED')).toBeUndefined();
  });

  it('answers undefined, not throw, when the components table does not exist yet', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    // Never initialized — its components table was never created.
    expect(await chain.getComponent(collection, 'ANY_HASH')).toBeUndefined();
  });

  it('appends a root entry and reads it back', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);

    // A ROOT has nothing changed or removed — it names the state a node
    // already holds, not an edit. Its timeId is therefore content-derived.
    const root = await chain.append(collection, {
      snapshotRef: 'root-snap',
      previous: [],
    });

    expect(root.previous).toEqual([]);
    expect(root.removed).toEqual([]);
    expect(root.changed).toEqual({});
    expect(root.timeId).toBe(`0:root-snap`);

    const back = await chain.entry(collection, root.head);
    expect(back).toEqual(root);
  });

  it('stores and reads back the documents a non-root entry changes', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);

    const root = await chain.append(collection, {
      snapshotRef: 's0',
      previous: [],
    });
    const d1 = { _id: new Int32(1), name: 'Alice' };
    const edit = await chain.append(collection, {
      snapshotRef: 's1',
      changed: new Map([['1', d1]]),
      previous: [root.head],
    });

    expect(Object.keys(edit.changed)).toEqual(['1']);
    // A non-root entry's timeId is minted, never content-derived.
    expect(edit.timeId).toMatch(/^\d+:.{4}$/);

    const back = await chain.entry(collection, edit.head);
    expect(back).toEqual(edit);

    const stored = await chain.getComponent(collection, edit.changed['1']);
    expect(canon(stored)).toBe(canon(d1));
  });

  it('chains a second entry onto the first, with a minted (not content-derived) timeId', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);

    const root = await chain.append(collection, {
      snapshotRef: 's0',
      previous: [],
    });
    const edit = await chain.append(collection, {
      snapshotRef: 's1',
      changed: new Map([['1', { _id: new Int32(1), name: 'A' }]]),
      removed: ['2'],
      previous: [root.head],
    });

    expect(edit.previous).toEqual([root.head]);
    expect(edit.removed).toEqual(['2']);
    expect(edit.timeId).toMatch(/^\d+:.{4}$/);

    const back = await chain.entry(collection, edit.head);
    expect(back).toEqual(edit);
  });

  it('answers undefined for a head this node cannot resolve', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    expect(await chain.entry(collection, 'NOT_A_REF')).toBeUndefined();
  });

  it('answers undefined when the EditHistory row exists but its multiEdit does not', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    const key = chain.chainKey(collection);
    const [row] = await db.addEditHistory(key, {
      timeId: '0:x',
      multiEditRef: 'DOES_NOT_EXIST',
      dataRef: 'irrelevant',
      previous: [],
      _hash: '',
    } as never);
    const head = row[`${key}EditHistoryRef`];
    expect(await chain.entry(collection, head)).toBeUndefined();
  });

  it('answers undefined when the EditHistory and multiEdit rows exist but the edit does not', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    const key = chain.chainKey(collection);
    const [meRow] = await db.addMultiEdit(key, {
      previous: null,
      edit: 'DOES_NOT_EXIST',
      _hash: '',
    } as never);
    const multiEditRef = meRow[`${key}MultiEditsRef`];
    const [ehRow] = await db.addEditHistory(key, {
      timeId: '0:x',
      multiEditRef,
      dataRef: 'irrelevant',
      previous: [],
      _hash: '',
    } as never);
    const head = ehRow[`${key}EditHistoryRef`];
    expect(await chain.entry(collection, head)).toBeUndefined();
  });

  it('defaults previous/changed/removed when a resolvable but malformed entry carries none', async () => {
    // Every real `append()` entry carries all three — this is purely
    // defensive, for a row this node did not itself write.
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    const key = chain.chainKey(collection);
    const [edRow] = await db.addEdit(key, {
      name: 'malformed',
      action: { name: 'malformed', type: 'malformed', data: {}, _hash: '' },
      _hash: '',
    } as never);
    const editRef = edRow[`${key}EditsRef`];
    const [meRow] = await db.addMultiEdit(key, {
      previous: null,
      edit: editRef,
      _hash: '',
    } as never);
    const multiEditRef = meRow[`${key}MultiEditsRef`];
    const [ehRow] = await db.addEditHistory(key, {
      timeId: '0:x',
      multiEditRef,
      dataRef: 'irrelevant',
      previous: null,
      _hash: '',
    } as never);
    const head = ehRow[`${key}EditHistoryRef`];

    expect(await chain.entry(collection, head)).toEqual({
      head,
      timeId: '0:x',
      snapshotRef: undefined,
      previous: [],
      changed: {},
      removed: [],
    });
  });

  it('two independent chains starting from the SAME baseline content share a genuine common ancestor', async () => {
    // The whole point of content-derived root identity: two nodes that have
    // never exchanged a ref still agree on their root, because it is a
    // function of content, not of who booted first.
    const a = await mkDb();
    const chainA = new MongoEditChain(a.db, prefix);
    await chainA.init(collection);
    const b = await mkDb();
    const chainB = new MongoEditChain(b.db, prefix);
    await chainB.init(collection);

    const rootA = await chainA.append(collection, {
      snapshotRef: 'shared-baseline',
      previous: [],
    });
    const rootB = await chainB.append(collection, {
      snapshotRef: 'shared-baseline',
      previous: [],
    });

    expect(rootA.head).toBe(rootB.head);
    expect(rootA.timeId).toBe(rootB.timeId);
  });

  it('a different baseline produces a different root', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    const r1 = await chain.append(collection, { snapshotRef: 'a', previous: [] });
    const r2 = await chain.append(collection, { snapshotRef: 'b', previous: [] });
    expect(r1.head).not.toBe(r2.head);
  });
});

// .............................................................................

describe('MongoEditChain.relate', () => {
  const prefix = 'testDb';
  const collection = 'customers';

  // Three edits before the fork (h0 -> h1 -> h2), then two independent chains
  // over the SAME store continue from h2 — exactly what two nodes editing the
  // same collection while partitioned produce. Three deep so the common
  // ancestry the fork shares is itself more than one entry, which exercises
  // the dominated-ancestor walk's "already seen" branch.
  const mkFork = async (): Promise<{
    h0: string;
    h1: string;
    h2: string;
    ha: string;
    hb: string;
    chain: MongoEditChain;
  }> => {
    const { db } = await mkDb();
    const root = new MongoEditChain(db, prefix);
    await root.init(collection);
    const h0 = (await root.append(collection, { snapshotRef: 's0', previous: [] }))
      .head;
    const h1 = (
      await root.append(collection, { snapshotRef: 's1', previous: [h0] })
    ).head;
    const h2 = (
      await root.append(collection, { snapshotRef: 's2', previous: [h1] })
    ).head;

    // Same Db, same chain instance is fine — what matters is that `ha`/`hb`
    // each extend `h2` independently, which is exactly what `relate` has to
    // tell apart.
    const ha = (
      await root.append(collection, { snapshotRef: 'sa', previous: [h2] })
    ).head;
    const hb = (
      await root.append(collection, { snapshotRef: 'sb', previous: [h2] })
    ).head;

    return { h0, h1, h2, ha, hb, chain: root };
  };

  it('answers same for one ref related to itself, without a walk', async () => {
    const { h1, chain } = await mkFork();
    expect(await chain.relate(collection, h1, h1)).toEqual({ verdict: 'same' });
  });

  it('answers behind/ahead for a descent found on either side', async () => {
    const { h1, ha, chain } = await mkFork();
    expect(await chain.relate(collection, h1, ha)).toEqual({ verdict: 'behind' });
    expect(await chain.relate(collection, ha, h1)).toEqual({ verdict: 'ahead' });
  });

  it('answers fork, with the nearest common ancestor as base, for two genuinely concurrent refs', async () => {
    const { h2, ha, hb, chain } = await mkFork();
    expect(await chain.relate(collection, ha, hb)).toEqual({
      verdict: 'fork',
      base: h2,
    });
    expect(await chain.relate(collection, hb, ha)).toEqual({
      verdict: 'fork',
      base: h2,
    });
  });

  it('answers incomplete when a row cannot be resolved and no descent was found either way', async () => {
    const { hb, chain } = await mkFork();
    expect(
      await chain.relate(collection, 'NOT_IN_THIS_STORE', hb),
    ).toEqual({ verdict: 'incomplete' });
  });

  it('answers fork (never destructive) once the walk budget runs out before a real descent is found', async () => {
    const { h1, ha, chain } = await mkFork();
    expect(await chain.relate(collection, h1, ha, 1)).toEqual({
      verdict: 'fork',
      base: undefined,
    });
  });

  it('dedupes a ref reached twice within one walk, through a true multi-parent merge entry', async () => {
    // `previous` is `string[]` precisely to allow a merge entry with more
    // than one parent — nothing in `append()` restricts it to one, even
    // though an ordinary edit never passes more than one. The walk has to
    // handle a real one without re-fetching (or re-counting against the
    // budget) a ref its OWN earlier level already resolved.
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    const h0 = (await chain.append(collection, { snapshotRef: 's0', previous: [] }))
      .head;
    const h1 = (
      await chain.append(collection, { snapshotRef: 's1', previous: [h0] })
    ).head;
    // Both h1 AND its own parent h0 as direct parents: walking up from h1
    // reaches h0 too, so the second branch re-meets a ref the first branch
    // already resolved.
    const merge = (
      await chain.append(collection, { snapshotRef: 'merge', previous: [h1, h0] })
    ).head;

    expect(await chain.relate(collection, merge, h0)).toEqual({
      verdict: 'ahead',
    });
  });

  it('falls back to an empty parent list for a common ancestor the budget cut off before resolving', async () => {
    // Budget 3 per side resolves {ha: [h2], h2: [h1], h1: [h0]} without ever
    // fetching h0 itself — h0 is named as a PARENT but never becomes its own
    // key. The dominated-ancestor walk then asks for h0's own parents while
    // computing the fork base, and must treat "never resolved" the same as
    // "resolved with none" rather than throwing.
    const { h2, ha, hb, chain } = await mkFork();
    expect(await chain.relate(collection, ha, hb, 3)).toEqual({
      verdict: 'fork',
      base: h2,
    });
  });
});

// .............................................................................

describe('MongoEditChain.ownLineage', () => {
  const prefix = 'testDb';
  const collection = 'customers';

  it('answers an empty set for no head', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    expect(await chain.ownLineage(collection, undefined)).toEqual(new Set());
  });

  it('answers the full lineage and caches it on the head it was computed for', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    const h0 = (await chain.append(collection, { snapshotRef: 's0', previous: [] }))
      .head;
    const h1 = (
      await chain.append(collection, { snapshotRef: 's1', previous: [h0] })
    ).head;

    const lineage = await chain.ownLineage(collection, h1);
    expect(lineage).toEqual(new Set([h1, h0]));
    // Cached: a second call for the SAME head returns the same (cached) set.
    expect(await chain.ownLineage(collection, h1)).toBe(lineage);
  });

  it('extends a cached lineage in place when append() builds straight onto it', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    const h0 = (await chain.append(collection, { snapshotRef: 's0', previous: [] }))
      .head;
    const h1 = (
      await chain.append(collection, { snapshotRef: 's1', previous: [h0] })
    ).head;
    // Primes the cache on h1 — append()'s own cache-extend path only fires
    // when a PRIOR lineage is already cached.
    await chain.ownLineage(collection, h1);

    const h2 = (
      await chain.append(collection, { snapshotRef: 's2', previous: [h1] })
    ).head;
    const lineage = await chain.ownLineage(collection, h2);
    expect(lineage).toEqual(new Set([h2, h1, h0]));
  });

  it('answers undefined when a row in the lineage cannot be resolved', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    expect(
      await chain.ownLineage(collection, 'NOT_IN_THIS_STORE'),
    ).toBeUndefined();
  });

  it('dedupes a ref reached twice within one walk, through a true multi-parent merge entry', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    const h0 = (await chain.append(collection, { snapshotRef: 's0', previous: [] }))
      .head;
    const h1 = (
      await chain.append(collection, { snapshotRef: 's1', previous: [h0] })
    ).head;
    const merge = (
      await chain.append(collection, { snapshotRef: 'merge', previous: [h1, h0] })
    ).head;

    expect(await chain.ownLineage(collection, merge)).toEqual(
      new Set([merge, h1, h0]),
    );
  });

  it('stops at the walk budget, bounded rather than unbounded', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    let head = (await chain.append(collection, { snapshotRef: 's0', previous: [] }))
      .head;
    for (let i = 1; i <= 5; i++) {
      head = (
        await chain.append(collection, { snapshotRef: `s${i}`, previous: [head] })
      ).head;
    }

    // 6 entries total (s0..s5); a budget of 2 must not walk all of them.
    const lineage = await chain.ownLineage(collection, head, 2);
    expect(lineage).toBeDefined();
    expect(lineage?.size).toBeLessThan(6);
  });
});
