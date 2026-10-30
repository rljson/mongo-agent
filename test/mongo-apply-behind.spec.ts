// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Db } from '@rljson/db';
import { IoMem } from '@rljson/io';
import { Int32, ObjectId } from 'bson';
import { describe, expect, it } from 'vitest';

import {
  applyBehind,
  typedIdCandidates,
  type ApplyBehindTarget,
} from '../src/mongo-apply-behind.ts';
import { MongoEditChain } from '../src/mongo-edit-chain.ts';

const mkDb = async (): Promise<{ io: IoMem; db: Db }> => {
  const io = new IoMem();
  await io.init();
  await io.isReady();
  return { io, db: new Db(io) };
};

/** A minimal, real-effect fake of the Mongo driver slice `applyBehind` writes to. */
class FakeTarget implements ApplyBehindTarget {
  readonly docs = new Map<string, Record<string, unknown>>();
  readonly calls: string[] = [];

  replaceOne = async (
    filter: { _id: unknown },
    replacement: Record<string, unknown>,
  ): Promise<{ acknowledged: true }> => {
    this.calls.push(`replace ${String(filter._id)}`);
    this.docs.set(String(filter._id), replacement);
    return { acknowledged: true };
  };

  deleteOne = async (filter: { _id: unknown }): Promise<{ acknowledged: true }> => {
    this.calls.push(`delete ${String(filter._id)}`);
    this.docs.delete(String(filter._id));
    return { acknowledged: true };
  };
}

describe('typedIdCandidates', () => {
  it('always includes the string form', () => {
    expect(typedIdCandidates('alice')).toEqual(['alice']);
  });

  it('adds the ObjectId form for a 24-hex string', () => {
    const hex = '507f1f77bcf86cd799439011';
    const candidates = typedIdCandidates(hex);
    expect(candidates).toContainEqual(new ObjectId(hex));
    expect(candidates).toContain(hex);
  });

  it('adds the numeric form for an all-digit string', () => {
    expect(typedIdCandidates('42')).toEqual(['42', 42]);
  });

  it('adds the numeric form for a negative all-digit string', () => {
    expect(typedIdCandidates('-7')).toEqual(['-7', -7]);
  });
});

describe('applyBehind', () => {
  const prefix = 'testDb';
  const collection = 'customers';

  it('replays a linear chain: writes what changed, deletes what was removed, in order', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);

    const alice = { _id: new Int32(1), name: 'Alice' };
    const bob = { _id: new Int32(2), name: 'Bob' };

    const root = await chain.append(collection, { snapshotRef: 's0', previous: [] });
    const withAlice = await chain.append(collection, {
      snapshotRef: 's1',
      previous: [root.head],
      changed: new Map([['1', alice]]),
    });
    const withBobNoAlice = await chain.append(collection, {
      snapshotRef: 's2',
      previous: [withAlice.head],
      changed: new Map([['2', bob]]),
      removed: ['1'],
    });

    const target = new FakeTarget();
    const outcome = await applyBehind(
      chain,
      collection,
      target,
      root.head,
      withBobNoAlice.head,
    );

    expect(outcome).toBe('applied');
    // Alice was written, then removed again — the collection ends up with
    // only Bob, and the calls happened in chain order.
    expect([...target.docs.keys()]).toEqual(['2']);
    // '1' is all-digit, so its typed candidates are the string AND the
    // number — two idempotent delete calls, same as `_typedIdCandidates`.
    expect(target.calls).toEqual(['replace 1', 'replace 2', 'delete 1', 'delete 1']);
  });

  it('refuses, writing nothing more, when the path cannot be resolved', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    const h0 = (await chain.append(collection, { snapshotRef: 's0', previous: [] }))
      .head;
    const ha = (
      await chain.append(collection, { snapshotRef: 'sa', previous: [h0] })
    ).head;
    const hb = (
      await chain.append(collection, { snapshotRef: 'sb', previous: [h0] })
    ).head;

    const target = new FakeTarget();
    expect(await applyBehind(chain, collection, target, ha, hb)).toBe('refused');
    expect(target.calls).toEqual([]);
  });

  it('refuses when a changed entry names a component this node cannot read', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    const root = await chain.append(collection, { snapshotRef: 's0', previous: [] });
    const broken = await chain.append(collection, {
      snapshotRef: 's1',
      previous: [root.head],
      changed: new Map([['1', { _id: new Int32(1), name: 'Alice' }]]),
    });

    // The real chain resolves the path fine; only the component read fails —
    // a real-world case being a node whose component store lost a row a
    // peer's entry still names. Stubbed here rather than reproduced, since
    // the public API gives no way to write a dangling hash.
    const path = await chain.path(collection, root.head, broken.head);
    const stub = {
      path: async () => path,
      getComponent: async () => undefined,
    } as unknown as MongoEditChain;

    const target = new FakeTarget();
    expect(
      await applyBehind(stub, collection, target, root.head, broken.head),
    ).toBe('refused');
    expect(target.calls).toEqual([]);
  });

  it('deletes by every typed candidate for a removed id', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    const hex = '507f1f77bcf86cd799439011';
    const root = await chain.append(collection, { snapshotRef: 's0', previous: [] });
    const withDoc = await chain.append(collection, {
      snapshotRef: 's1',
      previous: [root.head],
      changed: new Map([[hex, { _id: hex, name: 'Alice' }]]),
    });
    const withoutDoc = await chain.append(collection, {
      snapshotRef: 's2',
      previous: [withDoc.head],
      removed: [hex],
    });

    const target = new FakeTarget();
    await applyBehind(chain, collection, target, root.head, withoutDoc.head);
    // One delete call per typed candidate (string + ObjectId) for the hex id.
    expect(target.calls.filter((c) => c.startsWith('delete'))).toHaveLength(2);
  });
});
