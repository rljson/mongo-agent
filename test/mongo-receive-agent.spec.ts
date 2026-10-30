// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Db } from '@rljson/db';
import { IoMem } from '@rljson/io';
import { Int32 } from 'bson';
import { describe, expect, it } from 'vitest';

import type { ApplyBehindTarget } from '../src/mongo-apply-behind.ts';
import { MongoEditChain } from '../src/mongo-edit-chain.ts';
import { MongoReceiveAgent } from '../src/mongo-receive-agent.ts';

const mkDb = async (): Promise<{ io: IoMem; db: Db }> => {
  const io = new IoMem();
  await io.init();
  await io.isReady();
  return { io, db: new Db(io) };
};

/** A minimal, real-effect fake of the Mongo driver slice applyBehind writes to. */
class FakeTarget implements ApplyBehindTarget {
  readonly docs = new Map<string, Record<string, unknown>>();

  replaceOne = async (
    filter: { _id: unknown },
    replacement: Record<string, unknown>,
  ): Promise<{ acknowledged: true }> => {
    this.docs.set(String(filter._id), replacement);
    return { acknowledged: true };
  };

  deleteOne = async (filter: { _id: unknown }): Promise<{ acknowledged: true }> => {
    this.docs.delete(String(filter._id));
    return { acknowledged: true };
  };
}

const prefix = 'testDb';
const collection = 'customers';

/** A node: its own chain + agent + fake target, independent of any other node's. */
const mkNode = async (): Promise<{
  chain: MongoEditChain;
  target: FakeTarget;
  agent: MongoReceiveAgent;
  announced: string[];
  blocked: Array<{ head: string; reason: string }>;
}> => {
  const { db } = await mkDb();
  const chain = new MongoEditChain(db, prefix);
  await chain.init(collection);
  const target = new FakeTarget();
  const announced: string[] = [];
  const blocked: Array<{ head: string; reason: string }> = [];
  const agent = new MongoReceiveAgent({
    chain,
    collection,
    target,
    onAnnounce: (head) => announced.push(head),
    onBlocked: (head, reason) => blocked.push({ head, reason }),
  });
  return { chain, target, agent, announced, blocked };
};

describe('MongoReceiveAgent.bootstrap', () => {
  it('has no head before bootstrap', async () => {
    const { agent } = await mkNode();
    expect(agent.ownHead()).toBeUndefined();
  });

  it('bootstraps an empty collection to the all-zero root', async () => {
    const { chain, agent } = await mkNode();
    const head = await agent.bootstrap();
    expect(agent.ownHead()).toBe(head);
    const entry = await chain.entry(collection, head);
    expect(entry?.snapshotRef).toBe('0'.repeat(64));
    expect(entry?.previous).toEqual([]);
  });

  it('two nodes starting from the same baseline content bootstrap to the identical root', async () => {
    const alice = { _id: new Int32(1), name: 'Alice' };
    const a = await mkNode();
    const b = await mkNode();
    const headA = await a.agent.bootstrap(new Map([['1', alice]]));
    const headB = await b.agent.bootstrap(new Map([['1', { ...alice }]]));
    expect(headA).toBe(headB);
  });
});

describe('MongoReceiveAgent — local writes', () => {
  it('does nothing on flushLocal when nothing is pending', async () => {
    const { agent } = await mkNode();
    const head = await agent.bootstrap();
    await agent.flushLocal();
    expect(agent.ownHead()).toBe(head);
  });

  it('flushes a first local change even before bootstrap, with no previous', async () => {
    const { chain, agent } = await mkNode();
    agent.recordChange('1', { _id: new Int32(1), name: 'Alice' });
    await agent.flushLocal();
    const entry = await chain.entry(collection, agent.ownHead() as string);
    expect(entry?.previous).toEqual([]);
  });

  it('batches every recordChange since the last flush into one entry', async () => {
    const { chain, agent } = await mkNode();
    const root = await agent.bootstrap();
    agent.recordChange('1', { _id: new Int32(1), name: 'Alice' });
    agent.recordChange('2', { _id: new Int32(2), name: 'Bob' });
    await agent.flushLocal();
    const head = agent.ownHead() as string;
    const entry = await chain.entry(collection, head);
    expect(entry?.previous).toEqual([root]);
    expect(Object.keys(entry?.changed ?? {}).sort()).toEqual(['1', '2']);
  });

  it('removes a sliceId it previously changed, within the same pending batch', async () => {
    const { chain, agent } = await mkNode();
    await agent.bootstrap();
    agent.recordChange('1', { _id: new Int32(1), name: 'Alice' });
    agent.recordChange('1', undefined);
    await agent.flushLocal();
    const entry = await chain.entry(collection, agent.ownHead() as string);
    expect(entry?.changed).toEqual({});
    expect(entry?.removed).toEqual(['1']);
  });

  it('updating then removing a document ends with the all-zero root again', async () => {
    const { chain, agent } = await mkNode();
    const root = await agent.bootstrap();
    agent.recordChange('1', { _id: new Int32(1), name: 'Alice' });
    await agent.flushLocal();
    agent.recordChange('1', undefined);
    await agent.flushLocal();
    const entry = await chain.entry(collection, agent.ownHead() as string);
    expect(entry?.snapshotRef).toBe('0'.repeat(64));
    expect(entry?.previous).not.toEqual([root]); // it is the entry after the one that added Alice
  });
});

describe('MongoReceiveAgent.receiveHead', () => {
  it('applies a behind head, writes it to the target, and adopts it as own', async () => {
    const sender = await mkNode();
    await sender.agent.bootstrap();
    sender.agent.recordChange('1', { _id: new Int32(1), name: 'Alice' });
    await sender.agent.flushLocal();
    const senderHead = sender.agent.ownHead() as string;

    // The receiver shares the sender's CHAIN (same store, as two nodes
    // reading/writing the same underlying db would), but has its own agent
    // state and its own target collection.
    const receiver = new MongoReceiveAgent({
      chain: sender.chain,
      collection,
      target: new FakeTarget(),
      onAnnounce: () => {},
    });
    await receiver.bootstrap();

    const outcome = await receiver.receiveHead(senderHead);
    expect(outcome).toBe('applied');
    expect(receiver.ownHead()).toBe(senderHead);
  });

  it('absorbs a removed sliceId from a replayed entry too, not just changed ones', async () => {
    const sender = await mkNode();
    await sender.agent.bootstrap();
    sender.agent.recordChange('1', { _id: new Int32(1), name: 'Alice' });
    await sender.agent.flushLocal();
    sender.agent.recordChange('1', undefined);
    await sender.agent.flushLocal();
    const finalHead = sender.agent.ownHead() as string;

    const receiver = new MongoReceiveAgent({
      chain: sender.chain,
      collection,
      target: new FakeTarget(),
      onAnnounce: () => {},
    });
    await receiver.bootstrap();
    expect(await receiver.receiveHead(finalHead)).toBe('applied');

    // The receiver's manifest must agree '1' is gone, not stuck on the
    // digest `recordChange` folded in BEFORE this apply overwrote it with
    // the sender's removal — proven by re-adding the identical document and
    // comparing against a node that only ever added it once, cleanly: a
    // stale digest would double-fold and land on a different root.
    receiver.recordChange('1', { _id: new Int32(1), name: 'Alice' });
    await receiver.flushLocal();
    const receiverEntry = await sender.chain.entry(collection, receiver.ownHead() as string);

    const clean = await mkNode();
    await clean.agent.bootstrap();
    clean.agent.recordChange('1', { _id: new Int32(1), name: 'Alice' });
    await clean.agent.flushLocal();
    const cleanEntry = await clean.chain.entry(collection, clean.agent.ownHead() as string);

    expect(receiverEntry?.snapshotRef).toBe(cleanEntry?.snapshotRef);
  });

  it('is in sync with its own head, after flushing pending local work first', async () => {
    const { agent } = await mkNode();
    const head = await agent.bootstrap();
    expect(await agent.receiveHead(head)).toBe('in-sync');
  });

  it('announces its own head to a sender that is behind it', async () => {
    const { agent, announced } = await mkNode();
    const root = await agent.bootstrap();
    agent.recordChange('1', { _id: new Int32(1), name: 'Alice' });
    await agent.flushLocal();
    const ahead = agent.ownHead() as string;

    expect(await agent.receiveHead(root)).toBe('announced');
    expect(announced).toEqual([ahead]);
  });

  it('converges a fork that turns out to be one state under two names, from both directions', async () => {
    // Three entries naming the IDENTICAL resulting content off the same
    // root, differing only in a literal (never minted) timeId — so their
    // relative order is fixed across every run, not left to `timeId()`'s
    // own minting. Positioning each test agent on one of them goes through
    // the real public API too: bootstrap to the root, then a real
    // `behind`-apply onto the chosen child.
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    const alice = { _id: new Int32(1), name: 'Alice' };
    const seed = new MongoReceiveAgent({
      chain,
      collection,
      target: new FakeTarget(),
      onAnnounce: () => {},
    });
    const root = await seed.bootstrap();

    const built = await Promise.all(
      ['t1', 't2', 't3'].map((timeId) =>
        chain.append(collection, {
          snapshotRef: 'alice-only',
          previous: [root],
          changed: new Map([['1', alice]]),
          timeId,
        }),
      ),
    );
    const [smaller, , bigger] = [...built].sort((a, b) =>
      a.head.localeCompare(b.head),
    );
    expect(smaller.head).not.toBe(bigger.head);

    const positioned = async (at: string) => {
      const target = new FakeTarget();
      const announced: string[] = [];
      const agent = new MongoReceiveAgent({
        chain,
        collection,
        target,
        onAnnounce: (head) => announced.push(head),
      });
      await agent.bootstrap();
      expect(await agent.receiveHead(at)).toBe('applied');
      return { agent, target, announced };
    };

    const onBigger = await positioned(bigger.head);
    expect(await onBigger.agent.receiveHead(smaller.head)).toBe('adopted');
    expect(onBigger.agent.ownHead()).toBe(smaller.head);
    expect(onBigger.target.docs.size).toBe(1); // only the earlier behind-apply wrote Alice

    const onSmaller = await positioned(smaller.head);
    expect(await onSmaller.agent.receiveHead(bigger.head)).toBe('announced');
    expect(onSmaller.agent.ownHead()).toBe(smaller.head);
    expect(onSmaller.announced).toEqual([smaller.head]);
  });

  it('defers a genuine fork to a later ticket (merge3)', async () => {
    const { chain, agent } = await mkNode();
    const root = await agent.bootstrap();
    agent.recordChange('1', { _id: new Int32(1), name: 'Alice' });
    await agent.flushLocal();
    const ownHead = agent.ownHead() as string;

    // A peer's entry, forked from the same root with a genuinely different
    // change.
    const theirs = (
      await chain.append(collection, {
        snapshotRef: 'peer-snap',
        previous: [root],
        changed: new Map([['2', { _id: new Int32(2), name: 'Bob' }]]),
      })
    ).head;

    expect(await agent.receiveHead(theirs)).toBe('deferred');
    expect(agent.ownHead()).toBe(ownHead);
  });

  it('reports a blocked head it cannot decide anything about', async () => {
    const { agent, blocked } = await mkNode();
    await agent.bootstrap();
    const outcome = await agent.receiveHead('NOT_IN_THIS_STORE');
    expect(outcome).toBe('blocked');
    expect(blocked).toHaveLength(1);
    expect(blocked[0]?.head).toBe('NOT_IN_THIS_STORE');
  });

  it('defaults to a silent no-op when no onBlocked was given', async () => {
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    const agent = new MongoReceiveAgent({
      chain,
      collection,
      target: new FakeTarget(),
      onAnnounce: () => {},
    });
    await agent.bootstrap();
    // No onBlocked configured — this must not throw.
    await expect(agent.receiveHead('NOT_IN_THIS_STORE')).resolves.toBe('blocked');
  });

  it('propagates refused from applyBehind without touching its own head or manifest', async () => {
    // A merge entry whose other parent is TWO hops away: with a walk budget
    // of 2, `relate` still proves `behind` (its budget check only gates
    // between batches, so the batch that discovers `root` as an ancestor is
    // allowed to overshoot it slightly) but `path`'s stricter topological
    // close — every entry fully resolved and placed, not just one matching
    // key found — runs out before the two-hop side branch resolves, and
    // refuses rather than apply a partial replay. Same budget-cutoff shape
    // as ticket 3's own test, now observed through the agent.
    const { db } = await mkDb();
    const chain = new MongoEditChain(db, prefix);
    await chain.init(collection);
    const target = new FakeTarget();
    const agent = new MongoReceiveAgent({
      chain,
      collection,
      target,
      onAnnounce: () => {},
      maxWalk: 2,
    });
    const root = await agent.bootstrap();
    const other0 = await chain.append(collection, { snapshotRef: 'o0', previous: [] });
    const other1 = await chain.append(collection, {
      snapshotRef: 'o1',
      previous: [other0.head],
    });
    const merge = await chain.append(collection, {
      snapshotRef: 'merge-snap',
      previous: [root, other1.head],
    });

    expect(await agent.receiveHead(merge.head)).toBe('refused');
    expect(agent.ownHead()).toBe(root);
    expect(target.docs.size).toBe(0);
  });

  it('keeps its own manifest correct across an apply, so a later local edit folds the right old digest out', async () => {
    const sender = await mkNode();
    await sender.agent.bootstrap();
    sender.agent.recordChange('1', { _id: new Int32(1), name: 'Alice' });
    await sender.agent.flushLocal();
    const firstHead = sender.agent.ownHead() as string;
    sender.agent.recordChange('1', { _id: new Int32(1), name: 'Alice II' });
    await sender.agent.flushLocal();
    const secondHead = sender.agent.ownHead() as string;

    const receiver = new MongoReceiveAgent({
      chain: sender.chain,
      collection,
      target: new FakeTarget(),
      onAnnounce: () => {},
    });
    await receiver.bootstrap();
    expect(await receiver.receiveHead(firstHead)).toBe('applied');
    expect(await receiver.receiveHead(secondHead)).toBe('applied');

    // A local edit to '1' now must fold out "Alice II"'s digest, not a stale
    // one — proven by the local root matching the sender's own root exactly
    // once the receiver makes (then undoes) the identical edit.
    receiver.recordChange('1', { _id: new Int32(1), name: 'changed locally' });
    receiver.recordChange('1', { _id: new Int32(1), name: 'Alice II' });
    await receiver.flushLocal();
    const receiverEntry = await sender.chain.entry(collection, receiver.ownHead() as string);
    const senderEntry = await sender.chain.entry(collection, secondHead);
    expect(receiverEntry?.snapshotRef).toBe(senderEntry?.snapshotRef);
  });
});
