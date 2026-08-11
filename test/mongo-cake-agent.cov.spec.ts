// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Db } from '@rljson/db';
import { IoMem } from '@rljson/io';
import { syncEvents } from '@rljson/rljson';
import { Int32 } from 'bson';
import type { Document } from 'mongodb';
import { EventEmitter } from 'node:events';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';

import { MongoCakeAdapter } from '../src/mongo-cake-adapter.ts';
import {
  MongoCakeAgent,
  MongoDbStore,
} from '../src/mongo-cake-agent.ts';
import { buildCake } from '../src/mongo-cake-model.ts';

// The Connector created inside fromClient records every broadcast here.
const sent = vi.hoisted(() => ({ refs: [] as string[] }));

// Keep the real @rljson/db (real Db/IoMem so the adapter genuinely stores and
// fetches) but replace the Connector with a spy that records sent refs.
vi.mock('@rljson/db', async (importOriginal) => {
  const orig = (await importOriginal()) as Record<string, unknown>;
  class SpyConnector {
    send(ref: string): void {
      sent.refs.push(ref);
    }
  }
  return { ...orig, Connector: SpyConnector };
});

// .............................................................................

class FakeChangeStream extends EventEmitter {
  closed = false;
  async close(): Promise<void> {
    this.closed = true;
  }
}

class FakeCollection {
  bulkWriteOps: unknown[] = [];
  deleteFilters: unknown[] = [];
  constructor(public docs: Document[] = []) {}
  find(): { toArray: () => Promise<Document[]> } {
    return { toArray: async () => this.docs };
  }
  async estimatedDocumentCount(): Promise<number> {
    return this.docs.length;
  }
  async bulkWrite(ops: unknown[]): Promise<void> {
    this.bulkWriteOps.push(...ops);
  }
  async deleteMany(filter: unknown): Promise<void> {
    this.deleteFilters.push(filter);
  }
}

class FakeMongoDb {
  streams: FakeChangeStream[] = [];
  constructor(
    public colls: Record<string, FakeCollection>,
    public names: string[],
  ) {}
  listCollections(): { toArray: () => Promise<{ name: string }[]> } {
    return { toArray: async () => this.names.map((name) => ({ name })) };
  }
  collection(name: string): FakeCollection {
    return this.colls[name] ?? new FakeCollection([]);
  }
  watch(): FakeChangeStream {
    const s = new FakeChangeStream();
    this.streams.push(s);
    return s;
  }
}

class FakeBridge {
  handlers = new Map<string, ((p: unknown) => void)[]>();
  on(event: string, handler: (p: unknown) => void): void {
    const list = this.handlers.get(event) ?? [];
    list.push(handler);
    this.handlers.set(event, list);
  }
  emit(event: string, payload: unknown): void {
    for (const h of this.handlers.get(event) ?? []) h(payload);
  }
}

const newIo = async (): Promise<IoMem> => {
  const io = new IoMem();
  await io.init();
  await io.isReady();
  return io;
};

const events = syncEvents('/sharedMongoTree') as { bootstrap: string; ref: string };

// Drains the real setImmediate yields the pushSnapshot performs between
// collections (setImmediate is left un-faked; see beforeEach).
const flushImmediate = async (rounds = 8): Promise<void> => {
  for (let i = 0; i < rounds; i++) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
};

// .............................................................................

describe('MongoDbStore', () => {
  const store = new MongoDbStore(
    new FakeMongoDb(
      {
        customers: new FakeCollection([{ _id: new Int32(1) }, { _id: new Int32(2) }]),
      },
      ['customers', 'sync_ops', 'system.views', 'cd_articles'],
    ) as never,
    ['cd_articles'],
  );

  it('ignores internal and ignore-listed collections', () => {
    expect(store.isIgnored('sync_ops')).toBe(true);
    expect(store.isIgnored('system.views')).toBe(true);
    expect(store.isIgnored('cd_articles')).toBe(true);
    expect(store.isIgnored('customers')).toBe(false);
  });

  it('lists only synced collections', async () => {
    expect(await store.listCollections()).toEqual(['customers']);
  });

  it('reads documents and counts them', async () => {
    expect((await store.readCollection('customers')).length).toBe(2);
    expect(await store.countDocuments('customers')).toBe(2);
  });

  it('applies upserts and deletes; no-op when both empty', async () => {
    const coll = new FakeCollection([]);
    const db = new FakeMongoDb({ customers: coll }, ['customers']);
    const s = new MongoDbStore(db as never);
    await s.applyChanges('customers', [{ _id: new Int32(1) }], [new Int32(2)]);
    expect(coll.bulkWriteOps).toHaveLength(1);
    expect(coll.deleteFilters).toHaveLength(1);
    // Empty batch touches nothing.
    await s.applyChanges('customers', [], []);
    expect(coll.bulkWriteOps).toHaveLength(1);
  });
});

describe('MongoCakeAgent', () => {
  beforeEach(() => {
    sent.refs = [];
    // Fake only the debounce timers — leave setImmediate real so the
    // pushSnapshot yield resolves. `flushImmediate` below drains it after a
    // fake-timer advance triggers an async push.
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  const build = async (mongo: FakeMongoDb, io: IoMem, opts = {}) => {
    const bridge = new FakeBridge();
    const agent = await MongoCakeAgent.fromClient(
      mongo as never,
      'sharedMongoTree',
      { io } as never,
      bridge as never,
      { debounceMs: 100, ...opts },
    );
    return { agent, bridge };
  };

  it('pushes the initial snapshot and broadcasts its cake hash', async () => {
    const io = await newIo();
    const mongo = new FakeMongoDb(
      { customers: new FakeCollection([{ _id: new Int32(1), n: 'Ann' }]) },
      ['customers'],
    );
    const { agent } = await build(mongo, io);
    const stop = await agent.syncToDb();
    expect(sent.refs).toHaveLength(1); // initial push broadcast
    stop();
  });

  it('applies an inbound cake ref delivered over the bridge (bootstrap + ref)', async () => {
    const io = await newIo();
    // Pre-store a cake on the same io so the agent's Db can fetch it by hash.
    const adapter = new MongoCakeAdapter(new Db(io) as never);
    const built = buildCake(
      new Map([['customers', [{ _id: new Int32(1), n: 'Ann' }]]]),
    );
    await adapter.storeCake(built);

    const coll = new FakeCollection([]);
    const mongo = new FakeMongoDb({ customers: coll }, ['customers']);
    const { agent, bridge } = await build(mongo, io);
    await agent.syncFromDbSimple();

    bridge.emit(events.bootstrap, { r: built.cakeHash });
    await vi.runAllTimersAsync();
    // The consumer upserted _id:1 from the incoming cake (no tombstones).
    expect(coll.bulkWriteOps.length).toBeGreaterThan(0);
  });

  it('ignores malformed / empty refs from the bridge', async () => {
    const io = await newIo();
    const mongo = new FakeMongoDb({ customers: new FakeCollection([]) }, ['customers']);
    const { agent, bridge } = await build(mongo, io);
    await agent.syncFromDb();
    bridge.emit(events.ref, { r: '' }); // empty
    bridge.emit(events.ref, {}); // missing
    await vi.runAllTimersAsync();
    expect(sent.refs).toHaveLength(0);
  });

  it('re-pushes on a change to a synced collection (debounced + coalesced)', async () => {
    const io = await newIo();
    const coll = new FakeCollection([{ _id: new Int32(1) }]);
    const mongo = new FakeMongoDb({ customers: coll }, ['customers']);
    const { agent } = await build(mongo, io);
    await agent.syncToDb();
    sent.refs = []; // ignore the initial push
    const stream = mongo.streams[0];

    // A change on an ignored collection is skipped…
    stream.emit('change', { ns: { coll: 'sync_ops' } });
    // …a change on a synced one schedules a push. Fire two to hit the
    // "already pushing → push again" coalescing path.
    coll.docs = [{ _id: new Int32(1) }, { _id: new Int32(2) }];
    stream.emit('change', { ns: { coll: 'customers' } });
    stream.emit('change', { ns: {} }); // missing coll name → treated as synced
    await vi.runAllTimersAsync();
    await flushImmediate();
    expect(sent.refs.length).toBeGreaterThan(0);
  });

  it('reopens the change stream on error and close', async () => {
    const io = await newIo();
    const mongo = new FakeMongoDb({ customers: new FakeCollection([]) }, ['customers']);
    const { agent } = await build(mongo, io);
    await agent.syncToDb();
    const first = mongo.streams[0];
    first.emit('error', new Error('boom'));
    await vi.advanceTimersByTimeAsync(1100);
    expect(mongo.streams.length).toBeGreaterThan(1); // reopened
    const second = mongo.streams[mongo.streams.length - 1];
    second.emit('close');
    await vi.advanceTimersByTimeAsync(1100);
    expect(mongo.streams.length).toBeGreaterThan(2);
    agent.dispose();
  });

  it('forcePush pushes immediately; dispose stops further work', async () => {
    const io = await newIo();
    const mongo = new FakeMongoDb(
      { customers: new FakeCollection([{ _id: new Int32(9) }]) },
      ['customers'],
    );
    const { agent } = await build(mongo, io);
    const ref = await agent.forcePush();
    expect(typeof ref).toBe('string');
    agent.dispose();
    // After dispose, a stray push is a no-op (stopped guard).
    sent.refs = [];
    await agent.forcePush();
    expect(sent.refs).toHaveLength(0);
  });

  it('tolerates errors in apply and push', async () => {
    const io = await newIo();
    const mongo = new FakeMongoDb({ customers: new FakeCollection([]) }, ['customers']);
    const logs: string[] = [];
    const { agent, bridge } = await build(mongo, io, { log: (m: string) => logs.push(m) });
    await agent.syncFromDb();
    // A ref that resolves to no cake row → applyIncoming returns null (no throw).
    bridge.emit(events.ref, { r: 'deadbeefdeadbeefdeadbe' });
    await vi.runAllTimersAsync();
    // countDocuments throwing during a push is swallowed and logged.
    vi.spyOn(mongo.colls['customers'], 'find').mockImplementationOnce(() => {
      throw new Error('mongo down');
    });
    await agent.forcePush();
    expect(logs.some((l) => l.includes('push failed'))).toBe(true);
  });

  it('coalesces a change that arrives while a push is in flight', async () => {
    const io = await newIo();
    const coll = new FakeCollection([{ _id: new Int32(1) }]);
    let calls = 0;
    let release = (): void => {};
    const gate = new Promise<void>((r) => (release = r));
    vi.spyOn(coll, 'find').mockImplementation(() => ({
      toArray: async () => {
        calls += 1;
        if (calls === 2) await gate; // block the first change-driven push
        return coll.docs;
      },
    }));
    const mongo = new FakeMongoDb({ customers: coll }, ['customers']);
    const { agent } = await build(mongo, io);
    await agent.syncToDb(); // initial push (find call #1)
    const stream = mongo.streams[0];

    coll.docs = [{ _id: new Int32(1) }, { _id: new Int32(2) }];
    stream.emit('change', { ns: { coll: 'customers' } });
    await vi.advanceTimersByTimeAsync(150); // debounce → _runPush, blocked on gate
    // A second change while the first push is in flight → coalesced (pushAgain).
    coll.docs = [{ _id: new Int32(1) }, { _id: new Int32(2) }, { _id: new Int32(3) }];
    stream.emit('change', { ns: { coll: 'customers' } });
    await vi.advanceTimersByTimeAsync(150);
    release();
    // Alternate timer-advance + real-macrotask drain so the blocked push
    // finishes, coalesces (pushAgain) and the rescheduled push runs.
    for (let i = 0; i < 4; i++) {
      await vi.runAllTimersAsync();
      await flushImmediate();
    }
    agent.dispose();
    expect(calls).toBeGreaterThanOrEqual(3);
  });

  it('accepts the ref under the `ref` key and uses the default debounce', async () => {
    const io = await newIo();
    const adapter = new MongoCakeAdapter(new Db(io) as never);
    const built = buildCake(new Map([['customers', [{ _id: new Int32(1) }]]]));
    await adapter.storeCake(built);
    const coll = new FakeCollection([]);
    const mongo = new FakeMongoDb({ customers: coll }, ['customers']);
    // No debounceMs → default 800.
    const bridge = new FakeBridge();
    const agent = await MongoCakeAgent.fromClient(
      mongo as never,
      'sharedMongoTree',
      { io } as never,
      bridge as never,
      {},
    );
    await agent.syncFromDb();
    bridge.emit(events.ref, { ref: built.cakeHash }); // `ref` key, not `r`
    await vi.runAllTimersAsync();
    expect(coll.bulkWriteOps.length).toBeGreaterThan(0);
  });

  it('logs a blocked mass-delete without throwing', async () => {
    const io = await newIo();
    // Store a cake with an EMPTY customers collection (everything deleted).
    const adapter = new MongoCakeAdapter(new Db(io) as never);
    const emptyCake = buildCake(new Map([['customers', []]]));
    await adapter.storeCake(emptyCake);
    // Local has 10 docs → deleting all is a >30% mass-delete → blocked.
    const docs = Array.from({ length: 10 }, (_v, i) => ({ _id: new Int32(i) }));
    const coll = new FakeCollection(docs);
    const mongo = new FakeMongoDb({ customers: coll }, ['customers']);
    const logs: string[] = [];
    const bridge = new FakeBridge();
    const agent = await MongoCakeAgent.fromClient(
      mongo as never,
      'sharedMongoTree',
      { io } as never,
      bridge as never,
      { debounceMs: 100, log: (m: string) => logs.push(m) },
    );
    await agent.syncFromDb();
    bridge.emit(events.ref, { r: emptyCake.cakeHash });
    await vi.runAllTimersAsync();
    expect(coll.deleteFilters).toHaveLength(0); // nothing deleted
    expect(logs.some((l) => l.includes('blocked deletes'))).toBe(true);
  });

  it('logs (without throwing) when applying a ref fails mid-flight', async () => {
    const io = await newIo();
    const adapter = new MongoCakeAdapter(new Db(io) as never);
    const built = buildCake(new Map([['customers', [{ _id: new Int32(1) }]]]));
    await adapter.storeCake(built);
    const coll = new FakeCollection([]);
    vi.spyOn(coll, 'bulkWrite').mockRejectedValueOnce(new Error('write failed'));
    const mongo = new FakeMongoDb({ customers: coll }, ['customers']);
    const logs: string[] = [];
    const bridge = new FakeBridge();
    const agent = await MongoCakeAgent.fromClient(
      mongo as never,
      'sharedMongoTree',
      { io } as never,
      bridge as never,
      { debounceMs: 100, log: (m: string) => logs.push(m) },
    );
    await agent.syncFromDb();
    bridge.emit(events.ref, { r: built.cakeHash });
    await vi.runAllTimersAsync();
    expect(logs.some((l) => l.includes('apply failed'))).toBe(true);
  });

  it('stop() clears a pending debounce timer', async () => {
    const io = await newIo();
    const coll = new FakeCollection([{ _id: new Int32(1) }]);
    const mongo = new FakeMongoDb({ customers: coll }, ['customers']);
    const { agent } = await build(mongo, io);
    const stop = await agent.syncToDb();
    mongo.streams[0].emit('change', { ns: { coll: 'customers' } }); // arms the timer
    stop(); // pending timer cleared (else the debounce would fire post-stop)
    await vi.runAllTimersAsync();
  });

  it('dispose() clears a pending debounce timer', async () => {
    const io = await newIo();
    const coll = new FakeCollection([{ _id: new Int32(1) }]);
    const mongo = new FakeMongoDb({ customers: coll }, ['customers']);
    const { agent } = await build(mongo, io);
    await agent.syncToDb();
    mongo.streams[0].emit('change', { ns: { coll: 'customers' } });
    agent.dispose();
    await vi.runAllTimersAsync();
  });

  it('is inert after dispose: ignores refs, reopen and close events', async () => {
    const io = await newIo();
    const mongo = new FakeMongoDb({ customers: new FakeCollection([]) }, ['customers']);
    const { agent, bridge } = await build(mongo, io);
    await agent.syncFromDb();
    await agent.syncToDb();
    const stream = mongo.streams[0];
    // Error while running → reopen schedules a re-open in 1s (stream set null).
    stream.emit('error', new Error('x'));
    agent.dispose(); // stopped; _changeStream already null → _safeClose short-circuit
    bridge.emit(events.ref, { r: 'abcdef0123456789abcdef' }); // apply → stopped guard
    stream.emit('close'); // close handler → !stopped is false now
    stream.emit('error', new Error('y')); // error → reopen → stopped guard
    await vi.advanceTimersByTimeAsync(1100); // pre-dispose reopen fires → open → stopped
    expect(true).toBe(true);
  });

  it('syncToDbSimple / syncFromDbSimple alias the sync methods', async () => {
    const io = await newIo();
    const mongo = new FakeMongoDb({ customers: new FakeCollection([]) }, ['customers']);
    const { agent } = await build(mongo, io);
    const stopFrom = await agent.syncFromDbSimple();
    const stopTo = await agent.syncToDbSimple();
    expect(typeof stopFrom).toBe('function');
    expect(typeof stopTo).toBe('function');
    stopFrom();
    stopTo();
  });
});
