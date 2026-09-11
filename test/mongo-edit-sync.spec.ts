// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Db } from '@rljson/db';
import { IoMem } from '@rljson/io';
import { Int32 } from 'bson';
import { createHash } from 'node:crypto';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { EditSyncConnector } from '../src/mongo-edit-sync.ts';
import { MongoEditSync } from '../src/mongo-edit-sync.ts';

// ---------------------------------------------------------------------------
// A minimal in-memory fake of the slice of the MongoDB driver MongoEditSync
// uses: collection(name) → { find(), watch(), replaceOne() }. The change stream
// is a hand-driven emitter so a test can inject change events at will.
// ---------------------------------------------------------------------------
class FakeChangeStream {
  private _handlers = new Map<string, (c: unknown) => void>();
  close = vi.fn(async () => {});
  on(ev: string, cb: (c: unknown) => void): this {
    this._handlers.set(ev, cb);
    return this;
  }
  emit(change: unknown): void {
    this._handlers.get('change')?.(change);
  }
  emitError(err: unknown): void {
    this._handlers.get('error')?.(err);
  }
}

class FakeCollection {
  stream = new FakeChangeStream();
  /** Every cursor `watch()` has ever returned, oldest first (`stream` is the latest). */
  readonly streams: FakeChangeStream[] = [this.stream];
  /** How many times `watch()` was called (1 after the initial adopt). */
  watchCalls = 0;
  replaceOne = vi.fn(async () => ({}));
  deleteOne = vi.fn(async () => ({}));
  deleteMany = vi.fn(async (filter: Record<string, unknown>) => {
    const before = this.docs.length;
    this.docs = this.docs.filter(
      (d) => !Object.entries(filter).every(([k, v]) => d[k] === v),
    );
    return { deletedCount: before - this.docs.length };
  });
  updateOne = vi.fn(
    async (
      filter: Record<string, unknown>,
      update: { $set?: Record<string, unknown> },
      opts?: { upsert?: boolean },
    ) => {
      const found = this.docs.find((d) =>
        Object.entries(filter).every(([k, v]) => d[k] === v),
      );
      if (found) {
        Object.assign(found, update.$set ?? {});
      } else if (opts?.upsert) {
        this.docs.push({ ...filter, ...(update.$set ?? {}) });
      }
      return {};
    },
  );
  /** The options passed to the most recent `watch()` (to assert `resumeAfter`). */
  watchOpts: Record<string, unknown> | undefined;
  /** How many times `find()` was called (0 ⇒ no full snapshot scan). */
  findCalls = 0;
  constructor(
    public docs: Array<Record<string, unknown>> = [],
    /** Optional hook run as each snapshot doc is yielded (to inject races). */
    public onYield?: () => void,
  ) {}
  find(): AsyncIterable<Record<string, unknown>> {
    this.findCalls++;
    const docs = this.docs;
    const onYield = this.onYield;
    return {
      async *[Symbol.asyncIterator]() {
        for (const d of docs) {
          onYield?.();
          yield d;
        }
      },
    };
  }
  /**
   * A fresh cursor object per call, mirroring the real driver: the cursor
   * MongoDB invalidates after a drop/rename is a dead object, not one that
   * comes back to life — only a NEW `watch()` call yields a live one.
   * `this.stream` always aliases the most recent, so existing single-watch
   * tests (`cols.x.stream.emit(...)`) keep working unchanged.
   */
  watch(_pipeline: unknown, opts?: Record<string, unknown>): FakeChangeStream {
    this.watchOpts = opts;
    this.watchCalls++;
    if (this.watchCalls > 1) {
      this.stream = new FakeChangeStream();
      this.streams.push(this.stream);
    }
    return this.stream;
  }
}

class FakeMongoDb {
  constructor(private _cols: Record<string, FakeCollection>) {}
  collection(name: string): FakeCollection {
    return (this._cols[name] ??= new FakeCollection());
  }
}

/** What a checkpoint holds, in the fake and in the real one. */
interface FakeCheckpointState {
  manifest: Record<string, string>;
  token: unknown;
  head?: string | null;
}

/** In-memory stand-in for EditCheckpoint (no filesystem). */
class FakeCheckpoint {
  saved: Array<{
    token: unknown;
    manifest: Record<string, string>;
    head: string | null;
  }> = [];
  load = vi.fn(
    async (c: string): Promise<FakeCheckpointState | undefined> => this.state[c],
  );
  save = vi.fn(
    async (
      c: string,
      m: Map<string, string>,
      token: unknown,
      head: string | null = null,
    ): Promise<void> => {
      this.saved.push({ token, manifest: Object.fromEntries(m), head });
      // Persist it the way the real one does, so a test can restart onto it.
      this.state[c] = { manifest: Object.fromEntries(m), token, head };
    },
  );
  constructor(
    public state: Record<string, FakeCheckpointState | undefined> = {},
  ) {}
}

const mkRljsonDb = async (): Promise<Db> => {
  const io = new IoMem();
  await io.init();
  await io.isReady();
  return new Db(io);
};

/** A recording connector; `reannounce` present unless a test drops it. */
const mkConnector = (): EditSyncConnector & {
  fire: (ref: string) => void;
  send: ReturnType<typeof vi.fn>;
  reannounce: ReturnType<typeof vi.fn>;
  invalidateReceived: ReturnType<typeof vi.fn>;
  reconnect: ReturnType<typeof vi.fn>;
} => {
  let cb: ((r: string) => void | Promise<void>) | undefined;
  return {
    send: vi.fn(),
    reannounce: vi.fn(),
    invalidateReceived: vi.fn(),
    reconnect: vi.fn(),
    listen: (fn) => {
      cb = fn;
    },
    fire: (ref) => void cb?.(ref),
  };
};

const tick = (ms = 20): Promise<void> =>
  new Promise((r) => setTimeout(r, ms));

const headFor = (
  send: ReturnType<typeof vi.fn>,
  collection: string,
): string => {
  const call = send.mock.calls
    .map((c) => c[0] as string)
    .find((r) => r.startsWith(`${collection}:`));
  if (!call) throw new Error(`no head broadcast for ${collection}`);
  return call;
};

/** A root no node can hold, for tagging a head as "from a peer ahead of us". */
const FOREIGN_ROOT = 'f'.repeat(64);

/**
 * Re-tags a head ref with a foreign content root so firing it back at its own
 * producer simulates a genuine peer. A head carries the root it produces, and
 * the receiver skips a head whose root it already holds — so a node's own head,
 * replayed unchanged, is (correctly) a no-op and would test nothing.
 * @param ref - The head ref as broadcast (`<collection>:<head>|<root>`).
 * @returns The same head tagged with {@link FOREIGN_ROOT}.
 */
const asPeerHead = (ref: string): string =>
  `${ref.split('|')[0]}|${FOREIGN_ROOT}`;

describe('MongoEditSync', () => {
  beforeEach(() => {
    // Fast, deterministic pull-retry for the empty-pull recovery paths.
    process.env['SL_EDIT_PULL_RETRIES'] = '1';
    process.env['SL_EDIT_PULL_BACKOFF_MS'] = '1';
    process.env['SL_EDIT_ROOT_DEBOUNCE_MS'] = '5';
  });
  afterEach(() => {
    vi.restoreAllMocks();
    delete process.env['SL_EDIT_HEARTBEAT_MS'];
    delete process.env['SL_EDIT_DELETE_DEBOUNCE_MS'];
    delete process.env['SL_EDIT_DELETE_ABS_MAX'];
    delete process.env['SL_EDIT_DELETE_FRACTION'];
    delete process.env['SL_EDIT_PULL_RETRIES'];
    delete process.env['SL_EDIT_PULL_BACKOFF_MS'];
    delete process.env['SL_EDIT_ROOT_DEBOUNCE_MS'];
    delete process.env['SL_EDIT_SAVE_DEBOUNCE_MS'];
    delete process.env['SL_EDIT_CHECKPOINT_MAX_ENTRIES'];
    delete process.env['SL_EDIT_APPLIED_MAX'];
    delete process.env['SL_EDIT_LWW_MAX'];
  });

  /** The content-root ref (`~R~<coll>:<root>`) last broadcast for a collection. */
  const rootRefOf = (
    conn: { send: ReturnType<typeof vi.fn>; reannounce: ReturnType<typeof vi.fn> },
    collection: string,
  ): string | undefined => {
    const calls = [...conn.reannounce.mock.calls, ...conn.send.mock.calls]
      .map((c) => c[0] as string)
      .filter((r) => r.startsWith(`~R~${collection}:`));
    return calls[calls.length - 1];
  };

  const lastRef = (
    send: ReturnType<typeof vi.fn>,
    collection: string,
  ): string => {
    const refs = send.mock.calls
      .map((c) => c[0] as string)
      .filter((r) => r.startsWith(`${collection}:`));
    return refs[refs.length - 1];
  };

  it('baseline is manifest-only (root, not head); a live change produces a head that applies back', async () => {
    const cols = {
      customers: new FakeCollection([
        { _id: new Int32(1), name: 'Alice' },
        { _id: new Int32(2), name: 'Bob' },
      ]),
      empties: new FakeCollection([]),
    };
    const mongo = new FakeMongoDb(cols);
    const db = await mkRljsonDb();
    const conn = mkConnector();
    const sync = new MongoEditSync(
      mongo as never,
      db,
      conn,
      ['customers', 'empties'],
      'p',
    );
    await sync.start();

    // The baseline broadcasts a content ROOT (via reannounce), NOT a head — the
    // cake stays empty until a live change (no super-linear baseline chain).
    expect(rootRefOf(conn, 'customers')).toMatch(/^~R~customers:/);
    expect(conn.send.mock.calls.some((c) => (c[0] as string).startsWith('customers:'))).toBe(false);

    // A live change appends to the cake → a head is broadcast.
    cols.customers.stream.emit({ operationType: 'insert', fullDocument: { _id: new Int32(3), name: 'Carol' } });
    await tick();
    const ref = headFor(conn.send, 'customers');

    // Feed the head back as if a peer ahead of us sent it → applied into mongo.
    const peerRef = asPeerHead(ref);
    conn.fire(peerRef);
    await (sync as unknown as { _applyChain: Map<string, Promise<void>> })._applyChain.get('customers');
    expect(cols.customers.replaceOne).toHaveBeenCalledTimes(1);

    // Re-feeding the same head is a no-op (since === head).
    cols.customers.replaceOne.mockClear();
    conn.fire(peerRef);
    await tick();
    expect(cols.customers.replaceOne).not.toHaveBeenCalled();

    await sync.stop();
    expect(cols.customers.stream.close).toHaveBeenCalledTimes(1);
  });

  describe('receive-liveness watchdog', () => {
    type Internals = {
      _lastInboundAt: number;
      _rxReconnectCooldownUntil: number;
      _coldStartComplete: boolean;
      _checkReceiveLiveness: () => void;
    };
    const mkSync = async (conn: ReturnType<typeof mkConnector>) => {
      const cols = { customers: new FakeCollection([]) };
      const sync = new MongoEditSync(
        new FakeMongoDb(cols) as never,
        await mkRljsonDb(),
        conn,
        ['customers'],
        'p',
      );
      await sync.start();
      return sync;
    };

    it('an inbound ref stamps _lastInboundAt (arms the watchdog)', async () => {
      const conn = mkConnector();
      const sync = await mkSync(conn);
      const i = sync as unknown as Internals;
      i._lastInboundAt = 0;
      conn.fire('~R~customers:' + 'a'.repeat(12));
      expect(i._lastInboundAt).toBeGreaterThan(0);
      await sync.stop();
    });

    it('reconnects once inbound has been silent past the window', async () => {
      const conn = mkConnector();
      const sync = await mkSync(conn);
      const i = sync as unknown as Internals;
      i._coldStartComplete = true;
      i._lastInboundAt = Date.now() - 120_000; // long past the 45s window
      i._rxReconnectCooldownUntil = 0;
      i._checkReceiveLiveness();
      expect(conn.reconnect).toHaveBeenCalledTimes(1);
      // Re-armed from now → an immediate second pass does nothing (window + cooldown).
      i._checkReceiveLiveness();
      expect(conn.reconnect).toHaveBeenCalledTimes(1);
      await sync.stop();
    });

    it('does NOT reconnect when it has never heard the fleet (maybe alone)', async () => {
      const conn = mkConnector();
      const sync = await mkSync(conn);
      const i = sync as unknown as Internals;
      i._coldStartComplete = true;
      i._lastInboundAt = 0; // never received anything
      i._checkReceiveLiveness();
      expect(conn.reconnect).not.toHaveBeenCalled();
      await sync.stop();
    });

    it('does NOT reconnect while inbound is recent or during cooldown', async () => {
      const conn = mkConnector();
      const sync = await mkSync(conn);
      const i = sync as unknown as Internals;
      i._coldStartComplete = true;
      // Recent inbound → within window.
      i._lastInboundAt = Date.now();
      i._checkReceiveLiveness();
      expect(conn.reconnect).not.toHaveBeenCalled();
      // Silent past window, but cooldown still in force.
      i._lastInboundAt = Date.now() - 120_000;
      i._rxReconnectCooldownUntil = Date.now() + 60_000;
      i._checkReceiveLiveness();
      expect(conn.reconnect).not.toHaveBeenCalled();
      await sync.stop();
    });

    it('is inert when the connector cannot reconnect', async () => {
      const conn = mkConnector();
      (conn as { reconnect?: unknown }).reconnect = undefined;
      const sync = await mkSync(conn);
      const i = sync as unknown as Internals;
      i._coldStartComplete = true;
      i._lastInboundAt = Date.now() - 120_000;
      expect(() => i._checkReceiveLiveness()).not.toThrow();
      await sync.stop();
    });

    it('is disabled by SL_EDIT_RX_WATCHDOG_MS=0', async () => {
      process.env['SL_EDIT_RX_WATCHDOG_MS'] = '0';
      try {
        const conn = mkConnector();
        const sync = await mkSync(conn);
        const i = sync as unknown as Internals;
        i._coldStartComplete = true;
        i._lastInboundAt = Date.now() - 120_000;
        i._checkReceiveLiveness();
        expect(conn.reconnect).not.toHaveBeenCalled();
        await sync.stop();
      } finally {
        delete process.env['SL_EDIT_RX_WATCHDOG_MS'];
      }
    });

    it('swallows a throwing reconnect and still arms the cooldown', async () => {
      const conn = mkConnector();
      conn.reconnect.mockImplementation(() => {
        throw new Error('socket gone');
      });
      const sync = await mkSync(conn);
      const i = sync as unknown as Internals;
      i._coldStartComplete = true;
      i._lastInboundAt = Date.now() - 120_000;
      i._rxReconnectCooldownUntil = 0;
      expect(() => i._checkReceiveLiveness()).not.toThrow();
      expect(conn.reconnect).toHaveBeenCalledTimes(1);
      expect(i._rxReconnectCooldownUntil).toBeGreaterThan(Date.now());
      await sync.stop();
    });
  });

  it('broadcasts a new head on a live insert and suppresses the echo', async () => {
    const cols = { customers: new FakeCollection([]) };
    const mongo = new FakeMongoDb(cols);
    const conn = mkConnector();
    const sync = new MongoEditSync(mongo as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    conn.send.mockClear();

    const doc = { _id: new Int32(7), name: 'Carol' };
    cols.customers.stream.emit({ operationType: 'insert', fullDocument: doc });
    await tick();
    const ref = headFor(conn.send, 'customers');

    // Apply the head (marks _appliedHash for _id=7).
    conn.fire(asPeerHead(ref));
    await (sync as unknown as { _applyChain: Map<string, Promise<void>> })._applyChain.get('customers');
    conn.send.mockClear();

    // A change stream event echoing that same content must NOT re-broadcast.
    cols.customers.stream.emit({ operationType: 'replace', fullDocument: doc });
    await tick();
    expect(conn.send).not.toHaveBeenCalled();

    // An update with different content DOES broadcast.
    cols.customers.stream.emit({
      operationType: 'update',
      fullDocument: { _id: new Int32(7), name: 'Caroline' },
    });
    await tick();
    expect(conn.send).toHaveBeenCalled();
    await sync.stop();
  });

  it('ignores unhandled ops and events without a fullDocument', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    conn.send.mockClear();

    cols.customers.stream.emit({ operationType: 'drop' }); // unhandled op
    cols.customers.stream.emit({ operationType: 'insert' }); // no fullDocument
    await tick();
    expect(conn.send).not.toHaveBeenCalled();
    await sync.stop();
  });

  it('queues change events that arrive during the snapshot and drains them after', async () => {
    // onYield fires the change handler mid-snapshot → the queue path with
    // snapshotDone still false is exercised.
    let injected = false;
    const cols: Record<string, FakeCollection> = {};
    cols.customers = new FakeCollection(
      [{ _id: new Int32(1), name: 'A' }],
      () => {
        if (injected) return;
        injected = true;
        cols.customers.stream.emit({
          operationType: 'insert',
          fullDocument: { _id: new Int32(9), name: 'MidSnapshot' },
        });
      },
    );
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    await tick();
    // Both the snapshot doc and the mid-snapshot insert produced heads.
    expect(conn.send.mock.calls.length).toBeGreaterThanOrEqual(1);
    await sync.stop();
  });

  it('drops malformed refs and refs for unsynced collections', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();

    conn.fire('no-colon-here'); // idx < 0
    conn.fire('unknownColl:someHead'); // not in collections
    await tick();
    expect(cols.customers.replaceOne).not.toHaveBeenCalled();
    await sync.stop();
  });

  it('drops a root ref for an unsynced collection when no shouldSync predicate is given', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();

    conn.fire(`~R~peerOnly:${'a'.repeat(64)}`);
    await tick();

    const internals = sync as unknown as { _collections: Set<string> };
    expect(internals._collections.has('peerOnly')).toBe(false);
    await sync.stop();
  });

  it('drops a root ref for a peer-only collection the shouldSync predicate rejects', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
      undefined,
      undefined,
      () => false, // shouldSync rejects every peer-only collection
    );
    await sync.start();

    conn.fire(`~R~peerOnly:${'a'.repeat(64)}`);
    await tick();

    const internals = sync as unknown as { _collections: Set<string> };
    expect(internals._collections.has('peerOnly')).toBe(false);
    await sync.stop();
  });

  it('adopts a peer-only collection on an unknown root ref and triggers anti-entropy', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
      undefined,
      undefined,
      () => true, // shouldSync accepts every peer-only collection
    );
    await sync.start();

    const internals = sync as unknown as {
      _collections: Set<string>;
      _ae: { trigger: (c: string) => boolean };
    };
    const triggerSpy = vi.spyOn(internals._ae, 'trigger');

    // A bulk import / cold-start delta never produces a head, only this root
    // broadcast — the only signal this collection ever gets. Fire a second
    // root ref for the SAME collection immediately (no await in between) so it
    // lands while the first adoption is still in flight — the in-flight guard
    // must skip starting a duplicate adoption.
    conn.fire(`~R~peerOnly:${'a'.repeat(64)}`);
    conn.fire(`~R~peerOnly:${'b'.repeat(64)}`);
    await tick(50);

    expect(internals._collections.has('peerOnly')).toBe(true);
    expect(triggerSpy).toHaveBeenCalledWith('peerOnly');
    await sync.stop();
  });

  it('logs and clears the in-flight guard when adopt-on-ref (root path) fails', async () => {
    class BrokenCollection extends FakeCollection {
      watch(): FakeChangeStream {
        throw new Error('boom');
      }
    }
    const cols: Record<string, FakeCollection> = {
      customers: new FakeCollection([]),
      peerOnly: new BrokenCollection([]),
    };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
      undefined,
      undefined,
      () => true,
    );
    await sync.start();

    const internals = sync as unknown as { _adoptingOnRef: Set<string> };
    conn.fire(`~R~peerOnly:${'a'.repeat(64)}`);
    await tick(50);

    expect(internals._adoptingOnRef.has('peerOnly')).toBe(false);
    await sync.stop();
  });

  it('retries a throwing pull, then applies once it succeeds', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();

    const adapter = (sync as unknown as { _adapter: { collectPuts: unknown } })._adapter;
    // Attempt 0 throws; attempt 1 (the single retry) succeeds with a doc.
    adapter.collectPuts = vi
      .fn()
      .mockRejectedValueOnce(new Error('pull not ready'))
      .mockResolvedValue({
        puts: [{ collection: 'customers', sliceId: '1', doc: { _id: new Int32(1), name: 'A' } }],
        complete: true,
        sealed: ['HEAD_A'],
      });
    const chain = () =>
      (sync as unknown as { _applyChain: Map<string, Promise<void>> })._applyChain.get('customers');

    conn.fire('customers:HEAD_A');
    await chain()?.catch(() => {});
    expect(adapter.collectPuts).toHaveBeenCalledTimes(2);
    expect(cols.customers.replaceOne).toHaveBeenCalledTimes(1);
    await sync.stop();
  });

  it('walks a foreign lineage against EVERY applied ref, not just the last head', async () => {
    // Chains are never merged: each node appends only its own writes to its
    // own chain, so a fleet of n nodes has n independent lineages and every
    // receiver applies heads from all of them. A single "last applied head"
    // could only ever hold one lineage's tip — a head from any other lineage
    // had no common ancestor with it and was replayed from its ROOT, writing
    // that node's stale versions over newer ones and re-creating deleted
    // documents. The walk floor is the SET of everything applied.
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();

    const internals = sync as unknown as {
      _adapter: { collectPuts: unknown };
      _applyChain: Map<string, Promise<void>>;
      _applied: Map<string, Set<string>>;
    };
    // We have already applied refs from two different lineages.
    internals._applied.set('customers', new Set(['OURS_1', 'THEIRS_1']));

    // The set is live and grows as refs are sealed — snapshot it at call time.
    const stopAtWhenCalled: string[][] = [];
    const collect = vi.fn(
      async (_c: string, _head: string, stopAt: ReadonlySet<string>) => {
        stopAtWhenCalled.push([...stopAt]);
        return {
          puts: [
            {
              collection: 'customers',
              sliceId: '7',
              doc: { _id: new Int32(7) },
              timeId: '2000:aaaa',
            },
          ],
          complete: true,
          sealed: ['THEIRS_2'],
        };
      },
    );
    internals._adapter.collectPuts = collect;

    conn.fire('customers:THEIRS_2');
    await internals._applyChain.get('customers')?.catch(() => {});

    // One pull, against the whole applied set — no root replay, no fallback.
    expect(collect).toHaveBeenCalledTimes(1);
    expect(stopAtWhenCalled).toEqual([['OURS_1', 'THEIRS_1']]);
    expect(cols.customers.replaceOne).toHaveBeenCalledTimes(1);
    expect(internals._applied.get('customers')?.has('THEIRS_2')).toBe(true);
    expect(conn.invalidateReceived).not.toHaveBeenCalled();
    await sync.stop();
  });

  it('an edit older than the one a document already carries is skipped', async () => {
    // The convergence guarantee: a `timeId` is minted once by the node that
    // made the edit and travels with the row, so every node orders the same
    // two edits identically. Applying only the newer one makes every apply
    // monotonic per document, which is what makes a replay harmless.
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    const internals = sync as unknown as {
      _adapter: { collectPuts: unknown };
      _applyChain: Map<string, Promise<void>>;
      _appliedTimeId: Map<string, Map<string, string>>;
    };
    internals._appliedTimeId.set(
      'customers',
      new Map([['1', '5000:zzzz']]),
    );
    internals._adapter.collectPuts = vi.fn().mockResolvedValue({
      puts: [
        {
          collection: 'customers',
          sliceId: '1',
          doc: { _id: new Int32(1), name: 'stale' },
          timeId: '4000:aaaa',
        },
        {
          collection: 'customers',
          sliceId: '2',
          doc: { _id: new Int32(2), name: 'fresh' },
          timeId: '4000:aaaa',
        },
      ],
      complete: true,
      sealed: ['HEAD_T'],
    });

    conn.fire('customers:HEAD_T');
    await internals._applyChain.get('customers')?.catch(() => {});

    // Only the document with no newer edit on record is written.
    expect(cols.customers.replaceOne).toHaveBeenCalledTimes(1);
    expect(
      (cols.customers.replaceOne.mock.calls[0] as unknown[])[1],
    ).toMatchObject({ name: 'fresh' });
    await sync.stop();
  });

  it('a complete pull carrying nothing new is a no-op, not a retry', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();

    const internals = sync as unknown as {
      _adapter: { collectPuts: unknown };
      _applyChain: Map<string, Promise<void>>;
      _applied: Map<string, Set<string>>;
    };
    // A COMPLETE walk with no puts means every edit this head carries is
    // already applied — a re-announce. Nothing to retry, nothing to re-arm.
    internals._adapter.collectPuts = vi
      .fn()
      .mockResolvedValue({ puts: [], complete: true, sealed: ['HEAD_X'] });

    conn.fire('customers:HEAD_X');
    await internals._applyChain.get('customers')?.catch(() => {});
    expect(internals._adapter.collectPuts).toHaveBeenCalledTimes(1);
    expect(conn.invalidateReceived).not.toHaveBeenCalled();
    expect(cols.customers.replaceOne).not.toHaveBeenCalled();
    // Sealed refs are remembered so the next walk stops here.
    expect(internals._applied.get('customers')?.has('HEAD_X')).toBe(true);
    await sync.stop();
  });

  it('a PARTIAL pull applies what resolved, seals nothing, and re-arms the ref', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();

    const internals = sync as unknown as {
      _adapter: { collectPuts: unknown };
      _applyChain: Map<string, Promise<void>>;
      _applied: Map<string, Set<string>>;
    };
    // A truncated chain: puts present but complete=false (an ancestor row was
    // unresolvable on every read path). Discarding it is what left a node on
    // the old state until it was restarted — the head was invalidated and the
    // content root never matched again. The resolvable part is applied instead;
    // it cannot do harm, because an apply never moves a document backwards.
    internals._adapter.collectPuts = vi.fn().mockResolvedValue({
      puts: [
        {
          collection: 'customers',
          sliceId: '1',
          doc: { _id: new Int32(1), name: 'A' },
          timeId: '1000:aaaa',
        },
      ],
      complete: false,
      sealed: [],
    });

    conn.fire('customers:HEAD_P');
    await internals._applyChain.get('customers')?.catch(() => {});
    // Retried (attempt 0 + 1) while incomplete, then applied what it had.
    expect(internals._adapter.collectPuts).toHaveBeenCalledTimes(2);
    expect(cols.customers.replaceOne).toHaveBeenCalledTimes(1);
    // Re-armed so a later re-announce delivers the head again…
    expect(conn.invalidateReceived).toHaveBeenCalledWith('customers:HEAD_P');
    // …and nothing was sealed, so the next walk still reaches the missing rows.
    expect(internals._applied.get('customers')?.size ?? 0).toBe(0);
    await sync.stop();
  });

  it('stops re-arming a PARTIAL head once the walk floor stops moving', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();

    const internals = sync as unknown as {
      _adapter: { collectPuts: unknown };
      _applyChain: Map<string, Promise<void>>;
      _pullRetries: number;
      _headRearmCap: number;
    };
    internals._pullRetries = 0; // one collectPuts per apply, no backoff wait
    // Every walk resolves the same nothing and seals nothing: a head whose edit
    // chain is fundamentally incomplete — the common case for a collection that
    // was bulk-imported and therefore stored chain-free by anti-entropy. The
    // walk floor never moves, so re-arming it on every re-announce forever is an
    // event-loop-starving no-op that also stalls the anti-entropy path that
    // alone can converge it.
    internals._adapter.collectPuts = vi
      .fn()
      .mockResolvedValue({ puts: [], complete: false, sealed: [] });

    const cap = internals._headRearmCap;
    for (let i = 0; i < cap + 2; i++) {
      conn.fire('customers:HEAD_STUCK');
      await internals._applyChain.get('customers')?.catch(() => {});
    }
    // Re-armed while it might still be the transient fresh-connection race, then
    // gave up and left the collection to anti-entropy — exactly `cap` re-arms,
    // never an unbounded spin.
    expect(conn.invalidateReceived).toHaveBeenCalledTimes(cap);
    await sync.stop();
  });

  it('keeps re-arming a PARTIAL head while the walk floor still advances', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();

    const internals = sync as unknown as {
      _adapter: { collectPuts: unknown };
      _applyChain: Map<string, Promise<void>>;
      _pullRetries: number;
      _headRearmCap: number;
    };
    internals._pullRetries = 0;
    // Each walk is still partial but seals a NEW ref — the floor advances every
    // time, the genuine "peer is serving the chain a chunk at a time" case. The
    // no-progress count must reset on every advance, so re-arming continues well
    // past the cap and the chain is never abandoned mid-heal.
    let n = 0;
    internals._adapter.collectPuts = vi
      .fn()
      .mockImplementation(() =>
        Promise.resolve({ puts: [], complete: false, sealed: [`S${n++}`] }),
      );

    const fires = internals._headRearmCap + 3;
    for (let i = 0; i < fires; i++) {
      conn.fire('customers:HEAD_ADV');
      await internals._applyChain.get('customers')?.catch(() => {});
    }
    expect(conn.invalidateReceived).toHaveBeenCalledTimes(fires);
    await sync.stop();
  });

  it('re-pulls a partial chain and seals it once it becomes complete', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();

    const internals = sync as unknown as {
      _adapter: { collectPuts: unknown };
      _applyChain: Map<string, Promise<void>>;
      _applied: Map<string, Set<string>>;
    };
    // Attempt 0: truncated (missing ancestor). Attempt 1: the ancestor is now
    // resolvable → complete → the head is sealed. Proves the gap self-heals
    // once the peer serves the full chain.
    const put = {
      collection: 'customers',
      sliceId: '1',
      doc: { _id: new Int32(1), name: 'A' },
      timeId: '1000:aaaa',
    };
    internals._adapter.collectPuts = vi
      .fn()
      .mockResolvedValueOnce({ puts: [put], complete: false, sealed: [] })
      .mockResolvedValue({ puts: [put], complete: true, sealed: ['HEAD_Q'] });

    conn.fire('customers:HEAD_Q');
    await internals._applyChain.get('customers')?.catch(() => {});
    expect(internals._adapter.collectPuts).toHaveBeenCalledTimes(2);
    expect(cols.customers.replaceOne).toHaveBeenCalledTimes(1);
    expect(internals._applied.get('customers')?.has('HEAD_Q')).toBe(true);
    await sync.stop();
  });

  it('bounds the applied-ref set and the per-document timeIds (FIFO)', async () => {
    // Both maps are bounded so a long-lived node cannot grow them without
    // limit. Eviction only costs a longer walk (or one more apply) later —
    // never correctness.
    process.env['SL_EDIT_APPLIED_MAX'] = '1';
    process.env['SL_EDIT_LWW_MAX'] = '1';
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    const internals = sync as unknown as {
      _adapter: { collectPuts: unknown };
      _applyChain: Map<string, Promise<void>>;
      _applied: Map<string, Set<string>>;
      _appliedTimeId: Map<string, Map<string, string>>;
    };
    internals._adapter.collectPuts = vi.fn().mockResolvedValue({
      puts: [
        {
          collection: 'customers',
          sliceId: '1',
          doc: { _id: new Int32(1) },
          timeId: '1000:aaaa',
        },
        {
          collection: 'customers',
          sliceId: '2',
          doc: { _id: new Int32(2) },
          timeId: '1000:bbbb',
        },
      ],
      complete: true,
      sealed: ['SEAL_1', 'SEAL_2'],
    });

    conn.fire('customers:HEAD_B');
    await internals._applyChain.get('customers')?.catch(() => {});

    expect([...(internals._applied.get('customers') as Set<string>)]).toEqual([
      'SEAL_2',
    ]);
    expect([
      ...(internals._appliedTimeId.get('customers') as Map<string, string>),
    ]).toEqual([['2', '1000:bbbb']]);
    await sync.stop();
  });

  it('a failing mongo write rejects the apply chain; the next ref still applies', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    const internals = sync as unknown as {
      _adapter: { collectPuts: unknown };
      _applyChain: Map<string, Promise<void>>;
    };
    internals._adapter.collectPuts = vi.fn().mockResolvedValue({
      puts: [{ collection: 'customers', sliceId: '1', doc: { _id: new Int32(1) } }],
      complete: true,
      sealed: [],
    });
    // First write throws → _applyHead rejects → the chain's .catch(() => {})
    // swallows it so the next ref still applies.
    cols.customers.replaceOne
      .mockRejectedValueOnce(new Error('mongo down'))
      .mockResolvedValue({} as never);

    conn.fire('customers:HEAD_1');
    await internals._applyChain.get('customers')?.catch(() => {});
    conn.fire('customers:HEAD_2');
    await internals._applyChain.get('customers')?.catch(() => {});
    expect(cols.customers.replaceOne).toHaveBeenCalledTimes(2);
    await sync.stop();
  });

  it('gives up and invalidates when the pull keeps throwing', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    const internals = sync as unknown as {
      _adapter: { collectPuts: unknown };
      _applyChain: Map<string, Promise<void>>;
    };
    internals._adapter.collectPuts = vi
      .fn()
      .mockRejectedValue(new Error('never ready')); // throws on every attempt

    conn.fire('customers:HEAD_Z');
    await internals._applyChain.get('customers')?.catch(() => {});
    expect(internals._adapter.collectPuts).toHaveBeenCalledTimes(2);
    expect(conn.invalidateReceived).toHaveBeenCalledWith('customers:HEAD_Z');
    await sync.stop();
  });

  it('re-announces live heads on the heartbeat (skipping collections with no head), bypassing send-dedup', async () => {
    process.env['SL_EDIT_HEARTBEAT_MS'] = '25';
    // `empties` gets no change → no cake head → the heartbeat's `!head` continue.
    const cols = {
      customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]),
      empties: new FakeCollection([]),
    };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers', 'empties'],
      'p',
    );
    await sync.start();
    // A live change on customers creates a head; empties stays head-less.
    cols.customers.stream.emit({ operationType: 'insert', fullDocument: { _id: new Int32(2), name: 'B' } });
    await tick();
    conn.reannounce.mockClear();
    await tick(70);
    expect(conn.reannounce).toHaveBeenCalledWith(expect.stringMatching(/^customers:/));
    expect(conn.reannounce).not.toHaveBeenCalledWith(expect.stringMatching(/^empties:/));
    await sync.stop();
  });

  it('announceHeads on demand reports how many collections it announced', async () => {
    // An operator's "push now" is exactly the heartbeat's body, run at once —
    // a local write is already on the wire by the time the change stream
    // returns, so a head re-announce is the only thing left to push.
    const cols = {
      customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]),
      empties: new FakeCollection([]),
    };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers', 'empties'],
      'p',
    );
    await sync.start();
    cols.customers.stream.emit({
      operationType: 'insert',
      fullDocument: { _id: new Int32(2), name: 'B' },
    });
    await tick();
    conn.reannounce.mockClear();

    // `empties` has no head, so it is not counted — the number is what was
    // actually announced, not what is watched.
    expect(sync.announceHeads()).toBe(1);
    expect(conn.reannounce).toHaveBeenCalledWith(
      expect.stringMatching(/^customers:/),
    );
    await sync.stop();
  });

  it('announceHeads announces nothing on a node that watches nothing', async () => {
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb({}) as never,
      await mkRljsonDb(),
      conn,
      [],
      'p',
    );
    await sync.start();
    expect(sync.announceHeads()).toBe(0);
    expect(conn.reannounce).not.toHaveBeenCalled();
    await sync.stop();
  });

  it('does not broadcast when putDoc yields no head (unknown collection)', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    conn.send.mockClear();
    (sync as unknown as { _adapter: { putDoc: unknown } })._adapter.putDoc = vi
      .fn()
      .mockResolvedValue(null);
    cols.customers.stream.emit({
      operationType: 'insert',
      fullDocument: { _id: new Int32(3), name: 'NoHead' },
    });
    await tick();
    expect(conn.send).not.toHaveBeenCalled();
    await sync.stop();
  });

  it('applies the fraction guard to docs added AFTER start-up (live baseline)', async () => {
    // Snapshot-time baseline was 0 here, so the old code fell through to the
    // absolute cap and the fraction guard never applied to a collection that
    // grew after start-up. With the live manifest baseline the same burst is
    // correctly recognised as a mass delete.
    process.env['SL_EDIT_DELETE_DEBOUNCE_MS'] = '10';
    process.env['SL_EDIT_DELETE_ABS_MAX'] = '1000';
    process.env['SL_EDIT_DELETE_FRACTION'] = '0.3';
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();

    // Grow the collection to 20 docs post-snapshot.
    for (let i = 1; i <= 20; i++) {
      cols.customers.stream.emit({
        operationType: 'insert',
        fullDocument: { _id: new Int32(i), name: `c${i}` },
      });
    }
    await tick(60);
    conn.send.mockClear();

    // Delete 12 of 20 (60% > 30%) — must be blocked as a mass delete.
    for (let i = 1; i <= 12; i++) {
      cols.customers.stream.emit({
        operationType: 'delete',
        documentKey: { _id: new Int32(i) },
      });
    }
    await tick(60);

    expect(conn.send).not.toHaveBeenCalledWith(
      expect.stringMatching(/^customers:/),
    );
    await sync.stop();
  });

  it('adopts a collection a PEER announces but this node does not have', async () => {
    // The consumer half of the deadlock. This node's synced set is built from
    // its OWN collections, and discovery only adopts what appears in its own
    // mongo — which a peer-only collection never would, because the very sync
    // that would create it is the one being dropped. So it could never arrive.
    const cols: Record<string, FakeCollection> = {
      customers: new FakeCollection([]),
    };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
      undefined,
      undefined,
      () => true, // everything is syncable here
    );
    await sync.start();

    // A peer announces a collection this node has never seen.
    conn.fire('newFromPeer:some-head-ref');
    await tick(60);

    const known = (sync as unknown as { _collections: Set<string> })._collections;
    expect(known.has('newFromPeer')).toBe(true);

    // A burst of refs for the same unknown collection must adopt it ONCE.
    const cols2: Record<string, FakeCollection> = { customers: new FakeCollection([]) };
    const conn2 = mkConnector();
    const sync2 = new MongoEditSync(
      new FakeMongoDb(cols2) as never,
      await mkRljsonDb(),
      conn2,
      ['customers'],
      'p',
      undefined,
      undefined,
      () => true,
    );
    await sync2.start();
    const adoptSpy = vi.spyOn(
      sync2 as unknown as { _adoptCollection: (c: string) => Promise<void> },
      '_adoptCollection',
    );
    conn2.fire('burst:head-1');
    conn2.fire('burst:head-2');
    conn2.fire('burst:head-3');
    await tick(60);
    expect(adoptSpy).toHaveBeenCalledTimes(1);
    await sync2.stop();

    await sync.stop();
  });

  it('survives a failed on-demand adoption without wedging the guard', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
      undefined,
      undefined,
      () => true,
    );
    await sync.start();
    (sync as unknown as { _adoptCollection: unknown })._adoptCollection = vi
      .fn()
      .mockRejectedValue(new Error('cake init failed'));

    conn.fire('brokenColl:head');
    await tick(60);
    // The in-flight guard must be released, or a transient failure would block
    // that collection for the process lifetime.
    const inflight = (sync as unknown as { _adoptingOnRef: Set<string> })
      ._adoptingOnRef;
    expect(inflight.has('brokenColl')).toBe(false);
    await sync.stop();
  });

  it('does not adopt a peer collection the filter rejects', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
      undefined,
      undefined,
      (name) => name !== 'sync_internal',
    );
    await sync.start();
    conn.fire('sync_internal:some-head');
    await tick(40);
    const known = (sync as unknown as { _collections: Set<string> })._collections;
    expect(known.has('sync_internal')).toBe(false);
    await sync.stop();
  });

  it('drops a peer collection when no predicate is supplied', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    conn.fire('unknownColl:head');
    await tick(40);
    const known = (sync as unknown as { _collections: Set<string> })._collections;
    expect(known.has('unknownColl')).toBe(false);
    await sync.stop();
  });

  it('adopts a collection created after start-up (live discovery)', async () => {
    // The whole reason __synctest previously had to be pre-created by hand:
    // one change stream is opened per collection at start-up, so a collection
    // that did not exist yet was never watched.
    process.env['SL_EDIT_DISCOVER_MS'] = '5';
    const cols: Record<string, FakeCollection> = {
      customers: new FakeCollection([]),
    };
    const conn = mkConnector();
    let desired = ['customers'];
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
      async () => desired,
    );
    await sync.start();
    conn.send.mockClear();

    // A collection appears in Mongo after start-up.
    cols['newColl'] = new FakeCollection([{ _id: new Int32(7), name: 'Late' }]);
    desired = ['customers', 'newColl'];
    await tick(40);

    // Adopted: its content root was broadcast (manifest-only baseline — the
    // cake stays empty until a live change, so a root, not a head, is sent).
    expect(conn.reannounce).toHaveBeenCalledWith(
      expect.stringMatching(/^~R~newColl:/),
    );
    // ...and it is now live on the change stream.
    conn.send.mockClear();
    cols['newColl'].stream.emit({
      operationType: 'insert',
      fullDocument: { _id: new Int32(8), name: 'Live' },
    });
    await tick();
    expect(conn.send).toHaveBeenCalledWith(
      expect.stringMatching(/^newColl:/),
    );
    await sync.stop();
    delete process.env['SL_EDIT_DISCOVER_MS'];
  });

  it('does not re-adopt a collection it already syncs', async () => {
    process.env['SL_EDIT_DISCOVER_MS'] = '5';
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
      async () => ['customers'],
    );
    await sync.start();
    const watchCalls = vi.spyOn(cols.customers, 'watch');
    await tick(40);
    expect(watchCalls).not.toHaveBeenCalled();
    await sync.stop();
    delete process.env['SL_EDIT_DISCOVER_MS'];
  });

  it('_adoptCollection is a no-op when the collection is already adopted', async () => {
    // Defensive guard directly on _adoptCollection itself (independent of
    // _reconcile's own membership filter, which is exercised by "does not
    // re-adopt a collection it already syncs" above). Call the private
    // method a second time for a collection start() already adopted, and
    // confirm it returns before touching the adapter again.
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    const adapterInit = vi.spyOn((sync as any)._adapter, 'init');
    await (sync as unknown as {
      _adoptCollection: (c: string) => Promise<void>;
    })._adoptCollection('customers');
    expect(adapterInit).not.toHaveBeenCalled();
    await sync.stop();
  });

  it('survives a discovery supplier that rejects', async () => {
    process.env['SL_EDIT_DISCOVER_MS'] = '5';
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
      async () => {
        throw new Error('listCollections failed');
      },
    );
    await sync.start();
    await tick(40);
    // Still live: the rejection was swallowed, not left unhandled.
    conn.send.mockClear();
    cols.customers.stream.emit({
      operationType: 'insert',
      fullDocument: { _id: new Int32(1), name: 'ok' },
    });
    await tick();
    expect(conn.send).toHaveBeenCalledWith(
      expect.stringMatching(/^customers:/),
    );
    await sync.stop();
    delete process.env['SL_EDIT_DISCOVER_MS'];
  });

  it('keeps draining after a change whose putDoc throws (no wedged pump)', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    conn.send.mockClear();

    const real = (
      sync as unknown as { _adapter: { putDoc: (c: string, d: unknown) => Promise<string | null> } }
    )._adapter.putDoc.bind((sync as unknown as { _adapter: unknown })._adapter);
    const putDoc = vi
      .fn()
      .mockRejectedValueOnce(new Error('edit chain write failed'))
      .mockImplementation((c: string, d: unknown) => real(c, d));
    (sync as unknown as { _adapter: { putDoc: unknown } })._adapter.putDoc = putDoc;

    // First change throws; without the try/finally in `pump` this latches
    // `pumping` and the collection's producer side dies for good.
    cols.customers.stream.emit({
      operationType: 'insert',
      fullDocument: { _id: new Int32(1), name: 'Boom' },
    });
    await tick();
    // A later change must still be picked up and broadcast.
    cols.customers.stream.emit({
      operationType: 'insert',
      fullDocument: { _id: new Int32(2), name: 'Fine' },
    });
    await tick();

    expect(putDoc).toHaveBeenCalledTimes(2);
    expect(conn.send).toHaveBeenCalledWith(
      expect.stringMatching(/^customers:/),
    );
    await sync.stop();
  });

  it('rolls the manifest back when putDoc throws, so the content root stays honest', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    await tick(20);
    const rootBefore = rootRefOf(conn, 'customers');

    (sync as unknown as { _adapter: { putDoc: unknown } })._adapter.putDoc = vi
      .fn()
      .mockRejectedValue(new Error('edit chain write failed'));

    cols.customers.stream.emit({
      operationType: 'insert',
      fullDocument: { _id: new Int32(9), name: 'Ghost' },
    });
    await tick(30);

    // The doc never entered the edit chain, so the broadcast root must not
    // advertise it — otherwise peers could never converge on a pullable root.
    expect(rootRefOf(conn, 'customers')).toBe(rootBefore);
    await sync.stop();
  });

  it('propagates a delete as a tombstone and applies it as deleteOne', async () => {
    process.env['SL_EDIT_DELETE_DEBOUNCE_MS'] = '10';
    const cols = {
      customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]),
    };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    conn.send.mockClear();

    // A live delete → after the debounce a tombstone head is broadcast.
    cols.customers.stream.emit({
      operationType: 'delete',
      documentKey: { _id: new Int32(1) },
    });
    await tick(40);
    const ref = lastRef(conn.send, 'customers');
    expect(ref).toMatch(/^customers:/);

    // Applying that head deletes the doc (deleteOne, not replaceOne).
    conn.fire(asPeerHead(ref));
    await (sync as unknown as { _applyChain: Map<string, Promise<void>> })._applyChain.get('customers');
    expect(cols.customers.deleteOne).toHaveBeenCalledTimes(1);
    await sync.stop();
  });

  // `_recordTombstone` keeps an in-memory guard so a repeated delete of the
  // same _id does not write the tombstone log again. The guard is what makes
  // the persist safe to leave best-effort: without it, a delete storm on one
  // id would hammer the log collection from inside the delete path.
  it('records a repeated delete of the same id only once', async () => {
    process.env['SL_EDIT_DELETE_DEBOUNCE_MS'] = '10';
    // Only THIS test gives the tombstone log a working `updateOne`, so the
    // shared FakeCollection keeps exercising the best-effort catch elsewhere.
    const tombstoneLog = new FakeCollection() as FakeCollection & {
      updateOne: ReturnType<typeof vi.fn>;
    };
    tombstoneLog.updateOne = vi.fn(async () => ({}));
    const cols = {
      customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]),
      sl_edit_tombstones: tombstoneLog,
    };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();

    const del = (): void =>
      cols.customers.stream.emit({
        operationType: 'delete',
        documentKey: { _id: new Int32(1) },
      });

    del();
    await tick(40);
    expect(tombstoneLog.updateOne).toHaveBeenCalledTimes(1);

    del();
    await tick(40);
    expect(tombstoneLog.updateOne).toHaveBeenCalledTimes(1);
    await sync.stop();
  });

  it('a rejected tombstone-log persist is swallowed, not a delete-breaking failure', async () => {
    process.env['SL_EDIT_DELETE_DEBOUNCE_MS'] = '10';
    // Give the tombstone log collection a real `updateOne` that REJECTS —
    // unlike a missing method (which throws synchronously, the other
    // defensive branch _recordTombstone guards), this exercises the async
    // `.catch()` on the persist call itself.
    const tombstoneLog = {
      updateOne: vi.fn(async () => {
        throw new Error('mongo unavailable');
      }),
    };
    const cols = {
      customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]),
      sl_edit_tombstones: tombstoneLog,
    };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols as never) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    cols.customers.stream.emit({
      operationType: 'delete',
      documentKey: { _id: new Int32(1) },
    });
    await tick(40);
    // The delete itself still propagates — a failed BEST-EFFORT persist must
    // never abort the delete it is trying to durably record.
    expect(lastRef(conn.send, 'customers')).toMatch(/^customers:/);
    expect(tombstoneLog.updateOne).toHaveBeenCalled();
    await sync.stop();
  });

  it('a tombstone persist that throws SYNCHRONOUSLY (not a function) is swallowed too', async () => {
    process.env['SL_EDIT_DELETE_DEBOUNCE_MS'] = '10';
    // No `updateOne` at all, unlike the rejected-promise case above: calling it
    // throws synchronously, exercising the outer try/catch rather than the
    // `.catch()` on the returned promise.
    const cols = {
      customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]),
      sl_edit_tombstones: {},
    };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols as never) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    cols.customers.stream.emit({
      operationType: 'delete',
      documentKey: { _id: new Int32(1) },
    });
    await tick(40);
    // The delete itself still propagates — a synchronously-throwing persist
    // must never abort the delete it is trying to durably record.
    expect(lastRef(conn.send, 'customers')).toMatch(/^customers:/);
    await sync.stop();
  });

  it('records a tombstone even when no tombstone map exists yet for the collection', async () => {
    process.env['SL_EDIT_DELETE_DEBOUNCE_MS'] = '10';
    const cols = {
      customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]),
    };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    // `_loadTombstones` (run during adopt) always pre-creates an EMPTY map for
    // every synced collection, so `_recordTombstone`'s own "no map yet" branch
    // is otherwise unreachable in the same process. Force that exact
    // first-ever-tombstone state to exercise it directly.
    (sync as unknown as { _tombstones: Map<string, unknown> })._tombstones.delete(
      'customers',
    );
    cols.customers.stream.emit({
      operationType: 'delete',
      documentKey: { _id: new Int32(1) },
    });
    await tick(40);
    const tombstones = (
      sync as unknown as { _tombstones: Map<string, Map<string, unknown>> }
    )._tombstones.get('customers');
    expect(tombstones?.has('1')).toBe(true);
    await sync.stop();
  });

  it('forgetTombstones clears the in-memory guard and the persistent log for that collection', async () => {
    process.env['SL_EDIT_DELETE_DEBOUNCE_MS'] = '10';
    const cols = {
      customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]),
      products: new FakeCollection([{ _id: new Int32(9), name: 'Z' }]),
    };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers', 'products'],
      'p',
    );
    await sync.start();
    cols.customers.stream.emit({
      operationType: 'delete',
      documentKey: { _id: new Int32(1) },
    });
    cols.products.stream.emit({
      operationType: 'delete',
      documentKey: { _id: new Int32(9) },
    });
    await tick(40);
    const tombLog = new FakeMongoDb(cols).collection('sl_edit_tombstones');
    // Both collections' deletes persisted a tombstone log entry.
    expect(
      tombLog.docs.filter((d) => d['collection'] === 'customers'),
    ).toHaveLength(1);
    expect(
      tombLog.docs.filter((d) => d['collection'] === 'products'),
    ).toHaveLength(1);

    await sync.forgetTombstones('customers');

    expect(
      (sync as unknown as { _tombstones: Map<string, unknown> })._tombstones.has(
        'customers',
      ),
    ).toBe(false);
    // Only the forgotten collection's log entries are removed.
    expect(
      tombLog.docs.filter((d) => d['collection'] === 'customers'),
    ).toHaveLength(0);
    expect(
      tombLog.docs.filter((d) => d['collection'] === 'products'),
    ).toHaveLength(1);
    await sync.stop();
  });

  it('forgetTombstones swallows a failed persistent-log delete — the in-memory guard still clears', async () => {
    const tombstoneLog = {
      updateOne: vi.fn(async () => ({})),
      deleteMany: vi.fn(async () => {
        throw new Error('mongo unavailable');
      }),
    };
    const cols = {
      customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]),
      sl_edit_tombstones: tombstoneLog,
    };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols as never) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    await expect(sync.forgetTombstones('customers')).resolves.toBeUndefined();
    expect(tombstoneLog.deleteMany).toHaveBeenCalledWith({
      collection: 'customers',
    });
    await sync.stop();
  });

  it('echo-suppresses a peer-applied delete', async () => {
    process.env['SL_EDIT_DELETE_DEBOUNCE_MS'] = '10';
    const cols = {
      customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]),
    };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    // Produce + apply a tombstone (sets the applied-hash for _id=1).
    cols.customers.stream.emit({
      operationType: 'delete',
      documentKey: { _id: new Int32(1) },
    });
    await tick(40);
    conn.fire(asPeerHead(lastRef(conn.send, 'customers')));
    await (sync as unknown as { _applyChain: Map<string, Promise<void>> })._applyChain.get('customers');
    conn.send.mockClear();

    // The delete change stream event that the apply itself caused is an echo.
    cols.customers.stream.emit({
      operationType: 'delete',
      documentKey: { _id: new Int32(1) },
    });
    await tick(40);
    expect(conn.send).not.toHaveBeenCalled();
    await sync.stop();
  });

  it('a collection drop/rename discards the stale manifest and rebuilds it from Mongo, instead of leaving old entries XORed into the root forever', async () => {
    const cols = {
      customers: new FakeCollection([
        { _id: new Int32(1), name: 'Alice' },
        { _id: new Int32(2), name: 'Bob' },
      ]),
    };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    expect(cols.customers.findCalls).toBe(1);

    // Out-of-band `mongorestore --drop`: Mongo now holds a completely different
    // set of documents. On the real driver a single-collection `drop` is
    // ALWAYS immediately followed by `invalidate`, which kills this cursor
    // server-side — no per-doc delete for the old rows, and no further event
    // will ever arrive on it, live or not.
    const deadStream = cols.customers.stream;
    cols.customers.docs = [{ _id: new Int32(9), name: 'Zoe' }];
    deadStream.emit({ operationType: 'drop' });
    deadStream.emit({ operationType: 'invalidate' });
    await tick();

    // The manifest must be re-derived from Mongo's current content: exactly the
    // new doc, none of the old ones lingering (which would XOR into the root
    // forever and never match a peer that only ever saw the new content).
    const manifest = (
      sync as unknown as { _manifest: Map<string, Map<string, string>> }
    )._manifest.get('customers');
    expect(manifest ? [...manifest.keys()] : []).toEqual(['9']);
    expect(cols.customers.findCalls).toBe(2);

    // `invalidate` must have opened a FRESH cursor — the real driver never
    // delivers another event on the dropped one (this is what MongoDB's own
    // `invalidate` contract guarantees; the point under test is that OUR code
    // reacts to it by opening a replacement instead of going quiet forever).
    expect(cols.customers.watchCalls).toBe(2);
    expect(cols.customers.stream).not.toBe(deadStream);

    // A live change arriving on the new cursor still reaches the manifest —
    // sync did not silently stop after the drop.
    cols.customers.stream.emit({
      operationType: 'insert',
      fullDocument: { _id: new Int32(11), name: 'Nadia' },
    });
    await tick();
    expect(
      (
        sync as unknown as { _manifest: Map<string, Map<string, string>> }
      )._manifest.get('customers')?.has('11'),
    ).toBe(true);

    // The vanished sliceIds (1, 2) must be tombstoned, not just dropped — a
    // peer that still holds them needs to be TOLD to delete them, or it
    // additively backfills them right back the moment it sees this node as
    // "missing" content it used to have (the exact resurrection this guards
    // against).
    const tombstones = (
      sync as unknown as { _tombstones: Map<string, Map<string, unknown>> }
    )._tombstones.get('customers');
    expect(tombstones ? [...tombstones.keys()].sort() : []).toEqual(['1', '2']);

    // A `rename` gets the same treatment as `drop` (both carry the same "the
    // old content is gone" meaning); its own trailing `invalidate` only
    // reopens the cursor, it does not re-trigger the scan this line already did.
    cols.customers.docs = [{ _id: new Int32(10), name: 'Yara' }];
    cols.customers.stream.emit({ operationType: 'rename' });
    cols.customers.stream.emit({ operationType: 'invalidate' });
    await tick();
    expect(cols.customers.findCalls).toBe(3);
    expect(cols.customers.watchCalls).toBe(3);
    const manifest2 = (
      sync as unknown as { _manifest: Map<string, Map<string, string>> }
    )._manifest.get('customers');
    expect(manifest2 ? [...manifest2.keys()] : []).toEqual(['10']);

    await sync.stop();
  });

  it('blocks a mass-delete burst (guard) and cancels timers on stop', async () => {
    process.env['SL_EDIT_DELETE_DEBOUNCE_MS'] = '10';
    process.env['SL_EDIT_DELETE_FRACTION'] = '0.3';
    // baseline 10 → fraction guard active, limit = ceil(10*0.3) = 3.
    const docs = Array.from({ length: 10 }, (_, i) => ({ _id: new Int32(i) }));
    const cols = { customers: new FakeCollection(docs) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    conn.send.mockClear();

    // 3 deletes >= limit(3) → whole burst dropped, no tombstone broadcast.
    for (const i of [0, 1, 2]) {
      cols.customers.stream.emit({
        operationType: 'delete',
        documentKey: { _id: new Int32(i) },
      });
    }
    await tick(40);
    expect(conn.send).not.toHaveBeenCalled();
    // A blocked id must not be tombstoned either — that would leak the
    // "blocked" delete to a peer via anti-entropy's own bucket-serving
    // (`hasTombstone`), which the guard does not gate at all. Reproduced
    // live: an 8-doc burst the guard correctly logged BLOCKED still cost the
    // peer its first id, tombstoned the instant the change-stream event
    // arrived — well before the debounced guard decision even ran.
    expect(
      (sync as unknown as { _tombstones: Map<string, Map<string, unknown>> })
        ._tombstones.get('customers')
        ?.has('0'),
    ).toBe(false);
    await sync.stop();
  });

  it('lets an ANNOUNCED mass delete through', async () => {
    // The guard cannot tell a deliberate reset from an accidental wipe. The
    // lab lowers the threshold to 5 so its guard recipe can trip it, which made
    // every probe-collection reset trip it too: one node blocked the reset,
    // kept its documents, and the mesh held divergent roots for the rest of
    // the run while four other recipes reported missing documents.
    process.env['SL_EDIT_DELETE_DEBOUNCE_MS'] = '10';
    process.env['SL_EDIT_DELETE_FRACTION'] = '0.3';
    const docs = Array.from({ length: 10 }, (_, i) => ({ _id: new Int32(i) }));
    const cols = { customers: new FakeCollection(docs) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();

    const heads = () =>
      conn.send.mock.calls
        .map((c: unknown[]) => String(c[0]))
        .filter((r: string) => !r.startsWith('~R~'));
    const burst = async () => {
      for (const i of [0, 1, 2]) {
        cols.customers.stream.emit({
          operationType: 'delete',
          documentKey: { _id: new Int32(i) },
        });
      }
      await tick(40);
    };

    // Unannounced: blocked, and nothing is applied — so the baseline is
    // unchanged and the identical burst below is a like-for-like comparison.
    conn.send.mockClear();
    await burst();
    expect(heads()).toEqual([]);

    // Announced: the same burst propagates as tombstones.
    conn.send.mockClear();
    sync.expectMassDelete('customers');
    await burst();
    expect(heads().length).toBeGreaterThan(0);

    await sync.stop();
  });

  it('ignores a delete without a documentKey _id, and _flushDeletes is a no-op when nothing is pending', async () => {
    const cols = { customers: new FakeCollection([]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    conn.send.mockClear();
    cols.customers.stream.emit({ operationType: 'delete', documentKey: {} });
    await tick();
    expect(conn.send).not.toHaveBeenCalled();
    // Direct flush with an empty buffer hits the early-return guard.
    await (sync as unknown as { _flushDeletes: (c: string) => Promise<void> })._flushDeletes('customers');
    // Flush a pending delete on an empty collection: the live baseline is
    // manifest(0) + pending(1) = 1, below the fraction-guard floor of 10, so
    // only the absolute cap applies and the tombstone propagates.
    const internals = sync as unknown as {
      _pendingDeletes: Map<string, Set<unknown>>;
      _flushDeletes: (c: string) => Promise<void>;
    };
    internals._pendingDeletes.set('customers', new Set([new Int32(1)]));
    await internals._flushDeletes('customers');
    expect(conn.send).toHaveBeenCalledWith(expect.stringMatching(/^customers:/));
    await sync.stop();
  });

  it('cancels a pending delete-flush timer on stop', async () => {
    process.env['SL_EDIT_DELETE_DEBOUNCE_MS'] = '5000'; // long → won't fire
    const cols = { customers: new FakeCollection([{ _id: new Int32(1) }]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    // Buffer a delete so a flush timer is pending, then stop before it fires.
    cols.customers.stream.emit({
      operationType: 'delete',
      documentKey: { _id: new Int32(1) },
    });
    await sync.stop();
    expect(
      (sync as unknown as { _deleteTimers: Map<string, unknown> })._deleteTimers.size,
    ).toBe(0);
  });

  it('does not broadcast a tombstone when putDoc yields no head', async () => {
    process.env['SL_EDIT_DELETE_DEBOUNCE_MS'] = '10';
    const cols = { customers: new FakeCollection([{ _id: new Int32(1) }]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(
      new FakeMongoDb(cols) as never,
      await mkRljsonDb(),
      conn,
      ['customers'],
      'p',
    );
    await sync.start();
    conn.send.mockClear();
    (sync as unknown as { _adapter: { putDoc: unknown } })._adapter.putDoc = vi
      .fn()
      .mockResolvedValue(null);
    cols.customers.stream.emit({
      operationType: 'delete',
      documentKey: { _id: new Int32(1) },
    });
    await tick(40);
    expect(conn.send).not.toHaveBeenCalled();
    await sync.stop();
  });

  it('falls back to send when the connector has no reannounce', async () => {
    process.env['SL_EDIT_HEARTBEAT_MS'] = '25';
    const cols = { customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]) };
    const base = mkConnector();
    const conn: EditSyncConnector = { send: base.send, listen: base.listen };
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    // Live change → a head exists to re-announce on the heartbeat.
    cols.customers.stream.emit({ operationType: 'insert', fullDocument: { _id: new Int32(2), name: 'B' } });
    await tick();
    base.send.mockClear();
    await tick(70);
    expect(base.send).toHaveBeenCalledWith(expect.stringMatching(/^customers:/));
    await sync.stop();
  });

  // ----- content root (git-like, content-deterministic convergence) -----

  it('broadcasts a content root at snapshot, deterministic for the same data', async () => {
    const docs = [
      { _id: new Int32(2), name: 'Bob' },
      { _id: new Int32(1), name: 'Alice' },
    ];
    const connA = mkConnector();
    const a = new MongoEditSync(new FakeMongoDb({ customers: new FakeCollection([...docs]) }) as never, await mkRljsonDb(), connA, ['customers'], 'p');
    await a.start();
    const refA = rootRefOf(connA, 'customers');
    expect(refA).toMatch(/^~R~customers:[0-9a-f]{64}$/);

    // A second node with the SAME docs (any order) computes the SAME root.
    const connB = mkConnector();
    const b = new MongoEditSync(new FakeMongoDb({ customers: new FakeCollection([...docs].reverse()) }) as never, await mkRljsonDb(), connB, ['customers'], 'p');
    await b.start();
    expect(rootRefOf(connB, 'customers')).toBe(refA);
    await a.stop();
    await b.stop();
  });

  it('content root is the incremental XOR of per-entry digests (O(1), never re-hashes the manifest)', async () => {
    // The root is a maintained XOR accumulator, not a re-hash of the whole
    // manifest — that full re-hash on a multi-million-entry manifest (cd_models)
    // pegged the hub via the heartbeat. This pins the scheme AND checks every
    // incremental path: add, no-op, update, delete, delete-of-absent.
    const cols = {
      customers: new FakeCollection([
        { _id: new Int32(2), name: 'Bob' },
        { _id: new Int32(1), name: 'Alice' },
        { _id: new Int32(3), name: 'Carol' },
      ]),
    };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    const internals = sync as unknown as {
      _manifest: Map<string, Map<string, string>>;
      _contentRoot: (c: string) => string;
      _setManifest: (c: string, id: unknown, hash: string | null) => void;
    };
    const manifest = internals._manifest.get('customers') as Map<string, string>;

    // Pin the scheme: root === hex of XOR over sha256(sliceId | docHash).
    const xor = (m: Map<string, string>): string => {
      const acc = Buffer.alloc(32);
      for (const [id, h] of m) {
        const d = createHash('sha256').update(id).update('|').update(h).digest();
        for (let i = 0; i < 32; i++) acc[i] ^= d[i];
      }
      return acc.toString('hex');
    };
    const root0 = internals._contentRoot('customers');
    expect(root0).toBe(xor(manifest));
    expect(root0).toMatch(/^[0-9a-f]{64}$/);

    // Add a new entry → root moves; adding the identical entry again is a no-op.
    internals._setManifest('customers', 999, 'a'.repeat(64));
    const rootAdded = internals._contentRoot('customers');
    expect(rootAdded).not.toBe(root0);
    expect(rootAdded).toBe(xor(manifest));
    internals._setManifest('customers', 999, 'a'.repeat(64));
    expect(internals._contentRoot('customers')).toBe(rootAdded);

    // Removing that entry returns the root to the original (XOR self-inverse);
    // deleting an id that was never present is a no-op.
    internals._setManifest('customers', 999, null);
    expect(internals._contentRoot('customers')).toBe(root0);
    internals._setManifest('customers', 424242, null);
    expect(internals._contentRoot('customers')).toBe(root0);

    // Changing an existing doc's hash moves the root and stays scheme-correct.
    internals._setManifest('customers', 1, 'b'.repeat(64));
    expect(internals._contentRoot('customers')).toBe(xor(manifest));
    expect(internals._contentRoot('customers')).not.toBe(root0);
    await sync.stop();
  });

  it('checkpoints a mega collection on the longer interval — never skips it', async () => {
    // This used to be a hard cap that skipped the checkpoint entirely, so a
    // collection above it kept NO resume token and re-read every document on
    // every restart. The fleet's 563k-document catalog sat just above the old
    // 500k cap: 2-3 minutes and ~7.8 GB per hub restart, with nodes starting in
    // that window left behind. Writing is streamed now, so size only decides
    // how OFTEN — never whether.
    process.env['SL_EDIT_SAVE_DEBOUNCE_MS'] = '5';
    process.env['SL_EDIT_CHECKPOINT_LARGE_ENTRIES'] = '1';
    process.env['SL_EDIT_CHECKPOINT_LARGE_DEBOUNCE_MS'] = '40';
    // Seeded, so the manifest is already large when the first live change
    // decides which interval to use — exactly the production shape, where the
    // snapshot has filled it long before anything is edited.
    const cols = {
      customers: new FakeCollection([
        { _id: new Int32(1), name: 'A' },
        { _id: new Int32(2), name: 'B' },
      ]),
    };
    const cp = new FakeCheckpoint();
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p', undefined, cp as never);
    await sync.start();
    cols.customers.stream.emit({ _id: { tk: 'B' }, operationType: 'insert', fullDocument: { _id: new Int32(3), name: 'C' } });
    await tick();
    // The manifest is over the "large" threshold, so the short debounce has
    // NOT fired …
    expect(cp.save).not.toHaveBeenCalled();
    // … but the long one does, and the token goes to disk.
    await tick(80);
    expect(cp.save).toHaveBeenCalledTimes(1);
    expect(cp.saved[0].token).toEqual({ tk: 'B' });
    await sync.stop();
    delete process.env['SL_EDIT_SAVE_DEBOUNCE_MS'];
    delete process.env['SL_EDIT_CHECKPOINT_LARGE_ENTRIES'];
    delete process.env['SL_EDIT_CHECKPOINT_LARGE_DEBOUNCE_MS'];
    delete process.env['SL_EDIT_APPLIED_MAX'];
    delete process.env['SL_EDIT_LWW_MAX'];
  });

  it('a head tagged with a root we already hold is a no-op (converged reconnect)', async () => {
    const cols = { customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    // A live change gives us a head + updates our content root.
    cols.customers.stream.emit({ operationType: 'insert', fullDocument: { _id: new Int32(2), name: 'B' } });
    await tick();
    const head = headFor(conn.send, 'customers');

    // The head carries the root it produces, and that root is already ours →
    // nothing to fetch, no pull.
    conn.fire(head);
    await (sync as unknown as { _applyChain: Map<string, Promise<void>> })._applyChain.get('customers');
    expect(cols.customers.replaceOne).not.toHaveBeenCalled();
    await sync.stop();
  });

  it('a head tagged with a root we do NOT hold is pulled', async () => {
    const cols = { customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    cols.customers.stream.emit({ operationType: 'insert', fullDocument: { _id: new Int32(2), name: 'B' } });
    await tick();
    const head = headFor(conn.send, 'customers');

    conn.fire(asPeerHead(head));
    await (sync as unknown as { _applyChain: Map<string, Promise<void>> })._applyChain.get('customers');
    expect(cols.customers.replaceOne).toHaveBeenCalledTimes(1);
    await sync.stop();
  });

  it('an untagged head (peer on an older build) is always pulled, never skipped', async () => {
    const cols = { customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    const adapter = (sync as unknown as { _adapter: { collectPuts: unknown } })._adapter;
    adapter.collectPuts = vi.fn().mockResolvedValue({
      puts: [{ collection: 'customers', sliceId: '2', doc: { _id: new Int32(2), name: 'B' } }],
      complete: true,
      sealed: ['HEAD_FROM_OLD_PEER'],
    });

    // No `|root` suffix → we cannot prove we already hold it, so we pull.
    conn.fire('customers:HEAD_FROM_OLD_PEER');
    await (sync as unknown as { _applyChain: Map<string, Promise<void>> })._applyChain.get('customers');
    expect(cols.customers.replaceOne).toHaveBeenCalledTimes(1);
    await sync.stop();
  });

  it('a third node’s matching root cannot swallow another peer’s head', async () => {
    // REGRESSION (live-diagnosed on four demo PCs, 2026-08-17): the skip used to
    // consult one shared "last root any peer announced" slot. With three or more
    // nodes, node C’s root — identical to ours because C and we are converged —
    // arrived between node B’s head and its root, and made us drop B’s head.
    // Inserts and deletes vanished silently and the fleet stayed diverged with no
    // repair path. Correctness must not depend on how many peers are talking.
    const cols = { customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    const ourRoot = (rootRefOf(conn, 'customers') as string).split(':')[1];
    const adapter = (sync as unknown as { _adapter: { collectPuts: unknown } })._adapter;
    adapter.collectPuts = vi.fn().mockResolvedValue({
      puts: [{ collection: 'customers', sliceId: '2', doc: { _id: new Int32(2), name: 'B' } }],
      complete: true,
      sealed: ['HEAD_FROM_B'],
    });

    // Node C is converged with us and says so.
    conn.fire(`~R~customers:${ourRoot}`);
    // Node B is AHEAD and sends a head carrying the root it produces.
    conn.fire(`customers:HEAD_FROM_B${'|'}${FOREIGN_ROOT}`);
    await (sync as unknown as { _applyChain: Map<string, Promise<void>> })._applyChain.get('customers');

    // B's change must land despite C's matching root.
    expect(cols.customers.replaceOne).toHaveBeenCalledTimes(1);
    await sync.stop();
  });

  it('ignores malformed and unknown-collection root refs', async () => {
    const cols = { customers: new FakeCollection([{ _id: new Int32(1) }]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    const chain = () =>
      (sync as unknown as { _applyChain: Map<string, Promise<void>> })._applyChain.get('customers');
    const adapter = (sync as unknown as { _adapter: { collectPuts: unknown } })._adapter;
    // The pull comes back empty → the head is NOT latched, so it stays pending
    // and a later divergent root is what re-drives it.
    const collectPuts = vi
      .fn()
      .mockResolvedValue({ puts: [], complete: true, sealed: [] });
    adapter.collectPuts = collectPuts;
    conn.fire(`customers:HEAD_P${'|'}${FOREIGN_ROOT}`);
    await chain();
    collectPuts.mockClear();

    conn.fire('~R~no-colon-here'); // malformed → ignored
    conn.fire('~R~otherColl:abc'); // unknown collection → ignored
    await tick();
    expect(collectPuts).not.toHaveBeenCalled();

    // A valid, divergent root for a synced collection DOES re-drive the head —
    // and now the rows are resolvable, so the change finally lands.
    collectPuts.mockResolvedValue({
      puts: [{ collection: 'customers', sliceId: '2', doc: { _id: new Int32(2) } }],
      complete: true,
      sealed: [],
    });
    conn.fire('~R~customers:abc123');
    await chain();
    expect(collectPuts).toHaveBeenCalled();
    expect(cols.customers.replaceOne).toHaveBeenCalledTimes(1);
    await sync.stop();
  });

  it('re-broadcasts the content root (debounced) on a live change and a delete', async () => {
    const cols = { customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    const root0 = rootRefOf(conn, 'customers');

    cols.customers.stream.emit({
      operationType: 'insert',
      fullDocument: { _id: new Int32(2), name: 'B' },
    });
    await tick(30); // past the 5ms root debounce
    const root1 = rootRefOf(conn, 'customers');
    expect(root1).not.toBe(root0);

    cols.customers.stream.emit({
      operationType: 'delete',
      documentKey: { _id: new Int32(2) },
    });
    await tick(30);
    const root2 = rootRefOf(conn, 'customers');
    expect(root2).toBe(root0); // back to the pre-insert manifest → same root
    await sync.stop();
  });

  it('cancels a pending root timer on stop', async () => {
    const cols = { customers: new FakeCollection([{ _id: new Int32(1) }]) };
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p');
    await sync.start();
    cols.customers.stream.emit({
      operationType: 'insert',
      fullDocument: { _id: new Int32(9), name: 'Z' },
    });
    // stop before the 5ms debounce fires
    await sync.stop();
    expect(
      (sync as unknown as { _rootTimers: Map<string, unknown> })._rootTimers.size,
    ).toBe(0);
  });

  // ---- Resume token / checkpoint ----

  it('resumes from a checkpoint: restores the manifest, reopens with resumeAfter, no full scan', async () => {
    const cols = { customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]) };
    const cp = new FakeCheckpoint({
      customers: { manifest: { '1': 'h1', '2': 'h2' }, token: { tk: 'T1' } },
    });
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p', undefined, cp as never);
    await sync.start();
    // Reopened from the token, and NO full collection scan.
    expect(cols.customers.watchOpts?.['resumeAfter']).toEqual({ tk: 'T1' });
    expect(cols.customers.findCalls).toBe(0);
    // Manifest restored → content root known without reading docs.
    const manifest = (sync as unknown as {
      _manifest: Map<string, Map<string, string>>;
    })._manifest.get('customers');
    expect(manifest?.size).toBe(2);
    expect(manifest?.get('2')).toBe('h2');
    await sync.stop();
  });

  it('does a full snapshot when the checkpoint has no token yet', async () => {
    const cols = { customers: new FakeCollection([{ _id: new Int32(1) }]) };
    const cp = new FakeCheckpoint({ customers: { manifest: {}, token: null } });
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p', undefined, cp as never);
    await sync.start();
    expect(cols.customers.findCalls).toBe(1); // fell through to a full snapshot
    expect(cols.customers.watchOpts?.['resumeAfter']).toBeUndefined();
    await sync.stop();
  });

  it('saves a checkpoint (manifest + latest token) after changes, debounced', async () => {
    process.env['SL_EDIT_SAVE_DEBOUNCE_MS'] = '5';
    const cols = { customers: new FakeCollection([]) };
    const cp = new FakeCheckpoint(); // no state → full snapshot
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p', undefined, cp as never);
    await sync.start();

    // Two changes inside one debounce window: the 2nd hits the "timer exists" path.
    cols.customers.stream.emit({ _id: { tk: 'TK1' }, operationType: 'insert', fullDocument: { _id: new Int32(5), name: 'E' } });
    cols.customers.stream.emit({ _id: { tk: 'TK2' }, operationType: 'insert', fullDocument: { _id: new Int32(6), name: 'F' } });
    await tick();
    expect(cp.save).toHaveBeenCalled();
    const last = cp.saved.at(-1);
    expect(last?.token).toEqual({ tk: 'TK2' }); // latest token wins
    expect(last?.manifest['5']).toBeDefined();
    expect(last?.manifest['6']).toBeDefined();
    await sync.stop();
    delete process.env['SL_EDIT_SAVE_DEBOUNCE_MS'];
  });

  it('flushes a pending checkpoint on stop (no token lost on shutdown)', async () => {
    const cols = { customers: new FakeCollection([]) };
    const cp = new FakeCheckpoint();
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p', undefined, cp as never);
    await sync.start();
    cols.customers.stream.emit({ _id: { tk: 'FINAL' }, operationType: 'insert', fullDocument: { _id: new Int32(7) } });
    await tick(); // let _onChange record the token (debounce still pending, 1000ms default)
    await sync.stop(); // stop flushes the pending checkpoint
    expect(cp.saved.at(-1)?.token).toEqual({ tk: 'FINAL' });
  });

  it('falls back to a full snapshot when the resume token is too old (stream error)', async () => {
    const cols = { customers: new FakeCollection([{ _id: new Int32(1), name: 'A' }]) };
    const cp = new FakeCheckpoint({ customers: { manifest: { '1': 'h' }, token: { tk: 'OLD' } } });
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p', undefined, cp as never);
    await sync.start();
    expect(cols.customers.findCalls).toBe(0); // resumed, no scan yet

    cols.customers.stream.emitError(new Error('resume of change stream was not possible'));
    cols.customers.stream.emitError(new Error('second error is ignored')); // re-entry guard
    await tick();
    expect(cols.customers.findCalls).toBe(1); // fell back to a full snapshot
    await sync.stop();
  });
  describe('health — what nothing outside this class could see', () => {
    // `MongoAgent.getSyncHealth().changeStreamAlive` belongs to the LEGACY
    // tree sync, which is never started when this one is. So on the lab build
    // every node reported a dead change stream, the Dashboard raised an ERROR
    // on all four machines, and the sync was working the whole time.

    it('reports the streams it actually has open', async () => {
      const cols = {
        customers: new FakeCollection([{ _id: new Int32(1), name: 'Alice' }]),
        orders: new FakeCollection([]),
      };
      const sync = new MongoEditSync(
        new FakeMongoDb(cols) as never,
        await mkRljsonDb(),
        mkConnector(),
        ['customers', 'orders'],
        'p',
      );

      // Told about two collections, watching neither yet: `watching` is what
      // this sync is responsible for, `open` is what it actually holds — and
      // the gap between them is the thing worth seeing.
      expect(sync.health()).toMatchObject({ watching: 2, open: 0 });

      await sync.start();

      expect(sync.health()).toMatchObject({ watching: 2, open: 2 });
    });

    it('has no last change before one has arrived, and one after', async () => {
      const cols = { customers: new FakeCollection([]) };
      const sync = new MongoEditSync(
        new FakeMongoDb(cols) as never,
        await mkRljsonDb(),
        mkConnector(),
        ['customers'],
        'p',
      );
      await sync.start();

      expect(sync.health().lastChangeAt).toBeNull();

      cols.customers.stream.emit({
        operationType: 'insert',
        fullDocument: { _id: new Int32(1), name: 'Carol' },
      });
      await tick();

      expect(sync.health().lastChangeAt).toBeGreaterThan(0);
    });

    describe('the state ref two nodes compare', () => {
      /**
       * A sync over the given documents.
       * @param docs - Per collection.
       * @returns The started sync.
       */
      const syncOver = async (
        docs: Record<string, { _id: Int32; name: string }[]>,
      ): Promise<MongoEditSync> => {
        const cols: Record<string, FakeCollection> = {};
        for (const [name, rows] of Object.entries(docs)) {
          cols[name] = new FakeCollection(rows);
        }
        const sync = new MongoEditSync(
          new FakeMongoDb(cols) as never,
          await mkRljsonDb(),
          mkConnector(),
          Object.keys(docs),
          'p',
        );
        await sync.start();
        return sync;
      };

      it('is the same on two nodes holding the same data', async () => {
        // The whole point: order-independent, so two nodes derive it without
        // exchanging anything.
        const a = await syncOver({
          customers: [
            { _id: new Int32(1), name: 'Alice' },
            { _id: new Int32(2), name: 'Bob' },
          ],
        });
        const b = await syncOver({
          customers: [
            { _id: new Int32(2), name: 'Bob' },
            { _id: new Int32(1), name: 'Alice' },
          ],
        });

        expect(a.health().stateRef).toBe(b.health().stateRef);
        expect(a.health().stateRef).toMatch(/^[0-9a-f]{64}$/);
      });

      it('differs when the data differs', async () => {
        const a = await syncOver({
          customers: [{ _id: new Int32(1), name: 'Alice' }],
        });
        const b = await syncOver({
          customers: [{ _id: new Int32(1), name: 'Alicia' }],
        });

        expect(a.health().stateRef).not.toBe(b.health().stateRef);
      });

      it('differs when the same documents sit in a different collection', async () => {
        // The collection name is folded in, so a document that moved still
        // shows — XOR-ing the roots alone would have hidden it.
        const a = await syncOver({
          customers: [{ _id: new Int32(1), name: 'Alice' }],
        });
        const b = await syncOver({
          orders: [{ _id: new Int32(1), name: 'Alice' }],
        });

        expect(a.health().stateRef).not.toBe(b.health().stateRef);
      });

      it('breaks down per collection, so a matrix can say WHICH', async () => {
        // The node-level fold conflates "the shared data disagrees" with
        // "these machines hold different collections". On the lab both were
        // true at once and the grid could only say "different".
        const sync = await syncOver({
          customers: [{ _id: new Int32(1), name: 'Alice' }],
          orders: [],
        });

        const { roots } = sync.health();

        expect(Object.keys(roots).sort()).toEqual(['customers', 'orders']);
        expect(roots['customers']).toMatch(/^[0-9a-f]{64}$/);
        // An empty collection is 64 zeros — a real root, not a missing one.
        expect(roots['orders']).toBe('0'.repeat(64));
      });

      it('gives the same collection the same root on two nodes', async () => {
        const a = await syncOver({
          shared: [{ _id: new Int32(1), name: 'Alice' }],
          extra: [{ _id: new Int32(9), name: 'Only here' }],
        });
        const b = await syncOver({
          shared: [{ _id: new Int32(1), name: 'Alice' }],
        });

        // The node roots differ — one has a collection the other lacks — but
        // the shared collection agrees, which is the actionable half.
        expect(a.health().stateRef).not.toBe(b.health().stateRef);
        expect(a.health().roots['shared']).toBe(b.health().roots['shared']);
      });

      it('has nothing to say before it has worked its state out', async () => {
        // The only honest unknown: this node has not finished looking.
        const sync = new MongoEditSync(
          new FakeMongoDb({}) as never,
          await mkRljsonDb(),
          mkConnector(),
          [],
          'p',
        );

        expect(sync.health().stateRef).toBeNull();
      });

      it('says an empty database holds nothing, which two of them share', async () => {
        // A database that was deliberately emptied HAS a state, and two of
        // them hold the same one. Reporting "unknown" there left four lab
        // machines reset to a common empty baseline reading as unknowable
        // rather than as agreed.
        const a = await syncOver({});
        const b = await syncOver({});

        expect(a.health().stateRef).toBe('0'.repeat(64));
        expect(a.health().stateRef).toBe(b.health().stateRef);
      });

      it('does not confuse an empty database with one empty collection', async () => {
        // The fold hashes the name alongside the root, so one empty
        // collection is sha256(name|zeros) — never zeros.
        const empty = await syncOver({});
        const oneEmpty = await syncOver({ orders: [] });

        expect(oneEmpty.health().stateRef).not.toBe(empty.health().stateRef);
      });
    });
  });

});

// .............................................................................

describe('MongoEditSync — a restart with a durable store', () => {
  /**
   * A durable Io, from the sync's point of view: the SAME Db across two
   * sequential syncs. With an in-memory one the table is empty again after a
   * restart and a fresh lineage is the consistent answer; it is the durable
   * case that forks.
   * @returns A sync over the given db, collections and checkpoint.
   */
  const mkSync = (
    db: Db,
    cols: Record<string, FakeCollection>,
    conn: ReturnType<typeof mkConnector>,
    cp: FakeCheckpoint,
  ): MongoEditSync =>
    new MongoEditSync(
      new FakeMongoDb(cols) as never,
      db,
      conn,
      ['customers'],
      'p',
      undefined,
      cp as never,
    );

  it('continues the chain instead of starting a second one', async () => {
    const db = await mkRljsonDb();
    const cp = new FakeCheckpoint();
    const cols = { customers: new FakeCollection([]) };

    // --- first run: one live change, so a head exists ---
    const connA = mkConnector();
    const syncA = mkSync(db, cols, connA, cp);
    await syncA.start();
    cols.customers.stream.emit({
      operationType: 'insert',
      fullDocument: { _id: new Int32(1), name: 'A' },
    });
    await tick(40);
    const adapterA = (syncA as unknown as { _adapter: { headRef: (c: string) => string | null } })._adapter;
    const headA = adapterA.headRef('customers');
    expect(headA).toBeTruthy();
    await syncA.stop();

    // The head was checkpointed alongside the token.
    expect(cp.state['customers']?.head).toBe(headA);

    // --- restart onto the same store ---
    const syncB = mkSync(db, cols, mkConnector(), cp);
    await syncB.start();
    const adapterB = (syncB as unknown as { _adapter: { headRef: (c: string) => string | null } })._adapter;

    // THE point: the new sync picked up where the old one left off. Without
    // this the next edit starts a lineage whose `previous` never reaches
    // `headA`, and a peer walking back stops at the fork — so everything
    // written before the restart reads as absent while sitting in the table.
    expect(adapterB.headRef('customers')).toBe(headA);
    await syncB.stop();
  });

  it('starts a fresh chain when the store was wiped under the checkpoint', async () => {
    // A checkpoint can outlive the store it describes: a cleared cache
    // directory, a restore from backup. Refusing to start would strand the
    // node over a resumption that is an optimisation, not a requirement.
    const db = await mkRljsonDb();
    const cp = new FakeCheckpoint({
      customers: { manifest: {}, token: null, head: 'HEAD_NOT_IN_THIS_STORE' },
    });
    const cols = { customers: new FakeCollection([]) };
    const sync = mkSync(db, cols, mkConnector(), cp);
    await sync.start();
    const adapter = (sync as unknown as { _adapter: { headRef: (c: string) => string | null } })._adapter;
    expect(adapter.headRef('customers')).toBeNull();
    await sync.stop();
  });
});
