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
  replaceOne = vi.fn(async () => ({}));
  deleteOne = vi.fn(async () => ({}));
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
  watch(_pipeline: unknown, opts?: Record<string, unknown>): FakeChangeStream {
    this.watchOpts = opts;
    return this.stream;
  }
}

class FakeMongoDb {
  constructor(private _cols: Record<string, FakeCollection>) {}
  collection(name: string): FakeCollection {
    return (this._cols[name] ??= new FakeCollection());
  }
}

/** In-memory stand-in for EditCheckpoint (no filesystem). */
class FakeCheckpoint {
  saved: Array<{ token: unknown; manifest: Record<string, string> }> = [];
  load = vi.fn(
    async (
      c: string,
    ): Promise<{ manifest: Record<string, string>; token: unknown } | undefined> =>
      this.state[c],
  );
  save = vi.fn(
    async (c: string, m: Map<string, string>, token: unknown): Promise<void> => {
      this.saved.push({ token, manifest: Object.fromEntries(m) });
    },
  );
  constructor(
    public state: Record<
      string,
      { manifest: Record<string, string>; token: unknown } | undefined
    > = {},
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
} => {
  let cb: ((r: string) => void | Promise<void>) | undefined;
  return {
    send: vi.fn(),
    reannounce: vi.fn(),
    invalidateReceived: vi.fn(),
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

  it('skips the checkpoint for a manifest above the size cap (mega collection)', async () => {
    // A mega collection must not serialize its whole manifest to disk on every
    // change — that stalls the loop as badly as the old root re-hash. Above the
    // cap the checkpoint is skipped (the collection rebuilds by scan on restart).
    process.env['SL_EDIT_SAVE_DEBOUNCE_MS'] = '5';
    process.env['SL_EDIT_CHECKPOINT_MAX_ENTRIES'] = '1';
    const cols = { customers: new FakeCollection([]) };
    const cp = new FakeCheckpoint();
    const conn = mkConnector();
    const sync = new MongoEditSync(new FakeMongoDb(cols) as never, await mkRljsonDb(), conn, ['customers'], 'p', undefined, cp as never);
    await sync.start();
    cols.customers.stream.emit({ _id: { tk: 'A' }, operationType: 'insert', fullDocument: { _id: new Int32(1), name: 'A' } });
    cols.customers.stream.emit({ _id: { tk: 'B' }, operationType: 'insert', fullDocument: { _id: new Int32(2), name: 'B' } });
    await tick();
    expect(cp.save).not.toHaveBeenCalled(); // 2 entries > cap 1 → skipped
    await sync.stop();
    delete process.env['SL_EDIT_SAVE_DEBOUNCE_MS'];
    delete process.env['SL_EDIT_CHECKPOINT_MAX_ENTRIES'];
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
});
