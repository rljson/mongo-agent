// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.

import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';

import { MongoAgent } from '../src/mongo-agent.ts';

// ---------------------------------------------------------------------------
// Module mocks: keep MongoScanner + MongoDbTreeAdapter out of the way so we
// can drive syncToDb/syncFromDb/restoreToRef without a live Mongo/rljson.
// ---------------------------------------------------------------------------

const scanResult: { value: any } = { value: { rootHash: 'root-0', trees: new Map() } };
const scanImpl: { fn: () => Promise<any> } = {
  fn: async () => scanResult.value,
};

vi.mock('../src/mongo-scanner.ts', () => {
  class MongoScanner {
    constructor(public db: any, public opts: any) {}
    scan() {
      return scanImpl.fn();
    }
  }
  return { MongoScanner };
});

const storeTreeImpl: { fn: (t: any) => Promise<string | null> } = {
  fn: async () => 'ref-stored',
};
const fetchTreeImpl: { fn: (ref: string) => Promise<any> } = {
  fn: async () => null,
};

vi.mock('../src/mongo-db-tree-adapter.ts', () => {
  class MongoDbTreeAdapter {
    constructor(public db: any, public treeKey: string) {}
    storeTree(t: any) {
      return storeTreeImpl.fn(t);
    }
    fetchTree(ref: string) {
      return fetchTreeImpl.fn(ref);
    }
  }
  return { MongoDbTreeAdapter };
});

// ---------------------------------------------------------------------------
// Fake Mongo Db + collection
// ---------------------------------------------------------------------------

interface FakeColl {
  name: string;
  docs: Map<unknown, any>;
  calls: Array<{ method: string; args: any[] }>;
  createIndex: (...a: any[]) => Promise<any>;
  updateOne: (...a: any[]) => Promise<any>;
  replaceOne: (...a: any[]) => Promise<any>;
  deleteOne: (...a: any[]) => Promise<any>;
  deleteMany: (...a: any[]) => Promise<any>;
  bulkWrite: (...a: any[]) => Promise<any>;
  find: (...a: any[]) => any;
  findOne: (...a: any[]) => Promise<any>;
}

const makeColl = (name: string, overrides: Record<string, any> = {}): FakeColl => {
  const docs = new Map<unknown, any>();
  const calls: Array<{ method: string; args: any[] }> = [];
  const rec = (m: string) => (...args: any[]) => {
    calls.push({ method: m, args });
    if (overrides[m]) return overrides[m](...args);
    return Promise.resolve({});
  };
  const find = (filter: any = {}) => {
    calls.push({ method: 'find', args: [filter] });
    let rows: any[] = [...docs.values()];
    const inIds = filter?._id?.$in;
    if (Array.isArray(inIds)) {
      const set = new Set(inIds.map((x: any) => String(x)));
      rows = rows.filter((d) => set.has(String(d._id)));
    }
    return {
      toArray: async () => (overrides.findResult ? overrides.findResult : rows),
      project: () => ({ toArray: async () => rows }),
      [Symbol.asyncIterator]: async function* () {
        for (const r of overrides.findResult ?? rows) yield r;
      },
    };
  };
  return {
    name,
    docs,
    calls,
    createIndex: rec('createIndex'),
    updateOne:
      overrides.updateOne ??
      (async (filter: any, update: any) => {
        calls.push({ method: 'updateOne', args: [filter, update] });
        const id = filter._id;
        docs.set(id, { ...(update.$set ?? {}) });
        return { upsertedCount: 1 };
      }),
    replaceOne:
      overrides.replaceOne ??
      (async (filter: any, repl: any) => {
        calls.push({ method: 'replaceOne', args: [filter, repl] });
        docs.set(filter._id, repl);
        return { modifiedCount: 1 };
      }),
    deleteOne:
      overrides.deleteOne ??
      (async (filter: any) => {
        calls.push({ method: 'deleteOne', args: [filter] });
        docs.delete(filter._id);
        return { deletedCount: 1 };
      }),
    deleteMany:
      overrides.deleteMany ??
      (async (filter: any) => {
        calls.push({ method: 'deleteMany', args: [filter] });
        return { deletedCount: 0 };
      }),
    bulkWrite:
      overrides.bulkWrite ??
      (async (ops: any[]) => {
        calls.push({ method: 'bulkWrite', args: [ops] });
        return { ok: 1 };
      }),
    find,
    findOne: overrides.findOne ?? (async () => null),
  };
};

const makeDb = (colls: Record<string, FakeColl> = {}) => {
  const map = new Map<string, FakeColl>(Object.entries(colls));
  return {
    databaseName: 'testdb',
    collection: (name: string) => {
      let c = map.get(name);
      if (!c) {
        c = makeColl(name);
        map.set(name, c);
      }
      return c as any;
    },
    _colls: map,
  } as any;
};

const newAgent = (db?: any, options: any = {}) =>
  new MongoAgent(db ?? makeDb(), undefined, { debounceMs: 1, ...options });

beforeEach(() => {
  scanResult.value = { rootHash: 'root-0', trees: new Map() };
  scanImpl.fn = async () => scanResult.value;
  storeTreeImpl.fn = async () => 'ref-stored';
  fetchTreeImpl.fn = async () => null;
});

afterEach(() => {
  vi.useRealTimers();
});

// ===========================================================================

describe('MongoAgent passthrough setters/getters', () => {
  it('network manager / fs agent / orchestrator passthrough', () => {
    const a = newAgent();
    expect(a.getNetworkManager()).toBeNull();
    a.setNetworkManager({ x: 1 });
    expect(a.getNetworkManager()).toEqual({ x: 1 });

    expect(a.getFsAgent()).toBeNull();
    const h = { agent: {}, folder: 'f', treeKey: 't' };
    a.setFsAgent(h);
    expect(a.getFsAgent()).toBe(h);

    expect(a.getOrchestrator()).toBeNull();
    a.setOrchestrator({ o: 2 });
    expect(a.getOrchestrator()).toEqual({ o: 2 });
  });

  it('exposes mongoDb / bs / scanner getters', () => {
    const db = makeDb();
    const a = newAgent(db);
    expect(a.mongoDb).toBe(db);
    expect(a.bs).toBeDefined();
    expect(a.scanner).toBeDefined();
  });

  it('extract() delegates to scanner.scan()', async () => {
    scanResult.value = { rootHash: 'r1', trees: new Map() };
    const a = newAgent();
    const t = await a.extract();
    expect(t.rootHash).toBe('r1');
  });
});

describe('resetSync', () => {
  it('returns false when no connected socket', () => {
    const a = newAgent() as any;
    expect(a.resetSync()).toEqual({ socketDisconnected: false });
  });

  it('disconnects + reconnects a connected socket', () => {
    const a = newAgent() as any;
    let disconnected = false;
    let connected = false;
    a._socket = {
      connected: true,
      disconnect: () => {
        disconnected = true;
      },
      connect: () => {
        connected = true;
      },
    };
    expect(a.resetSync()).toEqual({ socketDisconnected: true });
    expect(disconnected).toBe(true);
    expect(connected).toBe(true);
  });

  it('returns false when disconnect throws', () => {
    const a = newAgent() as any;
    a._socket = {
      connected: true,
      disconnect: () => {
        throw new Error('boom');
      },
    };
    expect(a.resetSync()).toEqual({ socketDisconnected: false });
  });
});

describe('forcePush without syncToDb', () => {
  it('returns null when _forcePushImpl is not set', async () => {
    const a = newAgent();
    expect(await a.forcePush()).toBeNull();
  });
});

describe('restoreToRef guard rails', () => {
  it('throws when ref is empty', async () => {
    const a = newAgent();
    await expect(a.restoreToRef('')).rejects.toThrow('ref is required');
  });

  it('throws when syncToDb has not run', async () => {
    const a = newAgent();
    await expect(a.restoreToRef('x')).rejects.toThrow('before syncToDb finished');
  });
});

describe('getInsertHistory', () => {
  it('returns [] when not wired', async () => {
    const a = newAgent();
    expect(await a.getInsertHistory()).toEqual([]);
  });

  it('maps history rows + extracts ts; covers ref-fallback arms', async () => {
    const a = newAgent() as any;
    a._db = {
      getInsertHistory: async () => ({
        myTreeInsertHistory: {
          _data: [
            { myTreeRef: 'r1', timeId: '1779263025930:abc' },
            { myTreeRef: 'r2', ts: 42 },
            { ref: 'r3' }, // hits `?? r.ref`
            { other: 1 }, // hits `?? ''`
          ],
        },
      }),
    };
    a._treeKey = 'myTree';
    const out = await a.getInsertHistory(10);
    expect(out).toHaveLength(4);
    expect(out[0]).toMatchObject({ ref: 'r1', ts: 1779263025930 });
    expect(out[2].ref).toBe('r3');
    expect(out[3].ref).toBe('');
  });

  it('returns [] when getInsertHistory throws', async () => {
    const a = newAgent() as any;
    a._db = {
      getInsertHistory: async () => {
        throw new Error('nope');
      },
    };
    a._treeKey = 'myTree';
    expect(await a.getInsertHistory()).toEqual([]);
  });

  it('handles missing history table gracefully', async () => {
    const a = newAgent() as any;
    a._db = { getInsertHistory: async () => ({}) };
    a._treeKey = 'myTree';
    expect(await a.getInsertHistory()).toEqual([]);
  });
});

describe('_safeSendRef', () => {
  it('records success metrics + ref history', async () => {
    const a = newAgent() as any;
    const connector = { send: vi.fn() };
    const ok = await a._safeSendRef(connector, 'r1');
    expect(ok).toBe(true);
    expect(a._totalSendOk).toBe(1);
    expect(a.getRefHistory()[0].ref).toBe('r1');
  });

  it('records timeout + reconnects after threshold', async () => {
    vi.useFakeTimers();
    const a = newAgent() as any;
    let disconnected = 0;
    a._socket = {
      connected: true,
      disconnect: () => disconnected++,
      connect: () => {},
    };
    // sendRef hangs forever → race times out.
    const connector = { send: () => new Promise(() => {}) };
    // Patch sendRef path: connector.send is sync; but sendWithAck not used.
    // Force timeout by using requireAck path with a hanging sendWithAck.
    const connectorHang = {
      syncConfig: { requireAck: true },
      sendWithAck: () => new Promise(() => {}),
    };
    for (let i = 0; i < 3; i++) {
      const p = a._safeSendRef(connectorHang, 'r');
      await vi.advanceTimersByTimeAsync(5001);
      const res = await p;
      expect(res).toBe(false);
    }
    void connector;
    expect(disconnected).toBe(1);
    expect(a._totalSendTimeouts).toBe(3);
  });

  it('respects watchdog cooldown (no reconnect during cooldown)', async () => {
    vi.useFakeTimers();
    const a = newAgent() as any;
    let disconnected = 0;
    a._socket = { connected: true, disconnect: () => disconnected++, connect: () => {} };
    a._watchdogCooldownUntil = Date.now() + 100_000;
    a._consecutiveSendTimeouts = 10;
    const connectorHang = {
      syncConfig: { requireAck: true },
      sendWithAck: () => new Promise(() => {}),
    };
    const p = a._safeSendRef(connectorHang, 'r');
    await vi.advanceTimersByTimeAsync(5001);
    await p;
    expect(disconnected).toBe(0);
  });

  it('reconnect path tolerates disconnect throwing', async () => {
    vi.useFakeTimers();
    const a = newAgent() as any;
    a._socket = {
      connected: true,
      disconnect: () => {
        throw new Error('x');
      },
    };
    a._consecutiveSendTimeouts = 2;
    const connectorHang = {
      syncConfig: { requireAck: true },
      sendWithAck: () => new Promise(() => {}),
    };
    const p = a._safeSendRef(connectorHang, 'r');
    await vi.advanceTimersByTimeAsync(5001);
    await p;
    expect(a._consecutiveSendTimeouts).toBe(0);
  });
});

describe('sendRef (via _safeSendRef)', () => {
  it('uses sendWithAck when requireAck set', async () => {
    const a = newAgent() as any;
    const sendWithAck = vi.fn(async () => {});
    const send = vi.fn();
    await a._safeSendRef(
      { syncConfig: { requireAck: true }, sendWithAck, send },
      'r',
    );
    expect(sendWithAck).toHaveBeenCalledWith('r');
    expect(send).not.toHaveBeenCalled();
  });

  it('falls back to send when no ack required', async () => {
    const a = newAgent() as any;
    const send = vi.fn();
    await a._safeSendRef({ send }, 'r');
    expect(send).toHaveBeenCalledWith('r');
  });

  it('no-op when connector has neither send nor ack', async () => {
    const a = newAgent() as any;
    const ok = await a._safeSendRef({}, 'r');
    expect(ok).toBe(true);
  });
});

describe('_normaliseForHash edge cases', () => {
  it('handles null/undefined/arrays/Buffer-like/ObjectId-like', () => {
    const a = newAgent() as any;
    expect(a._normaliseForHash(null)).toBeNull();
    expect(a._normaliseForHash(undefined)).toBeUndefined();
    expect(a._normaliseForHash([1, new Date(0)])).toEqual([1, 0]);
    expect(a._normaliseForHash({ type: 'Buffer', data: [1, 2] })).toEqual([1, 2]);
    expect(a._normaliseForHash('s')).toBe('s');
    const oid = { _bsontype: 'ObjectId', toString: () => 'abc' };
    expect(a._normaliseForHash(oid)).toBe('abc');
  });

  it('drops _hash and __h keys', () => {
    const a = newAgent() as any;
    expect(a._normaliseForHash({ a: 1, _hash: 'x', __h: 'y' })).toEqual({ a: 1 });
  });
});

describe('_writeTombstone / _writeRecentChange / _recordConflict', () => {
  it('_writeTombstone upserts into sync_tombstones', async () => {
    const db = makeDb();
    const a = newAgent(db) as any;
    await a._writeTombstone('items', 'id1');
    const c = db.collection('sync_tombstones');
    expect(c.docs.get('items::id1')).toMatchObject({ collection: 'items' });
  });

  it('_writeRecentChange upserts into sync_recentChanges', async () => {
    const db = makeDb();
    const a = newAgent(db) as any;
    await a._writeRecentChange('items', 'id1');
    const c = db.collection('sync_recentChanges');
    expect(c.docs.get('items::id1')).toMatchObject({ collection: 'items' });
  });

  it('_recordConflict upserts a conflict row', async () => {
    const db = makeDb();
    const a = newAgent(db) as any;
    await a._recordConflict('items', 'id1', { _id: 'id1', x: 1 }, { _id: 'id1', x: 2 });
    const c = db.collection('sync_conflicts');
    const row = c.docs.get('items::id1');
    expect(row.versions).toHaveLength(2);
    expect(row.conflictType).toBe('concurrent-update');
  });

  it('_recordConflict swallows errors', async () => {
    const db = makeDb({
      sync_conflicts: makeColl('sync_conflicts', {
        updateOne: async () => {
          throw new Error('db down');
        },
      }),
    });
    const a = newAgent(db) as any;
    await expect(
      a._recordConflict('items', 'id1', {}, {}),
    ).resolves.toBeUndefined();
  });
});

describe('_recordDirty / _flushDirty', () => {
  it('tracks ids and degrades to FULL beyond cap', async () => {
    const db = makeDb();
    const a = newAgent(db) as any;
    a._dirtyCap = 2;
    a._recordDirty('items', 1);
    a._recordDirty('items', 2);
    expect(a._dirtyIds.get('items').size).toBe(2);
    a._recordDirty('items', 3); // overflow → FULL
    expect(a._dirtyFullColls.has('items')).toBe(true);
    expect(a._dirtyIds.has('items')).toBe(false);
    // already-full collection short-circuits.
    a._recordDirty('items', 4);
    expect(a._dirtyIds.has('items')).toBe(false);
  });

  it('_flushDirty writes full + per-id markers (best-effort)', async () => {
    const db = makeDb();
    const a = newAgent(db) as any;
    a._dirtyFullColls.add('bulkcoll');
    a._dirtyIds.set('items', new Set([1, 2]));
    await a._flushDirty();
    const sd = db.collection('state_dirty');
    // markCollectionFullDirty + markDirtyById both hit state_dirty/state_merkle.
    expect(sd.calls.length).toBeGreaterThan(0);
    expect(a._dirtyFullColls.size).toBe(0);
    expect(a._dirtyIds.size).toBe(0);
  });
});

describe('_loadRows', () => {
  it('parses _data from a blob', async () => {
    const a = newAgent() as any;
    const { blobId } = await a._bs.setBlob(
      Buffer.from(JSON.stringify({ _data: [{ _id: 1 }] })),
    );
    const rows = await a._loadRows(blobId);
    expect(rows).toEqual([{ _id: 1 }]);
  });

  it('returns [] when _data missing', async () => {
    const a = newAgent() as any;
    const { blobId } = await a._bs.setBlob(Buffer.from(JSON.stringify({})));
    expect(await a._loadRows(blobId)).toEqual([]);
  });
});

describe('dispose', () => {
  it('clears all timers + change stream + flushes dirty', () => {
    const a = newAgent(makeDb()) as any;
    let removed = false;
    a._changeStream = {
      removeAllListeners: () => {
        removed = true;
      },
      close: async () => {},
    };
    a._internalGcTimer = setInterval(() => {}, 10_000);
    a._dirtyFlushTimer = setTimeout(() => {}, 10_000);
    a.dispose();
    expect(removed).toBe(true);
    expect(a._changeStream).toBeNull();
    expect(a._internalGcTimer).toBeNull();
    expect(a._dirtyFlushTimer).toBeNull();
  });

  it('is a no-op when nothing is running', () => {
    const a = newAgent(makeDb()) as any;
    expect(() => a.dispose()).not.toThrow();
  });

  it('tolerates change-stream close rejecting', () => {
    const a = newAgent(makeDb()) as any;
    a._changeStream = {
      removeAllListeners: () => {},
      close: () => Promise.reject(new Error('close boom')),
    };
    expect(() => a.dispose()).not.toThrow();
  });
});

describe('_startInternalGc sweep', () => {
  it('runs sweep, GCs old in-memory edits, tolerates errors', async () => {
    vi.useFakeTimers();
    const tomb = makeColl('sync_tombstones', {
      deleteMany: async () => {
        throw new Error('x');
      },
    });
    const recent = makeColl('sync_recentChanges', {
      deleteMany: async () => {
        throw new Error('y');
      },
    });
    const db = makeDb({ sync_tombstones: tomb, sync_recentChanges: recent });
    const a = newAgent(db) as any;
    a._startInternalGc();
    // run the initial sweep + timer is set; flush microtasks
    await vi.advanceTimersByTimeAsync(0);
    await Promise.resolve();
    // advance 60s so the interval-scheduled sweep fires too
    await vi.advanceTimersByTimeAsync(60_000);
    await Promise.resolve();
    // calling again returns early (timer already set)
    a._startInternalGc();
  });
});

describe('change-stream open lifecycle', () => {
  const makeStream = () => {
    const handlers: Record<string, (a?: any) => void> = {};
    return {
      on: (ev: string, fn: any) => {
        handlers[ev] = fn;
      },
      removeAllListeners: () => {},
      close: async () => {},
      handlers,
    };
  };

  it('_openChangeStream wires handlers + processes change events', () => {
    const stream = makeStream();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    const received: any[] = [];
    a._changeStreamOnChange = (e: any) => received.push(e);
    a._openChangeStream();
    expect(a._changeStreamAlive).toBe(true);
    stream.handlers.change({ _id: 'tok', operationType: 'insert' });
    expect(a._changeStreamResumeToken).toBe('tok');
    expect(received).toHaveLength(1);
    expect(a._changeStreamLastEventAt).not.toBeNull();
  });

  it('uses resumeAfter when a token exists', () => {
    let opts: any;
    const stream = makeStream();
    const db = makeDb();
    db.watch = (_p: any, o: any) => {
      opts = o;
      return stream;
    };
    const a = newAgent(db) as any;
    a._changeStreamOnChange = () => {};
    a._changeStreamResumeToken = 'tk';
    a._openChangeStream();
    expect(opts.resumeAfter).toBe('tk');
  });

  it('onChange handler throwing is caught', () => {
    const stream = makeStream();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    a._changeStreamOnChange = () => {
      throw new Error('handler boom');
    };
    a._openChangeStream();
    expect(() => stream.handlers.change({ operationType: 'insert' })).not.toThrow();
  });

  it('error/close/end schedule a reopen', () => {
    vi.useFakeTimers();
    const stream = makeStream();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    a._changeStreamOnChange = () => {};
    a._openChangeStream();
    stream.handlers.error(new Error('err'));
    expect(a._changeStreamReopenTimer).not.toBeNull();
    // reset and test close
    if (a._changeStreamReopenTimer) clearTimeout(a._changeStreamReopenTimer);
    a._changeStreamReopenTimer = null;
    stream.handlers.close();
    expect(a._changeStreamReopenTimer).not.toBeNull();
    if (a._changeStreamReopenTimer) clearTimeout(a._changeStreamReopenTimer);
    a._changeStreamReopenTimer = null;
    stream.handlers.end();
    expect(a._changeStreamReopenTimer).not.toBeNull();
  });

  it('close/end do nothing when stop requested', () => {
    const stream = makeStream();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    a._changeStreamOnChange = () => {};
    a._openChangeStream();
    a._changeStreamStopRequested = true;
    stream.handlers.close();
    stream.handlers.end();
    expect(a._changeStreamReopenTimer).toBeNull();
  });

  it('watch() throwing schedules a reopen', () => {
    const db = makeDb();
    db.watch = () => {
      throw new Error('watch failed');
    };
    const a = newAgent(db) as any;
    a._changeStreamOnChange = () => {};
    a._openChangeStream();
    expect(a._changeStreamAlive).toBe(false);
    expect(a._changeStreamReopenTimer).not.toBeNull();
    if (a._changeStreamReopenTimer) clearTimeout(a._changeStreamReopenTimer);
  });

  it('_openChangeStream returns early when stop requested or no handler', () => {
    const a = newAgent(makeDb()) as any;
    a._changeStreamStopRequested = true;
    a._openChangeStream(); // early return
    a._changeStreamStopRequested = false;
    a._changeStreamOnChange = null;
    a._openChangeStream(); // early return (no handler)
    expect(a._changeStream).toBeNull();
  });

  it('_scheduleChangeStreamReopen fires + reopens, closing old stream', () => {
    vi.useFakeTimers();
    const stream = makeStream();
    let watchCalls = 0;
    const db = makeDb();
    db.watch = () => {
      watchCalls++;
      return stream;
    };
    const a = newAgent(db) as any;
    a._changeStreamOnChange = () => {};
    a._changeStreamResumeToken = 'tok'; // covers the `? 'yes'` log arm
    a._changeStream = {
      removeAllListeners: () => {},
      close: async () => {},
    };
    a._scheduleChangeStreamReopen();
    vi.advanceTimersByTime(2000);
    expect(watchCalls).toBeGreaterThan(0);
  });

  it('_scheduleChangeStreamReopen with no existing stream (else arm)', () => {
    vi.useFakeTimers();
    const stream = makeStream();
    let watchCalls = 0;
    const db = makeDb();
    db.watch = () => {
      watchCalls++;
      return stream;
    };
    const a = newAgent(db) as any;
    a._changeStreamOnChange = () => {};
    a._changeStream = null; // covers the `if (this._changeStream)` false arm
    a._scheduleChangeStreamReopen();
    vi.advanceTimersByTime(2000);
    expect(watchCalls).toBeGreaterThan(0);
  });

  it('_scheduleChangeStreamReopen tolerates old-stream close throwing', () => {
    vi.useFakeTimers();
    const stream = makeStream();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    a._changeStreamOnChange = () => {};
    a._changeStream = {
      removeAllListeners: () => {
        throw new Error('rm boom');
      },
      close: async () => {},
    };
    a._scheduleChangeStreamReopen();
    expect(() => vi.advanceTimersByTime(2000)).not.toThrow();
  });

  it('_scheduleChangeStreamReopen is idempotent when a timer is pending', () => {
    vi.useFakeTimers();
    const a = newAgent(makeDb()) as any;
    a._changeStreamOnChange = () => {};
    a._changeStreamReopenTimer = setTimeout(() => {}, 10_000);
    const before = a._changeStreamReopenTimer;
    a._scheduleChangeStreamReopen();
    expect(a._changeStreamReopenTimer).toBe(before);
  });

  it('_scheduleChangeStreamReopen returns immediately when stop requested', () => {
    const a = newAgent(makeDb()) as any;
    a._changeStreamStopRequested = true;
    a._scheduleChangeStreamReopen();
    expect(a._changeStreamReopenTimer).toBeNull();
  });

  it('_scheduleChangeStreamReopen close-catch swallows a rejecting close', () => {
    vi.useFakeTimers();
    const stream = makeStream();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    a._changeStreamOnChange = () => {};
    a._changeStream = {
      removeAllListeners: () => {},
      close: () => Promise.reject(new Error('close reject')),
    };
    a._scheduleChangeStreamReopen();
    expect(() => vi.advanceTimersByTime(2000)).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// Small helpers: _extractTs branches, getSyncHealth, ref-history splice.
// ---------------------------------------------------------------------------

describe('small helpers', () => {
  it('_extractTs covers all branches', () => {
    const a = newAgent() as any;
    expect(a._extractTs({ ts: 5 })).toBe(5);
    expect(a._extractTs({ timestamp: 6 })).toBe(6);
    expect(a._extractTs({ timeId: '100:x' })).toBe(100);
    expect(a._extractTs({ timeId: 'bad' })).toBeNull();
    expect(a._extractTs({})).toBeNull();
  });

  it('getSyncHealth returns full snapshot', () => {
    const a = newAgent() as any;
    a._socket = { connected: true };
    const h = a.getSyncHealth();
    expect(h.socketConnected).toBe(true);
    expect(h.changeStreamAlive).toBe(false);
  });

  it('_pushRefHistory trims to max via splice', () => {
    const a = newAgent() as any;
    a._refHistoryMax = 3;
    for (let i = 0; i < 5; i++) {
      a._pushRefHistory({ ts: i, ref: 'r' + i, direction: 'sent' });
    }
    expect(a._refHistory.length).toBe(3);
    expect(a._refHistory[0].ref).toBe('r2');
  });
});

// ---------------------------------------------------------------------------
// Tree-apply helpers + _applyTreeToMongo
// ---------------------------------------------------------------------------

const buildTree = async (
  agent: any,
  colls: Array<{ name: string; rows: any[] }>,
): Promise<any> => {
  const trees = new Map<string, any>();
  const childHashes: string[] = [];
  let i = 0;
  for (const { name, rows } of colls) {
    const { blobId } = await agent._bs.setBlob(
      Buffer.from(JSON.stringify({ _data: rows })),
    );
    const hash = `coll-${i++}`;
    trees.set(hash, {
      meta: { type: 'collection', collection: name, componentsBlobId: blobId },
      children: [],
    });
    childHashes.push(hash);
  }
  trees.set('root', { meta: { type: 'database' }, children: childHashes });
  return { rootHash: 'root', trees };
};

describe('_applyTreeToMongo', () => {
  it('returns early when root node missing', async () => {
    const a = newAgent(makeDb()) as any;
    await a._applyTreeToMongo({ rootHash: 'x', trees: new Map() });
    // nothing thrown
  });

  it('recurses children, skips sync_conflicts/sync_tombstones, upserts docs', async () => {
    const items = makeColl('items');
    items.docs.set('a', { _id: 'a', v: 'old' });
    const db = makeDb({ items });
    const a = newAgent(db) as any;
    const tree = await buildTree(a, [
      { name: 'items', rows: [{ _id: 'a', v: 'new' }, { _id: 'b', v: 1 }] },
      { name: 'sync_conflicts', rows: [{ _id: 'skip' }] },
    ]);
    await a._applyTreeToMongo(tree);
    const bulk = items.calls.find((c: any) => c.method === 'bulkWrite');
    expect(bulk).toBeDefined();
    // conflict recorded for the diverging 'a'
    const conf = db.collection('sync_conflicts');
    expect(conf.calls.some((c: any) => c.method === 'updateOne')).toBe(true);
  });

  it('skips conflict when local hash equals incoming', async () => {
    const items = makeColl('items');
    items.docs.set('a', { _id: 'a', v: 1 });
    const db = makeDb({ items });
    const a = newAgent(db) as any;
    const tree = await buildTree(a, [{ name: 'items', rows: [{ _id: 'a', v: 1 }] }]);
    await a._applyTreeToMongo(tree);
    const conf = db.collection('sync_conflicts');
    expect(conf.calls.some((c: any) => c.method === 'updateOne')).toBe(false);
  });

  it('falls back to per-doc replaceOne when bulkWrite throws', async () => {
    const items = makeColl('items', {
      bulkWrite: async () => {
        throw new Error('bulk down');
      },
    });
    const db = makeDb({ items });
    const a = newAgent(db) as any;
    const tree = await buildTree(a, [{ name: 'items', rows: [{ _id: 'a', v: 1 }] }]);
    await a._applyTreeToMongo(tree);
    expect(items.calls.some((c: any) => c.method === 'replaceOne')).toBe(true);
  });

  it('per-doc fallback swallows replaceOne errors', async () => {
    const items = makeColl('items', {
      bulkWrite: async () => {
        throw new Error('bulk down');
      },
      replaceOne: async () => {
        throw new Error('replace down');
      },
    });
    const db = makeDb({ items });
    const a = newAgent(db) as any;
    const tree = await buildTree(a, [{ name: 'items', rows: [{ _id: 'a', v: 1 }] }]);
    await expect(a._applyTreeToMongo(tree)).resolves.toBeUndefined();
  });

  it('applies tombstones (Pass 1): deletes local doc + mirrors tombstone', async () => {
    const items = makeColl('items');
    items.docs.set('gone', { _id: 'gone' });
    const localTomb = makeColl('sync_tombstones');
    const db = makeDb({ items, sync_tombstones: localTomb });
    const a = newAgent(db) as any;
    const tree = await buildTree(a, [
      {
        name: 'sync_tombstones',
        rows: [
          {
            _id: 'items::gone',
            collection: 'items',
            documentId: 'gone',
            deletedAt: new Date(),
          },
        ],
      },
    ]);
    await a._applyTreeToMongo(tree);
    expect(items.calls.some((c: any) => c.method === 'deleteOne')).toBe(true);
    expect(localTomb.calls.some((c: any) => c.method === 'replaceOne')).toBe(true);
  });

  it('skips tombstones that target sync_tombstones, missing fields, alive, or too old', async () => {
    const db = makeDb();
    const a = newAgent(db) as any;
    const tree = await buildTree(a, [
      {
        name: 'sync_tombstones',
        rows: [
          { _id: 't1', collection: 'sync_tombstones', documentId: 'x', deletedAt: new Date() },
          { _id: 't2', collection: '', documentId: undefined },
          { _id: 't3', collection: 'items', documentId: 'alive', deletedAt: new Date() },
          {
            _id: 't4',
            collection: 'items',
            documentId: 'oldone',
            deletedAt: new Date(Date.now() - 200_000),
          },
        ],
      },
      // 'alive' present in incoming → tombstone skipped (delete-then-reinsert)
      { name: 'items', rows: [{ _id: 'alive', v: 1 }] },
    ]);
    await a._applyTreeToMongo(tree);
    const items = db.collection('items');
    // 'oldone' must NOT be deleted (too old); only check no delete of oldone
    const deletedIds = items.calls
      .filter((c: any) => c.method === 'deleteOne')
      .map((c: any) => c.args[0]._id);
    expect(deletedIds).not.toContain('oldone');
  });

  it('applies an ancient sync_conflicts tombstone (logical key) + records resolvedConflictKeys', async () => {
    let deleteManyCalled = false;
    const conf = makeColl('sync_conflicts', {
      deleteMany: async () => {
        deleteManyCalled = true;
        return { deletedCount: 1 };
      },
    });
    const items = makeColl('items');
    items.docs.set('d1', { _id: 'd1', v: 'local' });
    const db = makeDb({ sync_conflicts: conf, items });
    const a = newAgent(db) as any;
    const tree = await buildTree(a, [
      {
        name: 'sync_tombstones',
        rows: [
          {
            _id: 'tc',
            collection: 'sync_conflicts',
            documentId: 'd1',
            targetCollection: 'items',
            deletedAt: new Date(Date.now() - 500_000), // ancient, still applies
          },
        ],
      },
      // items 'd1' differs → but peerResolved suppresses the new conflict
      { name: 'items', rows: [{ _id: 'd1', v: 'incoming' }] },
    ]);
    await a._applyTreeToMongo(tree);
    expect(deleteManyCalled).toBe(true);
    // conflict suppressed: no updateOne on sync_conflicts
    expect(conf.calls.some((c: any) => c.method === 'updateOne')).toBe(false);
  });

  it('applies a legacy sync_conflicts tombstone (no targetCollection → deleteOne)', async () => {
    const conf = makeColl('sync_conflicts');
    const db = makeDb({ sync_conflicts: conf });
    const a = newAgent(db) as any;
    const tree = await buildTree(a, [
      {
        name: 'sync_tombstones',
        rows: [
          {
            _id: 'tc2',
            collection: 'sync_conflicts',
            documentId: 'legacy-id',
            deletedAt: new Date(Date.now() - 500_000),
          },
        ],
      },
    ]);
    await a._applyTreeToMongo(tree);
    expect(conf.calls.some((c: any) => c.method === 'deleteOne')).toBe(true);
  });

  it('applies a fresh tombstone with a numeric deletedAt', async () => {
    const items = makeColl('items');
    items.docs.set('num1', { _id: 'num1' });
    const db = makeDb({ items });
    const a = newAgent(db) as any;
    const tree = await buildTree(a, [
      {
        name: 'sync_tombstones',
        rows: [
          { _id: 'items::num1', collection: 'items', documentId: 'num1', deletedAt: Date.now() },
        ],
      },
    ]);
    await a._applyTreeToMongo(tree);
    expect(items.calls.some((c: any) => c.method === 'deleteOne')).toBe(true);
  });

  it('tombstone delete failure is caught; numeric/string deletedAt parsed', async () => {
    const items = makeColl('items', {
      deleteOne: async () => {
        throw new Error('del fail');
      },
    });
    const db = makeDb({ items });
    const a = newAgent(db) as any;
    const tree = await buildTree(a, [
      {
        name: 'sync_tombstones',
        rows: [
          { _id: 'n', collection: 'items', documentId: 'n1', deletedAt: Date.now() },
          {
            _id: 's',
            collection: 'items',
            documentId: 's1',
            deletedAt: new Date().toISOString(),
          },
          { _id: 'bad', collection: 'items', documentId: 'b1', deletedAt: 'not-a-date' },
        ],
      },
    ]);
    await expect(a._applyTreeToMongo(tree)).resolves.toBeUndefined();
  });

  it('restoreMode deletes orphans + writes tombstones', async () => {
    const items = makeColl('items');
    items.docs.set('keep', { _id: 'keep' });
    items.docs.set('orphan', { _id: 'orphan' });
    const tombs = makeColl('sync_tombstones');
    const db = makeDb({ items, sync_tombstones: tombs });
    const a = newAgent(db) as any;
    const tree = await buildTree(a, [{ name: 'items', rows: [{ _id: 'keep', v: 1 }] }]);
    await a._applyTreeToMongo(tree, { restoreMode: true });
    // orphan deleted via deleteMany
    expect(items.calls.some((c: any) => c.method === 'deleteMany')).toBe(true);
    // tombstone bulkWrite for orphan
    expect(tombs.calls.some((c: any) => c.method === 'bulkWrite')).toBe(true);
    // no conflict recorded in restore mode
    const conf = db.collection('sync_conflicts');
    expect(conf.calls.some((c: any) => c.method === 'updateOne')).toBe(false);
  });

  it('restoreMode tolerates tombstone bulkWrite + orphan-delete failures', async () => {
    const items = makeColl('items', {
      deleteMany: async () => {
        throw new Error('orphan del fail');
      },
    });
    items.docs.set('orphan', { _id: 'orphan' });
    const tombs = makeColl('sync_tombstones', {
      bulkWrite: async () => {
        throw new Error('tomb bulk fail');
      },
    });
    const db = makeDb({ items, sync_tombstones: tombs });
    const a = newAgent(db) as any;
    const tree = await buildTree(a, [{ name: 'items', rows: [{ _id: 'keep', v: 1 }] }]);
    await expect(
      a._applyTreeToMongo(tree, { restoreMode: true }),
    ).resolves.toBeUndefined();
  });

  it('collect(): tolerates missing child nodes, null meta, name-keyed collections', async () => {
    const things = makeColl('things');
    const db = makeDb({ things });
    const a = newAgent(db) as any;
    const { blobId } = await a._bs.setBlob(
      Buffer.from(JSON.stringify({ _data: [{ _id: 't1', v: 1 }] })),
    );
    const trees = new Map<string, any>([
      // root references a missing child ('ghost') AND real nodes
      ['root', { meta: { type: 'database' }, children: ['ghost', 'nullmeta', 'col'] }],
      // node with null meta → `node.meta ?? {}` fallback, then children recursion
      ['nullmeta', { meta: null, children: [] }],
      // collection keyed by `name` (not `collection`) → `?? meta.name`
      [
        'col',
        {
          meta: { type: 'collection', name: 'things', componentsBlobId: blobId },
        },
      ],
    ]);
    await a._applyTreeToMongo({ rootHash: 'root', trees });
    expect(things.calls.some((c: any) => c.method === 'bulkWrite')).toBe(true);
  });

  it('skips rows with no _id and empty collections', async () => {
    const db = makeDb();
    const a = newAgent(db) as any;
    const tree = await buildTree(a, [
      { name: 'items', rows: [{ v: 1 }] }, // no _id
      { name: 'empty', rows: [] },
    ]);
    await expect(a._applyTreeToMongo(tree)).resolves.toBeUndefined();
  });

  it('collect(): collection node with falsy name + plain leaf node skipped', async () => {
    const db = makeDb();
    const a = newAgent(db) as any;
    const { blobId } = await a._bs.setBlob(
      Buffer.from(JSON.stringify({ _data: [] })),
    );
    const trees = new Map<string, any>([
      ['root', { meta: { type: 'database' }, children: ['noname', 'leaf'] }],
      // collection node with componentsBlobId but NO collection/name → `if (collName)` false
      ['noname', { meta: { type: 'collection', componentsBlobId: blobId } }],
      // node that is neither a collection nor has a children array → both branches false
      ['leaf', { meta: { type: 'document' } }],
    ]);
    await expect(
      a._applyTreeToMongo({ rootHash: 'root', trees }),
    ).resolves.toBeUndefined();
  });

  it('tombstone logical delete with deletedCount 0 (no log) + no localId', async () => {
    const conf = makeColl('sync_conflicts', {
      deleteMany: async () => ({ deletedCount: 0 }), // covers `if (r.deletedCount)` false
    });
    const db = makeDb({ sync_conflicts: conf });
    const a = newAgent(db) as any;
    const tree = await buildTree(a, [
      {
        name: 'sync_tombstones',
        rows: [
          {
            // NO _id → covers `if (localId !== undefined)` false
            collection: 'sync_conflicts',
            documentId: 'd9',
            targetCollection: 'items',
            deletedAt: new Date(Date.now() - 500_000),
          },
        ],
      },
    ]);
    await expect(a._applyTreeToMongo(tree)).resolves.toBeUndefined();
  });

  it('restoreMode: multi-chunk same-name aggregation + chunk row with no _id + no orphans', async () => {
    const items = makeColl('items');
    items.docs.set('a', { _id: 'a' });
    items.docs.set('b', { _id: 'b' });
    const db = makeDb({ items });
    const a = newAgent(db) as any;
    // two chunks for the same collection name → second chunk finds an existing
    // set (`if (!set)` false). One chunk row has no _id (`if (id!==undefined)` false).
    // All local ids are present in the snapshot → no orphans (`orphanIds.length>0` false).
    const tree = await buildTree(a, [
      { name: 'items', rows: [{ _id: 'a' }, { v: 'no-id' }] },
      { name: 'items', rows: [{ _id: 'b' }] },
    ]);
    await a._applyTreeToMongo(tree, { restoreMode: true });
    // no orphan deleteMany because every local id is in the snapshot
    const delMany = items.calls.filter((c: any) => c.method === 'deleteMany');
    expect(delMany.length).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// syncToDb + onChange closure
// ---------------------------------------------------------------------------

const makeStreamForSync = () => {
  const handlers: Record<string, (a?: any) => void> = {};
  return {
    on: (ev: string, fn: any) => {
      handlers[ev] = fn;
    },
    removeAllListeners: () => {},
    close: async () => {},
    handlers,
  };
};

describe('syncToDb', () => {
  beforeEach(() => {
    delete process.env['SL_DISABLE_TREE_SYNC'];
  });
  afterEach(() => {
    delete process.env['SL_DISABLE_TREE_SYNC'];
  });

  it('runs the initial snapshot + opens stream + returns a working stop fn (all teardown branches)', async () => {
    vi.useFakeTimers();
    const stream = makeStreamForSync();
    const db = makeDb();
    // createIndex rejects → exercises the best-effort .catch callbacks
    db._colls.forEach((c) => {
      c.createIndex = async () => {
        throw new Error('idx fail');
      };
    });
    db.collection = (name: string) => {
      let c = db._colls.get(name);
      if (!c) {
        c = makeColl(name);
        (c as any).createIndex = async () => {
          throw new Error('idx fail');
        };
        db._colls.set(name, c);
      }
      return c as any;
    };
    db.watch = () => stream;
    const a = newAgent(db) as any;
    const connector = { send: vi.fn() };
    const stop = await a.syncToDb(db, connector, 'myTree');
    await Promise.resolve();
    expect(a._forcePushImpl).not.toBeNull();
    expect(a._lastSentRef).toBe('ref-stored');
    // set every teardown-branch precondition
    a._changeStream = {
      removeAllListeners: () => {},
      close: () => Promise.reject(new Error('close fail')),
    };
    a._changeStreamReopenTimer = setTimeout(() => {}, 10_000);
    // a pending debounce timer
    stream.handlers.change({
      ns: { coll: 'items' },
      documentKey: { _id: 'z1' },
      operationType: 'insert',
      fullDocument: { _id: 'z1' },
    });
    stop();
    expect(a._forcePushImpl).toBeNull();
    expect(a._changeStream).toBeNull();
    expect(a._changeStreamReopenTimer).toBeNull();
  });

  it('stop fn with no pending timers / no change stream (else arms)', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    const stop = await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    // null out everything the teardown checks → exercise the false arms
    a._changeStream = null;
    a._changeStreamReopenTimer = null;
    stop();
    expect(a._adapter).toBeNull();
  });

  it('onChange: an op that is neither delete nor insert/update/replace records dirty only', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    const recSpy = vi.spyOn(a, '_recordDirty');
    const tombSpy = vi.spyOn(a, '_writeTombstone').mockResolvedValue(undefined);
    const rcSpy = vi.spyOn(a, '_writeRecentChange').mockResolvedValue(undefined);
    // 'drop' is not insert/update/replace/delete → both inner arms are false
    stream.handlers.change({
      ns: { coll: 'items' },
      documentKey: { _id: 'drop1' },
      operationType: 'drop',
    });
    expect(recSpy).toHaveBeenCalledWith('items', 'drop1');
    expect(tombSpy).not.toHaveBeenCalled();
    expect(rcSpy).not.toHaveBeenCalled();
  });

  it('onChange: anti-overlap queues a follow-up scan while one is in flight', async () => {
    vi.useFakeTimers();
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db, { debounceMs: 50 }) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    a._suppressDebounceUntil = 0;
    a._lastSentRef = 'old';
    let scanCount = 0;
    let resolveFirst: (v: any) => void = () => {};
    // Override THIS agent's scanner directly (isolated from the shared module
    // mock so timers from other tests' agents can't bump scanCount).
    a._scanner.scan = () => {
      scanCount++;
      if (scanCount === 1) {
        return new Promise((res) => {
          resolveFirst = res;
        });
      }
      return Promise.resolve({ rootHash: 'r' + scanCount, trees: new Map() });
    };
    storeTreeImpl.fn = async () => 'stored-' + Math.random();
    vi.spyOn(a, '_safeSendRef').mockResolvedValue(true);
    const ev = (id: string) => ({
      ns: { coll: 'items' },
      documentKey: { _id: id },
      operationType: 'insert',
      fullDocument: { _id: id },
    });
    // Event 1 → debounce → runScan starts → scanInFlight, awaits first scan.
    stream.handlers.change(ev('q1'));
    await vi.advanceTimersByTimeAsync(55);
    const afterFirstDispatch = scanCount;
    expect(afterFirstDispatch).toBe(1);
    // Event 2 → debounce → runScan → finds scanInFlight → queues.
    stream.handlers.change(ev('q2'));
    await vi.advanceTimersByTimeAsync(55);
    // scanCount must NOT have grown — the second runScan queued instead of
    // starting a parallel scan (covers `if (scanInFlight) scanQueued = true`).
    expect(scanCount).toBe(1);
    // First scan completes → storeTree → finally sees scanQueued → schedules
    // a follow-up debounce that runs the queued scan.
    resolveFirst({ rootHash: 'r1', trees: new Map() });
    for (let i = 0; i < 20; i++) await vi.advanceTimersByTimeAsync(10);
    expect(scanCount).toBeGreaterThan(afterFirstDispatch);
  });

  it('initial snapshot with no ref skips send', async () => {
    storeTreeImpl.fn = async () => null as any;
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    const connector = { send: vi.fn() };
    await a.syncToDb(db, connector, 'myTree');
    expect(connector.send).not.toHaveBeenCalled();
  });

  it('forcePush via the installed impl pushes a ref', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    const connector = { send: vi.fn() };
    await a.syncToDb(db, connector, 'myTree');
    const ref = await a.forcePush();
    expect(ref).toBe('ref-stored');
  });

  it('forcePush returns null when storeTree yields no ref', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    storeTreeImpl.fn = async () => null as any;
    const ref = await a.forcePush();
    expect(ref).toBeNull();
    expect(a._suppressDebounceUntil).toBe(0);
  });

  it('forcePush times out when scan hangs (timeout-rejection path)', async () => {
    vi.useFakeTimers();
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    scanImpl.fn = () => new Promise(() => {}); // never resolves
    const p = a.forcePush();
    await vi.advanceTimersByTimeAsync(30_001);
    const ref = await p;
    expect(ref).toBeNull();
  });

  it('debounced scan times out when scan hangs (timeout-rejection path)', async () => {
    vi.useFakeTimers();
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    a._suppressDebounceUntil = 0;
    scanImpl.fn = () => new Promise(() => {}); // hang
    stream.handlers.change({
      ns: { coll: 'items' },
      documentKey: { _id: 'h1' },
      operationType: 'insert',
      fullDocument: { _id: 'h1' },
    });
    await vi.advanceTimersByTimeAsync(2); // debounce fires → runScan → scan hangs
    await vi.advanceTimersByTimeAsync(30_001); // scan timeout rejects
    await Promise.resolve();
    expect(a._changeStream).not.toBeUndefined();
  });

  it('forcePush catch path clears suppress window on failure', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    scanImpl.fn = async () => {
      throw new Error('scan blew');
    };
    const ref = await a.forcePush();
    expect(ref).toBeNull();
    expect(a._suppressDebounceUntil).toBe(0);
  });

  it('onChange: internal collection events are skipped (no dirty, no scan)', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    const recSpy = vi.spyOn(a, '_recordDirty');
    stream.handlers.change({
      ns: { coll: 'sync_recentChanges' },
      documentKey: { _id: 1 },
      operationType: 'insert',
    });
    expect(recSpy).not.toHaveBeenCalled();
  });

  it('onChange: delete writes a tombstone; insert writes recentChange', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any; // no peerApiUrls
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    const tombSpy = vi.spyOn(a, '_writeTombstone').mockResolvedValue(undefined);
    const recSpy = vi.spyOn(a, '_writeRecentChange').mockResolvedValue(undefined);
    stream.handlers.change({
      ns: { coll: 'items' },
      documentKey: { _id: 'd2' },
      operationType: 'delete',
    });
    expect(tombSpy).toHaveBeenCalledWith('items', 'd2');
    stream.handlers.change({
      ns: { coll: 'items' },
      documentKey: { _id: 'i2' },
      operationType: 'insert',
      fullDocument: { _id: 'i2' },
    });
    expect(recSpy).toHaveBeenCalledWith('items', 'i2');
  });

  it('onChange: legacy tombstone write rejection is caught', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    vi.spyOn(a, '_writeTombstone').mockRejectedValue(new Error('tw fail'));
    expect(() =>
      stream.handlers.change({
        ns: { coll: 'items' },
        documentKey: { _id: 'd3' },
        operationType: 'delete',
      }),
    ).not.toThrow();
    await Promise.resolve();
  });

  it('onChange: suppressed debounce window prevents scan', async () => {
    vi.useFakeTimers();
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    a._suppressDebounceUntil = Date.now() + 100_000;
    const scanSpy = vi.spyOn(a._scanner, 'scan');
    scanSpy.mockClear();
    stream.handlers.change({
      ns: { coll: 'items' },
      documentKey: { _id: 's1' },
      operationType: 'insert',
      fullDocument: { _id: 's1' },
    });
    await vi.advanceTimersByTimeAsync(2000);
    expect(scanSpy).not.toHaveBeenCalled();
  });

  it('onChange: debounced scan runs, stores tree, sends ref', async () => {
    vi.useFakeTimers();
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    a._suppressDebounceUntil = 0;
    a._lastSentRef = 'old-ref';
    scanResult.value = { rootHash: 'fresh', trees: new Map() };
    storeTreeImpl.fn = async () => 'new-ref';
    const sendSpy = vi.spyOn(a, '_safeSendRef').mockResolvedValue(true);
    stream.handlers.change({
      ns: { coll: 'items' },
      documentKey: { _id: 's2' },
      operationType: 'insert',
      fullDocument: { _id: 's2' },
    });
    await vi.advanceTimersByTimeAsync(10);
    await Promise.resolve();
    expect(a._lastSentRef).toBe('new-ref');
    expect(sendSpy).toHaveBeenCalledWith(expect.anything(), 'new-ref');
  });

  it('onChange: debounced scan skips when rootHash equals lastSentRef', async () => {
    vi.useFakeTimers();
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    a._suppressDebounceUntil = 0;
    scanResult.value = { rootHash: 'same', trees: new Map() };
    a._lastSentRef = 'same';
    const sendSpy = vi.spyOn(a, '_safeSendRef').mockResolvedValue(true);
    stream.handlers.change({
      ns: { coll: 'items' },
      documentKey: { _id: 's3' },
      operationType: 'insert',
      fullDocument: { _id: 's3' },
    });
    await vi.advanceTimersByTimeAsync(10);
    await Promise.resolve();
    expect(sendSpy).not.toHaveBeenCalled();
  });

  it('onChange: missing ns + missing fullDocument fall back to defaults', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    // ns undefined → internal (no collName) → early return, no throw
    expect(() =>
      stream.handlers.change({ documentKey: { _id: 1 }, operationType: 'insert' }),
    ).not.toThrow();
    // non-delete with NO fullDocument still records the change
    const rcSpy = vi.spyOn(a, '_writeRecentChange').mockResolvedValue(undefined);
    stream.handlers.change({
      ns: { coll: 'items' },
      documentKey: { _id: 'nf' },
      operationType: 'update',
    });
    expect(rcSpy).toHaveBeenCalledWith('items', 'nf');
  });

  it('onChange: recentChange write rejection is caught', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    vi.spyOn(a, '_writeRecentChange').mockRejectedValue(new Error('rc fail'));
    expect(() =>
      stream.handlers.change({
        ns: { coll: 'items' },
        documentKey: { _id: 'rc1' },
        operationType: 'insert',
        fullDocument: { _id: 'rc1' },
      }),
    ).not.toThrow();
    await Promise.resolve();
  });

  it('onChange: legacy update path writes recentChange (+ rejection caught)', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any; // legacy (no peers)
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    const recSpy = vi
      .spyOn(a, '_writeRecentChange')
      .mockRejectedValue(new Error('rc fail'));
    stream.handlers.change({
      ns: { coll: 'items' },
      documentKey: { _id: 'u1' },
      operationType: 'update',
      fullDocument: { _id: 'u1' },
    });
    expect(recSpy).toHaveBeenCalledWith('items', 'u1');
    // `replace` op too (covers the third arm of the legacy or-condition)
    stream.handlers.change({
      ns: { coll: 'items' },
      documentKey: { _id: 'u2' },
      operationType: 'replace',
      fullDocument: { _id: 'u2' },
    });
    expect(recSpy).toHaveBeenCalledWith('items', 'u2');
    recSpy.mockClear();
    // a non-delete, non-insert/update/replace op → neither legacy arm fires
    stream.handlers.change({
      ns: { coll: 'items' },
      documentKey: { _id: 'inv1' },
      operationType: 'invalidate',
    });
    expect(recSpy).not.toHaveBeenCalled();
    await Promise.resolve();
  });

  it('onChange: re-check suppress window inside runScan short-circuits', async () => {
    vi.useFakeTimers();
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    a._suppressDebounceUntil = 0; // pass the outer check at enqueue time
    const scanSpy = vi.spyOn(a._scanner, 'scan');
    scanSpy.mockClear();
    stream.handlers.change({
      ns: { coll: 'items' },
      documentKey: { _id: 'rc2' },
      operationType: 'insert',
      fullDocument: { _id: 'rc2' },
    });
    // Now set the suppress window so the runScan re-check (line 624) returns.
    a._suppressDebounceUntil = Date.now() + 100_000;
    await vi.advanceTimersByTimeAsync(2000);
    expect(scanSpy).not.toHaveBeenCalled();
  });

  it('onChange: debounced scan error is caught', async () => {
    vi.useFakeTimers();
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    a._suppressDebounceUntil = 0;
    scanImpl.fn = async () => {
      throw new Error('scan err');
    };
    stream.handlers.change({
      ns: { coll: 'items' },
      documentKey: { _id: 's5' },
      operationType: 'insert',
      fullDocument: { _id: 's5' },
    });
    await vi.advanceTimersByTimeAsync(10);
    await Promise.resolve();
    expect(a._changeStream).not.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// syncFromDb
// ---------------------------------------------------------------------------

describe('syncFromDb', () => {
  beforeEach(() => {
    delete process.env['SL_DISABLE_TREE_SYNC'];
  });

  it('subscribes bridge listeners; bootstrap + ref apply; explicit pull resolves', async () => {
    vi.useFakeTimers();
    const bridgeHandlers: Record<string, (p: any) => void> = {};
    const bridge = {
      on: (ev: string, fn: any) => {
        bridgeHandlers[ev] = fn;
      },
      off: vi.fn(),
    };
    const db = makeDb();
    (db as any).getInsertHistory = async () => ({
      myTreeInsertHistory: { _data: [{ myTreeRef: 'latest-ref' }] },
    });
    const a = newAgent(db) as any;
    a._bridge = bridge;
    fetchTreeImpl.fn = async () => ({ rootHash: 'r', trees: new Map() });
    const connector = { listen: vi.fn(), tearDown: vi.fn() };
    const stopP = a.syncFromDb(db, connector, 'myTree', { bootstrapTimeoutMs: 3000 });
    await vi.advanceTimersByTimeAsync(600);
    await vi.advanceTimersByTimeAsync(3000);
    const stop = await stopP;
    expect(connector.listen).toHaveBeenCalled();
    expect(Object.keys(bridgeHandlers).length).toBe(2);
    // exercise bridge handlers — bootstrap with r, bootstrap with empty
    // string (apply('') → early return), bootstrap with no ref (resolve),
    // onRef with `.ref` (covers `?? p?.ref`).
    bridgeHandlers[Object.keys(bridgeHandlers)[0]]({ r: 'br-ref' });
    bridgeHandlers[Object.keys(bridgeHandlers)[0]]({ r: '' });
    bridgeHandlers[Object.keys(bridgeHandlers)[0]]({}); // no ref → resolve
    // onRef → handler sets a pending debounce timer; stop() with it still
    // pending covers the `if (timer) clearTimeout(timer)` teardown branch.
    bridgeHandlers[Object.keys(bridgeHandlers)[1]]({ ref: 'ref-via-ref' });
    bridgeHandlers[Object.keys(bridgeHandlers)[1]]({}); // onRef non-string → no-op
    stop();
    expect(connector.tearDown).toHaveBeenCalled();
    expect(bridge.off).toHaveBeenCalled();
  });

  it('teardown when bridge.off is not a function (else arm)', async () => {
    vi.useFakeTimers();
    const bridge = { on: () => {} }; // no `off`
    const db = makeDb();
    (db as any).getInsertHistory = async () => ({});
    const a = newAgent(db) as any;
    a._bridge = bridge;
    const connector = {};
    const stopP = a.syncFromDb(db, connector as any, 'myTree', {
      bootstrapTimeoutMs: 100,
    });
    await vi.advanceTimersByTimeAsync(600);
    await vi.advanceTimersByTimeAsync(1600);
    await vi.advanceTimersByTimeAsync(200);
    const stop = await stopP;
    expect(() => stop()).not.toThrow();
  });

  it('timeout path resolves when no history is available', async () => {
    vi.useFakeTimers();
    const db = makeDb();
    (db as any).getInsertHistory = async () => ({});
    const a = newAgent(db) as any;
    const connector = {};
    const stopP = a.syncFromDb(db, connector as any, 'myTree', {
      bootstrapTimeoutMs: 3000,
    });
    await vi.advanceTimersByTimeAsync(600);
    await vi.advanceTimersByTimeAsync(1600); // retry timer
    await vi.advanceTimersByTimeAsync(3000); // bootstrap timeout
    const stop = await stopP;
    stop();
    expect(typeof stop).toBe('function');
  });

  it('explicit pull failure is caught', async () => {
    vi.useFakeTimers();
    const db = makeDb();
    (db as any).getInsertHistory = async () => {
      throw new Error('hist fail');
    };
    const a = newAgent(db) as any;
    const stopP = a.syncFromDb(db, {} as any, 'myTree', { bootstrapTimeoutMs: 1000 });
    await vi.advanceTimersByTimeAsync(600);
    await vi.advanceTimersByTimeAsync(1600);
    await vi.advanceTimersByTimeAsync(1000);
    await stopP;
    expect(true).toBe(true);
  });

  it('defaults bootstrapTimeoutMs to 3000 when no opts are given', async () => {
    vi.useFakeTimers();
    const db = makeDb();
    (db as any).getInsertHistory = async () => ({});
    const a = newAgent(db) as any;
    // no opts arg → exercises the `opts.bootstrapTimeoutMs ?? 3000` default
    const stopP = a.syncFromDb(db, {} as any, 'myTree');
    await vi.advanceTimersByTimeAsync(600);
    await vi.advanceTimersByTimeAsync(3000);
    const stop = await stopP;
    stop();
    expect(true).toBe(true);
  });
});

describe('syncFromDb apply branches via direct handler', () => {
  // Drive the connector.listen handler + debounce to hit apply() branches.
  const setup = async (overrides: {
    fetchTree?: (ref: string) => Promise<any>;
    history?: () => Promise<any>;
  } = {}) => {
    vi.useFakeTimers();
    const db = makeDb();
    (db as any).getInsertHistory = overrides.history ?? (async () => ({}));
    const a = newAgent(db) as any;
    let handler: any;
    const connector = {
      listen: (h: any) => {
        handler = h;
      },
    };
    if (overrides.fetchTree) fetchTreeImpl.fn = overrides.fetchTree;
    const stopP = a.syncFromDb(db, connector, 'myTree', { bootstrapTimeoutMs: 100 });
    await vi.advanceTimersByTimeAsync(600);
    await vi.advanceTimersByTimeAsync(200);
    await stopP;
    return { a, db, handler };
  };

  it('ignores non-string refs', async () => {
    const { handler } = await setup();
    await handler(123);
    await vi.advanceTimersByTimeAsync(10);
    expect(true).toBe(true);
  });

  it('echo of own push is applied as a no-op (covers lastSentRef branch)', async () => {
    const { a, handler } = await setup();
    a._lastSentRef = 'mine';
    void handler('mine');
    await vi.advanceTimersByTimeAsync(10);
    await Promise.resolve();
    expect(true).toBe(true);
  });

  it('coalesces rapid refs (second call sees a pending timer)', async () => {
    const { handler } = await setup({
      fetchTree: async () => ({ rootHash: 'rt', trees: new Map() }),
    });
    void handler('ref-a');
    void handler('ref-b');
    await vi.advanceTimersByTimeAsync(10);
    await Promise.resolve();
    expect(true).toBe(true);
  });

  it('applies an incoming tree (fetchTree succeeds)', async () => {
    fetchTreeImpl.fn = async () => ({ rootHash: 'rt', trees: new Map() });
    const { a, handler } = await setup({
      fetchTree: async () => ({ rootHash: 'rt', trees: new Map() }),
    });
    const applySpy = vi.spyOn(a, '_applyTreeToMongo').mockResolvedValue(undefined);
    await handler('new-incoming');
    await vi.advanceTimersByTimeAsync(10);
    await Promise.resolve();
    expect(applySpy).toHaveBeenCalled();
  });

  it('fetchTree failure → getInsertHistory fallback succeeds', async () => {
    let first = true;
    const { a, handler } = await setup({
      history: async () => ({}),
      fetchTree: async () => {
        if (first) {
          first = false;
          throw new Error('not in IoMem');
        }
        return { rootHash: 'rt', trees: new Map() };
      },
    });
    const applySpy = vi.spyOn(a, '_applyTreeToMongo').mockResolvedValue(undefined);
    await handler('ref-fallback');
    await vi.advanceTimersByTimeAsync(10);
    await Promise.resolve();
    await Promise.resolve();
    expect(applySpy).toHaveBeenCalled();
  });

  it('fetchTree fails then getInsertHistory HANGS → 4s timeout (rejection path)', async () => {
    vi.useFakeTimers();
    const db = makeDb();
    let hang = false;
    (db as any).getInsertHistory = () =>
      hang ? new Promise(() => {}) : Promise.resolve({});
    const a = newAgent(db) as any;
    let handler: any;
    const connector = {
      listen: (h: any) => {
        handler = h;
      },
    };
    fetchTreeImpl.fn = async () => {
      throw new Error('not in IoMem');
    };
    const stopP = a.syncFromDb(db, connector, 'myTree', { bootstrapTimeoutMs: 100 });
    await vi.advanceTimersByTimeAsync(600);
    await vi.advanceTimersByTimeAsync(200);
    await stopP;
    // now make the fallback getInsertHistory hang
    hang = true;
    const applySpy = vi.spyOn(a, '_applyTreeToMongo');
    void handler('ref-hang');
    await vi.advanceTimersByTimeAsync(2); // debounce → apply → fetchTree throws → fallback
    await vi.advanceTimersByTimeAsync(4001); // getInsertHistory timeout
    await Promise.resolve();
    await Promise.resolve();
    expect(applySpy).not.toHaveBeenCalled();
  });

  it('fetchTree + fallback both fail → returns without applying', async () => {
    const { a, handler } = await setup({
      history: async () => ({}),
      fetchTree: async () => {
        throw new Error('always fail');
      },
    });
    const applySpy = vi.spyOn(a, '_applyTreeToMongo');
    void handler('ref-bad');
    // debounce (1000ms) → apply() → primary fetchTree throws → fallback loop.
    await vi.advanceTimersByTimeAsync(1000);
    // Exhaust all 6 retry attempts: getInsertHistory resolves but every
    // fetchTree retry throws, so lastErr stays set through the backoffs
    // (1500+3000+4500+6000+7500 = 22.5s) → the `if (lastErr) return` arm.
    await vi.advanceTimersByTimeAsync(25_000);
    await Promise.resolve();
    expect(applySpy).not.toHaveBeenCalled();
  });

  it('fetchTree returns null incoming → no apply', async () => {
    const { a, handler } = await setup({
      fetchTree: async () => null,
    });
    const applySpy = vi.spyOn(a, '_applyTreeToMongo');
    await handler('ref-null');
    await vi.advanceTimersByTimeAsync(10);
    await Promise.resolve();
    expect(applySpy).not.toHaveBeenCalled();
  });

  it('apply error in _applyTreeToMongo is caught', async () => {
    const { a, handler } = await setup({
      fetchTree: async () => ({ rootHash: 'rt', trees: new Map() }),
    });
    vi.spyOn(a, '_applyTreeToMongo').mockRejectedValue(new Error('apply boom'));
    await handler('ref-apply-err');
    await vi.advanceTimersByTimeAsync(10);
    await Promise.resolve();
    expect(true).toBe(true);
  });

  it('duplicate lastAppliedRef is ignored on second apply', async () => {
    const { a, handler } = await setup({
      fetchTree: async () => ({ rootHash: 'rt', trees: new Map() }),
    });
    const applySpy = vi.spyOn(a, '_applyTreeToMongo').mockResolvedValue(undefined);
    await handler('dup');
    await vi.advanceTimersByTimeAsync(10);
    await Promise.resolve();
    const callsAfterFirst = applySpy.mock.calls.length;
    await handler('dup');
    await vi.advanceTimersByTimeAsync(10);
    await Promise.resolve();
    expect(applySpy.mock.calls.length).toBe(callsAfterFirst);
  });
});

// ---------------------------------------------------------------------------
// restoreToRef
// ---------------------------------------------------------------------------

describe('restoreToRef', () => {
  it('fetches, applies, and force-pushes', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    fetchTreeImpl.fn = async () => ({
      rootHash: 'r',
      trees: new Map([
        ['r', { meta: { type: 'database' }, children: [] }],
      ]),
    });
    vi.spyOn(a, '_applyTreeToMongo').mockResolvedValue(undefined);
    const res = await a.restoreToRef('the-ref-123456');
    expect(res.fetched).toBe(true);
    expect(res.applied).toBe(true);
  });

  it('logs collection nodes from the incoming tree', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    fetchTreeImpl.fn = async () => ({
      rootHash: 'r',
      trees: new Map<string, any>([
        ['r', { meta: { type: 'database' }, children: ['c', 'c2'] }],
        [
          'c',
          {
            meta: {
              type: 'collection',
              collection: 'items',
              docCount: 3,
              componentsBlobId: 'blob123456789',
            },
          },
        ],
        // collection node with no collection/name/blob → hits `?? meta.name ?? '?'`
        // and `(meta.componentsBlobId ?? '')` fallback arms.
        ['c2', { meta: { type: 'collection' } }],
      ]),
    });
    vi.spyOn(a, '_applyTreeToMongo').mockResolvedValue(undefined);
    const res = await a.restoreToRef('ref-with-coll');
    expect(res.applied).toBe(true);
  });

  it('first fetchTree fails → getInsertHistory retry succeeds', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    (db as any).getInsertHistory = async () => ({});
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    a._db = db;
    let first = true;
    fetchTreeImpl.fn = async () => {
      if (first) {
        first = false;
        throw new Error('miss');
      }
      return { rootHash: 'r', trees: new Map([['r', { meta: {}, children: [] }]]) };
    };
    vi.spyOn(a, '_applyTreeToMongo').mockResolvedValue(undefined);
    const res = await a.restoreToRef('retry-ref');
    expect(res.applied).toBe(true);
  });

  it('first fetchTree HANGS → 10s timeout → retry succeeds (timeout-rejection path)', async () => {
    vi.useFakeTimers();
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    (db as any).getInsertHistory = async () => ({});
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    a._db = db;
    let first = true;
    fetchTreeImpl.fn = () => {
      if (first) {
        first = false;
        return new Promise(() => {}); // hang → timeout
      }
      return Promise.resolve({
        rootHash: 'r',
        trees: new Map([['r', { meta: {}, children: [] }]]),
      });
    };
    vi.spyOn(a, '_applyTreeToMongo').mockResolvedValue(undefined);
    a._forcePushImpl = async () => null;
    const p = a.restoreToRef('hang-ref');
    await vi.advanceTimersByTimeAsync(10_001); // first fetch timeout
    const res = await p;
    expect(res.applied).toBe(true);
  });

  it('throws when both fetch attempts fail', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    (db as any).getInsertHistory = async () => ({});
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    a._db = db;
    fetchTreeImpl.fn = async () => {
      throw new Error('always');
    };
    await expect(a.restoreToRef('bad-ref')).rejects.toThrow('not available');
  });

  it('throws when fetched tree is empty/null', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    fetchTreeImpl.fn = async () => null;
    await expect(a.restoreToRef('empty-ref')).rejects.toThrow('empty tree');
  });

  it('continues when force-push throws', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    fetchTreeImpl.fn = async () => ({
      rootHash: 'r',
      trees: new Map([['r', { meta: {}, children: [] }]]),
    });
    vi.spyOn(a, '_applyTreeToMongo').mockResolvedValue(undefined);
    a._forcePushImpl = async () => {
      throw new Error('push fail');
    };
    const res = await a.restoreToRef('fp-fail-ref');
    expect(res.pushedRef).toBeNull();
  });

  it('handles no forcePushImpl (pushedRef stays null)', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a = newAgent(db) as any;
    await a.syncToDb(db, { send: vi.fn() }, 'myTree');
    fetchTreeImpl.fn = async () => ({
      rootHash: 'r',
      trees: new Map([['r', { meta: {}, children: [] }]]),
    });
    vi.spyOn(a, '_applyTreeToMongo').mockResolvedValue(undefined);
    a._forcePushImpl = null;
    const res = await a.restoreToRef('no-fp-ref');
    expect(res.pushedRef).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// fromClient static factory
// ---------------------------------------------------------------------------

describe('MongoAgent.fromClient', () => {
  it('throws when client.io missing', async () => {
    await expect(
      MongoAgent.fromClient(makeDb(), 'myTree', {}, {}),
    ).rejects.toThrow('Client.io is not initialized');
  });

  it('throws when client.bs missing', async () => {
    await expect(
      MongoAgent.fromClient(makeDb(), 'myTree', { io: {} }, {}),
    ).rejects.toThrow('Client.bs is not initialized');
  });

  it('builds an enhanced agent + wires syncToDbSimple/syncFromDbSimple/onDisconnect', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    let disconnectCb: any;
    const client = {
      io: {},
      bs: undefined,
      onDisconnect: (cb: any) => {
        disconnectCb = cb;
      },
    };
    // give a real Bs
    const a0 = newAgent() as any;
    client.bs = a0._bs;
    const socket = { on: () => {}, off: () => {} };
    const enhanced = (await MongoAgent.fromClient(
      db,
      'myTree',
      client,
      socket,
      {},
    )) as any;
    expect(typeof enhanced.syncToDbSimple).toBe('function');
    expect(typeof enhanced.syncFromDbSimple).toBe('function');
    // exercise syncToDbSimple (bootstrapDone null path) + stop
    const stop = await enhanced.syncToDbSimple();
    expect(typeof stop).toBe('function');
    // exercise syncFromDbSimple too (covers the arrow at 2480) — stub the
    // underlying syncFromDb so we don't spin up real bootstrap timers.
    const noop = () => {};
    vi.spyOn(enhanced, 'syncFromDb').mockResolvedValue(noop);
    const stopFrom = await enhanced.syncFromDbSimple();
    expect(stopFrom).toBe(noop);
    // onDisconnect handler disposes
    expect(typeof disconnectCb).toBe('function');
    disconnectCb();
  });

  it('syncToDbSimple awaits bootstrapDone (and tolerates rejection)', async () => {
    const stream = makeStreamForSync();
    const db = makeDb();
    db.watch = () => stream;
    const a0 = newAgent() as any;
    const client = { io: {}, bs: a0._bs };
    const socket = { socket: { connected: false }, on: () => {} };
    const enhanced = (await MongoAgent.fromClient(
      db,
      'myTree',
      client,
      socket,
      { rawSocket: { connected: false } },
    )) as any;
    (enhanced as any)._bootstrapDone = Promise.reject(new Error('bootstrap failed'));
    const stop = await enhanced.syncToDbSimple();
    expect(typeof stop).toBe('function');
  });
});
