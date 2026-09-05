// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.

import { describe, expect, it } from 'vitest';

import { MongoAgent } from '../src/mongo-agent.ts';


/**
 * Tests for the private content-hash + ref-history bookkeeping on MongoAgent.
 * These hit shape contracts only; no MongoDB / no rljson client.
 *
 * `_contentHash` / `_normaliseForHash` are private — we reach them with a
 * cast. If the surface ever needs to be public for an SDK, switch to a
 * proper exported helper.
 */
describe('MongoAgent: _contentHash + ref-history', () => {
  // Construct without a real Mongo Db — most methods we test don't touch it.
  const newAgent = (): MongoAgent =>
    new MongoAgent({ databaseName: 'test' } as any, undefined, { debounceMs: 1 });

  it('_contentHash is stable for same content + different key order', () => {
    const agent = newAgent() as any;
    const a = agent._contentHash({ a: 1, b: 2, c: 3 });
    const b = agent._contentHash({ c: 3, a: 1, b: 2 });
    expect(a).toBe(b);
  });

  it('_contentHash ignores the rljson `_hash` field', () => {
    const agent = newAgent() as any;
    const a = agent._contentHash({ _id: 'x', n: 1 });
    const b = agent._contentHash({ _id: 'x', n: 1, _hash: 'irrelevant' });
    expect(a).toBe(b);
  });

  it('_contentHash treats BSON Date and numeric-ms as equal', () => {
    // This is the fix for the phantom "concurrent-update" conflict storm
    // we hit when the rljson roundtrip mapped Date → number on one peer.
    const agent = newAgent() as any;
    const d = new Date('2026-05-15T10:00:00.000Z');
    const asDate = agent._contentHash({ _id: 'x', t: d });
    const asNumber = agent._contentHash({ _id: 'x', t: d.getTime() });
    expect(asDate).toBe(asNumber);
  });

  it('_contentHash collapses ObjectId-like to its string form', () => {
    const agent = newAgent() as any;
    const fakeObjectId = { _bsontype: 'ObjectId', toString: () => 'deadbeef' };
    const asObj = agent._contentHash({ _id: fakeObjectId });
    const asStr = agent._contentHash({ _id: 'deadbeef' });
    expect(asObj).toBe(asStr);
  });

  it('getRefHistory returns latest entries first, capped at limit', () => {
    const agent = newAgent() as any;
    for (let i = 0; i < 5; i++) {
      agent._pushRefHistory({
        ts: 1000 + i,
        ref: `ref-${i}`,
        direction: i % 2 === 0 ? 'sent' : 'received',
      });
    }
    const history = agent.getRefHistory(3);
    expect(history).toHaveLength(3);
    expect(history[0].ref).toBe('ref-4'); // newest first
    expect(history[2].ref).toBe('ref-2');
  });

  it('getRefHistory never grows beyond _refHistoryMax', () => {
    const agent = newAgent() as any;
    const max = agent._refHistoryMax as number;
    for (let i = 0; i < max + 50; i++) {
      agent._pushRefHistory({ ts: i, ref: `r-${i}`, direction: 'sent' });
    }
    expect((agent._refHistory as unknown[]).length).toBe(max);
  });

  it('getSyncHealth returns the expected shape', () => {
    const agent = newAgent() as any;
    const h = agent.getSyncHealth();
    expect(h).toHaveProperty('lastSendOkAt');
    expect(h).toHaveProperty('consecutiveTimeouts');
    expect(h).toHaveProperty('totalSendOk');
    expect(h).toHaveProperty('totalSendTimeouts');
    expect(h).toHaveProperty('socketConnected');
    expect(h).toHaveProperty('forcePushReady');
    expect(h).toHaveProperty('changeStreamAlive');
    expect(h).toHaveProperty('changeStreamLastEventAt');
    expect(h).toHaveProperty('changeStreamReopenAttempts');
    expect(h.consecutiveTimeouts).toBe(0);
    expect(h.forcePushReady).toBe(false); // syncToDb not started in tests
    expect(h.changeStreamAlive).toBe(false); // never opened
  });

  it('change-stream auto-reopen schedules + uses exponential backoff capped at 5s', () => {
    const agent = newAgent() as any;
    agent._changeStreamOnChange = () => {};
    agent._changeStreamStopRequested = false;
    const setTimeoutCalls: Array<{ delay: number }> = [];
    const realSetTimeout = global.setTimeout;
    (global as any).setTimeout = (cb: any, delay: number) => {
      setTimeoutCalls.push({ delay });
      return realSetTimeout(() => {}, 100_000) as any; // dummy handle
    };
    try {
      for (let i = 0; i < 6; i++) {
        agent._changeStreamReopenTimer = null;
        agent._scheduleChangeStreamReopen();
      }
    } finally {
      (global as any).setTimeout = realSetTimeout;
    }
    expect(setTimeoutCalls).toHaveLength(6);
    // Attempt 1 → 1000ms, 2 → 2000ms, 3 → 4000ms, 4+ → 5000ms (cap)
    expect(setTimeoutCalls[0].delay).toBe(1000);
    expect(setTimeoutCalls[1].delay).toBe(2000);
    expect(setTimeoutCalls[2].delay).toBe(4000);
    expect(setTimeoutCalls[3].delay).toBe(5000);
    expect(setTimeoutCalls[4].delay).toBe(5000);
    expect(setTimeoutCalls[5].delay).toBe(5000);
  });

  it('_extractTs reads `r.ts` first (legacy writer compatibility)', () => {
    const agent = newAgent() as any;
    expect(agent._extractTs({ ts: 1779263025930 })).toBe(1779263025930);
  });

  it('_extractTs reads `r.timestamp` when `ts` is absent', () => {
    const agent = newAgent() as any;
    expect(agent._extractTs({ timestamp: 42 })).toBe(42);
  });

  it('_extractTs parses the rljson `timeId` "<epochMs>:<rand>" format', () => {
    const agent = newAgent() as any;
    // Real-world shape produced by rljson InsertHistory:
    //   timeId: "1779263025930:h7y0"
    // The colon-prefixed suffix exists to keep concurrent inserts ordered
    // when two refs land in the same ms; we only care about the head.
    expect(agent._extractTs({ timeId: '1779263025930:h7y0' })).toBe(1779263025930);
  });

  it('_extractTs returns null for unrecognised / malformed rows', () => {
    const agent = newAgent() as any;
    expect(agent._extractTs({})).toBeNull();
    expect(agent._extractTs({ ts: 'not-a-number' })).toBeNull();
    expect(agent._extractTs({ timeId: 'no-colon-here' })).toBeNull();
    expect(agent._extractTs({ timeId: ':123' })).toBeNull();
    expect(agent._extractTs({ timeId: 'abc:1234' })).toBeNull();
  });

  it('_extractTs prefers explicit ts over timeId when both present', () => {
    const agent = newAgent() as any;
    // ts wins over timeId — required so legacy callers that already set
    // ts get the same value through the new helper.
    const got = agent._extractTs({ ts: 100, timeId: '999:xyz' });
    expect(got).toBe(100);
  });

  it('change-stream reopen is suppressed once stop is requested', () => {
    const agent = newAgent() as any;
    agent._changeStreamOnChange = () => {};
    agent._changeStreamStopRequested = true;
    let scheduled = 0;
    const realSetTimeout = global.setTimeout;
    (global as any).setTimeout = (...args: any[]) => {
      scheduled++;
      return realSetTimeout(args[0], 100_000) as any;
    };
    try {
      agent._scheduleChangeStreamReopen();
    } finally {
      (global as any).setTimeout = realSetTimeout;
    }
    expect(scheduled).toBe(0);
  });
});
