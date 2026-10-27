// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it, vi } from 'vitest';

import {
  MongoEditSignals,
  SIGNAL_IDS_MAX,
  SIGNAL_LOG_MAX,
} from '../src/mongo-edit-signals.ts';

describe('MongoEditSignals', () => {
  it('records a signal, sorted and counted, bounded at SIGNAL_IDS_MAX', () => {
    const signals = new MongoEditSignals();
    const ids = Array.from({ length: SIGNAL_IDS_MAX + 5 }, (_, i) => String(i));
    const signal = signals.add({
      kind: 'deletion/refused',
      collection: 'customers',
      ids: [...ids].reverse(), // unsorted on the way in
      action: 'review',
    });

    expect(signal.ids).toEqual(
      [...ids].sort().slice(0, SIGNAL_IDS_MAX),
    );
    expect(signal.idCount).toBe(ids.length);
    expect(signal.id).toMatch(/^[0-9a-f]{16}$/);
    expect(typeof signal.at).toBe('number');
    expect(signals.all).toEqual([signal]);
  });

  it('accepts a fixed `at`, for a test that needs a stable stamp', () => {
    const signals = new MongoEditSignals();
    const signal = signals.add({
      kind: 'deletion/refused',
      collection: 'c',
      ids: ['1'],
      action: 'none',
      at: 12345,
    });
    expect(signal.at).toBe(12345);
  });

  it('gives a repeat of the same fact a different id via a counter', () => {
    const signals = new MongoEditSignals();
    const input = {
      kind: 'deletion/refused' as const,
      collection: 'c',
      ids: ['1', '2'],
      action: 'review' as const,
      at: 1000,
    };
    const first = signals.add(input);
    const second = signals.add(input);
    expect(second.id).not.toBe(first.id);
    expect(second.id.startsWith(first.id)).toBe(true);
  });

  it('drops the oldest once past SIGNAL_LOG_MAX, keeping the newest', () => {
    const signals = new MongoEditSignals();
    for (let i = 0; i < SIGNAL_LOG_MAX + 3; i++) {
      signals.add({
        kind: 'deletion/refused',
        collection: 'c',
        ids: [String(i)],
        action: 'none',
        at: i,
      });
    }
    expect(signals.all).toHaveLength(SIGNAL_LOG_MAX);
    expect(signals.all[0]?.at).toBe(3);
    expect(signals.all[signals.all.length - 1]?.at).toBe(SIGNAL_LOG_MAX + 2);
  });

  it('keeps a per-kind total across the cap, so dropped signals still count', () => {
    const signals = new MongoEditSignals();
    for (let i = 0; i < SIGNAL_LOG_MAX + 3; i++) {
      signals.add({
        kind: 'deletion/refused',
        collection: 'c',
        ids: [String(i)],
        action: 'none',
      });
    }
    expect(signals.totals()).toEqual({ 'deletion/refused': SIGNAL_LOG_MAX + 3 });
  });

  it('filters needingAction to only action: required signals', () => {
    const signals = new MongoEditSignals();
    signals.add({ kind: 'deletion/refused', collection: 'c', ids: ['1'], action: 'review' });
    const required = signals.add({
      kind: 'deletion/refused',
      collection: 'c',
      ids: ['2'],
      action: 'required',
    });
    expect(signals.needingAction).toEqual([required]);
  });

  it('subscribes and unsubscribes a listener', () => {
    const signals = new MongoEditSignals();
    const listener = vi.fn();
    const unsubscribe = signals.subscribe(listener);

    signals.add({ kind: 'deletion/refused', collection: 'c', ids: ['1'], action: 'none' });
    expect(listener).toHaveBeenCalledTimes(1);

    unsubscribe();
    signals.add({ kind: 'deletion/refused', collection: 'c', ids: ['2'], action: 'none' });
    expect(listener).toHaveBeenCalledTimes(1);
  });

  it('does not let a throwing listener break add() or other listeners', () => {
    const signals = new MongoEditSignals();
    const thrower = vi.fn(() => {
      throw new Error('host bug');
    });
    const sane = vi.fn();
    signals.subscribe(thrower);
    signals.subscribe(sane);

    expect(() =>
      signals.add({ kind: 'deletion/refused', collection: 'c', ids: ['1'], action: 'none' }),
    ).not.toThrow();
    expect(sane).toHaveBeenCalledTimes(1);
  });
});
