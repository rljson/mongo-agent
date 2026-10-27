// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { createHash } from 'node:crypto';

// .............................................................................
// ONE CHANNEL FOR EVERYTHING THE MASS-DELETE GUARD DECIDES.
//
// A refused deletion used to be a line in `_log` only — visible on the machine
// running the agent, to someone grepping its console, and nowhere else. A host
// application cannot grep a log on a machine it is not running on, and "why
// did my delete not propagate" is a question it has to be able to answer.
// Mirrors `@rljson/fs-agent`'s `FsSignals` (its `'deletion/refused'` kind is
// the same fact about the same guard, one layer up), trimmed to what
// mongo-agent currently produces — a single source (the guard), not several.
// .............................................................................

/** What a {@link MongoEditSignal} is about. */
export type MongoEditSignalKind =
  /** A deletion was refused by the mass-delete guard and will not propagate. */
  'deletion/refused';

/** Whether a person has to do something. */
export type MongoEditSignalAction =
  /** Resolved; nothing to do. */
  | 'none'
  /** Resolved, but somebody may want to look. */
  | 'review'
  /** NOT resolved. It stays this way until a person acts. */
  | 'required';

/** One thing the mass-delete guard decided, in a form a program can act on. */
export interface MongoEditSignal {
  /**
   * This signal's identity: a hash of what happened, unique within the log.
   * Derived rather than counted, so a host can key a row by it.
   */
  id: string;
  /** What happened. */
  kind: MongoEditSignalKind;
  /** When, epoch ms. */
  at: number;
  /** The collection the guard is protecting. */
  collection: string;
  /**
   * The document ids it affected, sorted and stringified.
   *
   * Capped at {@link SIGNAL_IDS_MAX} with `idCount` carrying the true number,
   * because a mass deletion names thousands and a signal has to stay small
   * enough to keep.
   */
  ids: readonly string[];
  /** How many ids were affected, which may exceed `ids.length`. */
  idCount: number;
  /** Whether a person has to do something. */
  action: MongoEditSignalAction;
  /** One sentence for a person. Never a stack trace. */
  detail?: string;
}

/** What a signal says before the sink stamps, identifies and bounds it. */
export type MongoEditSignalInput = Omit<
  MongoEditSignal,
  'id' | 'at' | 'ids' | 'idCount'
> & {
  /** The affected ids; sorted, capped and counted by the sink. */
  ids: readonly string[];
  /** Overrides the clock, for a test that needs a fixed stamp. */
  at?: number;
};

/** How many signals {@link MongoEditSignals.all} keeps. */
export const SIGNAL_LOG_MAX = 200;

/** How many ids one signal names before it only counts them. */
export const SIGNAL_IDS_MAX = 20;

/**
 * The signal sink: bounded, countable, subscribable.
 *
 * Deliberately has no I/O of its own — a pure structure a test can drive
 * without a database, which is also what keeps its own tests honest.
 */
export class MongoEditSignals {
  private readonly _signals: MongoEditSignal[] = [];
  private readonly _totals = new Map<MongoEditSignalKind, number>();
  private readonly _listeners = new Set<(signal: MongoEditSignal) => void>();

  /**
   * Records one signal.
   * @param input - What happened.
   * @returns The signal as recorded, stamped and bounded.
   */
  add(input: MongoEditSignalInput): MongoEditSignal {
    const ids = [...input.ids].sort();
    const kept = ids.slice(0, SIGNAL_IDS_MAX);
    const unidentified: Omit<MongoEditSignal, 'id'> = {
      ...input,
      at: input.at ?? Date.now(),
      ids: kept,
      idCount: ids.length,
    };
    const signal: MongoEditSignal = {
      id: this._uniqueId(unidentified),
      ...unidentified,
    };
    this._signals.push(signal);
    // The cap drops the OLDEST, because a collection in trouble produces
    // signals faster than anybody reads them and the newest are the ones that
    // still describe the current state.
    while (this._signals.length > SIGNAL_LOG_MAX) this._signals.shift();
    this._totals.set(signal.kind, (this._totals.get(signal.kind) ?? 0) + 1);

    // A listener that throws is the host's problem and must not take the sink
    // with it — the alternative is a UI bug stopping a sync.
    for (const listener of this._listeners) {
      try {
        listener(signal);
      } catch {
        /* reported by the caller, which owns its own error log */
      }
    }
    return signal;
  }

  /** Every signal kept, oldest first. */
  get all(): readonly MongoEditSignal[] {
    return this._signals;
  }

  /** Only the signals a person still has to act on. */
  get needingAction(): readonly MongoEditSignal[] {
    return this._signals.filter((s) => s.action === 'required');
  }

  /**
   * How many of each kind have ever been recorded, dropped ones included.
   * @returns The counts, keyed by kind.
   */
  totals(): Readonly<Record<string, number>> {
    return Object.fromEntries(this._totals);
  }

  /**
   * Subscribes to signals as they happen.
   * @param listener - Called once per signal.
   * @returns Unsubscribes.
   */
  subscribe(listener: (signal: MongoEditSignal) => void): () => void {
    this._listeners.add(listener);
    return () => {
      this._listeners.delete(listener);
    };
  }

  /**
   * An id for a signal: a hash of what it says, made unique within the log.
   *
   * Two signals that say the same thing in the same millisecond are possible
   * — a refusal raised twice at once — and must still be two rows, so a
   * repeat gets a counter rather than the same id.
   * @param signal - The signal, without its id.
   * @returns The id.
   */
  private _uniqueId(signal: Omit<MongoEditSignal, 'id'>): string {
    const base = createHash('sha256')
      .update(
        JSON.stringify([
          signal.kind,
          signal.at,
          signal.collection,
          signal.ids,
          signal.idCount,
          signal.detail ?? '',
        ]),
      )
      .digest('hex')
      .slice(0, 16);
    let id = base;
    for (let n = 2; this._signals.some((kept) => kept.id === id); n++) {
      id = `${base}.${n}`;
    }
    return id;
  }
}
