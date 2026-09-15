// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import {
  Binary,
  Decimal128,
  deserialize as bsonDeserialize,
  Double,
  EJSON,
  Int32,
  Long,
  ObjectId,
  serialize as bsonSerialize,
} from 'bson';
import { describe, expect, it } from 'vitest';

import {
  bodyToDoc,
  componentToDoc,
  docHash,
  docToBody,
  docToComponent,
  mongoCanonical,
  sortKeys,
} from '../src/mongo-component-codec.ts';

describe('mongo-component-codec', () => {
  it('round-trips every BSON type losslessly', () => {
    const doc = {
      _id: new Int32(42), // CARAT integer id
      oid: new ObjectId('64b7f0c2e4b0a1a2b3c4d5e6'),
      when: new Date('2026-08-04T10:00:00.000Z'),
      big: Long.fromString('9007199254740993'),
      price: Decimal128.fromString('19.99'),
      ratio: new Double(3.14),
      blob: new Binary(Buffer.from([1, 2, 3])),
      nested: { a: new Int32(1), b: [new Date(0), 'x'] },
      arr: [new Int32(1), new Int32(2)],
      str: 'hello',
      n: 5,
      bool: true,
      nil: null,
    };

    const comp = docToComponent(doc);
    expect(typeof comp._hash).toBe('string');
    expect(comp._hash.length).toBeGreaterThan(0);

    const back = componentToDoc(comp);
    // Canonical EJSON string equality is type-aware (Int32 !== Double etc.)
    expect(EJSON.stringify(back, { relaxed: false })).toBe(
      EJSON.stringify(doc, { relaxed: false }),
    );
  });

  it('is deterministic — same document yields the same _hash', () => {
    const a = docToComponent({ _id: new Int32(1), when: new Date('2026-01-01') });
    const b = docToComponent({ _id: new Int32(1), when: new Date('2026-01-01') });
    expect(a._hash).toBe(b._hash);
  });

  it('preserves the integer _id type (not coerced to double)', () => {
    const back = componentToDoc(docToComponent({ _id: new Int32(7) }));
    expect(back._id).toBeInstanceOf(Int32);
    expect((back._id as Int32).value).toBe(7);
  });

  it('strips any incoming _hash before hashing', () => {
    const withHash = docToComponent({
      _id: new Int32(1),
      _hash: 'stale',
    } as never);
    const without = docToComponent({ _id: new Int32(1) });
    expect(withHash._hash).toBe(without._hash);
  });

  it('distinguishes Int32 from Double in the content hash', () => {
    const asInt = docToComponent({ v: new Int32(1) });
    const asDouble = docToComponent({ v: new Double(1) });
    expect(asInt._hash).not.toBe(asDouble._hash);
  });

  describe('docHash (BSON content hash)', () => {
    it('is a deterministic 64-hex digest over every BSON kind', () => {
      const doc = {
        _id: new Int32(1),
        when: new Date('2026-01-01T00:00:00.000Z'),
        nil: null,
        ok: true,
        name: 'carat',
        tags: ['a', 'b'],
        nested: { x: new Int32(2), y: [true, null] },
      };
      const a = docHash(doc);
      const b = docHash({ ...doc });
      expect(a).toHaveLength(64);
      expect(a).toBe(b);
    });

    it('changes when any content changes', () => {
      expect(docHash({ v: 'a' })).not.toBe(docHash({ v: 'b' }));
    });

    it('separates values but CONVERGES ambiguous numbers (Int32 1 == Double 1)', () => {
      // Different VALUES still hash differently…
      expect(docHash({ v: '1' })).not.toBe(docHash({ v: new Int32(1) }));
      expect(docHash({ v: 'true' })).not.toBe(docHash({ v: true }));
      expect(docHash({ v: new Int32(1) })).not.toBe(docHash({ v: new Int32(2) }));
      // …but an Int32 1 and a Double 1 are the SAME value and MUST hash the same:
      // either is what a node may read back after a promote, and the content root
      // has to converge regardless of which storage form each node landed on.
      expect(docHash({ v: new Int32(1) })).toBe(docHash({ v: new Double(1) }));
    });

    it('hashes a large document (huge array) in one shot without throwing', () => {
      // The mega catalogs crashed the old canonical-string hash. Hashing the raw
      // BSON buffer is a single update; a document is at most 16MB so the buffer
      // is always small. This body stays well under the limit and must not throw.
      const big = { _id: new Int32(1), rows: Array.from({ length: 50_000 }, (_, i) => `r${i}`) };
      let hash = '';
      expect(() => {
        hash = docHash(big);
      }).not.toThrow();
      expect(hash).toHaveLength(64);
      expect(docHash(big)).toBe(hash);
    });

    it('is FIELD-ORDER-insensitive — same content in a different order hashes the same', () => {
      expect(docHash({ _id: 'x', a: 1, b: 2 })).toBe(
        docHash({ b: 2, _id: 'x', a: 1 }),
      );
      // …including nested objects, which the codec round-trip reorders.
      expect(docHash({ _id: 'x', n: { z: 1, a: 2 } })).toBe(
        docHash({ _id: 'x', n: { a: 2, z: 1 } }),
      );
    });

    it('agrees between a source doc and its edit-chain round-trip (the convergence fix)', () => {
      // The exact source→receiver split that diverged live: the source hashes its
      // raw fullDocument; a receiver pulls the doc through the codec and hashes
      // that. The codec reorders nested keys — the hash must survive it.
      const source = {
        _id: new Int32(7),
        meta: { created: new Date('2026-09-09T00:00:00Z'), by: 'a', tag: 'z' },
        vals: [new Int32(1), new Int32(2)],
        name: 'carol',
      };
      const roundTripped = bodyToDoc(docToBody(source));
      expect(docHash(source)).toBe(docHash(roundTripped));
    });

    it('array order still matters (order is data in an array)', () => {
      expect(docHash({ _id: 'x', a: [1, 2] })).not.toBe(
        docHash({ _id: 'x', a: [2, 1] }),
      );
    });

    it('converges every CARAT BSON type through the edit-chain round-trip', () => {
      // A doc using each type CARAT actually stores. Its raw hash and its
      // codec-round-tripped hash MUST agree — this is exactly the source→receiver
      // split that flooded anti-entropy.
      const doc = {
        _id: new Int32(42),
        code: 'EUR',
        rate: new Double(1.0), // ambiguous integer-valued double
        amount: 3, // plain JS number
        when: new Date('2026-09-10T00:00:00.000Z'),
        oid: new ObjectId('64b7f0c2e4b0a1a2b3c4d5e6'),
        price: Decimal128.fromString('19.99'),
        blob: new Binary(Buffer.from([1, 2, 3])),
        meta: { z: 1, a: new Int32(2), tags: ['x', 'y'] },
      };
      const rt = bodyToDoc(docToBody(doc));
      expect(docHash(doc)).toBe(docHash(rt));
    });

    it('converges a large Long too — canonical EJSON keeps it a $numberLong string', () => {
      // A Long above 2^53 survives the canonical-EJSON codec (it is tagged
      // `$numberLong` as a string, so no JS-number precision is involved) and
      // hashes identically on both sides.
      const doc = { _id: 'x', l: Long.fromString('90071992547409931') };
      const rt = bodyToDoc(docToBody(doc));
      expect(docHash(doc)).toBe(docHash(rt));
    });
  });

  describe('sortKeys', () => {
    it('sorts nested plain-object keys but leaves BSON leaves and arrays intact', () => {
      const oid = new ObjectId('64b7f0c2e4b0a1a2b3c4d5e6');
      const out = sortKeys({
        b: 2,
        a: { z: new Int32(9), m: [3, 1, 2] },
        _id: oid,
      }) as Record<string, unknown>;
      expect(Object.keys(out)).toEqual(['_id', 'a', 'b']);
      expect(Object.keys(out['a'] as object)).toEqual(['m', 'z']);
      expect(out['_id']).toBe(oid); // BSON wrapper untouched (same reference)
      expect((out['a'] as { m: number[] }).m).toEqual([3, 1, 2]); // array order kept
    });

    it('leaves a primitive, a Date, and null unchanged', () => {
      const d = new Date(0);
      expect(sortKeys(5)).toBe(5);
      expect(sortKeys(d)).toBe(d);
      expect(sortKeys(null)).toBe(null);
    });
  });

  describe('mongoCanonical', () => {
    it('pins a component-decoded doc to the Mongo read-back form', () => {
      // The exact shape a bulk import produces: every field a plain JS number,
      // so the driver stores the large `createdAt` (Date.now()) as a Double.
      const stored = bsonDeserialize(
        bsonSerialize({
          _id: 'bt2-099243',
          idx: 99243,
          batch: 'bt2',
          createdAt: 1_725_000_000_000,
        }),
      );
      // Encode → decode through the component codec, exactly as an anti-entropy
      // pull does. Canonical EJSON tags the large integer `$numberLong`, so it
      // decodes as a BSON Long — a different raw-BSON encoding than the stored
      // Double, hence a different hash. This is the mismatch that made a pulled
      // doc's content root disagree with its read-back twin and wedged the
      // backfill in an endless re-pull.
      const decoded = bodyToDoc(docToBody(stored));
      // `docHash` now canonicalizes internally (mongoCanonical + sortKeys), so the
      // pulled doc and its read-back twin hash the SAME even though their raw BSON
      // differs (Long wrapper vs stored Double) — the mismatch that wedged the
      // backfill is gone at the hash level.
      expect(docHash(decoded)).toBe(docHash(stored));
      // Explicitly re-canonicalizing is idempotent — still the same hash.
      expect(docHash(mongoCanonical(decoded))).toBe(docHash(stored));
    });

    it('leaves the types Mongo itself keeps hash-identical', () => {
      // An Int32 id, a Date, and a Long too large to promote to a JS number all
      // survive the canonicalization with the same content hash — only the
      // ambiguous integer-vs-Double numbers are pinned.
      const build = () => ({
        _id: new Int32(7),
        when: new Date('2026-01-01T00:00:00.000Z'),
        big: Long.fromString('9007199254740993'), // 2^53 + 1, unsafe to promote
        who: new ObjectId('64b7f0c2e4b0a1a2b3c4d5e6'),
        blob: new Binary(Buffer.from([1, 2, 3])),
        price: Decimal128.fromString('19.99'),
      });
      expect(docHash(mongoCanonical(build()))).toBe(docHash(build()));
    });
  });
});
