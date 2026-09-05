// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Binary, Decimal128, Double, EJSON, Int32, Long, ObjectId } from 'bson';
import { describe, expect, it } from 'vitest';

import {
  componentToDoc,
  docHash,
  docToComponent,
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

    it('is BSON-type-aware — Int32 1 differs from string "1" and from Double 1', () => {
      expect(docHash({ v: '1' })).not.toBe(docHash({ v: new Int32(1) }));
      expect(docHash({ v: new Int32(1) })).not.toBe(docHash({ v: new Double(1) }));
      expect(docHash({ v: 'true' })).not.toBe(docHash({ v: true }));
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
  });
});
