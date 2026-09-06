// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';

import { AE_BUCKET_COUNT } from '../src/mongo-anti-entropy';
import {
  bucketOf,
  decodePartial,
  entryDigest,
  MANIFEST_BUCKET_COUNT,
  PartialManifest,
} from '../src/mongo-manifest-hash';

const xor = (a: Buffer, b: Buffer): Buffer => {
  const out = Buffer.alloc(a.length);
  for (let i = 0; i < a.length; i++) out[i] = a[i] ^ b[i];
  return out;
};

describe('mongo-manifest-hash', () => {
  it('keeps the bucket count in lock-step with the anti-entropy module', () => {
    // The two constants are duplicated (leaf module stays dependency-free) but
    // MUST be identical — a divergence corrupts the shared content root.
    expect(MANIFEST_BUCKET_COUNT).toBe(AE_BUCKET_COUNT);
  });

  describe('bucketOf', () => {
    it('is deterministic and lands inside the bucket range', () => {
      for (const key of ['', 'a', 'abc', '2400042', 'äöü']) {
        const b = bucketOf(key);
        expect(b).toBe(bucketOf(key));
        expect(b).toBeGreaterThanOrEqual(0);
        expect(b).toBeLessThan(MANIFEST_BUCKET_COUNT);
        expect(Number.isInteger(b)).toBe(true);
      }
    });

    it('keeps a doc in the same bucket regardless of its hash (keyed on id only)', () => {
      // bucketOf takes only the sliceId, so an UPDATE (new hash) does not move
      // the doc between buckets.
      expect(bucketOf('doc-1')).toBe(bucketOf('doc-1'));
    });

    it('spreads keys across many buckets', () => {
      const seen = new Set<number>();
      for (let i = 0; i < 5000; i++) seen.add(bucketOf(`id-${i}`));
      // FNV-1a gives a wide spread; require clearly-more-than-one bucket.
      expect(seen.size).toBeGreaterThan(1000);
    });
  });

  describe('entryDigest', () => {
    it('returns a stable 32-byte sha256 digest', () => {
      const d = entryDigest('slice', 'a'.repeat(64));
      expect(d).toHaveLength(32);
      expect(d.equals(entryDigest('slice', 'a'.repeat(64)))).toBe(true);
    });

    it('is injective across the id|hash boundary', () => {
      // The `|` separator + fixed-length hash suffix prevents aliasing between
      // (id, hash) pairs whose concatenations would otherwise collide.
      const a = entryDigest('ab', 'c'.repeat(64));
      const b = entryDigest('a', 'b' + 'c'.repeat(64));
      expect(a.equals(b)).toBe(false);
    });
  });

  describe('PartialManifest', () => {
    it('folds documents into entries + accumulators', () => {
      const p = new PartialManifest();
      p.add('k1', '1'.repeat(64));
      p.add('k2', '2'.repeat(64));
      expect(p.entries.size).toBe(2);
      expect(p.entries.get('k1')).toBe('1'.repeat(64));
      // acc == XOR of the two entry digests.
      const expected = xor(
        entryDigest('k1', '1'.repeat(64)),
        entryDigest('k2', '2'.repeat(64)),
      );
      expect(p.acc.equals(expected)).toBe(true);
      // The per-bucket accumulator for k1 holds k1's digest.
      const off = bucketOf('k1') * 32;
      expect(
        p.bucketAcc
          .subarray(off, off + 32)
          .equals(entryDigest('k1', '1'.repeat(64))),
      ).toBe(true);
    });

    it('round-trips through encode/decodePartial byte-for-byte', () => {
      const p = new PartialManifest();
      p.add('short', 'a'.repeat(64));
      p.add('a-longer-slice-id', 'b'.repeat(64));
      p.add('ü-unicode', 'c'.repeat(64));

      const decoded = decodePartial(p.encode());
      expect(decoded.acc.equals(p.acc)).toBe(true);
      expect(decoded.bucketAcc.equals(p.bucketAcc)).toBe(true);
      expect(new Map(decoded.entries)).toEqual(p.entries);
    });

    it('decodes an empty partial', () => {
      const decoded = decodePartial(new PartialManifest().encode());
      expect(decoded.entries).toEqual([]);
      expect(decoded.acc.equals(Buffer.alloc(32))).toBe(true);
    });

    it('merges two disjoint partials by XOR of accumulators', () => {
      // The whole-collection root is the XOR of any disjoint partition of it —
      // this is what lets N range-workers each build one and fold into the whole.
      const whole = new PartialManifest();
      const left = new PartialManifest();
      const right = new PartialManifest();
      for (let i = 0; i < 10; i++) {
        const key = `id-${i}`;
        const hash = i.toString(16).padStart(64, '0');
        whole.add(key, hash);
        (i % 2 === 0 ? left : right).add(key, hash);
      }
      const merged = xor(left.acc, right.acc);
      expect(merged.equals(whole.acc)).toBe(true);
      const mergedBuckets = xor(left.bucketAcc, right.bucketAcc);
      expect(mergedBuckets.equals(whole.bucketAcc)).toBe(true);
    });
  });
});
