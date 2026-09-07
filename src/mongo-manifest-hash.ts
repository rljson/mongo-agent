// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

// .............................................................................
// Shared manifest-hashing primitives used by BOTH the in-process cold-start
// scan (MongoEditSync) and the out-of-process parallel-scan worker
// (sea-edit-hash-entry). Keeping the bucket function, the entry digest, and the
// partial-manifest binary format in ONE place is what guarantees a worker's
// partial result is byte-for-byte mergeable with the main process's — a
// divergence here would corrupt the content root.
// .............................................................................

import { createHash } from 'node:crypto';

/**
 * Number of manifest buckets — MUST equal `AE_BUCKET_COUNT` in
 * `mongo-anti-entropy.ts`. Duplicated as its own constant (rather than imported)
 * only to keep this leaf module dependency-free; the two are asserted equal in
 * the unit tests so they can never silently diverge.
 */
export const MANIFEST_BUCKET_COUNT = 4096;

/**
 * The manifest bucket for a sliceId: a cheap, deterministic FNV-1a fold into
 * `MANIFEST_BUCKET_COUNT` buckets. Non-cryptographic (bucketing needs only a
 * stable, uniform spread and this is on the hot scan path) but identical on
 * every node and in every worker. Keyed on the sliceId alone so an update keeps
 * a doc in the same bucket.
 * @param key - The sliceId (stringified `_id`).
 * @returns The bucket index in `[0, MANIFEST_BUCKET_COUNT)`.
 */
export const bucketOf = (key: string): number => {
  let h = 0x811c9dc5;
  for (let i = 0; i < key.length; i++) {
    h ^= key.charCodeAt(i) & 0xff;
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0) % MANIFEST_BUCKET_COUNT;
};

/**
 * The 32-byte entry digest `sha256(sliceId | docHash)` XOR-folded into the root
 * accumulator. Injective per `(sliceId, docHash)`: the docHash is a fixed-length
 * 64-hex suffix, so the `|`-joined byte stream cannot alias.
 * @param key - The sliceId (stringified `_id`).
 * @param hash - The document content hash (64-hex).
 * @returns The 32-byte digest.
 */
export const entryDigest = (key: string, hash: string): Buffer =>
  createHash('sha256').update(key).update('|').update(hash).digest();

/**
 * A partial manifest built by scanning a subset (an `_id` range) of a
 * collection: the sliceId→docHash entries plus the XOR accumulators they fold
 * into. Two partials over disjoint ranges merge by set-union of entries and XOR
 * of accumulators (XOR is associative + commutative), so N workers can each
 * build one and the main process folds them into the whole.
 */
export class PartialManifest {
  /** sliceId → docHash for every doc in this partition. */
  readonly entries = new Map<string, string>();
  /** 32-byte XOR of every entry digest (the partition's root contribution). */
  readonly acc = Buffer.alloc(32);
  /** `MANIFEST_BUCKET_COUNT × 32` bytes: per-bucket XOR of entry digests. */
  readonly bucketAcc = Buffer.alloc(MANIFEST_BUCKET_COUNT * 32);

  /**
   * Folds one document (its sliceId + content hash) into this partial.
   * @param key - The sliceId (stringified `_id`).
   * @param hash - The document content hash (64-hex).
   */
  add(key: string, hash: string): void {
    this.entries.set(key, hash);
    const d = entryDigest(key, hash);
    const off = bucketOf(key) * 32;
    for (let i = 0; i < 32; i++) {
      this.acc[i] ^= d[i];
      this.bucketAcc[off + i] ^= d[i];
    }
  }

  /**
   * Serializes this partial to a self-describing binary buffer:
   * `[u32 count][acc 32][bucketAcc N*32]` then per entry
   * `[u16 keyLen][key utf8][hash 32 raw]`. Compact + fast to parse (no JSON),
   * and `hash` is stored as its 16 raw bytes... actually 32 hex chars → kept as
   * the 64-hex ASCII to stay loss-free across the odd non-sha256 hash.
   * @returns The encoded buffer.
   */
  encode(): Buffer {
    const parts: Buffer[] = [];
    const header = Buffer.alloc(4);
    header.writeUInt32LE(this.entries.size, 0);
    parts.push(header, this.acc, this.bucketAcc);
    for (const [key, hash] of this.entries) {
      const kb = Buffer.from(key, 'utf8');
      const hb = Buffer.from(hash, 'utf8');
      const meta = Buffer.alloc(4);
      meta.writeUInt16LE(kb.length, 0);
      meta.writeUInt16LE(hb.length, 2);
      parts.push(meta, kb, hb);
    }
    return Buffer.concat(parts);
  }
}

/**
 * The result of decoding a {@link PartialManifest#encode} buffer: the raw
 * accumulators (for XOR-merge) and an iterator-friendly entry list. Returned as
 * plain data (not a `PartialManifest`) because the merger only needs to XOR the
 * accumulators in and copy the entries — never re-fold them.
 */
export interface DecodedPartial {
  /** 32-byte accumulator to XOR into the whole. */
  acc: Buffer;
  /** `MANIFEST_BUCKET_COUNT × 32` bucket accumulators to XOR into the whole. */
  bucketAcc: Buffer;
  /** The `[sliceId, docHash]` entries. */
  entries: Array<[string, string]>;
}

/**
 * Decodes a buffer produced by {@link PartialManifest#encode}.
 * @param buf - The encoded buffer.
 * @returns The accumulators and entries.
 */
export const decodePartial = (buf: Buffer): DecodedPartial => {
  const count = buf.readUInt32LE(0);
  const acc = buf.subarray(4, 36);
  const bucketAcc = buf.subarray(36, 36 + MANIFEST_BUCKET_COUNT * 32);
  let off = 36 + MANIFEST_BUCKET_COUNT * 32;
  const entries: Array<[string, string]> = new Array(count);
  for (let i = 0; i < count; i++) {
    const kl = buf.readUInt16LE(off);
    const hl = buf.readUInt16LE(off + 2);
    off += 4;
    const key = buf.toString('utf8', off, off + kl);
    off += kl;
    const hash = buf.toString('utf8', off, off + hl);
    off += hl;
    entries[i] = [key, hash];
  }
  return { acc, bucketAcc, entries };
};
