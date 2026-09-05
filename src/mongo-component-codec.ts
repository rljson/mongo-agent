// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { EJSON, serialize as bsonSerialize } from 'bson';
import type { Document } from 'mongodb';
import { createHash } from 'node:crypto';

/**
 * BSON-lossless codec between a MongoDB document and an RLJSON component row.
 *
 * Background: the historical `MongoToRljsonConverter.convertDocument` does a
 * lossy JSON roundtrip (ObjectId → hex string, Date → ISO string, Int32 →
 * plain number). That collapse is the root cause of the recurring BSON-type
 * drift: an Int32 `_id` (CARAT uses integer ids) written back as a double, or
 * a Date written back as a string — same value, different BSON type, different
 * state hash, and in some cases a CARAT crash on the integer id.
 *
 * This codec instead uses canonical Extended JSON (`relaxed: false`). Every
 * BSON type survives the full round-trip — Int32 stays Int32, Long stays Long,
 * Date stays Date, ObjectId/Decimal128/Binary keep their type. The serialized
 * form is plain JSON (`$oid` / `$date` / `$numberInt` / … keys), so it is
 * walkable by `@rljson/hash` and byte-identical on every node → the content
 * hash is deterministic and equal across peers.
 */

/** A MongoDB document encoded as a content-addressed RLJSON component. */
export type MongoComponent = Record<string, unknown> & { _hash: string };

// .............................................................................
/**
 * Recursively removes every `_hash` field from a value. `hip()` hashes in
 * place and stamps a deterministic `_hash` on nested objects too; those must
 * be stripped before decoding so the reconstructed document is the pure Mongo
 * payload (and so Extended-JSON wrappers like `{ $binary: … }` decode cleanly).
 * @param value - Any JSON value from a component row.
 * @returns The same structure with all `_hash` keys removed.
 */
export const stripHashes = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(stripHashes);
  if (value && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(value as Record<string, unknown>)) {
      if (key === '_hash') continue;
      out[key] = stripHashes(val);
    }
    return out;
  }
  return value;
};

// .............................................................................
/**
 * Encodes a MongoDB document as a content-addressed RLJSON component.
 *
 * Any pre-existing `_hash` on the input is dropped so the component's hash is
 * derived purely from the document content.
 * @param doc - The raw MongoDB document (may contain BSON types).
 * @returns The document as canonical Extended JSON with a deterministic
 *   `_hash` attached.
 */
export const docToComponent = (doc: Document): MongoComponent => {
  return hip(docToBody(doc) as never) as MongoComponent;
};

// .............................................................................
/**
 * The content hash of a MongoDB document — the value stored in the content-root
 * manifest and used to detect echoes.
 *
 * Serves the same role as `docToComponent(doc)._hash` (a deterministic,
 * cross-node-consistent digest of the document) but is computed by SHA-256 over
 * the document's raw BSON bytes in a single `hash.update()` — the "hash the
 * payload, don't map the structure" approach. That matters for CARAT's mega
 * catalogs:
 * - `hip()`/`JSON.stringify` materialize the whole canonical string in memory,
 *   so a large document overflows V8's ~512MB max string length
 *   (`RangeError: Invalid string length`) and crashed the cold-start scan.
 * - Feeding the canonical form token-by-token to `hash.update()` avoided the
 *   string but, in the SEA runtime, tripped a native `val->IsString()`
 *   assertion in the many tiny string updates.
 * BSON serialization sidesteps both: a MongoDB document is at most 16MB, so its
 * BSON buffer is always small, hashed in one shot with no string path, no giant
 * allocation, and no recursion. It is deterministic and cross-node consistent
 * because peers store byte-identical BSON for the same document (identical field
 * order and BSON types — Int32 stays Int32, so the hash is type-aware just as
 * the old canonical hash was).
 * @param doc - The raw MongoDB document (may contain BSON types).
 * @returns The document's 64-hex content hash.
 */
export const docHash = (doc: Document): string =>
  createHash('sha256').update(bsonSerialize(doc)).digest('hex');

// .............................................................................
/**
 * Serializes a MongoDB document to its clean canonical-Extended-JSON body — the
 * plain-JSON payload with every pre-existing `_hash` removed, but WITHOUT
 * hashing it. Used when the body is stored under a single `json` column (so the
 * document's arbitrary fields don't each need a declared table column); the row
 * is hashed by the caller. A document carried over from an earlier sync can
 * hold stale nested `_hash` fields, which `hip()` would reject — stripping them
 * makes the content hash derive purely from the payload.
 * @param doc - The raw MongoDB document (may contain BSON types).
 * @returns The document as a clean canonical-Extended-JSON object.
 */
export const docToBody = (doc: Document): Record<string, unknown> => {
  const ejson = EJSON.serialize(doc, { relaxed: false }) as Record<
    string,
    unknown
  >;
  return stripHashes(ejson) as Record<string, unknown>;
};

// .............................................................................
/**
 * Decodes a clean Extended-JSON body (from {@link docToBody}, possibly carrying
 * nested `_hash` fields stamped by `hip()`) back into a MongoDB document with
 * its BSON types restored.
 * @param body - The stored `json` document body.
 * @returns The MongoDB document with BSON types restored.
 */
export const bodyToDoc = (body: Record<string, unknown>): Document => {
  const clean = stripHashes(body) as Record<string, unknown>;
  return EJSON.deserialize(clean, { relaxed: false }) as Document;
};

// .............................................................................
/**
 * Decodes an RLJSON component back into a MongoDB document, restoring the
 * original BSON types (Int32, Long, Date, ObjectId, Decimal128, Binary, …) so
 * a write-back preserves the exact on-disk types.
 *
 * The component's own `_hash` is stripped before decoding.
 * @param component - A component produced by {@link docToComponent}.
 * @returns The MongoDB document with BSON types restored.
 */
export const componentToDoc = (component: MongoComponent): Document =>
  bodyToDoc(component);
