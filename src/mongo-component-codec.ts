// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import {
  deserialize as bsonDeserialize,
  EJSON,
  serialize as bsonSerialize,
} from 'bson';
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
 * Recursively sorts plain-object keys so a serialize-based hash is
 * field-order-insensitive. Two documents with the same fields in a different
 * order (the codec round-trip reorders nested keys) then hash the same.
 *
 * A BSON type wrapper (ObjectId, Int32, Long, Double, Decimal128, Binary,
 * Timestamp, …) carries a `_bsontype` tag and a native `Date` is a Date
 * instance — both are LEAF values: reordering their internal fields would
 * corrupt them, so they pass through untouched. Arrays keep their order (order
 * is data in an array, not incidental).
 * @param value - Any value from a MongoDB document.
 * @returns The value with every nested plain object's keys sorted.
 */
export const sortKeys = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (
    value !== null &&
    typeof value === 'object' &&
    (value as { _bsontype?: unknown })._bsontype === undefined &&
    !(value instanceof Date)
  ) {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      out[key] = sortKeys((value as Record<string, unknown>)[key]);
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
 * BSON serialization sidesteps the giant-string/native-assert traps: a MongoDB
 * document is at most 16MB, so its BSON buffer is small and hashed in one shot.
 *
 * CANONICALIZATION (why the raw BSON is not hashed directly). `bsonSerialize` is
 * sensitive to BOTH field order and BSON number type, and the earlier assumption
 * that "peers store byte-identical BSON for the same document" is FALSE for any
 * doc that travelled the edit chain: the codec's canonical-EJSON round-trip
 * ({@link docToBody}/{@link bodyToDoc}) both reorders nested-object keys AND can
 * leave a value in a different number type than the source read from Mongo. The
 * source hashed its raw `fullDocument`, every receiver hashed the round-tripped
 * form, and the two disagreed — so the content root diverged for content that is
 * identical by value (the order/type-insensitive state checkpoint matched on
 * every node), and anti-entropy re-reconciled that phantom-differing bucket
 * forever, flooding the connector and starving live head propagation (observed
 * live 2026-09-09: `ae … want+=0 drop+=0` looping, live inserts reaching only the
 * origin). {@link mongoCanonical} pins the number type to Mongo's promoted form
 * and {@link sortKeys} orders the fields, so source and receiver agree for every
 * BSON type CARAT stores (Int32, Double, string, Date, ObjectId, Decimal128,
 * Binary, nested objects). The hash is now value-equal for ambiguous numbers (an
 * Int32 `1` and a Double `1` hash the same — which is exactly what convergence
 * needs, since either may be what a node read back). KNOWN LIMIT: an integer
 * larger than 2^53 stored as a `Long` loses precision through the codec's EJSON
 * round-trip itself (a data-level codec bug, not a hashing one), so such a doc
 * cannot converge here — CARAT catalog/currency data does not use values that
 * large. Canonicalizing materializes a per-doc copy — bounded (≤16MB, no giant
 * string, no `hip` recursion) — a small cold-start cost for a hash that converges.
 * @param doc - The raw MongoDB document (may contain BSON types).
 * @returns The document's 64-hex content hash.
 */
export const docHash = (doc: Document): string =>
  createHash('sha256')
    .update(bsonSerialize(sortKeys(mongoCanonical(doc)) as Document))
    .digest('hex');

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

// .............................................................................
/**
 * Normalizes a document to the exact form MongoDB hands back on a read.
 *
 * The driver serializes an inserted JS value with its own rules and, on read,
 * promotes what it can back to plain JS numbers (`promoteLongs`/`promoteValues`
 * are on by default). Two of those rules disagree with canonical Extended JSON:
 * canonical EJSON encodes any integer above the Int32 range as `$numberLong`,
 * but the driver stores a large *JS-number* integer (e.g. `Date.now()`) as a
 * BSON **Double**. So a document decoded from a component ({@link bodyToDoc}
 * hands back a BSON `Long`) is NOT byte-identical to the same document read
 * straight from Mongo (a promoted number → stored Double) — and since
 * {@link docHash} hashes the raw BSON bytes, the two forms hash differently.
 *
 * That single-byte type gap is enough to break convergence: the content hash of
 * a PULLED doc (anti-entropy backfill) never equals the hash of the same doc
 * read natively on every other node, so the manifest/content-root disagree, the
 * echo of the write is not recognised, and anti-entropy re-pulls the same
 * bucket forever (observed live: a 100k bulk import wedged three nodes at a
 * fraction of the delta, `pullAndApply applied=1000` every round, 2026-09-07).
 *
 * A driver-faithful serialize→deserialize round-trip collapses the typed
 * wrappers to precisely the promoted form Mongo returns, so a doc reconstructed
 * from a component is byte-identical to its stored-and-read-back twin. Types
 * Mongo keeps distinct (Date, ObjectId, Decimal128, Binary, an Int32 id, a Long
 * too large to promote) survive unchanged — only the ambiguous integer-vs-Double
 * numbers are pinned to the storage form.
 * @param doc - A document decoded from a component (may hold typed BSON numbers).
 * @returns The same document in the byte-exact shape Mongo returns on read.
 */
export const mongoCanonical = (doc: Document): Document =>
  bsonDeserialize(bsonSerialize(doc)) as Document;
