// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh } from '@rljson/hash';
import type { Collection, Document } from 'mongodb';

import type { ColumnCfg, ComponentsTable, TableCfg } from '@rljson/rljson';

/**
 * MongoDB to RLJSON Component Converter
 *
 * Agnostic mode: Mongo docs are passed through as-is into the ComponentsTable
 * `_data` rows; only an `_hash` field is appended. We DO NOT do schema-
 * discovery / type-cast / per-field re-shape anymore. Reasoning:
 *
 *   - The previous "discover-then-cast" pipeline mapped BSON Date → number
 *     etc. so the receiving node ended up with a BSON Number where the
 *     sender had a BSON Date — same value, different BSON type, different
 *     state-merkle-hash → phantom conflicts + dbRoot drift.
 *   - Staying agnostic means whatever a user puts into Mongo travels
 *     untouched. The downside is BSON-specific types (Date, ObjectId, …)
 *     still round-trip through plain JSON, so they re-emerge on the other
 *     side as their JSON representation (ISO string, hex string). If you
 *     need true BSON-type preservation across the wire, that's a separate
 *     change to switch the blob serialiser to EJSON.
 *
 * The TableCfg returned by `discoverSchema` is kept for API compatibility
 * but is intentionally minimal — downstream code that iterates over it for
 * type casting will be a no-op.
 */
export class MongoToRljsonConverter {
  /**
   * Discovers schema from a MongoDB collection.
   *
   * In agnostic mode this no longer inspects documents; it returns a minimal
   * `TableCfg` carrying only the `_hash` column, kept for API compatibility.
   * @param collection - The MongoDB collection whose schema (table key) is read.
   * @param _sampleSize - Retained for API compatibility; ignored in agnostic
   *   mode where documents are no longer sampled to infer field types.
   * @returns A minimal TableCfg with just the `_hash` column.
   */
  async discoverSchema(
    collection: Collection,
    _sampleSize = 100,
  ): Promise<TableCfg> {
    // Agnostic mode: we no longer inspect documents to infer per-field types.
    // Returns a minimal TableCfg with just the `_hash` column. The whole
    // doc lives in the ComponentsTable `_data` row unmodified.
    void _sampleSize;
    return hip<TableCfg>({
      key: collection.collectionName,
      type: 'components',
      columns: [
        {
          key: '_hash',
          type: 'string',
          titleLong: 'Hash',
          titleShort: 'Hash',
        },
      ],
      isHead: false,
      isRoot: false,
      isShared: true,
      _hash: '',
    });
  }

  /**
   * Converts MongoDB collection to RLJSON ComponentsTable
   * @param collection - MongoDB collection
   * @param tableCfg - Table configuration
   * @param limit - Maximum number of documents to convert (optional)
   * @returns ComponentsTable with all documents
   */
  async convertCollection(
    collection: Collection,
    tableCfg: TableCfg,
    limit?: number,
  ): Promise<ComponentsTable<any>> {
    const query = limit ? collection.find().limit(limit) : collection.find();
    const docs = await query.toArray();

    // Convert in chunks with setImmediate yields between them so the event
    // loop can answer HTTP requests / process change-stream events while
    // a big collection (10k+ docs) is being serialised. Without this the
    // initial scan on startup froze the UI for several seconds.
    const data: any[] = new Array(docs.length);
    const CHUNK = 500;
    for (let i = 0; i < docs.length; i += CHUNK) {
      const end = Math.min(i + CHUNK, docs.length);
      for (let j = i; j < end; j++) {
        data[j] = this.convertDocument(docs[j], tableCfg);
      }
      if (end < docs.length) {
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
    }

    const componentsTable = hip<ComponentsTable<any>>({
      _tableCfg: tableCfg._hash as string,
      _type: 'components',
      _data: data,
      _hash: '',
    });

    return componentsTable;
  }

  /* eslint-disable tsdoc/syntax -- jsdoc/require-yields mandates an @yields tag
     with a {type}, but the tsdoc parser has no @yields tag defined and rejects
     the braces; the two configured plugins conflict on this generator. */
  /**
   * Cursor-streaming variant of `convertCollection`. Yields chunks of at most
   * `chunkSize` docs converted to ComponentsTables. The whole point is to
   * AVOID `cursor.toArray()` (which holds N×~5 KB docs in V8 at once and
   * OOMs on a 1.4 M-doc `cd_articles`-class collection).
   *
   * Caller iterates with `for await`; per-chunk memory is bounded by
   * `chunkSize` × avg-doc-size. The cursor's own internal buffer is the
   * driver's `batchSize` (kept small at 500).
   *
   * Each emitted ComponentsTable is independently hashable and small enough
   * to JSON.stringify on a 4 GB heap.
   * @param collection - The MongoDB collection streamed via a sorted, batched
   *   cursor (`_id` ascending, driver `batchSize` 500) instead of `toArray()`.
   * @param tableCfg - Table configuration whose `_hash` identifies the config
   *   referenced by every emitted ComponentsTable.
   * @param chunkSize - Maximum number of converted documents per emitted
   *   ComponentsTable; bounds per-chunk heap usage.
   * @yields {ComponentsTable} A ComponentsTable holding up to `chunkSize`
   *   converted documents.
   */
  /* eslint-enable tsdoc/syntax */
  async *convertCollectionStreaming(
    collection: Collection,
    tableCfg: TableCfg,
    chunkSize: number,
  ): AsyncGenerator<ComponentsTable<any>, void, void> {
    const cursor = collection.find({}, { sort: { _id: 1 }, batchSize: 500 });
    let buf: any[] = [];
    try {
      while (await cursor.hasNext()) {
        const doc = await cursor.next();
        if (!doc) break;
        buf.push(this.convertDocument(doc, tableCfg));
        if (buf.length >= chunkSize) {
          const out = hip<ComponentsTable<any>>({
            _tableCfg: tableCfg._hash as string,
            _type: 'components',
            _data: buf,
            _hash: '',
          });
          buf = [];
          // Yield to the event loop so the test API and change-stream
          // listener can run between chunks.
          await new Promise<void>((resolve) => setImmediate(resolve));
          yield out;
        }
      }
      if (buf.length > 0) {
        yield hip<ComponentsTable<any>>({
          _tableCfg: tableCfg._hash as string,
          _type: 'components',
          _data: buf,
          _hash: '',
        });
      }
    } finally {
      await cursor.close().catch(() => {});
    }
  }

  /**
   * Agnostic conversion: the row IS the Mongo doc. We only attach a
   * deterministic `_hash` so downstream rljson code (which expects rows
   * to be hashed) keeps working. `tableCfg` is intentionally ignored.
   *
   * Implementation note: `hsh()` from `@rljson/hash` walks the value with a
   * strict JSON-only allowlist — it throws `"Unsupported type: object"` on
   * BSON-specific types (ObjectId, Date, Decimal128, Long, Buffer, …). To
   * stay agnostic AND keep `hsh()` happy we do a single JSON-roundtrip
   * before hashing:
   *
   *   - `ObjectId.toJSON()` → hex string
   *   - `Date.toJSON()`     → ISO string
   *   - `Buffer.toJSON()`   → `{ type: 'Buffer', data: [...] }`
   *   - any other class with a `toJSON()` collapses to plain JSON
   *
   * This is a lossy normalization (types collapse to their JSON shape), but
   * it is the same on both nodes, so the same payload travels untouched
   * and the row-hash is deterministic. If you ever need true BSON-type
   * preservation across the wire, switch the blob serialiser to EJSON.
   * @param doc - The raw MongoDB document; its existing `_hash` is stripped and
   *   the rest is JSON-roundtripped before a fresh deterministic hash is added.
   * @param _tableCfg - Retained for API compatibility; intentionally ignored
   *   since agnostic conversion does not reshape rows per column config.
   * @returns The document as a plain JSON object with a deterministic `_hash`.
   */
  convertDocument(doc: Document, _tableCfg: TableCfg): any {
    void _tableCfg;
    const { _hash: _existing, ...rest } = doc as Record<string, unknown>;
    void _existing;
    // JSON-roundtrip: forces every BSON-shape into its toJSON() form so
    // @rljson/hash can walk it without "Unsupported type: object".
    const plain = JSON.parse(JSON.stringify(rest));
    return hsh({ ...plain, _hash: '' });
  }

  /**
   * Merges two TableCfgs into a new one containing the union of columns.
   * Columns are deduplicated by `key`; the existing column type wins (to
   * keep historical row hashes stable). Caller is responsible for hashing
   * the result via `hip()` if needed.
   * @param existing - The current TableCfg; its column types win on key
   *   collisions to keep historical row hashes stable.
   * @param incoming - The TableCfg whose columns are added only for keys not
   *   already present in `existing`.
   * @returns A new TableCfg containing the union of columns (`_hash` first,
   *   the rest sorted by key).
   */
  mergeTableCfg(existing: TableCfg, incoming: TableCfg): TableCfg {
    const byKey = new Map<string, ColumnCfg>();
    for (const c of existing.columns) byKey.set(c.key, c);
    for (const c of incoming.columns) {
      if (!byKey.has(c.key)) byKey.set(c.key, c);
    }
    const columns = Array.from(byKey.values()).sort((a, b) =>
      a.key === '_hash' ? -1 : b.key === '_hash' ? 1 : a.key.localeCompare(b.key),
    );
    return hip<TableCfg>({
      key: existing.key,
      type: 'components',
      columns,
      isHead: existing.isHead,
      isRoot: existing.isRoot,
      isShared: existing.isShared,
      _hash: '',
    });
  }

}
