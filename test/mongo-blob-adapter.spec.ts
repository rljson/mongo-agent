// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.

import { BsMem } from '@rljson/bs';
import { describe, expect, it } from 'vitest';

import {
  MongoBlobAdapter,
  type DocumentBlobMeta,
} from '../src/mongo-blob-adapter.ts';

describe('MongoBlobAdapter', () => {
  it('constructs a default BsMem when no bs is injected', () => {
    const adapter = new MongoBlobAdapter();
    expect(adapter.bs).toBeInstanceOf(BsMem);
  });

  it('uses the injected bs instance and exposes it via the getter', () => {
    const bs = new BsMem();
    const adapter = new MongoBlobAdapter(bs);
    expect(adapter.bs).toBe(bs);
  });

  it('documentToBlob stores content and returns correct metadata', async () => {
    const bs = new BsMem();
    const adapter = new MongoBlobAdapter(bs);
    const doc = { _id: 'abc123', name: 'Ada', value: 42 };

    const meta = await adapter.documentToBlob(doc, 'caratdb', 'customers');

    expect(meta.docId).toBe('abc123');
    expect(meta.collection).toBe('customers');
    expect(meta.database).toBe('caratdb');
    expect(typeof meta.blobId).toBe('string');
    expect(meta.blobId.length).toBeGreaterThan(0);
    expect(meta.size).toBe(Buffer.from(JSON.stringify(doc), 'utf-8').length);
    expect(typeof meta.mtime).toBe('number');

    // The blob really landed in the injected store and round-trips.
    const blob = await bs.getBlob(meta.blobId);
    expect(JSON.parse(blob.content.toString('utf-8'))).toEqual(doc);
  });

  it('documentToBlob stringifies a non-string _id via String(doc._id)', async () => {
    const adapter = new MongoBlobAdapter();
    const meta = await adapter.documentToBlob(
      { _id: 12345 },
      'db',
      'coll',
    );
    expect(meta.docId).toBe('12345');
  });

  it('documentToBlob honors options.bs override (not the instance bs)', async () => {
    const instanceBs = new BsMem();
    const overrideBs = new BsMem();
    const adapter = new MongoBlobAdapter(instanceBs);
    const doc = { _id: 'x', payload: 'override-target' };

    const meta = await adapter.documentToBlob(doc, 'db', 'coll', {
      bs: overrideBs,
    });

    // Stored in override, retrievable from override.
    const fromOverride = await overrideBs.getBlob(meta.blobId);
    expect(JSON.parse(fromOverride.content.toString('utf-8'))).toEqual(doc);

    // Instance store does not have it.
    await expect(instanceBs.getBlob(meta.blobId)).rejects.toBeDefined();
  });

  it('documentsToBlobs converts every doc and preserves order', async () => {
    const adapter = new MongoBlobAdapter();
    const docs = [
      { _id: 'a', n: 1 },
      { _id: 'b', n: 2 },
      { _id: 'c', n: 3 },
    ];

    const metas = await adapter.documentsToBlobs(docs, 'db', 'coll');

    expect(metas).toHaveLength(3);
    expect(metas.map((m) => m.docId)).toEqual(['a', 'b', 'c']);
  });

  it('documentsToBlobs returns an empty array for no docs', async () => {
    const adapter = new MongoBlobAdapter();
    const metas = await adapter.documentsToBlobs([], 'db', 'coll');
    expect(metas).toEqual([]);
  });

  it('blobToDocument round-trips a stored document', async () => {
    const adapter = new MongoBlobAdapter();
    const doc = { _id: 'rt1', greeting: 'hello', nested: { ok: true } };

    const meta = await adapter.documentToBlob(doc, 'db', 'coll');
    const restored = await adapter.blobToDocument(meta);

    expect(restored).toEqual(doc);
  });

  it('blobToDocument honors options.bs override', async () => {
    const overrideBs = new BsMem();
    const adapter = new MongoBlobAdapter(); // instance bs is separate default
    const doc = { _id: 'ov', val: 'from-override' };

    const meta = await adapter.documentToBlob(doc, 'db', 'coll', {
      bs: overrideBs,
    });
    const restored = await adapter.blobToDocument(meta, { bs: overrideBs });

    expect(restored).toEqual(doc);
  });

  it('blobsToDocuments restores every document in order', async () => {
    const adapter = new MongoBlobAdapter();
    const docs = [
      { _id: 'a', n: 1 },
      { _id: 'b', n: 2 },
    ];
    const metas = await adapter.documentsToBlobs(docs, 'db', 'coll');

    const restored = await adapter.blobsToDocuments(metas);

    expect(restored).toEqual(docs);
  });

  it('blobsToDocuments returns an empty array for no metadata', async () => {
    const adapter = new MongoBlobAdapter();
    const restored = await adapter.blobsToDocuments([] as DocumentBlobMeta[]);
    expect(restored).toEqual([]);
  });
});
