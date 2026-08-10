// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { MongoScanner } from '../src/mongo-scanner.ts';


/**
 * Tests for the chunked-streaming path in `_scanCollection`.
 *
 * Earlier the scanner had a single `convertCollection` + `JSON.stringify`
 * path that returned `null` and warned when the result was too big to fit
 * in one V8 string (~512 MB cap). That left big prod collections
 * (cd_articles at 1.4 M docs, cd_models at 9 M) un-synced.
 *
 * The scanner now picks one of two paths per collection:
 *  - Path A (small): `estimatedDocumentCount() <= 50 k`. Single-shot, single
 *    blob, same tree layout as before (zero hash drift for unchanged colls).
 *  - Path B (big): cursor-streaming, one ComponentsTable per ~50 k-doc chunk,
 *    each stored as its own blob. The scanner returns one Tree per chunk;
 *    the apply path on the peer side iterates per-tree, so multi-chunk
 *    collections naturally produce N (name, blobId) upsert passes — no
 *    multi-blob logic needed downstream.
 */
describe('MongoScanner._scanCollection: chunked streaming', () => {
  let warnSpy: ReturnType<typeof vi.spyOn>;
  let logSpy: ReturnType<typeof vi.spyOn>;
  beforeEach(() => {
    warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
  });
  afterEach(() => {
    warnSpy.mockRestore();
    logSpy.mockRestore();
  });

  /**
   * Fake mongo Db + collection. estimatedDocumentCount controls which path
   * the scanner takes. The other methods are stubs sufficient for the unit
   * under test — the streaming path uses our stubbed `convertCollectionStreaming`,
   * not the real cursor walk.
   */
  const makeFakeDb = (estimatedCount: number) =>
    ({
      databaseName: 'caratdb',
      collection: () => ({
        estimatedDocumentCount: async () => estimatedCount,
      }),
    }) as any;

  it('small collection: single-shot path emits exactly one tree (old layout)', async () => {
    const scanner = new MongoScanner(makeFakeDb(38)) as any;
    scanner._converter = {
      discoverSchema: async () => ({ columns: [{ key: '_id' }], _hash: 'h1' }),
      mergeTableCfg: (a: any) => a,
      convertCollection: async () => ({
        _data: [{ _id: 1 }, { _id: 2 }],
        _hash: 'small',
      }),
    };
    scanner._bs = { setBlob: async () => ({ blobId: 'blob-A' }) };
    const trees = await scanner._scanCollection('sd_options', new Map());
    expect(trees).toHaveLength(1);
    expect((trees[0] as any).meta.componentsBlobId).toBe('blob-A');
    expect((trees[0] as any).meta.collection).toBe('sd_options');
    expect((trees[0] as any).meta.chunkIndex).toBeUndefined();
  });

  it('big collection: streams into multiple chunk trees with distinct blob ids', async () => {
    const scanner = new MongoScanner(makeFakeDb(150_000)) as any;
    let blobCounter = 0;
    scanner._bs = {
      setBlob: async () => ({ blobId: `chunk-blob-${blobCounter++}` }),
    };
    scanner._converter = {
      discoverSchema: async () => ({ columns: [], _hash: 'h1' }),
      mergeTableCfg: (a: any) => a,
      convertCollection: async () => {
        throw new Error('streaming path must NOT use convertCollection');
      },
      // 3 chunks of fake data
      convertCollectionStreaming: async function* () {
        for (let i = 0; i < 3; i++) {
          yield { _data: new Array(50_000).fill({ _id: i }), _hash: `c${i}` };
        }
      },
    };
    const trees = await scanner._scanCollection('cd_articles', new Map());
    expect(trees).toHaveLength(3);
    const ids = trees.map((t: any) => t.meta.componentsBlobId).sort();
    expect(ids).toEqual(['chunk-blob-0', 'chunk-blob-1', 'chunk-blob-2']);
    // Each chunk MUST carry chunkIndex so the hash is unique even when
    // _data collides; without it the content-addressed Tree map would
    // collapse two chunks into one and lose data.
    const chunkIndexes = trees
      .map((t: any) => t.meta.chunkIndex)
      .sort();
    expect(chunkIndexes).toEqual([0, 1, 2]);
    // All chunks share the same collection name (so the apply path's
    // (name, blobId) collector picks each up under the right target).
    for (const t of trees) {
      expect((t as any).meta.collection).toBe('cd_articles');
      expect((t as any).meta.type).toBe('collection');
    }
  });

  it('big collection with one giant chunk: drops the chunk, keeps the rest', async () => {
    // If a single chunk would still bust the V8 string cap (extreme
    // avg-doc-size), we warn and drop that chunk but continue iterating the
    // others — so the collection still partially syncs.
    const scanner = new MongoScanner(makeFakeDb(100_000)) as any;
    let blobCounter = 0;
    scanner._bs = {
      setBlob: async () => ({ blobId: `b${blobCounter++}` }),
    };
    scanner._converter = {
      discoverSchema: async () => ({ columns: [], _hash: 'h1' }),
      mergeTableCfg: (a: any) => a,
      convertCollection: async () => {
        throw new Error('unused');
      },
      // chunk 0 OK, chunk 1 fails JSON.stringify, chunk 2 OK
      convertCollectionStreaming: async function* () {
        yield { _data: [{ _id: 'a' }], _hash: 'c0' };
        yield {
          _data: [{ _id: 'b' }],
          _hash: 'c1',
          bad: {
            toJSON: () => {
              throw new RangeError('Invalid string length');
            },
          },
        };
        yield { _data: [{ _id: 'c' }], _hash: 'c2' };
      },
    };
    const trees = await scanner._scanCollection('cd_models', new Map());
    expect(trees).toHaveLength(2);
    expect(warnSpy).toHaveBeenCalledTimes(1);
    const warning = String(warnSpy.mock.calls[0][0]);
    expect(warning).toContain('cd_models');
    expect(warning).toContain('chunk 1');
  });

  it('non-RangeError in a chunk is rethrown (does not silently swallow real bugs)', async () => {
    const scanner = new MongoScanner(makeFakeDb(100_000)) as any;
    scanner._bs = { setBlob: async () => ({ blobId: 'b' }) };
    scanner._converter = {
      discoverSchema: async () => ({ columns: [], _hash: 'h1' }),
      mergeTableCfg: (a: any) => a,
      convertCollection: async () => {
        throw new Error('unused');
      },
      convertCollectionStreaming: async function* () {
        yield {
          _data: [{ _id: 1 }],
          _hash: 'c0',
          bad: {
            toJSON: () => {
              throw new TypeError('boom from converter');
            },
          },
        };
      },
    };
    await expect(
      scanner._scanCollection('genuine-bug', new Map()),
    ).rejects.toThrow(/boom from converter/);
  });

  it('estimatedDocumentCount failure forces streaming path (safer than OOM)', async () => {
    const fakeDb = {
      databaseName: 'caratdb',
      collection: () => ({
        estimatedDocumentCount: async () => {
          throw new Error('not authorized');
        },
      }),
    } as any;
    const scanner = new MongoScanner(fakeDb) as any;
    scanner._bs = { setBlob: async () => ({ blobId: 'b' }) };
    let streamingUsed = false;
    scanner._converter = {
      discoverSchema: async () => ({ columns: [], _hash: 'h1' }),
      mergeTableCfg: (a: any) => a,
      convertCollection: async () => {
        throw new Error('estimated-count failure should NOT use single-shot');
      },
      convertCollectionStreaming: async function* () {
        streamingUsed = true;
        yield { _data: [{ _id: 1 }], _hash: 'c0' };
      },
    };
    const trees = await scanner._scanCollection('uncertain-size', new Map());
    expect(streamingUsed).toBe(true);
    expect(trees).toHaveLength(1);
  });
});
