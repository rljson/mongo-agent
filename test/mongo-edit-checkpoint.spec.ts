// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

import { EditCheckpoint } from '../src/mongo-edit-checkpoint.ts';

describe('EditCheckpoint', () => {
  const dirs: string[] = [];
  afterEach(async () => {
    for (const d of dirs.splice(0)) await rm(d, { recursive: true, force: true });
  });
  const mkDir = async (): Promise<string> => {
    const d = await mkdtemp(join(tmpdir(), 'edit-cp-'));
    dirs.push(d);
    return d;
  };

  it('load returns undefined when no checkpoint exists', async () => {
    const cp = new EditCheckpoint(await mkDir());
    expect(await cp.load('customers')).toBeUndefined();
  });

  it('save then load round-trips manifest + token, atomically overwriting', async () => {
    const cp = new EditCheckpoint(await mkDir());
    await cp.save('customers', new Map([['1', 'ha'], ['2', 'hb']]), { tk: 'T1' });
    const loaded = await cp.load('customers');
    expect(loaded?.token).toEqual({ tk: 'T1' });
    expect(loaded?.manifest).toEqual({ '1': 'ha', '2': 'hb' });

    // A second save atomically replaces the file (tmp + rename).
    await cp.save('customers', new Map([['1', 'ha']]), { tk: 'T2' });
    const l2 = await cp.load('customers');
    expect(l2?.token).toEqual({ tk: 'T2' });
    expect(l2?.manifest).toEqual({ '1': 'ha' });
  });

  it('normalizes a nullish token to null', async () => {
    const cp = new EditCheckpoint(await mkDir());
    await cp.save('c', new Map(), undefined);
    expect((await cp.load('c'))?.token).toBeNull();
  });

  it('defaults missing manifest/token fields when loading a sparse file', async () => {
    const dir = await mkDir();
    const cp = new EditCheckpoint(dir);
    await writeFile(join(dir, 'customers.json'), '{}', 'utf8');
    const loaded = await cp.load('customers');
    expect(loaded).toEqual({ manifest: {}, token: null });
  });

  it('load returns undefined for a corrupt file', async () => {
    const dir = await mkDir();
    const cp = new EditCheckpoint(dir);
    await writeFile(join(dir, 'customers.json'), 'not json{', 'utf8');
    expect(await cp.load('customers')).toBeUndefined();
  });

  // A manifest larger than the stream's 16 KiB high-water mark makes
  // `out.write()` return false, which is the whole point of the streamed
  // writer: it waits for 'drain' instead of buffering the file in memory.
  // Anything smaller never exercises that path, so the guarantee that a
  // mega manifest costs constant memory would go untested.
  it('applies backpressure and still round-trips a manifest past the high-water mark', async () => {
    const cp = new EditCheckpoint(await mkDir());
    const manifest = new Map<string, string>();
    for (let i = 0; i < 5_000; i++) {
      manifest.set(`slice-${i}`, `hash-${i}`.padEnd(64, '0'));
    }
    await cp.save('bigColl', manifest, { _data: 'resume' });

    const loaded = await cp.load('bigColl');
    expect(loaded?.token).toEqual({ _data: 'resume' });
    expect(Object.keys(loaded?.manifest ?? {})).toHaveLength(5_000);
    expect(loaded?.manifest['slice-4999']).toBe(manifest.get('slice-4999'));
  });

  // The reader skips empty lines so a file that ends with a newline — every
  // file `save` writes — does not parse '' as an entry.
  it('ignores blank lines in a streamed checkpoint', async () => {
    const dir = await mkDir();
    const cp = new EditCheckpoint(dir);
    await writeFile(
      join(dir, 'gapped.json'),
      `${JSON.stringify({ token: null })}\n\n${JSON.stringify(['a', 'h1'])}\n\n`,
      'utf8',
    );
    expect(await cp.load('gapped')).toEqual({ manifest: { a: 'h1' }, token: null });
  });

  it('percent-encodes path-unsafe collection names', async () => {
    const cp = new EditCheckpoint(await mkDir());
    await cp.save('a/b:c', new Map([['x', 'h']]), { tk: 'T' });
    expect((await cp.load('a/b:c'))?.manifest).toEqual({ x: 'h' });
  });
});
