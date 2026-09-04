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

  it('percent-encodes path-unsafe collection names', async () => {
    const cp = new EditCheckpoint(await mkDir());
    await cp.save('a/b:c', new Map([['x', 'h']]), { tk: 'T' });
    expect((await cp.load('a/b:c'))?.manifest).toEqual({ x: 'h' });
  });
});
