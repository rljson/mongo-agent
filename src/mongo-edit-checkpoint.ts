// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, readFile, rename } from 'node:fs/promises';
import { join } from 'node:path';
import { createInterface } from 'node:readline';

// .............................................................................

/**
 * Prefix a streamed checkpoint's first line carries. That line IS
 * `{"token":…}`; every line after it is one `[sliceId, hash]` pair, so neither
 * writing nor reading a mega manifest ever materializes it as a single string.
 * A file not starting with this is a legacy single-object checkpoint.
 */
const STREAM_HEADER = '{"token"';

/**
 * The small, MUTABLE per-collection sync pointer persisted across restarts.
 *
 * Deliberately minimal — it holds only what a restart needs to skip the full
 * cold-start:
 * - `manifest`: the `sliceId → doc content-hash` map. Its SHA-256 is the
 *   collection's content root, so restoring it makes the root known instantly
 *   without re-reading every document.
 * - `token`: the MongoDB change-stream resume token, so the stream reopens with
 *   `resumeAfter` and replays only the changes missed while the agent was down.
 *
 * It stores NO document bodies and NO cake/edit history: Mongo is the body
 * source of truth, and the content root (not a cake replay) drives baseline
 * convergence, so the cake stays a fresh, incremental append after a restart.
 */
export interface EditCheckpointState {
  /** `sliceId → doc content-hash` (the content-root manifest). */
  manifest: Record<string, string>;
  /** The MongoDB change-stream resume token, or `null` if none captured yet. */
  token: unknown | null;
}

/**
 * On-disk checkpoint store for {@link EditCheckpointState}, one JSON file per
 * collection under a single directory. Writes are atomic (tmp file + rename) so
 * a crash mid-write never corrupts a live checkpoint.
 */
export class EditCheckpoint {
  /**
   * Creates a checkpoint store rooted at a directory.
   * @param _dir - Directory the per-collection checkpoint files live in.
   */
  constructor(private readonly _dir: string) {}

  /**
   * The checkpoint file path for a collection (name percent-encoded so a
   * collection with path-unsafe characters cannot escape the directory).
   * @param collection - The collection name.
   * @returns The absolute checkpoint file path.
   */
  private _file(collection: string): string {
    return join(this._dir, `${encodeURIComponent(collection)}.json`);
  }

  /**
   * Loads a collection's checkpoint, or `undefined` when there is none yet or
   * the file is unreadable/corrupt (either way the caller does a full snapshot).
   * @param collection - The collection to load.
   * @returns The persisted state, or `undefined`.
   */
  async load(collection: string): Promise<EditCheckpointState | undefined> {
    const file = this._file(collection);
    let head: string;
    try {
      head = await this._firstLine(file);
    } catch {
      return undefined;
    }
    // Legacy single-object files predate the streamed format. They are small
    // by construction — the old writer refused anything large — so reading one
    // whole is safe.
    if (!head.startsWith(STREAM_HEADER)) {
      try {
        const parsed = JSON.parse(
          await readFile(file, 'utf8'),
        ) as Partial<EditCheckpointState>;
        return { manifest: parsed.manifest ?? {}, token: parsed.token ?? null };
      } catch {
        return undefined;
      }
    }
    try {
      return await this._readStreamed(file);
    } catch {
      /* v8 ignore next -- @preserve unreadable mid-file: caller re-scans */
      return undefined;
    }
  }

  /**
   * The first line of a file, without reading the rest of it.
   * @param file - The file to peek into.
   * @returns The first line.
   */
  private async _firstLine(file: string): Promise<string> {
    const stream = createReadStream(file, { encoding: 'utf8' });
    try {
      for await (const line of createInterface({ crlfDelay: Infinity, input: stream })) {
        return line;
      }
      /* v8 ignore next -- @preserve an empty file has no first line */
      return '';
    } finally {
      stream.destroy();
    }
  }

  /**
   * Reads a streamed checkpoint line by line, so a mega manifest never becomes
   * one giant string.
   * @param file - The checkpoint file.
   * @returns The persisted state.
   */
  private async _readStreamed(
    file: string,
  ): Promise<EditCheckpointState | undefined> {
    const manifest: Record<string, string> = {};
    let token: unknown = null;
    let first = true;
    for await (const line of createInterface({
      crlfDelay: Infinity,
      input: createReadStream(file, { encoding: 'utf8' }),
    })) {
      if (line.length === 0) continue;
      if (first) {
        first = false;
        token = (JSON.parse(line) as { token?: unknown }).token ?? null;
        continue;
      }
      const [sliceId, hash] = JSON.parse(line) as [string, string];
      manifest[sliceId] = hash;
    }
    /* v8 ignore next -- @preserve header-only file cannot happen: save writes it last */
    if (first) return undefined;
    return { manifest, token };
  }

  /**
   * Atomically writes a collection's checkpoint (tmp file + rename).
   * @param collection - The collection.
   * @param manifest - The current content-hash manifest.
   * @param token - The latest change-stream resume token (or `null`).
   */
  async save(
    collection: string,
    manifest: Map<string, string>,
    token: unknown,
  ): Promise<void> {
    await mkdir(this._dir, { recursive: true });
    const file = this._file(collection);
    const tmp = `${file}.tmp`;
    const out = createWriteStream(tmp, { encoding: 'utf8' });
    const write = (chunk: string): Promise<void> =>
      out.write(chunk)
        ? Promise.resolve()
        : new Promise<void>((resolve) => out.once('drain', () => resolve()));

    // Header first, then one line per entry. The old writer built the whole
    // thing as a single object and a single string, which is why it refused a
    // manifest above half a million entries — and why a collection larger than
    // that was never checkpointed at all, and re-scanned in full on every
    // restart. Streaming it costs a constant amount of memory, so size is no
    // longer a reason to skip.
    await write(`${JSON.stringify({ token: token ?? null })}\n`);
    for (const [sliceId, hash] of manifest) {
      await write(`${JSON.stringify([sliceId, hash])}\n`);
    }
    await new Promise<void>((resolve, reject) => {
      out.once('error', reject);
      out.end(resolve);
    });
    await rename(tmp, file);
  }
}
