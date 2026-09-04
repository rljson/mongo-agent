// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

// .............................................................................

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
    let raw: string;
    try {
      raw = await readFile(this._file(collection), 'utf8');
    } catch {
      return undefined;
    }
    try {
      const parsed = JSON.parse(raw) as Partial<EditCheckpointState>;
      return { manifest: parsed.manifest ?? {}, token: parsed.token ?? null };
    } catch {
      return undefined;
    }
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
    const payload: EditCheckpointState = {
      manifest: Object.fromEntries(manifest),
      token: token ?? null,
    };
    const file = this._file(collection);
    const tmp = `${file}.tmp`;
    await writeFile(tmp, JSON.stringify(payload), 'utf8');
    await rename(tmp, file);
  }
}
