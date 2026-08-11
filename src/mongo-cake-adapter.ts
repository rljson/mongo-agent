// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { Route } from '@rljson/rljson';

import type { Db } from '@rljson/db';
import type { TableCfg } from '@rljson/rljson';

import { componentsTableFor } from './mongo-cake-model.ts';
import type { BuiltCake, CakeRow } from './mongo-cake-model.ts';

// .............................................................................
// Persistence for the Cake/Layer/Component model. Cakes, layers and components
// are stored as uniform content-addressed rows (`{ _hash, doc }`) in `@rljson`
// Db tables and pulled back by hash via `readRowsByHashes` — the migration
// doc §6 flow. No trees, no Bs blobs, no Layer/Cake controllers: a receiver
// pulls the cake row, then only the changed layer rows, then only the missing
// component rows.
// .............................................................................

/** Table key holding all Cake rows. */
export const CAKE_TABLE = 'mongoCakes';
/** Table key holding all Layer rows. */
export const LAYER_TABLE = 'mongoLayers';
/**
 * Components table key for a collection.
 * @param collection - The collection name.
 * @returns The collection's components table key.
 */
export const componentsTableOf = (collection: string): string =>
  componentsTableFor(collection);

/**
 * Builds the `{ _hash, doc }` table configuration shared by cakes, layers and
 * component tables. Two columns are the minimum a components table allows; the
 * schemaless document/structure lives in the single `doc` string column.
 * @param key - The table key.
 * @returns A hashed `components` TableCfg with `_hash` + `doc` columns.
 */
const rowTableCfg = (key: string): TableCfg =>
  hip<TableCfg>({
    key,
    type: 'components',
    columns: [
      { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'Hash' },
      { key: 'doc', type: 'string', titleLong: 'Doc', titleShort: 'Doc' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
    _hash: '',
  } as unknown as TableCfg);

// .............................................................................

/**
 * Stores and fetches Cake/Layer/Component rows through an `@rljson` Db. On the
 * producer, {@link storeCake} persists a whole snapshot and returns the cake
 * hash to broadcast. On a receiver, {@link fetchCake} / {@link fetchLayers} /
 * {@link fetchComponents} pull by hash — cascading to the cloud peer when the
 * Db is backed by a sync Client — so only changed layers and missing components
 * travel.
 */
export class MongoCakeAdapter {
  private readonly _created = new Set<string>();

  constructor(private readonly _db: Db) {}

  /* eslint-disable tsdoc/syntax -- jsdoc/require-param mandates a block tag for
     the nested `options.skipNotification` property, but tsdoc/syntax rejects the
     dotted parameter name; the two rules conflict, so tsdoc/syntax is disabled
     for this documented-but-dotted @param only. */
  /**
   * Persists a built Cake: every component row (per collection), every layer
   * row, then the cake row last so its InsertHistory entry is the head ref.
   * @param built - The snapshot's Cake/Layer/Component rows.
   * @param options - Insert options.
   * @param options.skipNotification - Suppress Connector observer notifications
   *   for the component and layer writes (avoids restore-loop echo); the cake
   *   write always notifies so peers learn the new head.
   * @returns The cake row's content hash — the ref to broadcast.
   */
  /* eslint-enable tsdoc/syntax */
  async storeCake(
    built: BuiltCake,
    options: { skipNotification?: boolean } = {},
  ): Promise<string> {
    for (const [componentsTable, rows] of built.components) {
      await this._insertRows(componentsTable, [...rows.values()], true);
    }
    await this._insertRows(LAYER_TABLE, [...built.layers.values()], true);
    await this._insertRows(CAKE_TABLE, [built.cake], options.skipNotification);
    return built.cakeHash;
  }

  /**
   * Fetches a Cake row by hash (cascades to the peer when not local).
   * @param cakeHash - The cake content hash received from a peer.
   * @returns The Cake row, or null when it cannot be resolved.
   */
  async fetchCake(cakeHash: string): Promise<CakeRow | null> {
    const rows = await this._readRows(CAKE_TABLE, [cakeHash]);
    return (rows.get(cakeHash) as CakeRow | undefined) ?? null;
  }

  /**
   * Fetches Layer rows by hash (cascades to the peer when not local).
   * @param layerHashes - The layer content hashes to resolve.
   * @returns Layer rows keyed by hash.
   */
  async fetchLayers(layerHashes: string[]): Promise<Map<string, CakeRow>> {
    return (await this._readRows(LAYER_TABLE, layerHashes)) as Map<string, CakeRow>;
  }

  /**
   * Fetches Component rows for a collection by hash (cascades to the peer).
   * @param componentsTable - The collection's components table key.
   * @param hashes - The component hashes to resolve.
   * @returns Component rows keyed by hash.
   */
  async fetchComponents(
    componentsTable: string,
    hashes: string[],
  ): Promise<Map<string, CakeRow>> {
    return (await this._readRows(componentsTable, hashes)) as Map<string, CakeRow>;
  }

  // ...........................................................................

  /**
   * Ensures a `{ _hash, doc }` table exists before it is written or read.
   * @param key - The table key to create on first use.
   */
  private async _ensureTable(key: string): Promise<void> {
    if (this._created.has(key)) return;
    await this._db.core.createTableWithInsertHistory(rowTableCfg(key));
    this._created.add(key);
  }

  /**
   * Inserts content-addressed rows into a `{ _hash, doc }` table.
   * @param key - The table key to insert into.
   * @param rows - The content-addressed rows to insert.
   * @param skipNotification - When true, suppress Connector observer
   *   notifications for this write.
   */
  private async _insertRows(
    key: string,
    rows: CakeRow[],
    skipNotification?: boolean,
  ): Promise<void> {
    await this._ensureTable(key);
    if (rows.length === 0) return;
    const cfg = rowTableCfg(key);
    await (this._db as unknown as {
      insert: (route: Route, tree: unknown, options?: unknown) => Promise<unknown>;
    }).insert(
      Route.fromFlat(key),
      { [key]: { _type: 'components', _tableCfg: cfg._hash, _data: rows, _hash: '' } },
      { skipNotification },
    );
  }

  /**
   * Reads rows by hash, ensuring the table exists first (cascades to peer).
   * @param key - The table key to read from.
   * @param hashes - The content hashes to resolve.
   * @returns Rows keyed by hash.
   */
  private async _readRows(
    key: string,
    hashes: string[],
  ): Promise<Map<string, unknown>> {
    await this._ensureTable(key);
    if (hashes.length === 0) return new Map();
    return this._db.core.readRowsByHashes(key, hashes);
  }
}
