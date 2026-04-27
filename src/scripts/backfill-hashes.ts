// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { MongoClient } from 'mongodb';

import { computeIntegrityHash } from '../hashing/integrity-hash.ts';

/**
 * Collections that never enter the state hash and therefore don't need
 * `__h` populated. Mirrors the IGNORED set used by the hash script.
 */
const IGNORED = new Set([
  'sync_ops',
  'sync_state',
  'sync_local',
  'sync_resume',
  'sync_conflicts',
  'sync_audit',
  'state_checkpoints',
  'state_merkle',
  'state_dirty',
  'sync_head',
  'sync_test',
  'replication_test',
  'lock_history',
  'locking',
  'db_locks',
  'offline_changes',
]);

interface BackfillResult {
  coll: string;
  done: number;
  durationMs: number;
  rate: number;
}

async function backfillOne(
  client: MongoClient,
  dbName: string,
  collName: string,
): Promise<BackfillResult> {
  const coll = client.db(dbName).collection(collName);
  const missing = await coll.countDocuments({ __h: { $exists: false } });
  if (missing === 0) {
    console.log(`[${collName}] all docs already have __h, skipping`);
    return { coll: collName, done: 0, durationMs: 0, rate: 0 };
  }
  console.log(`[${collName}] backfilling __h on ${missing} docs...`);

  const cursor = coll.find(
    { __h: { $exists: false } },
    { sort: { _id: 1 }, batchSize: 2000 },
  );

  const BULK = 1000;
  let bulk: Array<{
    updateOne: {
      filter: Record<string, unknown>;
      update: Record<string, unknown>;
    };
  }> = [];
  let done = 0;
  const t0 = Date.now();
  let lastReport = t0;

  for await (const doc of cursor) {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { __h, ...rest } = doc as Record<string, unknown>;
    const h = computeIntegrityHash(rest);
    bulk.push({
      updateOne: {
        filter: { _id: doc._id, __h: { $exists: false } },
        update: { $set: { __h: h } },
      },
    });
    if (bulk.length >= BULK) {
      await coll.bulkWrite(bulk, { ordered: false });
      done += bulk.length;
      bulk = [];
      const now = Date.now();
      if (now - lastReport >= 5000) {
        const dt = now - t0;
        console.log(
          `[${collName}] progress ${done}/${missing} (${Math.round(done / (dt / 1000 || 1))} docs/s)`,
        );
        lastReport = now;
      }
    }
  }
  if (bulk.length) {
    await coll.bulkWrite(bulk, { ordered: false });
    done += bulk.length;
  }
  const dt = Date.now() - t0;
  const rate = Math.round(done / Math.max(1, dt / 1000));
  console.log(
    `[${collName}] done: ${done} docs in ${dt}ms (${rate} docs/s)`,
  );
  return { coll: collName, done, durationMs: dt, rate };
}

/**
 * Backfills `__h` (per-doc integrity hash) on documents that don't have one
 * yet so subsequent state-hash recomputes can use the `{_id, __h}`
 * projection fast path.
 *
 * Targets, in precedence order:
 *   1. `COLL=foo,bar` env var → exactly those collections (comma-separated).
 *   2. Otherwise, every collection in the database that isn't `system.*`
 *      and isn't on the IGNORED set above.
 *
 * Required env: MONGO_URI, DB_NAME.
 *
 * Idempotent — re-running is a no-op once all docs have `__h`. Safe to run
 * with the agent up; writes use `__h: {$exists: false}` filters so
 * concurrent change-stream-driven inserts aren't double-hashed.
 */
async function main(): Promise<void> {
  const uri = process.env.MONGO_URI;
  if (!uri) throw new Error('MONGO_URI required');
  const dbName = process.env.DB_NAME;
  if (!dbName) throw new Error('DB_NAME required');

  const requested = (process.env.COLL || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);

  const client = new MongoClient(uri);
  await client.connect();
  try {
    const db = client.db(dbName);
    let targets: string[];
    if (requested.length > 0) {
      targets = requested;
    } else {
      const all = await db.listCollections({}, { nameOnly: true }).toArray();
      targets = all
        .map((x: { name: string }) => x.name)
        .filter(
          (n: string) => !n.startsWith('system.') && !IGNORED.has(n),
        )
        .sort();
    }
    console.log(
      `Target collections (${targets.length}): ${targets.join(', ')}`,
    );

    const results: BackfillResult[] = [];
    for (const collName of targets) {
      results.push(await backfillOne(client, dbName, collName));
    }

    const totalDocs = results.reduce((a, b) => a + b.done, 0);
    const totalMs = results.reduce((a, b) => a + b.durationMs, 0);
    console.log('\nSummary:');
    for (const r of results) {
      console.log(
        `  ${r.coll}: ${r.done} docs in ${r.durationMs}ms (${r.rate} docs/s)`,
      );
    }
    console.log(
      `Total: ${totalDocs} docs across ${results.length} collections in ${totalMs}ms`,
    );
  } finally {
    await client.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
