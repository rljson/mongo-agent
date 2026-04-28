// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { EJSON } from 'bson';
import { MongoClient } from 'mongodb';

/**
 * Repairs collections by pulling missing docs from a healthy peer.
 *
 * `restore-from-chain` can only recover docs whose history is in
 * `sync_ops`. Anything that landed via `mongorestore` (or any other path
 * that bypassed the change-stream) won't have an op to replay. This
 * script closes that gap: it diffs the local _id set against a peer's
 * over the agent's `/diff/:coll/ids` endpoint and copies any missing
 * docs back via `/diff/:coll/doc`.
 *
 * Routing: requests go to the local hub (`HUB_URL`) which forwards to
 * the peer agent's `/diff/...` endpoints. No direct LAN access to the
 * peer's mongo or WinRM is needed.
 *
 * Defaults to "fill missing only" — drift detection (doc exists locally
 * but with different content than peer) is out of scope here; use
 * `restore-from-chain` for that on chain-tracked docs, or extend this
 * script with a hash-tree compare if you need it for baseline data.
 *
 * Required env: MONGO_URI, DB_NAME, HUB_URL (e.g. http://localhost:3200),
 * PEER_NODE_ID (e.g. laptop1).
 * Optional env: COLL=foo,bar to limit scope; otherwise scans every
 * non-IGNORED collection.
 */

const URI = process.env.MONGO_URI;
if (!URI) throw new Error('MONGO_URI required');
const DB_NAME = process.env.DB_NAME;
if (!DB_NAME) throw new Error('DB_NAME required');
const HUB_URL = process.env.HUB_URL;
if (!HUB_URL) throw new Error('HUB_URL required');
const PEER_NODE_ID = process.env.PEER_NODE_ID;
if (!PEER_NODE_ID) throw new Error('PEER_NODE_ID required');

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

async function relayPost<T>(path: string, body: unknown): Promise<T> {
  const url = `${HUB_URL}/hub/relay/${PEER_NODE_ID}${path}`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`peer ${path} failed ${resp.status}: ${text}`);
  }
  return (await resp.json()) as T;
}

async function repairCollection(
  client: MongoClient,
  collName: string,
): Promise<{ coll: string; missing: number; restored: number }> {
  const coll = client.db(DB_NAME).collection(collName);
  const localIds = new Set(
    (
      await coll
        .find({}, { projection: { _id: 1 } })
        .map((d) => d._id as unknown)
        .toArray()
    ).map((id) => keyOf(id)),
  );

  const peerResp = await relayPost<{ ids: unknown[] }>(`/diff/${collName}/ids`, {});
  const peerIds = peerResp.ids.map((raw) => EJSON.deserialize(raw as never));

  const missing = peerIds.filter((id) => !localIds.has(keyOf(id)));
  console.log(
    `[${collName}] local=${localIds.size}, peer=${peerIds.length}, missing=${missing.length}`,
  );

  let restored = 0;
  for (const id of missing) {
    const docResp = await relayPost<{ doc: unknown | null }>(
      `/diff/${collName}/doc`,
      { id: EJSON.serialize(id as never) },
    );
    if (!docResp.doc) {
      console.warn(`  [${collName}] peer no longer has _id=${keyOf(id)} — skipping`);
      continue;
    }
    const doc = EJSON.deserialize(docResp.doc as never) as Record<string, unknown>;
    await coll.replaceOne({ _id: id } as Record<string, unknown>, doc, {
      upsert: true,
    });
    restored += 1;
    if (restored % 100 === 0) console.log(`  [${collName}] restored ${restored}/${missing.length}`);
  }
  console.log(
    `[${collName}] done: ${restored} restored, ${missing.length - restored} skipped`,
  );
  return { coll: collName, missing: missing.length, restored };
}

/**
 * Stable, comparable string key for any BSON _id (ObjectId, string,
 * number, etc.) so we can diff sets locally vs peer.
 */
function keyOf(id: unknown): string {
  if (id === null || id === undefined) return 'null';
  if (typeof id === 'string') return `s:${id}`;
  if (typeof id === 'number') return `n:${id}`;
  if (typeof (id as { toHexString?: () => string }).toHexString === 'function') {
    return `o:${(id as { toHexString: () => string }).toHexString()}`;
  }
  return `j:${JSON.stringify(id)}`;
}

async function main(): Promise<void> {
  const requested = (process.env.COLL || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);

  const client = new MongoClient(URI as string);
  await client.connect();
  try {
    const db = client.db(DB_NAME as string);
    let targets: string[];
    if (requested.length > 0) {
      targets = requested;
    } else {
      const all = await db.listCollections({}, { nameOnly: true }).toArray();
      targets = all
        .map((x: { name: string }) => x.name)
        .filter((n: string) => !n.startsWith('system.') && !IGNORED.has(n))
        .sort();
    }
    console.log(
      `restore-from-peer ${PEER_NODE_ID} via ${HUB_URL} → targets (${targets.length}): ${targets.join(', ')}`,
    );

    const summary = [];
    for (const collName of targets) {
      summary.push(await repairCollection(client, collName));
    }
    const totalRestored = summary.reduce((a, b) => a + b.restored, 0);
    const totalMissing = summary.reduce((a, b) => a + b.missing, 0);
    console.log(
      `\nTotal: ${totalRestored} restored / ${totalMissing} missing across ${summary.length} collections`,
    );
  } finally {
    await client.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
