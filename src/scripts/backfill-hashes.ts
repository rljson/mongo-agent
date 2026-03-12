// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { close, connect, getDb } from '../db.ts';
import { computeIntegrityHash } from '../hashing/integrity-hash.ts';

/**
 * Backfills integrity hashes for documents missing the __h field
 */
async function main(): Promise<void> {
  const uri = process.env.MONGO_URI;
  if (!uri) throw new Error('MONGO_URI missing');

  await connect(uri);
  const db = getDb();
  const coll = db.collection('articles');

  const cursor = coll.find(
    { __h: { $exists: false } },
    { sort: { _id: 1 }, batchSize: 2000 }
  );

  let n = 0;
  const bulk = [] as Array<{
    updateOne: {
      filter: Record<string, unknown>;
      update: Record<string, unknown>;
    };
  }>;
  const BULK_SIZE = 1000;

  for await (const doc of cursor) {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { __h, ...rest } = doc;
    const h = computeIntegrityHash(rest as Record<string, unknown>);

    bulk.push({
      updateOne: {
        filter: { _id: doc._id, __h: { $exists: false } },
        update: { $set: { __h: h } },
      },
    });

    if (bulk.length >= BULK_SIZE) {
      await coll.bulkWrite(bulk, { ordered: false });
      n += bulk.length;
      bulk.length = 0;
      if (n % 100000 === 0) console.log('backfilled', n);
    }
  }

  if (bulk.length) {
    await coll.bulkWrite(bulk, { ordered: false });
    n += bulk.length;
  }

  console.log('done backfilled', n);
  await close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
