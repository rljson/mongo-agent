#!/usr/bin/env tsx

import { Db, MongoClient } from 'mongodb';

import { computeIntegrityHash } from '../../dist/index.js';


const MONGO_A_URI =
  process.env.MONGO_A_URI ||
  'mongodb://localhost:27017/syncdb?directConnection=true';
const MONGO_B_URI =
  process.env.MONGO_B_URI ||
  'mongodb://localhost:27018/syncdb?directConnection=true';

interface DirtyDoc {
  coll: string;
  full?: boolean;
  partition?: number;
  reason?: string;
  dirtyAt: Date;
}

async function main(): Promise<void> {
  console.log('');
  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('  Integrity Hash Performance Test');
  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('');

  // Connect to both databases
  console.log('ℹ Connecting to MongoDB instances...');
  const clientA = await MongoClient.connect(MONGO_A_URI);
  const clientB = await MongoClient.connect(MONGO_B_URI);
  const dbA = clientA.db();
  const dbB = clientB.db();
  console.log('✓ Connected to both MongoDB instances');
  console.log('');

  // ================================================================
  // STEP 1: Check current state of __h fields
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 1: Checking integrity hash status');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  const totalDocsA = await dbA.collection('articles').countDocuments({});
  const withHashA = await dbA
    .collection('articles')
    .countDocuments({ __h: { $exists: true } });
  const withoutHashA = totalDocsA - withHashA;

  const totalDocsB = await dbB.collection('articles').countDocuments({});
  const withHashB = await dbB
    .collection('articles')
    .countDocuments({ __h: { $exists: true } });
  const withoutHashB = totalDocsB - withHashB;

  console.log('MongoDB A:');
  console.log(`  Total documents: ${totalDocsA.toLocaleString()}`);
  console.log(
    `  With __h field:  ${withHashA.toLocaleString()} (${((withHashA / totalDocsA) * 100).toFixed(1)}%)`,
  );
  console.log(
    `  Without __h:     ${withoutHashA.toLocaleString()} (${((withoutHashA / totalDocsA) * 100).toFixed(1)}%)`,
  );
  console.log('');

  console.log('MongoDB B:');
  console.log(`  Total documents: ${totalDocsB.toLocaleString()}`);
  console.log(
    `  With __h field:  ${withHashB.toLocaleString()} (${((withHashB / totalDocsB) * 100).toFixed(1)}%)`,
  );
  console.log(
    `  Without __h:     ${withoutHashB.toLocaleString()} (${((withoutHashB / totalDocsB) * 100).toFixed(1)}%)`,
  );
  console.log('');

  // ================================================================
  // STEP 2: Backfill integrity hashes if needed
  // ================================================================
  if (withoutHashA > 0 || withoutHashB > 0) {
    console.log(
      '──────────────────────────────────────────────────────────────────────',
    );
    console.log('STEP 2: Backfilling integrity hashes');
    console.log(
      '──────────────────────────────────────────────────────────────────────',
    );
    console.log('');

    if (withoutHashA > 0) {
      console.log(
        `⏳ Backfilling ${withoutHashA.toLocaleString()} documents on MongoDB A...`,
      );
      const startA = Date.now();
      await backfillHashes(dbA, 'articles');
      const durationA = ((Date.now() - startA) / 1000).toFixed(2);
      console.log(
        `✓ MongoDB A backfilled in ${durationA}s (${Math.round(withoutHashA / parseFloat(durationA)).toLocaleString()} docs/sec)`,
      );
      console.log('');
    }

    if (withoutHashB > 0) {
      console.log(
        `⏳ Backfilling ${withoutHashB.toLocaleString()} documents on MongoDB B...`,
      );
      const startB = Date.now();
      await backfillHashes(dbB, 'articles');
      const durationB = ((Date.now() - startB) / 1000).toFixed(2);
      console.log(
        `✓ MongoDB B backfilled in ${durationB}s (${Math.round(withoutHashB / parseFloat(durationB)).toLocaleString()} docs/sec)`,
      );
      console.log('');
    }
  }

  // ================================================================
  // STEP 3: Performance comparison
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 3: Performance comparison (with vs without __h)');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  // Sample 10,000 documents to test performance
  const sampleSize = Math.min(10000, totalDocsA);
  console.log(
    `ℹ Testing with sample of ${sampleSize.toLocaleString()} documents from MongoDB A`,
  );
  console.log('');

  const sampleDocs = await dbA
    .collection('articles')
    .find({})
    .limit(sampleSize)
    .toArray();

  // Test 1: Using existing __h field
  console.log('Test 1: Using existing __h field (fast path)');
  const start1 = Date.now();
  let hashCount1 = 0;
  for (const doc of sampleDocs) {
    void (doc.__h || computeIntegrityHash(doc));
    hashCount1++;
  }
  const duration1 = Date.now() - start1;
  console.log(`  Time: ${duration1}ms`);
  console.log(
    `  Throughput: ${Math.round(hashCount1 / (duration1 / 1000)).toLocaleString()} docs/sec`,
  );
  console.log(`  Avg per doc: ${(duration1 / hashCount1).toFixed(3)}ms`);
  console.log('');

  // Test 2: Recomputing hash (simulating without __h)
  console.log('Test 2: Recomputing hash from scratch (slow path)');
  const start2 = Date.now();
  let hashCount2 = 0;
  for (const doc of sampleDocs) {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { __h: _h, ...rest } = doc;
    computeIntegrityHash(rest);
    hashCount2++;
  }
  const duration2 = Date.now() - start2;
  console.log(`  Time: ${duration2}ms`);
  console.log(
    `  Throughput: ${Math.round(hashCount2 / (duration2 / 1000)).toLocaleString()} docs/sec`,
  );
  console.log(`  Avg per doc: ${(duration2 / hashCount2).toFixed(3)}ms`);
  console.log('');

  const speedup = (duration2 / duration1).toFixed(2);
  console.log(`💡 Performance improvement: ${speedup}x faster with __h field`);
  console.log('');

  // ================================================================
  // STEP 4: Check dirty partition tracking
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 4: Dirty partition tracking status');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  const dirtyA = await dbA
    .collection<DirtyDoc>('state_dirty')
    .find({})
    .toArray();
  const dirtyB = await dbB
    .collection<DirtyDoc>('state_dirty')
    .find({})
    .toArray();

  console.log('MongoDB A dirty partitions:');
  if (dirtyA.length === 0) {
    console.log('  (none tracked yet - clean state)');
  } else {
    for (const d of dirtyA) {
      if (d.full) {
        console.log(
          `  ⚠ FULL rescan needed for ${d.coll} (reason: ${d.reason})`,
        );
      } else {
        console.log(
          `  • ${d.coll} partition ${d.partition} (dirty since ${d.dirtyAt})`,
        );
      }
    }
  }
  console.log('');

  console.log('MongoDB B dirty partitions:');
  if (dirtyB.length === 0) {
    console.log('  (none tracked yet - clean state)');
  } else {
    for (const d of dirtyB) {
      if (d.full) {
        console.log(
          `  ⚠ FULL rescan needed for ${d.coll} (reason: ${d.reason})`,
        );
      } else {
        console.log(
          `  • ${d.coll} partition ${d.partition} (dirty since ${d.dirtyAt})`,
        );
      }
    }
  }
  console.log('');

  // ================================================================
  // STEP 5: Check merkle partition metadata
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 5: Merkle tree partition metadata');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  const merkleA = await dbA
    .collection('state_merkle')
    .find({ coll: 'articles' })
    .sort({ idx: 1 })
    .toArray();
  const merkleB = await dbB
    .collection('state_merkle')
    .find({ coll: 'articles' })
    .sort({ idx: 1 })
    .toArray();

  console.log(`MongoDB A: ${merkleA.length} partitions`);
  if (merkleA.length > 0) {
    console.log(
      `  First partition: ${merkleA[0].count} docs (${merkleA[0].minId} → ${merkleA[0].maxId})`,
    );
    console.log(
      `  Last partition:  ${merkleA[merkleA.length - 1].count} docs (${merkleA[merkleA.length - 1].minId} → ${merkleA[merkleA.length - 1].maxId})`,
    );
  }
  console.log('');

  console.log(`MongoDB B: ${merkleB.length} partitions`);
  if (merkleB.length > 0) {
    console.log(
      `  First partition: ${merkleB[0].count} docs (${merkleB[0].minId} → ${merkleB[0].maxId})`,
    );
    console.log(
      `  Last partition:  ${merkleB[merkleB.length - 1].count} docs (${merkleB[merkleB.length - 1].minId} → ${merkleB[merkleB.length - 1].maxId})`,
    );
  }
  console.log('');

  // ================================================================
  // Summary
  // ================================================================
  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('Summary');
  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('');
  console.log('✓ Integrity hashes (__h):');
  console.log(
    `  - MongoDB A: ${withHashA === totalDocsA ? '100%' : withHashA.toLocaleString() + '/' + totalDocsA.toLocaleString()} documents`,
  );
  console.log(
    `  - MongoDB B: ${withHashB === totalDocsB ? '100%' : withHashB.toLocaleString() + '/' + totalDocsB.toLocaleString()} documents`,
  );
  console.log('');
  console.log('✓ Performance benefit:');
  console.log(`  - ${speedup}x faster hash computation with __h field`);
  console.log(
    `  - Estimated full scan savings: ${((totalDocsA * (duration2 - duration1)) / sampleSize / 1000).toFixed(1)}s`,
  );
  console.log('');
  console.log('✓ Merkle tree partitions:');
  console.log(
    `  - MongoDB A: ${merkleA.length} partitions ready for incremental updates`,
  );
  console.log(
    `  - MongoDB B: ${merkleB.length} partitions ready for incremental updates`,
  );
  console.log('');

  if (dirtyA.length > 0 || dirtyB.length > 0) {
    console.log('⚠ Dirty partitions tracked:');
    console.log(`  - MongoDB A: ${dirtyA.length} dirty partitions`);
    console.log(`  - MongoDB B: ${dirtyB.length} dirty partitions`);
    console.log(
      '  (These partitions will be recalculated on next incremental checkpoint)',
    );
    console.log('');
  }

  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('');

  await clientA.close();
  await clientB.close();
}

/**
 * Backfill __h integrity hashes for documents that don't have it
 */
async function backfillHashes(db: Db, collName: string): Promise<number> {
  const coll = db.collection(collName);
  const cursor = coll.find(
    { __h: { $exists: false } },
    { sort: { _id: 1 }, batchSize: 2000 },
  );

  let n = 0;
  const bulk: Array<{
    updateOne: {
      filter: { _id: unknown; __h: { $exists: boolean } };
      update: { $set: { __h: string } };
    };
  }> = [];
  const BULK_SIZE = 1000;

  for await (const doc of cursor) {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { __h: _h, ...rest } = doc;
    const h = computeIntegrityHash(rest);

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
      if (n % 50000 === 0) {
        process.stdout.write(`  Backfilled ${n.toLocaleString()}...\r`);
      }
    }
  }

  if (bulk.length) {
    await coll.bulkWrite(bulk, { ordered: false });
    n += bulk.length;
  }

  return n;
}

main().catch((err) => {
  console.error('Error:', err);
  process.exit(1);
});
