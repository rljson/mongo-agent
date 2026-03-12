#!/usr/bin/env tsx

/**
 * Test dirty partition tracking
 *
 * This script:
 * 1. Makes changes to documents in MongoDB A
 *  2. Shows which partitions get marked as dirty
 * 3. Demonstrates incremental state hash optimization
 */

import { MongoClient, ObjectId } from 'mongodb';

import { listDirtyForCollection, markDirtyById, MerklePartition } from '../../dist/index.js';


const MONGO_A_URI =
  process.env.MONGO_A_URI ||
  'mongodb://localhost:27017/syncdb?directConnection=true';

interface MerkleDoc extends MerklePartition {
  coll: string;
}

async function main(): Promise<void> {
  console.log('');
  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('  Dirty Partition Tracking Test');
  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('');

  const client = await MongoClient.connect(MONGO_A_URI);
  const db = client.db();

  // ================================================================
  // STEP 1: Check current dirty state
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 1: Current dirty partition state');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  const beforeDirty = await listDirtyForCollection(db, 'articles');
  console.log('Before changes:');
  if (beforeDirty.full) {
    console.log('  ⚠ FULL rescan required');
  } else if (beforeDirty.partitions.length === 0) {
    console.log('  ✓ Clean state (no dirty partitions)');
  } else {
    console.log(
      `  ${beforeDirty.partitions.length} dirty partitions: ${beforeDirty.partitions.join(', ')}`,
    );
  }
  console.log('');

  // ================================================================
  // STEP 2: Get some sample documents from different partitions
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 2: Finding documents in different partitions');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  // Get merkle partitions to find documents in different ranges
  const merkle = await db
    .collection<MerkleDoc>('state_merkle')
    .find({ coll: 'articles' })
    .sort({ idx: 1 })
    .limit(5)
    .toArray();

  if (merkle.length === 0) {
    console.log(
      '⚠ No merkle partitions found. Run benchmark-state-hash.js first.',
    );
    console.log('');
    await client.close();
    return;
  }

  console.log(`Found ${merkle.length} partitions to test:`);
  const testDocs: Array<{
    doc: { _id: ObjectId; [key: string]: unknown };
    partition: number;
  }> = [];

  for (const part of merkle) {
    // Find one document in this partition
    const doc = await db.collection('articles').findOne({
      _id: { $gte: part.minId, $lte: part.maxId },
    });

    if (doc) {
      testDocs.push({ doc, partition: part.idx });
      console.log(
        `  Partition ${part.idx}: Document ${doc._id} (range: ${part.minId} → ${part.maxId})`,
      );
    }
  }
  console.log('');

  // ================================================================
  // STEP 3: Make changes and track dirty partitions
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 3: Making changes and tracking dirty partitions');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  for (const { doc, partition } of testDocs) {
    console.log(`⏳ Updating document ${doc._id} (partition ${partition})...`);

    // Update the document
    await db
      .collection('articles')
      .updateOne(
        { _id: doc._id },
        { $set: { testField: `dirty-test-${Date.now()}` } },
      );

    // Mark it as dirty
    await markDirtyById(db, 'articles', doc._id, {
      reason: 'manual_test_update',
    });

    console.log(`  ✓ Updated and marked partition ${partition} as dirty`);
  }
  console.log('');

  // ================================================================
  // STEP 4: Check dirty state after changes
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 4: Dirty partition state after changes');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  const afterDirty = await listDirtyForCollection(db, 'articles');
  console.log('After changes:');
  if (afterDirty.full) {
    console.log('  ⚠ FULL rescan required');
  } else if (afterDirty.partitions.length === 0) {
    console.log('  ✓ Clean state (no dirty partitions)');
  } else {
    console.log(
      `  ${afterDirty.partitions.length} dirty partitions: ${afterDirty.partitions.join(', ')}`,
    );
  }
  console.log('');

  // Show details
  const dirtyDocs = await db
    .collection('state_dirty')
    .find({ coll: 'articles' })
    .sort({ partition: 1 })
    .toArray();

  console.log('Detailed dirty tracking:');
  for (const d of dirtyDocs) {
    if (d.full) {
      console.log(
        `  • FULL scan (reason: ${d.reason || 'unknown'}, marked at: ${d.dirtyAt})`,
      );
    } else {
      console.log(`  • Partition ${d.partition} (marked at: ${d.dirtyAt})`);
    }
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

  const newDirty = afterDirty.partitions.filter(
    (p) => !beforeDirty.partitions.includes(p),
  );

  console.log('✓ Test completed successfully!');
  console.log('');
  console.log(`  Modified documents: ${testDocs.length}`);
  console.log(
    `  New dirty partitions: ${newDirty.length} (${newDirty.join(', ')})`,
  );
  console.log(`  Total dirty partitions: ${afterDirty.partitions.length}`);
  console.log('');

  const totalPartitions = await db
    .collection('state_merkle')
    .countDocuments({ coll: 'articles' });
  const cleanPartitions = totalPartitions - afterDirty.partitions.length;

  console.log('Incremental update benefits:');
  console.log(
    `  • Only ${afterDirty.partitions.length}/${totalPartitions} partitions need recalculation`,
  );
  console.log(
    `  • ${cleanPartitions} partitions (${((cleanPartitions / totalPartitions) * 100).toFixed(1)}%) can be skipped`,
  );
  console.log('');

  console.log('Next steps:');
  console.log('  • Incremental state hash would only process dirty partitions');
  console.log(
    '  • After recalculation, clear dirty flags for processed partitions',
  );
  console.log('  • Significant performance improvement for small changes');
  console.log('');
  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('');

  await client.close();
}

main().catch((err) => {
  console.error('Error:', err);
  process.exit(1);
});
