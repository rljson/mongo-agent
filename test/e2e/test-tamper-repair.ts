#!/usr/bin/env tsx

/**
 * Tamper Repair Script
 *
 * Detects differences between two databases using state hashes and merkle partitions,
 * then repairs the tampered database by copying correct documents from the clean database.
 */

import { MongoClient, ObjectId } from 'mongodb';

import { computeIntegrityHash, computeStateCheckpoint, MerklePartition } from '../../dist/index.js';


const MONGO_A_URI =
  process.env.MONGO_A_URI ||
  'mongodb://localhost:27017/syncdb?directConnection=true';
const MONGO_B_URI =
  process.env.MONGO_B_URI ||
  'mongodb://localhost:27018/syncdb?directConnection=true';

// Allow specifying which DB to repair
const REPAIR_DB = process.env.REPAIR_DB || 'B'; // 'A' or 'B'

interface MerkleDoc extends MerklePartition {
  coll: string;
}

interface DiffPartition {
  idx: number;
  minId: ObjectId;
  maxId: ObjectId;
  count: number;
}

async function main(): Promise<void> {
  console.log('');
  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('  Database Tamper Repair Tool');
  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('');

  const clientA = await MongoClient.connect(MONGO_A_URI);
  const clientB = await MongoClient.connect(MONGO_B_URI);
  const dbA = clientA.db();
  const dbB = clientB.db();

  console.log('✓ Connected to both MongoDB instances');
  console.log('');

  // Determine which DB to repair
  const sourceDB = REPAIR_DB === 'B' ? dbA : dbB;
  const targetDB = REPAIR_DB === 'B' ? dbB : dbA;
  const sourceName =
    REPAIR_DB === 'B' ? 'MongoDB A (source)' : 'MongoDB B (source)';
  const targetName =
    REPAIR_DB === 'B' ? 'MongoDB B (target)' : 'MongoDB A (target)';

  console.log(`Repair configuration:`);
  console.log(`  Source (clean):   ${sourceName}`);
  console.log(`  Target (repair):  ${targetName}`);
  console.log('');

  // ================================================================
  // STEP 1: Compute state hashes
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 1: Computing state hashes');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  console.log(`⏳ Computing state hash for ${sourceName}...`);
  const startSource = Date.now();
  const checkpointSource = await computeStateCheckpoint({
    db: sourceDB,
    ignoredColls: new Set([
      'sync_ops',
      'sync_state',
      'sync_local',
      'sync_resume',
      'state_checkpoints',
      'state_merkle',
      'state_dirty',
    ]),
    partitionSize: 50000,
  });
  const durationSource = ((Date.now() - startSource) / 1000).toFixed(2);
  console.log(`✓ Hash: ${checkpointSource.dbRoot} (${durationSource}s)`);
  console.log('');

  console.log(`⏳ Computing state hash for ${targetName}...`);
  const startTarget = Date.now();
  const checkpointTarget = await computeStateCheckpoint({
    db: targetDB,
    ignoredColls: new Set([
      'sync_ops',
      'sync_state',
      'sync_local',
      'sync_resume',
      'state_checkpoints',
      'state_merkle',
      'state_dirty',
    ]),
    partitionSize: 50000,
  });
  const durationTarget = ((Date.now() - startTarget) / 1000).toFixed(2);
  console.log(`✓ Hash: ${checkpointTarget.dbRoot} (${durationTarget}s)`);
  console.log('');

  if (checkpointSource.dbRoot === checkpointTarget.dbRoot) {
    console.log('✓ State hashes MATCH - no repair needed!');
    console.log('');
    await clientA.close();
    await clientB.close();
    return;
  }

  console.log('⚠ State hashes DIFFER - repair needed');
  console.log('');

  // ================================================================
  // STEP 2: Identify differing partitions
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 2: Identifying differing partitions using merkle tree');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  const merkleSource = await sourceDB
    .collection<MerkleDoc>('state_merkle')
    .find({ coll: 'articles' })
    .sort({ idx: 1 })
    .toArray();

  const merkleTarget = await targetDB
    .collection<MerkleDoc>('state_merkle')
    .find({ coll: 'articles' })
    .sort({ idx: 1 })
    .toArray();

  const diffPartitions: DiffPartition[] = [];

  for (let i = 0; i < merkleSource.length; i++) {
    const partSource = merkleSource[i];
    const partTarget = merkleTarget[i];

    if (partSource && partTarget && partSource.root !== partTarget.root) {
      diffPartitions.push({
        idx: i,
        minId: partSource.minId,
        maxId: partSource.maxId,
        count: partSource.count,
      });

      console.log(
        `  ⚠ Partition ${i}: DIFFERS (${partSource.count.toLocaleString()} docs)`,
      );
    }
  }

  console.log('');
  console.log(
    `✓ Found ${diffPartitions.length} differing partitions out of ${merkleSource.length} total`,
  );
  console.log(
    `  Efficiency: Only need to scan ${((diffPartitions.length / merkleSource.length) * 100).toFixed(1)}% of database`,
  );
  console.log('');

  if (diffPartitions.length === 0) {
    console.log(
      '✓ No differing partitions found (hashes differ but partitions match?)',
    );
    console.log('  This might indicate a collection-level difference.');
    await clientA.close();
    await clientB.close();
    return;
  }

  // ================================================================
  // STEP 3: Find and repair specific documents
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 3: Finding and repairing tampered documents');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  let totalScanned = 0;
  let totalRepaired = 0;
  const repairedDocs: Array<{ _id: ObjectId; partition: number }> = [];

  for (const partition of diffPartitions) {
    console.log(
      `⏳ Processing partition ${partition.idx} (${partition.count.toLocaleString()} docs)...`,
    );

    // Get all docs from this partition in both databases
    const docsSource = await sourceDB
      .collection('articles')
      .find({ _id: { $gte: partition.minId, $lte: partition.maxId } })
      .sort({ _id: 1 })
      .toArray();

    const docsTarget = await targetDB
      .collection('articles')
      .find({ _id: { $gte: partition.minId, $lte: partition.maxId } })
      .sort({ _id: 1 })
      .toArray();

    // Create hash maps
    const hashMapSource = new Map<string, string>();
    const docMapSource = new Map<string, Record<string, unknown>>();

    for (const doc of docsSource) {
      const hash = doc.__h || computeIntegrityHash(doc);
      const idStr = doc._id.toString();
      hashMapSource.set(idStr, hash);
      docMapSource.set(idStr, doc);
    }

    const hashMapTarget = new Map<string, string>();
    for (const doc of docsTarget) {
      const hash = doc.__h || computeIntegrityHash(doc);
      hashMapTarget.set(doc._id.toString(), hash);
    }

    // Find differences and prepare repairs
    const bulkOps: Array<{
      replaceOne: {
        filter: { _id: ObjectId };
        replacement: Record<string, unknown>;
        upsert: boolean;
      };
    }> = [];

    for (const doc of docsSource) {
      const idStr = doc._id.toString();
      const hashSource = hashMapSource.get(idStr);
      const hashTarget = hashMapTarget.get(idStr);

      totalScanned++;

      if (hashSource !== hashTarget) {
        // Document differs or doesn't exist in target - replace it
        const correctDoc = docMapSource.get(idStr);

        if (correctDoc) {
          bulkOps.push({
            replaceOne: {
              filter: { _id: doc._id as ObjectId },
              replacement: correctDoc,
              upsert: true,
            },
          });

          repairedDocs.push({
            _id: doc._id as ObjectId,
            partition: partition.idx,
          });

          totalRepaired++;
        }
      }
    }

    // Execute repairs
    if (bulkOps.length > 0) {
      await targetDB
        .collection('articles')
        .bulkWrite(bulkOps, { ordered: false });
      console.log(
        `  ✓ Repaired ${bulkOps.length} documents in partition ${partition.idx}`,
      );
    } else {
      console.log(`  ✓ No repairs needed in partition ${partition.idx}`);
    }
  }

  console.log('');
  console.log(`✓ Scanned ${totalScanned.toLocaleString()} documents`);
  console.log(`✓ Repaired ${totalRepaired.toLocaleString()} documents`);
  console.log('');

  // ================================================================
  // STEP 4: Verify repair
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 4: Verifying repair');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  console.log(`⏳ Recomputing state hash for ${targetName}...`);
  const startVerify = Date.now();
  const checkpointVerify = await computeStateCheckpoint({
    db: targetDB,
    ignoredColls: new Set([
      'sync_ops',
      'sync_state',
      'sync_local',
      'sync_resume',
      'state_checkpoints',
      'state_merkle',
      'state_dirty',
    ]),
    partitionSize: 50000,
  });
  const durationVerify = ((Date.now() - startVerify) / 1000).toFixed(2);
  console.log(`✓ Hash: ${checkpointVerify.dbRoot} (${durationVerify}s)`);
  console.log('');

  console.log('Final verification:');
  console.log(`  ${sourceName}: ${checkpointSource.dbRoot}`);
  console.log(`  ${targetName}: ${checkpointVerify.dbRoot}`);
  console.log('');

  if (checkpointSource.dbRoot === checkpointVerify.dbRoot) {
    console.log('✨ SUCCESS! State hashes now MATCH - repair complete!');
  } else {
    console.log(
      '⚠ State hashes still differ - additional investigation needed',
    );
  }
  console.log('');

  // ================================================================
  // SUMMARY
  // ================================================================
  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('Repair Summary');
  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('');

  console.log('✓ Repair Statistics:');
  console.log(
    `  - Partitions scanned: ${diffPartitions.length} out of ${merkleSource.length} (${((diffPartitions.length / merkleSource.length) * 100).toFixed(1)}%)`,
  );
  console.log(`  - Documents scanned: ${totalScanned.toLocaleString()}`);
  console.log(`  - Documents repaired: ${totalRepaired.toLocaleString()}`);
  console.log('');

  const totalDocs = merkleSource.reduce((sum, p) => sum + p.count, 0);
  const efficiency = (((totalDocs - totalScanned) / totalDocs) * 100).toFixed(
    1,
  );

  console.log('✓ Merkle Tree Efficiency:');
  console.log(
    `  - Total database size: ${totalDocs.toLocaleString()} documents`,
  );
  console.log(`  - Scanned only: ${totalScanned.toLocaleString()} documents`);
  console.log(`  - Efficiency gain: ${efficiency}% of documents skipped`);
  console.log('');

  if (checkpointSource.dbRoot === checkpointVerify.dbRoot) {
    console.log('✨ Databases are now synchronized!');
  }
  console.log('');

  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('');

  await clientA.close();
  await clientB.close();
}

main().catch((err) => {
  console.error('Error:', err);
  process.exit(1);
});
