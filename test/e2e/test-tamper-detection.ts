#!/usr/bin/env tsx

/**
 * Tamper Detection Test
 *
 * Simulates a scenario where one database is tampered with outside the sync system,
 * detects the tampering using state hashes and merkle partitions,
 * and identifies which documents were affected.
 */

import { MongoClient, ObjectId } from 'mongodb';

import { computeIntegrityHash, computeStateCheckpoint, MerklePartition } from '../../dist/index.js';


const MONGO_A_URI =
  process.env.MONGO_A_URI ||
  'mongodb://localhost:27017/syncdb?directConnection=true';
const MONGO_B_URI =
  process.env.MONGO_B_URI ||
  'mongodb://localhost:27018/syncdb?directConnection=true';

interface MerkleDoc extends MerklePartition {
  coll: string;
}

interface TamperedDoc {
  _id: ObjectId;
  partition: number;
  originalHash: string;
  tamperedHash: string;
  minId: ObjectId;
  maxId: ObjectId;
}

interface DiffPartition {
  idx: number;
  minId: ObjectId;
  maxId: ObjectId;
  count: number;
  hashA: string;
  hashB: string;
}

async function main(): Promise<void> {
  console.log('');
  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('  Tamper Detection & Merkle Tree Verification Test');
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

  // ================================================================
  // STEP 1: Verify initial state hashes match
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 1: Computing initial state hashes');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  console.log('⏳ Computing state hash for MongoDB A...');
  const startA1 = Date.now();
  const checkpointA1 = await computeStateCheckpoint({
    db: dbA,
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
  const durationA1 = ((Date.now() - startA1) / 1000).toFixed(2);
  console.log(`✓ MongoDB A hash: ${checkpointA1.dbRoot} (${durationA1}s)`);
  console.log('');

  console.log('⏳ Computing state hash for MongoDB B...');
  const startB1 = Date.now();
  const checkpointB1 = await computeStateCheckpoint({
    db: dbB,
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
  const durationB1 = ((Date.now() - startB1) / 1000).toFixed(2);
  console.log(`✓ MongoDB B hash: ${checkpointB1.dbRoot} (${durationB1}s)`);
  console.log('');

  if (checkpointA1.dbRoot === checkpointB1.dbRoot) {
    console.log('✓ Initial state hashes MATCH - databases are identical');
  } else {
    console.log('⚠ Initial state hashes DIFFER - databases are not identical');
    console.log(
      '  Run ./scripts/restore-databases.sh first to ensure identical starting state',
    );
    await clientA.close();
    await clientB.close();
    return;
  }
  console.log('');

  // ================================================================
  // STEP 2: Simulate tampering on MongoDB B
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 2: Simulating database tampering on MongoDB B');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');
  console.log(
    '⚠ Tampering scenario: Someone modifies documents directly on MongoDB B',
  );
  console.log('  (bypassing sync agents, change streams, and sync_ops)');
  console.log('');

  // Get some random documents from different partitions
  const merkleB = await dbB
    .collection<MerkleDoc>('state_merkle')
    .find({ coll: 'articles' })
    .sort({ idx: 1 })
    .toArray();

  if (merkleB.length === 0) {
    console.log(
      '⚠ No merkle partitions found. Run benchmark-state-hash.js first.',
    );
    await clientA.close();
    await clientB.close();
    return;
  }

  // Tamper with 5 documents across different partitions
  const tamperedDocs: TamperedDoc[] = [];
  const partitionsToTamper = [0, 3, 7];

  console.log(
    `⏳ Tampering with documents in ${partitionsToTamper.length} partitions...`,
  );
  console.log('');

  for (const partIdx of partitionsToTamper) {
    if (partIdx >= merkleB.length) continue;

    const partition = merkleB[partIdx];

    // Find a document in this partition
    const doc = await dbB.collection('articles').findOne({
      _id: { $gte: partition.minId, $lte: partition.maxId },
    });

    if (doc) {
      const originalHash = doc.__h || computeIntegrityHash(doc);

      // Tamper: Add a field that doesn't go through sync
      await dbB.collection('articles').updateOne(
        { _id: doc._id },
        {
          $set: {
            TAMPERED: true,
            tamperedAt: new Date().toISOString(),
            tamperedField: `Modified outside sync system - partition ${partIdx}`,
          },
        },
      );

      // Get the tampered version
      const tamperedDoc = await dbB
        .collection('articles')
        .findOne({ _id: doc._id });
      const tamperedHash = tamperedDoc ? computeIntegrityHash(tamperedDoc) : '';

      tamperedDocs.push({
        _id: doc._id as ObjectId,
        partition: partIdx,
        originalHash,
        tamperedHash,
        minId: partition.minId,
        maxId: partition.maxId,
      });

      console.log(`  ⚠ Partition ${partIdx}: Tampered document ${doc._id}`);
      console.log(`     Original hash: ${originalHash.substring(0, 16)}...`);
      console.log(`     Tampered hash: ${tamperedHash.substring(0, 16)}...`);
    }
  }

  console.log('');
  console.log(
    `✓ Tampered ${tamperedDocs.length} documents across ${partitionsToTamper.length} partitions`,
  );
  console.log('');

  // ================================================================
  // STEP 3: Detect tampering using state hashes
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 3: Detecting tampering using state hash comparison');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  console.log('⏳ Recomputing state hash for tampered MongoDB B...');
  const startB2 = Date.now();
  const checkpointB2 = await computeStateCheckpoint({
    db: dbB,
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
  const durationB2 = ((Date.now() - startB2) / 1000).toFixed(2);
  console.log(`✓ MongoDB B hash: ${checkpointB2.dbRoot} (${durationB2}s)`);
  console.log('');

  console.log('Database root hash comparison:');
  console.log(`  MongoDB A (clean):    ${checkpointA1.dbRoot}`);
  console.log(`  MongoDB B (tampered): ${checkpointB2.dbRoot}`);
  console.log('');

  if (checkpointA1.dbRoot !== checkpointB2.dbRoot) {
    console.log('🚨 TAMPERING DETECTED! State hashes differ!');
  } else {
    console.log('✓ State hashes match (no tampering detected)');
  }
  console.log('');

  // ================================================================
  // STEP 4: Use merkle tree to identify affected partitions
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 4: Using merkle tree to identify tampered partitions');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  const merkleA = await dbA
    .collection<MerkleDoc>('state_merkle')
    .find({ coll: 'articles' })
    .sort({ idx: 1 })
    .toArray();

  const merkleB2 = await dbB
    .collection<MerkleDoc>('state_merkle')
    .find({ coll: 'articles' })
    .sort({ idx: 1 })
    .toArray();

  const diffPartitions: DiffPartition[] = [];

  console.log('Comparing partition hashes:');
  console.log('');

  for (let i = 0; i < merkleA.length; i++) {
    const partA = merkleA[i];
    const partB = merkleB2[i];

    if (partA && partB && partA.root !== partB.root) {
      diffPartitions.push({
        idx: i,
        minId: partA.minId,
        maxId: partA.maxId,
        count: partA.count,
        hashA: partA.root,
        hashB: partB.root,
      });

      console.log(`  ⚠ Partition ${i}: DIFFERS`);
      console.log(
        `     Range: ${partA.minId} → ${partA.maxId} (${partA.count.toLocaleString()} docs)`,
      );
      console.log(`     MongoDB A: ${partA.root.substring(0, 16)}...`);
      console.log(`     MongoDB B: ${partB.root.substring(0, 16)}...`);
    }
  }

  console.log('');
  console.log(
    `✓ Found ${diffPartitions.length} tampered partitions out of ${merkleA.length} total`,
  );
  console.log(
    `  Efficiency: Only need to scan ${((diffPartitions.length / merkleA.length) * 100).toFixed(1)}% of database`,
  );
  console.log('');

  // ================================================================
  // STEP 5: Identify specific tampered documents
  // ================================================================
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('STEP 5: Identifying specific tampered documents');
  console.log(
    '──────────────────────────────────────────────────────────────────────',
  );
  console.log('');

  interface TamperedFound {
    _id: ObjectId;
    partition: number;
    hashA: string | undefined;
    hashB: string;
    hasTamperedField: boolean;
  }

  const tamperedFound: TamperedFound[] = [];

  for (const partition of diffPartitions) {
    console.log(
      `⏳ Scanning partition ${partition.idx} (${partition.count.toLocaleString()} documents)...`,
    );

    // Get all docs from this partition in both databases
    const docsA = await dbA
      .collection('articles')
      .find({ _id: { $gte: partition.minId, $lte: partition.maxId } })
      .sort({ _id: 1 })
      .toArray();

    const docsB = await dbB
      .collection('articles')
      .find({ _id: { $gte: partition.minId, $lte: partition.maxId } })
      .sort({ _id: 1 })
      .toArray();

    // Compare hashes
    const hashMapA = new Map<string, string>();
    for (const doc of docsA) {
      const hash = doc.__h || computeIntegrityHash(doc);
      hashMapA.set(doc._id.toString(), hash);
    }

    for (const doc of docsB) {
      const hashB = doc.__h || computeIntegrityHash(doc);
      const hashA = hashMapA.get(doc._id.toString());

      if (hashA !== hashB) {
        tamperedFound.push({
          _id: doc._id as ObjectId,
          partition: partition.idx,
          hashA,
          hashB,
          hasTamperedField: doc.TAMPERED === true,
        });

        console.log(`  🚨 Found tampered document: ${doc._id}`);
        console.log(`     Hash A: ${hashA?.substring(0, 16)}...`);
        console.log(`     Hash B: ${hashB.substring(0, 16)}...`);
      }
    }
  }

  console.log('');
  console.log(`✓ Identified ${tamperedFound.length} tampered documents`);
  console.log('');

  // ================================================================
  // SUMMARY
  // ================================================================
  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('Summary');
  console.log(
    '══════════════════════════════════════════════════════════════════════',
  );
  console.log('');

  console.log('✓ Tampering Detection Results:');
  console.log(`  - Tampered ${tamperedDocs.length} documents`);
  console.log(`  - Detected ${tamperedFound.length} tampered documents`);
  console.log(
    `  - Affected ${diffPartitions.length} out of ${merkleA.length} partitions (${((diffPartitions.length / merkleA.length) * 100).toFixed(1)}%)`,
  );
  console.log('');

  console.log('✓ Merkle Tree Efficiency:');
  console.log(
    `  - Total documents: ${merkleA.reduce((sum, p) => sum + p.count, 0).toLocaleString()}`,
  );
  console.log(
    `  - Documents to scan: ${diffPartitions.reduce((sum, p) => sum + p.count, 0).toLocaleString()}`,
  );
  console.log(
    `  - Scan reduction: ${(100 - (diffPartitions.reduce((sum, p) => sum + p.count, 0) / merkleA.reduce((sum, p) => sum + p.count, 0)) * 100).toFixed(1)}% fewer documents`,
  );
  console.log('');

  console.log('Next steps:');
  console.log(
    '  1. Run test-tamper-repair.js to restore MongoDB B from MongoDB A',
  );
  console.log('  2. Verify state hashes match again after repair');
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
