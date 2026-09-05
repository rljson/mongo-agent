#!/usr/bin/env node
/**
 * Apply already-resolved conflicts back to their collections
 */

import { MongoClient } from 'mongodb';

const MONGO_URI =
  process.env.MONGO_URI || 'mongodb://mongoa:27017/syncdb?replicaSet=rsA';

async function main() {
  console.log('🔌 Connecting to MongoDB...');
  const client = new MongoClient(MONGO_URI);
  await client.connect();

  const db = client.db('syncdb');
  const conflicts = db.collection('sync_conflicts');

  // Find all resolved conflicts
  const resolvedConflicts = await conflicts
    .find({ status: 'resolved' })
    .toArray();

  console.log(`\n📊 Found ${resolvedConflicts.length} resolved conflicts`);

  for (const conflict of resolvedConflicts) {
    console.log(`\n🔄 Processing: ${conflict.conflictId}`);

    const resolution = conflict.resolution;
    if (!resolution) {
      console.log('  ⚠️  No resolution data found');
      continue;
    }

    // Get the resolved document
    const resolvedDoc =
      resolution.mergedDocument || resolution.selectedVersion?.data;

    if (!resolvedDoc) {
      console.log('  ⚠️  No resolved document found');
      continue;
    }

    // Apply to the collection
    const collectionName = conflict.collection || 'articles';
    const collection = db.collection(collectionName);

    const result = await collection.updateOne(
      { _id: conflict.documentId },
      {
        $set: {
          ...resolvedDoc,
          _resolvedFrom: conflict.conflictId,
          _resolvedAt: new Date(resolution.resolvedAt || Date.now()),
          _resolvedBy: resolution.resolutionType,
        },
      },
      { upsert: true },
    );

    console.log(`  ✅ Applied to ${collectionName}`);
    console.log(`     Resolution type: ${resolution.resolutionType}`);
    console.log(`     Document ID: ${conflict.documentId}`);
    console.log(`     Modified count: ${result.modifiedCount}`);
  }

  console.log('\n✅ Done! All resolved conflicts have been applied.');

  await client.close();
}

main().catch(console.error);
