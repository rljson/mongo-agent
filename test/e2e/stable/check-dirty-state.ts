import { MongoClient } from 'mongodb';

async function check() {
  const client = new MongoClient('mongodb://localhost:27017');
  await client.connect();

  const db = client.db('test_articles_performance');

  // Check state_merkle (cache)
  const cacheCount = await db.collection('state_merkle').countDocuments();
  console.log(`\n📦 Cache (state_merkle): ${cacheCount} partitions`);

  // Check state_dirty
  const dirtyDocs = await db.collection('state_dirty').find().toArray();
  console.log(
    `\n🔥 Dirty tracking (state_dirty): ${dirtyDocs.length} documents`,
  );

  for (const doc of dirtyDocs) {
    console.log('\nDirty document:');
    console.log(JSON.stringify(doc, null, 2));
  }

  await client.close();
}

check().catch(console.error);
