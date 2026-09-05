#!/usr/bin/env node
/**
 * Insert test data into MongoDB to test sync functionality
 */

import { MongoClient } from 'mongodb';

const MONGO_URI =
  process.env.MONGO_URI || 'mongodb://mongoa:27017/syncdb?replicaSet=rsA';

async function main() {
  console.log('🔌 Connecting to MongoDB...');
  const client = new MongoClient(MONGO_URI);
  await client.connect();

  const db = client.db('syncdb');
  const users = db.collection('users');

  console.log('📝 Inserting test users...');

  await users.insertMany([
    {
      _id: 'user-001',
      name: 'Alice Johnson',
      email: 'alice@example.com',
      age: 30,
      department: 'Engineering',
      status: 'active',
      createdAt: new Date(),
    },
    {
      _id: 'user-002',
      name: 'Bob Smith',
      email: 'bob@example.com',
      age: 28,
      department: 'Sales',
      status: 'active',
      createdAt: new Date(),
    },
    {
      _id: 'user-003',
      name: 'Carol Davis',
      email: 'carol@example.com',
      age: 35,
      department: 'Marketing',
      status: 'active',
      createdAt: new Date(),
    },
  ]);

  console.log('✅ Inserted 3 test users');

  // Wait a bit, then update one
  console.log('⏳ Waiting 2 seconds...');
  await new Promise((resolve) => setTimeout(resolve, 2000));

  console.log('🔄 Updating Alice...');
  await users.updateOne(
    { _id: 'user-001' },
    {
      $set: {
        age: 31,
        department: 'Engineering - Senior',
        updatedAt: new Date(),
      },
    },
  );

  console.log('✅ Updated Alice');

  // Check sync_ops
  const syncOps = db.collection('sync_ops');
  const opsCount = await syncOps.countDocuments();
  console.log(`\n📊 Total sync operations captured: ${opsCount}`);

  if (opsCount > 0) {
    const latestOps = await syncOps.find().sort({ seq: -1 }).limit(3).toArray();
    console.log('\n📋 Latest sync operations:');
    latestOps.forEach((op) => {
      console.log(
        `  - Seq ${op.seq}: ${op.type} on ${op.ns} (${op.documentKey?._id || 'N/A'})`,
      );
    });
  }

  await client.close();
  console.log(
    '\n✅ Done! Data inserted and sync operations should be visible.',
  );
  console.log(
    '👉 Check MongoDB Compass for the "users" collection and "sync_ops" collection',
  );
}

main().catch(console.error);
