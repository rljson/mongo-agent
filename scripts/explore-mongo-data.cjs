#!/usr/bin/env node

/**
 * MongoDB Real Data Explorer
 * Simple CLI tool to view your sync data
 */

const { MongoClient, ObjectId } = require('mongodb');

const mongoUrl = 'mongodb://localhost:27017';

async function exploreData() {
  const client = new MongoClient(mongoUrl);

  try {
    await client.connect();
    console.log('✅ Connected to MongoDB\n');

    const db = client.db('syncdb');

    // 1. Collection Statistics
    console.log('📊 Collection Statistics:');
    console.log('='.repeat(60));

    const collections = ['sync_ops', 'articles', 'sync_state', 'sync_local'];
    for (const collName of collections) {
      try {
        const count = await db.collection(collName).countDocuments();
        console.log(
          `  ${collName.padEnd(20)} ${count.toLocaleString()} documents`,
        );
      } catch (e) {
        // Collection might not exist
      }
    }

    console.log('\n📍 Node Activity:');
    console.log('='.repeat(60));

    // 2. Node Statistics
    const syncOps = db.collection('sync_ops');
    const nodeStats = await syncOps
      .aggregate([
        {
          $group: {
            _id: '$origin',
            totalOps: { $sum: 1 },
            lastOp: { $max: '$seq' },
            firstOp: { $min: '$seq' },
            operations: { $push: '$operationType' },
          },
        },
      ])
      .toArray();

    for (const node of nodeStats) {
      const opTypes = node.operations.reduce((acc, op) => {
        acc[op] = (acc[op] || 0) + 1;
        return acc;
      }, {});

      console.log(`\n  Node: ${node._id}`);
      console.log(`    Total Operations: ${node.totalOps.toLocaleString()}`);
      console.log(`    Sequence Range: ${node.firstOp} → ${node.lastOp}`);
      console.log(`    Operations: ${JSON.stringify(opTypes)}`);
    }

    console.log('\n\n🔍 Recent Operations:');
    console.log('='.repeat(60));

    // 3. Recent Operations
    const recentOps = await syncOps
      .find({})
      .sort({ seq: -1 })
      .limit(5)
      .toArray();

    for (const op of recentOps) {
      console.log(`\n  ${op._id}`);
      console.log(
        `    Origin: ${op.origin} | Seq: ${op.seq} | Type: ${op.operationType}`,
      );
      console.log(`    Time: ${op.ts}`);
      console.log(`    Doc: ${op.docId}`);
      console.log(`    Hash: ${op.opHash?.substring(0, 16)}...`);
    }

    console.log('\n\n🔀 Potential Conflicts:');
    console.log('='.repeat(60));

    // 4. Find potential conflicts
    const conflicts = await syncOps
      .aggregate([
        {
          $group: {
            _id: '$docId',
            operations: {
              $push: {
                origin: '$origin',
                type: '$operationType',
                ts: '$ts',
                seq: '$seq',
              },
            },
            count: { $sum: 1 },
            nodes: { $addToSet: '$origin' },
          },
        },
        {
          $match: {
            count: { $gte: 2 },
            $expr: { $gt: [{ $size: '$nodes' }, 1] },
          },
        },
        { $limit: 5 },
      ])
      .toArray();

    if (conflicts.length === 0) {
      console.log('  ✅ No conflicts detected');
    } else {
      for (const conflict of conflicts) {
        console.log(`\n  Document: ${conflict._id}`);
        console.log(`    Nodes involved: ${conflict.nodes.join(', ')}`);
        console.log(`    Operations: ${conflict.count}`);

        // Show operations timeline
        conflict.operations.sort((a, b) => new Date(a.ts) - new Date(b.ts));
        conflict.operations.forEach((op) => {
          console.log(
            `      ${new Date(op.ts).toISOString()} | ${op.origin} | ${op.type}`,
          );
        });
      }
    }

    console.log('\n\n💡 Tips:');
    console.log('='.repeat(60));
    console.log('  • Install MongoDB Compass for visual exploration');
    console.log('  • Connection: mongodb://localhost:27017');
    console.log('  • Database: syncdb');
    console.log('  • Check MONGODB_VISUALIZATION_GUIDE.md for more details');
    console.log('');
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await client.close();
  }
}

exploreData();
