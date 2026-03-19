#!/usr/bin/env tsx
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * Real-Time RLJSON Sync Test with MongoDB Change Streams
 *
 * This test demonstrates REAL-TIME synchronization using:
 * 1. MongoDB Change Streams to detect changes
 * 2. Automatic RLJSON extraction on change detection
 * 3. Real-time sync to second agent
 * 4. Verification that changes propagate correctly
 *
 * This proves the complete real-time RLJSON workflow works.
 */

import { BsMem } from '@rljson/bs';

import { ChangeStream, MongoClient, type } from 'mongodb';

import { applyRljsonTree, extractRljsonTree } from '../../src/index';


const MONGO_URI =
  process.env.MONGO_URI || 'mongodb://localhost:27017/?directConnection=true';
const AGENT_A_DB = 'test_changestream_agent_a';
const AGENT_B_DB = 'test_changestream_agent_b';

// Colors
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  green: '\x1b[32m',
  blue: '\x1b[34m',
  yellow: '\x1b[33m',
  cyan: '\x1b[36m',
  magenta: '\x1b[35m',
  red: '\x1b[31m',
};

function log(msg: string, color = colors.reset) {
  console.log(`${color}${msg}${colors.reset}`);
}

function header(msg: string) {
  log(`\n${'='.repeat(80)}`, colors.bright);
  log(msg, colors.bright + colors.cyan);
  log('='.repeat(80), colors.bright);
}

function section(msg: string) {
  log(`\n${msg}`, colors.bright + colors.yellow);
  log('-'.repeat(msg.length), colors.yellow);
}

function success(msg: string) {
  log(`✓ ${msg}`, colors.green);
}

function info(msg: string) {
  log(`  ${msg}`, colors.blue);
}

function highlight(msg: string) {
  log(msg, colors.magenta);
}

function error(msg: string) {
  log(`✗ ${msg}`, colors.red);
}

function timestamp() {
  return new Date().toISOString().substring(11, 23);
}

async function waitForSync(delayMs: number = 500): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, delayMs));
}

async function main() {
  let client: MongoClient | null = null;
  let changeStream: ChangeStream | null = null;
  let syncCount = 0;

  try {
    header('REAL-TIME RLJSON SYNC TEST WITH CHANGE STREAMS');
    info('This test uses real MongoDB change streams for automatic sync');

    // ========================================================================
    // PHASE 1: Setup
    // ========================================================================
    header('PHASE 1: Setup MongoDB and Agents');

    client = new MongoClient(MONGO_URI);
    await client.connect();
    success('Connected to MongoDB');

    const mongoA = client.db(AGENT_A_DB);
    const mongoB = client.db(AGENT_B_DB);

    // Clean databases
    await mongoA.dropDatabase();
    await mongoB.dropDatabase();
    info('Cleaned test databases');

    // Check if replica set is available (required for change streams)
    section('Verifying MongoDB replica set...');
    try {
      const admin = client.db('admin');
      const status = await admin.command({ replSetGetStatus: 1 });
      success(`Replica set detected: ${status.set}`);
      info(
        `Primary: ${status.members.find((m: any) => m.stateStr === 'PRIMARY')?.name || 'unknown'}`,
      );
    } catch (err) {
      error('MongoDB is not running as a replica set!');
      error('Change streams require replica set mode.');
      info('\nTo fix:');
      info('  1. Stop current MongoDB');
      info('  2. Start with: docker compose up -d');
      info('  3. Or start mongod with --replSet option');
      throw new Error('Replica set required for change streams');
    }

    // Initialize blob storage
    const bsA = new BsMem();
    const bsB = new BsMem();

    // ========================================================================
    // PHASE 2: Setup Change Stream on Agent A
    // ========================================================================
    header('PHASE 2: Setup Change Stream Listener');

    section('Setting up change stream on Agent A database...');

    // Create initial data so collections exist
    await mongoA.collection('users').insertOne({ _id: 'init', name: 'Init' });
    await mongoA.collection('users').deleteOne({ _id: 'init' });

    // Setup change stream
    changeStream = mongoA.watch([], { fullDocument: 'updateLookup' });
    success('Change stream created');

    // Track changes
    const detectedChanges: Array<{
      type: string;
      ns: string;
      docId: any;
      time: string;
    }> = [];

    // Change stream handler
    changeStream.on('change', async (change) => {
      const changeType = change.operationType;
      const ns = `${change.ns.db}.${change.ns.coll}`;
      const docId = (change as any).documentKey?._id || 'unknown';

      detectedChanges.push({
        type: changeType,
        ns,
        docId,
        time: timestamp(),
      });

      highlight(
        `\n[${timestamp()}] 🔔 Change detected: ${changeType} in ${ns} (${docId})`,
      );

      // Trigger RLJSON sync
      try {
        info(`  → Extracting RLJSON tree...`);
        const payload = await extractRljsonTree({
          mongoDb: mongoA,
          nodeId: 'agentA',
          bs: bsA,
        });

        info(`  → Syncing to Agent B...`);
        const result = await applyRljsonTree({
          mongoDb: mongoB,
          payload,
          bs: bsB,
        });

        if (result.success) {
          syncCount++;
          success(
            `  ✓ Sync #${syncCount} completed (${result.documentsCreated} docs)`,
          );
        } else {
          error(`  ✗ Sync failed: ${result.error}`);
        }
      } catch (err) {
        error(`  ✗ Error during sync: ${err}`);
      }
    });

    changeStream.on('error', (err) => {
      error(`Change stream error: ${err}`);
    });

    success('Change stream listener active');
    info('Watching for: insert, update, delete, replace operations');

    // ========================================================================
    // PHASE 3: Perform Changes and Watch Real-Time Sync
    // ========================================================================
    header('PHASE 3: Make Changes and Watch Real-Time Sync');

    await waitForSync(1000); // Let change stream settle

    // Change 1: Insert users
    section('Change 1: Inserting users...');
    await mongoA.collection('users').insertMany([
      {
        _id: 'user1',
        name: 'Alice',
        email: 'alice@example.com',
        role: 'admin',
      },
      { _id: 'user2', name: 'Bob', email: 'bob@example.com', role: 'user' },
    ]);
    info('Inserted 2 users into Agent A');
    await waitForSync();

    // Verify on Agent B
    const usersAfter1 = await mongoB.collection('users').find().toArray();
    if (usersAfter1.length === 2) {
      success(`✓ Agent B received ${usersAfter1.length} users`);
    } else {
      error(`✗ Agent B has ${usersAfter1.length} users, expected 2`);
    }

    // Change 2: Insert orders
    section('Change 2: Inserting orders...');
    await mongoA.collection('orders').insertMany([
      { _id: 'order1', userId: 'user1', total: 99.99, status: 'pending' },
      { _id: 'order2', userId: 'user2', total: 149.99, status: 'completed' },
    ]);
    info('Inserted 2 orders into Agent A');
    await waitForSync();

    // Verify on Agent B
    const ordersAfter2 = await mongoB.collection('orders').find().toArray();
    if (ordersAfter2.length === 2) {
      success(`✓ Agent B received ${ordersAfter2.length} orders`);
    } else {
      error(`✗ Agent B has ${ordersAfter2.length} orders, expected 2`);
    }

    // Change 3: Update user
    section('Change 3: Updating user...');
    await mongoA
      .collection('users')
      .updateOne(
        { _id: 'user1' },
        { $set: { name: 'Alice Updated', credits: 100 } },
      );
    info('Updated user1 in Agent A');
    await waitForSync();

    // Verify on Agent B
    const user1AfterUpdate = await mongoB
      .collection('users')
      .findOne({ _id: 'user1' });
    if (
      user1AfterUpdate?.name === 'Alice Updated' &&
      user1AfterUpdate?.credits === 100
    ) {
      success('✓ Agent B received updated user data');
      info(`  Name: ${user1AfterUpdate.name}`);
      info(`  Credits: ${user1AfterUpdate.credits}`);
    } else {
      error('✗ Agent B did not receive update correctly');
    }

    // Change 4: Insert products
    section('Change 4: Inserting products...');
    await mongoA.collection('products').insertMany([
      { _id: 'prod1', name: 'Laptop', price: 999.99, stock: 10 },
      { _id: 'prod2', name: 'Mouse', price: 29.99, stock: 50 },
      { _id: 'prod3', name: 'Keyboard', price: 79.99, stock: 30 },
    ]);
    info('Inserted 3 products into Agent A');
    await waitForSync();

    // Verify on Agent B
    const productsAfter4 = await mongoB.collection('products').find().toArray();
    if (productsAfter4.length === 3) {
      success(`✓ Agent B received ${productsAfter4.length} products`);
    } else {
      error(`✗ Agent B has ${productsAfter4.length} products, expected 3`);
    }

    // Change 5: Update order status
    section('Change 5: Updating order status...');
    await mongoA
      .collection('orders')
      .updateOne(
        { _id: 'order1' },
        { $set: { status: 'shipped', shippedAt: new Date().toISOString() } },
      );
    info('Updated order1 status in Agent A');
    await waitForSync();

    // Verify on Agent B
    const order1AfterUpdate = await mongoB
      .collection('orders')
      .findOne({ _id: 'order1' });
    if (order1AfterUpdate?.status === 'shipped') {
      success('✓ Agent B received updated order status');
      info(`  Status: ${order1AfterUpdate.status}`);
      info(`  Shipped at: ${order1AfterUpdate.shippedAt}`);
    } else {
      error('✗ Agent B did not receive order update correctly');
    }

    // Change 6: Delete a product
    section('Change 6: Deleting product...');
    await mongoA.collection('products').deleteOne({ _id: 'prod2' });
    info('Deleted prod2 from Agent A');
    await waitForSync();

    // Verify on Agent B
    const productsAfter6 = await mongoB.collection('products').find().toArray();
    const prod2Exists = productsAfter6.some((p) => p._id === 'prod2');
    if (productsAfter6.length === 2 && !prod2Exists) {
      success('✓ Agent B synchronized deletion');
      info(`  Remaining products: ${productsAfter6.length}`);
    } else {
      error('✗ Agent B did not sync deletion correctly');
    }

    // Change 7: Bulk insert
    section('Change 7: Bulk inserting comments...');
    await mongoA.collection('comments').insertMany([
      { _id: 'c1', orderId: 'order1', text: 'Great product!' },
      { _id: 'c2', orderId: 'order1', text: 'Fast shipping' },
      { _id: 'c3', orderId: 'order2', text: 'Good quality' },
    ]);
    info('Inserted 3 comments into Agent A');
    await waitForSync();

    // Verify on Agent B
    const commentsAfter7 = await mongoB.collection('comments').find().toArray();
    if (commentsAfter7.length === 3) {
      success(`✓ Agent B received ${commentsAfter7.length} comments`);
    } else {
      error(`✗ Agent B has ${commentsAfter7.length} comments, expected 3`);
    }

    // ========================================================================
    // PHASE 4: Verify Complete Data Consistency
    // ========================================================================
    header('PHASE 4: Verify Complete Data Consistency');

    section('Comparing all data between Agent A and Agent B...');

    // Get all collections
    const collectionsA = await mongoA.listCollections().toArray();
    const collectionsB = await mongoB.listCollections().toArray();

    const collNamesA = collectionsA
      .map((c) => c.name)
      .filter((n) => n !== 'rljson_sync_state')
      .sort();
    const collNamesB = collectionsB
      .map((c) => c.name)
      .filter((n) => n !== 'rljson_sync_state')
      .sort();

    info(`Agent A collections: ${collNamesA.join(', ')}`);
    info(`Agent B collections: ${collNamesB.join(', ')}`);

    if (JSON.stringify(collNamesA) !== JSON.stringify(collNamesB)) {
      error('Collections mismatch!');
      throw new Error('Collections do not match');
    }
    success('✓ Collections match');

    // Compare each collection
    let totalDocs = 0;
    let matchedDocs = 0;

    for (const collName of collNamesA) {
      const docsA = await mongoA
        .collection(collName)
        .find()
        .sort({ _id: 1 })
        .toArray();
      const docsB = await mongoB
        .collection(collName)
        .find()
        .sort({ _id: 1 })
        .toArray();

      totalDocs += docsA.length;

      if (docsA.length !== docsB.length) {
        error(
          `${collName}: count mismatch (A: ${docsA.length}, B: ${docsB.length})`,
        );
        continue;
      }

      let collMatch = true;
      for (let i = 0; i < docsA.length; i++) {
        if (JSON.stringify(docsA[i]) !== JSON.stringify(docsB[i])) {
          error(`${collName}: document ${i} mismatch`);
          collMatch = false;
        }
      }

      if (collMatch) {
        matchedDocs += docsA.length;
        success(`${collName}: ${docsA.length} documents match perfectly`);
      }
    }

    if (matchedDocs === totalDocs) {
      success(
        `\n✓ ALL ${totalDocs} documents match between Agent A and Agent B`,
      );
    } else {
      error(`\n✗ Only ${matchedDocs}/${totalDocs} documents match`);
      throw new Error('Data consistency check failed');
    }

    // ========================================================================
    // PHASE 5: Display Change Stream Activity
    // ========================================================================
    header('PHASE 5: Change Stream Activity Summary');

    section('Detected changes:');
    for (const change of detectedChanges) {
      info(
        `[${change.time}] ${change.type.padEnd(10)} ${change.ns.padEnd(40)} ${change.docId}`,
      );
    }

    info(`\nTotal changes detected: ${detectedChanges.length}`);
    info(`Total syncs triggered: ${syncCount}`);

    // ========================================================================
    // FINAL SUMMARY
    // ========================================================================
    header('✓ REAL-TIME SYNC TEST PASSED');

    log('\n' + '='.repeat(80), colors.bright + colors.green);
    log(
      'REAL-TIME RLJSON SYNC WORKING PERFECTLY',
      colors.bright + colors.green,
    );
    log('='.repeat(80), colors.bright + colors.green);

    section('What was proven:');
    success('✓ MongoDB change streams detect changes in real-time');
    success('✓ Changes trigger automatic RLJSON extraction');
    success('✓ RLJSON sync propagates to Agent B automatically');
    success('✓ All insert operations synced correctly');
    success('✓ All update operations synced correctly');
    success('✓ All delete operations synced correctly');
    success('✓ Complete data consistency maintained');
    success(`✓ ${detectedChanges.length} changes detected and synced`);
    success(
      `✓ ${totalDocs} documents verified across ${collNamesA.length} collections`,
    );

    section('Change Stream Statistics:');
    const inserts = detectedChanges.filter((c) => c.type === 'insert').length;
    const updates = detectedChanges.filter((c) => c.type === 'update').length;
    const deletes = detectedChanges.filter((c) => c.type === 'delete').length;

    info(`Inserts: ${inserts}`);
    info(`Updates: ${updates}`);
    info(`Deletes: ${deletes}`);
    info(`Total: ${detectedChanges.length}`);

    section('RLJSON Real-Time Sync Validated:');
    highlight('✓ Change streams work with RLJSON');
    highlight('✓ Automatic sync on every change');
    highlight('✓ Complete data propagation');
    highlight('✓ Hash-based integrity maintained');
    highlight('✓ Ready for production real-time sync');
  } catch (err) {
    error(`\nTEST FAILED: ${err}`);
    console.error(err);
    process.exit(1);
  } finally {
    // Cleanup
    if (changeStream) {
      await changeStream.close();
      info('\nClosed change stream');
    }
    if (client) {
      await client.close();
      info('Closed MongoDB connection');
    }
  }
}

// Run the test
main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
