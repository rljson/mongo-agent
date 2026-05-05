#!/usr/bin/env tsx
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * End-to-End Test: RLJSON Agent-to-Agent Sync
 *
 * This test demonstrates hash-based synchronization between two agents:
 * 1. Agent A: Has data in MongoDB
 * 2. Agent A: Extracts RLJSON tree with hashes
 * 3. Agent B: Receives the tree payload
 * 4. Agent B: Applies the tree to its MongoDB
 * 5. Verification: Both agents have identical data
 *
 * This simulates the real sync process where agents exchange
 * hash-based tree structures instead of raw JSON operations.
 */

import { Db as MongoDb, MongoClient, type } from 'mongodb';

import { applyRljsonTree, extractRljsonTree, RljsonTreePayload, type } from '../../src/index.ts';


// Test configuration
const MONGO_URI =
  process.env.MONGO_URI || 'mongodb://localhost:27017/?directConnection=true';
const AGENT_A_DB = 'test_agent_a';
const AGENT_B_DB = 'test_agent_b';

// ANSI color codes for output
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

function log(message: string, color = colors.reset) {
  console.log(`${color}${message}${colors.reset}`);
}

function header(message: string) {
  log(`\n${'='.repeat(70)}`, colors.bright);
  log(message, colors.bright + colors.cyan);
  log('='.repeat(70), colors.bright);
}

function section(message: string) {
  log(`\n${message}`, colors.bright + colors.yellow);
  log('-'.repeat(message.length), colors.yellow);
}

function success(message: string) {
  log(`✓ ${message}`, colors.green);
}

function info(message: string) {
  log(`  ${message}`, colors.blue);
}

function error(message: string) {
  log(`✗ ${message}`, colors.red);
}

function highlight(message: string) {
  log(message, colors.magenta);
}

async function setupTestData(mongoDb: MongoDb): Promise<void> {
  section('Setting up test data in Agent A');

  // Insert users
  await mongoDb.collection('users').insertMany([
    { _id: 'user1', name: 'Alice', email: 'alice@example.com', role: 'admin' },
    { _id: 'user2', name: 'Bob', email: 'bob@example.com', role: 'user' },
    {
      _id: 'user3',
      name: 'Charlie',
      email: 'charlie@example.com',
      role: 'user',
    },
  ]);
  success('Inserted 3 users');

  // Insert posts
  await mongoDb.collection('posts').insertMany([
    {
      _id: 'post1',
      title: 'Hello World',
      author: 'user1',
      content: 'First post!',
    },
    {
      _id: 'post2',
      title: 'MongoDB Tips',
      author: 'user1',
      content: 'Use indexes wisely.',
    },
    {
      _id: 'post3',
      title: 'RLJSON Intro',
      author: 'user2',
      content: 'Hash-based sync is cool.',
    },
  ]);
  success('Inserted 3 posts');

  // Insert comments
  await mongoDb.collection('comments').insertMany([
    { _id: 'comment1', postId: 'post1', author: 'user2', text: 'Great post!' },
    {
      _id: 'comment2',
      postId: 'post1',
      author: 'user3',
      text: 'Thanks for sharing.',
    },
    {
      _id: 'comment3',
      postId: 'post2',
      author: 'user2',
      text: 'Very helpful.',
    },
    { _id: 'comment4', postId: 'post3', author: 'user1', text: 'Agreed!' },
  ]);
  success('Inserted 4 comments');

  info(`Total documents: 10 across 3 collections`);
}

async function displayAgentData(
  mongoDb: MongoDb,
  agentName: string,
): Promise<void> {
  section(`${agentName} - Current Data`);

  const collections = await mongoDb.listCollections().toArray();

  for (const collInfo of collections) {
    const collName = collInfo.name;
    const count = await mongoDb.collection(collName).countDocuments();
    info(`${collName}: ${count} documents`);

    if (count > 0) {
      const docs = await mongoDb.collection(collName).find().limit(2).toArray();
      for (const doc of docs) {
        highlight(`  - ${JSON.stringify(doc)}`);
      }
      if (count > 2) {
        info(`  ... and ${count - 2} more`);
      }
    }
  }
}

function displayTreePayload(payload: RljsonTreePayload): void {
  section('RLJSON Tree Payload (What Agent B Receives)');

  info(`Origin: ${payload.origin}`);
  info(`Root Hash: ${payload.rootHash}`);
  info(`Total Nodes: ${payload.totalNodes}`);
  info(`Timestamp: ${payload.timestamp}`);

  highlight(`\nTree Structure (${payload.nodes.length} nodes):`);
  for (const item of payload.nodes.slice(0, 5)) {
    const node = item.node;
    const meta = node.meta;

    if (meta.type === 'database') {
      info(`  📦 Database: ${meta.database} [hash: ${item.hash}]`);
    } else if (meta.type === 'collection') {
      info(`    📁 Collection: ${meta.collection} [hash: ${item.hash}]`);
    } else if (meta.type === 'document') {
      info(
        `      📄 Document: ${meta.docId} [hash: ${item.hash}, blobId: ${meta.blobId}]`,
      );
    }
  }
  if (payload.nodes.length > 5) {
    info(`  ... and ${payload.nodes.length - 5} more nodes`);
  }

  highlight(`\nBlob Storage (${payload.blobs.length} blobs):`);
  for (const blob of payload.blobs.slice(0, 3)) {
    const decoded = Buffer.from(blob.content, 'base64').toString('utf-8');
    const preview =
      decoded.length > 80 ? decoded.substring(0, 80) + '...' : decoded;
    info(`  Blob ${blob.blobId}: ${preview}`);
  }
  if (payload.blobs.length > 3) {
    info(`  ... and ${payload.blobs.length - 3} more blobs`);
  }

  // Calculate payload size
  const payloadJson = JSON.stringify(payload);
  const sizeKB = (payloadJson.length / 1024).toFixed(2);
  info(`\nPayload Size: ${sizeKB} KB`);
}

async function verifySync(mongoA: MongoDb, mongoB: MongoDb): Promise<boolean> {
  section('Verification - Comparing Agent A and Agent B');

  const collectionsA = await mongoA.listCollections().toArray();
  const collectionsB = await mongoB.listCollections().toArray();

  // Filter out system/metadata collections
  const collNamesA = collectionsA
    .map((c) => c.name)
    .filter((n) => n !== 'rljson_sync_state')
    .sort();
  const collNamesB = collectionsB
    .map((c) => c.name)
    .filter((n) => n !== 'rljson_sync_state')
    .sort();

  if (JSON.stringify(collNamesA) !== JSON.stringify(collNamesB)) {
    error('Collections do not match!');
    info(`Agent A: ${collNamesA.join(', ')}`);
    info(`Agent B: ${collNamesB.join(', ')}`);
    return false;
  }
  success(`Collections match: ${collNamesA.join(', ')}`);

  let allMatch = true;

  for (const collName of collNamesA) {
    const countA = await mongoA.collection(collName).countDocuments();
    const countB = await mongoB.collection(collName).countDocuments();

    if (countA !== countB) {
      error(
        `Collection ${collName}: Agent A has ${countA} docs, Agent B has ${countB} docs`,
      );
      allMatch = false;
      continue;
    }

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

    // Canonicalize key order before compare (ComponentsTable roundtrip alphabetizes fields)
    const canon = (docs: any[]) =>
      JSON.stringify(
        docs.map((d) =>
          Object.keys(d)
            .sort()
            .reduce<Record<string, unknown>>((o, k) => {
              o[k] = d[k];
              return o;
            }, {}),
        ),
      );

    if (canon(docsA) !== canon(docsB)) {
      error(`Collection ${collName}: Documents differ!`);
      allMatch = false;
      continue;
    }

    success(`Collection ${collName}: ${countA} documents match perfectly`);
  }

  return allMatch;
}

async function main(): Promise<void> {
  header('RLJSON Agent-to-Agent Sync Test');

  let clientA: MongoClient | null = null;
  let clientB: MongoClient | null = null;

  try {
    // Connect to MongoDB for Agent A
    section('Connecting to MongoDB for Agent A');
    clientA = new MongoClient(MONGO_URI);
    await clientA.connect();
    success('Connected to MongoDB for Agent A');

    const mongoA = clientA.db(AGENT_A_DB);

    // Clean up Agent A database
    await mongoA.dropDatabase();
    info(`Dropped database: ${AGENT_A_DB}`);

    // Setup test data in Agent A
    await setupTestData(mongoA);

    // Display Agent A data
    await displayAgentData(mongoA, 'Agent A');

    // ========================================
    // STEP 1: Extract RLJSON Tree from Agent A
    // ========================================
    header('STEP 1: Agent A - Extract RLJSON Tree');

    const startExtract = Date.now();
    const payload = await extractRljsonTree({
      mongoDb: mongoA,
      nodeId: 'agentA',
    });
    const extractTime = Date.now() - startExtract;

    success(`Extracted tree in ${extractTime}ms`);
    info(`Root hash: ${payload.rootHash}`);
    info(`Total nodes: ${payload.totalNodes}`);
    info(`Total blobs: ${payload.blobs.length}`);

    // Display what's being sent
    displayTreePayload(payload);

    // ========================================
    // STEP 2: Agent B Receives and Applies
    // ========================================
    header('STEP 2: Agent B - Receive and Apply RLJSON Tree');

    // Connect to MongoDB for Agent B
    section('Connecting to MongoDB for Agent B');
    clientB = new MongoClient(MONGO_URI);
    await clientB.connect();
    success('Connected to MongoDB for Agent B');

    const mongoB = clientB.db(AGENT_B_DB);

    // Clean up Agent B database (simulate empty agent)
    await mongoB.dropDatabase();
    info(`Dropped database: ${AGENT_B_DB} (simulating empty agent)`);

    // Agent B applies the tree
    section('Applying RLJSON tree to Agent B');

    const startApply = Date.now();
    const result = await applyRljsonTree({
      mongoDb: mongoB,
      payload: payload,
    });
    const applyTime = Date.now() - startApply;

    success(`Applied tree in ${applyTime}ms`);
    info(`Nodes applied: ${result.nodesApplied}`);
    info(`Blobs received: ${result.blobsReceived}`);
    info(`Documents created: ${result.documentsCreated}`);
    info(`Root hash verified: ${result.rootHash}`);

    // Display Agent B data after sync
    await displayAgentData(mongoB, 'Agent B (After Sync)');

    // ========================================
    // STEP 3: Verify Sync Succeeded
    // ========================================
    header('STEP 3: Verify Synchronization');

    const syncSuccessful = await verifySync(mongoA, mongoB);

    if (syncSuccessful) {
      header('✅ TEST PASSED: RLJSON Sync Successful!');
      success('Agent A and Agent B have identical data');
      success('Hash-based synchronization works correctly');

      // Performance summary
      section('Performance Summary');
      info(`Extract time: ${extractTime}ms`);
      info(`Apply time: ${applyTime}ms`);
      info(`Total time: ${extractTime + applyTime}ms`);

      const payloadSize = (JSON.stringify(payload).length / 1024).toFixed(2);
      info(`Payload size: ${payloadSize} KB`);
      info(`Documents synced: ${result.documentsCreated}`);
      info(
        `Efficiency: ${(result.documentsCreated / parseFloat(payloadSize)).toFixed(1)} docs/KB`,
      );
    } else {
      error('TEST FAILED: Sync verification failed!');
      process.exit(1);
    }
  } catch (err) {
    error(`Test failed with error: ${err}`);
    console.error(err);
    process.exit(1);
  } finally {
    // Cleanup
    if (clientA) {
      await clientA.close();
      info('Closed Agent A connection');
    }
    if (clientB) {
      await clientB.close();
      info('Closed Agent B connection');
    }
  }
}

// Run the test
main();
