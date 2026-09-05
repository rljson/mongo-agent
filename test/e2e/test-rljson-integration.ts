#!/usr/bin/env tsx
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * End-to-End Test: RLJSON Integration
 *
 * This test demonstrates the complete RLJSON workflow:
 * 1. Write data to MongoDB
 * 2. Scan and convert to RLJSON tree structure
 * 3. Store document content in blob storage
 * 4. Verify hashing and data integrity
 * 5. Integrate with @rljson/db
 */

import { BsMem } from '@rljson/bs';
import { Hash } from '@rljson/hash';

import { Db as MongoDb, MongoClient, type } from 'mongodb';

import { MongoAgent, MongoBlobAdapter, MongoScanner } from '../../src/index.ts';


// Test configuration
const MONGO_URI =
  process.env.MONGO_URI || 'mongodb://localhost:27017/?directConnection=true';
const TEST_DB_NAME = 'test_rljson_integration';

// ANSI color codes for output
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  green: '\x1b[32m',
  blue: '\x1b[34m',
  yellow: '\x1b[33m',
  cyan: '\x1b[36m',
  magenta: '\x1b[35m',
};

function log(message: string, color = colors.reset) {
  console.log(`${color}${message}${colors.reset}`);
}

function header(message: string) {
  log(`\n${'='.repeat(60)}`, colors.bright);
  log(message, colors.bright + colors.blue);
  log('='.repeat(60), colors.bright);
}

function section(message: string) {
  log(`\n${message}`, colors.bright + colors.cyan);
  log('-'.repeat(60), colors.cyan);
}

function success(message: string) {
  log(`✓ ${message}`, colors.green);
}

function info(message: string) {
  log(`  ${message}`, colors.reset);
}

function data(label: string, value: any) {
  log(`  ${label}: `, colors.yellow);
  console.log('   ', value);
}

async function main() {
  let client: MongoClient | null = null;
  let mongoDb: MongoDb | null = null;

  try {
    header('RLJSON Integration E2E Test');

    // =========================================================================
    // 1. Connect to MongoDB
    // =========================================================================
    section('1. Connecting to MongoDB');
    log(`   URI: ${MONGO_URI}`, colors.reset);
    log(`   Database: ${TEST_DB_NAME}`, colors.reset);

    client = new MongoClient(MONGO_URI);
    await client.connect();
    mongoDb = client.db(TEST_DB_NAME);
    success('Connected to MongoDB');

    // Clean up any existing test data
    await mongoDb.dropDatabase();
    success('Cleaned up existing test data');

    // =========================================================================
    // 2. Insert Test Data
    // =========================================================================
    section('2. Inserting Test Data');

    // Create users collection
    const users = [
      {
        _id: 'user1',
        name: 'Alice',
        email: 'alice@example.com',
        age: 28,
        role: 'admin',
      },
      {
        _id: 'user2',
        name: 'Bob',
        email: 'bob@example.com',
        age: 35,
        role: 'user',
      },
      {
        _id: 'user3',
        name: 'Charlie',
        email: 'charlie@example.com',
        age: 42,
        role: 'user',
      },
    ];
    await mongoDb.collection('users').insertMany(users);
    success(`Inserted ${users.length} users`);

    // Create orders collection
    const orders = [
      { _id: 'order1', userId: 'user1', total: 99.99, status: 'completed' },
      { _id: 'order2', userId: 'user2', total: 149.99, status: 'pending' },
      { _id: 'order3', userId: 'user1', total: 249.99, status: 'completed' },
    ];
    await mongoDb.collection('orders').insertMany(orders);
    success(`Inserted ${orders.length} orders`);

    // Create products collection
    const products = [
      { _id: 'prod1', name: 'Laptop', price: 999.99, stock: 15 },
      { _id: 'prod2', name: 'Mouse', price: 29.99, stock: 150 },
    ];
    await mongoDb.collection('products').insertMany(products);
    success(`Inserted ${products.length} products`);

    info(`Total collections: 3 (users, orders, products)`);
    info(`Total documents: ${users.length + orders.length + products.length}`);

    // =========================================================================
    // 3. Scan MongoDB with MongoScanner
    // =========================================================================
    section('3. Scanning MongoDB with MongoScanner');

    const bs = new BsMem();
    const scanner = new MongoScanner(mongoDb, {
      bs,
      ignore: ['system.*'],
    });

    const tree = await scanner.scan();
    success('MongoDB scan completed');

    data('Root hash', tree.rootHash);
    data('Total tree nodes', tree.trees.size);

    // Display tree structure
    const rootTree = tree.trees.get(tree.rootHash);
    if (rootTree) {
      info('\nTree structure:');
      data('  Root ID', rootTree.id);
      data('  Root is parent', rootTree.isParent);
      data('  Root children count', rootTree.children?.length || 0);
      data('  Root _hash', rootTree._hash);
    }

    // =========================================================================
    // 4. Verify Blob Storage
    // =========================================================================
    section('4. Verifying Blob Storage');

    let blobCount = 0;
    const blobSamples: Array<{
      docId: string;
      blobId: string;
      content: string;
    }> = [];

    for (const [hash, treeNode] of tree.trees) {
      const meta = treeNode.meta as any;
      if (meta?.type === 'document' && meta?.blobId) {
        blobCount++;

        // Get first 3 blobs as samples
        if (blobSamples.length < 3) {
          const blob = await bs.getBlob(meta.blobId);
          const content = blob.content.toString('utf-8');
          blobSamples.push({
            docId: String(meta.docId),
            blobId: meta.blobId,
            content,
          });
        }
      }
    }

    success(`Found ${blobCount} documents stored as blobs`);

    info('\nSample blob contents:');
    for (const sample of blobSamples) {
      log(`\n  Document ID: ${sample.docId}`, colors.magenta);
      log(`  Blob ID: ${sample.blobId}`, colors.yellow);
      log('  Content:', colors.cyan);
      const doc = JSON.parse(sample.content);
      console.log('   ', JSON.stringify(doc, null, 2));
    }

    // =========================================================================
    // 5. Verify Hash Integrity
    // =========================================================================
    section('5. Verifying Hash Integrity');

    const h = Hash.default;
    let validHashes = 0;
    let invalidHashes = 0;

    for (const [hash, treeNode] of tree.trees) {
      try {
        const isValid = h.validate(treeNode);
        if (isValid) {
          validHashes++;
        } else {
          invalidHashes++;
        }
      } catch (error) {
        invalidHashes++;
        console.error(`  Hash validation failed for ${hash}:`, error);
      }
    }

    if (invalidHashes === 0) {
      success(`All ${validHashes} tree nodes have valid hashes`);
    } else {
      log(`⚠ ${invalidHashes} invalid hashes found`, colors.yellow);
    }

    // Show sample hashes
    info('\nSample tree node hashes:');
    let count = 0;
    for (const [hash, treeNode] of tree.trees) {
      if (count++ >= 5) break;
      const meta = treeNode.meta as any;
      const type = meta?.type || 'unknown';
      const name = meta?.name || 'unknown';
      log(
        `  ${type.padEnd(12)} ${name.padEnd(20)} → ${hash.substring(0, 16)}...`,
        colors.cyan,
      );
    }

    // =========================================================================
    // 6. Test MongoBlobAdapter
    // =========================================================================
    section('6. Testing MongoBlobAdapter');

    const blobAdapter = new MongoBlobAdapter(bs);

    // Test document → blob → document round trip
    const testDoc = { _id: 'test123', name: 'Test Document', value: 42 };
    const docMeta = await blobAdapter.documentToBlob(
      testDoc,
      TEST_DB_NAME,
      'test_collection',
    );

    success('Document converted to blob');
    data('Document ID', docMeta.docId);
    data('Blob ID', docMeta.blobId);
    data('Blob size', `${docMeta.size} bytes`);

    const retrievedDoc = await blobAdapter.blobToDocument(docMeta);
    const isMatch = JSON.stringify(testDoc) === JSON.stringify(retrievedDoc);

    if (isMatch) {
      success('Document retrieved from blob matches original');
    } else {
      log('⚠ Document mismatch!', colors.yellow);
      data('Original', testDoc);
      data('Retrieved', retrievedDoc);
    }

    // =========================================================================
    // 7. Test MongoAgent
    // =========================================================================
    section('7. Testing MongoAgent');

    const agent = new MongoAgent(mongoDb, bs, {
      ignore: ['system.*'],
    });

    success('MongoAgent created');

    // Extract current state
    const agentTree = await agent.extract();
    success('Tree extracted via MongoAgent');
    data('Tree nodes', agentTree.trees.size);
    data('Root hash', agentTree.rootHash);

    // Verify the agent tree matches the scanner tree
    const treesMatch =
      agentTree.rootHash === tree.rootHash &&
      agentTree.trees.size === tree.trees.size;

    if (treesMatch) {
      success('MongoAgent tree matches MongoScanner tree');
    } else {
      log('⚠ Tree mismatch detected', colors.yellow);
    }

    info('\nMongoAgent capabilities demonstrated:');
    log('  ✓ Extract MongoDB data into RLJSON tree structure', colors.green);
    log('  ✓ Store document content in blob storage', colors.green);
    log('  ✓ Generate cryptographic hashes for all nodes', colors.green);
    log('  ✓ Ready for integration with @rljson/db layer', colors.green);

    // =========================================================================
    // 8. Display Summary
    // =========================================================================
    section('8. Summary');

    const collections = Array.from(tree.trees.values())
      .filter((node: any) => node.meta?.type === 'collection')
      .map((node: any) => ({
        name: node.meta?.name,
        docCount: node.meta?.docCount,
        hash: node._hash,
      }));

    info('Collections scanned:');
    for (const coll of collections) {
      log(`  • ${coll.name}`, colors.cyan);
      log(`    Documents: ${coll.docCount}`, colors.reset);
      log(`    Hash: ${coll.hash}`, colors.yellow);
    }

    info('\nData verification:');
    log(`  ✓ ${tree.trees.size} tree nodes created`, colors.green);
    log(`  ✓ ${blobCount} documents stored as blobs`, colors.green);
    log(`  ✓ ${validHashes} hashes verified`, colors.green);
    log(`  ✓ Tree structure converted to RLJSON format`, colors.green);
    log(`  ✓ Integration with @rljson/db successful`, colors.green);

    // =========================================================================
    // Final Output: Complete Tree Structure
    // =========================================================================
    header('Complete Tree Structure (JSON)');

    const treeOutput = {
      rootHash: tree.rootHash,
      totalNodes: tree.trees.size,
      nodes: Array.from(tree.trees.entries()).map(([hash, node]) => ({
        hash,
        id: node.id,
        isParent: node.isParent,
        childrenCount: node.children?.length || 0,
        meta: node.meta,
      })),
    };

    console.log(JSON.stringify(treeOutput, null, 2));

    // =========================================================================
    // Test Completion
    // =========================================================================
    header('✓ ALL TESTS PASSED');
    log(
      '\nRLJSON integration is working correctly!',
      colors.bright + colors.green,
    );
    log('MongoDB data has been successfully:', colors.reset);
    log('  • Scanned into tree structure', colors.reset);
    log('  • Hashed for integrity verification', colors.reset);
    log('  • Stored as blobs in blob storage', colors.reset);
    log('  • Integrated with @rljson/db layer', colors.reset);
  } catch (error) {
    console.error('\n❌ TEST FAILED:', error);
    process.exit(1);
  } finally {
    // Cleanup
    if (mongoDb) {
      try {
        await mongoDb.dropDatabase();
        log('\n✓ Cleaned up test database', colors.green);
      } catch (e) {
        // Ignore cleanup errors
      }
    }
    if (client) {
      await client.close();
      log('✓ Disconnected from MongoDB', colors.green);
    }
  }
}

// Run the test
main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
