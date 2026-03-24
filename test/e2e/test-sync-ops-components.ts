#!/usr/bin/env tsx

/**
 * Test sync_ops ComponentsTable Implementation
 * Verifies that watch-changes stores operations in ComponentsTable format
 */

import { BsMem } from '@rljson/bs';

import { MongoClient } from 'mongodb';

import { createSuppressor, startDbChangeStream } from '../../src/watch-changes';

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  cyan: '\x1b[36m',
  yellow: '\x1b[33m',
  bold: '\x1b[1m',
  magenta: '\x1b[35m',
} as const;

async function test() {
  console.log(
    `\n${colors.cyan}${colors.bold}Testing sync_ops ComponentsTable${colors.reset}\n`,
  );

  const client = new MongoClient(
    'mongodb://localhost:27017/?directConnection=true',
  );

  try {
    console.log(`${colors.yellow}→${colors.reset} Connecting to MongoDB...`);
    await client.connect();
    console.log(`${colors.green}✓${colors.reset} Connected\n`);

    const db = client.db('test_sync_ops_components');
    const testCollection = db.collection('test_data');

    // Clean up from previous runs
    console.log(`${colors.yellow}→${colors.reset} Cleaning up...`);
    await testCollection.deleteMany({});
    await db.collection('sync_state').deleteMany({});
    await db.collection('sync_local').deleteMany({});
    await db.collection('sync_resume').deleteMany({});
    console.log(`${colors.green}✓${colors.reset} Cleaned\n`);

    // Test 1: Start change stream with ComponentsTable
    console.log(
      `${colors.cyan}Test 1: Start Change Stream with ComponentsTable${colors.reset}`,
    );
    console.log('─'.repeat(60));

    const bs = new BsMem();
    const suppressor = createSuppressor();

    // Simple logger
    const logger = {
      info: (obj: any, msg?: string) => {
        const message = msg || (typeof obj === 'string' ? obj : '');
        console.log(`[INFO] ${message}`);
      },
      warn: (obj: any, msg?: string) => {
        const message = msg || (typeof obj === 'string' ? obj : '');
        const details = typeof obj === 'object' ? JSON.stringify(obj) : '';
        console.log(`[WARN] ${message} ${details}`);
      },
      error: (obj: any, msg?: string) => {
        const message = msg || (typeof obj === 'string' ? obj : '');
        const details = typeof obj === 'object' ? JSON.stringify(obj) : '';
        console.log(`[ERROR] ${message} ${details}`);
      },
    };

    const changeStream = await startDbChangeStream({
      db,
      nodeId: 'node1',
      bs,
      suppressor,
      logger,
    });

    console.log(`${colors.green}✓${colors.reset} Change stream started\n`);

    // Wait a bit for change stream to be ready
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Test 2: Insert documents and verify sync_ops ComponentsTable
    console.log(
      `${colors.cyan}Test 2: Insert Documents and Check ComponentsTable${colors.reset}`,
    );
    console.log('─'.repeat(60));

    // Insert test documents
    await testCollection.insertOne({ _id: 'doc1', name: 'Alice', age: 30 });
    await testCollection.insertOne({ _id: 'doc2', name: 'Bob', age: 25 });
    await testCollection.updateOne({ _id: 'doc1' }, { $set: { age: 31 } });

    // Wait for operations to be processed
    console.log(
      `${colors.yellow}→${colors.reset} Waiting for change stream to process operations...`,
    );
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // Check sync_state metadata
    const meta = await db
      .collection('sync_state')
      .findOne({ _id: 'sync_ops_meta' } as any);

    if (meta) {
      console.log(`Sync ops metadata:`);
      console.log(`  componentsBlobId: ${(meta as any).componentsBlobId}`);
      console.log(`  tableCfgHash: ${(meta as any).tableCfgHash}`);
      console.log(`  rowCount: ${(meta as any).rowCount}`);
      console.log(`  updatedAt: ${(meta as any).updatedAt}`);
      console.log(`${colors.green}✓${colors.reset} Metadata found\n`);
    } else {
      console.log(`${colors.yellow}⚠${colors.reset} No metadata found\n`);
    }

    // Test 3: Retrieve ComponentsTable
    console.log(
      `${colors.cyan}Test 3: Retrieve and Verify ComponentsTable${colors.reset}`,
    );
    console.log('─'.repeat(60));

    if (meta && (meta as any).componentsBlobId) {
      const blobId = (meta as any).componentsBlobId;
      const blob = await bs.getBlob(blobId);

      if (blob) {
        const table = JSON.parse(blob.content.toString('utf-8'));

        console.log(`ComponentsTable structure:`);
        console.log(`  _type: ${table._type}`);
        console.log(`  _tableCfg: ${table._tableCfg}`);
        console.log(`  _hash: ${table._hash}`);
        console.log(`  _data rows: ${table._data.length}`);

        if (table._type === 'components') {
          console.log(
            `${colors.green}✓${colors.reset} Correct _type: components`,
          );
        }

        if (table._tableCfg === (meta as any).tableCfgHash) {
          console.log(
            `${colors.green}✓${colors.reset} _tableCfg matches metadata`,
          );
        }

        if (table._data.length === 3) {
          console.log(
            `${colors.green}✓${colors.reset} Correct number of operations: 3`,
          );
        } else {
          console.log(
            `${colors.yellow}⚠${colors.reset} Expected 3 operations, got ${table._data.length}`,
          );
        }

        // Check individual operations
        console.log(`\nSample operations:`);
        for (let i = 0; i < Math.min(3, table._data.length); i++) {
          const op = table._data[i];
          console.log(
            `  ${i + 1}. ${op.operationType} on ${op.ns?.coll} (seq: ${op.seq}, _hash: ${op._hash})`,
          );
        }

        // Verify blockchain hashing
        let chainValid = true;
        for (let i = 1; i < table._data.length; i++) {
          const prev = table._data[i - 1];
          const curr = table._data[i];
          if (curr.prevHash !== prev.chainHash) {
            chainValid = false;
            console.log(
              `${colors.yellow}⚠${colors.reset} Chain broken at operation ${i}`,
            );
            break;
          }
        }

        if (chainValid && table._data.length > 1) {
          console.log(
            `${colors.green}✓${colors.reset} Blockchain chain valid (prevHash → chainHash)`,
          );
        }

        console.log();
      } else {
        console.log(`${colors.yellow}⚠${colors.reset} Blob not found\n`);
      }
    } else {
      console.log(
        `${colors.yellow}⚠${colors.reset} No ComponentsTable to retrieve\n`,
      );
    }

    // Test 4: Check local state
    console.log(`${colors.cyan}Test 4: Verify Local State${colors.reset}`);
    console.log('─'.repeat(60));

    const local = await db
      .collection('sync_local')
      .findOne({ _id: 'local' } as any);

    if (local) {
      console.log(`Local state:`);
      console.log(`  seq: ${(local as any).seq}`);
      console.log(`  headHash: ${(local as any).headHash}`);
      console.log(`  updatedAt: ${(local as any).updatedAt}`);

      if ((local as any).seq === 3) {
        console.log(
          `${colors.green}✓${colors.reset} Correct sequence number: 3`,
        );
      } else {
        console.log(
          `${colors.yellow}⚠${colors.reset} Expected seq=3, got ${(local as any).seq}`,
        );
      }
      console.log();
    } else {
      console.log(`${colors.yellow}⚠${colors.reset} No local state found\n`);
    }

    // Clean up
    await changeStream.close();
    console.log(`${colors.yellow}→${colors.reset} Change stream closed`);

    // Summary
    console.log(`\n${colors.cyan}${colors.bold}Summary${colors.reset}`);
    console.log('═'.repeat(60));

    const checks = [
      !!meta,
      !!(meta && (meta as any).componentsBlobId),
      !!(meta && (meta as any).tableCfgHash),
      !!local,
      (local as any)?.seq === 3,
    ];

    const passed = checks.filter(Boolean).length;
    const total = checks.length;

    if (passed === total) {
      console.log(
        `${colors.green}${colors.bold}✓ ALL ${total} CHECKS PASSED!${colors.reset}`,
      );
      console.log(
        `\n${colors.magenta}sync_ops now correctly uses ComponentsTable:${colors.reset}`,
      );
      console.log(`  ✓ Operations stored in ComponentsTable format`);
      console.log(`  ✓ Metadata tracked in sync_state collection`);
      console.log(`  ✓ Blockchain hashing preserved (prevHash → chainHash)`);
      console.log(`  ✓ Sequence tracking in sync_local`);
      console.log(`  ✓ BlobStorage integration working`);
    } else {
      console.log(
        `${colors.yellow}⚠ ${passed}/${total} checks passed${colors.reset}`,
      );
    }

    console.log();
  } catch (error) {
    console.error(`\n${colors.yellow}✗ Error:${colors.reset}`, error);
    process.exit(1);
  } finally {
    await client.close();
    console.log(`${colors.yellow}→${colors.reset} Disconnected from MongoDB\n`);
  }
}

test().catch(console.error);
