#!/usr/bin/env node
// Quick test to verify change stream metadata is captured

import { BsMem } from '@rljson/bs';
import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/?directConnection=true';

async function main() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();

  try {
    const db = client.db('test_sync_ops');
    const bs = new BsMem();

    // Get metadata
    const meta = await db.collection('sync_state').findOne({ _id: 'sync_ops_meta' } as any);

    if (!meta || !(meta as any).componentsBlobId) {
      console.log('❌ No sync ops found. Run test-sync-ops-components.ts first.');
      return;
    }

    // Load ComponentsTable
    const blobId = (meta as any).componentsBlobId;
    const blob = await bs.getBlob(blobId);

    if (!blob) {
      console.log('❌ Could not load blob');
      return;
    }

    const table = JSON.parse(blob.content.toString('utf-8'));

    console.log('\n🔍 Checking Change Stream Metadata in Sync Ops\n');
    console.log('═'.repeat(70));

    if (table._data.length === 0) {
      console.log('❌ No operations found');
      return;
    }

    // Check first operation
    const firstOp = table._data[0];
    console.log('\n📄 First Sync Operation:');
    console.log('─'.repeat(70));
    
    console.log(`\nBasic Fields:`);
    console.log(`  _id: ${firstOp._id}`);
    console.log(`  origin: ${firstOp.origin}`);
    console.log(`  seq: ${firstOp.seq}`);
    console.log(`  operationType: ${firstOp.operationType}`);
    
    console.log(`\nBlockchain Fields:`);
    console.log(`  prevHash: ${firstOp.prevHash}`);
    console.log(`  opHash: ${firstOp.opHash}`);
    console.log(`  chainHash: ${firstOp.chainHash}`);

    console.log(`\nChange Stream Metadata:`);
    
    if (firstOp.changeStreamId !== undefined) {
      console.log(`  ✅ changeStreamId: ${JSON.stringify(firstOp.changeStreamId, null, 2).substring(0, 100)}...`);
    } else {
      console.log(`  ❌ changeStreamId: NOT CAPTURED`);
    }

    if (firstOp.clusterTime !== undefined) {
      console.log(`  ✅ clusterTime: ${JSON.stringify(firstOp.clusterTime, null, 2)}`);
    } else {
      console.log(`  ❌ clusterTime: NOT CAPTURED`);
    }

    if (firstOp.wallTime !== undefined) {
      console.log(`  ✅ wallTime: ${firstOp.wallTime}`);
    } else {
      console.log(`  ❌ wallTime: NOT CAPTURED`);
    }

    console.log('\n' + '═'.repeat(70));
    
    const hasMetadata = firstOp.changeStreamId !== undefined || 
                       firstOp.clusterTime !== undefined || 
                       firstOp.wallTime !== undefined;

    if (hasMetadata) {
      console.log('\n✅ SUCCESS: Change stream metadata is being captured!\n');
    } else {
      console.log('\n❌ FAILED: Change stream metadata is NOT being captured\n');
    }

  } finally {
    await client.close();
  }
}

main().catch(console.error);
