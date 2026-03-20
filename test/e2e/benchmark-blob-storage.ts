#!/usr/bin/env tsx

/**
 * Simple Blob Storage Performance Test
 */

import { BsMem } from '@rljson/bs';
import { MongoClient } from 'mongodb';

const MONGO_A_URI =
  process.env.MONGO_A_URI ||
  'mongodb://localhost:27017/?directConnection=true';

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  cyan: '\x1b[36m',
  bold: '\x1b[1m',
} as const;

function formatTime(ms: number): string {
  if (ms < 1000) return `${ms.toFixed(0)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)}KB`;
  return `${(bytes / 1024 / 1024).toFixed(2)}MB`;
}

async function test() {
  const client = new MongoClient(MONGO_A_URI);
  await client.connect();
  
  const db = client.db('syncdb');
  const docs = await db.collection('articles').find().limit(1000).toArray();
  
  console.log(`\n${colors.cyan}Testing blob storage with ${docs.length} documents${colors.reset}\n`);
  
  const bs = new BsMem();
  const blobIds: string[] = [];
  
  // Store
  const startStore = Date.now();
  for (const doc of docs) {
    const content = JSON.stringify(doc);
    const props = await bs.setBlob(Buffer.from(content, 'utf-8'));
    blobIds.push(props.blobId);
  }
  const storeTime = Date.now() - startStore;
  
  console.log(`${colors.green}✓${colors.reset} Stored ${docs.length} blobs in ${formatTime(storeTime)}`);
  console.log(`  ${(storeTime / docs.length).toFixed(2)}ms per blob`);
  
  // Retrieve
  const startRetrieve = Date.now();
  let totalSize = 0;
  for (const blobId of blobIds) {
    const blob = await bs.getBlob(blobId);
    totalSize += blob.content.length;
  }
  const retrieveTime = Date.now() - startRetrieve;
  
  console.log(`${colors.green}✓${colors.reset} Retrieved ${docs.length} blobs in ${formatTime(retrieveTime)}`);
  console.log(`  ${(retrieveTime / docs.length).toFixed(2)}ms per blob`);
  console.log(`  Total size: ${formatBytes(totalSize)}`);
  
  // Extrapolate
  const fullCount = 552321;
  const fullStoreTime = (storeTime / docs.length) * fullCount;
  const fullRetrieveTime = (retrieveTime / docs.length) * fullCount;
  const fullSize = (totalSize / docs.length) * fullCount;
  
  console.log(`\n${colors.cyan}Extrapolation for 552k documents:${colors.reset}`);
  console.log(`  Store time: ${formatTime(fullStoreTime)}`);
  console.log(`  Retrieve time: ${formatTime(fullRetrieveTime)}`);
  console.log(`  Total blob size: ${formatBytes(fullSize)}`);
  
  await client.close();
}

test().catch(console.error);
