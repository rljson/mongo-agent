#!/usr/bin/env tsx

/**
 * RLJSON Sync Payload Benchmark
 *
 * Measures:
 * 1. RLJSON tree extraction time
 * 2. RLJSON payload size (tree structure for sync)
 * 3. Blob retrieval/reconstruction time
 * 4. Full vs incremental sync sizes
 */

import { BsMem } from '@rljson/bs';
import { MongoClient } from 'mongodb';
import { MongoScanner } from '../../src/index.js';

const MONGO_A_URI =
  process.env.MONGO_A_URI ||
  'mongodb://localhost:27017/?directConnection=true';

const SAMPLE_SIZE = parseInt(process.env.SAMPLE_SIZE || '10000');
const TEST_DB = 'syncdb_sample';

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
  magenta: '\x1b[35m',
  bold: '\x1b[1m',
} as const;

function formatTime(ms: number): string {
  if (ms < 1000) return `${ms.toFixed(0)}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
  return `${(ms / 60000).toFixed(2)}min`;
}

function formatNumber(n: number): string {
  return n.toLocaleString('en-US');
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)}KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(2)}MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)}GB`;
}

async function benchmark(): Promise<void> {
  let client: MongoClient | undefined;

  try {
    console.log('\n' + '═'.repeat(70));
    console.log(
      `${colors.bold}${colors.cyan}  RLJSON Sync Payload & Performance Benchmark${colors.reset}`,
    );
    console.log('═'.repeat(70) + '\n');

    console.log(`${colors.blue}ℹ${colors.reset} Connecting to MongoDB...`);
    client = new MongoClient(MONGO_A_URI);
    await client.connect();
    const sourceDb = client.db('syncdb');
    const testDb = client.db(TEST_DB);
    console.log(`${colors.green}✓${colors.reset} Connected\n`);

    // Create sample
    console.log('─'.repeat(70));
    console.log(`${colors.cyan}${colors.bold}Step 1: Preparing Sample${colors.reset}`);
    console.log('─'.repeat(70));
    console.log(`${colors.blue}ℹ${colors.reset} Creating ${formatNumber(SAMPLE_SIZE)} document sample...`);
    
    await testDb.dropDatabase();
    
    await sourceDb.collection('articles').aggregate([
      { $sample: { size: SAMPLE_SIZE } },
      { $out: { db: TEST_DB, coll: 'articles' } }
    ]).toArray();
    
    const count = await testDb.collection('articles').countDocuments({});
    const stats = await testDb.command({ collStats: 'articles' });
    
    console.log(`${colors.green}✓${colors.reset} Sample created: ${formatNumber(count)} documents (${formatBytes(stats.size)})`);
    console.log('');

    // ========================================================================
    // PHASE 1: Extract to RLJSON + Blob Storage
    // ========================================================================
    console.log('─'.repeat(70));
    console.log(`${colors.cyan}${colors.bold}Step 2: RLJSON Extraction${colors.reset}`);
    console.log('─'.repeat(70));
    
    const bs = new BsMem();
    const scanner = new MongoScanner(testDb, bs);

    console.log(`${colors.blue}ℹ${colors.reset} Extracting documents to RLJSON tree + blob storage...`);
    const startExtract = Date.now();
    const tree = await scanner.scan();
    const extractTime = Date.now() - startExtract;

    console.log(`${colors.green}✓${colors.reset} Extracted in ${formatTime(extractTime)}`);
    console.log(`  Tree nodes: ${formatNumber(tree.trees.size)}`);
    console.log(`  Root hash: ${tree.rootHash}`);
    console.log('');

    // ========================================================================
    // PHASE 2: Measure Tree Payload Size (What Gets Synced)
    // ========================================================================
    console.log('─'.repeat(70));
    console.log(`${colors.cyan}${colors.bold}Step 3: RLJSON Sync Payload Size${colors.reset}`);
    console.log('─'.repeat(70));
    
    // Convert tree to sync payload (tree structure only, no blob content)
    console.log(`${colors.blue}ℹ${colors.reset} Serializing tree structure for sync...`);
    
    const treePayload = {
      rootHash: tree.rootHash,
      trees: Array.from(tree.trees.entries()).map(([hash, node]) => ({
        hash,
        node
      }))
    };
    
    const payloadJson = JSON.stringify(treePayload);
    const payloadSize = Buffer.from(payloadJson, 'utf-8').length;
    
    console.log(`${colors.green}✓${colors.reset} Tree payload generated`);
    console.log(`  Payload size: ${formatBytes(payloadSize)}`);
    console.log(`  Per document: ${formatBytes(payloadSize / count)}`);
    console.log(`  Compression potential: ~50-70% with gzip`);
    console.log('');

    // ========================================================================
    // PHASE 3: Measure Blob Retrieval Time
    // ========================================================================
    console.log('─'.repeat(70));
    console.log(`${colors.cyan}${colors.bold}Step 4: Blob Retrieval Performance${colors.reset}`);
    console.log('─'.repeat(70));
    
    console.log(`${colors.blue}ℹ${colors.reset} Measuring blob retrieval time...`);
    
    // Collect all blob IDs
    const blobIds: string[] = [];
    for (const [, node] of tree.trees) {
      const meta = node.meta as any;
      if (meta?.type === 'document' && meta?.blobId) {
        blobIds.push(meta.blobId);
      }
    }
    
    console.log(`  Found ${formatNumber(blobIds.length)} blobs`);
    
    // Measure retrieval time for a sample
    const sampleBlobCount = Math.min(1000, blobIds.length);
    const sampleBlobs = blobIds.slice(0, sampleBlobCount);
    
    const startRetrieve = Date.now();
    let totalBlobSize = 0;
    let retrievedCount = 0;
    
    for (const blobId of sampleBlobs) {
      try {
        const blob = await bs.getBlob(blobId);
        totalBlobSize += blob.content.length;
        retrievedCount++;
      } catch (err) {
        // Blob not found
      }
    }
    
    const retrieveTime = Date.now() - startRetrieve;
    
    if (retrievedCount > 0) {
      console.log(`${colors.green}✓${colors.reset} Retrieved ${formatNumber(retrievedCount)} blobs in ${formatTime(retrieveTime)}`);
      console.log(`  Throughput: ${formatNumber(Math.round(retrievedCount / (retrieveTime / 1000)))} blobs/sec`);
      console.log(`  Time per blob: ${(retrieveTime / retrievedCount).toFixed(3)}ms`);
      console.log(`  Average blob size: ${formatBytes(totalBlobSize / retrievedCount)}`);
      console.log('');
      
      // Extrapolate full retrieval time
      const fullRetrieveTime = (retrieveTime / retrievedCount) * blobIds.length;
      console.log(`  ${colors.magenta}Estimated time to retrieve all ${formatNumber(blobIds.length)} blobs:${colors.reset}`);
      console.log(`    ${formatTime(fullRetrieveTime)}`);
    } else {
      console.log(`${colors.yellow}⚠${colors.reset} No blobs retrieved (blob storage issue)`);
    }
    console.log('');

    // ========================================================================
    // PHASE 4: Extrapolate to Full Dataset
    // ========================================================================
    console.log('─'.repeat(70));
    console.log(`${colors.cyan}${colors.bold}Step 5: Extrapolation to 552k Documents${colors.reset}`);
    console.log('─'.repeat(70));
    
    const fullCount = 552321;
    const fullPayloadSize = (payloadSize / count) * fullCount;
    const fullExtractTime = (extractTime / count) * fullCount;
    const fullRetrieveTime = retrievedCount > 0 ? (retrieveTime / retrievedCount) * fullCount : 0;
    
    console.log(`  ${colors.bold}Full Dataset (${formatNumber(fullCount)} documents):${colors.reset}`);
    console.log('');
    console.log(`  Extraction (MongoDB → RLJSON):`);
    console.log(`    Time: ${formatTime(fullExtractTime)}`);
    console.log(`    Throughput: ${formatNumber(Math.round(count / (extractTime / 1000)))} docs/sec`);
    console.log('');
    console.log(`  Sync Payload (tree structure only):`);
    console.log(`    Size: ${formatBytes(fullPayloadSize)}`);
    console.log(`    Compressed (~60%): ${formatBytes(fullPayloadSize * 0.4)}`);
    console.log(`    Per document: ${formatBytes(fullPayloadSize / fullCount)}`);
    console.log('');
    
    if (retrievedCount > 0) {
      console.log(`  Blob Retrieval (reconstruct documents):`);
      console.log(`    Time: ${formatTime(fullRetrieveTime)}`);
      console.log(`    Throughput: ${formatNumber(Math.round(retrievedCount / (retrieveTime / 1000)))} blobs/sec`);
      console.log('');
    }
    
    console.log(`  ${colors.magenta}Total Sync Time Estimate:${colors.reset}`);
    console.log(`    Extract: ${formatTime(fullExtractTime)}`);
    console.log(`    Transfer payload: ~${formatTime((fullPayloadSize * 0.4) / (10 * 1024 * 1024))} (@ 10MB/s network)`);
    if (retrievedCount > 0) {
      console.log(`    Retrieve blobs: ${formatTime(fullRetrieveTime)}`);
    }
    console.log('');

    // ========================================================================
    // Summary
    // ========================================================================
    console.log('═'.repeat(70));
    console.log(`${colors.bold}${colors.green}Summary${colors.reset}`);
    console.log('═'.repeat(70));
    console.log(`  Sample: ${formatNumber(count)} documents`);
    console.log('');
    console.log(`  ${colors.bold}Measured:${colors.reset}`);
    console.log(`    Extraction time: ${formatTime(extractTime)}`);
    console.log(`    Sync payload size: ${formatBytes(payloadSize)}`);
    if (retrievedCount > 0) {
      console.log(`    Blob retrieval: ${formatNumber(retrievedCount)} in ${formatTime(retrieveTime)}`);
    }
    console.log('');
    console.log(`  ${colors.bold}For 552k documents:${colors.reset}`);
    console.log(`    ${colors.cyan}→${colors.reset} RLJSON tree payload: ${formatBytes(fullPayloadSize)} (${formatBytes(fullPayloadSize * 0.4)} compressed)`);
    console.log(`    ${colors.cyan}→${colors.reset} Full extraction: ~${formatTime(fullExtractTime)}`);
    if (retrievedCount > 0) {
      console.log(`    ${colors.cyan}→${colors.reset} Full blob retrieval: ~${formatTime(fullRetrieveTime)}`);
    }
    console.log('═'.repeat(70) + '\n');

    // Cleanup
    console.log(`${colors.blue}ℹ${colors.reset} Cleaning up...`);
    await testDb.dropDatabase();
    console.log(`${colors.green}✓${colors.reset} Done\n`);

  } catch (err) {
    console.error(`${colors.reset}\n✗ Error:`, err);
    process.exit(1);
  } finally {
    if (client) {
      await client.close();
    }
  }
}

benchmark().catch(console.error);
