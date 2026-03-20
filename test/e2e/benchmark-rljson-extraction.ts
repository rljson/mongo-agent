#!/usr/bin/env tsx

/**
 * RLJSON Extraction Performance Benchmark
 *
 * Tests the RLJSON extraction performance:
 * - Scanning MongoDB collections
 * - Converting to RLJSON tree structure
 * - Storing documents in blob storage
 * - Creating hash chains for integrity
 * 
 * Testing with 552k+ documents from cd_articles collection
 */

import { BsMem } from '@rljson/bs';
import { MongoClient } from 'mongodb';
import { MongoAgent } from '../../src/index.js';

const MONGO_A_URI =
  process.env.MONGO_A_URI ||
  'mongodb://localhost:27017/syncdb?directConnection=true';

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
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
      `${colors.bold}${colors.cyan}  RLJSON Extraction Performance Benchmark${colors.reset}`,
    );
    console.log('═'.repeat(70) + '\n');

    console.log(
      `${colors.blue}ℹ${colors.reset} Connecting to MongoDB...`,
    );
    client = new MongoClient(MONGO_A_URI);
    await client.connect();
    const db = client.db('syncdb');
    console.log(
      `${colors.green}✓${colors.reset} Connected to MongoDB\n`,
    );

    // Get collection stats
    console.log('─'.repeat(70));
    console.log(
      `${colors.cyan}${colors.bold}Collection Statistics${colors.reset}`,
    );
    console.log('─'.repeat(70));

    const collections = await db.listCollections().toArray();
    const stats: Array<{ name: string; count: number; size: number }> = [];
    let totalDocs = 0;
    let totalSize = 0;

    for (const coll of collections) {
      const count = await db.collection(coll.name).countDocuments({});
      const collStats = await db.command({ collStats: coll.name });
      stats.push({
        name: coll.name,
        count,
        size: collStats.size || 0,
      });
      totalDocs += count;
      totalSize += collStats.size || 0;
    }

    console.log(`  Total Collections: ${stats.length}`);
    console.log(`  Total Documents: ${formatNumber(totalDocs)}`);
    console.log(`  Total Size: ${formatBytes(totalSize)}`);
    console.log('');

    for (const stat of stats) {
      if (stat.count > 0) {
        console.log(`  ${stat.name}:`);
        console.log(`    Documents: ${formatNumber(stat.count)}`);
        console.log(`    Size: ${formatBytes(stat.size)}`);
      }
    }
    console.log('');

    // Initialize MongoAgent with blob storage
    console.log('─'.repeat(70));
    console.log(
      `${colors.cyan}${colors.bold}RLJSON Extraction Test${colors.reset}`,
    );
    console.log('─'.repeat(70));
    console.log(
      `${colors.blue}ℹ${colors.reset} Creating MongoAgent with blob storage...`,
    );
    
    const bs = new BsMem();
    const agent = new MongoAgent(db, bs);
    console.log(`${colors.green}✓${colors.reset} MongoAgent created\n`);

    // Benchmark extraction
    console.log(
      `${colors.blue}ℹ${colors.reset} Starting RLJSON extraction...`,
    );
    console.log(
      `${colors.blue}ℹ${colors.reset} This will:`,
    );
    console.log(`  - Scan all documents from MongoDB`);
    console.log(`  - Build RLJSON tree structure with hash chains`);
    console.log(`  - Store document content in blob storage`);
    console.log('');

    const startExtract = Date.now();
    let progressInterval = setInterval(() => {
      const elapsed = Date.now() - startExtract;
      process.stdout.write(
        `\r${colors.yellow}⏳${colors.reset} Extracting... ${formatTime(elapsed)} elapsed`,
      );
    }, 1000);

    const tree = await agent.extract();
    
    clearInterval(progressInterval);
    const extractTime = Date.now() - startExtract;

    console.log(
      `\r${colors.green}✓${colors.reset} Extraction completed in ${colors.bold}${formatTime(extractTime)}${colors.reset}`,
    );
    console.log('');

    // Analyze results
    console.log('─'.repeat(70));
    console.log(
      `${colors.cyan}${colors.bold}Extraction Results${colors.reset}`,
    );
    console.log('─'.repeat(70));
    
    console.log(`  Root Hash: ${tree.rootHash}`);
    console.log(`  Total Tree Nodes: ${formatNumber(tree.trees.size)}`);
    console.log('');

    // Count nodes by type
    let dbNodes = 0;
    let collNodes = 0;
    let docNodes = 0;
    let blobCount = 0;
    let totalBlobSize = 0;

    for (const [, node] of tree.trees) {
      const meta = node.meta as any;
      if (meta?.type === 'database') dbNodes++;
      else if (meta?.type === 'collection') collNodes++;
      else if (meta?.type === 'document') {
        docNodes++;
        if (meta?.blobId) {
          blobCount++;
          const blob = await bs.getBlob(meta.blobId);
          totalBlobSize += blob.content.length;
        }
      }
    }

    console.log(`  Node Distribution:`);
    console.log(`    Database nodes: ${dbNodes}`);
    console.log(`    Collection nodes: ${collNodes}`);
    console.log(`    Document nodes: ${formatNumber(docNodes)}`);
    console.log('');

    console.log(`  Blob Storage:`);
    console.log(`    Total blobs: ${formatNumber(blobCount)}`);
    console.log(`    Total blob size: ${formatBytes(totalBlobSize)}`);
    console.log(`    Avg blob size: ${formatBytes(totalBlobSize / blobCount)}`);
    console.log('');

    // Performance metrics
    console.log('─'.repeat(70));
    console.log(
      `${colors.cyan}${colors.bold}Performance Metrics${colors.reset}`,
    );
    console.log('─'.repeat(70));

    const docsPerSec = totalDocs / (extractTime / 1000);
    const bytesPerSec = totalSize / (extractTime / 1000);
    const timePerDoc = extractTime / totalDocs;

    console.log(`  Extraction Performance:`);
    console.log(`    Total time: ${formatTime(extractTime)}`);
    console.log(`    Documents processed: ${formatNumber(totalDocs)}`);
    console.log(`    Data size: ${formatBytes(totalSize)}`);
    console.log('');
    console.log(`    Throughput:`);
    console.log(`      ${formatNumber(Math.round(docsPerSec))} docs/sec`);
    console.log(`      ${formatBytes(bytesPerSec)}/sec`);
    console.log(`      ${timePerDoc.toFixed(3)}ms per document`);
    console.log('');

    console.log(`  Memory Efficiency:`);
    console.log(`    Original data size: ${formatBytes(totalSize)}`);
    console.log(`    Blob storage size: ${formatBytes(totalBlobSize)}`);
    const ratio = (totalBlobSize / totalSize) * 100;
    console.log(`    Storage ratio: ${ratio.toFixed(1)}%`);
    console.log('');

    // Sample blobs
    console.log('─'.repeat(70));
    console.log(
      `${colors.cyan}${colors.bold}Sample Blobs (first 5)${colors.reset}`,
    );
    console.log('─'.repeat(70));

    let sampleCount = 0;
    for (const [hash, node] of tree.trees) {
      const meta = node.meta as any;
      if (meta?.type === 'document' && meta?.blobId && sampleCount < 5) {
        const blob = await bs.getBlob(meta.blobId);
        const content = blob.content.toString('utf-8');
        const doc = JSON.parse(content);
        
        console.log(`  ${sampleCount + 1}. Document ID: ${meta.docId}`);
        console.log(`     Collection: ${meta.collection}`);
        console.log(`     Hash: ${hash.substring(0, 16)}...`);
        console.log(`     Blob ID: ${meta.blobId.substring(0, 16)}...`);
        console.log(`     Size: ${formatBytes(blob.content.length)}`);
        console.log(`     Keys: ${Object.keys(doc).length}`);
        console.log('');
        sampleCount++;
      }
    }

    // Summary
    console.log('═'.repeat(70));
    console.log(
      `${colors.bold}${colors.green}Summary${colors.reset}`,
    );
    console.log('═'.repeat(70));
    console.log(`  ✓ Successfully extracted ${formatNumber(totalDocs)} documents`);
    console.log(`  ✓ Stored ${formatNumber(blobCount)} blobs (${formatBytes(totalBlobSize)})`);
    console.log(`  ✓ Created ${formatNumber(tree.trees.size)} hash-chained nodes`);
    console.log(`  ✓ Throughput: ${formatNumber(Math.round(docsPerSec))} docs/sec`);
    console.log(`  ✓ Performance: ${formatTime(extractTime)} total time`);
    console.log('═'.repeat(70) + '\n');

  } catch (err) {
    console.error(`${colors.red}✗ Error:${colors.reset}`, err);
    process.exit(1);
  } finally {
    if (client) {
      await client.close();
      console.log(`${colors.blue}ℹ${colors.reset} Closed MongoDB connection\n`);
    }
  }
}

benchmark().catch(console.error);
