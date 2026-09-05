#!/usr/bin/env tsx

/**
 * RLJSON Extraction Benchmark - Sample Test
 *
 * Tests RLJSON extraction on a sample of the articles collection
 * to demonstrate performance without hitting memory limits
 */

import { BsMem } from '@rljson/bs';

import { MongoClient } from 'mongodb';

import { MongoScanner } from '../../src/index.js';

const MONGO_A_URI =
  process.env.MONGO_A_URI || 'mongodb://localhost:27017/?directConnection=true';

const SAMPLE_SIZE = parseInt(process.env.SAMPLE_SIZE || '10000');
const TEST_DB = 'syncdb_sample';

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
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
  if (bytes < 1024 * 1024 * 1024)
    return `${(bytes / 1024 / 1024).toFixed(2)}MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)}GB`;
}

async function benchmark(): Promise<void> {
  let client: MongoClient | undefined;

  try {
    console.log('\n' + '═'.repeat(70));
    console.log(
      `${colors.bold}${colors.cyan}  RLJSON Extraction - Sample Test (${formatNumber(SAMPLE_SIZE)} docs)${colors.reset}`,
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
    console.log(`${colors.cyan}${colors.bold}Preparing Sample${colors.reset}`);
    console.log('─'.repeat(70));
    console.log(
      `${colors.blue}ℹ${colors.reset} Creating sample of ${formatNumber(SAMPLE_SIZE)} documents...`,
    );

    await testDb.dropDatabase();

    const sampleResult = await sourceDb
      .collection('articles')
      .aggregate([
        { $sample: { size: SAMPLE_SIZE } },
        { $out: { db: TEST_DB, coll: 'articles' } },
      ])
      .toArray();

    const count = await testDb.collection('articles').countDocuments({});
    const stats = await testDb.command({ collStats: 'articles' });

    console.log(`${colors.green}✓${colors.reset} Sample created`);
    console.log(`  Documents: ${formatNumber(count)}`);
    console.log(`  Size: ${formatBytes(stats.size)}`);
    console.log(`  Avg doc size: ${formatBytes(stats.size / count)}`);
    console.log('');

    // Initialize scanner with blob storage
    const bs = new BsMem();
    const scanner = new MongoScanner(testDb, bs);

    console.log('─'.repeat(70));
    console.log(`${colors.cyan}${colors.bold}RLJSON Extraction${colors.reset}`);
    console.log('─'.repeat(70));
    console.log(`${colors.blue}ℹ${colors.reset} Starting extraction...`);
    console.log('');

    const startTime = Date.now();
    let progressInterval = setInterval(() => {
      const elapsed = Date.now() - startTime;
      process.stdout.write(
        `\r${colors.yellow}⏳${colors.reset} Extracting... ${formatTime(elapsed)} elapsed`,
      );
    }, 500);

    const tree = await scanner.scan();

    clearInterval(progressInterval);
    const extractTime = Date.now() - startTime;

    console.log(
      `\r${colors.green}✓${colors.reset} Extraction completed in ${colors.bold}${formatTime(extractTime)}${colors.reset}`,
    );
    console.log('');

    // Analyze results
    console.log('─'.repeat(70));
    console.log(`${colors.cyan}${colors.bold}Results${colors.reset}`);
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
          try {
            const blob = await bs.getBlob(meta.blobId);
            totalBlobSize += blob.content.length;
          } catch (err) {
            // Blob might not be found in some cases
          }
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
    console.log(`    Total size: ${formatBytes(totalBlobSize)}`);
    console.log(`    Avg size: ${formatBytes(totalBlobSize / blobCount)}`);
    console.log('');

    // Performance metrics
    console.log('─'.repeat(70));
    console.log(`${colors.cyan}${colors.bold}Performance${colors.reset}`);
    console.log('─'.repeat(70));

    const docsPerSec = count / (extractTime / 1000);
    const bytesPerSec = stats.size / (extractTime / 1000);
    const timePerDoc = extractTime / count;

    console.log(`  Total time: ${formatTime(extractTime)}`);
    console.log(`  Documents: ${formatNumber(count)}`);
    console.log(`  Data size: ${formatBytes(stats.size)}`);
    console.log('');
    console.log(`  Throughput:`);
    console.log(`    ${formatNumber(Math.round(docsPerSec))} docs/sec`);
    console.log(`    ${formatBytes(bytesPerSec)}/sec`);
    console.log(`    ${timePerDoc.toFixed(3)}ms per document`);
    console.log('');

    console.log(`  Storage Efficiency:`);
    console.log(`    Original: ${formatBytes(stats.size)}`);
    console.log(`    Blobs: ${formatBytes(totalBlobSize)}`);
    const ratio = (totalBlobSize / stats.size) * 100;
    console.log(`    Ratio: ${ratio.toFixed(1)}%`);
    console.log('');

    // Extrapolate to full dataset
    const fullCount = 552321;
    const estimatedTime = (extractTime / count) * fullCount;
    const estimatedSize = (totalBlobSize / count) * fullCount;

    console.log('─'.repeat(70));
    console.log(
      `${colors.cyan}${colors.bold}Extrapolation to Full Dataset (552k docs)${colors.reset}`,
    );
    console.log('─'.repeat(70));
    console.log(`  Estimated time: ${formatTime(estimatedTime)}`);
    console.log(`  Estimated blob storage: ${formatBytes(estimatedSize)}`);
    console.log(
      `  Estimated tree nodes: ${formatNumber(fullCount + 2)} (approx)`,
    );
    console.log('');

    // Summary
    console.log('═'.repeat(70));
    console.log(`${colors.bold}${colors.green}Summary${colors.reset}`);
    console.log('═'.repeat(70));
    console.log(
      `  ✓ Extracted ${formatNumber(count)} documents in ${formatTime(extractTime)}`,
    );
    console.log(
      `  ✓ Throughput: ${formatNumber(Math.round(docsPerSec))} docs/sec`,
    );
    console.log(`  ✓ ${timePerDoc.toFixed(3)}ms per document`);
    console.log(`  ✓ Blob storage: ${formatBytes(totalBlobSize)}`);
    console.log('');
    console.log(
      `  ${colors.yellow}Note:${colors.reset} Full 552k collection would take ~${formatTime(estimatedTime)}`,
    );
    console.log(
      `  ${colors.yellow}Note:${colors.reset} And require ~${formatBytes(estimatedSize)} blob storage`,
    );
    console.log('═'.repeat(70) + '\n');

    // Cleanup
    console.log(`${colors.blue}ℹ${colors.reset} Cleaning up test database...`);
    await testDb.dropDatabase();
    console.log(`${colors.green}✓${colors.reset} Cleanup complete\n`);
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
