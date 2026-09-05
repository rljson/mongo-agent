#!/usr/bin/env tsx

/**
 * Load fixture data from old repo into Docker MongoDB
 * This allows testing with realistic 1000-article dataset
 */

import { MongoClient } from 'mongodb';
import fs from 'node:fs';


const MONGO_A_URI =
  process.env.MONGO_A_URI ||
  'mongodb://localhost:27017/syncdb?directConnection=true';
const FIXTURE_PATH =
  process.env.FIXTURE_PATH ||
  '/Users/hermanmertke/mongodbsync/test/unit/fixtures/articles-1000.json';

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
} as const;

function log(color: string, symbol: string, message: string): void {
  console.log(`${color}${symbol}${colors.reset} ${message}`);
}

async function loadFixtures(): Promise<void> {
  console.log('\n🌱 Loading fixture data from old repo...\n');

  // Check if fixture file exists
  if (!fs.existsSync(FIXTURE_PATH)) {
    log(colors.red, '✗', `Fixture file not found: ${FIXTURE_PATH}`);
    log(
      colors.yellow,
      '💡',
      'Use seed-testdata.sh to generate test data instead',
    );
    process.exit(1);
  }

  log(colors.blue, 'ℹ', `Reading fixtures from: ${FIXTURE_PATH}`);
  const articles = JSON.parse(fs.readFileSync(FIXTURE_PATH, 'utf8')) as Record<
    string,
    unknown
  >[];
  log(
    colors.green,
    '✓',
    `Loaded ${articles.length} articles from fixture file`,
  );

  // Connect to MongoDB A
  log(colors.blue, 'ℹ', 'Connecting to MongoDB A...');
  const client = new MongoClient(MONGO_A_URI);

  try {
    await client.connect();
    log(colors.green, '✓', 'Connected to MongoDB A');

    const db = client.db('syncdb');
    const collection = db.collection('articles');

    // Clear existing test data
    const deleteResult = await collection.deleteMany({ testData: true });
    if (deleteResult.deletedCount > 0) {
      log(
        colors.blue,
        'ℹ',
        `Deleted ${deleteResult.deletedCount} existing test documents`,
      );
    }

    // Mark articles as test data and insert
    const articlesWithTestFlag = articles.map((article) => ({
      ...article,
      testData: true,
      importedFrom: 'fixtures',
      importedAt: new Date(),
    }));

    log(
      colors.blue,
      'ℹ',
      `Inserting ${articlesWithTestFlag.length} articles...`,
    );
    const result = await collection.insertMany(articlesWithTestFlag, {
      ordered: false,
    });
    log(
      colors.green,
      '✓',
      `Inserted ${Object.keys(result.insertedIds).length} articles`,
    );

    // Show stats
    const total = await collection.countDocuments();
    console.log(`\n📊 Database statistics:`);
    console.log(`   Total articles: ${total}`);
    console.log(
      `   Test articles:  ${await collection.countDocuments({ testData: true })}`,
    );

    console.log(
      `\n💡 Tip: Wait a few seconds for sync to MongoDB B, then run:`,
    );
    console.log(`   tsx test-integrity-hash.ts`);
    console.log(`   tsx test-state-hash.ts`);
    console.log(`   ./test-sync.sh`);
  } catch (error) {
    const err = error as Error;
    log(colors.red, '✗', `Error: ${err.message}`);
    process.exit(1);
  } finally {
    await client.close();
  }
}

loadFixtures().catch(console.error);
