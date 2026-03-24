#!/usr/bin/env tsx

/**
 * Test Updated MongoScanner with ComponentsTable Structure
 * Verifies that MongoScanner now stores collections as ComponentsTables
 */

import { MongoClient } from 'mongodb';

import { MongoScanner } from '../../src/mongo-scanner';

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
    `\n${colors.cyan}${colors.bold}Testing Updated MongoScanner with ComponentsTable${colors.reset}\n`,
  );

  const client = new MongoClient(
    'mongodb://localhost:27017/?directConnection=true',
  );

  try {
    console.log(`${colors.yellow}→${colors.reset} Connecting to MongoDB...`);
    await client.connect();
    console.log(`${colors.green}✓${colors.reset} Connected\n`);

    const db = client.db('test_scanner_components');
    const collection = db.collection('products');

    // Clean and insert test data
    console.log(`${colors.yellow}→${colors.reset} Inserting test data...`);
    await collection.deleteMany({});
    await collection.insertMany([
      { _id: 'p1', name: 'Laptop', price: 999.99, stock: 15, available: true },
      { _id: 'p2', name: 'Mouse', price: 29.99, stock: 50, available: true },
      { _id: 'p3', name: 'Keyboard', price: 79.99, stock: 0, available: false },
      { _id: 'p4', name: 'Monitor', price: 299.99, stock: 8, available: true },
    ]);
    console.log(`${colors.green}✓${colors.reset} Inserted 4 products\n`);

    // Test 1: Scan with MongoScanner
    console.log(`${colors.cyan}Test 1: MongoScanner.scan()${colors.reset}`);
    console.log('─'.repeat(60));

    const scanner = new MongoScanner(db);
    const tree = await scanner.scan();

    console.log(`Tree Root Hash: ${tree.rootHash}`);
    console.log(`Total Tree Nodes: ${tree.trees.size}`);

    // Should have:
    // 1. Root node (database)
    // 2. Collection node (products) - NO per-document nodes!
    console.log(`\nExpected structure:`);
    console.log(`  - 1 database node`);
    console.log(`  - 1 collection node (no per-document nodes)`);
    console.log(`\nActual: ${tree.trees.size} nodes total`);

    if (tree.trees.size === 2) {
      console.log(
        `${colors.green}✓${colors.reset} Correct node count (database + collection only)\n`,
      );
    } else {
      console.log(
        `${colors.yellow}⚠${colors.reset} Expected 2 nodes, got ${tree.trees.size}\n`,
      );
    }

    // Test 2: Inspect Collection Node
    console.log(
      `${colors.cyan}Test 2: Collection Node Structure${colors.reset}`,
    );
    console.log('─'.repeat(60));

    const rootNode = tree.trees.get(tree.rootHash)!;
    console.log(`Root node ID: ${rootNode.id}`);
    console.log(`Root node type: ${(rootNode.meta as any)?.type}`);
    console.log(`Root node children: ${rootNode.children?.length || 0}`);

    const collNodeHash = rootNode.children![0];
    const collNode = tree.trees.get(collNodeHash)!;
    const collMeta = collNode.meta as any;

    console.log(`\nCollection node:`);
    console.log(`  ID: ${collNode.id}`);
    console.log(`  Type: ${collMeta.type}`);
    console.log(`  Name: ${collMeta.name}`);
    console.log(`  Document count: ${collMeta.docCount}`);
    console.log(`  Is parent: ${collNode.isParent}`);
    console.log(`  Has children: ${collNode.children ? 'YES' : 'NO'}`);

    // Check new ComponentsTable fields
    if (collMeta.tableCfgHash) {
      console.log(
        `  ${colors.green}✓${colors.reset} tableCfgHash: ${collMeta.tableCfgHash}`,
      );
    } else {
      console.log(`  ${colors.yellow}⚠${colors.reset} tableCfgHash: missing`);
    }

    if (collMeta.componentsBlobId) {
      console.log(
        `  ${colors.green}✓${colors.reset} componentsBlobId: ${collMeta.componentsBlobId}`,
      );
    } else {
      console.log(
        `  ${colors.yellow}⚠${colors.reset} componentsBlobId: missing`,
      );
    }

    console.log();

    // Test 3: Retrieve ComponentsTable
    console.log(
      `${colors.cyan}Test 3: Retrieve ComponentsTable from Blob${colors.reset}`,
    );
    console.log('─'.repeat(60));

    const componentsTable = await scanner.getComponentsTable(
      collMeta.componentsBlobId,
    );

    console.log(`ComponentsTable structure:`);
    console.log(`  _type: ${componentsTable._type}`);
    console.log(`  _tableCfg: ${componentsTable._tableCfg}`);
    console.log(`  _hash: ${componentsTable._hash}`);
    console.log(`  _data length: ${componentsTable._data.length}`);

    console.log(`\nFirst row:`);
    const firstRow = componentsTable._data[0];
    console.log(JSON.stringify(firstRow, null, 2));

    if (componentsTable._type === 'components') {
      console.log(`${colors.green}✓${colors.reset} Correct _type: components`);
    }

    if (componentsTable._tableCfg === collMeta.tableCfgHash) {
      console.log(
        `${colors.green}✓${colors.reset} _tableCfg matches tableCfgHash`,
      );
    }

    if (componentsTable._data.length === 4) {
      console.log(`${colors.green}✓${colors.reset} Correct number of rows: 4`);
    }

    // Check all rows are hashed
    let allHashed = true;
    for (const row of componentsTable._data) {
      if (!row._hash) {
        allHashed = false;
        break;
      }
    }

    if (allHashed) {
      console.log(`${colors.green}✓${colors.reset} All rows have _hash`);
    } else {
      console.log(`${colors.yellow}⚠${colors.reset} Some rows missing _hash`);
    }

    console.log();

    // Test 4: Retrieve TableCfg
    console.log(`${colors.cyan}Test 4: Retrieve TableCfg${colors.reset}`);
    console.log('─'.repeat(60));

    const tableCfg = scanner.getTableCfg('products');

    if (tableCfg) {
      console.log(`TableCfg found:`);
      console.log(`  key: ${tableCfg.key}`);
      console.log(`  type: ${tableCfg.type}`);
      console.log(`  _hash: ${tableCfg._hash}`);
      console.log(`  columns (${tableCfg.columns.length}):`);

      for (const col of tableCfg.columns) {
        console.log(
          `    ${col.key.padEnd(12)} ${col.type.padEnd(10)} "${col.titleLong}"`,
        );
      }

      console.log(
        `${colors.green}✓${colors.reset} TableCfg retrieved successfully`,
      );
    } else {
      console.log(`${colors.yellow}⚠${colors.reset} TableCfg not found`);
    }

    console.log();

    // Summary
    console.log(`${colors.cyan}${colors.bold}Summary${colors.reset}`);
    console.log('═'.repeat(60));

    const checks = [
      tree.trees.size === 2,
      !collNode.isParent,
      !collNode.children,
      !!collMeta.tableCfgHash,
      !!collMeta.componentsBlobId,
      componentsTable._type === 'components',
      componentsTable._tableCfg === collMeta.tableCfgHash,
      componentsTable._data.length === 4,
      allHashed,
      !!tableCfg,
    ];

    const passed = checks.filter(Boolean).length;
    const total = checks.length;

    if (passed === total) {
      console.log(
        `${colors.green}${colors.bold}✓ ALL ${total} CHECKS PASSED!${colors.reset}`,
      );
      console.log(
        `\n${colors.magenta}MongoScanner now correctly uses ComponentsTable structure:${colors.reset}`,
      );
      console.log(`  ✓ Collections stored as ComponentsTable blobs`);
      console.log(`  ✓ No per-document tree nodes`);
      console.log(`  ✓ TableCfg discovered and cached`);
      console.log(`  ✓ All rows properly hashed`);
      console.log(`  ✓ Tree structure simplified (database → collection)`);
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
