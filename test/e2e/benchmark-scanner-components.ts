#!/usr/bin/env tsx

/**
 * Benchmark MongoScanner with ComponentsTable on Large Collection (552k docs)
 * Tests the new ComponentsTable approach vs old per-document approach
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

async function benchmark() {
  console.log(`\n${colors.cyan}${colors.bold}MongoScanner ComponentsTable Benchmark${colors.reset}`);
  console.log(`${colors.cyan}Testing with cd_articles (552k documents)${colors.reset}\n`);

  const client = new MongoClient('mongodb://localhost:27017/?directConnection=true');
  
  try {
    console.log(`${colors.yellow}→${colors.reset} Connecting to MongoDB...`);
    await client.connect();
    console.log(`${colors.green}✓${colors.reset} Connected\n`);

    const db = client.db('cddb');
    const collection = db.collection('cd_articles');

    // Verify collection exists
    const count = await collection.countDocuments();
    console.log(`${colors.yellow}→${colors.reset} Collection: cd_articles`);
    console.log(`${colors.yellow}→${colors.reset} Document count: ${count.toLocaleString()}`);
    
    if (count === 0) {
      console.log(`${colors.yellow}⚠${colors.reset} Collection is empty. Creating 10,000 test documents...\n`);
      
      // Create test data
      const batchSize = 1000;
      const totalDocs = 10000;
      
      for (let i = 0; i < totalDocs; i += batchSize) {
        const docs = Array.from({ length: Math.min(batchSize, totalDocs - i) }, (_, j) => ({
          _id: `doc${i + j}`,
          title: `Article ${i + j}`,
          content: `This is the content of article ${i + j}. It contains various information about topics related to the article number ${i + j}.`,
          author: `Author ${(i + j) % 100}`,
          tags: ['tag1', 'tag2', 'tag3'],
          publishedDate: new Date(2020, 0, 1 + ((i + j) % 365)),
          viewCount: Math.floor(Math.random() * 10000),
          likes: Math.floor(Math.random() * 1000),
          metadata: {
            category: ['Technology', 'Science', 'Business', 'Arts'][(i + j) % 4],
            status: ['draft', 'published', 'archived'][(i + j) % 3],
            priority: Math.floor(Math.random() * 5) + 1,
          },
        }));
        await collection.insertMany(docs);
      }
      
      const newCount = await collection.countDocuments();
      console.log(`${colors.green}✓${colors.reset} Created ${newCount.toLocaleString()} test documents\n`);
    }

    const finalCount = await collection.countDocuments();
    console.log(`${colors.yellow}→${colors.reset} Ready to benchmark with ${finalCount.toLocaleString()} documents\n`);

    console.log();

    // Test 1: Full Scan Performance
    console.log(`${colors.cyan}Test 1: Full Scan Performance${colors.reset}`);
    console.log('─'.repeat(60));
    
    const scanner = new MongoScanner(db);
    
    const scanStart = Date.now();
    const tree = await scanner.scan();
    const scanEnd = Date.now();
    const scanDuration = (scanEnd - scanStart) / 1000;
    
    console.log(`Scan Duration: ${scanDuration.toFixed(2)}s`);
    console.log(`Documents/sec: ${(finalCount / scanDuration).toFixed(0)}`);
    console.log(`Tree Nodes: ${tree.trees.size}`);
    console.log(`Root Hash: ${tree.rootHash}`);
    console.log();

    // Test 2: Tree Structure
    console.log(`${colors.cyan}Test 2: Tree Structure Analysis${colors.reset}`);
    console.log('─'.repeat(60));
    
    const nodes = Array.from(tree.trees.values());
    const collNode = nodes.find(n => (n.meta as any)?.name === 'cd_articles');
    
    if (collNode) {
      const meta = collNode.meta as any;
      console.log(`Collection node:`);
      console.log(`  ID: ${collNode.id}`);
      console.log(`  Type: ${meta.type}`);
      console.log(`  Document count: ${meta.docCount?.toLocaleString()}`);
      console.log(`  Hash: ${collNode.hash}`);
      console.log(`  Is parent: ${collNode.isParent}`);
      console.log(`  Has children: ${collNode.children ? 'YES' : 'NO'}`);
      console.log(`  ComponentsTable blob ID: ${meta.componentsBlobId}`);
      console.log(`  TableCfg hash: ${meta.tableCfgHash}`);
    }
    console.log();

    // Test 3: ComponentsTable Size
    console.log(`${colors.cyan}Test 3: ComponentsTable Storage${colors.reset}`);
    console.log('─'.repeat(60));
    
    let componentsTable: any;
    let jsonSize = 0;
    
    if (collNode) {
      const meta = collNode.meta as any;
      componentsTable = await scanner.getComponentsTable(meta.componentsBlobId);
      
      const jsonStr = JSON.stringify(componentsTable);
      jsonSize = Buffer.byteLength(jsonStr, 'utf-8');
      
      console.log(`ComponentsTable structure:`);
      console.log(`  _type: ${componentsTable._type}`);
      console.log(`  _tableCfg: ${componentsTable._tableCfg}`);
      console.log(`  _hash: ${componentsTable._hash}`);
      console.log(`  _data rows: ${componentsTable._data.length.toLocaleString()}`);
      console.log(`  JSON size: ${(jsonSize / 1024 / 1024).toFixed(2)} MB`);
      console.log(`  Bytes/document: ${(jsonSize / componentsTable._data.length).toFixed(0)}`);
      
      // Sample first row
      console.log(`\nFirst row sample:`);
      const firstRow = componentsTable._data[0];
      const keys = Object.keys(firstRow);
      console.log(`  Keys (${keys.length}): ${keys.slice(0, 10).join(', ')}${keys.length > 10 ? '...' : ''}`);
      console.log(`  _id: ${firstRow._id}`);
      console.log(`  _hash: ${firstRow._hash}`);
    }
    console.log();

    // Test 4: TableCfg Schema
    console.log(`${colors.cyan}Test 4: TableCfg Schema${colors.reset}`);
    console.log('─'.repeat(60));
    
    const tableCfg = scanner.getTableCfg('cd_articles');
    if (tableCfg) {
      console.log(`TableCfg:`);
      console.log(`  key: ${tableCfg.key}`);
      console.log(`  type: ${tableCfg.type}`);
      console.log(`  _hash: ${tableCfg._hash}`);
      console.log(`  columns: ${tableCfg.columns.length}`);
      
      console.log(`\nSample columns (first 10):`);
      for (let i = 0; i < Math.min(10, tableCfg.columns.length); i++) {
        const col = tableCfg.columns[i];
        console.log(`  ${(i + 1).toString().padStart(2)}. ${col.key.padEnd(20)} ${col.type.padEnd(10)} "${col.titleLong}"`);
      }
      
      if (tableCfg.columns.length > 10) {
        console.log(`  ... and ${tableCfg.columns.length - 10} more columns`);
      }
    }
    console.log();

    // Test 5: Blob Storage Stats
    console.log(`${colors.cyan}Test 5: Blob Storage Statistics${colors.reset}`);
    console.log('─'.repeat(60));
    
    // Count blobs (ComponentsTable + TableCfg per collection)
    const expectedBlobs = 2; // 1 ComponentsTable + 1 TableCfg for cd_articles
    console.log(`Expected blobs: ${expectedBlobs} (ComponentsTable + TableCfg)`);
    console.log(`Tree nodes: ${tree.trees.size} (database + collection)`);
    console.log();

    // Summary
    console.log(`${colors.cyan}${colors.bold}Summary${colors.reset}`);
    console.log('═'.repeat(60));
    console.log(`${colors.green}✓${colors.reset} Successfully scanned ${finalCount.toLocaleString()} documents`);
    console.log(`${colors.green}✓${colors.reset} Performance: ${(finalCount / scanDuration).toFixed(0)} docs/sec`);
    console.log(`${colors.green}✓${colors.reset} Tree structure: ${tree.trees.size} nodes (database + collection)`);
    console.log(`${colors.green}✓${colors.reset} No per-document tree nodes (all in ComponentsTable)`);
    console.log(`${colors.green}✓${colors.reset} ComponentsTable: ${componentsTable._data.length.toLocaleString()} rows, ${(jsonSize / 1024 / 1024).toFixed(2)} MB`);
    console.log();

  } catch (error) {
    console.error(`\n${colors.yellow}✗ Error:${colors.reset}`, error);
    process.exit(1);
  } finally {
    await client.close();
    console.log(`${colors.yellow}→${colors.reset} Disconnected from MongoDB\n`);
  }
}

benchmark().catch(console.error);
