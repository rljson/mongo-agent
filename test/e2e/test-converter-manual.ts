#!/usr/bin/env tsx

/**
 * Manual Test for MongoToRljsonConverter
 * Run this to verify the converter works correctly
 */

import { MongoClient } from 'mongodb';
import { MongoToRljsonConverter } from '../../src/mongo-to-rljson-converter';

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  cyan: '\x1b[36m',
  yellow: '\x1b[33m',
  bold: '\x1b[1m',
} as const;

async function test() {
  console.log(`\n${colors.cyan}${colors.bold}Testing MongoToRljsonConverter${colors.reset}\n`);

  const client = new MongoClient('mongodb://localhost:27017/?directConnection=true');
  
  try {
    console.log(`${colors.yellow}→${colors.reset} Connecting to MongoDB...`);
    await client.connect();
    console.log(`${colors.green}✓${colors.reset} Connected\n`);

    const db = client.db('test_converter_manual');
    const collection = db.collection('users');

    // Clean and insert test data
    console.log(`${colors.yellow}→${colors.reset} Inserting test data...`);
    await collection.deleteMany({});
    await collection.insertMany([
      { _id: '1', name: 'Alice', age: 30, email: 'alice@example.com', active: true },
      { _id: '2', name: 'Bob', age: 25, email: 'bob@example.com', active: false },
      { _id: '3', name: 'Charlie', age: 35, email: 'charlie@example.com', active: true },
    ]);
    console.log(`${colors.green}✓${colors.reset} Inserted 3 documents\n`);

    const converter = new MongoToRljsonConverter();

    // Test 1: Schema Discovery
    console.log(`${colors.cyan}Test 1: Schema Discovery${colors.reset}`);
    console.log('─'.repeat(50));
    
    const tableCfg = await converter.discoverSchema(collection);
    
    console.log(`Table Key: ${tableCfg.key}`);
    console.log(`Table Type: ${tableCfg.type}`);
    console.log(`Table Hash: ${tableCfg._hash}`);
    console.log(`\nColumns (${tableCfg.columns.length}):`);
    
    for (const col of tableCfg.columns) {
      console.log(`  ${col.key.padEnd(15)} ${col.type.padEnd(10)} "${col.titleLong}" / "${col.titleShort}"`);
    }
    console.log(`${colors.green}✓${colors.reset} Schema discovered\n`);

    // Test 2: Document Conversion
    console.log(`${colors.cyan}Test 2: Document Conversion${colors.reset}`);
    console.log('─'.repeat(50));
    
    const doc = await collection.findOne({ _id: '1' });
    const converted = converter.convertDocument(doc!, tableCfg);
    
    console.log(`Original document:`, doc);
    console.log(`\nConverted row:`);
    console.log(JSON.stringify(converted, null, 2));
    console.log(`${colors.green}✓${colors.reset} Document converted and hashed\n`);

    // Test 3: Collection Conversion
    console.log(`${colors.cyan}Test 3: Collection Conversion to ComponentsTable${colors.reset}`);
    console.log('─'.repeat(50));
    
    const componentsTable = await converter.convertCollection(collection, tableCfg);
    
    console.log(`ComponentsTable:`);
    console.log(`  _type: ${componentsTable._type}`);
    console.log(`  _tableCfg: ${componentsTable._tableCfg}`);
    console.log(`  _hash: ${componentsTable._hash}`);
    console.log(`  _data (${componentsTable._data.length} rows):`);
    
    for (const row of componentsTable._data) {
      console.log(`    - ${row._id}: ${row.name} (hash: ${row._hash.substring(0, 8)}...)`);
    }
    
    console.log(`${colors.green}✓${colors.reset} Collection converted to ComponentsTable\n`);

    // Verify structure
    console.log(`${colors.cyan}Verification${colors.reset}`);
    console.log('─'.repeat(50));
    
    let allValid = true;
    
    // Check all rows are hashed
    for (const row of componentsTable._data) {
      if (!row._hash) {
        console.log(`${colors.yellow}⚠${colors.reset} Row ${row._id} has no hash`);
        allValid = false;
      }
    }
    
    if (allValid) {
      console.log(`${colors.green}✓${colors.reset} All rows properly hashed`);
    }
    
    // Check table is hashed
    if (componentsTable._hash) {
      console.log(`${colors.green}✓${colors.reset} ComponentsTable properly hashed`);
    } else {
      console.log(`${colors.yellow}⚠${colors.reset} ComponentsTable has no hash`);
      allValid = false;
    }
    
    // Check tableCfg reference
    if (componentsTable._tableCfg === tableCfg._hash) {
      console.log(`${colors.green}✓${colors.reset} TableCfg reference correct`);
    } else {
      console.log(`${colors.yellow}⚠${colors.reset} TableCfg reference mismatch`);
      allValid = false;
    }
    
    console.log();
    if (allValid) {
      console.log(`${colors.green}${colors.bold}✓ ALL TESTS PASSED!${colors.reset}\n`);
    } else {
      console.log(`${colors.yellow}${colors.bold}⚠ SOME CHECKS FAILED${colors.reset}\n`);
    }

  } catch (error) {
    console.error(`\n${colors.yellow}✗ Error:${colors.reset}`, error);
    process.exit(1);
  } finally {
    await client.close();
    console.log(`${colors.yellow}→${colors.reset} Disconnected from MongoDB\n`);
  }
}

test().catch(console.error);
