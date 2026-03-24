#!/usr/bin/env tsx
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * E2E test for TablesCfgTable storage and retrieval
 * Tests the complete RLJSON architecture implementation
 */

import { MongoClient } from 'mongodb';
import { BsMem } from '@rljson/bs';
import { MongoScanner } from '../../src/mongo-scanner.ts';
import { SYNC_OPS_TABLE_CFG } from '../../src/watch-changes.ts';
import type { TablesCfgTable } from '@rljson/rljson';

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/?directConnection=true';
const DB_NAME = 'test_tablecfg';

async function main() {
  console.log('🧪 Testing TablesCfgTable Storage & Retrieval\n');
  
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  
  try {
    const db = client.db(DB_NAME);
    
    // Setup: Create test collections with data
    await db.dropDatabase();
    
    const users = db.collection('users');
    await users.insertMany([
      { name: 'Alice', age: 30, email: 'alice@example.com' },
      { name: 'Bob', age: 25, email: 'bob@example.com' },
      { name: 'Charlie', age: 35, email: 'charlie@example.com' },
    ]);
    
    const products = db.collection('products');
    await products.insertMany([
      { name: 'Laptop', price: 999.99, inStock: true },
      { name: 'Mouse', price: 29.99, inStock: true },
      { name: 'Keyboard', price: 79.99, inStock: false },
    ]);
    
    console.log('✅ Created test collections: users (3 docs), products (3 docs)\n');
    
    // Test 1: Scan and create TablesCfgTable
    console.log('📋 Test 1: Scan collections and create TablesCfgTable');
    const bs = new BsMem();
    const scanner = new MongoScanner(db, { bs });
    
    const tree = await scanner.scan();
    console.log(`  ✓ Scanned database, root hash: ${tree.rootHash.substring(0, 16)}...`);
    
    const rootTree = scanner.getRootTree();
    if (!rootTree) {
      throw new Error('Root tree is null');
    }
    
    const rootMeta = rootTree.meta as any;
    const tableCfgsTableBlobId = rootMeta.tableCfgsTableBlobId;
    
    if (!tableCfgsTableBlobId) {
      throw new Error('❌ tableCfgsTableBlobId not found in root metadata');
    }
    console.log(`  ✓ TablesCfgTable blob ID: ${tableCfgsTableBlobId.substring(0, 16)}...`);
    
    // Test 2: Load TablesCfgTable from blob
    console.log('\n📥 Test 2: Load TablesCfgTable from blob storage');
    const tableCfgsTable = await scanner.loadTablesCfgTable(tableCfgsTableBlobId);
    
    console.log(`  ✓ Loaded TablesCfgTable with ${tableCfgsTable._data.length} schemas`);
    
    // Test 3: Verify all expected TableCfgs are present
    console.log('\n🔍 Test 3: Verify TableCfg schemas');
    const expectedKeys = ['users', 'products', 'sync_ops'];
    
    for (const key of expectedKeys) {
      const tableCfg = tableCfgsTable._data.find((cfg) => cfg.key === key);
      if (!tableCfg) {
        throw new Error(`❌ TableCfg for '${key}' not found`);
      }
      console.log(`  ✓ Found TableCfg for '${key}' (${tableCfg.columns.length} columns)`);
      console.log(`    Hash: ${tableCfg._hash || 'N/A'}`);
    }
    
    // Test 4: Verify sync_ops TableCfg is included
    console.log('\n🔄 Test 4: Verify sync_ops schema is included');
    const syncOpsFromTable = tableCfgsTable._data.find((cfg) => cfg.key === 'sync_ops');
    
    if (!syncOpsFromTable) {
      throw new Error('❌ sync_ops TableCfg not found in TablesCfgTable');
    }
    
    if (syncOpsFromTable._hash !== SYNC_OPS_TABLE_CFG._hash) {
      throw new Error(`❌ sync_ops hash mismatch: ${syncOpsFromTable._hash} !== ${SYNC_OPS_TABLE_CFG._hash}`);
    }
    
    console.log(`  ✓ sync_ops TableCfg matches expected hash: ${syncOpsFromTable._hash?.substring(0, 16)}...`);
    console.log(`  ✓ sync_ops has ${syncOpsFromTable.columns.length} columns`);
    
    // Test 5: Verify schema columns for users
    console.log('\n📊 Test 5: Verify users schema columns');
    const usersTableCfg = tableCfgsTable._data.find((cfg) => cfg.key === 'users');
    
    if (!usersTableCfg) {
      throw new Error('❌ users TableCfg not found');
    }
    
    const expectedUserColumns = ['_hash', 'name', 'age', 'email'];
    const actualColumns = usersTableCfg.columns.map((col) => col.key);
    
    for (const colKey of expectedUserColumns) {
      if (!actualColumns.includes(colKey)) {
        throw new Error(`❌ Expected column '${colKey}' not found in users schema`);
      }
      const col = usersTableCfg.columns.find((c) => c.key === colKey);
      console.log(`  ✓ Column '${colKey}' (type: ${col!.type})`);
    }
    
    // Test 6: Test getTableCfgByHash method
    console.log('\n🔎 Test 6: Test getTableCfgByHash retrieval');
    const usersHash = usersTableCfg._hash as string;
    const retrievedCfg = scanner.getTableCfgByHash(tableCfgsTable, usersHash);
    
    if (!retrievedCfg) {
      throw new Error('❌ getTableCfgByHash failed to retrieve users TableCfg');
    }
    
    if (retrievedCfg.key !== 'users') {
      throw new Error(`❌ Retrieved wrong TableCfg: ${retrievedCfg.key} !== users`);
    }
    
    console.log(`  ✓ Retrieved TableCfg by hash: ${retrievedCfg.key}`);
    console.log(`  ✓ Hash lookup working correctly`);
    
    // Test 7: Verify ComponentsTable references correct TableCfg
    console.log('\n🔗 Test 7: Verify ComponentsTable references');
    const collectionTrees = Array.from(tree.trees.values()).filter(
      (t: any) => t.meta?.type === 'collection'
    );
    
    for (const collTree of collectionTrees) {
      const collMeta = collTree.meta as any;
      const collName = collMeta.name;
      const tableCfgHash = collMeta.tableCfgHash;
      const componentsBlobId = collMeta.componentsBlobId;
      
      if (!tableCfgHash || !componentsBlobId) {
        console.log(`  ⚠ Skipping ${collName}: missing metadata`);
        continue;
      }
      
      // Verify the tableCfgHash exists in TablesCfgTable
      const referencedCfg = scanner.getTableCfgByHash(tableCfgsTable, tableCfgHash);
      
      if (!referencedCfg) {
        throw new Error(`❌ ComponentsTable for '${collName}' references unknown TableCfg hash: ${tableCfgHash}`);
      }
      
      // Load ComponentsTable and verify it has the same _tableCfg hash
      const componentsTable = await scanner.getComponentsTable(componentsBlobId);
      
      if (componentsTable._tableCfg !== tableCfgHash) {
        throw new Error(`❌ ComponentsTable._tableCfg mismatch for ${collName}`);
      }
      
      console.log(`  ✓ Collection '${collName}' ComponentsTable references correct schema`);
      console.log(`    TableCfg: ${referencedCfg.key} (${referencedCfg.columns.length} columns)`);
      console.log(`    Data rows: ${componentsTable._data.length}`);
    }
    
    // Test 8: Test round-trip: save and reload
    console.log('\n🔄 Test 8: Test TablesCfgTable round-trip');
    const newBlobId = await scanner.saveTablesCfgTable(tableCfgsTable);
    const reloadedTable = await scanner.loadTablesCfgTable(newBlobId);
    
    if (reloadedTable._data.length !== tableCfgsTable._data.length) {
      throw new Error('❌ Reloaded TablesCfgTable has different number of schemas');
    }
    
    console.log(`  ✓ Saved and reloaded TablesCfgTable successfully`);
    console.log(`  ✓ Schema count matches: ${reloadedTable._data.length}`);
    
    // Test 9: Verify TableCfg structure matches RLJSON spec
    console.log('\n📋 Test 9: Verify RLJSON TableCfg structure compliance');
    for (const tableCfg of tableCfgsTable._data) {
      // Check required fields
      if (!tableCfg.key || typeof tableCfg.key !== 'string') {
        throw new Error(`❌ TableCfg missing or invalid 'key': ${tableCfg.key}`);
      }
      
      if (!tableCfg.type || typeof tableCfg.type !== 'string') {
        throw new Error(`❌ TableCfg '${tableCfg.key}' missing or invalid 'type'`);
      }
      
      if (!Array.isArray(tableCfg.columns)) {
        throw new Error(`❌ TableCfg '${tableCfg.key}' missing or invalid 'columns'`);
      }
      
      if (typeof tableCfg.isHead !== 'boolean') {
        throw new Error(`❌ TableCfg '${tableCfg.key}' missing or invalid 'isHead'`);
      }
      
      if (typeof tableCfg.isRoot !== 'boolean') {
        throw new Error(`❌ TableCfg '${tableCfg.key}' missing or invalid 'isRoot'`);
      }
      
      if (typeof tableCfg.isShared !== 'boolean') {
        throw new Error(`❌ TableCfg '${tableCfg.key}' missing or invalid 'isShared'`);
      }
      
      // Check _hash field
      if (!tableCfg._hash || typeof tableCfg._hash !== 'string') {
        throw new Error(`❌ TableCfg '${tableCfg.key}' missing or invalid '_hash'`);
      }
      
      console.log(`  ✓ TableCfg '${tableCfg.key}' structure compliant`);
    }
    
    // Summary
    console.log('\n' + '='.repeat(60));
    console.log('✅ All TablesCfgTable tests passed!');
    console.log('='.repeat(60));
    console.log(`\nSummary:`);
    console.log(`  • Total schemas in TablesCfgTable: ${tableCfgsTable._data.length}`);
    console.log(`  • Collection schemas: ${tableCfgsTable._data.filter(c => c.key !== 'sync_ops').length}`);
    console.log(`  • System schemas (sync_ops): 1`);
    console.log(`  • All schemas stored in blob storage: ✓`);
    console.log(`  • All schemas retrievable by hash: ✓`);
    console.log(`  • ComponentsTable references valid: ✓`);
    console.log(`  • RLJSON structure compliance: ✓`);
    
  } catch (error) {
    console.error('\n❌ Test failed:');
    console.error(error);
    process.exit(1);
  } finally {
    await client.close();
  }
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
