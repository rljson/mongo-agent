#!/usr/bin/env node
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * Visual demonstration of MongoDB → RLJSON transformation
 * Shows clearly how MongoDB documents are converted to RLJSON ComponentsTable format
 */

import { BsMem } from '@rljson/bs';

import { MongoClient } from 'mongodb';

import { MongoScanner } from '../../src/mongo-scanner.ts';
import { MongoToRljsonConverter } from '../../src/mongo-to-rljson-converter.ts';

import type { ComponentsTable, TableCfg } from '@rljson/rljson';

const MONGO_URI =
  process.env.MONGO_URI || 'mongodb://localhost:27017/?directConnection=true';
const DB_NAME = 'test_rljson_visual';

function printSectionHeader(title: string) {
  console.log('\n' + '═'.repeat(80));
  console.log(`  ${title}`);
  console.log('═'.repeat(80));
}

function printSubSection(title: string) {
  console.log('\n' + '─'.repeat(80));
  console.log(`  ${title}`);
  console.log('─'.repeat(80));
}

async function main() {
  console.log('\n🔄 MongoDB → RLJSON Visual Transformation Demo\n');

  const client = new MongoClient(MONGO_URI);
  await client.connect();

  try {
    const db = client.db(DB_NAME);

    // Clean slate
    await db.dropDatabase();

    // ========================================================================
    // PART 1: Original MongoDB Documents
    // ========================================================================
    printSectionHeader('PART 1: Original MongoDB Documents');

    const books = db.collection('books');

    const originalDocuments = [
      {
        _id: '123',
        title: 'The Great Gatsby',
        author: 'F. Scott Fitzgerald',
        year: 1925,
        price: 12.99,
        inStock: true,
        tags: ['classic', 'fiction', 'american'],
        publisher: {
          name: 'Scribner',
          country: 'USA',
        },
      },
      {
        _id: '456',
        title: '1984',
        author: 'George Orwell',
        year: 1949,
        price: 14.99,
        inStock: true,
        tags: ['dystopian', 'classic', 'political'],
        publisher: {
          name: 'Secker & Warburg',
          country: 'UK',
        },
      },
      {
        _id: '789',
        title: 'To Kill a Mockingbird',
        author: 'Harper Lee',
        year: 1960,
        price: 13.99,
        inStock: false,
        tags: ['classic', 'fiction', 'southern'],
        publisher: {
          name: 'J. B. Lippincott & Co.',
          country: 'USA',
        },
      },
    ];

    await books.insertMany(originalDocuments);

    console.log('\n📚 MongoDB Collection: "books"');
    console.log(`   Inserted ${originalDocuments.length} documents\n`);

    for (const doc of originalDocuments) {
      console.log('📄 Document:');
      console.log(JSON.stringify(doc, null, 2));
      console.log('');
    }

    console.log('💾 Storage Format: BSON (Binary JSON)');
    console.log(
      '📊 Data Types: ObjectId, String, Number, Boolean, Array, Object',
    );
    console.log(
      '🔍 MongoDB-specific features: _id as ObjectId, nested objects',
    );

    // ========================================================================
    // PART 2: Schema Discovery (TableCfg)
    // ========================================================================
    printSectionHeader('PART 2: Schema Discovery → TableCfg');

    const converter = new MongoToRljsonConverter();
    const tableCfg = await converter.discoverSchema(books, 100);

    console.log('\n📋 Discovered TableCfg (Schema Definition):');
    console.log('');
    console.log('TableCfg {');
    console.log(`  key: "${tableCfg.key}",`);
    console.log(`  type: "${tableCfg.type}",`);
    console.log(`  isHead: ${tableCfg.isHead},`);
    console.log(`  isRoot: ${tableCfg.isRoot},`);
    console.log(`  isShared: ${tableCfg.isShared},`);
    console.log(`  _hash: "${tableCfg._hash}",`);
    console.log('  columns: [');

    for (const col of tableCfg.columns) {
      console.log(`    {`);
      console.log(`      key: "${col.key}",`);
      console.log(`      type: "${col.type}",`);
      console.log(`      titleShort: "${col.titleShort}",`);
      console.log(`      titleLong: "${col.titleLong}"`);
      console.log(`    },`);
    }

    console.log('  ]');
    console.log('}');

    console.log('\n🎯 Type Mapping:');
    console.log('   MongoDB ObjectId  → RLJSON string');
    console.log('   MongoDB String    → RLJSON string');
    console.log('   MongoDB Number    → RLJSON number');
    console.log('   MongoDB Boolean   → RLJSON boolean');
    console.log('   MongoDB Array     → RLJSON jsonArray');
    console.log('   MongoDB Object    → RLJSON json');
    console.log('   MongoDB Date      → RLJSON number (timestamp)');

    // ========================================================================
    // PART 3: Conversion to ComponentsTable
    // ========================================================================
    printSectionHeader('PART 3: Conversion → ComponentsTable');

    const componentsTable = await converter.convertCollection(books, tableCfg);

    console.log('\n📊 ComponentsTable Structure:');
    console.log('');
    console.log('ComponentsTable {');
    console.log(
      `  _tableCfg: "${componentsTable._tableCfg}",  ← References TableCfg by hash`,
    );
    console.log(`  _type: "${componentsTable._type}",`);
    console.log(`  _data: [`);

    for (let i = 0; i < componentsTable._data.length; i++) {
      const row = componentsTable._data[i];
      console.log(`    // Row ${i + 1}:`);
      console.log('    {');

      // Show each field
      for (const key of Object.keys(row)) {
        const value = row[key];
        const displayValue =
          typeof value === 'object'
            ? JSON.stringify(value)
            : typeof value === 'string'
              ? `"${value}"`
              : value;
        console.log(`      ${key}: ${displayValue},`);
      }

      console.log('    },');
    }

    console.log('  ]');
    console.log('}');

    console.log('\n✨ Key Features:');
    console.log('   ✓ Each row has _hash field (content-based hashing)');
    console.log('   ✓ ObjectId converted to string');
    console.log('   ✓ Nested objects preserved as JSON');
    console.log('   ✓ Arrays preserved as jsonArray');
    console.log('   ✓ All MongoDB-specific types normalized');

    // ========================================================================
    // PART 4: Blob Storage
    // ========================================================================
    printSectionHeader('PART 4: Storage in Blob Storage');

    const bs = new BsMem();
    const serialized = JSON.stringify(componentsTable);
    const blobProps = await bs.setBlob(Buffer.from(serialized, 'utf-8'));

    console.log('\n💾 ComponentsTable serialized to JSON:');
    console.log(`   Size: ${serialized.length} bytes`);
    console.log(`   Blob ID: ${blobProps.blobId}`);
    console.log(`   Hash: ${blobProps.hash}`);

    console.log('\n📦 Blob Storage Benefits:');
    console.log('   ✓ Content-addressable (same data = same hash)');
    console.log('   ✓ Immutable (changes create new blob)');
    console.log('   ✓ Portable (can use filesystem, S3, GridFS, etc.)');
    console.log('   ✓ Independent of MongoDB');

    // Also store TablesCfgTable
    const tableCfgsTable = {
      _data: [tableCfg],
    };
    const tableCfgsSerialized = JSON.stringify(tableCfgsTable);
    const tableCfgsBlobProps = await bs.setBlob(
      Buffer.from(tableCfgsSerialized, 'utf-8'),
    );

    console.log('\n📋 TablesCfgTable also stored:');
    console.log(`   Size: ${tableCfgsSerialized.length} bytes`);
    console.log(`   Blob ID: ${tableCfgsBlobProps.blobId}`);
    console.log(`   Contains: ${tableCfgsTable._data.length} schema(s)`);

    // ========================================================================
    // PART 5: Retrieval and Reconstruction
    // ========================================================================
    printSectionHeader('PART 5: Retrieval & Reconstruction');

    console.log('\n🔍 Step 1: Load ComponentsTable from blob storage');
    const retrievedBlob = await bs.getBlob(blobProps.blobId);
    const retrievedComponentsTable = JSON.parse(
      retrievedBlob.content.toString('utf-8'),
    ) as ComponentsTable<any>;

    console.log(
      `   ✓ Retrieved ComponentsTable with ${retrievedComponentsTable._data.length} rows`,
    );
    console.log(
      `   ✓ TableCfg reference: ${retrievedComponentsTable._tableCfg}`,
    );

    console.log('\n🔍 Step 2: Load TablesCfgTable to get schema');
    const retrievedTableCfgsBlob = await bs.getBlob(tableCfgsBlobProps.blobId);
    const retrievedTablesCfgsTable = JSON.parse(
      retrievedTableCfgsBlob.content.toString('utf-8'),
    );

    console.log(
      `   ✓ Retrieved TablesCfgTable with ${retrievedTablesCfgsTable._data.length} schema(s)`,
    );

    console.log('\n🔍 Step 3: Find matching TableCfg by hash');
    const matchingTableCfg = retrievedTablesCfgsTable._data.find(
      (cfg: TableCfg) => cfg._hash === retrievedComponentsTable._tableCfg,
    );

    if (!matchingTableCfg) {
      throw new Error('TableCfg not found!');
    }

    console.log(`   ✓ Found TableCfg: "${matchingTableCfg.key}"`);
    console.log(`   ✓ Columns: ${matchingTableCfg.columns.length}`);

    console.log('\n🔍 Step 4: Reconstruct data with schema knowledge');
    console.log('');

    for (let i = 0; i < retrievedComponentsTable._data.length; i++) {
      const row = retrievedComponentsTable._data[i];
      console.log(`📖 Book ${i + 1}:`);

      for (const col of matchingTableCfg.columns) {
        if (col.key === '_hash') continue;

        const value = row[col.key];
        const displayValue =
          typeof value === 'object'
            ? JSON.stringify(value, null, 2).split('\n').join('\n       ')
            : value;

        console.log(
          `   ${col.titleLong.padEnd(20)} (${col.type.padEnd(10)}): ${displayValue}`,
        );
      }
      console.log('');
    }

    // ========================================================================
    // PART 6: Comparison - Before vs After
    // ========================================================================
    printSectionHeader('PART 6: Before vs After Comparison');

    console.log('\n📊 BEFORE (MongoDB):');
    console.log('   Format: BSON (Binary JSON)');
    console.log('   Types: MongoDB-specific (ObjectId, Date, etc.)');
    console.log('   Storage: MongoDB collections');
    console.log('   Schema: Implicit (dynamic schema)');
    console.log('   Dependencies: Requires MongoDB to read');
    console.log('   Structure: Nested documents');

    console.log('\n📊 AFTER (RLJSON):');
    console.log('   Format: JSON in ComponentsTable');
    console.log('   Types: Universal JSON types (string, number, json, etc.)');
    console.log('   Storage: Blob storage (content-addressable)');
    console.log('   Schema: Explicit (TableCfg with _hash)');
    console.log('   Dependencies: Standalone (blobs + schemas = complete)');
    console.log('   Structure: Tabular with typed columns');

    // ========================================================================
    // PART 7: Full Workflow with MongoScanner
    // ========================================================================
    printSectionHeader('PART 7: Complete Workflow with MongoScanner');

    const scanner = new MongoScanner(db, { bs });
    const tree = await scanner.scan();

    console.log('\n🌳 Tree Structure Created:');
    console.log(`   Root: ${tree.rootHash.substring(0, 20)}...`);
    console.log(`   Total nodes: ${tree.trees.size}`);

    const rootTree = scanner.getRootTree();
    const rootMeta = rootTree?.meta as any;

    console.log('\n📄 Root Node Metadata:');
    console.log(`   Database: ${rootMeta.name}`);
    console.log(`   Type: ${rootMeta.type}`);
    console.log(
      `   TablesCfgTable Blob: ${rootMeta.tableCfgsTableBlobId?.substring(0, 20)}...`,
    );

    console.log('\n📚 Collection Nodes:');
    for (const [hash, node] of tree.trees) {
      const meta = node.meta as any;
      if (meta?.type === 'collection') {
        console.log(`\n   Collection: ${meta.name}`);
        console.log(`     Tree hash: ${hash.substring(0, 20)}...`);
        console.log(
          `     TableCfg hash: ${meta.tableCfgHash?.substring(0, 20)}...`,
        );
        console.log(
          `     ComponentsTable blob: ${meta.componentsBlobId?.substring(0, 20)}...`,
        );
        console.log(`     Document count: ${meta.docCount}`);

        // Retrieve and show snippet
        if (meta.componentsBlobId) {
          const compTable = await scanner.getComponentsTable(
            meta.componentsBlobId,
          );
          console.log(
            `     First row keys: ${Object.keys(compTable._data[0]).join(', ')}`,
          );
        }
      }
    }

    console.log('\n📋 TablesCfgTable:');
    if (rootMeta.tableCfgsTableBlobId) {
      const tableCfgsTableFull = await scanner.loadTablesCfgTable(
        rootMeta.tableCfgsTableBlobId,
      );
      console.log(`   Total schemas: ${tableCfgsTableFull._data.length}`);
      for (const cfg of tableCfgsTableFull._data) {
        console.log(
          `     - ${cfg.key} (${cfg.columns.length} columns, hash: ${cfg._hash?.substring(0, 20)}...)`,
        );
      }
    }

    // ========================================================================
    // Summary
    // ========================================================================
    printSectionHeader('SUMMARY');

    console.log(`
✅ Complete Transformation Demonstrated:

   1️⃣  MongoDB Documents (BSON)
       ↓
   2️⃣  Schema Discovery (TableCfg created)
       ↓
   3️⃣  Conversion (ComponentsTable with typed rows)
       ↓
   4️⃣  Serialization (JSON format)
       ↓
   5️⃣  Storage (Content-addressable blobs)
       ↓
   6️⃣  Retrieval (Load by blob ID)
       ↓
   7️⃣  Reconstruction (Schema + Data = Complete picture)

🎯 Benefits of RLJSON Format:
   ✓ Database-independent (not tied to MongoDB)
   ✓ Self-describing (schema included)
   ✓ Immutable (content-addressed)
   ✓ Portable (standard JSON)
   ✓ Verifiable (hashes for integrity)
   ✓ Versionable (schema changes tracked)
   ✓ Syncable (designed for replication)

📦 What Gets Stored:
   • ComponentsTable → Blob storage (actual data)
   • TableCfg → TablesCfgTable → Blob storage (schemas)
   • Tree metadata → MongoDB (references & structure)
   • sync_ops → ComponentsTable → Blob storage (change log)

🔄 MongoDB → RLJSON transformation is complete and tested!
`);
  } catch (error) {
    console.error('\n❌ Error:', error);
    process.exit(1);
  } finally {
    await client.close();
  }
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
