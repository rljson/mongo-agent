#!/usr/bin/env node
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * Demonstrates WHY RLJSON ComponentsTable must be in blob storage, not MongoDB
 * Shows the architectural reasons and what breaks if stored in MongoDB
 */

import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/?directConnection=true';

function printSection(title: string) {
  console.log('\n' + '═'.repeat(80));
  console.log(`  ${title}`);
  console.log('═'.repeat(80) + '\n');
}

async function main() {
  console.log('\n🏗️ Why Store RLJSON in Blob Storage vs MongoDB?\n');
  
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  
  try {
    const db = client.db('test_storage_choice');
    await db.dropDatabase();
    
    // ========================================================================
    // PART 1: The Two Approaches
    // ========================================================================
    printSection('PART 1: Two Possible Architectures');
    
    console.log('❌ APPROACH A: Store Everything in MongoDB\n');
    console.log('MongoDB Collections:');
    console.log('├─ users (source data)');
    console.log('├─ products (source data)');
    console.log('├─ sync_users_components (ComponentsTable for users)');
    console.log('├─ sync_products_components (ComponentsTable for products)');
    console.log('├─ sync_ops (sync operations)');
    console.log('└─ sync_tablecfgs (TablesCfgTable)\n');
    
    console.log('✅ APPROACH B: Hybrid (Source in MongoDB, Sync in Blobs)\n');
    console.log('MongoDB Collections:');
    console.log('├─ users (source data)');
    console.log('├─ products (source data)');
    console.log('└─ sync_state (metadata + references)\n');
    console.log('Blob Storage:');
    console.log('├─ blob_abc123... (users ComponentsTable)');
    console.log('├─ blob_xyz789... (products ComponentsTable)');
    console.log('├─ blob_def456... (sync_ops ComponentsTable)');
    console.log('└─ blob_ghi789... (TablesCfgTable)\n');
    
    // ========================================================================
    // PART 2: The Fundamental Reason - Separation of Concerns
    // ========================================================================
    printSection('PART 2: Reason #1 - Separation of Concerns');
    
    console.log('🎯 MongoDB Role: SOURCE DATABASE');
    console.log('   • Stores your application data');
    console.log('   • Users interact with this');
    console.log('   • Business logic reads/writes here');
    console.log('   • Change streams watch for changes\n');
    
    console.log('🎯 Blob Storage Role: SYNC TRANSPORT');
    console.log('   • Stores transformed data for sync');
    console.log('   • Immutable snapshots');
    console.log('   • Can be sent to other systems');
    console.log('   • Independent of source database\n');
    
    console.log('💡 Analogy:');
    console.log('   MongoDB = Your warehouse (stores inventory)');
    console.log('   Blobs = Shipping containers (transport goods)');
    console.log('   You don\'t store shipping containers IN the warehouse!\n');
    
    // ========================================================================
    // PART 3: The Recursion Problem
    // ========================================================================
    printSection('PART 3: Reason #2 - Avoiding Infinite Loops');
    
    console.log('❌ If ComponentsTable stored in MongoDB:\n');
    console.log('1. User inserts into "users" collection');
    console.log('   → Change stream detects insert');
    console.log('2. System updates "sync_users_components" collection');
    console.log('   → Change stream detects this too! 🔥');
    console.log('3. System captures this as a sync operation');
    console.log('   → Writes to "sync_ops" collection');
    console.log('   → Change stream detects this! 🔥🔥');
    console.log('4. Infinite recursion!\n');
    
    console.log('Solutions if using MongoDB:');
    console.log('  A) Exclude sync collections from change stream');
    console.log('     ❌ Problem: Misses legitimate changes to sync data');
    console.log('  B) Complex filtering logic');
    console.log('     ❌ Problem: Error-prone, hard to maintain');
    console.log('  C) Separate MongoDB instance for sync');
    console.log('     ❌ Problem: Now need 2 databases!\n');
    
    console.log('✅ With Blob Storage:\n');
    console.log('1. User inserts into "users" collection');
    console.log('   → Change stream detects insert');
    console.log('2. System writes ComponentsTable to blob storage');
    console.log('   → NOT in MongoDB, no change event! ✓');
    console.log('3. Clean separation, no recursion problems!\n');
    
    // ========================================================================
    // PART 4: Portability
    // ========================================================================
    printSection('PART 4: Reason #3 - Database Independence');
    
    console.log('🌐 Goal: Sync MongoDB → PostgreSQL\n');
    
    console.log('❌ If stored in MongoDB:');
    console.log('   PostgreSQL node needs:');
    console.log('   • MongoDB driver (to read sync_* collections)');
    console.log('   • MongoDB connection (source AND target)');
    console.log('   • Handle MongoDB-specific types');
    console.log('   • Now PostgreSQL depends on MongoDB! 🤔\n');
    
    console.log('✅ With Blob Storage:');
    console.log('   PostgreSQL node needs:');
    console.log('   • HTTP client (to fetch blobs)');
    console.log('   • JSON parser (standard)');
    console.log('   • No MongoDB dependency! ✓');
    console.log('   • Blob storage = universal transport layer\n');
    
    console.log('Real-world example:');
    console.log('   MongoDB (A) → Blob (S3) → PostgreSQL (B)');
    console.log('   PostgreSQL (B) → Blob (S3) → MySQL (C)');
    console.log('   MySQL (C) → Blob (S3) → SQLite (D)');
    console.log('   All using the SAME blob format!\n');
    
    // ========================================================================
    // PART 5: Immutability
    // ========================================================================
    printSection('PART 5: Reason #4 - Content-Addressed Immutability');
    
    console.log('📦 Blob Storage Properties:\n');
    console.log('Content-Addressed:');
    console.log('   blobId = hash(content)');
    console.log('   Same content → same ID');
    console.log('   Different content → different ID\n');
    
    console.log('Immutable:');
    console.log('   Once written, NEVER changes');
    console.log('   Updates create NEW blobs');
    console.log('   Old versions preserved\n');
    
    console.log('Example Timeline:');
    console.log('   T1: users_v1 → blob_abc123 (3 users)');
    console.log('   T2: users_v2 → blob_xyz789 (4 users)');
    console.log('   T3: users_v3 → blob_def456 (4 users, one updated)');
    console.log('   All versions preserved, can compare/verify!\n');
    
    console.log('❌ If stored in MongoDB:');
    console.log('   db.sync_users_components.replaceOne(...)');
    console.log('   → Old version LOST');
    console.log('   → No automatic versioning');
    console.log('   → Can\'t verify historical snapshots\n');
    
    // ========================================================================
    // PART 6: Storage Abstraction
    // ========================================================================
    printSection('PART 6: Reason #5 - Flexible Storage Backends');
    
    console.log('🔌 Blob Storage Abstraction (Bs interface):\n');
    console.log('Current implementation can use:');
    console.log('   • BsMem (in-memory, for testing)');
    console.log('   • BsFs (filesystem)');
    console.log('   • BsS3 (Amazon S3)');
    console.log('   • BsGridFS (MongoDB GridFS)  ← Still uses MongoDB, but differently!');
    console.log('   • BsAzure (Azure Blob Storage)');
    console.log('   • BsGCS (Google Cloud Storage)\n');
    
    console.log('✅ Benefits:');
    console.log('   • Development: Use BsMem (fast, no setup)');
    console.log('   • Testing: Use BsFs (persistent, local)');
    console.log('   • Production: Use BsS3 (scalable, distributed)');
    console.log('   • All using the SAME code!\n');
    
    console.log('❌ If stored directly in MongoDB collections:');
    console.log('   • Locked into MongoDB storage');
    console.log('   • Can\'t easily move to S3');
    console.log('   • Can\'t use CDN for blob distribution');
    console.log('   • Can\'t optimize storage separately\n');
    
    // ========================================================================
    // PART 7: Size and Performance
    // ========================================================================
    printSection('PART 7: Reason #6 - Size and Performance');
    
    console.log('📊 ComponentsTable can be LARGE:\n');
    
    // Simulate size calculation
    const rowCount = 100000;
    const avgRowSize = 500; // bytes
    const componentTableSize = rowCount * avgRowSize;
    const sizeInMB = (componentTableSize / 1024 / 1024).toFixed(2);
    
    console.log(`Example: 100,000 documents collection`);
    console.log(`   Average row: 500 bytes`);
    console.log(`   ComponentsTable size: ${sizeInMB} MB\n`);
    
    console.log('❌ In MongoDB Collection:');
    console.log(`   • Single document limit: 16 MB`);
    console.log(`   • Would need to split into multiple documents`);
    console.log(`   • Complex pagination logic`);
    console.log(`   • Queries might fetch entire table`);
    console.log(`   • Index overhead\n`);
    
    console.log('✅ In Blob Storage:');
    console.log(`   • No size limit (S3 supports 5TB objects)`);
    console.log(`   • Fetch entire blob in one request`);
    console.log(`   • Can use CDN/caching`);
    console.log(`   • No index overhead`);
    console.log(`   • Streaming support\n`);
    
    // ========================================================================
    // PART 8: What Actually Goes in MongoDB
    // ========================================================================
    printSection('PART 8: What SHOULD Go in MongoDB');
    
    console.log('✅ MongoDB stores CONTROL DATA:\n');
    
    console.log('sync_state collection:');
    console.log('{');
    console.log('  _id: "users",');
    console.log('  tableCfgHash: "abc123...",        ← Reference');
    console.log('  componentsBlobId: "blob_xyz...",  ← Reference');
    console.log('  lastScanned: "2026-03-24T10:00:00.000Z",');
    console.log('  docCount: 100000');
    console.log('}\n');
    
    console.log('sync_local collection:');
    console.log('{');
    console.log('  _id: "local",');
    console.log('  seq: 42,                          ← Sequence counter');
    console.log('  headHash: "xyz789...",            ← Blockchain head');
    console.log('  syncOpsBlobId: "blob_ops123..."   ← Reference to ops blob');
    console.log('}\n');
    
    console.log('Key insight:');
    console.log('   MongoDB = METADATA (small, fast queries)');
    console.log('   Blobs = PAYLOAD (large, immutable content)\n');
    
    // ========================================================================
    // PART 9: Real-World Scenario
    // ========================================================================
    printSection('PART 9: Real-World Scenario');
    
    console.log('🌍 Scenario: Multi-Region Sync\n');
    
    console.log('Architecture:');
    console.log('   Region A (US): MongoDB + Agent');
    console.log('   Region B (EU): PostgreSQL + Agent');
    console.log('   Region C (Asia): MySQL + Agent');
    console.log('   Central: S3 Blob Storage\n');
    
    console.log('Sync Flow:');
    console.log('   1. US Agent: Scans MongoDB → ComponentsTable → S3 blob');
    console.log('   2. EU Agent: Fetches S3 blob → Applies to PostgreSQL');
    console.log('   3. Asia Agent: Fetches S3 blob → Applies to MySQL\n');
    
    console.log('✅ Benefits of Blob Storage:');
    console.log('   • US Agent doesn\'t need PostgreSQL driver');
    console.log('   • EU Agent doesn\'t need MongoDB driver');
    console.log('   • S3 provides global distribution (CDN)');
    console.log('   • Each region reads once, S3 caches');
    console.log('   • Bandwidth optimization');
    console.log('   • Fault tolerance (S3 durability: 99.999999999%)\n');
    
    console.log('❌ If stored in MongoDB:');
    console.log('   • EU/Asia agents need MongoDB connection');
    console.log('   • All traffic goes to US MongoDB');
    console.log('   • No caching (direct DB queries)');
    console.log('   • Single point of failure');
    console.log('   • Bandwidth costs to US MongoDB\n');
    
    // ========================================================================
    // PART 10: GridFS Exception
    // ========================================================================
    printSection('PART 10: Exception - GridFS');
    
    console.log('🤔 "But GridFS stores blobs IN MongoDB!"\n');
    console.log('Yes, but key difference:\n');
    
    console.log('GridFS:');
    console.log('   • Stores blobs AS FILES (chunks)');
    console.log('   • Separate collections (fs.files, fs.chunks)');
    console.log('   • NOT monitored by change streams for sync');
    console.log('   • Binary blob storage, not structured documents');
    console.log('   • Abstracted through Bs interface\n');
    
    console.log('Regular Collection:');
    console.log('   • Stores documents');
    console.log('   • Monitored by change streams');
    console.log('   • Would create recursion problem\n');
    
    console.log('So GridFS is OK because:');
    console.log('   • Uses Bs interface (can swap to S3 later)');
    console.log('   • Not watched by sync change streams');
    console.log('   • Treated as opaque blobs');
    console.log('   • Good middle ground for self-contained deployments\n');
    
    // ========================================================================
    // Summary
    // ========================================================================
    printSection('SUMMARY: Why Blob Storage?');
    
    console.log('6 Key Reasons:\n');
    console.log('1️⃣  Separation of Concerns');
    console.log('   MongoDB = source, Blobs = sync transport\n');
    
    console.log('2️⃣  Avoid Infinite Loops');
    console.log('   Change streams don\'t watch blob storage\n');
    
    console.log('3️⃣  Database Independence');
    console.log('   Sync to PostgreSQL, MySQL, SQLite without MongoDB driver\n');
    
    console.log('4️⃣  Content-Addressed Immutability');
    console.log('   Automatic versioning, verification, deduplication\n');
    
    console.log('5️⃣  Storage Abstraction');
    console.log('   S3, filesystem, GridFS - swap without code changes\n');
    
    console.log('6️⃣  Size & Performance');
    console.log('   No 16MB limit, CDN support, streaming\n');
    
    console.log('🎯 The Golden Rule:');
    console.log('   MongoDB stores: METADATA & REFERENCES');
    console.log('   Blob storage stores: PAYLOAD & CONTENT');
    console.log('   Together they form the complete system!\n');
    
    console.log('📦 Think of it like Git:');
    console.log('   .git/refs/ = metadata (MongoDB)');
    console.log('   .git/objects/ = content blobs (Blob storage)');
    console.log('   Git doesn\'t store full files in refs, neither should we!\n');
    
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
