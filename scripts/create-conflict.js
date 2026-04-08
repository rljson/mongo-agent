#!/usr/bin/env node
/**
 * Create a merge conflict by modifying the same article from two different "nodes"
 */

import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI || 'mongodb://mongoa:27017/syncdb?replicaSet=rsA';

async function main() {
  console.log('🔌 Connecting to MongoDB...');
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  
  const db = client.db('syncdb');
  const articles = db.collection('articles');
  const syncOps = db.collection('sync_ops');
  
  // Step 1: Create a base article
  const articleId = `article-conflict-${Date.now()}`;
  console.log(`\n📝 Creating base article: ${articleId}`);
  
  await articles.insertOne({
    _id: articleId,
    title: 'Understanding Database Synchronization',
    author: 'John Doe',
    content: 'This article explains database sync concepts.',
    status: 'draft',
    version: 1,
    lastModified: new Date(),
  });
  
  console.log('✅ Base article created');
  
  // Wait a bit for change stream to capture it
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  // Step 2: Simulate "Node A" modification
  console.log('\n🔄 Simulating Node A modification...');
  await articles.updateOne(
    { _id: articleId },
    { 
      $set: { 
        title: 'Understanding Database Synchronization - Updated',
        status: 'published',
        version: 2,
        lastModified: new Date(),
        modifiedBy: 'nodeA'
      } 
    }
  );
  
  // Manually insert a sync operation as if it came from Node A
  const nodeASeq = await syncOps.countDocuments({ origin: 'nodeA' }) + 1;
  const nodeADoc = await articles.findOne({ _id: articleId });
  
  await syncOps.insertOne({
    origin: 'nodeA',
    seq: nodeASeq,
    type: 'update',
    ns: 'syncdb.articles',
    documentKey: { _id: articleId },
    fullDocument: nodeADoc,
    timestamp: new Date(),
    chainHash: `hash-nodeA-${nodeASeq}`,
  });
  
  console.log('✅ Node A modification recorded');
  
  // Step 3: Simulate concurrent "Node B" modification (different changes)
  console.log('\n🔄 Simulating Node B modification (concurrent)...');
  
  // Reset to version 1 to simulate concurrent edit
  await articles.updateOne(
    { _id: articleId },
    { 
      $set: { 
        title: 'Understanding Database Synchronization',
        content: 'This article explains database sync concepts in depth with examples.',
        author: 'John Doe & Jane Smith',
        status: 'review',
        version: 2,
        lastModified: new Date(),
        modifiedBy: 'nodeB',
        reviewers: ['Jane Smith']
      } 
    }
  );
  
  const nodeBSeq = await syncOps.countDocuments({ origin: 'nodeB' }) + 1;
  const nodeBDoc = await articles.findOne({ _id: articleId });
  
  await syncOps.insertOne({
    origin: 'nodeB',
    seq: nodeBSeq,
    type: 'update',
    ns: 'syncdb.articles',
    documentKey: { _id: articleId },
    fullDocument: nodeBDoc,
    timestamp: new Date(),
    chainHash: `hash-nodeB-${nodeBSeq}`,
  });
  
  console.log('✅ Node B modification recorded');
  
  // Step 4: Detect and store the conflict
  console.log('\n⚠️  Detecting conflict...');
  
  const conflicts = db.collection('sync_conflicts');
  
  const conflict = {
    conflictId: `conflict-${articleId}-${Date.now()}`,
    documentId: articleId,
    collection: 'articles',
    database: 'syncdb',
    detectedAt: Date.now(),
    status: 'pending',
    conflictType: 'concurrent-update',
    versions: [
      {
        documentId: articleId,
        data: await syncOps.findOne({ origin: 'nodeA', documentKey: { _id: articleId } }).then(op => op.fullDocument),
        timestamp: Date.now() - 1000,
        nodeId: 'nodeA',
        operationId: `op-nodeA-${nodeASeq}`,
        operationType: 'update',
        stateHash: `hash-nodeA-${nodeASeq}`,
        componentsHash: `comp-nodeA-${nodeASeq}`
      },
      {
        documentId: articleId,
        data: await syncOps.findOne({ origin: 'nodeB', documentKey: { _id: articleId } }).then(op => op.fullDocument),
        timestamp: Date.now(),
        nodeId: 'nodeB',
        operationId: `op-nodeB-${nodeBSeq}`,
        operationType: 'update',
        stateHash: `hash-nodeB-${nodeBSeq}`,
        componentsHash: `comp-nodeB-${nodeBSeq}`
      }
    ]
  };
  
  await conflicts.insertOne(conflict);
  
  console.log('✅ Conflict stored in sync_conflicts collection');
  
  // Summary
  console.log('\n' + '='.repeat(60));
  console.log('🎯 CONFLICT CREATED SUCCESSFULLY!');
  console.log('='.repeat(60));
  console.log(`📄 Article ID: ${articleId}`);
  console.log(`🔗 Conflict ID: ${conflict.conflictId}`);
  console.log(`\n📊 Differences:`);
  console.log(`   Node A: Changed title and status to "published"`);
  console.log(`   Node B: Changed content, author, and status to "review"`);
  console.log(`\n👉 Check MongoDB Compass:`);
  console.log(`   - Database: syncdb`);
  console.log(`   - Collection: sync_conflicts`);
  console.log(`\n🌐 Refresh your UI at http://localhost:4200`);
  console.log('='.repeat(60));
  
  await client.close();
}

main().catch(console.error);
