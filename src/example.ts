// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { MongoAgent } from './mongo-agent.ts';


export const example = async () => {
  // Print methods
  const l = console.log;
  const h1 = (text: string) => l(`${text}`);
  const h2 = (text: string) => l(`  ${text}`);
  const p = (text: string) => l(`    ${text}`);

  // Example 1: Basic MongoAgent
  h1('MongoAgent.example');
  h2('Returns an instance of the MongoAgent.');
  const mongoAgent = MongoAgent.example;
  p(JSON.stringify(mongoAgent, null, 2));

  // Example 2: MongoDB Scanner
  h1('\nMongoScanner - Scan MongoDB database');
  h2('Scans collections and extracts RLJSON tree structure');

  // Note: In real usage, you would connect to MongoDB first:
  // import { connect } from './db.ts';
  // const mongoDb = await connect('mongodb://localhost:27017/mydb');

  p('// Connect to MongoDB');
  p('const mongoDb = await connect("mongodb://localhost:27017/mydb");');
  p('');
  p('// Create scanner with options');
  p('const scanner = new MongoScanner(mongoDb, {');
  p('  ignore: ["temp_*", "cache_*"],  // Ignore temp collections');
  p('  include: ["users", "orders"],   // Or only include specific ones');
  p('});');
  p('');
  p('// Scan the database');
  p('const tree = await scanner.scan();');
  p('');
  p(`// Result:`);
  p(`//   Scanned collections into tree structure`);
  p(`//   Root hash: <database_hash>`);
  p(`//   Documents stored as blobs in Bs`);

  // Show root tree structure
  h2('Root tree structure:');
  p('const rootTree = scanner.getRootTree();');
  p('if (rootTree) {');
  p('  console.log(`Database: ${rootTree.id}`);');
  p('  console.log(`Collections: ${rootTree.children?.length || 0}`);');
  p('}');

  // Example 3: Watch for changes
  h1('\nMongoScanner - Watch for changes');
  h2('Register a callback to be notified of MongoDB changes');

  p('scanner.onChange(async (change) => {');
  p('  console.log(`${change.type.toUpperCase()}: ${change.path}`);');
  p('  console.log(`Document ID: ${change.docId}`);');
  p('});');
  p('');
  p('// Changes will be logged when documents are modified');

  // Example 4: Integration with @rljson/db
  h1('\nMongoAgent - Integration with @rljson/db');
  h2('Use MongoAgent with RLJSON database layer');

  p('import { Io } from "@rljson/io";');
  p('import { Db } from "@rljson/db";');
  p('import { BsMem } from "@rljson/bs";');
  p('');
  p('// Setup RLJSON database');
  p('const io = new Io();');
  p('const rljsonDb = new Db(io);');
  p('const bs = new BsMem();');
  p('');
  p('// Create MongoAgent');
  p('const agent = new MongoAgent(mongoDb, bs);');
  p('');
  p('// Extract MongoDB structure and store in RLJSON DB');
  p('const ref = await agent.storeInDb(rljsonDb, "mongoTree");');
  p('console.log(`Stored tree with ref: ${ref}`);');

  // Example 5: Manual Database Sync with Connector
  h1('\nMongoAgent - Manual Database Sync');
  h2('Manually sync MongoDB changes to database using Connector');
  p('Use syncToDb() with a Connector for socket-based synchronization:');
  p('');
  p('import { Connector } from "@rljson/db";');
  p('import { Route } from "@rljson/rljson";');
  p('import { SocketMock } from "@rljson/io";');
  p('');
  p('const socket = new SocketMock();');
  p('const route = Route.fromFlat("/mongoTree+");');
  p('const connector = new Connector(rljsonDb, route, socket);');
  p('');
  p('const stopSync = await agent.syncToDb(rljsonDb, connector, "mongoTree");');
  p('');
  p('// Agent now:');
  p('// 1. Watches for MongoDB changes');
  p('// 2. Extracts trees and stores blobs');
  p('// 3. Broadcasts changes via Connector');
  p('');
  p('// Stop syncing:');
  p('stopSync();');
  p('agent.dispose();');

  // Example 6: Working with Hashes
  h1('\nIntegration with @rljson/hash');
  h2('MongoDB data is automatically hashed in tree structure');

  p('import { hsh, hip, validate } from "@rljson/hash";');
  p('');
  p('// Trees are automatically hashed when scanned:');
  p('const tree = await agent.extract();');
  p('');
  p('// Each node has a _hash property:');
  p('const rootTree = agent.scanner.getRootTree();');
  p('console.log(`Root hash: ${rootTree._hash}`);');
  p('');
  p('// You can validate the integrity:');
  p('import { Hash } from "@rljson/hash";');
  p('const h = Hash.default;');
  p('const isValid = h.validate(rootTree);');
  p('console.log(`Tree integrity valid: ${isValid}`);');

  // Example 7: Blob Storage
  h1('\nBlob Storage for Documents');
  h2('Document content is stored separately in blob storage');

  p('// Documents are stored as blobs:');
  p('const blobAdapter = agent.blobAdapter;');
  p('');
  p('// Convert a document to blob:');
  p('const doc = { _id: "123", name: "Test", value: 42 };');
  p('const meta = await blobAdapter.documentToBlob(');
  p('  doc,');
  p('  "mydb",');
  p('  "users"');
  p(');');
  p('console.log(`Stored as blob: ${meta.blobId}`);');
  p('');
  p('// Retrieve document from blob:');
  p('const retrievedDoc = await blobAdapter.blobToDocument(meta);');
  p('console.log(retrievedDoc); // { _id: "123", name: "Test", value: 42 }');

  // Example 8: Complete workflow
  h1('\nComplete Workflow Example');
  h2('End-to-end example of MongoDB to RLJSON synchronization');

  p('// 1. Connect to MongoDB');
  p('const mongoDb = await connect("mongodb://localhost:27017/mydb");');
  p('');
  p('// 2. Setup RLJSON infrastructure');
  p('const io = new Io();');
  p('const rljsonDb = new Db(io);');
  p('const bs = new BsMem(); // Or use BsFile, BsMongo, etc.');
  p('');
  p('// 3. Initialize MongoAgent');
  p('const agent = new MongoAgent(mongoDb, bs, {');
  p('  ignore: ["system.*", "temp_*"],');
  p('  include: ["users", "orders", "products"],');
  p('});');
  p('');
  p('// 4. Extract initial state');
  p('const tree = await agent.extract();');
  p('console.log(`Extracted ${tree.trees.size} tree nodes`);');
  p('');
  p('// 5. Store in RLJSON database');
  p('const ref = await agent.storeInDb(rljsonDb, "mongoTree");');
  p('console.log(`Initial state stored: ${ref}`);');
  p('');
  p('// 6. Setup real-time sync');
  p('const socket = new SocketMock();');
  p('const route = Route.fromFlat("/mongoTree+");');
  p('const connector = new Connector(rljsonDb, route, socket);');
  p('');
  p('const stopSync = await agent.syncToDb(rljsonDb, connector, "mongoTree");');
  p('');
  p('// 7. Now any MongoDB changes are automatically synced!');
  p('// Insert, update, delete operations will trigger tree updates');
  p('');
  p('// 8. Clean up when done');
  p('// stopSync();');
  p('// agent.dispose();');
};

/*
// Run via "npx vite-node src/example.ts"
example();
*/
