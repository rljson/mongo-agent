#!/usr/bin/env node
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * Demonstrates what's inside MongoDB's resume token (_data field)
 * Shows how the hex string encodes change stream metadata
 */

import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/?directConnection=true';

function hexToAscii(hex: string): string {
  let str = '';
  for (let i = 0; i < hex.length; i += 2) {
    const code = parseInt(hex.substr(i, 2), 16);
    if (code >= 32 && code <= 126) {
      str += String.fromCharCode(code);
    } else {
      str += '.';
    }
  }
  return str;
}

function highlightSection(data: string, start: number, length: number, label: string, color: string) {
  const section = data.substr(start, length);
  const ascii = hexToAscii(section);
  console.log(`${color}${label}${'\x1b[0m'}`);
  console.log(`  Hex:   ${section}`);
  console.log(`  ASCII: ${ascii}`);
  console.log(`  Pos:   ${start}-${start + length}\n`);
  return section;
}

async function main() {
  console.log('\n' + '═'.repeat(80));
  console.log('  Decoding MongoDB Resume Token (_data field)');
  console.log('═'.repeat(80) + '\n');

  const client = new MongoClient(MONGO_URI);
  await client.connect();

  try {
    const db = client.db('test_resume_token_demo');
    await db.dropDatabase();

    const collection = db.collection('users');

    // Create change stream
    const changeStream = collection.watch([], { fullDocument: 'updateLookup' });

    const eventPromise = new Promise((resolve) => {
      changeStream.once('change', (change) => resolve(change));
    });

    // Insert document to trigger event
    console.log('Inserting document to capture resume token...\n');
    await collection.insertOne({ 
      name: 'Alice',
      email: 'alice@example.com',
      age: 30 
    });

    const changeEvent = await eventPromise as any;
    await changeStream.close();

    // ========================================================================
    // Show the full change event
    // ========================================================================
    console.log('📡 Full Change Stream Event:\n');
    console.log(JSON.stringify(changeEvent, null, 2));
    console.log('\n' + '─'.repeat(80) + '\n');

    // ========================================================================
    // Extract and decode the resume token
    // ========================================================================
    const resumeToken = changeEvent._id;
    const dataField = resumeToken._data;

    console.log('🔍 Resume Token Structure:\n');
    console.log('Resume Token Object:');
    console.log(JSON.stringify(resumeToken, null, 2));
    console.log('\n' + '─'.repeat(80) + '\n');

    console.log('📝 The _data Field (Resume Token Payload):\n');
    console.log(`Full hex string (${dataField.length} characters):`);
    console.log(dataField);
    console.log('\n' + '─'.repeat(80) + '\n');

    console.log('🔬 Decoded Sections:\n');

    // Timestamp section (first ~8 bytes)
    const cyan = '\x1b[36m';
    const green = '\x1b[32m';
    const yellow = '\x1b[33m';
    const blue = '\x1b[34m';
    const magenta = '\x1b[35m';

    let pos = 0;
    
    // 1. Timestamp
    highlightSection(dataField, pos, 16, '1. Timestamp (Cluster Time)', cyan);
    pos += 16;

    // 2. Increment/sequence
    highlightSection(dataField, pos, 16, '2. Sequence/Increment', green);
    pos += 16;

    // 3. Token type indicator
    const tokenType = dataField.substr(pos, 4);
    console.log(`${yellow}3. Token Type Indicator${'\x1b[0m'}`);
    console.log(`  Hex:   ${tokenType}`);
    console.log(`  Value: 0x${tokenType}\n`);
    pos += 4;

    // 4. Look for "operationType" string
    const opTypeStart = dataField.indexOf('6F7065726174696F6E54797065');
    if (opTypeStart >= 0) {
      console.log(`${blue}4. Field Name: "operationType"${'\x1b[0m'}`);
      const opTypeHex = dataField.substr(opTypeStart, 26);
      console.log(`  Hex:   ${opTypeHex}`);
      console.log(`  ASCII: ${hexToAscii(opTypeHex)}`);
      console.log(`  Pos:   ${opTypeStart}-${opTypeStart + 26}\n`);
      
      // Get operation type value
      const afterOpType = opTypeStart + 26;
      const opTypeValueStart = dataField.indexOf('696E73657274', afterOpType);
      if (opTypeValueStart >= 0) {
        console.log(`${green}5. Operation Type Value: "insert"${'\x1b[0m'}`);
        const opValueHex = dataField.substr(opTypeValueStart, 12);
        console.log(`  Hex:   ${opValueHex}`);
        console.log(`  ASCII: ${hexToAscii(opValueHex)}`);
        console.log(`  Pos:   ${opTypeValueStart}-${opTypeValueStart + 12}\n`);
      }
    }

    // 5. Look for "documentKey" string
    const docKeyStart = dataField.indexOf('646F63756D656E744B6579');
    if (docKeyStart >= 0) {
      console.log(`${magenta}6. Field Name: "documentKey"${'\x1b[0m'}`);
      const docKeyHex = dataField.substr(docKeyStart, 22);
      console.log(`  Hex:   ${docKeyHex}`);
      console.log(`  ASCII: ${hexToAscii(docKeyHex)}`);
      console.log(`  Pos:   ${docKeyStart}-${docKeyStart + 22}\n`);
      
      // Get _id field name
      const idFieldStart = dataField.indexOf('645F6964', docKeyStart + 22);
      if (idFieldStart >= 0) {
        console.log(`${cyan}7. Field Name: "_id"${'\x1b[0m'}`);
        const idFieldHex = dataField.substr(idFieldStart, 8);
        console.log(`  Hex:   ${idFieldHex}`);
        console.log(`  ASCII: ${hexToAscii(idFieldHex)}`);
        console.log(`  Pos:   ${idFieldStart}-${idFieldStart + 8}\n`);
        
        // Get ObjectId value
        const objectIdStart = idFieldStart + 8;
        const objectIdHex = dataField.substr(objectIdStart, 24);
        console.log(`${green}8. Document _id (ObjectId)${'\x1b[0m'}`);
        console.log(`  Hex:   ${objectIdHex}`);
        console.log(`  Value: ObjectId("${objectIdHex}")`);
        console.log(`  Pos:   ${objectIdStart}-${objectIdStart + 24}\n`);
      }
    }

    // ========================================================================
    // Summary visualization
    // ========================================================================
    console.log('─'.repeat(80) + '\n');
    console.log('📊 Visual Breakdown:\n');
    
    console.log('Resume Token = Hex-Encoded Binary Data:');
    console.log('┌─────────────────────────────────────────────────────────────────┐');
    console.log('│ [Timestamp][Sequence][Type][operationType][insert]             │');
    console.log('│ [documentKey][_id][ObjectId][CollectionInfo][Position]         │');
    console.log('└─────────────────────────────────────────────────────────────────┘');
    
    console.log('\n🔑 What Each Part Does:\n');
    console.log('1. Timestamp       → When the change occurred (cluster time)');
    console.log('2. Sequence        → Operation order within same timestamp');
    console.log('3. Type Indicator  → Token format version');
    console.log('4. operationType   → "insert", "update", "delete", etc.');
    console.log('5. documentKey     → The _id of the changed document');
    console.log('6. Collection Info → Database and collection name');
    console.log('7. Position Info   → Exact oplog position');

    console.log('\n🎯 Why This Matters:\n');
    console.log('When you call: changeStream.resume(resumeToken)');
    console.log('MongoDB decodes this to:');
    console.log('  • Find the exact timestamp in the oplog');
    console.log('  • Locate the specific operation');
    console.log('  • Resume streaming from AFTER that point');
    console.log('  • Guarantee: No duplicates, no missed events');

    console.log('\n💡 In Our Implementation:\n');
    console.log('We store this as: changeStreamId.newString = resumeToken._data');
    console.log('Purpose: Resume sync if interrupted');
    console.log('Benefit: Pick up exactly where we left off');

    console.log('\n' + '═'.repeat(80));
    console.log('  Resume token decoded successfully!');
    console.log('═'.repeat(80) + '\n');

  } finally {
    await client.close();
  }
}

main().catch(console.error);
