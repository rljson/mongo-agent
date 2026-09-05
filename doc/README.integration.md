# RLJSON Integration Guide

This document explains how mongo-agent integrates with the RLJSON ecosystem, particularly the database middleware layer (@rljson/db).

## Architecture Overview

The mongo-agent follows the same architectural pattern as @rljson/fs-agent, adapted for MongoDB:

```
┌─────────────────┐
│   MongoDB       │  ← Source of truth (collections & documents)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  MongoScanner   │  ← Scans MongoDB, builds tree structure
└────────┬────────┘
         │
         ├─────► Tree (RLJSON structure with hashes)
         │
         └─────► Bs (Blob Storage for document content)

┌─────────────────┐
│  MongoAgent     │  ← Orchestrates scanning, syncing, storage
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  @rljson/db     │  ← Database middleware (Io, Db, Connector)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Storage Layer  │  ← IoMem, IoFile, IoPeer, etc.
└─────────────────┘
```

## Key Components

### 1. MongoScanner

Similar to `FsScanner`, scans MongoDB and extracts tree structure:

- **Input**: MongoDB database connection
- **Output**: Tree structure with hashed nodes
- **Storage**: Document content stored in Bs (blob storage)

```typescript
import { MongoScanner } from '@rljson/mongo-agent';
import { connect } from '@rljson/mongo-agent';

const mongoDb = await connect('mongodb://localhost:27017/mydb');
const scanner = new MongoScanner(mongoDb, {
  ignore: ['temp_*', 'cache_*'],
  include: ['users', 'orders'],
});

const tree = await scanner.scan();
console.log(`Root hash: ${tree.rootHash}`);
console.log(`Tree nodes: ${tree.trees.size}`);
```

### 2. MongoBlobAdapter

Handles conversion between MongoDB documents and blobs:

```typescript
import { MongoBlobAdapter } from '@rljson/mongo-agent';
import { BsMem } from '@rljson/bs';

const bs = new BsMem();
const adapter = new MongoBlobAdapter(bs);

// Document → Blob
const doc = { _id: '123', name: 'John', age: 30 };
const meta = await adapter.documentToBlob(doc, 'mydb', 'users');

// Blob → Document
const retrievedDoc = await adapter.blobToDocument(meta);
```

### 3. MongoDbAdapter

Stores MongoDB trees in RLJSON database:

```typescript
import { MongoDbAdapter } from '@rljson/mongo-agent';
import { Db } from '@rljson/db';
import { Io } from '@rljson/io';

const io = new Io();
const db = new Db(io);

const adapter = new MongoDbAdapter(db, 'mongoTree');
const ref = await adapter.storeMongoTree(tree);
```

### 4. MongoAgent

Main orchestrator class:

```typescript
import { MongoAgent } from '@rljson/mongo-agent';
import { Db } from '@rljson/db';
import { Io } from '@rljson/io';
import { BsMem } from '@rljson/bs';

const mongoDb = await connect('mongodb://localhost:27017/mydb');
const bs = new BsMem();
const agent = new MongoAgent(mongoDb, bs);

// Extract current state
const tree = await agent.extract();

// Store in RLJSON database
const io = new Io();
const rljsonDb = new Db(io);
const ref = await agent.storeInDb(rljsonDb, 'mongoTree');
```

## Integration with @rljson/db

### Data Flow

1. **Extract**: MongoAgent scans MongoDB and creates tree structure
2. **Store**: Trees are stored in RLJSON database via MongoDbAdapter
3. **Notify**: Changes trigger notifications through Db.notify
4. **Broadcast**: Connector broadcasts refs to connected clients

### Using with Connector

```typescript
import { Connector } from '@rljson/db';
import { Route } from '@rljson/rljson';
import { SocketMock } from '@rljson/io';

// Setup connector
const socket = new SocketMock();
const route = Route.fromFlat('/mongoTree+');
const connector = new Connector(rljsonDb, route, socket);

// Start syncing
const stopSync = await agent.syncToDb(rljsonDb, connector, 'mongoTree');

// Agent now watches MongoDB and broadcasts changes
// Stop when done
stopSync();
agent.dispose();
```

## Hash Integration

All tree nodes are automatically hashed using @rljson/hash:

```typescript
import { hsh, hip, validate } from '@rljson/hash';
import { Hash } from '@rljson/hash';

// Trees are automatically hashed during scan
const tree = await agent.extract();
const rootTree = agent.scanner.getRootTree();

// Verify integrity
const h = Hash.default;
const isValid = h.validate(rootTree);
console.log(`Tree integrity: ${isValid}`);
```

## Comparison with @rljson/fs-agent

| Aspect           | fs-agent                   | mongo-agent                      |
| ---------------- | -------------------------- | -------------------------------- |
| **Source**       | File system                | MongoDB database                 |
| **Scanner**      | FsScanner                  | MongoScanner                     |
| **Node Types**   | file, directory            | document, collection, database   |
| **Blob Content** | File content               | Document JSON                    |
| **Metadata**     | path, size, mtime          | docId, collection, database      |
| **Adapter**      | FsDbAdapter, FsBlobAdapter | MongoDbAdapter, MongoBlobAdapter |
| **Main Class**   | FsAgent                    | MongoAgent                       |

## Complete Example

Here's a complete example showing how to integrate MongoDB with the RLJSON database layer:

```typescript
import { connect } from '@rljson/mongo-agent';
import { MongoAgent } from '@rljson/mongo-agent';
import { Db } from '@rljson/db';
import { Io, SocketMock } from '@rljson/io';
import { BsMem } from '@rljson/bs';
import { Connector } from '@rljson/db';
import { Route } from '@rljson/rljson';

async function main() {
  // 1. Connect to MongoDB
  const mongoDb = await connect('mongodb://localhost:27017/mydb');

  // 2. Setup RLJSON infrastructure
  const io = new Io();
  const rljsonDb = new Db(io);
  const bs = new BsMem();

  // 3. Create MongoAgent
  const agent = new MongoAgent(mongoDb, bs, {
    ignore: ['system.*', 'temp_*'],
    include: ['users', 'orders', 'products'],
  });

  // 4. Extract initial state
  const tree = await agent.extract();
  console.log(`Extracted ${tree.trees.size} tree nodes`);

  // 5. Store in RLJSON database
  const ref = await agent.storeInDb(rljsonDb, 'mongoTree');
  console.log(`Initial state stored: ${ref}`);

  // 6. Setup real-time sync
  const socket = new SocketMock();
  const route = Route.fromFlat('/mongoTree+');
  const connector = new Connector(rljsonDb, route, socket);

  const stopSync = await agent.syncToDb(rljsonDb, connector, 'mongoTree');

  // 7. Now any MongoDB changes are automatically synced!
  console.log('Real-time sync active');

  // 8. Register a change callback
  agent.scanner.onChange(async (change) => {
    console.log(`Change detected: ${change.type} on ${change.path}`);
    console.log(`Document ID: ${change.docId}`);
  });

  // Keep running...
  // When done:
  // stopSync();
  // agent.dispose();
}

main().catch(console.error);
```

## Working with Static Examples

Similar to how @rljson/db has `example-static.ts`, you can create structured data:

```typescript
import { hip, hsh } from '@rljson/hash';
import { ComponentsTable, TableCfg } from '@rljson/rljson';

// Create table configuration
const usersTableCfg = hip<TableCfg>({
  key: 'users',
  type: 'components',
  columns: [
    { key: '_hash', type: 'string', titleLong: 'Hash' },
    { key: 'name', type: 'string', titleLong: 'Name' },
    { key: 'email', type: 'string', titleLong: 'Email' },
    { key: 'age', type: 'number', titleLong: 'Age' },
  ],
  isHead: false,
  isRoot: false,
  isShared: true,
  _hash: '',
});

// Create data
const users = hip<ComponentsTable<any>>({
  _tableCfg: usersTableCfg._hash as string,
  _type: 'components',
  _data: [
    { name: 'Alice', email: 'alice@example.com', age: 28, _hash: '' },
    { name: 'Bob', email: 'bob@example.com', age: 35, _hash: '' },
  ].map((user) => hsh(user)),
  _hash: '',
});
```

## Best Practices

1. **Use BsMem for development**, switch to BsFile or BsMongo for production
2. **Always validate hashes** after extracting trees
3. **Set appropriate ignore patterns** to exclude system collections
4. **Use Connector for real-time sync** instead of polling
5. **Handle errors gracefully** - MongoDB connections can fail
6. **Dispose agents** when done to free resources

## Troubleshooting

### Issue: Tree extraction is slow

- Use `include` to scan only necessary collections
- Add more collections to `ignore` pattern
- Consider increasing timeout settings

### Issue: Blobs not found

- Ensure Bs is persistent (use BsFile or BsMongo, not BsMem)
- Check that blob IDs match between tree metadata and storage
- Verify blob storage is properly initialized

### Issue: Changes not syncing

- Verify Connector is properly configured
- Check that MongoDB change streams are enabled
- Ensure onChange callbacks are registered before changes occur

## See Also

- [fs-agent documentation](https://github.com/rljson/fs-agent)
- [@rljson/db documentation](https://github.com/rljson/db)
- [@rljson/hash documentation](https://github.com/rljson/hash)
- [RLJSON specification](https://github.com/rljson/rljson)
