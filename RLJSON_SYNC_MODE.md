# RLJSON Sync Mode

This document explains how to use the RLJSON sync mode for hash-based synchronization instead of traditional operation-based sync.

## Overview

**Traditional Sync (Legacy Mode):**

- Agents sync by exchanging individual MongoDB operations (insert/update/delete)
- Operations are stored in `sync_ops` collection with operation chains
- Each operation contains full document data as JSON
- High bandwidth usage for large documents

**RLJSON Sync (New Mode):**

- Agents sync by exchanging tree structures with cryptographic hashes
- Documents are stored as blobs with unique blob IDs
- Only hashes and metadata are transmitted
- Dramatically reduced bandwidth for data that hasn't changed
- Built-in data integrity verification

## Architecture

### Data Structure

```
RLJSON Tree Payload:
{
  "origin": "nodeA",
  "rootHash": "ZsEo8nRDU4MDlW9PlPub2d",  // Root hash of entire database state
  "totalNodes": 12,
  "nodes": [
    {
      "hash": "CMsGdV3iKswmBALHNgDBni",   // Unique hash for this node
      "node": {
        "id": "user1",
        "isParent": false,
        "meta": {
          "type": "document",
          "collection": "users",
          "blobId": "vRgNkDYfiAHaN-Z8ftP3fF",  // Reference to blob storage
          "_hash": "rnK_t-yDfFP-Utz1BFFHD3"
        }
      }
    },
    ...
  ],
  "blobs": [
    {
      "blobId": "vRgNkDYfiAHaN-Z8ftP3fF",
      "content": "eyJfaWQiOiJ1c2VyMSIsIm5hbWUiOiJBbGljZSJ9"  // base64 encoded
    },
    ...
  ]
}
```

### Sync Flow

1. **Extract**: Agent extracts MongoDB state as RLJSON tree
   - Scans all collections and documents
   - Creates tree structure (database → collections → documents)
   - Stores document content in blob storage
   - Generates cryptographic hash for each node

2. **Transmit**: Agent sends tree to peers via hub
   - Sends tree structure with hashes
   - Includes blob IDs and content
   - Much smaller payload than raw JSON operations

3. **Apply**: Receiving agent applies tree to local MongoDB
   - Stores blobs in local blob storage
   - Reconstructs documents from blobs
   - Upserts documents to MongoDB collections
   - Records sync state with root hash

## Enabling RLJSON Sync

### Environment Variable

Set `USE_RLJSON_SYNC=true` to enable RLJSON mode:

```bash
# In docker-compose.yaml or .env
USE_RLJSON_SYNC=true
```

### Docker Compose Example

```yaml
services:
  agenta:
    environment:
      - USE_RLJSON_SYNC=true # Enable RLJSON mode
      - NODE_ID=nodeA
      - MONGO_URI=mongodb://mongoa:27017/syncdb?replicaSet=rsA
      - HUB_URL=http://hub:3200
      - PEERS=nodeB
      - SYNC_INTERVAL_MS=2000
```

### Programmatic Usage

```typescript
import { createAgentApp } from '@rljson/mongo-agent';

const app = createAgentApp({
  mongo: mongoClient,
  dbName: 'mydb',
  nodeId: 'nodeA',
  hubUrl: 'http://hub:3200',
  peers: ['nodeB', 'nodeC'],
  useRljsonSync: true, // Enable RLJSON mode
});
```

## API Endpoints

### Get RLJSON Tree

```bash
GET /rljson/tree
```

Returns the current database state as an RLJSON tree with hashes.

**Response:**

```json
{
  "ok": true,
  "payload": {
    "origin": "nodeA",
    "rootHash": "...",
    "totalNodes": 12,
    "nodes": [...],
    "blobs": [...]
  }
}
```

### Sync RLJSON Tree

```bash
POST /rljson/sync
Content-Type: application/json

{
  "origin": "nodeB",
  "rootHash": "...",
  "totalNodes": 12,
  "nodes": [...],
  "blobs": [...]
}
```

Applies an RLJSON tree from a peer to the local database.

**Response:**

```json
{
  "ok": true,
  "result": {
    "success": true,
    "rootHash": "...",
    "nodesApplied": 12,
    "blobsReceived": 8
  }
}
```

### Get Sync State

```bash
GET /rljson/state/:origin
```

Returns the RLJSON sync state for a specific origin.

**Response:**

```json
{
  "ok": true,
  "origin": "nodeB",
  "synced": true,
  "lastRootHash": "...",
  "lastSyncedAt": "2026-03-18T10:30:00Z",
  "totalNodes": 12,
  "totalBlobs": 8
}
```

## Testing RLJSON Sync

### Manual Test

1. Start agents with RLJSON mode enabled:

```bash
USE_RLJSON_SYNC=true docker compose up -d
```

2. Insert data into nodeA's MongoDB:

```bash
mongosh mongodb://localhost:27017/syncdb --eval '
  db.users.insertOne({name: "Alice", email: "alice@example.com"})
'
```

3. Check nodeA's RLJSON tree:

```bash
curl http://localhost:3001/rljson/tree | jq '.payload.rootHash'
```

4. Wait for sync (2 seconds by default)

5. Verify data synced to nodeB:

```bash
mongosh mongodb://localhost:27018/syncdb --eval 'db.users.find()'
```

6. Check sync state:

```bash
curl http://localhost:3002/rljson/state/nodeA | jq
```

### Automated E2E Test

The integration test demonstrates RLJSON extraction:

```bash
cd test/e2e
./test-rljson-integration.sh
```

This test shows:

- RLJSON tree extraction with hashes
- Blob storage for documents
- Hash integrity verification
- Complete tree structure output

## Benefits

### 1. Reduced Bandwidth

- **Before (Legacy)**: Send entire document on every change
- **After (RLJSON)**: Send only hash + blob ID if document unchanged

Example:

```javascript
// Legacy mode: ~500 bytes per document per sync
{
  "operationType": "insert",
  "fullDocument": {
    "_id": "user1",
    "name": "Alice",
    "email": "alice@example.com",
    "profile": { /* large object */ }
  }
}

// RLJSON mode: ~100 bytes (if document unchanged)
{
  "hash": "CMsGdV3iKswmBALHNgDBni",
  "node": {
    "meta": {
      "blobId": "vRgNkDYfiAHaN-Z8ftP3fF"
    }
  }
}
// Blob only sent once, then referenced by ID
```

### 2. Data Integrity

- Every node has a cryptographic hash
- Root hash represents entire database state
- Tamper detection built-in
- Verify data hasn't been modified

### 3. Efficient Delta Sync

- Compare root hashes to detect changes
- Only sync changed subtrees
- Future optimization: merkle tree diff

### 4. Blob Deduplication

- Same document → same blob ID
- Blobs stored once, referenced many times
- Efficient for immutable data

## Comparison

| Feature                | Legacy Mode           | RLJSON Mode                        |
| ---------------------- | --------------------- | ---------------------------------- |
| **Data Format**        | MongoDB operations    | Tree structures with hashes        |
| **Transmission**       | Full documents        | Hashes + blob IDs                  |
| **Integrity**          | Operation chain       | Cryptographic hashes               |
| **Bandwidth**          | High (full docs)      | Low (only changes)                 |
| **Deduplication**      | No                    | Yes (blobs)                        |
| **State Verification** | Operation replay      | Root hash comparison               |
| **Storage**            | `sync_ops` collection | `rljson_sync_state` + blob storage |

## Limitations

- **Initial sync**: First sync sends all blobs (same as legacy)
- **Blob storage**: Requires additional storage for blobs (in-memory by default)
- **Tree overhead**: Small overhead for tree structure metadata
- **Best for**: Read-heavy workloads, large documents, infrequent changes

## Future Enhancements

1. **Merkle Tree Diff**: Only sync changed branches
2. **Persistent Blob Storage**: Use BsFile or BsMongo instead of BsMem
3. **Compression**: Compress blobs before transmission
4. **Incremental Sync**: Send only new nodes since last sync
5. **Conflict Resolution**: Merge strategies for concurrent updates

## Migration Path

To migrate from legacy to RLJSON mode:

1. Run both modes in parallel (gradual rollout)
2. Monitor bandwidth and performance
3. Verify data integrity with hash checks
4. Switch entirely to RLJSON mode
5. Remove legacy `sync_ops` collections

## Troubleshooting

### Blobs not found

Check blob storage is properly initialized:

```typescript
import { BsMem } from '@rljson/bs';
const bs = new BsMem();
// Pass bs to MongoAgent
const agent = new MongoAgent(mongoDb, bs);
```

### Tree mismatch

Different scan times may produce different hashes (due to timestamps).
This is normal and doesn't indicate data corruption.

### Sync not happening

Check logs for:

```bash
docker compose logs agenta | grep RLJSON
docker compose logs agentb | grep RLJSON
```

Verify `USE_RLJSON_SYNC=true` is set in environment.
