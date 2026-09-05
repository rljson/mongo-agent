<!--
@license
Copyright (c) 2025 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Architecture

## Overview

**@rljson/mongo-agent** is a distributed MongoDB synchronization system that enables bidirectional replication between multiple MongoDB instances with built-in integrity verification and tamper detection.

### Key Capabilities

- **Bidirectional Sync**: Automatic change propagation between MongoDB instances
- **Integrity Verification**: SHA-256 hash chains ensure data hasn't been tampered with
- **Tamper Detection**: Merkle tree state checkpoints detect unauthorized modifications
- **Hub-based Relay**: Central hub coordinates communication between agents
- **Crash Recovery**: Resume tokens allow recovery from interruptions
- **Conflict Resolution**: Last-write-wins strategy with timestamp ordering

## System Components

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│  MongoDB A  │◄───────►│   Agent A   │◄───────►│     Hub     │
│  (Port 27017)│         │  (Port 3001)│         │  (Port 3200)│
└─────────────┘         └─────────────┘         └─────────────┘
                                                        ▲
                                                        │
                                                        ▼
┌─────────────┐         ┌─────────────┐         
│  MongoDB B  │◄───────►│   Agent B   │         
│  (Port 27018)│         │  (Port 3002)│         
└─────────────┘         └─────────────┘         
```

### MongoDB Instances
- **Primary data stores** running as replica sets
- User collections + internal sync collections
- Change streams monitor real-time modifications

### Agents
- **Fastify HTTP servers** attached to each MongoDB instance
- Monitor local changes via change streams
- Pull and apply operations from remote origins
- Expose REST API for sync operations

### Hub
- **Central relay server** (optional but recommended)
- Client registry and discovery
- Request forwarding between agents
- Enables agents behind firewalls to communicate

## Data Model

### Internal Collections

Each MongoDB instance maintains these sync-related collections:

#### `sync_ops` - Operation Log
Append-only log of all operations with hash chain:
```javascript
{
  _id: "nodeA_3070",           // origin_seq
  origin: "nodeA",             // Source node ID
  seq: 3070,                   // Sequential number
  operationType: "insert",     // insert|update|replace|delete
  ns: { db: "mydb", coll: "users" },
  docId: ObjectId("..."),      // Document ID
  payload: { fullDocument: {...} },
  ts: "2026-03-12T10:30:00Z",
  prevHash: "abc123...",       // Hash of previous operation
  chainHash: "def456..."       // SHA-256(prevHash + operation)
}
```

#### `sync_local` - Local Head State
Tracks the latest local operation:
```javascript
{
  _id: "local",
  seq: 3070,                   // Latest sequence number
  headHash: "def456...",       // Latest chain hash
  updatedAt: "2026-03-12T10:30:00Z"
}
```

#### `sync_state` - Remote Tracking
Tracks sync status for each remote origin:
```javascript
{
  origin: "nodeB",             // Remote node ID
  lastSeqSeen: 2072,           // Last seq pulled from origin
  lastHashSeen: "xyz789...",
  applied: {
    lastSeq: 2072,             // Last seq successfully applied
    lastHash: "xyz789..."
  },
  updatedAt: "2026-03-12T10:30:00Z",
  updatedBy: "nodeA"
}
```

#### `state_checkpoints` - Merkle Tree Snapshots
Periodic state verification points:
```javascript
{
  _id: ObjectId("..."),
  seq: 1000,                   // Checkpoint sequence
  rootHash: "abc123...",       // Merkle tree root hash
  docCount: 5432,
  createdAt: "2026-03-12T10:00:00Z"
}
```

#### `state_merkle` - Collection Hashes
Merkle tree nodes for tamper detection:
```javascript
{
  _id: "users_partition_0",
  collection: "users",
  partition: 0,
  hash: "def456...",          // Hash of sorted document hashes
  docCount: 100,
  updatedAt: "2026-03-12T10:30:00Z"
}
```

#### `sync_resume` - Change Stream Recovery
Resume token for crash recovery:
```javascript
{
  _id: "resume",
  token: { _data: "..." },    // MongoDB resume token
  updatedAt: "2026-03-12T10:30:00Z"
}
```

## Synchronization Flow

### 1. Change Detection (Local)

```
MongoDB Change → Change Stream → Serial Queue → appendOp()
                                                    ↓
                                           Insert to sync_ops
                                                    ↓
                                           Update sync_local
                                                    ↓
                                           Save resume token
```

**Key Points:**
- Change streams capture insert/update/replace/delete
- Serial queue ensures sequential processing
- Each operation gets a hash linking to previous operation
- Internal collections (sync_*, state_*, system.*) are ignored

### 2. Change Propagation (Remote)

```
Agent B ──HTTP GET──→ Hub ──HTTP Relay──→ Agent A /sync/info
   ↓
Discovers nodeA has seq 3070
   ↓
Agent B ──HTTP POST──→ Hub ──HTTP Relay──→ Agent A /sync/pull?origin=nodeA&lastSeq=0
   ↓
Receives operations [1..3070]
   ↓
applyOneOp() for each operation
   ↓
Insert/Update/Delete in local MongoDB B
   ↓
Suppressor prevents echo loop
   ↓
Record in sync_ops as remote operation
   ↓
Update sync_state for nodeA
```

**Key Points:**
- Agents poll each peer on a 2-second interval (configurable)
- Hub acts as reverse proxy for agents behind firewalls
- Operations are applied in sequence order
- Suppressor prevents re-syncing operations that came from remote

### 3. Integrity Verification

**Document Hash (on write):**
```javascript
// Canonical JSON → SHA-256
const hash = computeIntegrityHash(doc);
doc.__h = hash;  // Stored with document
```

**Chain Hash (on append):**
```javascript
// Links operations in tamper-evident chain
const chainHash = sha256Hex(prevHash + JSON.stringify(op));
```

**State Hash (periodic checkpoint):**
```javascript
// Merkle tree of all documents
const rootHash = computeStateCheckpoint(db, collection);
// Verifies entire collection hasn't been tampered with
```

## API Endpoints

### Agent Endpoints

**GET /health**
```javascript
{ ok: true, nodeId: "nodeA" }
```

**GET /sync/info**
```javascript
{
  nodeId: "nodeA",
  headSeq: 3070,
  headHash: "def456..."
}
```

**GET /sync/state/:origin**
```javascript
{
  ok: true,
  origin: "nodeB",
  lastSeqPulled: 2072,
  state: { lastSeqSeen: 2072, applied: { lastSeq: 2072 } }
}
```

**POST /sync/pull**
```javascript
// Request
{ origin: "nodeB", lastSeq: 0, limit: 1000 }

// Response
{
  ops: [{ _id: "nodeB_1", seq: 1, ... }, ...],
  hasMore: true
}
```

### Hub Endpoints

**POST /hub/register**
```javascript
// Register agent with hub
{ clientId: "nodeA", url: "http://agenta:3001" }
```

**GET /hub/clients**
```javascript
{
  clients: [
    { clientId: "nodeA", url: "http://agenta:3001", lastSeenAt: "..." },
    { clientId: "nodeB", url: "http://agentb:3002", lastSeenAt: "..." }
  ]
}
```

**GET /hub/relay/:clientId/*path**
```javascript
// Forwards request to registered client
// Example: /hub/relay/nodeA/sync/info → http://agenta:3001/sync/info
```

## Key Algorithms

### Hash Chain Verification

```typescript
// Verify operations haven't been tampered with
let prevHash = 'GENESIS';
for (const op of operations) {
  const expected = sha256Hex(prevHash + JSON.stringify(op));
  if (op.chainHash !== expected) {
    throw new Error('Hash chain broken - tamper detected!');
  }
  prevHash = op.chainHash;
}
```

### Merkle Tree State Checkpoint

```typescript
// Create tamper-evident snapshot of entire collection
const docHashes = await collection
  .find({})
  .map(doc => doc.__h || computeIntegrityHash(doc))
  .sort()
  .toArray();

const rootHash = sha256Hex(docHashes.join(''));
// Store in state_checkpoints
```

### Dirty Tracking

```typescript
// Identify which documents changed since last checkpoint
const dirty = await collection
  .find({ __h: { $exists: false } })
  .toArray();

// Recompute hashes for changed documents only
for (const doc of dirty) {
  doc.__h = computeIntegrityHash(doc);
  await collection.updateOne({ _id: doc._id }, { $set: { __h: doc.__h } });
}
```

## Configuration

### Environment Variables

**Agent:**
```bash
PORT=3001                    # Agent HTTP port
NODE_ID=nodeA                # Unique node identifier
MONGO_URI=mongodb://...      # MongoDB connection string
DB_NAME=syncdb               # Database to sync
HUB_URL=http://hub:3200      # Hub URL (optional)
PEERS=nodeB,nodeC            # Comma-separated peer IDs
SYNC_INTERVAL_MS=2000        # Pull interval (default: 2000ms)
```

**Hub:**
```bash
PORT=3200                    # Hub HTTP port
```

## Deployment Patterns

### Pattern 1: Hub-based (Recommended)
```
┌──────────┐
│   Hub    │  Central coordination
└────┬─────┘
     │
     ├─────► Agent A (behind firewall)
     │
     └─────► Agent B (behind firewall)
```

**Pros:** Agents can be behind firewalls, simpler networking
**Cons:** Single point of failure (but stateless, easily replicated)

### Pattern 2: Mesh (Peer-to-Peer)
```
Agent A ◄────► Agent B
   ▲              ▲
   │              │
   └──────────────┘
        Agent C
```

**Pros:** No central dependency
**Cons:** Requires direct connectivity between all agents

## Testing Strategy

### Unit Tests (177 tests - 99.68% coverage)
- `test/**/*.spec.ts` - Vitest tests
- Mock MongoDB operations
- Focus on individual functions

### E2E Tests (4 suites - all passing)
- `test/e2e/` - Integration tests
- Require Docker and MongoDB
- Test full sync workflows

### Coverage Exclusions
- `src/agent-server.ts` - Runtime server (tested via e2e)
- `src/hub/index.ts` - Runtime hub (tested via e2e)
- `src/scripts/**` - Utility scripts

## Performance Characteristics

### Throughput
- ~3000+ operations/second per agent
- Batch operations in chunks of 1000
- Serial queue prevents race conditions

### Storage Overhead
- Each operation: ~500 bytes in sync_ops
- Document overhead: 64 bytes (__h field)
- Merkle tree: ~100 bytes per partition

### Network
- Polling: 1 HTTP request per peer every 2 seconds
- Large syncs: Paginated in 1000-operation batches
- Hub relay: Adds ~5ms latency

## Security Considerations

### Tamper Detection
- ✅ Hash chains detect operation modifications
- ✅ Merkle trees detect unauthorized document changes
- ✅ State checkpoints enable periodic verification

### Not Included (Add if needed)
- ⚠️ Authentication/authorization
- ⚠️ Encryption at rest
- ⚠️ TLS/HTTPS enforcement
- ⚠️ Rate limiting

## Limitations & Trade-offs

### Conflict Resolution
- **Last-write-wins** based on timestamp
- No automatic conflict detection for simultaneous updates
- Consider application-level conflict resolution for critical data

### Eventual Consistency
- Not suitable for strong consistency requirements
- Propagation delay: 2-4 seconds typical
- Use MongoDB transactions for local consistency

### Scalability
- Tested with 2-3 nodes
- Hub is stateless and can scale horizontally
- Consider partitioning for >10 nodes

### Network Partitions
- Agents continue local operations
- Sync resumes automatically when network recovers
- No automatic split-brain resolution
