# Change Stream Metadata & Blockchain Chain

## The Question
**"Do we need to convert or take the change stream ID into account for the blockchain chain?"**

## Short Answer
**Yes, we should ADD change stream metadata (resume token, cluster time) to sync_ops for:**
- ✅ Correlation with MongoDB events
- ✅ Verification of ordering
- ✅ Debugging capabilities
- ✅ Completeness

**But it serves a DIFFERENT purpose than the blockchain chain.**

---

## Current State

### ✅ What We Have

**Blockchain Chain (Content Integrity):**
```javascript
{
  _id: "nodeA_1",
  seq: 1,
  prevHash: "GENESIS",       // Previous op's chainHash
  opHash: "abc123...",       // Hash of current op data
  chainHash: "xyz789...",    // Hash(prevHash + opHash)
  // ... operation data
}
```

**Resume Token (Separate Collection):**
```javascript
// In sync_resume collection
{
  _id: "resume",
  token: { _data: "8267ABC..." },  // MongoDB resume token
  updatedAt: "2026-03-24T10:00:00.000Z"
}
```

### ❌ What We Don't Have

**Resume token NOT in sync_ops themselves:**
- Can't correlate sync_op → MongoDB event
- Can't verify our order matches MongoDB's order
- Missing MongoDB cluster time
- Missing wall time from MongoDB

---

## Two Different Chains

### 🔗 Our Blockchain Chain

**Purpose:** Content integrity and tamper detection  
**Mechanism:** Hash chain linking operations  
**Guarantees:**
- Sequential processing
- Tamper detection (any change breaks chain)
- Content-based verification

**Example:**
```
Op1: prevHash=GENESIS  → chainHash=abc123
       ↓
Op2: prevHash=abc123   → chainHash=xyz789
       ↓
Op3: prevHash=xyz789   → chainHash=def456
```

### 🎫 MongoDB Resume Token Chain

**Purpose:** MongoDB ordering and resumption  
**Mechanism:** MongoDB internal event IDs  
**Guarantees:**
- Can resume from exact point
- No duplicate events
- MongoDB's ordering preserved

**Example:**
```
Event1: _id={_data:"8267A..."}
          ↓
Event2: _id={_data:"8267B..."}
          ↓
Event3: _id={_data:"8267C..."}
```

---

## What MongoDB Provides

Every change stream event includes:

```javascript
{
  // Resume token (unique change ID)
  _id: {
    _data: "8269C2678F000000042B042C0100296E5A...",
    _typeBits: Buffer
  },
  
  // MongoDB cluster timestamp
  clusterTime: {
    $timestamp: "7620767383342284804"
  },
  
  // Wall clock time
  wallTime: "2026-03-24T10:29:35.158Z",
  
  // The operation
  operationType: "insert",
  ns: { db: "mydb", coll: "users" },
  documentKey: { _id: "user123" },
  fullDocument: { ... }
}
```

---

## What We Should Store

### Current sync_ops Schema:
```javascript
{
  // Our fields
  _id: "nodeA_1",
  origin: "nodeA",
  seq: 1,
  
  // Blockchain chain
  prevHash: "GENESIS",
  opHash: "abc123...",
  chainHash: "xyz789...",
  
  // Operation data
  ns: { db: "mydb", coll: "users" },
  operationType: "insert",
  docId: "user123",
  payload: { fullDocument: {...} },
  ts: "2026-03-24T10:00:00.000Z"
}
```

### Enhanced sync_ops Schema (RECOMMENDED):
```javascript
{
  // Our fields (unchanged)
  _id: "nodeA_1",
  origin: "nodeA",
  seq: 1,
  
  // Blockchain chain (unchanged)
  prevHash: "GENESIS",
  opHash: "abc123...",
  chainHash: "xyz789...",
  
  // Operation data (unchanged)
  ns: { db: "mydb", coll: "users" },
  operationType: "insert",
  docId: "user123",
  payload: { fullDocument: {...} },
  ts: "2026-03-24T10:00:00.000Z",
  
  // NEW: MongoDB change stream metadata
  changeStreamId: {                 // Resume token
    _data: "8269C2678F...",
    _typeBits: Buffer
  },
  clusterTime: {                    // MongoDB cluster time
    $timestamp: "7620767383342284804"
  },
  wallTime: "2026-03-24T10:29:35.158Z"  // MongoDB wall time
}
```

---

## Benefits of Adding Change Stream Metadata

### 1. **Correlation**
Link sync_ops back to original MongoDB events:
```javascript
// Find MongoDB event that created this sync_op
const changeStreamEvent = findByResumeToken(syncOp.changeStreamId);
```

### 2. **Verification**
Validate our ordering matches MongoDB:
```javascript
// Verify: our seq order matches MongoDB resume token order
assert(syncOp1.changeStreamId < syncOp2.changeStreamId);
assert(syncOp1.seq < syncOp2.seq);
```

### 3. **Debugging**
Trace back to exact MongoDB event:
```javascript
// Resume from specific sync_op's point
const stream = collection.watch([], {
  resumeAfter: syncOp.changeStreamId
});
```

### 4. **Multi-Source Sync**
When syncing from multiple MongoDB instances:
```javascript
// Identify which MongoDB instance this came from
// Cluster time helps with causality across sources
if (op1.clusterTime < op2.clusterTime) {
  // op1 happened before op2 in MongoDB's view
}
```

### 5. **Clock Drift Detection**
Compare MongoDB time vs our time:
```javascript
const ourTime = new Date(syncOp.ts);
const mongoTime = new Date(syncOp.wallTime);
const drift = Math.abs(ourTime - mongoTime);
if (drift > 5000) {
  console.warn('Clock drift detected:', drift, 'ms');
}
```

---

## Implementation

### 1. Update SYNC_OPS_TABLE_CFG

Add three new columns:

```typescript
export const SYNC_OPS_TABLE_CFG = hip<TableCfg>({
  key: 'sync_ops',
  type: 'components',
  columns: [
    // ... existing columns ...
    { key: 'ts', type: 'string', titleShort: 'TS', titleLong: 'Timestamp' },
    
    // NEW: Change stream metadata columns
    { 
      key: 'changeStreamId', 
      type: 'json', 
      titleShort: 'CSId', 
      titleLong: 'Change Stream ID' 
    },
    { 
      key: 'clusterTime', 
      type: 'json', 
      titleShort: 'ClusterT', 
      titleLong: 'Cluster Time' 
    },
    { 
      key: 'wallTime', 
      type: 'string', 
      titleShort: 'WallT', 
      titleLong: 'Wall Time' 
    },
  ],
  // ... rest of config
});
```

### 2. Update SyncOp and SyncOpDoc Interfaces

```typescript
export interface SyncOp {
  ns: Namespace;
  operationType: string;
  docId: unknown;
  payload?: {
    fullDocument?: unknown;
    updateDescription?: unknown;
  } | null;
  ts?: string;
  
  // NEW: Change stream metadata
  changeStreamId?: any;
  clusterTime?: any;
  wallTime?: Date | string;
}

export interface SyncOpDoc extends SyncOp {
  _id: string;
  origin: string;
  seq: number;
  prevHash: string;
  opHash: string;
  chainHash: string;
}
```

### 3. Update appendOp Function

```typescript
async function appendOp(
  db: Db,
  bs: Bs,
  nodeId: string,
  op: SyncOp,
  logger?: Logger,
): Promise<SyncOpDoc> {
  // ... existing code ...
  
  const doc: SyncOpDoc = {
    _id: `${nodeId}_${nextSeq}`,
    origin: nodeId,
    seq: nextSeq,
    prevHash,
    opHash,
    chainHash,
    ns: op.ns,
    operationType: op.operationType,
    docId: op.docId,
    payload: op.payload,
    ts: op.ts,
    
    // NEW: Include change stream metadata
    changeStreamId: op.changeStreamId,
    clusterTime: op.clusterTime,
    wallTime: op.wallTime,
  };
  
  // ... rest of function
}
```

### 4. Update Change Stream Handler

```typescript
cs.on('change', (change: ChangeStreamDocument) => {
  q.enqueue(async () => {
    // ... existing extraction code ...
    
    const op: SyncOp = {
      ns: { db: ns.db, coll: ns.coll },
      operationType: change.operationType,
      docId,
      payload: {
        fullDocument: change.fullDocument ?? null,
        updateDescription: change.updateDescription ?? null,
      },
      ts: new Date().toISOString(),
      
      // NEW: Capture change stream metadata
      changeStreamId: change._id,
      clusterTime: change.clusterTime,
      wallTime: change.wallTime,
    };
    
    // Append operation
    await appendOp(db, blobStorage, nodeId, op, logger);
    
    // ... rest of handler
  });
});
```

---

## Both Together = Best of Both Worlds

| Feature | Blockchain Chain | Resume Token | Both Together |
|---------|-----------------|--------------|---------------|
| **Content Integrity** | ✅ | ❌ | ✅ |
| **Tamper Detection** | ✅ | ❌ | ✅ |
| **Resume Capability** | ❌ | ✅ | ✅ |
| **MongoDB Correlation** | ❌ | ✅ | ✅ |
| **Ordering Guarantee** | ✅ (ours) | ✅ (MongoDB) | ✅ (both) |
| **Debugging** | ❌ | ✅ | ✅ |
| **Verification** | ✅ (content) | ✅ (order) | ✅ (both) |

---

## Example: Full sync_op with Both Chains

```javascript
{
  // Our blockchain chain
  "_id": "nodeA_23",
  "origin": "nodeA",
  "seq": 23,
  "prevHash": "7f89a3b2c1d4e5f6...",
  "opHash": "a1b2c3d4e5f6g7h8...",
  "chainHash": "3e4f5a6b7c8d9e0f...",
  
  // Operation data
  "ns": { "db": "myapp", "coll": "users" },
  "operationType": "update",
  "docId": "user_456",
  "payload": {
    "updateDescription": {
      "updatedFields": { "status": "active" }
    }
  },
  "ts": "2026-03-24T10:30:15.123Z",
  
  // MongoDB change stream metadata
  "changeStreamId": {
    "_data": "8269C2678F000000172B042C01..."
  },
  "clusterTime": {
    "$timestamp": "7620767399123456789"
  },
  "wallTime": "2026-03-24T10:30:15.120Z",
  
  // Row hash (RLJSON)
  "_hash": "b5c6d7e8f9g0h1i2..."
}
```

**This gives you:**
- ✅ Content integrity (blockchain chain)
- ✅ Resume capability (resume token)
- ✅ Ordering verification (both)
- ✅ Correlation with MongoDB
- ✅ Tamper detection
- ✅ Debugging support
- ✅ Clock drift detection

---

## Conclusion

**Answer: YES, you should add change stream metadata!**

1. **Your blockchain chain is correct and valuable** - keep it!
2. **MongoDB resume token provides different guarantees** - add it!
3. **Together they complement each other**
4. **Small implementation effort, big benefits**

**Implementation:** Add 3 columns, pass change event to appendOp, done! 🎉
