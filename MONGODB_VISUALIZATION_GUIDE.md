# MongoDB Data Visualization Guide

## 📊 Best Tools for Viewing Your MongoDB Data

### 1. **MongoDB Compass** (Recommended - Official GUI)

**Download & Install:**

```bash
# macOS - Install via Homebrew
brew install --cask mongodb-compass

# Or download from: https://www.mongodb.com/try/download/compass
```

**Connect to Your Docker MongoDB:**

1. Open MongoDB Compass
2. Connection string: `mongodb://localhost:27017`
3. Click "Connect"

**What You'll See:**

- **Databases:** syncdb, CARATDB, test\_\* databases
- **Collections:**
  - `sync_ops` - 66,557+ operations with hash chains
  - `articles` - Your synced articles
  - `sync_state`, `state_merkle`, etc.

**Features:**

- ✅ Visual query builder
- ✅ Aggregation pipeline builder
- ✅ Schema analyzer
- ✅ Performance insights
- ✅ Real-time data updates

---

### 2. **MongoDB VS Code Extension**

**Install:**

1. Open VS Code
2. Go to Extensions (⌘+Shift+X)
3. Search for "MongoDB for VS Code"
4. Install it

**Connect:**

1. Click MongoDB icon in sidebar
2. Add connection: `mongodb://localhost:27017`
3. Browse your databases right in VS Code!

**Features:**

- ✅ IntelliSense for MongoDB queries
- ✅ Run queries from `.mongodb` files
- ✅ Document editing
- ✅ Aggregation playground

---

### 3. **Studio 3T** (Advanced Features)

**Download:** https://studio3t.com/download/

**Features:**

- ✅ SQL to MongoDB query translation
- ✅ Visual query builder
- ✅ Data comparison
- ✅ Import/Export tools

---

## 🔍 Quick Queries to Run

### View Operations with Hash Chains

```javascript
// In MongoDB Compass or mongosh
use syncdb
db.sync_ops.find({}).sort({seq: -1}).limit(10).pretty()
```

### Find Concurrent Operations (Potential Conflicts)

```javascript
db.sync_ops.aggregate([
  {
    $group: {
      _id: '$docId',
      operations: {
        $push: {
          origin: '$origin',
          operationType: '$operationType',
          ts: '$ts',
          seq: '$seq',
        },
      },
      count: { $sum: 1 },
    },
  },
  {
    $match: {
      count: { $gte: 2 },
    },
  },
  {
    $limit: 10,
  },
]);
```

### Get Node Statistics

```javascript
db.sync_ops.aggregate([
  {
    $group: {
      _id: '$origin',
      totalOps: { $sum: 1 },
      lastOperation: { $max: '$ts' },
      operations: {
        $push: '$operationType',
      },
    },
  },
]);
```

### View Hash Chain Integrity

```javascript
db.sync_ops
  .find({
    origin: 'nodeA',
  })
  .sort({ seq: 1 })
  .limit(5)
  .forEach((op) => {
    print(`Seq: ${op.seq}, Hash: ${op.opHash.substring(0, 16)}...`);
    print(`Chain: ${op.chainHash.substring(0, 16)}...`);
    print(`Prev: ${op.prevHash.substring(0, 16)}...`);
    print('---');
  });
```

---

## 🚀 Using the Conflict UI with Real Data

Once you have MongoDB Compass open:

1. **Browse the Data:**
   - Database: `syncdb`
   - Collection: `sync_ops`
   - See all 66,557+ operations

2. **Identify Conflicts:**
   - Look for duplicate `docId` values
   - Check timestamps for concurrent edits
   - Different `origin` (nodeA vs nodeB)

3. **Create Test Conflicts:**

```javascript
// In MongoDB Compass or mongosh
use syncdb

// Simulate two nodes editing same document
db.sync_ops.insertOne({
  _id: "test_conflict_1",
  origin: "nodeA",
  seq: 99999,
  prevHash: "previous",
  opHash: "hash1",
  chainHash: "chain1",
  ns: { db: "syncdb", coll: "articles" },
  operationType: "update",
  docId: ObjectId(),
  payload: {
    fullDocument: {
      title: "Article from Node A",
      content: "Updated by Node A",
      version: 1
    }
  },
  ts: new Date()
});

// Same document, different node, 2 seconds later
db.sync_ops.insertOne({
  _id: "test_conflict_2",
  origin: "nodeB",
  seq: 88888,
  prevHash: "previous",
  opHash: "hash2",
  chainHash: "chain2",
  ns: { db: "syncdb", coll: "articles" },
  operationType: "update",
  docId: ObjectId("same-as-above"),  // Use same docId!
  payload: {
    fullDocument: {
      title: "Article from Node B",
      content: "Updated by Node B",
      version: 2
    }
  },
  ts: new Date(Date.now() + 2000)
});
```

4. **Refresh the UI:**
   - Click "Trigger Sync" button in UI
   - See your test conflict appear!

---

## 📈 Data Visualization Tips

### 1. Operations Timeline

```javascript
// Visualize operations over time
db.sync_ops.aggregate([
  {
    $group: {
      _id: {
        $dateToString: { format: '%Y-%m-%d', date: { $toDate: '$ts' } },
      },
      count: { $sum: 1 },
    },
  },
  { $sort: { _id: 1 } },
]);
```

### 2. Operations by Type

```javascript
db.sync_ops.aggregate([
  {
    $group: {
      _id: '$operationType',
      count: { $sum: 1 },
    },
  },
]);
```

### 3. Node Activity

```javascript
db.sync_ops.aggregate([
  {
    $group: {
      _id: '$origin',
      operations: { $sum: 1 },
      collections: { $addToSet: '$ns.coll' },
    },
  },
]);
```

---

## 🎯 Next Steps

1. **Install MongoDB Compass** - Best visual tool
2. **Connect** to `mongodb://localhost:27017`
3. **Explore `syncdb` database**
4. **Run the queries above** to understand your data
5. **Create test conflicts** to see them in the UI

---

## 🔧 Troubleshooting

**Can't connect to MongoDB?**

```bash
# Check if Docker container is running
docker ps | grep mongo

# Check if port is accessible
nc -zv localhost 27017

# Restart MongoDB container
docker-compose restart mongoa
```

**Slow responses?**

- MongoDB might be under heavy load with 66K+ operations
- Consider adding indexes:
  ```javascript
  db.sync_ops.createIndex({ docId: 1, ts: 1 });
  db.sync_ops.createIndex({ origin: 1, seq: 1 });
  ```

---

## 📚 Learn More

- [MongoDB Compass Docs](https://www.mongodb.com/docs/compass/)
- [MongoDB Aggregation](https://www.mongodb.com/docs/manual/aggregation/)
- [VS Code MongoDB Extension](https://marketplace.visualstudio.com/items?itemName=mongodb.mongodb-vscode)
