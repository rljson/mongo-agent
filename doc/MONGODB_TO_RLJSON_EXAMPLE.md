# MongoDB → RLJSON Transformation Example

## 📊 BEFORE: MongoDB Document

```javascript
// Original MongoDB document
{
  "_id": "123",
  "title": "The Great Gatsby",
  "author": "F. Scott Fitzgerald",
  "year": 1925,
  "price": 12.99,
  "inStock": true,
  "tags": ["classic", "fiction", "american"],
  "publisher": {
    "name": "Scribner",
    "country": "USA"
  }
}
```

**MongoDB Characteristics:**

- 💾 Format: BSON (Binary JSON)
- 🔧 Types: MongoDB-specific (ObjectId, nested objects)
- 📁 Storage: MongoDB collection
- 📋 Schema: Implicit (dynamic, no enforcement)
- 🔗 Dependencies: Requires MongoDB to read
- 🏗️ Structure: Nested documents

---

## 📊 AFTER: RLJSON Format

### 1️⃣ TableCfg (Schema)

```javascript
{
  "key": "books",
  "type": "components",
  "isHead": false,
  "isRoot": false,
  "isShared": true,
  "_hash": "Rdd6kD_5FURkjH9UhKy95P",
  "columns": [
    { "key": "_hash", "type": "string", "titleLong": "Hash" },
    { "key": "_id", "type": "string", "titleLong": " Id" },
    { "key": "author", "type": "string", "titleLong": "Author" },
    { "key": "inStock", "type": "boolean", "titleLong": "InStock" },
    { "key": "price", "type": "number", "titleLong": "Price" },
    { "key": "publisher", "type": "json", "titleLong": "Publisher" },
    { "key": "tags", "type": "jsonArray", "titleLong": "Tags" },
    { "key": "title", "type": "string", "titleLong": "Title" },
    { "key": "year", "type": "number", "titleLong": "Year" }
  ]
}
```

### 2️⃣ ComponentsTable (Data)

```javascript
{
  "_tableCfg": "Rdd6kD_5FURkjH9UhKy95P",  // ← Points to TableCfg by hash
  "_type": "components",
  "_data": [
    {
      "_hash": "42iv101PeFeoO2Oj443SyJ",  // ← Content-based hash
      "_id": "123",
      "title": "The Great Gatsby",
      "author": "F. Scott Fitzgerald",
      "year": 1925,
      "price": 12.99,
      "inStock": true,
      "tags": ["classic", "fiction", "american"],
      "publisher": {
        "name": "Scribner",
        "country": "USA",
        "_hash": "-oUzkGPxt1tRgzGS7vpCgd"  // ← Even nested objects are hashed
      }
    }
    // ... more rows
  ]
}
```

### 3️⃣ Storage Structure

```
MongoDB:
  └─ sync_state collection
      └─ tableCfgsTableBlobId: "Khx-yf4RPjFIBZEW379E..."
      └─ references → blobs

Blob Storage:
  ├─ Blob: "qFpG90KuxnGm8mvBTCgv..."
  │   └─ Contains: ComponentsTable (books data)
  │
  └─ Blob: "Khx-yf4RPjFIBZEW379E..."
      └─ Contains: TablesCfgTable (all schemas)
          └─ books TableCfg
          └─ sync_ops TableCfg
```

**RLJSON Characteristics:**

- 💾 Format: Standard JSON
- 🔧 Types: Universal (string, number, boolean, json, jsonArray)
- 📁 Storage: Content-addressable blob storage
- 📋 Schema: Explicit TableCfg with hash
- 🔗 Dependencies: Standalone (blobs + schemas = complete)
- 🏗️ Structure: Tabular with typed columns

---

## 🔄 The Transformation Process

```
┌─────────────────────┐
│  MongoDB Document   │  ← Original BSON document
└──────────┬──────────┘
           │
           ↓ (1) Schema Discovery
┌─────────────────────┐
│     TableCfg        │  ← Column types inferred
└──────────┬──────────┘
           │
           ↓ (2) Type Conversion
┌─────────────────────┐
│  ComponentsTable    │  ← Normalized data with _hash on each row
└──────────┬──────────┘
           │
           ↓ (3) Serialization
┌─────────────────────┐
│    JSON String      │  ← Standard JSON format
└──────────┬──────────┘
           │
           ↓ (4) Content-Addressed Storage
┌─────────────────────┐
│   Blob Storage      │  ← Immutable, addressable by hash
└─────────────────────┘
```

---

## 🎯 Key Differences

| Aspect          | MongoDB            | RLJSON                     |
| --------------- | ------------------ | -------------------------- |
| **Format**      | BSON               | JSON                       |
| **Types**       | MongoDB-specific   | Universal                  |
| **Schema**      | Implicit (dynamic) | Explicit (TableCfg)        |
| **Storage**     | Collection         | Content-addressable blobs  |
| **Hashing**     | \_id only          | Every row + nested objects |
| **Portability** | MongoDB-dependent  | Database-independent       |
| **Integrity**   | Basic              | Hash-based verification    |
| **Versioning**  | Not built-in       | Schema hash tracking       |

---

## ✨ What RLJSON Adds

### 1. Database Independence

```javascript
// Same RLJSON format works for any database
MongoDB → ComponentsTable → PostgreSQL
MySQL  → ComponentsTable → SQLite
```

### 2. Schema Tracking

```javascript
// Know when structure changes
v1: TableCfg._hash = 'abc123...';
v2: TableCfg._hash = 'xyz789...'; // ← New column added
```

### 3. Content Integrity

```javascript
// Every row is hashed
row._hash = hash(row data)
// Detect corruption or tampering
```

### 4. Self-Describing

```javascript
// Data + Schema = Complete picture
blobId → ComponentsTable (data)
         ._tableCfg → TableCfg (schema)
```

### 5. Sync Protocol

```javascript
// Designed for replication
sync_ops → ComponentsTable
          → prevHash → chainHash (blockchain)
```

---

## 📝 Example: Retrieving Data

```javascript
// Step 1: Load ComponentsTable from blob
const componentsTable = await bs.getBlob(blobId);
// {
//   _tableCfg: "Rdd6kD_5FURkjH9UhKy95P",
//   _type: "components",
//   _data: [...]
// }

// Step 2: Load TablesCfgTable
const tableCfgsTable = await bs.getBlob(tableCfgsTableBlobId);

// Step 3: Find matching TableCfg by hash
const tableCfg = tableCfgsTable._data.find(
  (cfg) => cfg._hash === componentsTable._tableCfg,
);

// Step 4: Now you have both data and schema!
console.log(tableCfg.columns); // Know the structure
console.log(componentsTable._data); // Access the data

// No MongoDB needed! Just blobs + JSON parsing.
```

---

## 🚀 Benefits Summary

✅ **Portability** - Move data between different databases
✅ **Immutability** - Content-addressed blobs never change
✅ **Integrity** - Hash-based verification of data
✅ **Versioning** - Track schema evolution via hashes
✅ **Independence** - No MongoDB required to read data
✅ **Sync-friendly** - Built for distributed replication
✅ **Self-describing** - Schema always travels with data

---

## 🔍 Real-World Use Cases

### Use Case 1: Cross-Database Sync

```
MongoDB (Source) → RLJSON → PostgreSQL (Target)
```

### Use Case 2: Data Archiving

```
Live MongoDB → RLJSON blobs → S3 archive
(Can restore without MongoDB)
```

### Use Case 3: Schema Evolution

```
v1: books (4 columns) → hash: "abc123"
v2: books (6 columns) → hash: "xyz789"
Track changes, maintain compatibility
```

### Use Case 4: Data Integrity

```
Original: row._hash = "abc123"
Received: row._hash = "xyz789"
→ Detect corruption or tampering
```

---

## 🎬 Conclusion

RLJSON transforms MongoDB documents into a universal, portable format that:

- Works with any database
- Includes explicit schema information
- Provides content-based integrity checking
- Enables distributed synchronization
- Remains human-readable (JSON)

**The transformation is complete, tested, and production-ready!** ✅
