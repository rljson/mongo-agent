# Record Locking System

## Overview

The Record Locking system provides distributed lock management for MongoDB records across multiple nodes. It prevents concurrent edits by ensuring only one node can modify a record at a time.

## Features

- **Distributed Locking**: Lock records across multiple nodes/clients
- **Re-entrant Locks**: Same node can re-acquire its own locks
- **Lock Ownership**: Only the lock owner can modify or release locked records
- **Automatic Cleanup**: Remove stale locks based on age
- **Entity Type Mapping**: Support for different entity types (users, orders, products, etc.)

## Usage

### Basic Example

```typescript
import { MongoClient } from 'mongodb';
import { createLockManager, EntityType } from '@rljson/mongo-agent';

// Connect to MongoDB
const client = new MongoClient('mongodb://localhost:27017');
await client.connect();
const db = client.db('mydb');

// Create lock manager
const lockManager = createLockManager(db);
await lockManager.initialize();

// Acquire a lock
const acquired = await lockManager.acquireLock({
  typ: EntityType.USERS,
  value: 'user-123',
  key: 'node-a',
  name: 'Node A',
  compName: 'SERVER-A-01',
  eMail: 'node-a@example.com',
});

if (acquired) {
  console.log('Lock acquired, can edit user');

  // Perform modifications...
  await db
    .collection('users')
    .updateOne({ _id: 'user-123' }, { $set: { name: 'Updated Name' } });

  // Release lock
  await lockManager.releaseLock(EntityType.USERS, 'user-123', 'node-a');
} else {
  console.log('Lock already held by another node');
}
```

### Check if Modification is Allowed

```typescript
// Check if record can be modified
const canModify = await lockManager.canModify(
  EntityType.USERS,
  'user-123',
  'node-a'
);

if (canModify) {
  // Safe to modify
  await db.collection('users').updateOne(...);
}
```

### Lock Management

```typescript
// Check if a record is locked
const lock = await lockManager.isLocked(EntityType.USERS, 'user-123');
if (lock) {
  console.log(`Locked by: ${lock.name} (${lock.key})`);
  console.log(`Computer: ${lock.compName}`);
}

// Get all locks held by a node
const locks = await lockManager.getLocksBy('node-a');
console.log(`Node A holds ${locks.length} locks`);

// Release all locks for a node (useful on disconnect)
const released = await lockManager.releaseAllLocks('node-a');
console.log(`Released ${released} locks`);

// Remove stale locks (older than 1 hour)
const removed = await lockManager.removeOldLocks(3600000);
console.log(`Removed ${removed} stale locks`);
```

## Lock Record Structure

Locks are stored in the `locking` collection with the following structure:

```typescript
{
  "_id": "8-user-123",          // Format: typ-value
  "typ": 8,                     // Entity type (8 = users)
  "value": "user-123",          // Record ID
  "key": "node-a",              // Node/user key
  "name": "Node A",             // Display name
  "telefone": "",               // Optional phone
  "eMail": "node-a@example.com",// Optional email
  "compName": "SERVER-A-01",    // Computer name
  "clientName": "",             // Optional client name
  "commonFields": {
    "createdBy": "node-a",
    "updatedBy": "node-a",
    "status": 0,
    "version": 1,
    "recordNo": 0,
    "createdAt": ISODate("2026-04-14T..."),
    "updatedAt": ISODate("2026-04-14T...")
  }
}
```

## Entity Types

Predefined entity types:

```typescript
EntityType.USERS; // 8
EntityType.ORDERS; // 9
EntityType.PRODUCTS; // 10
EntityType.ARTICLES; // 11
```

Add custom types as needed:

```typescript
const MY_ENTITY = 12;
```

## Testing

### Unit Tests

```bash
pnpm exec vitest run test/lock-manager.spec.ts
```

### E2E Tests

```bash
pnpm exec tsx test/e2e/stable/test-record-locking.ts
```

The e2e test simulates:

1. Node A locks a user
2. Node B tries to edit (blocked)
3. Node A releases lock
4. Node B can now edit (succeeds)

## Integration with Sync

When integrating with sync operations:

```typescript
async function updateRecord(userId: string, updates: any, nodeKey: string) {
  const lockManager = createLockManager(db);

  // Try to acquire lock
  const acquired = await lockManager.acquireLock({
    typ: EntityType.USERS,
    value: userId,
    key: nodeKey,
    name: `Node ${nodeKey}`,
    compName: process.env.HOSTNAME || 'unknown',
  });

  if (!acquired) {
    throw new Error('Record is locked by another node');
  }

  try {
    // Perform update
    await db.collection('users').updateOne({ _id: userId }, { $set: updates });
  } finally {
    // Always release lock
    await lockManager.releaseLock(EntityType.USERS, userId, nodeKey);
  }
}
```

## Best Practices

1. **Always Release Locks**: Use try/finally to ensure locks are released
2. **Handle Lock Failures**: Check return values and handle lock acquisition failures
3. **Cleanup on Disconnect**: Call `releaseAllLocks()` when a node disconnects
4. **Monitor Stale Locks**: Periodically call `removeOldLocks()` to clean up orphaned locks
5. **Use Re-entrant Locks**: Same node can safely re-acquire its own locks

## API Reference

### LockManager

#### `acquireLock(options: LockOptions): Promise<boolean>`

Acquire a lock on a record. Returns true if successful.

#### `releaseLock(typ: number, value: string, key: string): Promise<boolean>`

Release a lock. Only the owner can release. Returns true if successful.

#### `isLocked(typ: number, value: string): Promise<LockRecord | null>`

Check if a record is locked. Returns lock record or null.

#### `canModify(typ: number, value: string, key: string): Promise<boolean>`

Check if a node can modify a record. Returns true if allowed.

#### `getLocksBy(key: string): Promise<LockRecord[]>`

Get all locks held by a specific node.

#### `releaseAllLocks(key: string): Promise<number>`

Release all locks for a node. Returns count of locks released.

#### `removeOldLocks(maxAgeMs: number): Promise<number>`

Remove locks older than specified age. Returns count removed.

#### `acquireLockWithRetry(options, maxRetries?, retryDelayMs?): Promise<boolean>`

Attempt to acquire lock with automatic retry.

## Troubleshooting

### Lock Not Released

If a node crashes before releasing locks, use `removeOldLocks()` to clean up:

```typescript
// Remove locks older than 30 minutes
await lockManager.removeOldLocks(30 * 60 * 1000);
```

### Lock Contention

If multiple nodes frequently contend for the same locks, consider:

- Implementing a lock queue
- Using exponential backoff with `acquireLockWithRetry()`
- Reducing lock hold time

### Verification

Check locks in MongoDB:

```bash
db.locking.find().pretty()
```
