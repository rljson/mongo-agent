# Offline Conflict Detection - Implementation Summary

## Overview

Implemented a comprehensive offline conflict detection system that tracks changes made by nodes during offline periods and detects conflicts with locks held by other nodes.

## What Was Implemented

### 1. Enhanced Lock Manager (`src/lock-manager.ts`)

#### New Collections
- **lock_history** - Stores historical records of released locks with acquire/release timestamps
- **offline_changes** - Tracks changes made by nodes while offline
- **sync_conflicts** - Stores detected conflicts for resolution

#### New Interfaces
```typescript
interface LockHistoryRecord extends LockRecord {
  acquiredAt: Date;
  releasedAt: Date;
}

interface OfflineChange {
  _id: string;
  typ: number;
  value: string;
  key: string;
  changeTimestamp: Date;
  changeData: any;
  collection: string;
  database: string;
}
```

#### New Methods
- **recordOfflineChange()** - Record a change made while offline
- **detectOfflineConflicts()** - Detect conflicts between offline changes and lock history
- **createConflictRecords()** - Create conflict records in sync_conflicts collection
- **clearOfflineChanges()** - Clear offline changes after processing
- **getOfflineChanges()** - Get all offline changes for a node
- **cleanLockHistory()** - Clean old lock history records

#### Modified Methods
- **releaseLock()** - Now saves lock to history before deletion
- **initialize()** - Creates indexes for new collections

### 2. E2E Test Suite (`test/e2e/stable/test-offline-conflicts.ts`)

Comprehensive tests covering:
1. **Offline conflict detection** - Verifies conflicts are detected when offline changes overlap with locks
2. **No conflict when no overlap** - Ensures no false positives
3. **Multiple offline conflicts** - Tests batch conflict detection
4. **Lock history cleanup** - Verifies old history is cleaned up

### 3. Unit Tests (`test/lock-manager.spec.ts`)

Added 8 new test suites:
- recordOfflineChange - 1 test
- detectOfflineConflicts - 3 tests
- createConflictRecords - 1 test
- clearOfflineChanges - 1 test
- cleanLockHistory - 2 tests

Total: 23 unit tests for lock manager (15 original + 8 new)

### 4. Documentation

Created comprehensive documentation:
- **OFFLINE_CONFLICT_DETECTION.md** - Complete system documentation with:
  - Architecture overview
  - Workflow diagrams
  - API documentation
  - Code examples
  - Best practices
  - Troubleshooting guide

### 5. Package Scripts

Added new test scripts to `package.json`:
```json
"test:e2e:locking": "tsx test/e2e/stable/test-record-locking.ts"
"test:e2e:offline": "tsx test/e2e/stable/test-offline-conflicts.ts"
```

## How It Works

### Conflict Detection Flow

```
1. Node A locks record
   └─> Lock created in `locking` collection

2. Node B goes offline
   └─> Makes change to same record
   └─> Change recorded in `offline_changes` collection

3. Node A releases lock
   └─> Lock copied to `lock_history` with timestamps
   └─> Active lock deleted

4. Node B comes online
   └─> Calls detectOfflineConflicts()
   └─> System compares offline_changes timestamps with lock_history
   └─> Finds overlap: change was made during lock period

5. Conflict detected
   └─> createConflictRecords() creates record in `sync_conflicts`
   └─> UI displays conflict for resolution
```

### Conflict Detection Logic

A conflict exists when ALL conditions are true:
- Offline change exists for a record
- Lock history exists for the same record
- Lock was held by a **different** node
- Change timestamp falls **within** lock period:
  ```
  lock.acquiredAt <= change.changeTimestamp <= lock.releasedAt
  ```

## Testing

### Run E2E Tests

```bash
# Test basic locking
pnpm test:e2e:locking

# Test offline conflict detection
pnpm test:e2e:offline
```

### Run Unit Tests

```bash
# All tests
pnpm test

# Just lock manager tests (requires MongoDB running)
pnpm vitest run test/lock-manager.spec.ts
```

## Files Modified

- `src/lock-manager.ts` - Enhanced with offline conflict detection
- `test/lock-manager.spec.ts` - Added 8 new test suites
- `package.json` - Added test scripts

## Files Created  - `test/e2e/stable/test-offline-conflicts.ts` - E2E test suite
- `OFFLINE_CONFLICT_DETECTION.md` - Complete documentation

## API Example

### Basic Usage

```typescript
// Node A locks and modifies record
await lockManager.acquireLock({
  typ: EntityType.USERS,
  value: 'user-123',
  key: 'nodeA',
  name: 'Node A',
  compName: 'SERVER-A'
});

// Node B goes offline, records change
await lockManager.recordOfflineChange(
  EntityType.USERS,
  'user-123',
  'nodeB',
  { name: 'Updated Name' },
  'users',
  'mydb'
);

// Node A releases lock (saved to history)
await lockManager.releaseLock(EntityType.USERS, 'user-123', 'nodeA');

// Node B comes online
const conflicts = await lockManager.detectOfflineConflicts('nodeB');
// conflicts.length === 1

// Create conflict records for UI resolution
await lockManager.createConflictRecords(conflicts);

// Clear processed offline changes
await lockManager.clearOfflineChanges('nodeB');
```

## Database Schema

### lock_history Collection

```javascript
{
  _id: "8-user-123",
  typ: 8,
  value: "user-123",
  key: "nodeA",
  name: "Node A",
  compName: "SERVER-A",
  acquiredAt: ISODate("2026-04-15T10:00:00Z"),
  releasedAt: ISODate("2026-04-15T10:05:00Z"),
  commonFields: { ... }
}
```

### offline_changes Collection

```javascript
{
  _id: "8-user-123-1713178500000",
  typ: 8,
  value: "user-123",
  key: "nodeB",
  changeTimestamp: ISODate("2026-04-15T10:03:00Z"),
  changeData: { name: "Updated Name" },
  collection: "users",
  database: "mydb"
}
```

### sync_conflicts Collection

```javascript
{
  conflictId: "offline-8-user-123-1713178500000",
  documentId: "user-123",
  collection: "users",
  database: "mydb",
  detectedAt: 1713178600000,
  status: "pending",
  conflictType: "offline-lock-conflict",
  offlineChange: {
    nodeId: "nodeB",
    timestamp: 1713178500000,
    data: { name: "Updated Name" }
  },
  lockInfo: {
    lockedBy: "nodeA",
    lockedByName: "Node A",
    acquiredAt: 1713178400000,
    releasedAt: 1713178500000
  }
}
```

## Performance Considerations

- All collections are properly indexed
- Lock history queries use indexed timestamps
- Offline change queries use indexed key + timestamp
- Recommend periodic cleanup of old lock history (7-30 days)

## Integration Points

- **UI Conflict Resolver** - Displays offline-lock-conflicts for manual resolution
- **Sync System** - Records offline changes during sync failures
- **Agent Server** - Detects conflicts on reconnection

## Next Steps

To integrate this into your application:

1. **Enable lock history tracking** - Already done when using LockManager
2. **Record offline changes** - Call `recordOfflineChange()` when sync fails
3. **Detect on reconnect** - Call `detectOfflineConflicts()` when node comes online
4. **Create conflicts** - Call `createConflictRecords()` for UI resolution
5. **Schedule cleanup** - Periodically run `cleanLockHistory()` to prevent unbounded growth

## Maintenance

Recommended cleanup schedule:
```typescript
// Daily: Clean lock history older than 7 days
const sevenDays = 7 * 24 * 60 * 60 * 1000;
await lockManager.cleanLockHistory(sevenDays);
```
