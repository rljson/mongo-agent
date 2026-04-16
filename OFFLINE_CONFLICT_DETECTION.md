# Offline Conflict Detection

## Overview

The offline conflict detection system tracks changes made by nodes during offline periods and detects conflicts with locks held by other nodes. When a node comes back online, it can identify if its offline changes conflicted with records that were locked by other nodes during the offline period.

## Architecture

### Collections

1. **locking** - Active locks currently held by nodes
2. **lock_history** - Historical record of all released locks
3. **offline_changes** - Changes made by nodes while offline
4. **sync_conflicts** - Detected conflicts requiring resolution

### Lock History Tracking

When a lock is released, it's not immediately deleted. Instead:
1. The lock record is copied to `lock_history` with timestamps:
   - `acquiredAt` - When the lock was first acquired
   - `releasedAt` - When the lock was released
2. The active lock is then removed from the `locking` collection

This history enables offline conflict detection by allowing comparison of offline change timestamps with lock periods.

## Workflow

### Scenario: Node goes offline and makes conflicting changes

```
Timeline:

T1: Node A acquires lock on user-123
T2: Node B goes offline
T3: Node B makes offline change to user-123 (recorded in offline_changes)
T4: Node A releases lock (record saved to lock_history)
T5: Node B comes back online
T6: System detects conflict (offline change overlaps with lock period)
T7: Conflict record created in sync_conflicts for manual resolution
```

### API Usage

#### 1. Record an Offline Change

When a node is offline and makes a change:

```typescript
await lockManager.recordOfflineChange(
  EntityType.USERS,        // Entity type
  'user-123',              // Record ID
  'nodeB',                 // Node making the change
  { name: 'New Name' },    // Change data
  'users',                 // Collection name
  'mydb'                   // Database name
);
```

#### 2. Detect Conflicts When Coming Back Online

```typescript
const conflicts = await lockManager.detectOfflineConflicts('nodeB');

// Returns array of conflicts:
// [
//   {
//     change: OfflineChange,      // The offline change
//     lock: LockHistoryRecord      // The conflicting lock
//   }
// ]
```

#### 3. Create Conflict Records

```typescript
const conflictCount = await lockManager.createConflictRecords(conflicts);

// Creates records in sync_conflicts collection for UI resolution
```

#### 4. Clear Offline Changes

After processing conflicts:

```typescript
const cleared = await lockManager.clearOfflineChanges('nodeB');
```

## Conflict Detection Logic

A conflict is detected when ALL of these conditions are true:

1. An offline change exists for a record
2. A lock history entry exists for the same record
3. The lock was held by a **different** node
4. The offline change timestamp falls **within** the lock period:
   ```
   lock.acquiredAt <= change.changeTimestamp <= lock.releasedAt
   ```

No conflict is detected when:
- The offline change was made **after** the lock was released
- The offline change was made by the **same node** that held the lock
- No lock existed for that record during the offline period

## Data Structures

### OfflineChange

```typescript
interface OfflineChange {
  _id: string;              // Unique ID
  typ: number;              // Entity type
  value: string;            // Record ID
  key: string;              // Node that made change
  changeTimestamp: Date;    // When change was made
  changeData: any;          // The actual change
  collection: string;       // Collection name
  database: string;         // Database name
}
```

### LockHistoryRecord

```typescript
interface LockHistoryRecord extends LockRecord {
  acquiredAt: Date;         // When lock was acquired
  releasedAt: Date;         // When lock was released
}
```

### Conflict Record (in sync_conflicts)

```typescript
{
  conflictId: 'offline-...',
  documentId: 'user-123',
  collection: 'users',
  database: 'mydb',
  detectedAt: 1234567890,
  status: 'pending',
  conflictType: 'offline-lock-conflict',
  offlineChange: {
    nodeId: 'nodeB',
    timestamp: 1234567890,
    data: { ... }
  },
  lockInfo: {
    lockedBy: 'nodeA',
    lockedByName: 'Node A',
    acquiredAt: 1234567880,
    releasedAt: 1234567895
  },
  versions: [ ... ]
}
```

## Maintenance

### Clean Old Lock History

Lock history grows over time. Clean old records periodically:

```typescript
// Remove lock history older than 7 days
const maxAge = 7 * 24 * 60 * 60 * 1000; // 7 days in ms
const cleaned = await lockManager.cleanLockHistory(maxAge);
```

### Best Practices

1. **Clean lock history regularly** - Schedule cleanup jobs to prevent unbounded growth
2. **Process offline changes promptly** - When a node comes online, detect and resolve conflicts immediately
3. **Clear processed offline changes** - After conflict detection, clear offline changes to avoid reprocessing
4. **Monitor conflict rates** - High conflict rates may indicate network instability or coordination issues

## Testing

Run the end-to-end test suite:

```bash
pnpm test:e2e:offline-conflicts
```

Run unit tests:

```bash
pnpm test test/lock-manager.spec.ts
```

## Examples

### Example 1: Basic Offline Conflict

```typescript
// Node A locks user
await lockManager.acquireLock({
  typ: EntityType.USERS,
  value: 'user-123',
  key: 'nodeA',
  name: 'Node A',
  compName: 'SERVER-A'
});

// Node B goes offline, makes change
await lockManager.recordOfflineChange(
  EntityType.USERS,
  'user-123',
  'nodeB',
  { email: 'newemail@example.com' },
  'users',
  'mydb'
);

// Node A releases lock
await lockManager.releaseLock(EntityType.USERS, 'user-123', 'nodeA');

// Node B comes online
const conflicts = await lockManager.detectOfflineConflicts('nodeB');
// conflicts.length === 1

await lockManager.createConflictRecords(conflicts);
// Conflict created in sync_conflicts collection

await lockManager.clearOfflineChanges('nodeB');
// Offline changes cleared
```

### Example 2: No Conflict (Change After Lock)

```typescript
// Node A locks and releases
await lockManager.acquireLock({ ... });
await lockManager.releaseLock(...);

// Wait some time...
await new Promise(resolve => setTimeout(resolve, 1000));

// Node B makes offline change AFTER lock was released
await lockManager.recordOfflineChange(...);

// No conflict detected
const conflicts = await lockManager.detectOfflineConflicts('nodeB');
// conflicts.length === 0
```

## Integration with UI

The conflict resolver UI displays offline-lock-conflicts with:
- **Offline Change** - Shows what the offline node tried to change
- **Lock Info** - Shows which node had the lock and when
- **Resolution Options** - Allow user to choose offline change or discard it

See `ui-conflict-resolver` for the conflict resolution interface.

## Performance Considerations

- Lock history queries are indexed on `acquiredAt` and `releasedAt`
- Offline change queries are indexed on `key` and `changeTimestamp`
- For high-volume systems, consider:
  - Partitioning lock history by time
  - Archiving old lock history to separate collection
  - Batch processing of offline changes

## Troubleshooting

### Conflicts not detected

1. Verify lock history is being created:
   ```typescript
   const history = await db.collection('lock_history').find().toArray();
   console.log('Lock history:', history);
   ```

2. Check offline change timestamps:
   ```typescript
   const changes = await lockManager.getOfflineChanges('nodeId');
   console.log('Offline changes:', changes);
   ```

3. Verify timestamp overlap:
   - Ensure offline change timestamp falls within lock period
   - Check system clocks are synchronized across nodes

### Lock history growing too large

Implement regular cleanup:
```typescript
// Daily cleanup job
setInterval(async () => {
  const sevenDays = 7 * 24 * 60 * 60 * 1000;
  await lockManager.cleanLockHistory(sevenDays);
}, 24 * 60 * 60 * 1000); // Run daily
```
