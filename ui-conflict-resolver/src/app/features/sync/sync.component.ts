import { CommonModule } from '@angular/common';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { RouterLink } from '@angular/router';
import { interval, Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { formatDistanceToNow } from 'date-fns';

type SyncStatus = 'syncing' | 'idle' | 'error' | 'disconnected';
type CollectionSyncStatus = 'synced' | 'syncing' | 'pending' | 'error';

interface MongoAgentInfo {
  uri: string;
  treeKey: string;
  status: SyncStatus;
  connected: boolean;
  lastSeq: number;
  lastHash: string;
  lastSyncAt: Date | null;
  collectionCount: number;
  databases: string[];
}

interface SyncOrigin {
  nodeId: string;
  origin: string;
  lastSeqPulled: number;
  lastHashPulled: string;
  lastSeqApplied: number;
  lastHashApplied: string;
  updatedAt: Date;
  status: 'synced' | 'pending' | 'error';
}

interface SyncOperation {
  seq: number;
  origin: string;
  operationType: 'insert' | 'update' | 'replace' | 'delete';
  collection: string;
  database: string;
  timestamp: Date;
  chainHash: string;
}

interface CollectionInfo {
  name: string;
  database: string;
  documentCount: number;
  lastModified: Date;
  merkleHash?: string;
  syncStatus?: CollectionSyncStatus;
  lastSyncAt?: Date;
}

interface DatabaseTreeNode {
  name: string;
  collections: CollectionInfo[];
  expanded: boolean;
  syncStatus: CollectionSyncStatus;
  totalDocuments: number;
  lastSyncAt: Date | null;
}

@Component({
  selector: 'app-sync',
  standalone: true,
  imports: [CommonModule, RouterLink],
  templateUrl: './sync.component.html',
  styleUrls: ['./sync.component.scss']
})
export class SyncComponent implements OnInit, OnDestroy {
  // MongoDB Agent state
  mongoAgent: MongoAgentInfo = {
    uri: 'mongodb://mongoa:27017',
    treeKey: 'mongoTree',
    status: 'syncing',
    connected: true,
    lastSeq: 3070,
    lastHash: 'abc12ef3456789def',
    lastSyncAt: new Date(Date.now() - 5000),
    collectionCount: 5,
    databases: ['syncdb', 'rljson-sync']
  };

  // Sync origins
  syncOrigins: SyncOrigin[] = [
    {
      nodeId: 'node-b2e4-d9',
      origin: 'nodeB',
      lastSeqPulled: 2072,
      lastHashPulled: 'xyz789abc123def456',
      lastSeqApplied: 2072,
      lastHashApplied: 'xyz789abc123def456',
      updatedAt: new Date(Date.now() - 2000),
      status: 'synced'
    },
    {
      nodeId: 'node-c7f2-a1',
      origin: 'nodeC',
      lastSeqPulled: 1523,
      lastHashPulled: 'def456789abc123xyz',
      lastSeqApplied: 1520,
      lastHashApplied: 'abc123xyz789def456',
      updatedAt: new Date(Date.now() - 15000),
      status: 'pending'
    }
  ];

  // Recent sync operations
  recentOperations: SyncOperation[] = [
    {
      seq: 3070,
      origin: 'local',
      operationType: 'insert',
      collection: 'conflicts',
      database: 'syncdb',
      timestamp: new Date(Date.now() - 5000),
      chainHash: 'abc12ef3456789def'
    },
    {
      seq: 3069,
      origin: 'nodeB',
      operationType: 'update',
      collection: 'users',
      database: 'rljson-sync',
      timestamp: new Date(Date.now() - 12000),
      chainHash: 'def456789abc123xyz'
    },
    {
      seq: 3068,
      origin: 'local',
      operationType: 'replace',
      collection: 'orders',
      database: 'rljson-sync',
      timestamp: new Date(Date.now() - 18000),
      chainHash: 'xyz789def456abc123'
    },
    {
      seq: 3067,
      origin: 'nodeC',
      operationType: 'delete',
      collection: 'articles',
      database: 'syncdb',
      timestamp: new Date(Date.now() - 25000),
      chainHash: '123abc456def789xyz'
    }
  ];

  // Collections being synced
  collections: CollectionInfo[] = [
    {
      name: 'sync_conflicts',
      database: 'syncdb',
      documentCount: 5,
      lastModified: new Date(Date.now() - 5000),
      merkleHash: 'a1b2c3d4e5f6',
      syncStatus: 'synced',
      lastSyncAt: new Date(Date.now() - 5000)
    },
    {
      name: 'users',
      database: 'rljson-sync',
      documentCount: 142,
      lastModified: new Date(Date.now() - 12000),
      merkleHash: 'f6e5d4c3b2a1',
      syncStatus: 'syncing',
      lastSyncAt: new Date(Date.now() - 3000)
    },
    {
      name: 'orders',
      database: 'rljson-sync',
      documentCount: 2834,
      lastModified: new Date(Date.now() - 18000),
      merkleHash: '9f8e7d6c5b4a',
      syncStatus: 'synced',
      lastSyncAt: new Date(Date.now() - 18000)
    },
    {
      name: 'products',
      database: 'rljson-sync',
      documentCount: 567,
      lastModified: new Date(Date.now() - 45000),
      merkleHash: '3a2b1c0d9e8f',
      syncStatus: 'synced',
      lastSyncAt: new Date(Date.now() - 45000)
    },
    {
      name: 'articles',
      database: 'syncdb',
      documentCount: 89,
      lastModified: new Date(Date.now() - 120000),
      merkleHash: '7g6h5i4j3k2l',
      syncStatus: 'pending',
      lastSyncAt: new Date(Date.now() - 120000)
    },
    {
      name: 'inventory',
      database: 'rljson-sync',
      documentCount: 1243,
      lastModified: new Date(Date.now() - 8000),
      merkleHash: 'x9y8z7w6v5u4',
      syncStatus: 'syncing',
      lastSyncAt: new Date(Date.now() - 2000)
    }
  ];

  // Tree structure for databases and collections
  databaseTree: DatabaseTreeNode[] = [];

  // UI state
  selectedTab: 'overview' | 'origins' | 'operations' | 'collections' = 'overview';
  expandedAgent = true;

  private destroy$ = new Subject<void>();

  ngOnInit(): void {
    // Build initial tree structure
    this.buildDatabaseTree();

    // Simulate real-time updates
    interval(5000)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        // Update last sync time
        this.mongoAgent.lastSyncAt = new Date();
        this.mongoAgent.lastSeq++;
        
        // Randomly update an origin
        if (Math.random() > 0.5 && this.syncOrigins.length > 0) {
          const origin = this.syncOrigins[Math.floor(Math.random() * this.syncOrigins.length)];
          origin.lastSeqPulled++;
          origin.lastSeqApplied = origin.lastSeqPulled;
          origin.updatedAt = new Date();
          origin.status = 'synced';
        }
      });

    // Simulate collection syncing
    interval(3000)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        this.simulateCollectionSync();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  // Actions
  rescanAgent(): void {
    console.log('Re-scanning MongoDB agent...');
    this.mongoAgent.status = 'syncing';
    setTimeout(() => {
      this.mongoAgent.status = 'syncing';
      this.mongoAgent.lastSyncAt = new Date();
    }, 1000);
  }

  resetAgent(): void {
    if (confirm('Reset MongoDB agent state? This will clear all sync data.')) {
      console.log('Resetting MongoDB agent state...');
      this.mongoAgent.lastSeq = 0;
      this.mongoAgent.lastHash = 'GENESIS';
      this.mongoAgent.lastSyncAt = null;
    }
  }

  browseCollections(): void {
    this.selectedTab = 'collections';
  }

  syncOrigin(origin: SyncOrigin): void {
    console.log('Manually syncing origin:', origin.origin);
    origin.status = 'pending';
    setTimeout(() => {
      origin.lastSeqPulled += Math.floor(Math.random() * 10);
      origin.lastSeqApplied = origin.lastSeqPulled;
      origin.updatedAt = new Date();
      origin.status = 'synced';
    }, 1500);
  }

  viewOriginDetails(origin: SyncOrigin): void {
    console.log('View origin details:', origin);
    // Could open a modal or navigate to details page
  }

  // Helpers
  formatDistanceToNow(date: Date | null): string {
    if (!date) return 'Never';
    return formatDistanceToNow(date, { addSuffix: true });
  }

  getStatusIcon(status: SyncStatus): string {
    const icons = {
      syncing: '🔄',
      idle: '⏸️',
      error: '❌',
      disconnected: '🔌'
    };
    return icons[status];
  }

  getStatusClass(status: SyncStatus | SyncOrigin['status']): string {
    return `status-${status}`;
  }

  getOperationIcon(type: SyncOperation['operationType']): string {
    const icons = {
      insert: '➕',
      update: '✏️',
      replace: '🔄',
      delete: '🗑️'
    };
    return icons[type];
  }

  shortenHash(hash: string): string {
    return hash ? `${hash.substring(0, 6)}…${hash.substring(hash.length - 6)}` : 'N/A';
  }

  formatTimestamp(date: Date): string {
    return date.toLocaleTimeString('en-US', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    });
  }

  // Tree structure methods
  buildDatabaseTree(): void {
    const dbMap = new Map<string, CollectionInfo[]>();
    
    // Group collections by database
    this.collections.forEach(coll => {
      if (!dbMap.has(coll.database)) {
        dbMap.set(coll.database, []);
      }
      dbMap.get(coll.database)!.push(coll);
    });

    // Build tree nodes
    this.databaseTree = Array.from(dbMap.entries()).map(([dbName, collections]) => {
      const totalDocs = collections.reduce((sum, c) => sum + c.documentCount, 0);
      const lastSync = collections.reduce((latest, c) => {
        const cSync = c.lastSyncAt || c.lastModified;
        return !latest || cSync > latest ? cSync : latest;
      }, null as Date | null);
      
      // Determine database status based on collections
      let dbStatus: CollectionSyncStatus = 'synced';
      if (collections.some(c => c.syncStatus === 'syncing')) {
        dbStatus = 'syncing';
      } else if (collections.some(c => c.syncStatus === 'pending')) {
        dbStatus = 'pending';
      } else if (collections.some(c => c.syncStatus === 'error')) {
        dbStatus = 'error';
      }

      return {
        name: dbName,
        collections: collections.sort((a, b) => a.name.localeCompare(b.name)),
        expanded: true,
        syncStatus: dbStatus,
        totalDocuments: totalDocs,
        lastSyncAt: lastSync
      };
    }).sort((a, b) => a.name.localeCompare(b.name));
  }

  toggleDatabase(db: DatabaseTreeNode): void {
    db.expanded = !db.expanded;
  }

  syncCollection(collection: CollectionInfo): void {
    console.log('Manually syncing collection:', collection.name);
    collection.syncStatus = 'syncing';
    
    setTimeout(() => {
      collection.syncStatus = 'synced';
      collection.lastSyncAt = new Date();
      collection.lastModified = new Date();
      this.buildDatabaseTree(); // Rebuild tree to update database status
    }, 2000);
  }

  syncDatabase(db: DatabaseTreeNode): void {
    console.log('Syncing entire database:', db.name);
    db.collections.forEach(coll => {
      coll.syncStatus = 'syncing';
    });
    db.syncStatus = 'syncing';

    setTimeout(() => {
      db.collections.forEach(coll => {
        coll.syncStatus = 'synced';
        coll.lastSyncAt = new Date();
      });
      this.buildDatabaseTree();
    }, 3000);
  }

  simulateCollectionSync(): void {
    // Randomly pick a collection and change its sync status
    if (this.collections.length === 0) return;

    const randomColl = this.collections[Math.floor(Math.random() * this.collections.length)];
    
    if (randomColl.syncStatus === 'syncing') {
      // Complete the sync
      randomColl.syncStatus = 'synced';
      randomColl.lastSyncAt = new Date();
    } else if (Math.random() > 0.7) {
      // Start syncing
      randomColl.syncStatus = 'syncing';
    }

    this.buildDatabaseTree();
  }

  getSyncStatusIcon(status: CollectionSyncStatus): string {
    const icons = {
      synced: '✅',
      syncing: '🔄',
      pending: '⏳',
      error: '❌'
    };
    return icons[status];
  }

  getSyncStatusClass(status: CollectionSyncStatus): string {
    return `sync-status-${status}`;
  }
}
