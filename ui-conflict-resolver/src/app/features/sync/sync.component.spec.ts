import { ComponentFixture, TestBed, fakeAsync, tick, flush, discardPeriodicTasks } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { SyncComponent } from './sync.component';

describe('SyncComponent', () => {
  let component: SyncComponent;
  let fixture: ComponentFixture<SyncComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SyncComponent, RouterTestingModule],
    }).compileComponents();

    fixture = TestBed.createComponent(SyncComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    if (component) {
      component.ngOnDestroy();
    }
    fixture.destroy();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize with default values', () => {
    expect(component.selectedTab).toBe('overview');
    expect(component.expandedAgent).toBe(true);
    expect(component.mongoAgent).toBeDefined();
  });

  it('should build database tree on init', fakeAsync(() => {
    fixture.detectChanges();
    tick();
    
    expect(component.databaseTree.length).toBeGreaterThan(0);
    
    component.ngOnDestroy();
    flush();
  }));

  it('should update mongo agent periodically', fakeAsync(() => {
    fixture.detectChanges();
    const initialSeq = component.mongoAgent.lastSeq;
    
    tick(5000);
    
    expect(component.mongoAgent.lastSeq).toBeGreaterThan(initialSeq);
    
    component.ngOnDestroy();
    flush();
  }));

  it('should simulate collection sync', fakeAsync(() => {
    fixture.detectChanges();
    
    tick(3000);
    
    // Collection sync should have been called
    expect(component.databaseTree.length).toBeGreaterThan(0);
    
    component.ngOnDestroy();
    flush();
  }));

  it('should rescan agent', fakeAsync(() => {
    spyOn(console, 'log');
    component.rescanAgent();
    
    expect(component.mongoAgent.status).toBe('syncing');
    tick(1000);
    expect(console.log).toHaveBeenCalledWith('Re-scanning MongoDB agent...');
  }));

  it('should reset agent with confirmation', () => {
    spyOn(window, 'confirm').and.returnValue(true);
    spyOn(console, 'log');
    
    component.resetAgent();
    
    expect(component.mongoAgent.lastSeq).toBe(0);
    expect(component.mongoAgent.lastHash).toBe('GENESIS');
    expect(component.mongoAgent.lastSyncAt).toBeNull();
  });

  it('should not reset agent without confirmation', () => {
    spyOn(window, 'confirm').and.returnValue(false);
    const initialSeq = component.mongoAgent.lastSeq;
    
    component.resetAgent();
    
    expect(component.mongoAgent.lastSeq).toBe(initialSeq);
  });

  it('should browse collections', () => {
    component.browseCollections();
    expect(component.selectedTab).toBe('collections');
  });

  it('should sync origin', fakeAsync(() => {
    spyOn(console, 'log');
    const origin = component.syncOrigins[0];
    const initialSeq = origin.lastSeqPulled;
    
    component.syncOrigin(origin);
    
    expect(origin.status).toBe('pending');
    tick(1500);
    expect(origin.lastSeqPulled).toBeGreaterThan(initialSeq);
    expect(origin.status).toBe('synced');
  }));

  it('should view origin details', () => {
    spyOn(console, 'log');
    const origin = component.syncOrigins[0];
    
    component.viewOriginDetails(origin);
    
    expect(console.log).toHaveBeenCalledWith('View origin details:', origin);
  });

  it('should format distance to now', () => {
    const date = new Date();
    const result = component.formatDistanceToNow(date);
    expect(result).toContain('ago');
  });

  it('should return "Never" for null date', () => {
    const result = component.formatDistanceToNow(null);
    expect(result).toBe('Never');
  });

  it('should get status icon', () => {
    expect(component.getStatusIcon('syncing')).toBe('🔄');
    expect(component.getStatusIcon('idle')).toBe('⏸️');
    expect(component.getStatusIcon('error')).toBe('❌');
    expect(component.getStatusIcon('disconnected')).toBe('🔌');
  });

  it('should get status class', () => {
    expect(component.getStatusClass('syncing')).toBe('status-syncing');
    expect(component.getStatusClass('idle')).toBe('status-idle');
  });

  it('should get operation icon', () => {
    expect(component.getOperationIcon('insert')).toBe('➕');
    expect(component.getOperationIcon('update')).toBe('✏️');
    expect(component.getOperationIcon('replace')).toBe('🔄');
    expect(component.getOperationIcon('delete')).toBe('🗑️');
  });

  it('should shorten hash', () => {
    const hash = 'abcdef1234567890';
    const shortened = component.shortenHash(hash);
    expect(shortened).toContain('…');
    expect(shortened.length).toBeLessThan(hash.length);
  });

  it('should return N/A for empty hash', () => {
    expect(component.shortenHash('')).toBe('N/A');
  });

  it('should format timestamp', () => {
    const date = new Date();
    const formatted = component.formatTimestamp(date);
    expect(formatted).toMatch(/\d{2}:\d{2}:\d{2}/);
  });

  it('should toggle database expansion', () => {
    component.buildDatabaseTree();
    const db = component.databaseTree[0];
    const initialExpanded = db.expanded;
    
    component.toggleDatabase(db);
    
    expect(db.expanded).toBe(!initialExpanded);
  });

  it('should sync collection', fakeAsync(() => {
    spyOn(console, 'log');
    const collection = component.collections[0];
    
    component.syncCollection(collection);
    
    expect(collection.syncStatus).toBe('syncing');
    tick(2000);
    expect(collection.syncStatus).toBe('synced');
    
    component.ngOnDestroy();
    flush();
  }));

  it('should sync entire database', fakeAsync(() => {
    spyOn(console, 'log');
    component.buildDatabaseTree();
    const db = component.databaseTree[0];
    
    component.syncDatabase(db);
    
    expect(db.syncStatus).toBe('syncing');
    db.collections.forEach(coll => {
      expect(coll.syncStatus).toBe('syncing');
    });
    
    tick(3000);
    // Tree should be rebuilt after sync
    expect(component.databaseTree.length).toBeGreaterThan(0);
    
    component.ngOnDestroy();
    flush();
  }));

  it('should get sync status icon', () => {
    expect(component.getSyncStatusIcon('synced')).toBe('✅');
    expect(component.getSyncStatusIcon('syncing')).toBe('🔄');
    expect(component.getSyncStatusIcon('pending')).toBe('⏳');
    expect(component.getSyncStatusIcon('error')).toBe('❌');
  });

  it('should get sync status class', () => {
    expect(component.getSyncStatusClass('synced')).toBe('sync-status-synced');
    expect(component.getSyncStatusClass('syncing')).toBe('sync-status-syncing');
  });

  it('should build database tree correctly', () => {
    component.buildDatabaseTree();
    
    expect(component.databaseTree.length).toBeGreaterThan(0);
    component.databaseTree.forEach(db => {
      expect(db.collections.length).toBeGreaterThan(0);
      expect(db.totalDocuments).toBeGreaterThan(0);
    });
  });

  it('should unsubscribe on destroy', fakeAsync(() => {
    fixture.detectChanges();
    
    spyOn(component['destroy$'], 'next');
    spyOn(component['destroy$'], 'complete');
    
    component.ngOnDestroy();
    
    expect(component['destroy$'].next).toHaveBeenCalled();
    expect(component['destroy$'].complete).toHaveBeenCalled();
    
    discardPeriodicTasks();
  }));

  it('should handle multiple sync origins', () => {
    expect(component.syncOrigins.length).toBeGreaterThan(0);
    component.syncOrigins.forEach(origin => {
      expect(origin.nodeId).toBeDefined();
      expect(origin.origin).toBeDefined();
    });
  });

  it('should display recent operations', () => {
    expect(component.recentOperations.length).toBeGreaterThan(0);
    component.recentOperations.forEach(op => {
      expect(op.seq).toBeDefined();
      expect(op.operationType).toBeDefined();
    });
  });
});
