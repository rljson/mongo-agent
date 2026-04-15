import { ComponentFixture, TestBed, fakeAsync, tick, flush, discardPeriodicTasks } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { of, throwError } from 'rxjs';
import { DashboardComponent } from './dashboard.component';
import { SyncApiService } from '../../services/sync-api.service';
import { ConflictInfo } from '../../models/conflict.types';

describe('DashboardComponent', () => {
  let component: DashboardComponent;
  let fixture: ComponentFixture<DashboardComponent>;
  let syncApiService: jasmine.SpyObj<SyncApiService>;

  const mockConflicts: ConflictInfo[] = [
    {
      conflictId: 'conflict-1',
      documentId: 'doc-1',
      collection: 'users',
      database: 'test',
      detectedAt: Date.now(),
      status: 'pending',
      versions: [],
      conflictType: 'concurrent-update',
    },
    {
      conflictId: 'conflict-2',
      documentId: 'doc-2',
      collection: 'orders',
      database: 'test',
      detectedAt: Date.now(),
      status: 'pending',
      versions: [],
      conflictType: 'concurrent-update',
    },
  ];

  beforeEach(async () => {
    const syncApiSpy = jasmine.createSpyObj('SyncApiService', ['getConflicts']);

    await TestBed.configureTestingModule({
      imports: [DashboardComponent, HttpClientTestingModule, RouterTestingModule],
      providers: [{ provide: SyncApiService, useValue: syncApiSpy }],
    }).compileComponents();

    syncApiService = TestBed.inject(SyncApiService) as jasmine.SpyObj<SyncApiService>;
    fixture = TestBed.createComponent(DashboardComponent);
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

  it('should initialize with default node info', () => {
    expect(component.nodeInfo.nodeId).toBe('NodeA-abc123');
    expect(component.nodeInfo.role).toBe('hub');
    expect(component.nodeInfo.transportReady).toBe(true);
  });

  it('should load conflicts on init', fakeAsync(() => {
    syncApiService.getConflicts.and.returnValue(of(mockConflicts));
    
    fixture.detectChanges(); // triggers ngOnInit
    tick();

    expect(syncApiService.getConflicts).toHaveBeenCalled();
    expect(component.conflictsCount).toBe(2);
    expect(component.alerts.length).toBe(1);
    expect(component.alerts[0].type).toBe('warning');
    
    component.ngOnDestroy();
    flush();
  }));

  it('should handle no conflicts', fakeAsync(() => {
    syncApiService.getConflicts.and.returnValue(of([]));
    
    fixture.detectChanges();
    tick();

    expect(component.conflictsCount).toBe(0);
    expect(component.alerts.length).toBe(0);
    
    component.ngOnDestroy();
    flush();
  }));

  it('should handle conflicts API error gracefully', fakeAsync(() => {
    syncApiService.getConflicts.and.returnValue(throwError(() => new Error('API Error')));
    
    fixture.detectChanges();
    tick();

    // Should not throw and should handle silently
    expect(component.conflictsCount).toBe(0);
    
    component.ngOnDestroy();
    flush();
  }));

  it('should update uptime periodically', fakeAsync(() => {
    syncApiService.getConflicts.and.returnValue(of([]));
    
    const initialUptime = component.nodeInfo.uptime;
    fixture.detectChanges();
    
    tick(1000);
    expect(component.nodeInfo.uptime).toBeGreaterThan(initialUptime);
    
    tick(1000);
    expect(component.nodeInfo.uptime).toBeGreaterThan(initialUptime + 1000);
    
    component.ngOnDestroy();
    flush();
  }));

  it('should call loadConflicts periodically', fakeAsync(() => {
    syncApiService.getConflicts.and.returnValue(of([]));
    
    fixture.detectChanges();
    const initialCallCount = syncApiService.getConflicts.calls.count();
    
    tick(10000);
    expect(syncApiService.getConflicts.calls.count()).toBeGreaterThan(initialCallCount);
    
    component.ngOnDestroy();
    flush();
  }));

  it('should get uptime string', () => {
    component.nodeInfo.startedAt = Date.now() - 5000;
    const uptime = component.getUptime();
    expect(uptime).toBeTruthy();
  });

  it('should get last sync time for agent', () => {
    const agent = component.syncAgents[0];
    const lastSync = component.getLastSyncTime(agent);
    expect(lastSync).toContain('ago');
  });

  it('should return "Never" for agent with no lastSyncAt', () => {
    const agent = { ...component.syncAgents[0], lastSyncAt: undefined };
    const lastSync = component.getLastSyncTime(agent);
    expect(lastSync).toBe('Never');
  });

  it('should return correct status class', () => {
    expect(component.getStatusClass('syncing')).toBe('status-syncing');
    expect(component.getStatusClass('idle')).toBe('status-idle');
    expect(component.getStatusClass('error')).toBe('status-error');
    expect(component.getStatusClass('unknown')).toBe('');
  });

  it('should return correct alert class', () => {
    expect(component.getAlertClass('warning')).toBe('alert-warning');
    expect(component.getAlertClass('info')).toBe('alert-info');
    expect(component.getAlertClass('error')).toBe('alert-error');
    expect(component.getAlertClass('unknown')).toBe('');
  });

  it('should return correct alert icon', () => {
    expect(component.getAlertIcon('warning')).toBe('⚠️');
    expect(component.getAlertIcon('info')).toBe('ℹ️');
    expect(component.getAlertIcon('error')).toBe('❌');
    expect(component.getAlertIcon('unknown')).toBe('ℹ️');
  });

  it('should unsubscribe on destroy', fakeAsync(() => {
    syncApiService.getConflicts.and.returnValue(of([]));
    fixture.detectChanges();
    
    spyOn(component['destroy$'], 'next');
    spyOn(component['destroy$'], 'complete');
    
    component.ngOnDestroy();
    
    expect(component['destroy$'].next).toHaveBeenCalled();
    expect(component['destroy$'].complete).toHaveBeenCalled();
    
    discardPeriodicTasks();
  }));

  it('should create alert with correct message for single conflict', fakeAsync(() => {
    const singleConflict: ConflictInfo[] = [mockConflicts[0]];
    syncApiService.getConflicts.and.returnValue(of(singleConflict));
    
    fixture.detectChanges();
    tick();

    expect(component.alerts[0].message).toBe('1 conflict needs resolution');
    
    component.ngOnDestroy();
    flush();
  }));

  it('should create alert with correct message for multiple conflicts', fakeAsync(() => {
    syncApiService.getConflicts.and.returnValue(of(mockConflicts));
    
    fixture.detectChanges();
    tick();

    expect(component.alerts[0].message).toBe('2 conflicts need resolution');
    
    component.ngOnDestroy();
    flush();
  }));

  it('should filter out resolved conflicts', fakeAsync(() => {
    const mixedConflicts: ConflictInfo[] = [
      { ...mockConflicts[0], status: 'pending' },
      { ...mockConflicts[1], status: 'resolved' },
    ];
    syncApiService.getConflicts.and.returnValue(of(mixedConflicts));
    
    fixture.detectChanges();
    tick();

    expect(component.conflictsCount).toBe(1);
    
    component.ngOnDestroy();
    flush();
  }));
});
