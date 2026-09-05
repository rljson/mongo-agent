import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { of, throwError } from 'rxjs';

import { ConflictInfo } from '../../models/conflict.types';
import { SyncApiService } from '../../services/sync-api.service';

import { ConflictListComponent } from './conflict-list.component';


describe('ConflictListComponent', () => {
  let component: ConflictListComponent;
  let fixture: ComponentFixture<ConflictListComponent>;
  let syncApiService: jasmine.SpyObj<SyncApiService>;

  const mockConflicts: ConflictInfo[] = [
    {
      conflictId: 'conflict-1',
      documentId: 'doc-1',
      collection: 'users',
      database: 'test',
      detectedAt: Date.now() - 300000,
      status: 'pending',
      conflictType: 'concurrent-update',
      versions: [
        {
          documentId: 'doc-1',
          data: { _id: 'doc-1', name: 'Alice' },
          timestamp: Date.now(),
          nodeId: 'node-a',
          operationId: 'op-1',
          operationType: 'update',
        },
        {
          documentId: 'doc-1',
          data: { _id: 'doc-1', name: 'Alice Smith' },
          timestamp: Date.now(),
          nodeId: 'node-b',
          operationId: 'op-2',
          operationType: 'update',
        },
      ],
    },
    {
      conflictId: 'conflict-2',
      documentId: 'doc-2',
      collection: 'articles',
      database: 'test',
      detectedAt: Date.now() - 120000,
      status: 'resolved',
      conflictType: 'concurrent-update',
      versions: [],
    },
  ];

  beforeEach(async () => {
    const syncApiSpy = jasmine.createSpyObj('SyncApiService', [
      'pollConflicts',
      'getConflicts',
      'triggerSync',
    ]);

    await TestBed.configureTestingModule({
      imports: [
        ConflictListComponent,
        HttpClientTestingModule,
        RouterTestingModule,
      ],
      providers: [{ provide: SyncApiService, useValue: syncApiSpy }],
    }).compileComponents();

    fixture = TestBed.createComponent(ConflictListComponent);
    component = fixture.componentInstance;
    syncApiService = TestBed.inject(
      SyncApiService,
    ) as jasmine.SpyObj<SyncApiService>;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load conflicts on init', () => {
    syncApiService.pollConflicts.and.returnValue(of(mockConflicts));

    fixture.detectChanges();

    expect(component.loading).toBe(false);
    expect(component.conflicts.length).toBe(2);
  });

  it('should handle error when loading conflicts', () => {
    const error = new Error('Network error');
    syncApiService.pollConflicts.and.returnValue(throwError(() => error));

    fixture.detectChanges();

    expect(component.loading).toBe(false);
    expect(component.error).toBe('Failed to load conflicts: Network error');
  });

  it('should count pending conflicts', () => {
    syncApiService.pollConflicts.and.returnValue(of(mockConflicts));

    fixture.detectChanges();

    expect(component.getPendingCount()).toBe(1);
  });

  it('should get conflict type label', () => {
    expect(component.getConflictTypeLabel('concurrent-update')).toBe(
      'Concurrent Updates',
    );
    expect(component.getConflictTypeLabel('update-delete')).toBe(
      'Update vs Delete',
    );
    expect(component.getConflictTypeLabel('concurrent-insert')).toBe(
      'Concurrent Inserts',
    );
    expect(component.getConflictTypeLabel('unknown')).toBe('unknown');
  });

  it('should get status class', () => {
    expect(component.getStatusClass('pending')).toBe('status-pending');
    expect(component.getStatusClass('resolved')).toBe('status-resolved');
    expect(component.getStatusClass('ignored')).toBe('status-ignored');
    expect(component.getStatusClass('unknown')).toBe('');
  });

  it('should trigger sync successfully', () => {
    syncApiService.triggerSync.and.returnValue(of({ success: true }));

    component.triggerSync();

    expect(syncApiService.triggerSync).toHaveBeenCalled();
  });

  it('should handle trigger sync error', () => {
    const error = new Error('Sync failed');
    syncApiService.triggerSync.and.returnValue(throwError(() => error));
    spyOn(console, 'error');

    component.triggerSync();

    expect(console.error).toHaveBeenCalledWith(
      'Failed to trigger sync:',
      error,
    );
  });

  it('should format time ago correctly', () => {
    const result = component.getTimeAgo(Date.now() - 300000);
    expect(result).toContain('ago');
  });

  it('should handle empty conflicts list', () => {
    syncApiService.pollConflicts.and.returnValue(of([]));

    fixture.detectChanges();

    expect(component.conflicts.length).toBe(0);
    expect(component.getPendingCount()).toBe(0);
  });

  it('should clean up on destroy', () => {
    syncApiService.pollConflicts.and.returnValue(of(mockConflicts));

    fixture.detectChanges();
    spyOn(component['destroy$'], 'next');
    spyOn(component['destroy$'], 'complete');

    component.ngOnDestroy();

    expect(component['destroy$'].next).toHaveBeenCalled();
    expect(component['destroy$'].complete).toHaveBeenCalled();
  });
});
