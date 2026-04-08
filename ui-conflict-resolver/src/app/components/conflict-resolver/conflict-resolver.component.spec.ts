import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ActivatedRoute, Router } from '@angular/router';
import { of, throwError } from 'rxjs';

import { ConflictResolverComponent } from './conflict-resolver.component';
import { SyncApiService } from '../../services/sync-api.service';
import { DiffService } from '../../services/diff.service';
import { ConflictInfo, FieldConflict } from '../../models/conflict.types';

describe('ConflictResolverComponent', () => {
  let component: ConflictResolverComponent;
  let fixture: ComponentFixture<ConflictResolverComponent>;
  let syncApiService: jasmine.SpyObj<SyncApiService>;
  let diffService: DiffService;
  let router: Router;

  const mockConflict: ConflictInfo = {
    conflictId: 'conflict-1',
    documentId: 'doc-1',
    collection: 'users',
    database: 'test',
    detectedAt: Date.now(),
    status: 'pending',
    conflictType: 'concurrent-update',
    versions: [
      {
        documentId: 'doc-1',
        data: { _id: 'doc-1', name: 'Alice', age: 30, status: 'active' },
        timestamp: Date.now(),
        nodeId: 'node-a',
        operationId: 'op-1',
        operationType: 'update',
      },
      {
        documentId: 'doc-1',
        data: { _id: 'doc-1', name: 'Alice Smith', age: 31, status: 'active' },
        timestamp: Date.now(),
        nodeId: 'node-b',
        operationId: 'op-2',
        operationType: 'update',
      },
    ],
  };

  beforeEach(async () => {
    const syncApiSpy = jasmine.createSpyObj('SyncApiService', [
      'getConflict',
      'resolveConflict',
      'verifyHashChain',
    ]);

    await TestBed.configureTestingModule({
      imports: [
        ConflictResolverComponent,
        HttpClientTestingModule,
        RouterTestingModule,
      ],
      providers: [
        { provide: SyncApiService, useValue: syncApiSpy },
        DiffService,
        {
          provide: ActivatedRoute,
          useValue: {
            snapshot: {
              paramMap: {
                get: () => 'conflict-1',
              },
            },
          },
        },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ConflictResolverComponent);
    component = fixture.componentInstance;
    syncApiService = TestBed.inject(
      SyncApiService,
    ) as jasmine.SpyObj<SyncApiService>;
    diffService = TestBed.inject(DiffService);
    router = TestBed.inject(Router);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load conflict on init', () => {
    syncApiService.getConflict.and.returnValue(of(mockConflict));
    syncApiService.verifyHashChain.and.returnValue(
      of({ valid: true, details: {} }),
    );

    fixture.detectChanges();

    expect(component.conflict).toEqual(mockConflict);
    expect(component.loading).toBe(false);
    expect(component.fieldConflicts.length).toBeGreaterThan(0);
  });

  it('should handle missing conflict ID', () => {
    const activatedRoute = TestBed.inject(ActivatedRoute);
    spyOn(activatedRoute.snapshot.paramMap, 'get').and.returnValue(null);

    fixture.detectChanges();

    expect(component.error).toBe('No conflict ID provided');
    expect(component.loading).toBe(false);
  });

  it('should handle load conflict error', () => {
    const error = new Error('Not found');
    syncApiService.getConflict.and.returnValue(throwError(() => error));
    syncApiService.verifyHashChain.and.returnValue(of({ valid: false, details: {} }));

    fixture.detectChanges();

    expect(component.error).toBe('Failed to load conflict: Not found');
    expect(component.loading).toBe(false);
  });

  it('should verify hash chain', () => {
    syncApiService.getConflict.and.returnValue(of(mockConflict));
    syncApiService.verifyHashChain.and.returnValue(
      of({ valid: true, details: {} }),
    );

    fixture.detectChanges();

    expect(component.hashChainValid).toBe(true);
    expect(component.verifyingChain).toBe(false);
  });

  it('should handle hash chain verification failure', () => {
    syncApiService.getConflict.and.returnValue(of(mockConflict));
    const error = new Error('Verification failed');
    syncApiService.verifyHashChain.and.returnValue(throwError(() => error));

    fixture.detectChanges();

    expect(component.verifyingChain).toBe(false);
  });

  it('should select use-local resolution mode', () => {
    component.conflict = mockConflict;

    component.selectResolutionMode('use-local');

    expect(component.resolutionMode).toBe('use-local');
    expect(component.selectedVersion).toEqual(mockConflict.versions[0]);
    expect(component.mergedDocument).toEqual(mockConflict.versions[0].data);
  });

  it('should select use-remote resolution mode', () => {
    component.conflict = mockConflict;

    component.selectResolutionMode('use-remote');

    expect(component.resolutionMode).toBe('use-remote');
    expect(component.selectedVersion).toEqual(mockConflict.versions[1]);
    expect(component.mergedDocument).toEqual(mockConflict.versions[1].data);
  });

  it('should select field-merge resolution mode', () => {
    component.conflict = mockConflict;

    component.selectResolutionMode('field-merge');

    expect(component.resolutionMode).toBe('field-merge');
    expect(component.mergedDocument).toBeDefined();
  });

  it('should select field value for merge', () => {
    component.conflict = mockConflict;
    const fieldConflict: FieldConflict = {
      fieldPath: 'name',
      localValue: 'Alice',
      remoteValue: 'Alice Smith',
    };
    component.fieldConflicts = [fieldConflict];

    component.selectFieldValue(fieldConflict, false);

    expect(fieldConflict.selectedValue).toBe('Alice Smith');
  });

  it('should submit resolution successfully', () => {
    component.conflict = mockConflict;
    component.resolutionMode = 'use-local';
    component.selectedVersion = mockConflict.versions[0];
    component.mergedDocument = mockConflict.versions[0].data;

    syncApiService.resolveConflict.and.returnValue(
      of({ success: true, message: 'Resolved' }),
    );
    spyOn(router, 'navigate');

    component.submitResolution();

    expect(syncApiService.resolveConflict).toHaveBeenCalled();
    expect(router.navigate).toHaveBeenCalledWith(['/']);
  });

  it('should handle resolution submission error', () => {
    component.conflict = mockConflict;
    component.resolutionMode = 'use-local';
    component.selectedVersion = mockConflict.versions[0];

    const error = new Error('Resolution failed');
    syncApiService.resolveConflict.and.returnValue(throwError(() => error));

    component.submitResolution();

    expect(component.error).toBe('Failed to resolve conflict: Resolution failed');
  });

  it('should not submit without conflict or resolution mode', () => {
    component.conflict = null;
    component.resolutionMode = null;

    component.submitResolution();

    expect(syncApiService.resolveConflict).not.toHaveBeenCalled();
  });

  it('should format time ago', () => {
    const result = component.getTimeAgo(Date.now() - 60000);
    expect(result).toContain('ago');
  });

  it('should format JSON', () => {
    const obj = { name: 'Alice', age: 30 };
    const result = component.formatJson(obj);
    expect(result).toContain('"name": "Alice"');
    expect(result).toContain('"age": 30');
  });

  it('should get diff for field', () => {
    component.conflict = mockConflict;

    const result = component.getDiffForField('name');

    expect(result).toBeDefined();
  });

  it('should handle missing conflict in getDiffForField', () => {
    component.conflict = null;

    const result = component.getDiffForField('name');

    expect(result).toBe('');
  });

  it('should handle conflict with less than 2 versions', () => {
    component.conflict = { ...mockConflict, versions: [mockConflict.versions[0]] };

    const result = component.getDiffForField('name');

    expect(result).toBe('');
  });

  it('should toggle showRawJson', () => {
    expect(component.showRawJson).toBe(false);
    component.showRawJson = true;
    expect(component.showRawJson).toBe(true);
  });
});
