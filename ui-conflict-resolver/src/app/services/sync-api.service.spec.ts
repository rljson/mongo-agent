import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { ConflictInfo, ConflictResolution } from '../models/conflict.types';

import { SyncApiService } from './sync-api.service';


describe('SyncApiService', () => {
  let service: SyncApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [SyncApiService],
    });
    service = TestBed.inject(SyncApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should fetch conflicts', (done) => {
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
    ];

    service.getConflicts().subscribe((conflicts) => {
      expect(conflicts.length).toBe(1);
      expect(conflicts[0].conflictId).toBe('conflict-1');
      done();
    });

    const req = httpMock.expectOne(`${service['apiBaseUrl']}/conflicts`);
    expect(req.request.method).toBe('GET');
    req.flush(mockConflicts);
  });

  it('should resolve conflict', (done) => {
    const resolution: ConflictResolution = {
      conflictId: 'conflict-1',
      resolutionType: 'use-local',
      resolvedBy: 'user-1',
      resolvedAt: Date.now(),
    };

    service.resolveConflict(resolution).subscribe((response) => {
      expect(response.success).toBe(true);
      done();
    });

    const req = httpMock.expectOne(
      `${service['apiBaseUrl']}/conflicts/resolve`,
    );
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(resolution);
    req.flush({ success: true, message: 'Resolved' });
  });

  it('should verify hash chain', (done) => {
    const conflictId = 'conflict-1';

    service.verifyHashChain(conflictId).subscribe((result) => {
      expect(result.valid).toBe(true);
      done();
    });

    const req = httpMock.expectOne(
      `${service['apiBaseUrl']}/conflicts/${conflictId}/verify-chain`,
    );
    expect(req.request.method).toBe('GET');
    req.flush({ valid: true, details: {} });
  });

  it('should fetch single conflict', (done) => {
    const conflictId = 'conflict-1';
    const mockConflict: ConflictInfo = {
      conflictId: 'conflict-1',
      documentId: 'doc-1',
      collection: 'users',
      database: 'test',
      detectedAt: Date.now(),
      status: 'pending',
      versions: [],
      conflictType: 'concurrent-update',
    };

    service.getConflict(conflictId).subscribe((conflict) => {
      expect(conflict.conflictId).toBe('conflict-1');
      done();
    });

    const req = httpMock.expectOne(`${service['apiBaseUrl']}/conflicts/${conflictId}`);
    expect(req.request.method).toBe('GET');
    req.flush(mockConflict);
  });

  it('should get document history', (done) => {
    const documentId = 'doc-1';
    const mockHistory = [
      {
        documentId: 'doc-1',
        operationType: 'insert',
        timestamp: Date.now(),
        nodeId: 'node-a',
        operationId: 'op-1',
      },
    ];

    service.getDocumentHistory(documentId).subscribe((history) => {
      expect(history.length).toBe(1);
      done();
    });

    const req = httpMock.expectOne(
      `${service['apiBaseUrl']}/documents/${documentId}/history`,
    );
    expect(req.request.method).toBe('GET');
    req.flush(mockHistory);
  });

  it('should get agent status', (done) => {
    const mockStatus = [
      {
        nodeId: 'node-a',
        lastSync: Date.now(),
        stateHash: 'hash-123',
        pendingOperations: 5,
        isOnline: true,
      },
    ];

    service.getAgentStatus().subscribe((status) => {
      expect(status.length).toBe(1);
      expect(status[0].nodeId).toBe('node-a');
      done();
    });

    const req = httpMock.expectOne(`${service['apiBaseUrl']}/agents/status`);
    expect(req.request.method).toBe('GET');
    req.flush(mockStatus);
  });

  it('should trigger sync', (done) => {
    service.triggerSync().subscribe((result) => {
      expect(result.success).toBe(true);
      done();
    });

    const req = httpMock.expectOne(`${service['apiBaseUrl']}/sync/trigger`);
    expect(req.request.method).toBe('POST');
    req.flush({ success: true });
  });
});
