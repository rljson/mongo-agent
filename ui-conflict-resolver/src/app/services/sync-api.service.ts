import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { interval, Observable } from 'rxjs';
import { startWith, switchMap } from 'rxjs/operators';

import { ConflictInfo, ConflictResolution, SyncAgentStatus } from '../models/conflict.types';


@Injectable({
  providedIn: 'root',
})
export class SyncApiService {
  private apiBaseUrl = 'http://localhost:3000/api'; // Configurable

  constructor(private http: HttpClient) {}

  /**
   * Get all pending conflicts
   */
  getConflicts(): Observable<ConflictInfo[]> {
    return this.http.get<ConflictInfo[]>(`${this.apiBaseUrl}/conflicts`);
  }

  /**
   * Poll for conflicts every N seconds
   */
  pollConflicts(intervalMs: number = 5000): Observable<ConflictInfo[]> {
    return interval(intervalMs).pipe(
      startWith(0),
      switchMap(() => this.getConflicts()),
    );
  }

  /**
   * Get details for a specific conflict
   */
  getConflict(conflictId: string): Observable<ConflictInfo> {
    return this.http.get<ConflictInfo>(
      `${this.apiBaseUrl}/conflicts/${conflictId}`,
    );
  }

  /**
   * Submit a conflict resolution
   */
  resolveConflict(
    resolution: ConflictResolution,
  ): Observable<{ success: boolean; message: string }> {
    return this.http.post<{ success: boolean; message: string }>(
      `${this.apiBaseUrl}/conflicts/resolve`,
      resolution,
    );
  }

  /**
   * Get status of all sync agents/nodes
   */
  getAgentStatus(): Observable<SyncAgentStatus[]> {
    return this.http.get<SyncAgentStatus[]>(`${this.apiBaseUrl}/agents/status`);
  }

  /**
   * Get document history from the operation chain
   */
  getDocumentHistory(documentId: string): Observable<any[]> {
    return this.http.get<any[]>(
      `${this.apiBaseUrl}/documents/${documentId}/history`,
    );
  }

  /**
   * Verify hash chain integrity for a conflict
   */
  verifyHashChain(
    conflictId: string,
  ): Observable<{ valid: boolean; details: any }> {
    return this.http.get<{ valid: boolean; details: any }>(
      `${this.apiBaseUrl}/conflicts/${conflictId}/verify-chain`,
    );
  }

  /**
   * Force sync between nodes
   */
  triggerSync(): Observable<{ success: boolean }> {
    return this.http.post<{ success: boolean }>(
      `${this.apiBaseUrl}/sync/trigger`,
      {},
    );
  }
}
