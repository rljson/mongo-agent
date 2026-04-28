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

  /**
   * Liveness of hub + agents for the dashboard.
   */
  getServicesStatus(): Observable<{ hub: boolean; l1: boolean; l2: boolean }> {
    return this.http.get<{ hub: boolean; l1: boolean; l2: boolean }>(
      `${this.apiBaseUrl}/services/status`,
    );
  }

  startHub(): Observable<{ pid?: number; status: string }> {
    return this.http.post<{ pid?: number; status: string }>(
      `${this.apiBaseUrl}/services/start-hub`,
      {},
    );
  }

  startAgentL1(): Observable<{ pid?: number; status: string }> {
    return this.http.post<{ pid?: number; status: string }>(
      `${this.apiBaseUrl}/services/start-agent-l1`,
      {},
    );
  }

  startAgentL2(): Observable<{ pid?: number; status: string }> {
    return this.http.post<{ pid?: number; status: string }>(
      `${this.apiBaseUrl}/services/start-agent-l2`,
      {},
    );
  }

  stopHub(): Observable<{ stopped: boolean }> {
    return this.http.post<{ stopped: boolean }>(
      `${this.apiBaseUrl}/services/stop-hub`,
      {},
    );
  }
  stopAgentL1(): Observable<{ stopped: boolean }> {
    return this.http.post<{ stopped: boolean }>(
      `${this.apiBaseUrl}/services/stop-agent-l1`,
      {},
    );
  }
  stopAgentL2(): Observable<{ stopped: boolean }> {
    return this.http.post<{ stopped: boolean }>(
      `${this.apiBaseUrl}/services/stop-agent-l2`,
      {},
    );
  }

  // Repair operations — wrap the CLI scripts (restore-from-chain,
  // restore-from-peer, backfill-hashes). Each returns the script's exit
  // code + captured stdout/stderr so the UI can show the summary.
  restoreFromChain(
    dryRun = false,
  ): Observable<{ exitCode: number | null; stdout: string; stderr: string }> {
    return this.http.post<{
      exitCode: number | null;
      stdout: string;
      stderr: string;
    }>(`${this.apiBaseUrl}/repair/restore-from-chain`, { dryRun });
  }
  restoreFromPeer(
    coll?: string,
  ): Observable<{ exitCode: number | null; stdout: string; stderr: string }> {
    return this.http.post<{
      exitCode: number | null;
      stdout: string;
      stderr: string;
    }>(`${this.apiBaseUrl}/repair/restore-from-peer`, { coll });
  }
  backfillHashes(
    coll?: string,
  ): Observable<{ exitCode: number | null; stdout: string; stderr: string }> {
    return this.http.post<{
      exitCode: number | null;
      stdout: string;
      stderr: string;
    }>(`${this.apiBaseUrl}/repair/backfill-hashes`, { coll });
  }

  // Hash status — last state_checkpoints entry.
  getHashStatus(): Observable<{ checkpoint: unknown }> {
    return this.http.get<{ checkpoint: unknown }>(
      `${this.apiBaseUrl}/hash-status`,
    );
  }
}
