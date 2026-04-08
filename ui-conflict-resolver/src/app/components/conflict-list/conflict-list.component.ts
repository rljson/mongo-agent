import { CommonModule } from '@angular/common';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { RouterModule } from '@angular/router';

import { formatDistanceToNow } from 'date-fns';
import { Subject, takeUntil } from 'rxjs';

import { ConflictInfo } from '../../models/conflict.types';
import { SyncApiService } from '../../services/sync-api.service';


@Component({
  selector: 'app-conflict-list',
  standalone: true,
  imports: [CommonModule, RouterModule],
  templateUrl: './conflict-list.component.html',
  styleUrls: ['./conflict-list.component.scss'],
})
export class ConflictListComponent implements OnInit, OnDestroy {
  conflicts: ConflictInfo[] = [];
  loading = true;
  error: string | null = null;
  private destroy$ = new Subject<void>();

  constructor(private syncApi: SyncApiService) {}

  ngOnInit(): void {
    // Poll for conflicts every 5 seconds
    this.syncApi
      .pollConflicts(5000)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (conflicts) => {
          this.conflicts = conflicts;
          this.loading = false;
          this.error = null;
        },
        error: (err) => {
          this.error = 'Failed to load conflicts: ' + err.message;
          this.loading = false;
        },
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  getTimeAgo(timestamp: number): string {
    return formatDistanceToNow(new Date(timestamp), { addSuffix: true });
  }

  getConflictTypeLabel(type: string): string {
    const labels: Record<string, string> = {
      'concurrent-update': 'Concurrent Updates',
      'update-delete': 'Update vs Delete',
      'concurrent-insert': 'Concurrent Inserts',
    };
    return labels[type] || type;
  }

  getStatusClass(status: string): string {
    const classes: Record<string, string> = {
      pending: 'status-pending',
      resolved: 'status-resolved',
      ignored: 'status-ignored',
    };
    return classes[status] || '';
  }

  getPendingCount(): number {
    return this.conflicts.filter((c) => c.status === 'pending').length;
  }

  triggerSync(): void {
    this.syncApi.triggerSync().subscribe({
      next: () => {
        console.log('Sync triggered successfully');
      },
      error: (err) => {
        console.error('Failed to trigger sync:', err);
      },
    });
  }
}
