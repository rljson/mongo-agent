import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router, RouterModule } from '@angular/router';

import { formatDistanceToNow } from 'date-fns';

import {
  ConflictInfo, ConflictResolution, DocumentVersion, FieldConflict
} from '../../models/conflict.types';
import { DiffService } from '../../services/diff.service';
import { SyncApiService } from '../../services/sync-api.service';


@Component({
  selector: 'app-conflict-resolver',
  standalone: true,
  imports: [CommonModule, RouterModule, FormsModule],
  templateUrl: './conflict-resolver.component.html',
  styleUrls: ['./conflict-resolver.component.scss'],
})
export class ConflictResolverComponent implements OnInit {
  conflict: ConflictInfo | null = null;
  loading = true;
  error: string | null = null;

  // Resolution state
  resolutionMode: 'use-local' | 'use-remote' | 'field-merge' | null = null;
  selectedVersion: DocumentVersion | null = null;
  fieldConflicts: FieldConflict[] = [];
  mergedDocument: any = null;

  // UI state
  showRawJson = false;
  hashChainValid: boolean | null = null;
  verifyingChain = false;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private syncApi: SyncApiService,
    public diffService: DiffService,
  ) {}

  ngOnInit(): void {
    const conflictId = this.route.snapshot.paramMap.get('id');
    if (!conflictId) {
      this.error = 'No conflict ID provided';
      this.loading = false;
      return;
    }

    this.loadConflict(conflictId);
    this.verifyHashChain(conflictId);
  }

  loadConflict(conflictId: string): void {
    this.syncApi.getConflict(conflictId).subscribe({
      next: (conflict) => {
        this.conflict = conflict;
        this.loading = false;

        // Initialize field conflicts if we have exactly 2 versions
        if (conflict.versions.length === 2) {
          this.fieldConflicts = this.diffService.getFieldConflicts(
            conflict.versions[0].data,
            conflict.versions[1].data,
          );
        }
      },
      error: (err) => {
        this.error = 'Failed to load conflict: ' + err.message;
        this.loading = false;
      },
    });
  }

  verifyHashChain(conflictId: string): void {
    this.verifyingChain = true;
    this.syncApi.verifyHashChain(conflictId).subscribe({
      next: (result) => {
        this.hashChainValid = result.valid;
        this.verifyingChain = false;
      },
      error: () => {
        this.verifyingChain = false;
      },
    });
  }

  selectResolutionMode(mode: 'use-local' | 'use-remote' | 'field-merge'): void {
    this.resolutionMode = mode;

    if (mode === 'use-local' && this.conflict) {
      this.selectedVersion = this.conflict.versions[0];
      this.mergedDocument = this.selectedVersion.data;
    } else if (mode === 'use-remote' && this.conflict) {
      this.selectedVersion = this.conflict.versions[1];
      this.mergedDocument = this.selectedVersion.data;
    } else if (mode === 'field-merge' && this.conflict) {
      // Start with local version as base
      this.mergedDocument = JSON.parse(
        JSON.stringify(this.conflict.versions[0].data),
      );
    }
  }

  selectFieldValue(conflict: FieldConflict, useLocal: boolean): void {
    conflict.selectedValue = useLocal
      ? conflict.localValue
      : conflict.remoteValue;

    // Update merged document
    if (this.conflict) {
      this.mergedDocument = this.diffService.mergeFields(
        this.conflict.versions[0].data,
        this.fieldConflicts,
      );
    }
  }

  submitResolution(): void {
    if (!this.conflict || !this.resolutionMode) {
      return;
    }

    const resolution: ConflictResolution = {
      conflictId: this.conflict.conflictId,
      resolutionType: this.resolutionMode,
      selectedVersion: this.selectedVersion || undefined,
      mergedDocument: this.mergedDocument,
      resolvedBy: 'current-user', // TODO: Get from auth service
      resolvedAt: Date.now(),
    };

    this.syncApi.resolveConflict(resolution).subscribe({
      next: () => {
        this.router.navigate(['/']);
      },
      error: (err) => {
        this.error = 'Failed to resolve conflict: ' + err.message;
      },
    });
  }

  getTimeAgo(timestamp: number | undefined): string {
    if (!timestamp) return 'Unknown';
    return formatDistanceToNow(new Date(timestamp), { addSuffix: true });
  }

  formatJson(obj: any): string {
    return JSON.stringify(obj, null, 2);
  }

  getDiffForField(field: string): string {
    if (!this.conflict || this.conflict.versions.length < 2) {
      return '';
    }

    const local = this.diffService.formatValue(
      this.conflict.versions[0].data[field],
    );
    const remote = this.diffService.formatValue(
      this.conflict.versions[1].data[field],
    );

    return this.diffService.generateTextDiff(local, remote);
  }
}
