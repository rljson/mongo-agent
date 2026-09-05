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

        // Field-merge only makes sense when both sides actually have data.
        // For update-delete conflicts one side is null, so leave
        // fieldConflicts empty — the template hides the field-merge card.
        if (
          conflict.versions.length === 2 &&
          this.versionHasData(conflict.versions[0]) &&
          this.versionHasData(conflict.versions[1])
        ) {
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

  /**
   * A version has "data" iff its document body is present. The delete side
   * of an update-delete conflict has `data: null` and `operationType:
   * 'delete'` — meaning "this node says the doc should be gone."
   */
  versionHasData(v: DocumentVersion | undefined): boolean {
    return !!v && v.data !== null && v.data !== undefined;
  }

  isDeleteVersion(v: DocumentVersion | undefined): boolean {
    return !this.versionHasData(v) || v?.operationType === 'delete';
  }

  /**
   * field-merge is only available when both versions carry data. For
   * update-delete you must pick one side or the other (no field merge of
   * a deleted doc).
   */
  canFieldMerge(): boolean {
    if (!this.conflict || this.conflict.versions.length !== 2) return false;
    return (
      this.versionHasData(this.conflict.versions[0]) &&
      this.versionHasData(this.conflict.versions[1])
    );
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
    if (mode === 'field-merge' && !this.canFieldMerge()) {
      // Defensive: button should already be disabled in the template, but
      // double-check so we never enter a broken state.
      return;
    }
    this.resolutionMode = mode;

    if (mode === 'use-local' && this.conflict) {
      this.selectedVersion = this.conflict.versions[0];
      // null is intentional here for the delete-side of an update-delete
      // conflict — submitResolution sends it through and the API turns
      // null mergedDocument into a deleteOne.
      this.mergedDocument = this.selectedVersion.data;
    } else if (mode === 'use-remote' && this.conflict) {
      this.selectedVersion = this.conflict.versions[1];
      this.mergedDocument = this.selectedVersion.data;
    } else if (mode === 'field-merge' && this.conflict) {
      // Both versions guaranteed to have data here (canFieldMerge gated).
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
    if (
      !this.conflict ||
      this.conflict.versions.length < 2 ||
      !this.versionHasData(this.conflict.versions[0]) ||
      !this.versionHasData(this.conflict.versions[1])
    ) {
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

  getConflictTypeLabel(type: string | undefined): string {
    const labels: Record<string, string> = {
      'concurrent-update': 'Concurrent updates',
      'update-delete': 'Update vs delete',
      'concurrent-insert': 'Concurrent inserts',
    };
    return labels[type || ''] || type || 'Unknown';
  }
}
