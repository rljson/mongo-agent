import { Injectable } from '@angular/core';

import * as Diff from 'diff';

import { DiffResult, FieldConflict } from '../models/conflict.types';


@Injectable({
  providedIn: 'root',
})
export class DiffService {
  /**
   * Calculate detailed differences between two document versions
   */
  calculateDiff(local: any, remote: any): DiffResult {
    const result: DiffResult = {
      added: [],
      removed: [],
      modified: [],
    };

    // Get all unique keys
    const allKeys = new Set([
      ...Object.keys(local || {}),
      ...Object.keys(remote || {}),
    ]);

    for (const key of allKeys) {
      if (!(key in local) && key in remote) {
        result.added.push(key);
      } else if (key in local && !(key in remote)) {
        result.removed.push(key);
      } else if (JSON.stringify(local[key]) !== JSON.stringify(remote[key])) {
        result.modified.push({
          field: key,
          oldValue: local[key],
          newValue: remote[key],
        });
      }
    }

    return result;
  }

  /**
   * Get field-level conflicts for manual merge
   */
  getFieldConflicts(
    local: any,
    remote: any,
    basePath: string = '',
  ): FieldConflict[] {
    const conflicts: FieldConflict[] = [];
    const allKeys = new Set([
      ...Object.keys(local || {}),
      ...Object.keys(remote || {}),
    ]);

    for (const key of allKeys) {
      const fieldPath = basePath ? `${basePath}.${key}` : key;
      const localValue = local?.[key];
      const remoteValue = remote?.[key];

      // Skip _id and system fields
      if (key === '_id' || key.startsWith('_')) {
        continue;
      }

      if (
        typeof localValue === 'object' &&
        typeof remoteValue === 'object' &&
        !Array.isArray(localValue) &&
        !Array.isArray(remoteValue) &&
        localValue !== null &&
        remoteValue !== null
      ) {
        // Recurse for nested objects
        conflicts.push(
          ...this.getFieldConflicts(localValue, remoteValue, fieldPath),
        );
      } else if (JSON.stringify(localValue) !== JSON.stringify(remoteValue)) {
        conflicts.push({
          fieldPath,
          localValue,
          remoteValue,
        });
      }
    }

    return conflicts;
  }

  /**
   * Generate text diff for display
   */
  generateTextDiff(text1: string, text2: string): string {
    const diff = Diff.diffLines(text1, text2);
    let output = '';

    diff.forEach((part) => {
      const prefix = part.added ? '+ ' : part.removed ? '- ' : '  ';
      const lines = part.value.split('\n');
      lines.forEach((line) => {
        if (line) {
          output += prefix + line + '\n';
        }
      });
    });

    return output;
  }

  /**
   * Create a merged document from field selections
   */
  mergeFields(base: any, fieldConflicts: FieldConflict[]): any {
    const merged = JSON.parse(JSON.stringify(base)); // Deep clone

    for (const conflict of fieldConflicts) {
      if (conflict.selectedValue !== undefined) {
        this.setNestedValue(merged, conflict.fieldPath, conflict.selectedValue);
      }
    }

    return merged;
  }

  /**
   * Set a nested value in an object using dot notation
   */
  private setNestedValue(obj: any, path: string, value: any): void {
    const parts = path.split('.');
    let current = obj;

    for (let i = 0; i < parts.length - 1; i++) {
      if (!(parts[i] in current)) {
        current[parts[i]] = {};
      }
      current = current[parts[i]];
    }

    current[parts[parts.length - 1]] = value;
  }

  /**
   * Format value for display
   */
  formatValue(value: any): string {
    if (value === null) return 'null';
    if (value === undefined) return 'undefined';
    if (typeof value === 'object') {
      return JSON.stringify(value, null, 2);
    }
    return String(value);
  }
}
