// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { createHash } from 'node:crypto';

/**
 * Computes SHA-256 hash of a string input
 * @param input - String or value to hash
 * @returns 64-character hexadecimal hash
 */
export function sha256Hex(input: string | number | boolean): string {
  return createHash('sha256').update(String(input)).digest('hex');
}

/**
 * Recursively stringifies an object with sorted keys for deterministic output
 * @param value - Value to stringify
 * @returns Canonical JSON string representation
 */
function stableStringify(value: unknown): string {
  if (value === null || value === undefined) {
    return 'null';
  }

  if (typeof value !== 'object') {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return '[' + value.map(stableStringify).join(',') + ']';
  }

  const keys = Object.keys(value).sort();
  const parts: string[] = [];

  for (const k of keys) {
    // Never include __h field in content hash
    if (k === '__h') continue;

    const val = (value as Record<string, unknown>)[k];
    parts.push(JSON.stringify(k) + ':' + stableStringify(val));
  }

  return '{' + parts.join(',') + '}';
}

/**
 * Computes integrity hash for a document.
 * The hash is deterministic (same document always produces same hash)
 * and excludes the __h field itself to avoid circular dependencies.
 * @param doc - Document object to hash
 * @returns SHA-256 hash of the canonical representation
 */
export function computeIntegrityHash(doc: Record<string, unknown>): string {
  return sha256Hex(stableStringify(doc));
}

/**
 * Sync operation structure
 */
export interface SyncOp {
  ns?: unknown;
  operationType?: string | null;
  docId?: unknown;
  payload?: unknown;
  ts?: unknown;
  resumeToken?: unknown; // Intentionally excluded from hash
}

/**
 * Computes deterministic hash for a sync operation.
 * Excludes resumeToken as it can vary between clusters.
 * @param op - Sync operation object
 * @returns SHA-256 hash of operation's relevant fields
 */
export function computeOpHash(op: SyncOp | null | undefined): string {
  const slim = {
    ns: op?.ns ?? null,
    operationType: op?.operationType ?? null,
    docId: op?.docId ?? null,
    payload: op?.payload ?? null,
    ts: op?.ts ?? null,
  };

  return sha256Hex(stableStringify(slim));
}
