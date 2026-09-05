// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';
import {
  computeIntegrityHash,
  computeOpHash,
  sha256Hex,
  type SyncOp,
} from '../../src/hashing/integrity-hash.ts';

describe('integrity-hash', () => {
  describe('sha256Hex', () => {
    it('computes correct SHA-256 hash for "hello world"', () => {
      const hash = sha256Hex('hello world');
      expect(hash).toBe(
        'b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9'
      );
    });

    it('returns 64-character hex string', () => {
      const hash = sha256Hex('test');
      expect(hash).toHaveLength(64);
      expect(hash).toMatch(/^[a-f0-9]{64}$/);
    });

    it('handles numbers and booleans', () => {
      expect(sha256Hex(42)).toHaveLength(64);
      expect(sha256Hex(true)).toHaveLength(64);
    });
  });

  describe('computeIntegrityHash', () => {
    it('computes hash for simple document', () => {
      const doc = {
        _id: '507f1f77bcf86cd799439011',
        name: 'Test Document',
        value: 42,
      };

      const hash = computeIntegrityHash(doc);
      expect(hash).toHaveLength(64);
    });

    it('produces same hash for same document', () => {
      const doc = { title: 'Test', count: 10 };
      const hash1 = computeIntegrityHash(doc);
      const hash2 = computeIntegrityHash(doc);
      expect(hash1).toBe(hash2);
    });

    it('produces different hash for different document', () => {
      const doc1 = { title: 'Test', count: 10 };
      const doc2 = { title: 'Test', count: 11 };
      const hash1 = computeIntegrityHash(doc1);
      const hash2 = computeIntegrityHash(doc2);
      expect(hash1).not.toBe(hash2);
    });

    it('excludes __h field from hash computation', () => {
      const doc1 = { title: 'Test', count: 10 };
      const doc2 = { title: 'Test', count: 10, __h: 'some-hash-value' };
      const hash1 = computeIntegrityHash(doc1);
      const hash2 = computeIntegrityHash(doc2);
      expect(hash1).toBe(hash2); // Should be same since __h is excluded
    });

    it('handles nested objects', () => {
      const doc = {
        user: {
          name: 'Alice',
          metadata: {
            role: 'admin',
          },
        },
      };
      const hash = computeIntegrityHash(doc);
      expect(hash).toHaveLength(64);
    });

    it('handles arrays', () => {
      const doc = {
        tags: ['javascript', 'typescript', 'node'],
        counts: [1, 2, 3],
      };
      const hash = computeIntegrityHash(doc);
      expect(hash).toHaveLength(64);
    });

    it('is sensitive to array order', () => {
      const doc1 = { tags: ['a', 'b', 'c'] };
      const doc2 = { tags: ['c', 'b', 'a'] };
      const hash1 = computeIntegrityHash(doc1);
      const hash2 = computeIntegrityHash(doc2);
      expect(hash1).not.toBe(hash2);
    });

    it('is NOT sensitive to field order (canonical)', () => {
      const doc1 = { z: 3, a: 1, m: 2 };
      const doc2 = { a: 1, m: 2, z: 3 };
      const hash1 = computeIntegrityHash(doc1);
      const hash2 = computeIntegrityHash(doc2);
      expect(hash1).toBe(hash2);
    });
  });

  describe('computeOpHash', () => {
    it('computes hash for sync operation', () => {
      const op: SyncOp = {
        ns: 'test.collection',
        operationType: 'insert',
        docId: '507f1f77bcf86cd799439011',
        payload: { title: 'Test' },
        ts: Date.now(),
      };

      const hash = computeOpHash(op);
      expect(hash).toHaveLength(64);
    });

    it('excludes resumeToken from hash', () => {
      const baseOp: SyncOp = {
        ns: 'test.collection',
        operationType: 'insert',
        docId: '507f1f77bcf86cd799439011',
        payload: { title: 'Test' },
        ts: 1234567890,
      };

      const op1: SyncOp = { ...baseOp, resumeToken: 'token-1' };
      const op2: SyncOp = { ...baseOp, resumeToken: 'token-2' };

      const hash1 = computeOpHash(op1);
      const hash2 = computeOpHash(op2);

      expect(hash1).toBe(hash2); // Should be same since resumeToken excluded
    });

    it('handles null/undefined operation', () => {
      const hash1 = computeOpHash(null);
      const hash2 = computeOpHash(undefined);
      expect(hash1).toBe(hash2);
      expect(hash1).toHaveLength(64);
    });

    it('handles partial operation', () => {
      const op: SyncOp = {
        ns: 'test.collection',
        operationType: 'delete',
      };
      const hash = computeOpHash(op);
      expect(hash).toHaveLength(64);
    });
  });
});
