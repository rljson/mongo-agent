// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * Simple in-memory storage that mimics the BsMem API
 * but stores data directly as objects instead of blobs
 */
export class SimpleMemoryStorage {
  private storage: Map<string, Buffer>;
  private idCounter: number;

  constructor() {
    this.storage = new Map();
    this.idCounter = 0;
  }

  /**
   * Store a buffer and return blob properties
   * Compatible with Bs interface from @rljson/bs
   */
  async setBlob(content: Buffer): Promise<{ blobId: string; mtime: number }> {
    const blobId = `mem_${this.idCounter++}_${Date.now()}`;
    this.storage.set(blobId, content);
    return {
      blobId,
      mtime: Date.now(),
    };
  }

  /**
   * Retrieve a blob by ID
   * Compatible with Bs interface from @rljson/bs
   */
  async getBlob(blobId: string): Promise<{ content: Buffer } | null> {
    const content = this.storage.get(blobId);
    if (!content) {
      return null;
    }
    return { content };
  }

  /**
   * Check if a blob exists
   */
  has(blobId: string): boolean {
    return this.storage.has(blobId);
  }

  /**
   * Get the number of stored blobs
   */
  size(): number {
    return this.storage.size;
  }

  /**
   * Clear all stored data
   */
  clear(): void {
    this.storage.clear();
    this.idCounter = 0;
  }
}
