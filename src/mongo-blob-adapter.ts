// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Bs, BsMem } from '@rljson/bs';
import { Json } from '@rljson/json';


import type { Document } from 'mongodb';

// .............................................................................
// Types
// .............................................................................

/**
 * Document metadata returned when storing a document as a blob
 */
export interface DocumentBlobMeta extends Json {
  /** Document ID */
  docId: string;
  /** Collection name */
  collection: string;
  /** Database name */
  database: string;
  /** Blob ID where content is stored */
  blobId: string;
  /** Document size in bytes */
  size: number;
  /** Last modified timestamp (milliseconds since epoch) */
  mtime: number;
}

/**
 * Options for document-to-blob conversion
 */
export interface DocumentToBlobOptions {
  /** Custom blob storage (defaults to BsMem) */
  bs?: Bs;
}

/**
 * Options for blob-to-document conversion
 */
export interface BlobToDocumentOptions {
  /** Custom blob storage (defaults to BsMem) */
  bs?: Bs;
}

// .............................................................................
// MongoBlobAdapter Class
// .............................................................................

/**
 * Handles conversion between MongoDB documents and blobs in blob storage
 */
export class MongoBlobAdapter {
  private _bs: Bs;

  constructor(bs?: Bs) {
    this._bs = bs || new BsMem();
  }

  /**
   * Gets the blob storage instance
   */
  get bs(): Bs {
    return this._bs;
  }

  /**
   * Converts a document to a blob and returns metadata
   * @param doc - MongoDB document
   * @param database - Database name
   * @param collection - Collection name
   * @param options - Conversion options
   * @returns Document metadata including blob ID
   */
  async documentToBlob(
    doc: Document,
    database: string,
    collection: string,
    options: DocumentToBlobOptions = {},
  ): Promise<DocumentBlobMeta> {
    const bs = options.bs || this._bs;

    // Serialize document to JSON
    const content = JSON.stringify(doc);
    const buffer = Buffer.from(content, 'utf-8');

    // Store content in blob storage
    const blobProps = await bs.setBlob(buffer);

    // Build metadata
    const metadata: DocumentBlobMeta = {
      docId: String(doc._id),
      collection,
      database,
      blobId: blobProps.blobId,
      size: buffer.length,
      mtime: Date.now(),
    };

    return metadata;
  }

  /**
   * Converts multiple documents to blobs
   * @param docs - Array of MongoDB documents
   * @param database - Database name
   * @param collection - Collection name
   * @param options - Conversion options
   * @returns Array of document metadata
   */
  async documentsToBlobs(
    docs: Document[],
    database: string,
    collection: string,
    options: DocumentToBlobOptions = {},
  ): Promise<DocumentBlobMeta[]> {
    const results: DocumentBlobMeta[] = [];

    for (const doc of docs) {
      const metadata = await this.documentToBlob(
        doc,
        database,
        collection,
        options,
      );
      results.push(metadata);
    }

    return results;
  }

  /**
   * Retrieves a document from a blob using metadata
   * @param metadata - Document metadata including blob ID
   * @param options - Conversion options
   * @returns Parsed MongoDB document
   */
  async blobToDocument(
    metadata: DocumentBlobMeta,
    options: BlobToDocumentOptions = {},
  ): Promise<Document> {
    const bs = options.bs || this._bs;

    // Get blob content
    const blob = await bs.getBlob(metadata.blobId);

    // Parse JSON
    const content = blob.content.toString('utf-8');
    const doc = JSON.parse(content) as Document;

    return doc;
  }

  /**
   * Retrieves multiple documents from blobs
   * @param metadataList - Array of document metadata
   * @param options - Conversion options
   * @returns Array of parsed documents
   */
  async blobsToDocuments(
    metadataList: DocumentBlobMeta[],
    options: BlobToDocumentOptions = {},
  ): Promise<Document[]> {
    const results: Document[] = [];

    for (const metadata of metadataList) {
      const doc = await this.blobToDocument(metadata, options);
      results.push(doc);
    }

    return results;
  }
}
