// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * RLJSON-based synchronization between agents.
 *
 * Instead of syncing individual MongoDB operations, this module syncs
 * entire database state as RLJSON tree structures with cryptographic hashes.
 *
 * Benefits:
 * - Send only hashes instead of full documents
 * - Verify data integrity with cryptographic hashes
 * - Efficient delta sync (only send changed trees)
 * - Blob-based storage for document content
 */

import { Bs, BsMem } from '@rljson/bs';

import { MongoAgent } from '../mongo-agent.ts';


import type { Db as MongoDb } from 'mongodb';

import type { Tree } from '../mongo-scanner.ts';
/**
 * RLJSON sync state stored per origin
 */
export interface RljsonSyncState {
  /** Origin node identifier */
  origin: string;
  /** Last root hash seen from this origin */
  lastRootHash: string;
  /** Timestamp of last sync */
  lastSyncedAt: string;
  /** Total tree nodes synced */
  totalNodes: number;
  /** Total blobs synced */
  totalBlobs: number;
}

/**
 * RLJSON tree sync payload
 */
export interface RljsonTreePayload {
  /** Origin node identifier */
  origin: string;
  /** Root hash of the tree */
  rootHash: string;
  /** Total number of nodes in the tree */
  totalNodes: number;
  /** Tree nodes (hash → node mapping) */
  nodes: Array<{
    hash: string;
    node: Tree;
  }>;
  /** Blobs (blobId → content mapping) */
  blobs: Array<{
    blobId: string;
    content: string; // base64 encoded
  }>;
  /** Timestamp when tree was extracted */
  timestamp: string;
}

/**
 * Options for extracting RLJSON tree from MongoDB
 */
export interface ExtractRljsonTreeOptions {
  /** MongoDB database instance */
  mongoDb: MongoDb;
  /** Node identifier */
  nodeId: string;
  /** Blob storage instance (defaults to BsMem) */
  bs?: Bs;
  /** Collections to ignore */
  ignore?: string[];
  /** Collections to include (if specified, only these) */
  include?: string[];
}

/**
 * Options for applying RLJSON tree to MongoDB
 */
export interface ApplyRljsonTreeOptions {
  /** MongoDB database instance */
  mongoDb: MongoDb;
  /** Tree payload to apply */
  payload: RljsonTreePayload;
  /** Blob storage instance */
  bs?: Bs;
}

/**
 * Result of RLJSON tree sync
 */
export interface RljsonSyncResult {
  /** Whether sync was successful */
  success: boolean;
  /** Root hash applied */
  rootHash: string;
  /** Number of nodes applied */
  nodesApplied: number;
  /** Number of blobs received */
  blobsReceived: number;
  /** Number of documents created/updated */
  documentsCreated: number;
  /** Error message if failed */
  error?: string;
}

/**
 * Extracts current MongoDB state as RLJSON tree payload.
 * This creates a snapshot of the database with all documents stored as blobs.
 * @param options - Extraction options
 * @returns RLJSON tree payload ready to send to peers
 */
export async function extractRljsonTree(
  options: ExtractRljsonTreeOptions,
): Promise<RljsonTreePayload> {
  const { mongoDb, nodeId, bs = new BsMem(), ignore, include } = options;

  // Create MongoAgent to extract tree structure
  const agent = new MongoAgent(mongoDb, bs, {
    ignore: ['system.*', 'sync_*', 'state_*', 'rljson_*', ...(ignore || [])],
    include,
  });

  // Extract current state as tree
  const tree = await agent.extract();

  // Collect all nodes
  const nodes = Array.from(tree.trees.entries()).map(([hash, node]) => ({
    hash,
    node,
  }));

  // Collect all blobs (legacy per-document blobs + per-collection ComponentsTable blobs + root TablesCfgTable blob)
  const blobIds = new Set<string>();
  for (const node of tree.trees.values()) {
    const meta = node.meta as any;
    if (meta?.blobId) blobIds.add(meta.blobId);
    if (meta?.componentsBlobId) blobIds.add(meta.componentsBlobId);
    if (meta?.tableCfgsTableBlobId) blobIds.add(meta.tableCfgsTableBlobId);
  }

  const blobs: Array<{ blobId: string; content: string }> = [];
  for (const blobId of blobIds) {
    try {
      const blob = await bs.getBlob(blobId);
      blobs.push({
        blobId,
        content: blob.content.toString('base64'),
      });
    } catch (error) {
      console.error(`Failed to get blob ${blobId}:`, error);
    }
  }

  return {
    origin: nodeId,
    rootHash: tree.rootHash,
    totalNodes: tree.trees.size,
    nodes,
    blobs,
    timestamp: new Date().toISOString(),
  };
}

/**
 * Applies RLJSON tree payload to MongoDB.
 * This reconstructs the database state from the tree structure and blobs.
 * @param options - Application options
 * @returns Result of applying the tree
 */
export async function applyRljsonTree(
  options: ApplyRljsonTreeOptions,
): Promise<RljsonSyncResult> {
  const { mongoDb, payload, bs = new BsMem() } = options;

  try {
    // First, store all blobs
    for (const { content } of payload.blobs) {
      const buffer = Buffer.from(content, 'base64');
      // Store blob - blobId will be auto-generated and returned
      await bs.setBlob(buffer);
    }

    // Build tree map for lookup
    const treeMap = new Map<string, Tree>();
    for (const { hash, node } of payload.nodes) {
      treeMap.set(hash, node);
    }

    // Find root node
    const rootNode = treeMap.get(payload.rootHash);
    if (!rootNode) {
      throw new Error(`Root node not found: ${payload.rootHash}`);
    }

    // Track which documents should exist (for deletion detection)
    const expectedDocsByCollection = new Map<string, Set<any>>();

    // Apply tree structure by reconstructing documents
    let nodesApplied = 0;
    const documentsCreated = await applyTreeNode(
      mongoDb,
      rootNode,
      treeMap,
      bs,
      expectedDocsByCollection,
    );
    nodesApplied++;

    // Delete documents that don't exist in the payload (were deleted in source)
    let deletedCount = 0;
    for (const [collectionName, expectedIds] of expectedDocsByCollection) {
      const existingDocs = await mongoDb
        .collection(collectionName)
        .find({}, { projection: { _id: 1 } })
        .toArray();

      for (const doc of existingDocs) {
        if (!expectedIds.has(String(doc._id))) {
          await mongoDb.collection(collectionName).deleteOne({ _id: doc._id });
          deletedCount++;
        }
      }
    }

    if (deletedCount > 0) {
      // Optionally log deletions
      // console.log(`Deleted ${deletedCount} documents that were removed from source`);
    }

    // Save sync state
    await mongoDb.collection('rljson_sync_state').updateOne(
      { origin: payload.origin },
      {
        $set: {
          origin: payload.origin,
          lastRootHash: payload.rootHash,
          lastSyncedAt: new Date().toISOString(),
          totalNodes: payload.totalNodes,
          totalBlobs: payload.blobs.length,
        } satisfies RljsonSyncState,
      },
      { upsert: true },
    );

    return {
      success: true,
      rootHash: payload.rootHash,
      nodesApplied,
      blobsReceived: payload.blobs.length,
      documentsCreated,
    };
  } catch (error) {
    return {
      success: false,
      rootHash: payload.rootHash,
      nodesApplied: 0,
      documentsCreated: 0,
      blobsReceived: 0,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

/**
 * Recursively applies a tree node and its children.
 *
 * @param node - Tree node to apply
 * @param treeMap - Map of all tree nodes
 * @param bs - Blob storage instance
 * @param expectedDocsByCollection - Tracks which documents should exist (for deletion detection)
 */
async function applyTreeNode(
  mongoDb: MongoDb,
  node: Tree,
  treeMap: Map<string, Tree>,
  bs: Bs,
  expectedDocsByCollection?: Map<string, Set<any>>,
): Promise<number> {
  const meta = node.meta as any;
  let count = 0;

  if (!meta) return count;

  if (meta.type === 'document' && meta.blobId) {
    // Restore document from blob
    const collection = meta.collection;
    if (!collection) return count;

    const blob = await bs.getBlob(meta.blobId);
    const docContent = blob.content.toString('utf-8');
    const doc = JSON.parse(docContent);

    // Track expected document ID
    if (expectedDocsByCollection) {
      if (!expectedDocsByCollection.has(collection)) {
        expectedDocsByCollection.set(collection, new Set());
      }
      expectedDocsByCollection.get(collection)!.add(String(doc._id));
    }

    // Upsert document
    await mongoDb
      .collection(collection)
      .replaceOne({ _id: doc._id }, doc, { upsert: true });
    count++;
  } else if (meta.type === 'collection' && meta.componentsBlobId) {
    // New schema: collection's documents are packed into a single ComponentsTable blob
    const collection = meta.collection ?? meta.name;
    if (!collection) return count;

    const blob = await bs.getBlob(meta.componentsBlobId);
    const componentsTable = JSON.parse(blob.content.toString('utf-8')) as {
      _data: Array<Record<string, unknown>>;
    };

    if (expectedDocsByCollection && !expectedDocsByCollection.has(collection)) {
      expectedDocsByCollection.set(collection, new Set());
    }

    for (const row of componentsTable._data ?? []) {
      // Strip RLJSON-internal hash field; rest is the document
      const { _hash: _ignored, ...doc } = row as Record<string, unknown>;
      if ((doc as any)._id === undefined) continue;

      expectedDocsByCollection
        ?.get(collection)
        ?.add(String((doc as any)._id));

      await mongoDb
        .collection(collection)
        .replaceOne({ _id: (doc as any)._id }, doc as any, { upsert: true });
      count++;
    }
  } else if (meta.type === 'collection' && node.children) {
    // Legacy schema: per-document children
    for (const childHash of node.children) {
      const childNode = treeMap.get(childHash);
      if (childNode) {
        count += await applyTreeNode(
          mongoDb,
          childNode,
          treeMap,
          bs,
          expectedDocsByCollection,
        );
      }
    }
  } else if (meta.type === 'database' && node.children) {
    // Process all collections in this database
    for (const childHash of node.children) {
      const childNode = treeMap.get(childHash);
      if (childNode) {
        count += await applyTreeNode(
          mongoDb,
          childNode,
          treeMap,
          bs,
          expectedDocsByCollection,
        );
      }
    }
  }

  return count;
}
/**
 * Gets RLJSON sync state for an origin.
 * @param mongoDb - MongoDB database instance
 * @param origin - Origin node identifier
 * @returns Sync state or null if not found
 */
export async function getRljsonSyncState(
  mongoDb: MongoDb,
  origin: string,
): Promise<RljsonSyncState | null> {
  const state = await mongoDb
    .collection<RljsonSyncState>('rljson_sync_state')
    .findOne({ origin });

  return state;
}
