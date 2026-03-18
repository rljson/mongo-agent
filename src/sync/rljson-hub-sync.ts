// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/**
 * RLJSON-based peer synchronization via hub.
 *
 * This module provides functions to sync RLJSON trees between agents
 * through the hub relay service, using hashes instead of raw JSON.
 */

import type { Logger } from '../watch-changes.ts';
import type { RljsonTreePayload } from './rljson-sync.ts';

/**
 * Options for syncing RLJSON tree from peer via hub.
 */
export interface SyncRljsonFromHubOptions {
  /** Fastify instance with logger */
  fastify: { log: Logger };
  /** Hub URL for relay service */
  hubUrl: string;
  /** Peer client identifier */
  peerClientId: string;
  /** Local node identifier */
  localNodeId: string;
}

/**
 * Result of RLJSON sync from hub.
 */
export interface RljsonSyncFromHubResult {
  /** Whether sync was successful */
  success: boolean;
  /** Root hash received */
  rootHash?: string;
  /** Number of nodes received */
  nodesReceived?: number;
  /** Number of blobs received */
  blobsReceived?: number;
  /** Error message if failed */
  error?: string;
}

/**
 * Fetches RLJSON tree from peer via hub and applies it locally.
 *
 * This implements hash-based synchronization where:
 * 1. Request RLJSON tree from peer (via hub relay)
 * 2. Receive tree structure with hashes
 * 3. Receive blobs (document content)  * 4. Apply tree to local MongoDB
 * @param options - Sync options
 * @returns Result of synchronization
 */
export async function syncRljsonTreeFromHub(
  options: SyncRljsonFromHubOptions,
): Promise<RljsonSyncFromHubResult> {
  const { fastify, hubUrl, peerClientId } = options;

  try {
    // Fetch RLJSON tree from peer via hub relay
    const url = `${hubUrl}/hub/relay/${peerClientId}/rljson/tree`;

    fastify.log.info?.(
      { peer: peerClientId, url },
      'Fetching RLJSON tree from peer',
    );

    const resp = await fetch(url, {
      method: 'GET',
      headers: {
        'content-type': 'application/json',
      },
    });

    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`Failed to fetch RLJSON tree: ${resp.status} ${text}`);
    }

    const data = (await resp.json()) as {
      ok: boolean;
      payload?: RljsonTreePayload;
    };

    if (!data.ok || !data.payload) {
      throw new Error('Invalid response from peer');
    }

    const payload = data.payload;

    fastify.log.info?.(
      {
        peer: peerClientId,
        rootHash: payload.rootHash,
        nodes: payload.totalNodes,
        blobs: payload.blobs.length,
      },
      'Received RLJSON tree from peer',
    );

    // Now apply the tree to local MongoDB via our own /rljson/sync endpoint
    // This keeps the logic centralized
    const applyUrl = `http://localhost:${process.env.PORT || 3001}/rljson/sync`;

    const applyResp = await fetch(applyUrl, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify(payload),
    });

    if (!applyResp.ok) {
      const text = await applyResp.text();
      throw new Error(
        `Failed to apply RLJSON tree: ${applyResp.status} ${text}`,
      );
    }

    const applyData = (await applyResp.json()) as {
      ok: boolean;
      result?: {
        success: boolean;
        rootHash: string;
        nodesApplied: number;
        blobsReceived: number;
        error?: string;
      };
    };

    if (!applyData.ok || !applyData.result?.success) {
      throw new Error(
        `Failed to apply tree: ${applyData.result?.error || 'Unknown error'}`,
      );
    }

    fastify.log.info?.(
      {
        peer: peerClientId,
        rootHash: applyData.result.rootHash,
        nodesApplied: applyData.result.nodesApplied,
        blobsReceived: applyData.result.blobsReceived,
      },
      'Successfully synced RLJSON tree from peer',
    );

    return {
      success: true,
      rootHash: applyData.result.rootHash,
      nodesReceived: applyData.result.nodesApplied,
      blobsReceived: applyData.result.blobsReceived,
    };
  } catch (error) {
    fastify.log.error?.(
      {
        peer: peerClientId,
        error: error instanceof Error ? error.message : String(error),
      },
      'RLJSON sync from hub failed',
    );

    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}
