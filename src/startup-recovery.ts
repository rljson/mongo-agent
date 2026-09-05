// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import type { Db } from 'mongodb';
import type { Logger } from './watch-changes.ts';

/**
 * Resume token document stored in sync_resume collection.
 */
export interface ResumeTokenDoc {
  /** Document ID */
  _id: string;
  /** Resume token */
  token: unknown;
  /** Last update timestamp */
  updatedAt: string;
}

/**
 * Resume token validation result.
 */
export interface TokenValidityResult {
  /** Whether a resume token exists */
  hasToken: boolean;
  /** Whether the token is valid (true, false, or 'unknown') */
  isValid: boolean | 'unknown';
  /** Age of token in minutes */
  ageMinutes?: number;
  /** Age of token in hours */
  ageHours?: number;
  /** Success message */
  message?: string;
  /** Warning message if token may be invalid */
  warning?: string;
  /** Recommendation for handling old/invalid tokens */
  recommendation?: string;
  /** Error message if check failed */
  error?: string;
}

/**
 * Options for checking resume token validity.
 */
export interface CheckResumeTokenOptions {
  /** MongoDB database instance */
  db: Db;
  /** Logger instance */
  logger?: Logger;
}

/**
 * Options for performing startup recovery.
 */
export interface PerformStartupRecoveryOptions {
  /** MongoDB database instance */
  db: Db;
  /** Node identifier */
  nodeId: string;
  /** Logger instance */
  logger?: Logger;
}

/**
 * Options for clearing resume token.
 */
export interface ClearResumeTokenOptions {
  /** MongoDB database instance */
  db: Db;
  /** Logger instance */
  logger?: Logger;
}

/**
 * Check if resume token is still valid.
 * MongoDB resume tokens can become invalid if:
 * - Oplog has been truncated (older than oldest oplog entry)
 * - Too much time has passed
 * Returns info about resume token validity and recommendations.
 * @param options - Check options
 * @returns Token validity result
 */
export async function checkResumeTokenValidity(
  options: CheckResumeTokenOptions
): Promise<TokenValidityResult> {
  const { db, logger } = options;

  try {
    const resumeDoc = await db
      .collection<ResumeTokenDoc>('sync_resume')
      .findOne({ _id: 'resume' });

    if (!resumeDoc || !resumeDoc.token) {
      logger?.info?.('No resume token found - starting fresh change stream');
      return {
        hasToken: false,
        isValid: true,
        message: 'No resume token - will start from current position',
      };
    }

    const tokenAge = Date.now() - new Date(resumeDoc.updatedAt).getTime();
    const ageMinutes = Math.floor(tokenAge / 1000 / 60);
    const ageHours = Math.floor(ageMinutes / 60);

    logger?.info?.(
      {
        tokenAge: `${ageMinutes} minutes`,
        updatedAt: resumeDoc.updatedAt,
      },
      'Resume token found'
    );

    // Check oplog size and window
    const adminDb = db.admin();
    const replStatus = (await adminDb
      .command({ replSetGetStatus: 1 })
      .catch(() => null)) as { optimes?: unknown } | null;

    if (replStatus?.optimes) {
      const oplogInfo = replStatus.optimes;
      logger?.info?.({ oplogInfo }, 'Oplog status checked');
    }

    // Warn if token is older than 1 hour (might be at risk)
    if (ageHours >= 1) {
      logger?.warn?.(
        {
          tokenAgeHours: ageHours,
          risk: 'Resume token may be outside oplog window',
        },
        'Resume token is old - consider state hash verification'
      );

      return {
        hasToken: true,
        isValid: 'unknown',
        ageHours,
        warning:
          'Token is old - changes during offline period may be missed if oplog was truncated',
        recommendation:
          'Run state hash comparison to verify databases are in sync',
      };
    }

    return {
      hasToken: true,
      isValid: true,
      ageMinutes,
      message: 'Resume token is recent and likely valid',
    };
  } catch (err) {
    const error = err as Error;
    logger?.error?.({ err: error.message }, 'Error checking resume token validity');
    return {
      hasToken: false,
      isValid: false,
      error: error.message,
    };
  }
}

/**
 * Perform startup recovery checks.
 * This should be called during agent initialization.
 * @param options - Recovery options
 * @returns Token validity result
 */
export async function performStartupRecovery(
  options: PerformStartupRecoveryOptions
): Promise<TokenValidityResult> {
  const { db, nodeId, logger } = options;

  logger?.info?.({ nodeId }, 'Starting recovery checks...');

  // Check resume token status
  const tokenStatus = await checkResumeTokenValidity({ db, logger });

  // If token is old or invalid, log recommendations
  if (tokenStatus.warning || !tokenStatus.isValid) {
    logger?.warn?.(
      {
        nodeId,
        tokenStatus,
        recommendation:
          'Consider running state hash comparison or tamper detection to verify sync integrity',
      },
      'Startup recovery: potential sync gap detected'
    );
  } else {
    logger?.info?.({ nodeId }, 'Startup recovery: no issues detected');
  }

  return tokenStatus;
}

/**
 * Clear resume token (for testing or forcing full resync).
 * @param options - Clear options
 * @returns Promise that resolves when token is cleared
 */
export async function clearResumeToken(
  options: ClearResumeTokenOptions
): Promise<void> {
  const { db, logger } = options;

  await db.collection('sync_resume').deleteOne({ _id: 'resume' } as Record<string, unknown>);
  logger?.info?.('Resume token cleared');
}
