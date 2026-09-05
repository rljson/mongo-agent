/**
 * Types for RLJSON MongoDB Synchronization Conflicts
 */

export interface DocumentVersion {
  documentId: string;
  data: any; // The actual document data
  timestamp: number;
  nodeId: string;
  operationId: string;
  operationType: 'insert' | 'update' | 'delete' | 'replace';
  stateHash?: string;
  componentsHash?: string;
}

export interface ConflictInfo {
  conflictId: string;
  documentId: string;
  collection: string;
  database: string;
  detectedAt: number;
  status: 'pending' | 'resolved' | 'ignored';
  versions: DocumentVersion[];
  conflictType: 'concurrent-update' | 'update-delete' | 'concurrent-insert';
}

export interface ConflictResolution {
  conflictId: string;
  resolutionType: 'use-local' | 'use-remote' | 'manual-merge' | 'field-merge';
  selectedVersion?: DocumentVersion;
  mergedDocument?: any;
  resolvedBy: string;
  resolvedAt: number;
  reason?: string;
}

export interface FieldConflict {
  fieldPath: string;
  localValue: any;
  remoteValue: any;
  selectedValue?: any;
}

export interface DiffResult {
  added: string[];
  removed: string[];
  modified: Array<{
    field: string;
    oldValue: any;
    newValue: any;
  }>;
}

export interface SyncAgentStatus {
  nodeId: string;
  lastSync: number;
  stateHash: string;
  pendingOperations: number;
  isOnline: boolean;
}

export interface MergeMetadata {
  operationChain: string[];
  hashChainValid: boolean;
  merkleTreeState: string;
  conflictingNodes: string[];
}
