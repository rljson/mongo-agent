import { CommonModule } from '@angular/common';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';

import { formatDistanceToNow } from 'date-fns';
import { interval, Subject, takeUntil } from 'rxjs';

import { SyncApiService } from '../../services/sync-api.service';


interface NodeInfo {
  nodeId: string;
  role: 'hub' | 'client' | 'unassigned';
  hostname: string;
  address: string;
  port: number;
  uptime: number;
  startedAt: number;
  transportReady: boolean;
}

interface SyncAgent {
  type: 'mongo' | 'fs';
  name: string;
  icon: string;
  treeKey: string;
  status: 'syncing' | 'idle' | 'error';
  lastRef?: string;
  lastSyncAt?: number;
}

interface Alert {
  id: string;
  type: 'warning' | 'info' | 'error';
  message: string;
  timestamp: number;
  action?: { label: string; route: string };
}

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink],
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss'],
})
export class DashboardComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();

  // Node Info
  nodeInfo: NodeInfo = {
    nodeId: 'NodeA-abc123',
    role: 'hub',
    hostname: 'localhost',
    address: '127.0.0.1',
    port: 3000,
    uptime: 0,
    startedAt: Date.now(),
    transportReady: true,
  };

  // Network Stats
  networkStats = {
    topologyFormedBy: 'manual',
    nodesTotal: 1,
    nodesReachable: 1,
  };

  // Sync Agents
  syncAgents: SyncAgent[] = [
    {
      type: 'mongo',
      name: 'MongoDB Sync',
      icon: '🗄️',
      treeKey: 'mongoTree',
      status: 'syncing',
      lastRef: 'abc12ef...',
      lastSyncAt: Date.now() - 5000,
    },
  ];

  // Alerts
  alerts: Alert[] = [];
  conflictsCount = 0;

  // Service control (Hub + agents). `null` = not yet probed.
  services: { hub: boolean | null; l1: boolean | null; l2: boolean | null } = {
    hub: null,
    l1: null,
    l2: null,
  };
  // Per-button "starting" state to disable the button while a launch is in flight.
  starting: { hub: boolean; l1: boolean; l2: boolean } = {
    hub: false,
    l1: false,
    l2: false,
  };
  stopping: { hub: boolean; l1: boolean; l2: boolean } = {
    hub: false,
    l1: false,
    l2: false,
  };

  // Repair panel state
  repair: {
    running: 'chain' | 'peer' | 'backfill' | null;
    output: string;
    exitCode: number | null;
    chainColl: string;
    peerColl: string;
    backfillColl: string;
  } = {
    running: null,
    output: '',
    exitCode: null,
    chainColl: '',
    peerColl: 'customers',
    backfillColl: 'customers',
  };

  // Hash status snapshot (read from state_checkpoints)
  hashStatus: {
    loading: boolean;
    dbRoot: string | null;
    ts: number | null;
    mode: string | null;
    perColl: Array<{ name: string; root: string; partitions: number }>;
  } = {
    loading: false,
    dbRoot: null,
    ts: null,
    mode: null,
    perColl: [],
  };

  // Chain inspector state
  chainState: {
    loading: boolean;
    origins: string[];
    selectedOrigin: string;
    ops: Array<{
      _id: string;
      origin: string;
      seq: number;
      operationType: string;
      ns: { db: string; coll: string };
      docId: string | null;
      prevHash: string;
      opHash: string;
      chainHash: string;
      ts: string;
    }>;
    verifying: boolean;
    valid: boolean | null;
    firstBreakAt: { origin: string; seq: number } | null;
    total: number;
  } = {
    loading: false,
    origins: [],
    selectedOrigin: '',
    ops: [],
    verifying: false,
    valid: null,
    firstBreakAt: null,
    total: 0,
  };

  // Partition map state
  partitionState: {
    loading: boolean;
    collFilter: string;
    byColl: Array<{
      coll: string;
      partitions: Array<{
        idx: number;
        count: number;
        root: string;
        minId: string | null;
        maxId: string | null;
        updatedAt: string | null;
      }>;
    }>;
  } = {
    loading: false,
    collFilter: '',
    byColl: [],
  };

  constructor(private syncApi: SyncApiService) {}

  ngOnInit(): void {
    // Update uptime every second
    interval(1000)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        this.nodeInfo.uptime = Date.now() - this.nodeInfo.startedAt;
      });

    // Load conflicts for alerts
    this.loadConflicts();
    this.loadServicesStatus();
    this.loadHashStatus();
    this.loadChainOrigins();

    // Poll for conflicts every 10 seconds
    interval(10000)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => this.loadConflicts());

    // Poll service liveness every 5 seconds.
    interval(5000)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => this.loadServicesStatus());

    // Hash status changes infrequently — refresh every 30s.
    interval(30000)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => this.loadHashStatus());
  }

  loadServicesStatus(): void {
    this.syncApi.getServicesStatus().subscribe({
      next: (s) => {
        this.services = s;
      },
      error: () => {
        // API unreachable — show all as down.
        this.services = { hub: false, l1: false, l2: false };
      },
    });
  }

  startHub(): void {
    this.starting.hub = true;
    this.syncApi.startHub().subscribe({
      next: () => {
        this.starting.hub = false;
        this.loadServicesStatus();
      },
      error: () => {
        this.starting.hub = false;
        this.loadServicesStatus();
      },
    });
  }

  startAgentL1(): void {
    this.starting.l1 = true;
    this.syncApi.startAgentL1().subscribe({
      next: () => {
        this.starting.l1 = false;
        this.loadServicesStatus();
      },
      error: () => {
        this.starting.l1 = false;
        this.loadServicesStatus();
      },
    });
  }

  startAgentL2(): void {
    this.starting.l2 = true;
    this.syncApi.startAgentL2().subscribe({
      next: () => {
        this.starting.l2 = false;
        this.loadServicesStatus();
      },
      error: () => {
        this.starting.l2 = false;
        this.loadServicesStatus();
      },
    });
  }

  stopHub(): void {
    this.stopping.hub = true;
    this.syncApi.stopHub().subscribe({
      next: () => {
        this.stopping.hub = false;
        this.loadServicesStatus();
      },
      error: () => {
        this.stopping.hub = false;
        this.loadServicesStatus();
      },
    });
  }

  stopAgentL1(): void {
    this.stopping.l1 = true;
    this.syncApi.stopAgentL1().subscribe({
      next: () => {
        this.stopping.l1 = false;
        this.loadServicesStatus();
      },
      error: () => {
        this.stopping.l1 = false;
        this.loadServicesStatus();
      },
    });
  }

  stopAgentL2(): void {
    this.stopping.l2 = true;
    this.syncApi.stopAgentL2().subscribe({
      next: () => {
        this.stopping.l2 = false;
        this.loadServicesStatus();
      },
      error: () => {
        this.stopping.l2 = false;
        this.loadServicesStatus();
      },
    });
  }

  // ---- Repair handlers ---------------------------------------------------

  runRestoreFromChain(dryRun: boolean): void {
    this.repair.running = 'chain';
    this.repair.output = dryRun
      ? 'Running restore-from-chain (DRY_RUN)…'
      : 'Running restore-from-chain (LIVE)…';
    this.repair.exitCode = null;
    this.syncApi.restoreFromChain(dryRun).subscribe({
      next: (r) => {
        this.repair.running = null;
        this.repair.output = (r.stdout || '') + (r.stderr ? '\n' + r.stderr : '');
        this.repair.exitCode = r.exitCode;
      },
      error: (err) => {
        this.repair.running = null;
        this.repair.output = 'Error: ' + (err?.message || err);
        this.repair.exitCode = -1;
      },
    });
  }

  runRestoreFromPeer(): void {
    this.repair.running = 'peer';
    this.repair.output = `Running restore-from-peer (coll=${this.repair.peerColl || 'all'})…`;
    this.repair.exitCode = null;
    this.syncApi.restoreFromPeer(this.repair.peerColl || undefined).subscribe({
      next: (r) => {
        this.repair.running = null;
        this.repair.output = (r.stdout || '') + (r.stderr ? '\n' + r.stderr : '');
        this.repair.exitCode = r.exitCode;
      },
      error: (err) => {
        this.repair.running = null;
        this.repair.output = 'Error: ' + (err?.message || err);
        this.repair.exitCode = -1;
      },
    });
  }

  runBackfillHashes(): void {
    this.repair.running = 'backfill';
    this.repair.output = `Running backfill-hashes (coll=${this.repair.backfillColl || 'all'})…`;
    this.repair.exitCode = null;
    this.syncApi.backfillHashes(this.repair.backfillColl || undefined).subscribe({
      next: (r) => {
        this.repair.running = null;
        this.repair.output = (r.stdout || '') + (r.stderr ? '\n' + r.stderr : '');
        this.repair.exitCode = r.exitCode;
      },
      error: (err) => {
        this.repair.running = null;
        this.repair.output = 'Error: ' + (err?.message || err);
        this.repair.exitCode = -1;
      },
    });
  }

  // ---- Hash status -------------------------------------------------------

  loadHashStatus(): void {
    this.hashStatus.loading = true;
    this.syncApi.getHashStatus().subscribe({
      next: (resp) => {
        const cp = resp.checkpoint as
          | {
              dbRoot?: string;
              ts?: number;
              mode?: string;
              collections?: Record<string, { root: string; partitions: number }>;
            }
          | null
          | undefined;
        if (cp) {
          this.hashStatus.dbRoot = cp.dbRoot ?? null;
          this.hashStatus.ts = cp.ts ?? null;
          this.hashStatus.mode = cp.mode ?? null;
          this.hashStatus.perColl = Object.entries(cp.collections || {})
            .map(([name, meta]) => ({
              name,
              root: meta.root,
              partitions: meta.partitions,
            }))
            .sort((a, b) => a.name.localeCompare(b.name));
        } else {
          this.hashStatus.dbRoot = null;
          this.hashStatus.ts = null;
          this.hashStatus.mode = null;
          this.hashStatus.perColl = [];
        }
        this.hashStatus.loading = false;
      },
      error: () => {
        this.hashStatus.loading = false;
      },
    });
  }

  shortHash(h: string | null | undefined): string {
    if (!h) return '—';
    return h.length > 16 ? h.slice(0, 12) + '…' : h;
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  loadConflicts(): void {
    this.syncApi.getConflicts().subscribe({
      next: (conflicts) => {
        const pending = conflicts.filter((c) => c.status === 'pending');
        this.conflictsCount = pending.length;

        // Create alert if there are conflicts
        if (pending.length > 0) {
          this.alerts = [
            {
              id: 'conflicts',
              type: 'warning',
              message: `${pending.length} conflict${pending.length > 1 ? 's' : ''} ${pending.length > 1 ? 'need' : 'needs'} resolution`,
              timestamp: Date.now(),
              action: { label: 'View Conflicts', route: '/conflicts' },
            },
          ];
        } else {
          this.alerts = this.alerts.filter((a) => a.id !== 'conflicts');
        }
      },
      error: () => {
        // Silently handle errors
      },
    });
  }

  getUptime(): string {
    return formatDistanceToNow(this.nodeInfo.startedAt, { addSuffix: false });
  }

  getLastSyncTime(agent: SyncAgent): string {
    if (!agent.lastSyncAt) return 'Never';
    return formatDistanceToNow(agent.lastSyncAt, { addSuffix: true });
  }

  getStatusClass(status: string): string {
    const classes: Record<string, string> = {
      syncing: 'status-syncing',
      idle: 'status-idle',
      error: 'status-error',
    };
    return classes[status] || '';
  }

  getAlertClass(type: string): string {
    const classes: Record<string, string> = {
      warning: 'alert-warning',
      info: 'alert-info',
      error: 'alert-error',
    };
    return classes[type] || '';
  }

  getAlertIcon(type: string): string {
    const icons: Record<string, string> = {
      warning: '⚠️',
      info: 'ℹ️',
      error: '❌',
    };
    return icons[type] || 'ℹ️';
  }

  // ---- Chain inspector --------------------------------------------------

  loadChainOrigins(): void {
    this.syncApi.getChainOrigins().subscribe({
      next: (resp) => {
        this.chainState.origins = resp.origins || [];
        if (
          !this.chainState.selectedOrigin &&
          this.chainState.origins.length > 0
        ) {
          this.chainState.selectedOrigin = this.chainState.origins[0];
          this.loadChainOps();
        }
      },
    });
  }

  loadChainOps(): void {
    if (!this.chainState.selectedOrigin) return;
    this.chainState.loading = true;
    this.syncApi.getChain(this.chainState.selectedOrigin, 50, 0).subscribe({
      next: (resp) => {
        this.chainState.ops = resp.ops;
        this.chainState.loading = false;
      },
      error: () => {
        this.chainState.loading = false;
      },
    });
  }

  /**
   * Each op's `prevHash` should equal the previous op's `chainHash` for
   * the SAME origin. Visually we just compute the table-row class —
   * 'broken' when the link doesn't match. The full verify covers the
   * whole collection (this is just the visible page).
   */
  isChainLinkBroken(idx: number): boolean {
    if (idx === 0) {
      return this.chainState.ops[0]?.prevHash !== 'GENESIS';
    }
    const prev = this.chainState.ops[idx - 1];
    const cur = this.chainState.ops[idx];
    return !!prev && !!cur && cur.prevHash !== prev.chainHash;
  }

  verifyChainAll(): void {
    this.chainState.verifying = true;
    this.syncApi
      .verifyChainLinks(this.chainState.selectedOrigin || undefined)
      .subscribe({
        next: (r) => {
          this.chainState.valid = r.valid;
          this.chainState.firstBreakAt = r.firstBreakAt;
          this.chainState.total = r.total;
          this.chainState.verifying = false;
        },
        error: () => {
          this.chainState.verifying = false;
        },
      });
  }

  // ---- Partition map ----------------------------------------------------

  loadPartitions(): void {
    this.partitionState.loading = true;
    this.syncApi
      .getPartitions(this.partitionState.collFilter || undefined)
      .subscribe({
        next: (resp) => {
          // Group flat list by collection.
          type ByCollEntry = (typeof this.partitionState.byColl)[0];
          const map = new Map<string, ByCollEntry>();
          for (const p of resp.partitions) {
            let entry = map.get(p.coll);
            if (!entry) {
              entry = { coll: p.coll, partitions: [] };
              map.set(p.coll, entry);
            }
            entry.partitions.push({
              idx: p.idx,
              count: p.count,
              root: p.root,
              minId: p.minId,
              maxId: p.maxId,
              updatedAt: p.updatedAt,
            });
          }
          this.partitionState.byColl = Array.from(map.values()).sort(
            (a, b) => a.coll.localeCompare(b.coll),
          );
          this.partitionState.loading = false;
        },
        error: () => {
          this.partitionState.loading = false;
        },
      });
  }
}
