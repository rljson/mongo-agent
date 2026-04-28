import { CommonModule } from '@angular/common';
import { Component, OnDestroy, OnInit } from '@angular/core';
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
  imports: [CommonModule, RouterLink],
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

    // Poll for conflicts every 10 seconds
    interval(10000)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => this.loadConflicts());

    // Poll service liveness every 5 seconds.
    interval(5000)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => this.loadServicesStatus());
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
}
