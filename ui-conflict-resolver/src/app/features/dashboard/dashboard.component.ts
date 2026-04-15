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

    // Poll for conflicts every 10 seconds
    interval(10000)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => this.loadConflicts());
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
