import { CommonModule } from '@angular/common';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';

import { formatDistanceToNow } from 'date-fns';
import { interval, Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';


type NodeRole = 'hub' | 'client' | 'unassigned';
type FormationMethod = 'broadcast' | 'cloud' | 'static' | 'election' | 'manual';
type PeerState = 'reachable' | 'unreachable' | 'timeout';

interface NodeInfo {
  nodeId: string;
  hostname: string;
  addresses: string[];
  port: number;
  role: NodeRole;
  startedAt: Date;
  isLocal?: boolean;
}

interface PeerProbe {
  nodeId: string;
  hostname: string;
  address: string;
  latencyMs: number;
  failCount: number;
  state: PeerState;
  lastProbeAt: Date;
}

interface TopologyInfo {
  myRole: NodeRole;
  hubNodeId: string | null;
  hubAddress: string | null;
  formedBy: FormationMethod;
  formedAt: Date;
  transportReady: boolean;
  nodes: NodeInfo[];
  probes: PeerProbe[];
}

interface DiscoveryLayer {
  name: string;
  type: 'broadcast' | 'cloud' | 'static' | 'manual';
  active: boolean;
  config: string;
  peerCount: number;
}

@Component({
  selector: 'app-network',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './network.component.html',
  styleUrls: ['./network.component.scss'],
})
export class NetworkComponent implements OnInit, OnDestroy {
  // Topology state
  topology: TopologyInfo = {
    myRole: 'hub',
    hubNodeId: 'node-a3f1-c8',
    hubAddress: '192.168.1.94:3000',
    formedBy: 'broadcast',
    formedAt: new Date(Date.now() - 3600000), // 1 hour ago
    transportReady: true,
    nodes: [
      {
        nodeId: 'node-a3f1-c8',
        hostname: 'NB-21624',
        addresses: ['192.168.1.94'],
        port: 3000,
        role: 'hub',
        startedAt: new Date(Date.now() - 7200000),
        isLocal: true,
      },
      {
        nodeId: 'node-b2e4-d9',
        hostname: 'NB-2510',
        addresses: ['192.168.1.37'],
        port: 3000,
        role: 'client',
        startedAt: new Date(Date.now() - 5400000),
      },
      {
        nodeId: 'node-c7f2-a1',
        hostname: 'NB-2744',
        addresses: ['192.168.1.249'],
        port: 3000,
        role: 'client',
        startedAt: new Date(Date.now() - 3600000),
      },
    ],
    probes: [
      {
        nodeId: 'node-a3f1-c8',
        hostname: 'NB-21624',
        address: '192.168.1.94:3000',
        latencyMs: 3,
        failCount: 0,
        state: 'reachable',
        lastProbeAt: new Date(Date.now() - 2000),
      },
      {
        nodeId: 'node-b2e4-d9',
        hostname: 'NB-2510',
        address: '192.168.1.37:3000',
        latencyMs: 5,
        failCount: 0,
        state: 'reachable',
        lastProbeAt: new Date(Date.now() - 2000),
      },
      {
        nodeId: 'node-c7f2-a1',
        hostname: 'NB-2744',
        address: '192.168.1.249:3000',
        latencyMs: 4,
        failCount: 0,
        state: 'reachable',
        lastProbeAt: new Date(Date.now() - 2000),
      },
    ],
  };

  // Discovery layers
  discoveryLayers: DiscoveryLayer[] = [
    {
      name: 'Broadcast',
      type: 'broadcast',
      active: true,
      config: 'UDP:41234',
      peerCount: 3,
    },
    {
      name: 'Cloud',
      type: 'cloud',
      active: false,
      config: 'Not configured',
      peerCount: 0,
    },
    {
      name: 'Static',
      type: 'static',
      active: false,
      config: 'Not configured',
      peerCount: 0,
    },
    {
      name: 'Manual',
      type: 'manual',
      active: true,
      config: 'No override',
      peerCount: 0,
    },
  ];

  // UI state
  selectedHubNodeId: string | null = null;
  excludeNodeId: string | null = null;
  excludeDuration: number = 300000; // 5 minutes default
  isConnected: boolean = true;
  excludedNodes: Set<string> = new Set();

  private destroy$ = new Subject<void>();

  ngOnInit(): void {
    // Simulate real-time probe updates
    interval(3000)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        this.updateProbes();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  // Actions
  assignHub(): void {
    if (!this.selectedHubNodeId) {
      alert('Please select a node to assign as hub');
      return;
    }

    const selectedNode = this.topology.nodes.find(
      (n) => n.nodeId === this.selectedHubNodeId,
    );
    if (!selectedNode) return;

    console.log('Assigning hub to:', this.selectedHubNodeId);

    // Update topology
    this.topology.nodes.forEach((node) => {
      node.role = node.nodeId === this.selectedHubNodeId ? 'hub' : 'client';
    });

    this.topology.hubNodeId = this.selectedHubNodeId;
    this.topology.hubAddress = `${selectedNode.addresses[0]}:${selectedNode.port}`;
    this.topology.formedBy = 'manual';
    this.topology.formedAt = new Date();
    this.topology.myRole =
      this.topology.nodes.find((n) => n.isLocal)?.role || 'client';

    alert(
      `Hub assigned to ${selectedNode.hostname} (${this.selectedHubNodeId})`,
    );
  }

  clearOverride(): void {
    if (!confirm('Clear hub assignment override? Network will re-elect hub.')) {
      return;
    }

    console.log('Clearing hub override');
    this.topology.formedBy = 'election';
    this.topology.formedAt = new Date();

    // Simulate re-election (pick node with lowest nodeId)
    const sortedNodes = [...this.topology.nodes].sort((a, b) =>
      a.nodeId.localeCompare(b.nodeId),
    );
    const newHub = sortedNodes[0];

    this.topology.nodes.forEach((node) => {
      node.role = node.nodeId === newHub.nodeId ? 'hub' : 'client';
    });

    this.topology.hubNodeId = newHub.nodeId;
    this.topology.hubAddress = `${newHub.addresses[0]}:${newHub.port}`;
    this.topology.myRole =
      this.topology.nodes.find((n) => n.isLocal)?.role || 'client';

    alert(`Hub override cleared. New hub: ${newHub.hostname}`);
  }

  excludeFromElection(): void {
    if (!this.excludeNodeId) {
      alert('Please select a node to exclude');
      return;
    }

    const node = this.topology.nodes.find(
      (n) => n.nodeId === this.excludeNodeId,
    );
    if (!node) return;

    console.log(
      'Excluding from election:',
      this.excludeNodeId,
      'for',
      this.excludeDuration,
      'ms',
    );

    this.excludedNodes.add(this.excludeNodeId);

    setTimeout(() => {
      this.excludedNodes.delete(this.excludeNodeId!);
      alert(`${node.hostname} is no longer excluded from election`);
    }, this.excludeDuration);

    const minutes = Math.floor(this.excludeDuration / 60000);
    alert(`${node.hostname} excluded from election for ${minutes} minute(s)`);
  }

  disconnect(): void {
    if (
      !confirm('Disconnect from network? This will stop all sync operations.')
    ) {
      return;
    }

    console.log('Disconnecting from network');
    this.isConnected = false;
    this.topology.transportReady = false;

    alert('Disconnected from network');
  }

  reconnect(): void {
    console.log('Reconnecting to network');
    this.isConnected = true;
    this.topology.transportReady = true;

    alert('Reconnected to network');
  }

  viewNodeDetails(node: NodeInfo): void {
    const details = `
Node Details:
- Node ID: ${node.nodeId}
- Hostname: ${node.hostname}
- IP: ${node.addresses.join(', ')}
- Port: ${node.port}
- Role: ${node.role}
- Started: ${formatDistanceToNow(node.startedAt, { addSuffix: true })}
- Local: ${node.isLocal ? 'Yes' : 'No'}
    `.trim();

    alert(details);
  }

  viewProbeDetails(probe: PeerProbe): void {
    const details = `
Probe Details:
- Hostname: ${probe.hostname}
- Address: ${probe.address}
- Latency: ${probe.latencyMs}ms
- Fail Count: ${probe.failCount}
- State: ${probe.state}
- Last Probe: ${formatDistanceToNow(probe.lastProbeAt, { addSuffix: true })}
    `.trim();

    alert(details);
  }

  // Helpers
  private updateProbes(): void {
    this.topology.probes.forEach((probe) => {
      // Simulate latency fluctuation
      probe.latencyMs = Math.max(
        1,
        probe.latencyMs + (Math.random() - 0.5) * 2,
      );
      probe.lastProbeAt = new Date();

      // Occasionally simulate failures
      if (Math.random() < 0.05) {
        probe.failCount++;
        probe.state = 'timeout';
      } else {
        probe.state = 'reachable';
      }
    });
  }

  getLocalNode(): NodeInfo | undefined {
    return this.topology.nodes.find((n) => n.isLocal);
  }

  getReachableCount(): number {
    return this.topology.probes.filter((p) => p.state === 'reachable').length;
  }

  getFormationMethodLabel(method: FormationMethod): string {
    const labels = {
      broadcast: 'Broadcast Discovery',
      cloud: 'Cloud Registry',
      static: 'Static Configuration',
      election: 'Automatic Election',
      manual: 'Manual Assignment',
    };
    return labels[method];
  }

  getRoleIcon(role: NodeRole): string {
    const icons = {
      hub: '🎯',
      client: '💻',
      unassigned: '❓',
    };
    return icons[role];
  }

  getRoleClass(role: NodeRole): string {
    return `role-${role}`;
  }

  getStateIcon(state: PeerState): string {
    const icons = {
      reachable: '✓',
      unreachable: '✗',
      timeout: '⏱',
    };
    return icons[state];
  }

  getStateClass(state: PeerState): string {
    return `state-${state}`;
  }

  shortenNodeId(nodeId: string): string {
    return nodeId.split('-').slice(-2).join('-');
  }

  formatDistanceToNow(date: Date): string {
    return formatDistanceToNow(date, { addSuffix: true });
  }

  isNodeExcluded(nodeId: string): boolean {
    return this.excludedNodes.has(nodeId);
  }

  getHubNode(): NodeInfo | undefined {
    if (!this.topology.hubNodeId) return undefined;
    return this.topology.nodes.find(
      (n) => n.nodeId === this.topology.hubNodeId,
    );
  }

  getHubHostname(): string {
    const hubNode = this.getHubNode();
    return hubNode ? hubNode.hostname : 'Unknown';
  }
}
