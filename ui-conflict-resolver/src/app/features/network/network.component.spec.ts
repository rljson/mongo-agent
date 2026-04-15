import {
  ComponentFixture, discardPeriodicTasks, fakeAsync, flush, TestBed, tick
} from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { NetworkComponent } from './network.component';


describe('NetworkComponent', () => {
  let component: NetworkComponent;
  let fixture: ComponentFixture<NetworkComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [NetworkComponent, FormsModule],
    }).compileComponents();

    fixture = TestBed.createComponent(NetworkComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    if (component) {
      component.ngOnDestroy();
    }
    fixture.destroy();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize with default topology', () => {
    expect(component.topology.myRole).toBe('hub');
    expect(component.topology.nodes.length).toBeGreaterThan(0);
  });

  it('should update probes periodically', fakeAsync(() => {
    fixture.detectChanges();
    const initialProbe = component.topology.probes[0];
    const initialTimestamp = initialProbe.lastProbeAt;

    tick(3000);

    const updatedProbe = component.topology.probes.find(
      (p) => p.nodeId === initialProbe.nodeId,
    );
    expect(updatedProbe?.lastProbeAt).not.toEqual(initialTimestamp);

    component.ngOnDestroy();
    flush();
  }));

  it('should format uptime using formatDistanceToNow', () => {
    const date = new Date(Date.now() - 5000);
    const result = component.formatDistanceToNow(date);
    expect(result).toBeDefined();
    expect(typeof result).toBe('string');
  });

  it('should format dates correctly', () => {
    const date = new Date();
    const formatted = component.formatDistanceToNow(date);
    expect(formatted).toContain('ago');
  });

  it('should return role class', () => {
    expect(component.getRoleClass('hub')).toBe('role-hub');
    expect(component.getRoleClass('client')).toBe('role-client');
    expect(component.getRoleClass('unassigned')).toBe('role-unassigned');
  });

  it('should return state class', () => {
    expect(component.getStateClass('reachable')).toBe('state-reachable');
    expect(component.getStateClass('unreachable')).toBe('state-unreachable');
    expect(component.getStateClass('timeout')).toBe('state-timeout');
  });

  it('should return role icon', () => {
    expect(component.getRoleIcon('hub')).toBe('🎯');
    expect(component.getRoleIcon('client')).toBe('💻');
    expect(component.getRoleIcon('unassigned')).toBe('❓');
  });

  it('should return state icon', () => {
    expect(component.getStateIcon('reachable')).toBe('✓');
    expect(component.getStateIcon('unreachable')).toBe('✗');
    expect(component.getStateIcon('timeout')).toBe('⏱');
  });

  it('should shorten node ID', () => {
    const nodeId = 'node-a3f1-c8';
    const shortened = component.shortenNodeId(nodeId);
    expect(shortened).toBe('a3f1-c8');
  });

  it('should check if node is excluded', () => {
    expect(component.isNodeExcluded('some-node')).toBe(false);
  });

  it('should get hub node', () => {
    const hubNode = component.getHubNode();
    expect(hubNode).toBeDefined();
    expect(hubNode?.role).toBe('hub');
  });

  it('should get hub hostname', () => {
    const hostname = component.getHubHostname();
    expect(hostname).toBeDefined();
    expect(typeof hostname).toBe('string');
  });

  it('should identify local node', () => {
    const localNode = component.topology.nodes.find((n) => n.isLocal);
    expect(localNode).toBeDefined();
  });

  it('should calculate total nodes', () => {
    const count = component.topology.nodes.length;
    expect(count).toBeGreaterThan(0);
  });

  it('should calculate reachable nodes', () => {
    const reachable = component.topology.probes.filter(
      (p) => p.state === 'reachable',
    );
    expect(reachable.length).toBeGreaterThanOrEqual(0);
  });

  it('should unsubscribe on destroy', fakeAsync(() => {
    fixture.detectChanges();

    spyOn(component['destroy$'], 'next');
    spyOn(component['destroy$'], 'complete');

    component.ngOnDestroy();

    expect(component['destroy$'].next).toHaveBeenCalled();
    expect(component['destroy$'].complete).toHaveBeenCalled();

    discardPeriodicTasks();
  }));

  it('should get local node', () => {
    const localNode = component.getLocalNode();
    expect(localNode).toBeDefined();
    expect(localNode?.isLocal).toBe(true);
  });

  it('should get reachable count', () => {
    const count = component.getReachableCount();
    expect(typeof count).toBe('number');
    expect(count).toBeGreaterThanOrEqual(0);
  });

  it('should get formation method label', () => {
    const label = component.getFormationMethodLabel('broadcast');
    expect(label).toBeDefined();
    expect(typeof label).toBe('string');
  });
});
