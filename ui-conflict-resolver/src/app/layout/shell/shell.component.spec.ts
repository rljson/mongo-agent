import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ShellComponent } from './shell.component';


describe('ShellComponent', () => {
  let component: ShellComponent;
  let fixture: ComponentFixture<ShellComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ShellComponent, RouterTestingModule],
    }).compileComponents();

    fixture = TestBed.createComponent(ShellComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have default node ID', () => {
    expect(component.nodeId).toBe('NodeA-abc123');
  });

  it('should have default node role', () => {
    expect(component.nodeRole).toBe('Hub');
  });

  it('should have navigation items', () => {
    expect(component.navItems.length).toBe(6);
  });

  it('should have dashboard navigation item', () => {
    const dashboardItem = component.navItems.find(
      (item) => item.path === '/dashboard',
    );
    expect(dashboardItem).toBeDefined();
    expect(dashboardItem?.label).toBe('Dashboard');
    expect(dashboardItem?.icon).toBe('📊');
  });

  it('should have network navigation item', () => {
    const networkItem = component.navItems.find(
      (item) => item.path === '/network',
    );
    expect(networkItem).toBeDefined();
    expect(networkItem?.label).toBe('Network');
  });

  it('should have sync navigation item', () => {
    const syncItem = component.navItems.find((item) => item.path === '/sync');
    expect(syncItem).toBeDefined();
    expect(syncItem?.label).toBe('Sync');
  });

  it('should have conflicts navigation item', () => {
    const conflictsItem = component.navItems.find(
      (item) => item.path === '/conflicts',
    );
    expect(conflictsItem).toBeDefined();
    expect(conflictsItem?.label).toBe('Conflicts');
  });

  it('should have logs navigation item', () => {
    const logsItem = component.navItems.find((item) => item.path === '/logs');
    expect(logsItem).toBeDefined();
    expect(logsItem?.label).toBe('Logs');
  });

  it('should have settings navigation item', () => {
    const settingsItem = component.navItems.find(
      (item) => item.path === '/settings',
    );
    expect(settingsItem).toBeDefined();
    expect(settingsItem?.label).toBe('Settings');
  });

  it('should have icons for all navigation items', () => {
    component.navItems.forEach((item) => {
      expect(item.icon).toBeDefined();
      expect(item.icon.length).toBeGreaterThan(0);
    });
  });
});
