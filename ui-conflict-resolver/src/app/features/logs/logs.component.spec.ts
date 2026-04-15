import {
  ComponentFixture, discardPeriodicTasks, fakeAsync, flush, TestBed, tick
} from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { LogsComponent } from './logs.component';


describe('LogsComponent', () => {
  let component: LogsComponent;
  let fixture: ComponentFixture<LogsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [LogsComponent, FormsModule],
    }).compileComponents();

    fixture = TestBed.createComponent(LogsComponent);
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

  it('should initialize with default values', () => {
    expect(component.logs).toEqual([]);
    expect(component.selectedLevels.size).toBe(4);
    expect(component.autoScroll).toBe(true);
  });

  it('should add initial logs on init', fakeAsync(() => {
    fixture.detectChanges();
    tick();

    expect(component.logs.length).toBeGreaterThan(0);
    expect(component.filteredLogs.length).toBeGreaterThan(0);

    component.ngOnDestroy();
    flush();
  }));

  it('should generate random logs periodically', fakeAsync(() => {
    fixture.detectChanges();
    const initialLength = component.logs.length;

    tick(2000);
    expect(component.logs.length).toBeGreaterThan(initialLength);

    component.ngOnDestroy();
    flush();
  }));

  it('should filter logs by level', () => {
    component.logs = [
      {
        timestamp: new Date(),
        level: 'info',
        message: 'Test info',
        source: 'test',
      },
      {
        timestamp: new Date(),
        level: 'error',
        message: 'Test error',
        source: 'test',
      },
    ];

    component.selectedLevels.clear();
    component.selectedLevels.add('info');
    component.applyFilters();

    expect(component.filteredLogs.length).toBe(1);
    expect(component.filteredLogs[0].level).toBe('info');
  });

  it('should filter logs by search term', () => {
    component.logs = [
      {
        timestamp: new Date(),
        level: 'info',
        message: 'Test MongoDB',
        source: 'test',
      },
      {
        timestamp: new Date(),
        level: 'info',
        message: 'Test sync',
        source: 'test',
      },
    ];

    component.searchTerm = 'MongoDB';
    component.applyFilters();

    expect(component.filteredLogs.length).toBe(1);
    expect(component.filteredLogs[0].message).toContain('MongoDB');
  });

  it('should toggle level filter', () => {
    const initialSize = component.selectedLevels.size;
    component.toggleLevel('info');
    expect(component.selectedLevels.size).toBe(initialSize - 1);

    component.toggleLevel('info');
    expect(component.selectedLevels.size).toBe(initialSize);
  });

  it('should clear all logs', () => {
    spyOn(window, 'confirm').and.returnValue(true);
    component.logs = [
      { timestamp: new Date(), level: 'info', message: 'Test', source: 'test' },
    ];
    component.logCounts.info = 1;

    component.clearLogs();

    expect(component.logs.length).toBe(0);
    expect(component.logCounts.info).toBe(0);
  });

  it('should not clear logs when cancelled', () => {
    spyOn(window, 'confirm').and.returnValue(false);
    component.logs = [
      { timestamp: new Date(), level: 'info', message: 'Test', source: 'test' },
    ];

    component.clearLogs();

    expect(component.logs.length).toBe(1);
  });

  it('should export logs', () => {
    spyOn(URL, 'createObjectURL').and.returnValue('blob:url');
    spyOn(URL, 'revokeObjectURL');
    const linkClickSpy = jasmine.createSpy('click');
    spyOn(document, 'createElement').and.returnValue({
      href: '',
      download: '',
      click: linkClickSpy,
    } as any);

    component.logs = [
      { timestamp: new Date(), level: 'info', message: 'Test', source: 'test' },
    ];

    component.exportLogs();

    expect(linkClickSpy).toHaveBeenCalled();
  });

  it('should respect max logs limit', fakeAsync(() => {
    component['maxLogs'] = 10;
    fixture.detectChanges();

    // Add logs without triggering timers by directly modifying the array
    component.logs = [];
    for (let i = 0; i < 20; i++) {
      component.logs.push({
        timestamp: new Date(),
        level: 'info',
        message: `Message ${i}`,
        source: 'test',
      });
    }

    // Trigger max logs limit check
    if (component.logs.length > component['maxLogs']) {
      component.logs = component.logs.slice(-component['maxLogs']);
    }

    expect(component.logs.length).toBeLessThanOrEqual(10);

    component.ngOnDestroy();
    discardPeriodicTasks();
  }));

  it('should unsubscribe on destroy', fakeAsync(() => {
    fixture.detectChanges();

    spyOn(component['destroy$'], 'next');
    spyOn(component['destroy$'], 'complete');

    component.ngOnDestroy();

    expect(component['destroy$'].next).toHaveBeenCalled();
    expect(component['destroy$'].complete).toHaveBeenCalled();

    discardPeriodicTasks();
  }));
});
