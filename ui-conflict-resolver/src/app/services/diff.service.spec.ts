import { TestBed } from '@angular/core/testing';

import { DiffResult, FieldConflict } from '../models/conflict.types';

import { DiffService } from './diff.service';


describe('DiffService', () => {
  let service: DiffService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(DiffService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should calculate diff with added fields', () => {
    const local = { name: 'Alice', age: 30 };
    const remote = { name: 'Alice', age: 30, email: 'alice@example.com' };

    const diff: DiffResult = service.calculateDiff(local, remote);

    expect(diff.added).toContain('email');
    expect(diff.removed.length).toBe(0);
    expect(diff.modified.length).toBe(0);
  });

  it('should calculate diff with removed fields', () => {
    const local = { name: 'Alice', age: 30, email: 'alice@example.com' };
    const remote = { name: 'Alice', age: 30 };

    const diff: DiffResult = service.calculateDiff(local, remote);

    expect(diff.removed).toContain('email');
    expect(diff.added.length).toBe(0);
    expect(diff.modified.length).toBe(0);
  });

  it('should calculate diff with modified fields', () => {
    const local = { name: 'Alice', age: 30 };
    const remote = { name: 'Alice', age: 31 };

    const diff: DiffResult = service.calculateDiff(local, remote);

    expect(diff.modified.length).toBe(1);
    expect(diff.modified[0].field).toBe('age');
    expect(diff.modified[0].oldValue).toBe(30);
    expect(diff.modified[0].newValue).toBe(31);
  });

  it('should get field conflicts', () => {
    const local = { name: 'Alice', age: 30, status: 'active' };
    const remote = { name: 'Alice Smith', age: 30, status: 'inactive' };

    const conflicts: FieldConflict[] = service.getFieldConflicts(local, remote);

    expect(conflicts.length).toBe(2); // name and status differ
    expect(conflicts.find((c) => c.fieldPath === 'name')).toBeDefined();
    expect(conflicts.find((c) => c.fieldPath === 'status')).toBeDefined();
  });

  it('should merge fields correctly', () => {
    const base = { name: 'Alice', age: 30, status: 'active' };
    const fieldConflicts: FieldConflict[] = [
      {
        fieldPath: 'name',
        localValue: 'Alice',
        remoteValue: 'Alice Smith',
        selectedValue: 'Alice Smith',
      },
      {
        fieldPath: 'status',
        localValue: 'active',
        remoteValue: 'inactive',
        selectedValue: 'active',
      },
    ];

    const merged = service.mergeFields(base, fieldConflicts);

    expect(merged.name).toBe('Alice Smith');
    expect(merged.age).toBe(30);
    expect(merged.status).toBe('active');
  });

  it('should format values correctly', () => {
    expect(service.formatValue(null)).toBe('null');
    expect(service.formatValue(undefined)).toBe('undefined');
    expect(service.formatValue('test')).toBe('test');
    expect(service.formatValue(42)).toBe('42');
    expect(service.formatValue({ a: 1 })).toContain('"a": 1');
  });

  it('should handle nested objects in diff', () => {
    const local = { user: { name: 'Alice', age: 30 } };
    const remote = { user: { name: 'Alice', age: 31 } };

    const diff: DiffResult = service.calculateDiff(local, remote);

    expect(diff.modified.length).toBe(1);
    expect(diff.modified[0].field).toBe('user');
  });

  it('should handle arrays in diff', () => {
    const local = { tags: ['javascript', 'typescript'] };
    const remote = { tags: ['javascript', 'typescript', 'angular'] };

    const diff: DiffResult = service.calculateDiff(local, remote);

    expect(diff.modified.length).toBe(1);
    expect(diff.modified[0].field).toBe('tags');
  });

  it('should handle identical objects', () => {
    const local = { name: 'Alice', age: 30 };
    const remote = { name: 'Alice', age: 30 };

    const diff: DiffResult = service.calculateDiff(local, remote);

    expect(diff.added.length).toBe(0);
    expect(diff.removed.length).toBe(0);
    expect(diff.modified.length).toBe(0);
  });

  it('should handle empty objects', () => {
    const local = {};
    const remote = {};

    const diff: DiffResult = service.calculateDiff(local, remote);

    expect(diff.added.length).toBe(0);
    expect(diff.removed.length).toBe(0);
    expect(diff.modified.length).toBe(0);
  });

  it('should filter out _id and metadata fields from conflicts', () => {
    const local = { _id: 'doc-1', name: 'Alice', age: 30 };
    const remote = { _id: 'doc-1', name: 'Alice Smith', age: 30 };

    const conflicts: FieldConflict[] = service.getFieldConflicts(local, remote);

    expect(conflicts.find((c) => c.fieldPath === '_id')).toBeUndefined();
    expect(conflicts.find((c) => c.fieldPath === 'name')).toBeDefined();
  });

  it('should handle boolean values in formatValue', () => {
    expect(service.formatValue(true)).toBe('true');
    expect(service.formatValue(false)).toBe('false');
  });

  it('should handle arrays in formatValue', () => {
    const result = service.formatValue([1, 2, 3]);
    expect(result).toContain('[');
    expect(result).toContain('1');
  });

  it('should generate text diff', () => {
    const oldText = 'Hello World';
    const newText = 'Hello Angular';

    const diff = service.generateTextDiff(oldText, newText);

    expect(diff).toBeDefined();
    expect(diff.length).toBeGreaterThan(0);
  });

  it('should merge fields without selected values', () => {
    const base = { name: 'Alice', age: 30 };
    const fieldConflicts: FieldConflict[] = [
      {
        fieldPath: 'name',
        localValue: 'Alice',
        remoteValue: 'Alice Smith',
        // No selectedValue - should use localValue
      },
    ];

    const merged = service.mergeFields(base, fieldConflicts);

    expect(merged.name).toBe('Alice');
  });
});
