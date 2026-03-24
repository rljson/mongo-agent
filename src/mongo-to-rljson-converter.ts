// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh } from '@rljson/hash';
import type { Collection, Document } from 'mongodb';

import type { ColumnCfg, ComponentsTable, TableCfg } from '@rljson/rljson';

/**
 * MongoDB to RLJSON Component Converter
 * Converts MongoDB collections to RLJSON ComponentsTable format
 */
export class MongoToRljsonConverter {
  /**
   * Discovers schema from MongoDB collection by sampling documents
   * @param collection - MongoDB collection
   * @param sampleSize - Number of documents to sample (default: 100)
   * @returns TableCfg with discovered schema
   */
  async discoverSchema(
    collection: Collection,
    sampleSize = 100,
  ): Promise<TableCfg> {
    // Sample documents to infer schema
    const docs = await collection.find().limit(sampleSize).toArray();

    if (docs.length === 0) {
      // Empty collection - create minimal schema
      return hip<TableCfg>({
        key: collection.collectionName,
        type: 'components',
        columns: [
          {
            key: '_hash',
            type: 'string',
            titleLong: 'Hash',
            titleShort: 'Hash',
          },
        ],
        isHead: false,
        isRoot: false,
        isShared: true,
        _hash: '',
      });
    }

    // Collect all unique keys and their types
    const fieldTypes = new Map<string, Set<string>>();

    for (const doc of docs) {
      this._collectFieldTypes(doc, fieldTypes);
    }

    // Convert to column definitions
    const columns: ColumnCfg[] = [
      {
        key: '_hash',
        type: 'string',
        titleLong: 'Hash',
        titleShort: 'Hash',
      },
    ];

    // Sort keys for consistent schema
    const sortedKeys = Array.from(fieldTypes.keys()).sort();

    for (const key of sortedKeys) {
      if (key === '_hash') continue; // Already added

      const types = fieldTypes.get(key)!;
      const columnType = this._inferColumnType(types);

      columns.push({
        key,
        type: columnType as any, // Type assertion for RLJSON JsonValueType
        titleLong: this._formatTitle(key),
        titleShort: this._formatTitleShort(key),
      });
    }

    // Create and hash TableCfg
    const tableCfg = hip<TableCfg>({
      key: collection.collectionName,
      type: 'components',
      columns,
      isHead: false,
      isRoot: false,
      isShared: true,
      _hash: '',
    });

    return tableCfg;
  }

  /**
   * Converts MongoDB collection to RLJSON ComponentsTable
   * @param collection - MongoDB collection
   * @param tableCfg - Table configuration
   * @param limit - Maximum number of documents to convert (optional)
   * @returns ComponentsTable with all documents
   */
  async convertCollection(
    collection: Collection,
    tableCfg: TableCfg,
    limit?: number,
  ): Promise<ComponentsTable<any>> {
    const query = limit ? collection.find().limit(limit) : collection.find();
    const docs = await query.toArray();

    const data = docs.map((doc) => this.convertDocument(doc, tableCfg));

    const componentsTable = hip<ComponentsTable<any>>({
      _tableCfg: tableCfg._hash as string,
      _type: 'components',
      _data: data,
      _hash: '',
    });

    return componentsTable;
  }

  /**
   * Converts single MongoDB document to RLJSON component row
   * @param doc - MongoDB document
   * @param tableCfg - Table configuration
   * @returns Hashed row object
   */
  convertDocument(doc: Document, tableCfg: TableCfg): any {
    const row: any = { _hash: '' };

    // Convert each field according to schema
    for (const column of tableCfg.columns) {
      if (column.key === '_hash') continue;

      const value = doc[column.key];
      row[column.key] = this._convertValue(value, column.type);
    }

    // Hash the row
    return hsh(row);
  }

  /**
   * Collects field types from a document (recursive for nested objects)
   * @param doc - Document to analyze
   * @param fieldTypes - Map to store field types
   * @param prefix - Field name prefix for nested objects
   */
  private _collectFieldTypes(
    doc: any,
    fieldTypes: Map<string, Set<string>>,
    prefix = '',
  ): void {
    for (const key in doc) {
      if (!Object.prototype.hasOwnProperty.call(doc, key)) continue;

      const fullKey = prefix ? `${prefix}.${key}` : key;
      const value = doc[key];
      const valueType = this._getValueType(value);

      if (!fieldTypes.has(fullKey)) {
        fieldTypes.set(fullKey, new Set());
      }
      fieldTypes.get(fullKey)!.add(valueType);

      // For nested objects, recurse (but not for arrays)
      if (valueType === 'object' && value !== null && !Array.isArray(value)) {
        this._collectFieldTypes(value, fieldTypes, fullKey);
      }
    }
  }

  /**
   * Gets the type of a value
   * @param value - Value to check
   * @returns Type string
   */
  private _getValueType(value: any): string {
    if (value === null || value === undefined) return 'null';
    if (Array.isArray(value)) return 'array';
    if (value instanceof Date) return 'date';
    if (typeof value === 'object' && value._bsontype === 'ObjectId') {
      return 'objectid';
    }
    if (typeof value === 'object') return 'object';
    if (typeof value === 'number') {
      return Number.isInteger(value) ? 'number' : 'number';
    }
    if (typeof value === 'boolean') return 'boolean';
    if (typeof value === 'string') return 'string';
    return 'unknown';
  }

  /**
   * Infers RLJSON column type from MongoDB value types
   * @param types - Set of obser (JsonValueType)
   */
  private _inferColumnType(types: Set<string>): string {
    // Remove null from consideration
    const nonNullTypes = new Set(Array.from(types).filter((t) => t !== 'null'));

    if (nonNullTypes.size === 0) return 'string'; // All null, default to string
    if (nonNullTypes.size === 1) {
      const type = Array.from(nonNullTypes)[0];
      switch (type) {
        case 'string':
          return 'string';
        case 'number':
          return 'number';
        case 'boolean':
          return 'boolean';
        case 'date':
          return 'number'; // Store as timestamp
        case 'objectid':
          return 'string'; // Convert ObjectId to string
        case 'array':
          return 'jsonArray'; // Arrays stored as jsonArray
        case 'object':
          return 'json'; // Nested objects stored as json
          return 'json'; // Nested objects stored as JSON
        default:
          return 'string';
      }
    }

    // Mixed types - use json
    return 'json';
  }

  /**
   * Converts a value to the appropriate RLJSON type
   * @param value - Value to convert
   * @param columnType - Target column type
   * @returns Converted value
   */
  private _convertValue(value: any, columnType: string): any {
    if (value === null || value === undefined) return null;

    switch (columnType) {
      case 'string':
        if (typeof value === 'object' && value._bsontype === 'ObjectId') {
          return value.toString();
        }
        return String(value);

      case 'number':
        if (value instanceof Date) {
          return value.getTime();
        }
        return Number(value);

      case 'boolean':
        return Boolean(value);

      case 'json':
        if (typeof value === 'object' && value._bsontype === 'ObjectId') {
          return value.toString();
        }
        if (value instanceof Date) {
          return value.toISOString();
        }
        return value;

      default:
        return value;
    }
  }

  /**
   * Formats a field key into a human-readable title
   * @param key - Field key
   * @returns Formatted title
   */
  private _formatTitle(key: string): string {
    // Split on dots and underscores, capitalize each word
    return key
      .split(/[._]/)
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ');
  }

  /**
   * Formats a field key into a short title
   * @param key - Field key
   * @returns Short title
   */
  private _formatTitleShort(key: string): string {
    // Use last part after dot/underscore for short title
    const parts = key.split(/[._]/);
    const lastPart = parts[parts.length - 1];
    return lastPart.charAt(0).toUpperCase() + lastPart.slice(1);
  }
}
