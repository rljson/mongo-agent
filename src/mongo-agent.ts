// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Bs, BsMem } from '@rljson/bs';

import { MongoScanner, type MongoTree } from './mongo-scanner.ts';

import type { Db as MongoDb } from 'mongodb';

export interface MongoAgentOptions {
  ignore?: string[];
  include?: string[];
}

// .............................................................................
export class MongoAgent {
  private _scanner: MongoScanner | null = null;
  private _bs: Bs;

  constructor(db?: MongoDb, bs?: Bs, options: MongoAgentOptions = {}) {
    this._bs = bs ?? new BsMem();
    if (db) {
      this._scanner = new MongoScanner(db, {
        ignore: options.ignore,
        include: options.include,
        bs: this._bs,
      });
    }
  }

  /** Example instance for test purposes */
  static get example(): MongoAgent {
    return new MongoAgent();
  }

  get bs(): Bs {
    return this._bs;
  }

  async extract(): Promise<MongoTree> {
    if (!this._scanner) {
      throw new Error('MongoAgent: no MongoDB database provided to constructor');
    }
    return this._scanner.scan();
  }
}
