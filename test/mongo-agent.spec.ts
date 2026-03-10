// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';

import { MongoAgent } from '../src/mongo-agent';


describe('MongoAgent', () => {
  it('should validate a template', () => {
    const mongoAgent = MongoAgent.example;
    expect(mongoAgent).toBeDefined();
  });
});
