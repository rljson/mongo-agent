// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/// <reference types="vitest" />

import { defineConfig } from 'vite';


// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  return {
    test: {
      globals: true,
      environment: 'node',
      setupFiles: ['./test/setup/test-setup.ts'],
      // `*.test.ts` for the version test `gg one do publish` writes: left
      // out, it never runs, its source file stays uncovered and the 100 %
      // gate fails in the release pull request.
      include: ['**/test/**/*.spec.ts', '**/test/**/*.test.ts'],

      reporters: ['default'],
      coverage: {
        enabled: true,
        provider: 'v8', // "istanbul" or "v8"
        reporter: ['text', 'json', 'html'],
        include: ['src/**/*.ts'],
        exclude: [
          'src/index.ts',
          'src/scripts/**/*.ts',
          'src/hub/**/*.ts',
          'src/agent-server.ts',
        ],
        all: true,
        thresholds: {
          statements: 100,
          branches: 100,
          functions: 100,
          lines: 100,
        },
        checkCoverage: true,
      },
    },
    define: {
      'import.meta.vitest': mode !== 'production',
    },
  };
});
