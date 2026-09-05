import jsdoc from 'eslint-plugin-jsdoc';
import tsdoc from 'eslint-plugin-tsdoc';
import tseslint from 'typescript-eslint';

/** @type {import('eslint').Linter.Config[]} */
export default [
  // Ignore all JS files and the coverage folder. Also scope linting to the
  // library core: exclude the coverage-excluded utility scripts, the standalone
  // e2e / benchmark runners under test/e2e (not vitest unit specs), the separate
  // Angular UI sub-package (own lint config), and temporary probe files.
  {
    ignores: [
      '**/*.js',
      '**/*.cjs',
      '_*.ts',
      '_*.mts',
      'coverage/',
      'dist/',
      'node_modules',
      '.git',
      'src/scripts/',
      'test/e2e/',
      'ui-conflict-resolver/',
    ],
  },

  // Configure eslint for implementation files
  ...tseslint.configs.recommended,
  {
    rules: {
      // Typescript rules
      '@typescript-eslint/no-explicit-any': 'off',
    },
  },

  // Configure tsdoc
  {
    files: ['src/**/*.ts'],
    plugins: { tsdoc, jsdoc, tseslint },
    rules: {
      'tsdoc/syntax': 'error',
      ...jsdoc.configs['flat/recommended-typescript-flavor-error'].rules,
      'jsdoc/require-description': 'error',
      'jsdoc/require-param-type': 'off',
      'jsdoc/require-jsdoc': [
        'off',
        {
          require: {
            FunctionDeclaration: true,
            FunctionExpression: true,
            MethodDefinition: true,
            ClassDeclaration: true,
            ClassExpression: true,
            ArrowFunctionExpression: true,
          },
          contexts: [
            'TSInterfaceDeclaration',
            'TSTypeAliasDeclaration',
            'TSEnumDeclaration',
            'TSPropertySignature',
          ],
          publicOnly: true,
        },
      ],
      'jsdoc/require-returns-type': 'off',
      'jsdoc/require-returns': 'off',
    },
  },

  // Configure eslint for test files
  {
    files: ['**/*.spec.ts'],
    rules: {
      '@typescript-eslint/no-explicit-any': 'off',
      jsdoc: 'off',
    },
  },
];
