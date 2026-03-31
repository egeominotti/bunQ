import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import prettierConfig from 'eslint-config-prettier';

export default tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  ...tseslint.configs.strictTypeChecked,
  prettierConfig,
  {
    languageOptions: {
      parserOptions: {
        project: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      // ============ TypeScript Strict Rules (Enterprise) ============
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          caughtErrorsIgnorePattern: '^_',
        },
      ],
      '@typescript-eslint/explicit-function-return-type': 'off',
      '@typescript-eslint/explicit-module-boundary-types': 'off',
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/no-non-null-assertion': 'error',
      '@typescript-eslint/prefer-nullish-coalescing': 'error',
      '@typescript-eslint/prefer-optional-chain': 'error',
      '@typescript-eslint/strict-boolean-expressions': 'off',
      '@typescript-eslint/no-floating-promises': 'error',
      '@typescript-eslint/await-thenable': 'error',
      '@typescript-eslint/no-misused-promises': 'error',
      '@typescript-eslint/require-await': 'error',
      '@typescript-eslint/no-unnecessary-condition': 'error',
      '@typescript-eslint/restrict-template-expressions': 'off',
      '@typescript-eslint/no-confusing-void-expression': 'error',

      // ============ Type Safety (Enterprise) ============
      '@typescript-eslint/no-unsafe-assignment': 'error',
      '@typescript-eslint/no-unsafe-call': 'error',
      '@typescript-eslint/no-unsafe-member-access': 'error',
      '@typescript-eslint/no-unsafe-return': 'error',
      '@typescript-eslint/no-unsafe-argument': 'error',

      // ============ Import Consistency (Enterprise) ============
      '@typescript-eslint/consistent-type-imports': [
        'error',
        {
          prefer: 'type-imports',
          disallowTypeAnnotations: true,
          fixStyle: 'separate-type-imports',
        },
      ],
      '@typescript-eslint/consistent-type-exports': [
        'error',
        { fixMixedExportsWithInlineTypeSpecifier: true },
      ],
      '@typescript-eslint/no-import-type-side-effects': 'error',

      // ============ Code Quality (Enterprise) ============
      '@typescript-eslint/no-redundant-type-constituents': 'error',
      '@typescript-eslint/no-useless-empty-export': 'error',
      '@typescript-eslint/prefer-readonly': 'error',
      '@typescript-eslint/switch-exhaustiveness-check': 'error',
      '@typescript-eslint/no-mixed-enums': 'error',
      '@typescript-eslint/no-duplicate-enum-values': 'error',
      '@typescript-eslint/no-duplicate-type-constituents': 'error',

      // ============ General Rules (Enterprise) ============
      'no-console': 'off',
      'no-debugger': 'error',
      'no-duplicate-imports': 'off', // Handled by @typescript-eslint/consistent-type-imports
      'no-unused-expressions': 'error',
      'prefer-const': 'error',
      'no-var': 'error',
      eqeqeq: ['error', 'always'],
      curly: ['error', 'multi-line'],

      // ============ Complexity Limits (Enterprise) ============
      complexity: ['error', { max: 20 }],
      'max-depth': ['error', { max: 4 }],
      'max-nested-callbacks': ['error', { max: 3 }],
      'max-params': ['error', { max: 5 }],
    },
  },
  // ============ Benchmark Files ============
  {
    files: ['src/benchmark/**/*.ts'],
    rules: {
      '@typescript-eslint/no-non-null-assertion': 'off',
      '@typescript-eslint/no-unnecessary-condition': 'off',
      '@typescript-eslint/no-unsafe-assignment': 'off',
      '@typescript-eslint/no-unsafe-member-access': 'off',
      '@typescript-eslint/no-unsafe-call': 'off',
      '@typescript-eslint/no-unsafe-return': 'off',
      '@typescript-eslint/no-unsafe-argument': 'off',
      'max-params': 'off',
    },
  },
  // ============ CLI Files ============
  {
    files: ['src/cli/**/*.ts'],
    rules: {
      'max-params': ['error', { max: 6 }],
      complexity: ['error', { max: 35 }], // CLI formatters are inherently complex
      '@typescript-eslint/no-unnecessary-condition': 'off',
    },
  },
  // ============ Infrastructure Persistence (SQLite, prepared statements) ============
  {
    files: ['src/infrastructure/persistence/**/*.ts'],
    rules: {
      // Non-null assertions are safe after Map.get() with prior .has() check
      '@typescript-eslint/no-non-null-assertion': 'off',
    },
  },
  // ============ Infrastructure Server (HTTP, TCP, Protocol) ============
  {
    files: ['src/infrastructure/server/**/*.ts'],
    rules: {
      // Server handlers have inherent complexity from request routing
      complexity: ['error', { max: 45 }],
      '@typescript-eslint/no-unnecessary-condition': 'off',
    },
  },
  // ============ Infrastructure Scheduler ============
  {
    files: ['src/infrastructure/scheduler/**/*.ts'],
    rules: {
      '@typescript-eslint/no-non-null-assertion': 'off',
    },
  },
  // ============ Domain Queue (Priority Queue, Shard) ============
  {
    files: ['src/domain/queue/**/*.ts'],
    rules: {
      // Priority queue uses non-null assertions after index checks
      '@typescript-eslint/no-non-null-assertion': 'off',
      '@typescript-eslint/no-unnecessary-condition': 'off',
    },
  },
  // ============ Application Layer ============
  {
    files: ['src/application/**/*.ts'],
    rules: {
      // QueueManager has complex job scheduling logic
      'max-depth': ['error', { max: 5 }],
      '@typescript-eslint/no-unnecessary-condition': 'off',
      '@typescript-eslint/require-await': 'off', // Some async methods may not await
    },
  },
  // ============ Test Files ============
  {
    files: ['test/**/*.ts'],
    languageOptions: {
      parserOptions: {
        project: './tsconfig.test.json',
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      '@typescript-eslint/await-thenable': 'off',
      '@typescript-eslint/no-confusing-void-expression': 'off',
      '@typescript-eslint/no-non-null-assertion': 'off',
      '@typescript-eslint/require-await': 'off',
      '@typescript-eslint/no-unsafe-assignment': 'off',
      '@typescript-eslint/no-unsafe-member-access': 'off',
      '@typescript-eslint/no-unsafe-call': 'off',
      '@typescript-eslint/no-unsafe-return': 'off',
      '@typescript-eslint/no-unsafe-argument': 'off',
      'max-params': 'off',
      complexity: 'off',
    },
  },
  {
    ignores: ['dist/**', 'node_modules/**', 'src/mcp/**', '*.js', '*.cjs', '*.mjs'],
  }
);
