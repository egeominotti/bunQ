/** @type {import('@stryker-mutator/core').PartialStrykerOptions} */
const config = {
  mutate: ['src/flow-plan.ts', 'src/flow-plan-legacy.ts', 'src/flow-commit.ts'],
  testRunner: 'command',
  commandRunner: {
    command:
      'bun test tests/flow-plan.property.test.ts tests/flow-identity.property.test.ts tests/flow-plan.validation.test.ts tests/flow-plan.limits.test.ts tests/flow-plan.contract.test.ts tests/flow-commit.test.ts',
  },
  coverageAnalysis: 'off',
  reporters: ['clear-text', 'progress', 'json'],
  jsonReporter: {
    fileName: 'reports/mutation/mutation.json',
  },
  thresholds: {
    high: 99,
    low: 98,
    break: 98,
  },
  concurrency: 2,
};

export default config;
