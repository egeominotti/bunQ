/**
 * Regression guard for the v2.8.60 Linux CI failure.
 *
 * Bun runs test files in one process, so an embedded suite that requests a
 * persistent dataPath must reset the process-wide QueueManager at both file
 * boundaries. Otherwise the first test can inherit an in-memory manager from
 * an unrelated file and fail before its own cleanup hook gets a chance to run.
 */
import { describe, expect, test } from 'bun:test';
import { join } from 'node:path';

const PERSISTENT_EMBEDDED_SUITES = [
  'repro-model-child-recovered-alone.test.ts',
  'repro-workflow-abandon-vanished.test.ts',
  'repro-issue74-movetofailed-stacktrace.test.ts',
  'worker-api-enhancements.test.ts',
] as const;

describe('persistent embedded test-suite isolation', () => {
  for (const file of PERSISTENT_EMBEDDED_SUITES) {
    test(`${file} resets the shared manager at both suite boundaries`, async () => {
      const source = await Bun.file(join(import.meta.dir, file)).text();
      const entryReset = source.indexOf('beforeAll(shutdownManager);');
      const teardownReset = source.indexOf('shutdownManager();', entryReset + 1);

      expect(
        entryReset,
        `${file} must reset a manager leaked by an earlier test file before opening its dataPath`
      ).toBeGreaterThanOrEqual(0);
      expect(
        teardownReset,
        `${file} must also release its own manager so a later test file can select another dataPath`
      ).toBeGreaterThan(entryReset);
    });
  }
});
