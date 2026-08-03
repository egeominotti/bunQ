import { afterAll, describe, expect, test as bunTest } from 'bun:test';
import {
  printSummary,
  queueManager,
  test as recordFeature,
} from '../src/benchmark/full-features/harness';
import { retryWaitMs } from '../src/benchmark/full-features/core';

afterAll(() => queueManager.shutdown());

describe('full-features runner failure reporting', () => {
  bunTest('retry waits are derived from the actual jittered deadline', () => {
    expect(retryWaitMs(1300, 1000)).toBe(325);
    expect(retryWaitMs(900, 1000)).toBe(25);
  });

  bunTest('a failed feature makes the summary unsuccessful', async () => {
    await recordFeature('intentional regression fixture', () => false);
    expect(printSummary()).toBe(false);
  });
});
