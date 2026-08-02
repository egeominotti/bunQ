import { afterEach, describe, expect, test } from 'bun:test';
import { CoreE2eHarness, type CoreE2eMode } from './core-e2e/support/harness';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await harness?.close();
  harness = null;
});

describe.each([
  'embedded',
  'tcp',
] as const)('job scheduler upsert result parity (%s)', (mode: CoreE2eMode) => {
  test('returns the authoritative interval nextRun from the scheduler', async () => {
    harness = await CoreE2eHarness.start(mode, 'scheduler-upsert-interval');
    const queue = harness.queue('scheduler-upsert-interval');
    const schedulerId = harness.unique('interval');

    const added = await queue.upsertJobScheduler(schedulerId, { every: 3_600_000 });
    const fetched = await queue.getJobScheduler(schedulerId);

    expect(added?.every).toBe(3_600_000);
    expect(added?.next).toBe(fetched?.next);
  });

  test('returns the authoritative pattern nextRun from the scheduler', async () => {
    harness = await CoreE2eHarness.start(mode, 'scheduler-upsert-pattern');
    const queue = harness.queue('scheduler-upsert-pattern');
    const schedulerId = harness.unique('pattern');

    const added = await queue.upsertJobScheduler(schedulerId, {
      pattern: '0 * * * *',
      timezone: 'UTC',
    });
    const fetched = await queue.getJobScheduler(schedulerId);

    expect(added?.pattern).toBe('0 * * * *');
    expect(added?.next).toBe(fetched?.next);
  });
});
