import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure } from '../support/tracker';

export async function runQueueCronContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'queue-cron');
  const tracker = new CoverageTracker(mode, 'queue-cron-contract');

  try {
    const queue = harness.queue<{ value: number }>('cron');
    const intervalId = harness.unique('interval');
    const patternId = harness.unique('pattern');
    const beforeInterval = Date.now();
    const interval = await tracker.invoke('Queue', 'upsertJobScheduler', () =>
      queue.upsertJobScheduler(
        intervalId,
        { every: 60_000, limit: 2 },
        { name: 'interval-task', data: { value: 1 }, opts: { priority: 4 } }
      )
    );
    ensure(interval?.id === intervalId && interval.every === 60_000, 'interval scheduler mismatch');
    ensure(
      interval.next >= beforeInterval + 59_000,
      'interval scheduler returned an invalid next run'
    );
    const pattern = await queue.upsertJobScheduler(
      patternId,
      { pattern: '0 * * * *', timezone: 'UTC' },
      { name: 'pattern-task', data: { value: 2 } }
    );
    ensure(pattern?.id === patternId && pattern.pattern === '0 * * * *', 'pattern mismatch');

    const fetched = await tracker.invoke('Queue', 'getJobScheduler', () =>
      queue.getJobScheduler(intervalId)
    );
    ensure(fetched?.id === intervalId && fetched.limit === 2, 'getJobScheduler lost options');
    ensure(fetched.next === interval.next, 'interval upsert did not return authoritative next run');
    const fetchedPattern = await queue.getJobScheduler(patternId);
    ensure(
      fetchedPattern?.next === pattern.next,
      'pattern upsert did not return authoritative next run'
    );
    const schedulers = await tracker.invoke('Queue', 'getJobSchedulers', () =>
      queue.getJobSchedulers(0, -1, true)
    );
    ensure(
      schedulers.some((scheduler) => scheduler.id === intervalId) &&
        schedulers.some((scheduler) => scheduler.id === patternId),
      'getJobSchedulers omitted a scheduler'
    );
    const count = await tracker.invoke('Queue', 'getJobSchedulersCount', () =>
      queue.getJobSchedulersCount()
    );
    ensure(count === 2, `getJobSchedulersCount returned ${count}`);
    const removed = await tracker.invoke('Queue', 'removeJobScheduler', () =>
      queue.removeJobScheduler(intervalId)
    );
    ensure(removed, 'removeJobScheduler returned false');
    ensure((await queue.getJobScheduler(intervalId)) === null, 'removed scheduler still exists');
    await queue.removeJobScheduler(patternId);
  } finally {
    await harness.close();
  }

  return tracker;
}
