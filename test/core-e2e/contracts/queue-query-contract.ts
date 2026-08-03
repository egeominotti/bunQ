import type { Job } from '../../../src/client';
import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure, eventually } from '../support/tracker';

export async function runQueueQueryContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'queue-query');
  const tracker = new CoverageTracker(mode, 'queue-query-contract');
  let releaseActive = () => undefined;
  const activeGate = new Promise<void>((resolve) => {
    releaseActive = resolve;
  });

  try {
    const queryQueue = harness.queue<{ kind: string }>('query');
    const stateQueue = harness.queue<{ kind: string }>('states');

    await tracker.invoke('Queue', 'waitUntilReady', () => queryQueue.waitUntilReady());
    const waiting = await tracker.invoke('Queue', 'add', () =>
      queryQueue.add('waiting', { kind: 'waiting' }, { durable: true })
    );
    const bulk = await tracker.invoke('Queue', 'addBulk', () =>
      queryQueue.addBulk([
        { name: 'prioritized', data: { kind: 'priority' }, opts: { priority: 10, durable: true } },
        { name: 'delayed', data: { kind: 'delay' }, opts: { delay: 60_000, durable: true } },
      ])
    );
    ensure(bulk.length === 2, 'addBulk did not return both jobs');

    const fetched = await tracker.invoke('Queue', 'getJob', () => queryQueue.getJob(waiting.id));
    ensure(fetched?.data.kind === 'waiting', 'getJob returned the wrong payload');
    const waitingState = await tracker.invoke('Queue', 'getJobState', () =>
      queryQueue.getJobState(waiting.id)
    );
    ensure(waitingState === 'waiting', `getJobState returned ${waitingState}`);

    if (mode === 'embedded') {
      const syncJobs = tracker.call('Queue', 'getJobs', () => queryQueue.getJobs({ end: -1 }));
      ensure(syncJobs.length === 3, 'getJobs did not return authoritative embedded jobs');
    }
    const asyncJobs = await tracker.invoke('Queue', 'getJobsAsync', () =>
      queryQueue.getJobsAsync({ end: -1 })
    );
    ensure(asyncJobs.length === 3, `getJobsAsync returned ${asyncJobs.length} jobs`);

    if (mode === 'embedded') {
      const waitingSync = tracker.call('Queue', 'getWaiting', () => queryQueue.getWaiting(0, -1));
      ensure(
        waitingSync.some((job) => job.id === waiting.id),
        'getWaiting missed the job'
      );
    }
    const waitingAsync = await tracker.invoke('Queue', 'getWaitingAsync', () =>
      queryQueue.getWaitingAsync(0, -1)
    );
    ensure(
      waitingAsync.some((job) => job.id === waiting.id),
      'getWaitingAsync missed the job'
    );

    if (mode === 'embedded') {
      const delayedSync = tracker.call('Queue', 'getDelayed', () => queryQueue.getDelayed(0, -1));
      ensure(
        delayedSync.some((job) => job.id === bulk[1].id),
        'getDelayed missed the job'
      );
    }
    const delayedAsync = await tracker.invoke('Queue', 'getDelayedAsync', () =>
      queryQueue.getDelayedAsync(0, -1)
    );
    ensure(
      delayedAsync.some((job) => job.id === bulk[1].id),
      'getDelayedAsync missed the job'
    );

    const prioritized = await tracker.invoke('Queue', 'getPrioritized', () =>
      queryQueue.getPrioritized(0, -1)
    );
    ensure(
      prioritized.some((job) => job.id === bulk[0].id),
      'getPrioritized missed the job'
    );
    const prioritizedCount = await tracker.invoke('Queue', 'getPrioritizedCount', () =>
      queryQueue.getPrioritizedCount()
    );
    ensure(prioritizedCount === 1, `getPrioritizedCount returned ${prioritizedCount}`);

    const worker = harness.worker<{ kind: string }, { kind: string }>(
      stateQueue.name,
      async (job: Job<{ kind: string }>) => {
        if (job.data.kind === 'fail') throw new Error('core E2E failure');
        if (job.data.kind === 'active') await activeGate;
        return { kind: job.data.kind };
      },
      { concurrency: 1 }
    );
    await worker.waitUntilReady();

    const completed = await stateQueue.add('complete', { kind: 'complete' }, { durable: true });
    await eventually(
      () => stateQueue.getJobState(completed.id),
      (state) => state === 'completed',
      'completed fixture did not finish'
    );
    const failed = await stateQueue.add('fail', { kind: 'fail' }, { attempts: 1, durable: true });
    await eventually(
      () => stateQueue.getJobState(failed.id),
      (state) => state === 'failed',
      'failed fixture did not enter the DLQ'
    );
    const active = await stateQueue.add('active', { kind: 'active' }, { durable: true });
    await eventually(
      () => stateQueue.getJobState(active.id),
      (state) => state === 'active',
      'active fixture was not leased'
    );

    if (mode === 'embedded') {
      const activeSync = tracker.call('Queue', 'getActive', () => stateQueue.getActive(0, -1));
      ensure(
        activeSync.some((job) => job.id === active.id),
        'getActive missed the lease'
      );
    }
    const activeAsync = await tracker.invoke('Queue', 'getActiveAsync', () =>
      stateQueue.getActiveAsync(0, -1)
    );
    ensure(
      activeAsync.some((job) => job.id === active.id),
      'getActiveAsync missed the lease'
    );

    if (mode === 'embedded') {
      const completedSync = tracker.call('Queue', 'getCompleted', () =>
        stateQueue.getCompleted(0, -1)
      );
      ensure(
        completedSync.some((job) => job.id === completed.id),
        'getCompleted missed job'
      );
    }
    const completedAsync = await tracker.invoke('Queue', 'getCompletedAsync', () =>
      stateQueue.getCompletedAsync(0, -1)
    );
    ensure(
      completedAsync.some((job) => job.id === completed.id),
      'getCompletedAsync missed job'
    );

    if (mode === 'embedded') {
      const failedSync = tracker.call('Queue', 'getFailed', () => stateQueue.getFailed(0, -1));
      ensure(
        failedSync.some((job) => job.id === failed.id),
        'getFailed missed the DLQ job'
      );
    }
    const failedAsync = await tracker.invoke('Queue', 'getFailedAsync', () =>
      stateQueue.getFailedAsync(0, -1)
    );
    ensure(
      failedAsync.some((job) => job.id === failed.id),
      'getFailedAsync missed the DLQ job'
    );

    const counts = await tracker.invoke(
      'Queue',
      'getJobCounts',
      async () => await stateQueue.getJobCounts()
    );
    ensure(counts.active === 1 && counts.completed >= 1 && counts.failed >= 1, 'bad job counts');
    const asyncCounts = await tracker.invoke('Queue', 'getJobCountsAsync', () =>
      stateQueue.getJobCountsAsync()
    );
    ensure(asyncCounts.active === 1, 'getJobCountsAsync missed active job');

    if (mode === 'embedded') {
      const count = tracker.call('Queue', 'count', () => queryQueue.count());
      ensure(count === 3, `count returned ${count}`);
    }
    const countAsync = await tracker.invoke('Queue', 'countAsync', () => queryQueue.countAsync());
    ensure(countAsync === 3, `countAsync returned ${countAsync}`);
    if (mode === 'embedded') {
      const priorityCounts = tracker.call('Queue', 'getCountsPerPriority', () =>
        queryQueue.getCountsPerPriority()
      );
      ensure(priorityCounts[10] === 1, 'getCountsPerPriority missed priority 10');
    }
    const priorityCountsAsync = await tracker.invoke('Queue', 'getCountsPerPriorityAsync', () =>
      queryQueue.getCountsPerPriorityAsync()
    );
    ensure(priorityCountsAsync[10] === 1, 'getCountsPerPriorityAsync missed priority 10');

    const countChecks: Array<[string, () => Promise<number>, (value: number) => boolean]> = [
      ['getWaitingCount', () => queryQueue.getWaitingCount(), (value) => value >= 1],
      ['getDelayedCount', () => queryQueue.getDelayedCount(), (value) => value === 1],
      ['getActiveCount', () => stateQueue.getActiveCount(), (value) => value === 1],
      ['getCompletedCount', () => stateQueue.getCompletedCount(), (value) => value >= 1],
      ['getFailedCount', () => stateQueue.getFailedCount(), (value) => value >= 1],
    ];
    for (const [method, operation, valid] of countChecks) {
      const value = await tracker.invoke('Queue', method, operation);
      ensure(valid(value), `${method} returned ${value}`);
    }

    releaseActive();
    await eventually(
      () => stateQueue.getJobState(active.id),
      (state) => state === 'completed',
      'active fixture did not complete after release'
    );
  } finally {
    releaseActive();
    await harness.close();
  }

  return tracker;
}
