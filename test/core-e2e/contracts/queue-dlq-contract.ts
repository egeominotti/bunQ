import type { Queue, Worker } from '../../../src/client';
import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure, eventually } from '../support/tracker';

interface FailedFixture {
  id: string;
  queue: Queue<{ value: number }>;
  worker: Worker<{ value: number }, never>;
}

export async function runQueueDlqContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'queue-dlq');
  const tracker = new CoverageTracker(mode, 'queue-dlq-contract');

  const failedFixture = async (label: string): Promise<FailedFixture> => {
    const queue = harness.queue<{ value: number }>(label);
    const worker = harness.worker<{ value: number }, never>(queue.name, async () => {
      throw new Error(`expected ${label} failure`);
    });
    const job = await queue.add('fail', { value: 1 }, { attempts: 1, durable: true });
    await eventually(
      () => queue.getJobState(job.id),
      (state) => state === 'failed',
      `${label} did not enter the DLQ`
    );
    await worker.close();
    return { id: job.id, queue, worker };
  };

  const completedFixture = async (label: string): Promise<{ id: string; queue: Queue }> => {
    const queue = harness.queue(label);
    const worker = harness.worker(queue.name, async () => 'done');
    const job = await queue.add('complete', { value: 1 }, { durable: true });
    await eventually(
      () => queue.getJobState(job.id),
      (state) => state === 'completed',
      `${label} did not complete`
    );
    await worker.close();
    return { id: job.id, queue };
  };

  try {
    const inspection = await failedFixture('inspect');
    tracker.call('Queue', 'setDlqConfig', () =>
      inspection.queue.setDlqConfig({ maxEntries: 50, maxAge: null })
    );
    await eventually(
      () => inspection.queue.getDlqConfigAsync(),
      (config) => (config as { maxEntries?: number }).maxEntries === 50,
      'setDlqConfig was not applied'
    );
    if (mode === 'embedded') {
      const localConfig = tracker.call('Queue', 'getDlqConfig', () =>
        inspection.queue.getDlqConfig()
      );
      ensure(localConfig.maxEntries === 50, 'getDlqConfig missed configured value');
    }
    await tracker.invoke('Queue', 'setDlqConfigAsync', () =>
      inspection.queue.setDlqConfigAsync({ autoRetry: false, maxAutoRetries: 4 })
    );
    const config = await tracker.invoke('Queue', 'getDlqConfigAsync', () =>
      inspection.queue.getDlqConfigAsync()
    );
    ensure(config.maxAutoRetries === 4 && config.maxEntries === 50, 'DLQ config mismatch');

    if (mode === 'embedded') {
      const syncEntries = tracker.call('Queue', 'getDlq', () => inspection.queue.getDlq());
      ensure(
        syncEntries.some((entry) => entry.job.id === inspection.id),
        'getDlq missed failed job'
      );
    }
    const entries = await tracker.invoke('Queue', 'getDlqAsync', () =>
      inspection.queue.getDlqAsync({ reason: 'max_attempts_exceeded' })
    );
    ensure(
      entries.some((entry) => entry.job.id === inspection.id),
      'getDlqAsync missed job'
    );
    const jobs = await tracker.invoke('Queue', 'getDlqJobsAsync', () =>
      inspection.queue.getDlqJobsAsync(10)
    );
    ensure(
      jobs.some((job) => job.id === inspection.id),
      'getDlqJobsAsync missed job'
    );
    if (mode === 'embedded') {
      const syncStats = tracker.call('Queue', 'getDlqStats', () => inspection.queue.getDlqStats());
      ensure(syncStats.total === 1, `getDlqStats returned total ${syncStats.total}`);
    }
    const stats = await tracker.invoke('Queue', 'getDlqStatsAsync', () =>
      inspection.queue.getDlqStatsAsync()
    );
    ensure(stats.total === 1, `getDlqStatsAsync returned total ${stats.total}`);

    const retrySync = await failedFixture('retry-sync');
    const syncRetryCount = tracker.call('Queue', 'retryDlq', () =>
      retrySync.queue.retryDlq(retrySync.id)
    );
    ensure(typeof syncRetryCount === 'number', 'retryDlq did not return a number');
    await eventually(
      () => retrySync.queue.getJobState(retrySync.id),
      (state) => state === 'waiting',
      'retryDlq did not return job to waiting'
    );

    const retryAsync = await failedFixture('retry-async');
    const asyncRetryCount = await tracker.invoke('Queue', 'retryDlqAsync', () =>
      retryAsync.queue.retryDlqAsync(retryAsync.id)
    );
    ensure(asyncRetryCount === 1, `retryDlqAsync returned ${asyncRetryCount}`);
    ensure((await retryAsync.queue.getJobState(retryAsync.id)) === 'waiting', 'async retry failed');

    const filterSync = await failedFixture('filter-sync');
    const filterSyncCount = tracker.call('Queue', 'retryDlqByFilter', () =>
      filterSync.queue.retryDlqByFilter({ reason: 'max_attempts_exceeded', limit: 1 })
    );
    ensure(
      filterSyncCount === (mode === 'embedded' ? 1 : 0),
      `retryDlqByFilter returned ${filterSyncCount}`
    );
    await eventually(
      () => filterSync.queue.getJobState(filterSync.id),
      (state) => state === 'waiting',
      'sync filter retry did not reach the selected runtime'
    );

    const filterAsync = await failedFixture('filter-async');
    const filterAsyncCount = await tracker.invoke('Queue', 'retryDlqByFilterAsync', () =>
      filterAsync.queue.retryDlqByFilterAsync({ reason: 'max_attempts_exceeded', limit: 1 })
    );
    ensure(filterAsyncCount === 1, `retryDlqByFilterAsync returned ${filterAsyncCount}`);

    const purgeSync = await failedFixture('purge-sync');
    const syncPurgeCount = tracker.call('Queue', 'purgeDlq', () => purgeSync.queue.purgeDlq());
    ensure(typeof syncPurgeCount === 'number', 'purgeDlq did not return a number');
    await eventually(
      () => purgeSync.queue.getDlqStatsAsync(),
      (value) => (value as { total?: number }).total === 0,
      'purgeDlq did not empty the DLQ'
    );

    const purgeAsync = await failedFixture('purge-async');
    const asyncPurgeCount = await tracker.invoke('Queue', 'purgeDlqAsync', () =>
      purgeAsync.queue.purgeDlqAsync()
    );
    ensure(asyncPurgeCount === 1, `purgeDlqAsync returned ${asyncPurgeCount}`);

    const completedSync = await completedFixture('completed-sync');
    const syncCompletedCount = tracker.call('Queue', 'retryCompleted', () =>
      completedSync.queue.retryCompleted(completedSync.id)
    );
    ensure(typeof syncCompletedCount === 'number', 'retryCompleted did not return a number');
    await eventually(
      () => completedSync.queue.getJobState(completedSync.id),
      (state) => state === 'waiting',
      'retryCompleted did not return job to waiting'
    );

    const completedAsync = await completedFixture('completed-async');
    const asyncCompletedCount = await tracker.invoke('Queue', 'retryCompletedAsync', () =>
      completedAsync.queue.retryCompletedAsync(completedAsync.id)
    );
    ensure(asyncCompletedCount === 1, `retryCompletedAsync returned ${asyncCompletedCount}`);
    ensure(
      (await completedAsync.queue.getJobState(completedAsync.id)) === 'waiting',
      'retryCompletedAsync did not return job to waiting'
    );
  } finally {
    await harness.close();
  }

  return tracker;
}
