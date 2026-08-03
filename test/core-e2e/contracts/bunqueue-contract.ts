import { Bunqueue } from '../../../src/client';
import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure, eventually } from '../support/tracker';

export async function runBunqueueContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'bunqueue');
  const tracker = new CoverageTracker(mode, 'bunqueue-contract');

  try {
    let middlewareCalls = 0;
    let processed = 0;
    const app = new Bunqueue<{ value: number }, { doubled: number }>(harness.unique('simple'), {
      ...harness.workerOptions({ autorun: false, concurrency: 2 }),
      processor: async (job) => {
        processed++;
        return { doubled: job.data.value * 2 };
      },
      ttl: { defaultTtl: 60_000 },
      circuitBreaker: { threshold: 3, resetTimeout: 1_000 },
    });
    harness.addCleanup(() => app.close(true));
    tracker.call('Bunqueue', 'use', () =>
      app.use(async (_job, next) => {
        middlewareCalls++;
        return await next();
      })
    );
    tracker.call('Bunqueue', 'trigger', () =>
      app.trigger({
        on: 'source',
        create: 'follow-up',
        data: (result) => ({ value: (result as { doubled: number }).doubled }),
      })
    );

    let activeEvents = 0;
    let completedOnce = 0;
    const activeListener = (): void => {
      activeEvents++;
    };
    tracker.call('Bunqueue', 'on', () => app.on('active', activeListener));
    tracker.call('Bunqueue', 'once', () =>
      app.once('completed', () => {
        completedOnce++;
      })
    );
    tracker.call('Bunqueue', 'off', () => app.off('active', activeListener));
    app.on('active', activeListener);

    const first = await tracker.invoke('Bunqueue', 'add', () =>
      app.add('source', { value: 5 }, { durable: true })
    );
    const bulk = await tracker.invoke('Bunqueue', 'addBulk', () =>
      app.addBulk([
        { name: 'bulk', data: { value: 2 }, opts: { durable: true } },
        { name: 'bulk', data: { value: 3 }, opts: { durable: true } },
      ])
    );
    ensure(bulk.length === 2, 'Bunqueue.addBulk did not add both jobs');
    if (mode === 'embedded') {
      const syncCount = tracker.call('Bunqueue', 'count', () => app.count());
      ensure(syncCount === 3, `Bunqueue.count returned ${syncCount}`);
    }
    ensure(
      (await tracker.invoke('Bunqueue', 'countAsync', () => app.countAsync())) === 3,
      'bad count'
    );
    ensure(
      (await tracker.invoke('Bunqueue', 'getJob', () => app.getJob(first.id)))?.id === first.id,
      'getJob'
    );
    const syncCounts = await tracker.invoke(
      'Bunqueue',
      'getJobCounts',
      async () => await app.getJobCounts()
    );
    ensure(typeof syncCounts.waiting === 'number', 'getJobCounts did not return counters');
    const asyncCounts = await tracker.invoke('Bunqueue', 'getJobCountsAsync', () =>
      app.getJobCountsAsync()
    );
    ensure(asyncCounts.waiting === 3, `getJobCountsAsync waiting=${asyncCounts.waiting}`);

    ensure(
      !tracker.call('Bunqueue', 'isRunning', () => app.isRunning()),
      'autorun:false was running'
    );
    app.worker.run();
    tracker.call('Bunqueue', 'pause', () => app.pause());
    ensure(
      tracker.call('Bunqueue', 'isPaused', () => app.isPaused()),
      'Bunqueue.pause failed'
    );
    tracker.call('Bunqueue', 'resume', () => app.resume());
    await tracker.invoke('Bunqueue', 'pauseAsync', () => app.pauseAsync());
    ensure(await app.queue.isPausedAsync(), 'Bunqueue.pauseAsync did not pause its queue');
    await tracker.invoke('Bunqueue', 'resumeAsync', () => app.resumeAsync());
    ensure(!(await app.queue.isPausedAsync()), 'Bunqueue.resumeAsync did not resume its queue');
    await eventually(
      () => processed,
      (count) => Number(count) >= 4,
      'simple pipeline did not run'
    );
    ensure(middlewareCalls >= 4, 'middleware did not wrap source, bulk, and triggered jobs');
    ensure(activeEvents >= 4 && completedOnce === 1, 'Bunqueue event forwarding failed');

    const cronId = harness.unique('cron');
    const everyId = harness.unique('every');
    const cron = await tracker.invoke('Bunqueue', 'cron', () =>
      app.cron(cronId, '0 0 1 1 *', { value: 1 }, { limit: 2 })
    );
    const every = await tracker.invoke('Bunqueue', 'every', () =>
      app.every(everyId, 60_000, { value: 2 }, { limit: 3 })
    );
    ensure(cron?.id === cronId && every?.id === everyId, 'simple schedulers were not persisted');
    const crons = await tracker.invoke('Bunqueue', 'listCrons', async () => await app.listCrons());
    ensure(
      crons.some((item) => item.id === cronId),
      'listCrons missed cron schedule'
    );
    ensure(
      await tracker.invoke('Bunqueue', 'removeCron', async () => await app.removeCron(cronId)),
      'removeCron'
    );

    ensure(
      tracker.call('Bunqueue', 'getCircuitState', () => app.getCircuitState()) === 'closed',
      'initial circuit state was not closed'
    );
    tracker.call('Bunqueue', 'resetCircuit', () => app.resetCircuit());
    tracker.call('Bunqueue', 'setDefaultTtl', () => app.setDefaultTtl(120_000));
    tracker.call('Bunqueue', 'setNameTtl', () => app.setNameTtl('bulk', 120_000));

    tracker.call('Bunqueue', 'setGlobalRateLimit', () => app.setGlobalRateLimit(5, 2_000));
    await eventually(
      () => app.queue.getGlobalRateLimit(),
      (value) => (value as { max?: number } | null)?.max === 5,
      'Bunqueue.setGlobalRateLimit was not applied'
    );
    tracker.call('Bunqueue', 'removeGlobalRateLimit', () => app.removeGlobalRateLimit());
    await eventually(
      () => app.queue.getGlobalRateLimit(),
      (value) => value === null,
      'Bunqueue.removeGlobalRateLimit was not applied'
    );
    await tracker.invoke('Bunqueue', 'setGlobalRateLimitAsync', () =>
      app.setGlobalRateLimitAsync(7, 3_000)
    );
    ensure(
      (await app.queue.getGlobalRateLimit())?.max === 7,
      'Bunqueue.setGlobalRateLimitAsync was not authoritative'
    );
    await tracker.invoke('Bunqueue', 'removeGlobalRateLimitAsync', () =>
      app.removeGlobalRateLimitAsync()
    );
    ensure(
      (await app.queue.getGlobalRateLimit()) === null,
      'Bunqueue.removeGlobalRateLimitAsync was not authoritative'
    );

    let release = (): void => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let activeId = '';
    const cancellable = new Bunqueue<{ value: number }, void>(harness.unique('cancel'), {
      ...harness.workerOptions({ concurrency: 1 }),
      processor: async (job) => {
        activeId = job.id;
        await gate;
      },
    });
    harness.addCleanup(() => cancellable.close(true));
    const cancellableJob = await cancellable.add('cancel', { value: 1 }, { durable: true });
    await eventually(
      () => activeId,
      (id) => id === cancellableJob.id,
      'cancel job did not activate'
    );
    const signal = tracker.call('Bunqueue', 'getSignal', () => cancellable.getSignal(activeId));
    ensure(signal && !signal.aborted, 'getSignal did not expose active cancellation signal');
    tracker.call('Bunqueue', 'cancel', () => cancellable.cancel(activeId));
    ensure(
      tracker.call('Bunqueue', 'isCancelled', () => cancellable.isCancelled(activeId)),
      'Bunqueue.cancel did not abort the job signal'
    );
    release();

    const dlq = new Bunqueue<{ value: number }, never>(harness.unique('dlq'), {
      ...harness.workerOptions({ autorun: false }),
      processor: async () => {
        throw new Error('unused processor');
      },
    });
    harness.addCleanup(() => dlq.close(true));
    tracker.call('Bunqueue', 'setDlqConfig', () => dlq.setDlqConfig({ maxEntries: 20 }));
    await eventually(
      () => dlq.queue.getDlqConfigAsync(),
      (config) => (config as { maxEntries?: number }).maxEntries === 20,
      'Bunqueue.setDlqConfig was not applied'
    );
    await tracker.invoke('Bunqueue', 'setDlqConfigAsync', () =>
      dlq.setDlqConfigAsync({ maxAutoRetries: 3 })
    );
    const authoritativeConfig = await tracker.invoke('Bunqueue', 'getDlqConfigAsync', () =>
      dlq.getDlqConfigAsync()
    );
    ensure(
      authoritativeConfig.maxEntries === 20 && authoritativeConfig.maxAutoRetries === 3,
      'Bunqueue async DLQ config mismatch'
    );
    if (mode === 'embedded') {
      ensure(
        tracker.call('Bunqueue', 'getDlqConfig', () => dlq.getDlqConfig()).maxEntries === 20,
        'Bunqueue.getDlqConfig mismatch'
      );
    }
    const doomed = await dlq.add('doomed', { value: 1 }, { attempts: 1, durable: true });
    const pulled = await harness.brokerManager().pull(dlq.name);
    ensure(String(pulled?.id) === doomed.id, 'DLQ fixture was not pulled');
    if (!pulled) throw new Error('DLQ fixture missing');
    await harness.brokerManager().fail(pulled.id, 'simple DLQ failure');
    if (mode === 'embedded') {
      const syncDlq = tracker.call('Bunqueue', 'getDlq', () => dlq.getDlq());
      ensure(
        syncDlq.some((entry) => entry.job.id === doomed.id),
        'Bunqueue.getDlq missed job'
      );
      const stats = tracker.call('Bunqueue', 'getDlqStats', () => dlq.getDlqStats());
      ensure(stats.total === 1, `Bunqueue.getDlqStats returned ${stats.total}`);
    }
    const authoritativeEntries = await tracker.invoke('Bunqueue', 'getDlqAsync', () =>
      dlq.getDlqAsync()
    );
    ensure(
      authoritativeEntries.some((entry) => entry.job.id === doomed.id),
      'Bunqueue.getDlqAsync missed failed job'
    );
    const authoritativeStats = await tracker.invoke('Bunqueue', 'getDlqStatsAsync', () =>
      dlq.getDlqStatsAsync()
    );
    ensure(authoritativeStats.total === 1, 'Bunqueue.getDlqStatsAsync total mismatch');
    tracker.call('Bunqueue', 'retryDlq', () => dlq.retryDlq(doomed.id));
    await eventually(
      () => dlq.queue.getJobState(doomed.id),
      (state) => state === 'waiting',
      'Bunqueue.retryDlq did not retry the job'
    );
    const pulledAgain = await harness.brokerManager().pull(dlq.name);
    ensure(String(pulledAgain?.id) === doomed.id, 'retried DLQ fixture was not pulled');
    if (!pulledAgain) throw new Error('retried DLQ fixture missing');
    await harness.brokerManager().fail(pulledAgain.id, 'simple DLQ failure again');
    const retriedCount = await tracker.invoke('Bunqueue', 'retryDlqAsync', () =>
      dlq.retryDlqAsync(doomed.id)
    );
    ensure(retriedCount === 1, `Bunqueue.retryDlqAsync returned ${retriedCount}`);
    const pulledThird = await harness.brokerManager().pull(dlq.name);
    ensure(String(pulledThird?.id) === doomed.id, 'async-retried DLQ fixture was not pulled');
    if (!pulledThird) throw new Error('async-retried DLQ fixture missing');
    await harness.brokerManager().fail(pulledThird.id, 'simple DLQ failure third time');
    tracker.call('Bunqueue', 'purgeDlq', () => dlq.purgeDlq());
    await eventually(
      () => dlq.queue.getDlqAsync(),
      (entries) => Array.isArray(entries) && entries.length === 0,
      'Bunqueue.purgeDlq did not purge the server DLQ'
    );
    const purgeCandidate = await dlq.add(
      'purge-async',
      { value: 2 },
      { attempts: 1, durable: true }
    );
    const pulledForPurge = await harness.brokerManager().pull(dlq.name);
    ensure(
      String(pulledForPurge?.id) === purgeCandidate.id,
      'async purge DLQ fixture was not pulled'
    );
    if (!pulledForPurge) throw new Error('async purge DLQ fixture missing');
    await harness.brokerManager().fail(pulledForPurge.id, 'async purge failure');
    const purgedCount = await tracker.invoke('Bunqueue', 'purgeDlqAsync', () =>
      dlq.purgeDlqAsync()
    );
    ensure(purgedCount === 1, `Bunqueue.purgeDlqAsync returned ${purgedCount}`);

    await tracker.invoke('Bunqueue', 'close', () => app.close());
    ensure(
      tracker.call('Bunqueue', 'isClosed', () => app.isClosed()),
      'Bunqueue.close failed'
    );
  } finally {
    await harness.close();
  }

  return tracker;
}
