import { QueueGroup } from '../../../src/client';
import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure, eventually } from '../support/tracker';

export async function runQueueGroupContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'queue-group');
  const tracker = new CoverageTracker(mode, 'queue-group-contract');

  try {
    const group = new QueueGroup(harness.unique('namespace'));
    const first = tracker.call('QueueGroup', 'getQueue', () =>
      group.getQueue<{ value: number }>('first', harness.queueOptions())
    );
    const second = group.getQueue<{ value: number }>('second', harness.queueOptions());
    harness.addCleanup(() => first.close());
    harness.addCleanup(() => second.close());
    const worker = tracker.call('QueueGroup', 'getWorker', () =>
      group.getWorker<{ value: number }, number>(
        'first',
        async (job) => job.data.value * 2,
        harness.workerOptions()
      )
    );
    harness.addCleanup(() => worker.close(true));
    const processed = await first.add('group-worker', { value: 21 }, { durable: true });
    await eventually(
      () => first.getJobState(processed.id),
      (state) => state === 'completed',
      'QueueGroup worker did not process its namespaced queue'
    );
    await second.add('registered', { value: 1 }, { durable: true, delay: 60_000 });

    if (mode === 'embedded') {
      const syncNames = tracker.call('QueueGroup', 'listQueues', () => group.listQueues());
      ensure(
        syncNames.includes('first') && syncNames.includes('second'),
        'sync group listing incomplete'
      );
    }
    const asyncNames = await tracker.invoke('QueueGroup', 'listQueuesAsync', () =>
      group.listQueuesAsync()
    );
    ensure(
      asyncNames.includes('first') && asyncNames.includes('second'),
      'async group listing incomplete'
    );

    if (mode === 'embedded') {
      tracker.call('QueueGroup', 'pauseAll', () => group.pauseAll());
      ensure(await first.isPausedAsync(), 'pauseAll did not pause first queue');
      tracker.call('QueueGroup', 'resumeAll', () => group.resumeAll());
      ensure(!(await first.isPausedAsync()), 'resumeAll did not resume first queue');
    }
    await tracker.invoke('QueueGroup', 'pauseAllAsync', () => group.pauseAllAsync());
    ensure(await first.isPausedAsync(), 'pauseAllAsync did not pause first queue');
    ensure(await second.isPausedAsync(), 'pauseAllAsync did not pause second queue');
    await tracker.invoke('QueueGroup', 'resumeAllAsync', () => group.resumeAllAsync());
    ensure(!(await first.isPausedAsync()), 'resumeAllAsync did not resume first queue');

    await first.add('drain', { value: 1 }, { durable: true, delay: 60_000 });
    if (mode === 'embedded') {
      tracker.call('QueueGroup', 'drainAll', () => group.drainAll());
      ensure((await first.countAsync()) === 0, 'drainAll did not empty group');
    }
    await first.add('drain-async', { value: 2 }, { durable: true, delay: 60_000 });
    const drained = await tracker.invoke('QueueGroup', 'drainAllAsync', () =>
      group.drainAllAsync()
    );
    ensure(drained >= 1, `drainAllAsync returned ${drained}`);

    await second.add('obliterate', { value: 3 }, { durable: true, delay: 60_000 });
    if (mode === 'embedded') {
      tracker.call('QueueGroup', 'obliterateAll', () => group.obliterateAll());
      ensure((await second.countAsync()) === 0, 'obliterateAll did not clear group');
    }
    await second.add('obliterate-async', { value: 4 }, { durable: true, delay: 60_000 });
    await tracker.invoke('QueueGroup', 'obliterateAllAsync', () => group.obliterateAllAsync());
    ensure((await second.countAsync()) === 0, 'obliterateAllAsync did not clear group');
  } finally {
    await harness.close();
  }

  return tracker;
}
