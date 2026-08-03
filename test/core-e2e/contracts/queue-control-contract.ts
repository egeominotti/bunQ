import type { Job } from '../../../src/client';
import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure, eventually } from '../support/tracker';

export async function runQueueControlContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'queue-control');
  const tracker = new CoverageTracker(mode, 'queue-control-contract');
  let unlock = () => undefined;
  const lockGate = new Promise<void>((resolve) => {
    unlock = resolve;
  });

  try {
    const queue = harness.queue<{ value: number }>('mutations');
    const mutable = await queue.add('mutable', { value: 1 }, { durable: true });
    await tracker.invoke('Queue', 'updateJobProgress', () =>
      queue.updateJobProgress(mutable.id, { phase: 'queued' })
    );
    await tracker.invoke('Queue', 'addJobLog', () => queue.addJobLog(mutable.id, 'first log'));
    let logs = await tracker.invoke('Queue', 'getJobLogs', () => queue.getJobLogs(mutable.id));
    ensure(
      logs.logs.some((line) => line.includes('first log')),
      'getJobLogs missed the entry'
    );
    await tracker.invoke('Queue', 'clearJobLogs', () => queue.clearJobLogs(mutable.id));
    logs = await queue.getJobLogs(mutable.id);
    ensure(logs.count === 0, 'clearJobLogs did not remove entries');
    await tracker.invoke('Queue', 'updateJobData', () =>
      queue.updateJobData(mutable.id, { value: 2 })
    );
    ensure((await queue.getJob(mutable.id))?.data.value === 2, 'updateJobData did not persist');
    await tracker.invoke('Queue', 'changeJobPriority', () =>
      queue.changeJobPriority(mutable.id, { priority: 42, lifo: true })
    );
    ensure((await queue.getJob(mutable.id))?.priority === 42, 'changeJobPriority did not persist');
    await tracker.invoke('Queue', 'changeJobDelay', () => queue.changeJobDelay(mutable.id, 60_000));
    ensure((await queue.getJobState(mutable.id)) === 'delayed', 'changeJobDelay did not delay job');
    await tracker.invoke('Queue', 'promoteJob', () => queue.promoteJob(mutable.id));
    ensure((await queue.getJobState(mutable.id)) !== 'delayed', 'promoteJob did not promote job');

    const delayed = await queue.addBulk(
      [1, 2].map((value) => ({
        name: 'delayed',
        data: { value },
        opts: { delay: 60_000, durable: true },
      }))
    );
    const promoted = await tracker.invoke('Queue', 'promoteJobs', () =>
      queue.promoteJobs({ count: 2 })
    );
    ensure(promoted === 2, `promoteJobs returned ${promoted}`);
    for (const job of delayed)
      ensure((await queue.getJobState(job.id)) !== 'delayed', 'job stayed delayed');

    const removeSync = await queue.add('remove-sync', { value: 3 }, { durable: true });
    tracker.call('Queue', 'remove', () => queue.remove(removeSync.id));
    await eventually(
      () => queue.getJobState(removeSync.id),
      (state) => state === 'unknown',
      'remove did not delete the job'
    );
    const removeAsync = await queue.add('remove-async', { value: 4 }, { durable: true });
    await tracker.invoke('Queue', 'removeAsync', () => queue.removeAsync(removeAsync.id));
    ensure(
      (await queue.getJobState(removeAsync.id)) === 'unknown',
      'removeAsync did not delete job'
    );

    const lockQueue = harness.queue<{ value: number }>('lock');
    let activeJob: Job<{ value: number }> | null = null;
    const lockWorker = harness.worker<{ value: number }>(
      lockQueue.name,
      async (job) => {
        activeJob = job;
        await lockGate;
      },
      { concurrency: 1 }
    );
    await lockWorker.waitUntilReady();
    const locked = await lockQueue.add('locked', { value: 1 }, { durable: true });
    await eventually(
      () => activeJob,
      (job) => job !== null,
      'worker did not expose active job'
    );
    const token = activeJob?.token;
    ensure(typeof token === 'string' && token.length > 0, 'active job has no lock token');
    const extended = await tracker.invoke('Queue', 'extendJobLock', () =>
      lockQueue.extendJobLock(locked.id, token, 30_000)
    );
    ensure(extended === 30_000, `extendJobLock returned ${extended}`);
    unlock();
    await eventually(
      () => lockQueue.getJobState(locked.id),
      (state) => state === 'completed',
      'locked job did not complete'
    );

    const retryQueue = harness.queue<{ fail: boolean }>('retry');
    let attempts = 0;
    const retryWorker = harness.worker<{ fail: boolean }>(retryQueue.name, async () => {
      attempts++;
      if (attempts === 1) throw new Error('first attempt fails');
      return 'ok';
    });
    const retryJob = await retryQueue.add('retry', { fail: true }, { attempts: 1, durable: true });
    await eventually(
      () => retryQueue.getJobState(retryJob.id),
      (state) => state === 'failed',
      'retry fixture did not fail'
    );
    await tracker.invoke('Queue', 'retryJob', () => retryQueue.retryJob(retryJob.id));
    await eventually(
      () => retryQueue.getJobState(retryJob.id),
      (state) => state === 'completed',
      'retryJob did not re-run the job'
    );
    await tracker.invoke('Queue', 'retryJobs', () =>
      retryQueue.retryJobs({ state: 'completed', count: 1 })
    );
    await eventually(
      () => attempts,
      (value) => Number(value) >= 3,
      'retryJobs did not re-run job'
    );
    await retryWorker.close();

    const cleanQueue = harness.queue<{ value: number }>('clean');
    const cleanWorker = harness.worker(cleanQueue.name, async () => 'cleaned');
    const firstClean = await cleanQueue.add('clean', { value: 1 }, { durable: true });
    await eventually(
      () => cleanQueue.getJobState(firstClean.id),
      (state) => state === 'completed',
      'clean fixture did not complete'
    );
    if (mode === 'embedded') {
      const syncCleaned = tracker.call('Queue', 'clean', () =>
        cleanQueue.clean(0, 100, 'completed')
      );
      ensure(syncCleaned.includes(firstClean.id), 'clean missed the completed job');
    }
    const secondClean = await cleanQueue.add('clean', { value: 2 }, { durable: true });
    await eventually(
      () => cleanQueue.getJobState(secondClean.id),
      (state) => state === 'completed',
      'second clean fixture did not complete'
    );
    const asyncCleaned = await tracker.invoke('Queue', 'cleanAsync', () =>
      cleanQueue.cleanAsync(0, 100, 'completed')
    );
    ensure(asyncCleaned.includes(secondClean.id), 'cleanAsync did not return removed job ID');
    await cleanWorker.close();

    const control = harness.queue<{ value: number }>('control');
    tracker.call('Queue', 'pause', () => control.pause());
    await eventually(
      () => control.isPausedAsync(),
      (paused) => paused === true,
      'pause not applied'
    );
    if (mode === 'embedded') {
      const syncPaused = tracker.call('Queue', 'isPaused', () => control.isPaused());
      ensure(syncPaused, 'isPaused did not observe the paused queue');
    }
    tracker.call('Queue', 'resume', () => control.resume());
    await eventually(
      () => control.isPausedAsync(),
      (paused) => paused === false,
      'resume not applied'
    );
    await tracker.invoke('Queue', 'pauseAsync', () => control.pauseAsync());
    ensure(await control.isPausedAsync(), 'pauseAsync did not pause');
    const pausedAsync = await tracker.invoke('Queue', 'isPausedAsync', () =>
      control.isPausedAsync()
    );
    ensure(pausedAsync, 'isPausedAsync returned false');
    await tracker.invoke('Queue', 'resumeAsync', () => control.resumeAsync());

    const syncDrain = harness.queue<{ value: number }>('drain-sync');
    await syncDrain.add('job', { value: 1 }, { durable: true });
    tracker.call('Queue', 'drain', () => syncDrain.drain());
    await eventually(
      () => syncDrain.countAsync(),
      (count) => count === 0,
      'drain did not empty queue'
    );
    const asyncDrain = harness.queue<{ value: number }>('drain-async');
    await asyncDrain.add('job', { value: 1 }, { durable: true });
    const drained = await tracker.invoke('Queue', 'drainAsync', () => asyncDrain.drainAsync());
    ensure(drained === 1, `drainAsync returned ${drained}`);

    const syncObliterate = harness.queue<{ value: number }>('obliterate-sync');
    await syncObliterate.add('job', { value: 1 }, { durable: true });
    tracker.call('Queue', 'obliterate', () => syncObliterate.obliterate());
    await eventually(
      () => syncObliterate.countAsync(),
      (count) => count === 0,
      'obliterate did not clear queue'
    );
    const asyncObliterate = harness.queue<{ value: number }>('obliterate-async');
    await asyncObliterate.add('job', { value: 1 }, { durable: true });
    await tracker.invoke('Queue', 'obliterateAsync', () => asyncObliterate.obliterateAsync());
    ensure((await asyncObliterate.countAsync()) === 0, 'obliterateAsync did not clear queue');

    const closeQueue = harness.queue('close');
    tracker.call('Queue', 'close', () => closeQueue.close());
    const disconnectQueue = harness.queue('disconnect');
    await tracker.invoke('Queue', 'disconnect', () => disconnectQueue.disconnect());
  } finally {
    unlock();
    await harness.close();
  }

  return tracker;
}
