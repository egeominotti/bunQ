import { Worker, type Job } from '../../../src/client';
import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure, eventually } from '../support/tracker';

export async function runWorkerContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'worker');
  const tracker = new CoverageTracker(mode, 'worker-contract');

  try {
    const queue = harness.queue<{ value: number }>('lifecycle');
    const lifecycle = harness.worker<{ value: number }, number>(
      queue.name,
      async (job) => job.data.value * 2,
      { autorun: false, concurrency: 1 }
    );
    let activeEvents = 0;
    let completedEvents = 0;
    const activeListener = (): void => {
      activeEvents++;
    };
    tracker.call('Worker', 'on', () => lifecycle.on('active', activeListener));
    tracker.call('Worker', 'once', () =>
      lifecycle.once('completed', () => {
        completedEvents++;
      })
    );
    tracker.call('Worker', 'off', () => lifecycle.off('active', activeListener));
    lifecycle.on('active', activeListener);

    await tracker.invoke('Worker', 'waitUntilReady', () => lifecycle.waitUntilReady());
    ensure(!tracker.call('Worker', 'isRunning', () => lifecycle.isRunning()), 'autorun:false ran');
    tracker.call('Worker', 'run', () => lifecycle.run());
    ensure(lifecycle.isRunning(), 'run did not start worker');
    tracker.call('Worker', 'pause', () => lifecycle.pause());
    ensure(
      tracker.call('Worker', 'isPaused', () => lifecycle.isPaused()),
      'pause did not pause'
    );
    tracker.call('Worker', 'resume', () => lifecycle.resume());
    const lifecycleJob = await queue.add('lifecycle', { value: 21 }, { durable: true });
    await eventually(
      () => queue.getJobState(lifecycleJob.id),
      (state) => state === 'completed',
      'lifecycle worker did not complete job'
    );
    ensure(
      activeEvents === 1 && completedEvents === 1,
      'worker event methods did not fire exactly once'
    );

    const manualQueue = harness.queue<{ value: number }>('manual');
    const manual = harness.worker<{ value: number }, number>(
      manualQueue.name,
      async (job) => job.data.value + 1,
      { autorun: false, useLocks: false }
    );
    await manual.waitUntilReady();
    const manualAdded = await manualQueue.add('manual', { value: 4 }, { durable: true });
    const internal = await tracker.invoke('Worker', 'getNextJob', () => manual.getNextJob());
    ensure(internal && String(internal.id) === manualAdded.id, 'getNextJob returned wrong job');
    const next = await tracker.invoke('Worker', 'processJobManually', () =>
      manual.processJobManually(internal, undefined, async () => undefined)
    );
    ensure(next === undefined, 'processJobManually returned an unexpected job');
    await eventually(
      () => manualQueue.getJobState(manualAdded.id),
      (state) => state === 'completed',
      'processJobManually did not acknowledge job'
    );

    const lockQueue = harness.queue<{ value: number }>('lock');
    let lockWorker: Worker<{ value: number }>;
    let extended = -1;
    lockWorker = harness.worker<{ value: number }>(
      lockQueue.name,
      async (job: Job<{ value: number }>) => {
        ensure(job.token, 'locked worker job has no token');
        extended = await tracker.invoke('Worker', 'extendJobLocks', () =>
          lockWorker.extendJobLocks([job.id], [job.token as string], 30_000)
        );
      }
    );
    const locked = await lockQueue.add('lock', { value: 1 }, { durable: true });
    await eventually(
      () => lockQueue.getJobState(locked.id),
      (state) => state === 'completed',
      'lock extension job did not complete'
    );
    ensure(extended === 1, `extendJobLocks returned ${extended}`);

    const cancellationQueue = harness.queue<{ value: number }>('cancel');
    let release = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const startedIds = new Set<string>();
    const cancellation = harness.worker(
      cancellationQueue.name,
      async (job) => {
        startedIds.add(String(job.id));
        await gate;
      },
      { concurrency: 2 }
    );
    const first = await cancellationQueue.add('cancel', { value: 1 }, { durable: true });
    const second = await cancellationQueue.add('cancel', { value: 2 }, { durable: true });
    await eventually(
      async () => [
        await cancellationQueue.getJobState(first.id),
        await cancellationQueue.getJobState(second.id),
      ],
      (states) => Array.isArray(states) && states.every((state) => state === 'active'),
      'cancellation jobs were not both active'
    );
    await eventually(
      () => startedIds.size,
      (size) => size === 2,
      'cancellation processors did not both start'
    );
    ensure(
      startedIds.has(first.id) && startedIds.has(second.id),
      'worker started unexpected cancellation jobs'
    );
    const cancelled = tracker.call('Worker', 'cancelJob', () =>
      cancellation.cancelJob(first.id, 'single cancellation')
    );
    ensure(cancelled, 'cancelJob returned false for active job');
    ensure(
      tracker.call('Worker', 'isJobCancelled', () => cancellation.isJobCancelled(first.id)),
      'isJobCancelled missed cancelled job'
    );
    tracker.call('Worker', 'cancelAllJobs', () => cancellation.cancelAllJobs('cancel all'));
    ensure(
      cancellation.isJobCancelled(first.id) && cancellation.isJobCancelled(second.id),
      'cancelAllJobs missed an active job'
    );
    release();
    await cancellation.close();

    const limited = harness.worker(harness.unique('limited'), async () => undefined, {
      autorun: false,
      limiter: { max: 2, duration: 1_000 },
    });
    const limiterInfo = tracker.call('Worker', 'getRateLimiterInfo', () =>
      limited.getRateLimiterInfo()
    );
    ensure(limiterInfo?.max === 2 && limiterInfo.duration === 1_000, 'limiter info mismatch');
    tracker.call('Worker', 'rateLimit', () => limited.rateLimit(500));
    ensure(
      tracker.call('Worker', 'isRateLimited', () => limited.isRateLimited()),
      'not rate limited'
    );
    await tracker.invoke('Worker', 'startStalledCheckTimer', () =>
      limited.startStalledCheckTimer()
    );
    const startedAt = performance.now();
    await tracker.invoke('Worker', 'delay', () => limited.delay(20));
    ensure(performance.now() - startedAt >= 10, 'delay resolved too early');

    const groupRateQueue = harness.queue('manual-group-rate');
    let groupRateAttempts = 0;
    let groupRateWorker!: Worker;
    groupRateWorker = harness.worker(groupRateQueue.name, async (job) => {
      groupRateAttempts++;
      if (groupRateAttempts === 1) {
        await tracker.invoke('Worker', 'rateLimitGroup', () =>
          groupRateWorker.rateLimitGroup(job, 20)
        );
        throw Worker.RateLimitError();
      }
    });
    const groupRateJob = await groupRateQueue.add(
      'manual-group-rate',
      {},
      { group: { id: 'A' }, durable: true }
    );
    await eventually(
      () => groupRateQueue.getJobState(groupRateJob.id),
      (state) => state === 'completed',
      'rateLimitGroup did not requeue the job'
    );
    ensure(groupRateAttempts === 2, `rateLimitGroup ran ${groupRateAttempts} attempts`);

    await tracker.invoke('Worker', 'close', () => lifecycle.close());
    ensure(
      tracker.call('Worker', 'isClosed', () => lifecycle.isClosed()),
      'close did not close worker'
    );
  } finally {
    await harness.close();
  }

  return tracker;
}
