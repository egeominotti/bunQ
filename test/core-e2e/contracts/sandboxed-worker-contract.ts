import { resolve } from 'node:path';
import { SandboxedWorker } from '../../../src/client';
import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure, eventually } from '../support/tracker';

const PROCESSOR_PATH = resolve(import.meta.dir, '../fixtures/sandbox-processor.ts');

export async function runSandboxedWorkerContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'sandboxed-worker');
  const tracker = new CoverageTracker(mode, 'sandboxed-worker-contract');

  try {
    const queue = harness.queue<{ value: number }>('jobs');
    const worker = new SandboxedWorker<{ value: number }>(queue.name, {
      processor: PROCESSOR_PATH,
      concurrency: 2,
      timeout: 10_000,
      pollInterval: 5,
      ...(mode === 'embedded' ? {} : { connection: harness.connection() }),
    });
    harness.addCleanup(() => worker.stop(true));

    let ready = 0;
    let active = 0;
    let completed = 0;
    let progress = 0;
    let log = '';
    tracker.call('SandboxedWorker', 'once', () =>
      worker.once('ready', () => {
        ready++;
      })
    );
    tracker.call('SandboxedWorker', 'on', () =>
      worker
        .on('active', () => {
          active++;
        })
        .on('completed', (_job, result) => {
          ensure(
            (result as { doubled?: number }).doubled === 42,
            'sandboxed result was not returned from the child process'
          );
          completed++;
        })
        .on('progress', (_job, value) => {
          progress = value;
        })
        .on('log', (_job, message) => {
          log = message;
        })
        .on('error', () => undefined)
    );

    await tracker.invoke('SandboxedWorker', 'start', () => worker.start());
    ensure(ready === 1, 'sandboxed worker did not emit ready');
    ensure(
      tracker.call('SandboxedWorker', 'isRunning', () => worker.isRunning()),
      'not running'
    );
    const stats = tracker.call('SandboxedWorker', 'getStats', () => worker.getStats());
    ensure(
      stats.total === 2 && stats.idle === 2,
      `unexpected sandbox stats: ${JSON.stringify(stats)}`
    );

    const job = await queue.add('sandbox', { value: 21 }, { durable: true });
    await eventually(
      () => queue.getJobState(job.id),
      (state) => state === 'completed',
      'sandboxed worker did not complete its real queue job',
      20_000
    );
    ensure(active === 1 && completed === 1, 'sandboxed lifecycle events did not fire once');
    ensure(progress === 50, `sandboxed progress was ${progress}`);
    ensure(log === 'sandbox processor completed', `sandboxed log was ${JSON.stringify(log)}`);

    await tracker.invoke('SandboxedWorker', 'stop', () => worker.stop());
    ensure(!worker.isRunning() && worker.getStats().total === 0, 'sandboxed worker did not stop');
  } finally {
    await harness.close();
  }

  return tracker;
}
