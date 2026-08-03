import { describe, expect, test } from 'bun:test';
import { CoreE2eHarness, type CoreE2eMode } from './core-e2e/support/harness';
import { eventually } from './core-e2e/support/tracker';

const MODES = ['embedded', 'tcp'] as const satisfies readonly CoreE2eMode[];
const QUEUE_COUNT = 16;
const JOBS_PER_QUEUE = 500;

describe('batch completion lock contention', () => {
  for (const mode of MODES) {
    test(`${mode} completes every concurrently batched job without lock starvation`, async () => {
      const harness = await CoreE2eHarness.start(mode, 'batch-lock-contention');
      let completed = 0;

      try {
        const queues = Array.from({ length: QUEUE_COUNT }, (_, index) =>
          harness.queue<{ index: number }>(`queue-${index}`)
        );

        await Promise.all(
          queues.map((queue, queueIndex) =>
            queue.addBulk(
              Array.from({ length: JOBS_PER_QUEUE }, (_, jobIndex) => ({
                name: 'contended',
                data: { index: queueIndex * JOBS_PER_QUEUE + jobIndex },
                opts: { removeOnComplete: true },
              }))
            )
          )
        );

        const workers = queues.map((queue) => {
          const worker = harness.worker(queue.name, async (job) => job.data, {
            batchSize: 128,
            concurrency: 64,
            heartbeatInterval: 0,
          });
          worker.on('completed', () => completed++);
          return worker;
        });
        await Promise.all(workers.map((worker) => worker.waitUntilReady()));

        const total = QUEUE_COUNT * JOBS_PER_QUEUE;
        await eventually(
          () => completed,
          (value) => value === total,
          `${mode} batch completions stalled`,
          30_000
        );

        const counts = await Promise.all(queues.map((queue) => queue.getJobCountsAsync()));
        expect(completed).toBe(total);
        expect(counts.every((entry) => entry.waiting === 0 && entry.active === 0)).toBe(true);
      } finally {
        await harness.close();
      }
    }, 60_000);
  }
});
