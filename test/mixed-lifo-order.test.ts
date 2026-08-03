import { afterEach, describe, expect, test } from 'bun:test';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitUntil,
} from './docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

for (const mode of MODES) {
  describe(`mixed FIFO/LIFO total ordering [${mode}]`, () => {
    test('LIFO jobs run newest-first ahead of FIFO jobs at the same priority', async () => {
      harness = await startHarness('mixed-lifo', mode);
      const queue = harness.queue<{ label: string }>('mixed');
      await queue.pauseAsync();

      await queue.add('fifo-old', { label: 'fifo-old' }, { durable: true });
      await queue.add('lifo-old', { label: 'lifo-old' }, { lifo: true, durable: true });
      await queue.add('fifo-new', { label: 'fifo-new' }, { durable: true });
      await queue.add('lifo-new', { label: 'lifo-new' }, { lifo: true, durable: true });

      const order: string[] = [];
      harness.worker<{ label: string }, boolean>(
        queue.name,
        async (job) => {
          order.push(job.data.label);
          return true;
        },
        { concurrency: 1, batchSize: 1 }
      );
      await queue.resumeAsync();
      await waitUntil(() => order.length === 4, 'all mixed-order jobs', 15_000);

      expect(order).toEqual(['lifo-new', 'lifo-old', 'fifo-old', 'fifo-new']);
    }, 30_000);

    test('priority remains authoritative over the FIFO/LIFO tie-break', async () => {
      harness = await startHarness('mixed-lifo', mode);
      const queue = harness.queue<{ label: string }>('priority');
      await queue.pauseAsync();

      await queue.add(
        'higher-priority-fifo',
        { label: 'higher-priority-fifo' },
        { priority: 10, durable: true }
      );
      await queue.add(
        'lower-priority-lifo',
        { label: 'lower-priority-lifo' },
        { priority: 1, lifo: true, durable: true }
      );

      const order: string[] = [];
      harness.worker<{ label: string }, boolean>(
        queue.name,
        async (job) => {
          order.push(job.data.label);
          return true;
        },
        { concurrency: 1, batchSize: 1 }
      );
      await queue.resumeAsync();
      await waitUntil(() => order.length === 2, 'both prioritized jobs', 15_000);

      expect(order).toEqual(['higher-priority-fifo', 'lower-priority-lifo']);
    }, 30_000);
  });
}
