import { describe, expect, test } from 'bun:test';
import {
  type CoreE2eHarness,
  MODES,
  startHarness,
  waitForState,
  waitUntil,
} from './docs-guide-support';

async function withHarness(
  mode: (typeof MODES)[number],
  label: string,
  run: (harness: CoreE2eHarness) => Promise<void>
): Promise<void> {
  const harness = await startHarness(label, mode);
  try {
    await run(harness);
  } finally {
    await harness.close();
  }
}

for (const mode of MODES) {
  describe(`worker rate-limit admission [${mode}]`, () => {
    test('manual processing reserves the rate slot before invoking the processor', async () => {
      await withHarness(mode, 'manual-rate-admission', async (harness) => {
        const queue = harness.queue<{ value: number }>('manual-rate');
        const starts: number[] = [];
        const worker = harness.worker<{ value: number }, number>(
          queue.name,
          async (job) => {
            starts.push(Date.now());
            return job.data.value;
          },
          {
            autorun: false,
            useLocks: false,
            limiter: { max: 1, duration: 200 },
          }
        );
        await worker.waitUntilReady();
        const firstJob = await queue.add('first', { value: 1 }, { durable: true });
        const secondJob = await queue.add('second', { value: 2 }, { durable: true });

        const first = await worker.getNextJob();
        expect(first && String(first.id)).toBe(firstJob.id);
        if (!first) throw new Error('first manual job was not pulled');
        await worker.processJobManually(first);

        const second = await worker.getNextJob();
        expect(second && String(second.id)).toBe(secondJob.id);
        if (!second) throw new Error('second manual job was not pulled');
        await worker.processJobManually(second);

        expect(starts).toHaveLength(2);
        expect(starts[1] - starts[0]).toBeGreaterThanOrEqual(170);
        expect(worker.getRateLimiterInfo()?.current).toBe(1);
      });
    });

    test('manual processing honors group concurrency and releases it exactly once', async () => {
      await withHarness(mode, 'manual-group-admission', async (harness) => {
        const queue = harness.queue<{ tenant: string }>('manual-group');
        let active = 0;
        let peak = 0;
        const worker = harness.worker<{ tenant: string }, boolean>(
          queue.name,
          async () => {
            active++;
            peak = Math.max(peak, active);
            await Bun.sleep(60);
            active--;
            return true;
          },
          {
            autorun: false,
            useLocks: false,
            concurrency: 2,
            limiter: { max: 1, duration: 0, groupKey: 'tenant' },
          }
        );
        await worker.waitUntilReady();
        await queue.add('first', { tenant: 'acme' }, { durable: true });
        await queue.add('second', { tenant: 'acme' }, { durable: true });
        const first = await worker.getNextJob();
        const second = await worker.getNextJob();
        if (!first || !second) throw new Error('manual group jobs were not pulled');

        await Promise.all([worker.processJobManually(first), worker.processJobManually(second)]);

        expect(peak).toBe(1);
        expect(active).toBe(0);
      });
    });

    test('manual duplicate and concurrency gates run before rate admission', async () => {
      await withHarness(mode, 'manual-concurrency-admission', async (harness) => {
        const queue = harness.queue('manual-concurrency');
        let runs = 0;
        let release = (): void => undefined;
        const held = new Promise<void>((resolve) => {
          release = resolve;
        });
        const worker = harness.worker(
          queue.name,
          async () => {
            runs++;
            if (runs === 1) await held;
            return true;
          },
          {
            autorun: false,
            useLocks: false,
            concurrency: 1,
            limiter: { max: 5, duration: 60_000 },
          }
        );
        await worker.waitUntilReady();
        await queue.add('first', {}, { durable: true });
        await queue.add('second', {}, { durable: true });
        const first = await worker.getNextJob();
        const second = await worker.getNextJob();
        if (!first || !second) throw new Error('manual concurrency jobs were not pulled');

        const firstProcessing = worker.processJobManually(first);
        await waitUntil(() => runs === 1, 'the first manual processor to start');
        const duplicate = worker.processJobManually(first);
        const secondProcessing = worker.processJobManually(second);
        expect(await duplicate).toBeUndefined();
        await Bun.sleep(50);
        expect(runs).toBe(1);
        expect(worker.getRateLimiterInfo()?.current).toBe(1);

        release();
        await Promise.all([firstProcessing, secondProcessing]);
        expect(runs).toBe(2);
        expect(worker.getRateLimiterInfo()?.current).toBe(2);
      });
    });

    test('close releases a manual lease waiting for rate admission', async () => {
      await withHarness(mode, 'manual-close-admission', async (harness) => {
        const queue = harness.queue('manual-close');
        let runs = 0;
        const worker = harness.worker(
          queue.name,
          async () => {
            runs++;
            return true;
          },
          {
            autorun: false,
            useLocks: false,
            limiter: { max: 1, duration: 60_000 },
          }
        );
        await worker.waitUntilReady();
        const firstJob = await queue.add('first', {}, { durable: true });
        const secondJob = await queue.add('second', {}, { durable: true });
        const first = await worker.getNextJob();
        if (!first) throw new Error('first manual close job was not pulled');
        await worker.processJobManually(first);
        await waitForState(queue, firstJob.id, 'completed', 10_000);

        const second = await worker.getNextJob();
        if (!second) throw new Error('second manual close job was not pulled');
        const waiting = worker.processJobManually(second);
        await Bun.sleep(50);
        expect(runs).toBe(1);

        await worker.close(true);
        expect(await waiting).toBeUndefined();
        await waitForState(queue, secondJob.id, 'waiting', 10_000);
        expect(runs).toBe(1);
      });
    });

    test('failed and force-abandoned jobs consume one start token each', async () => {
      await withHarness(mode, 'terminal-rate-admission', async (harness) => {
        const failedQueue = harness.queue('failed-rate');
        const failedWorker = harness.worker(
          failedQueue.name,
          async () => {
            throw new Error('expected failure');
          },
          { limiter: { max: 5, duration: 60_000 } }
        );
        const failed = await failedQueue.add('failed', {}, { attempts: 1, durable: true });
        await waitForState(failedQueue, failed.id, 'failed', 10_000);
        expect(failedWorker.getRateLimiterInfo()?.current).toBe(1);

        const heldQueue = harness.queue('held-rate');
        let returned = false;
        let release = (): void => undefined;
        const held = new Promise<void>((resolve) => {
          release = resolve;
        });
        const heldWorker = harness.worker(
          heldQueue.name,
          async () => {
            await held;
            returned = true;
            return true;
          },
          { limiter: { max: 5, duration: 60_000 } }
        );
        const activeJob = await heldQueue.add('held', {}, { durable: true });
        await waitForState(heldQueue, activeJob.id, 'active', 10_000);
        await heldWorker.close(true);
        expect(heldWorker.getRateLimiterInfo()?.current).toBe(1);

        release();
        await waitUntil(() => returned, 'the force-abandoned processor to return');
        await Bun.sleep(50);
        expect(heldWorker.getRateLimiterInfo()?.current).toBe(1);
      });
    });
  });
}
