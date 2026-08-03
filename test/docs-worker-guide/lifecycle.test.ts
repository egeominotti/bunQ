/**
 * Executable proof for /guide/worker/lifecycle/
 * ("Worker Lifecycle: Pause, Resume, Graceful Shutdown").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { closeSharedTcpClient, shutdownManager } from '../../src/client';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
  waitUntil,
} from '../docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

for (const mode of MODES) {
  describe(`worker guide · lifecycle [${mode}]`, () => {
    test('autorun: false keeps the worker idle until run() is called', async () => {
      harness = await startHarness('worker-lifecycle', mode);
      const queue = harness.queue('manual-start');
      let processed = 0;

      const worker = harness.worker(
        queue.name,
        async () => {
          processed++;
          return true;
        },
        { autorun: false }
      );
      expect(worker.isRunning()).toBe(false);

      const job = await queue.add('task', {}, { durable: true });
      await Bun.sleep(300);
      expect(processed).toBe(0);

      worker.run();
      expect(worker.isRunning()).toBe(true);
      await waitForState(queue, job.id, 'completed', 10_000);
      expect(processed).toBe(1);
    }, 20_000);

    test('pause stops pulling new jobs and resume restarts it', async () => {
      harness = await startHarness('worker-lifecycle', mode);
      const queue = harness.queue('pausing');
      let processed = 0;

      const worker = harness.worker(queue.name, async () => {
        processed++;
        return true;
      });
      await worker.waitUntilReady();

      worker.pause();
      expect(worker.isPaused()).toBe(true);
      const job = await queue.add('task', {}, { durable: true });
      await Bun.sleep(300);
      expect(processed).toBe(0);
      expect(await queue.getJobState(job.id)).toBe('waiting');

      worker.resume();
      expect(worker.isPaused()).toBe(false);
      await waitForState(queue, job.id, 'completed', 10_000);
      expect(processed).toBe(1);
    }, 20_000);

    test('close() waits for the in-flight job and acknowledges it', async () => {
      harness = await startHarness('worker-lifecycle', mode);
      const queue = harness.queue('graceful');
      let finished = false;
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      const worker = harness.worker(queue.name, async () => {
        await held;
        finished = true;
        return { ok: true };
      });
      const job = await queue.add('slow', {}, { durable: true });
      await waitForState(queue, job.id, 'active', 10_000);

      const closing = worker.close();
      await Bun.sleep(100);
      expect(finished).toBe(false); // still waiting for the active job

      release();
      await closing;

      expect(finished).toBe(true);
      expect(worker.isClosed()).toBe(true);
      await waitForState(queue, job.id, 'completed', 10_000);
    }, 30_000);

    test('close(true) stops immediately without waiting for the active job', async () => {
      harness = await startHarness('worker-lifecycle', mode);
      const queue = harness.queue('forced');
      const errors: Error[] = [];
      let processorReturned = false;
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      const worker = harness.worker(queue.name, async () => {
        await held;
        processorReturned = true;
        return true;
      });
      worker.on('error', (error) => errors.push(error));
      const job = await queue.add('slow', {}, { durable: true });
      await waitForState(queue, job.id, 'active', 10_000);

      const startedAt = Date.now();
      await worker.close(true);

      expect(Date.now() - startedAt).toBeLessThan(2_000);
      expect(worker.isClosed()).toBe(true);
      // The job is not lost: TCP releases it back to waiting on disconnect,
      // embedded leaves it active for stall recovery.
      expect(['active', 'waiting']).toContain(await queue.getJobState(job.id));
      release();
      await waitUntil(() => processorReturned, 'the abandoned processor to return');
      await Bun.sleep(100);

      expect(errors).toEqual([]);
      expect(['active', 'waiting']).toContain(await queue.getJobState(job.id));
    }, 30_000);

    test('a closed worker stops consuming', async () => {
      harness = await startHarness('worker-lifecycle', mode);
      const queue = harness.queue('after-close');
      let processed = 0;

      const worker = harness.worker(queue.name, async () => {
        processed++;
        return true;
      });
      await worker.close();

      const job = await queue.add('task', {}, { durable: true });
      await Bun.sleep(400);

      expect(processed).toBe(0);
      expect(await queue.getJobState(job.id)).toBe('waiting');
    }, 20_000);
  });
}

describe('worker guide · lifecycle [shutdown helpers]', () => {
  test('shutdownManager and closeSharedTcpClient are callable teardown hooks', async () => {
    harness = await startHarness('worker-lifecycle', 'embedded');
    const queue = harness.queue('shutdown');
    const worker = harness.worker(queue.name, async () => true);
    const job = await queue.add('task', {}, { durable: true });
    await waitForState(queue, job.id, 'completed', 10_000);

    await worker.close();
    expect(shutdownManager).toBeFunction();
    expect(closeSharedTcpClient).toBeFunction();
    expect(() => {
      shutdownManager();
      closeSharedTcpClient();
    }).not.toThrow();

    harness = null; // the manager is already down; nothing left to close
    await waitUntil(() => worker.isClosed(), 'the worker to report closed');
  }, 20_000);
});
