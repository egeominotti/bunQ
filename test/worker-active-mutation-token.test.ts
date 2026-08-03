import { afterEach, describe, expect, test } from 'bun:test';
import type { Worker } from '../src/client';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
  waitUntil,
} from './docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

function deferred(): {
  promise: Promise<void>;
  resolve: () => void;
  reject: (error: unknown) => void;
} {
  let resolve!: () => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<void>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function collectWorkerSignals(worker: Worker): {
  errors: Error[];
  completed: string[];
  failed: string[];
} {
  const errors: Error[] = [];
  const completed: string[] = [];
  const failed: string[] = [];
  worker.on('error', (error) => errors.push(error));
  worker.on('completed', (job) => completed.push(job.id));
  worker.on('failed', (job) => failed.push(job.id));
  return { errors, completed, failed };
}

for (const mode of MODES) {
  describe(`worker-owned active mutations [${mode}]`, () => {
    test('Job.changeDelay forwards the current lease and persists the delayed transition', async () => {
      harness = await startHarness('worker-change-delay-token', mode);
      const queue = harness.queue(`change-delay-${mode}`);
      const changed = deferred();
      const worker = harness.worker(
        queue.name,
        async (job) => {
          try {
            expect(job.token).toBeString();
            await job.changeDelay(60_000);
            changed.resolve();
          } catch (error) {
            changed.reject(error);
            throw error;
          }
          return 'outcome-must-not-complete-a-delayed-job';
        },
        { autorun: false, concurrency: 1, lockDuration: 60_000 }
      );
      const signals = collectWorkerSignals(worker);

      const job = await queue.add('change-delay', {}, { durable: true });
      worker.run();

      await expect(changed.promise).resolves.toBeUndefined();
      await waitForState(queue, job.id, 'delayed');
      expect(harness.brokerManager().getLockInfo(job.id)).toBeNull();
      expect((await queue.getJob(job.id))?.delay).toBeGreaterThan(0);
      await worker.close();
      expect(signals).toEqual({ errors: [], completed: [], failed: [] });
      expect(await queue.getJobCountsAsync()).toMatchObject({ completed: 0, failed: 0 });
    }, 20_000);

    test('Job.retry forwards the current lease when moving an active job back to waiting', async () => {
      harness = await startHarness('worker-retry-token', mode);
      const queue = harness.queue(`retry-${mode}`);
      const retried = deferred();
      let worker!: Worker;
      worker = harness.worker(
        queue.name,
        async (job) => {
          try {
            expect(job.token).toBeString();
            await job.retry();
            worker.pause();
            retried.resolve();
          } catch (error) {
            retried.reject(error);
            throw error;
          }
          return 'outcome-must-not-complete-a-retried-job';
        },
        { autorun: false, concurrency: 1, lockDuration: 60_000 }
      );
      const signals = collectWorkerSignals(worker);

      const job = await queue.add('retry', {}, { durable: true });
      worker.run();

      await expect(retried.promise).resolves.toBeUndefined();
      await waitForState(queue, job.id, 'waiting');
      expect(harness.brokerManager().getLockInfo(job.id)).toBeNull();
      expect(await queue.getJobState(job.id)).not.toBe('completed');
      await worker.close();
      expect(signals).toEqual({ errors: [], completed: [], failed: [] });
      expect(await queue.getJobCountsAsync()).toMatchObject({ completed: 0, failed: 0 });
    }, 20_000);

    test('Job.moveToWait consumes the active generation without a terminal outcome', async () => {
      harness = await startHarness('worker-move-to-wait-token', mode);
      const queue = harness.queue(`move-to-wait-${mode}`);
      const transitioned = deferred();
      let worker!: Worker;
      worker = harness.worker(
        queue.name,
        async (job) => {
          expect(job.token).toBeString();
          expect(await job.moveToWait(job.token)).toBe(true);
          worker.pause();
          transitioned.resolve();
          return 'outcome-must-not-complete-a-waiting-job';
        },
        { autorun: false, concurrency: 1, lockDuration: 60_000 }
      );
      const signals = collectWorkerSignals(worker);

      const job = await queue.add('move-to-wait', {}, { durable: true });
      worker.run();

      await transitioned.promise;
      await waitForState(queue, job.id, 'waiting');
      expect(harness.brokerManager().getLockInfo(job.id)).toBeNull();
      await worker.close();
      expect(signals).toEqual({ errors: [], completed: [], failed: [] });
      expect(await queue.getJobCountsAsync()).toMatchObject({ completed: 0, failed: 0 });
    }, 20_000);

    test('Job.moveToDelayed consumes the active generation without a terminal outcome', async () => {
      harness = await startHarness('worker-move-to-delayed-token', mode);
      const queue = harness.queue(`move-to-delayed-${mode}`);
      const transitioned = deferred();
      const worker = harness.worker(
        queue.name,
        async (job) => {
          expect(job.token).toBeString();
          await job.moveToDelayed(Date.now() + 60_000, job.token);
          transitioned.resolve();
          return 'outcome-must-not-complete-a-delayed-job';
        },
        { autorun: false, concurrency: 1, lockDuration: 60_000 }
      );
      const signals = collectWorkerSignals(worker);

      const job = await queue.add('move-to-delayed', {}, { durable: true });
      worker.run();

      await transitioned.promise;
      await waitForState(queue, job.id, 'delayed');
      expect(harness.brokerManager().getLockInfo(job.id)).toBeNull();
      await worker.close();
      expect(signals).toEqual({ errors: [], completed: [], failed: [] });
      expect(await queue.getJobCountsAsync()).toMatchObject({ completed: 0, failed: 0 });
    }, 20_000);

    test('Job.moveToWaitingChildren consumes the active generation without a terminal outcome', async () => {
      harness = await startHarness('worker-move-to-waiting-children-token', mode);
      const queue = harness.queue(`move-to-waiting-children-${mode}`);
      const transitioned = deferred();
      const worker = harness.worker(
        queue.name,
        async (job) => {
          expect(job.token).toBeString();
          expect(await job.moveToWaitingChildren(job.token)).toBe(true);
          transitioned.resolve();
          return 'outcome-must-not-complete-a-waiting-children-job';
        },
        { autorun: false, concurrency: 1, lockDuration: 60_000 }
      );
      const signals = collectWorkerSignals(worker);

      const job = await queue.add('move-to-waiting-children', {}, { durable: true });
      worker.run();

      await transitioned.promise;
      await waitForState(queue, job.id, 'waiting-children');
      expect(harness.brokerManager().getLockInfo(job.id)).toBeNull();
      await worker.close();
      expect(signals).toEqual({ errors: [], completed: [], failed: [] });
      expect(await queue.getJobCountsAsync()).toMatchObject({ completed: 0, failed: 0 });
    }, 20_000);

    test('a processor exception after an applied transition cannot fail the new generation', async () => {
      harness = await startHarness('worker-transition-then-throw', mode);
      const queue = harness.queue(`transition-then-throw-${mode}`);
      const worker = harness.worker(
        queue.name,
        async (job) => {
          await job.changeDelay(60_000);
          throw new Error('must not overwrite the applied delayed transition');
        },
        { concurrency: 1, lockDuration: 60_000 }
      );
      const signals = collectWorkerSignals(worker);

      const job = await queue.add('transition-then-throw', {}, { durable: true });
      await waitForState(queue, job.id, 'delayed');
      await worker.close();
      expect(signals).toEqual({ errors: [], completed: [], failed: [] });
      expect(await queue.getJobCountsAsync()).toMatchObject({ completed: 0, failed: 0 });
    }, 20_000);

    test('a rejected token does not claim the outcome before normal failure handling', async () => {
      harness = await startHarness('worker-rejected-transition-token', mode);
      const queue = harness.queue(`rejected-transition-token-${mode}`);
      const worker = harness.worker(
        queue.name,
        async (job) => {
          await job.moveToWait('wrong-token');
          return 'unreachable';
        },
        { concurrency: 1, lockDuration: 60_000 }
      );
      const signals = collectWorkerSignals(worker);

      const job = await queue.add('rejected-transition-token', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed');
      await waitUntil(() => signals.failed.length === 1, 'worker failed event');
      await worker.close();
      expect(signals.errors).toEqual([]);
      expect(signals.completed).toEqual([]);
      expect(signals.failed).toEqual([job.id]);
      expect(await queue.getJobCountsAsync()).toMatchObject({ completed: 0, failed: 1 });
    }, 20_000);

    test('Job.discard wins before the processor return can complete the same generation', async () => {
      harness = await startHarness('worker-discard-generation', mode);
      const queue = harness.queue(`discard-generation-${mode}`);
      const worker = harness.worker(
        queue.name,
        (job) => {
          job.discard();
          return 'outcome-must-not-complete-a-discarded-job';
        },
        { concurrency: 1, lockDuration: 60_000 }
      );
      const signals = collectWorkerSignals(worker);

      const job = await queue.add('discard-generation', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed');
      await worker.close();
      expect(signals).toEqual({ errors: [], completed: [], failed: [] });
      expect(await queue.getJobCountsAsync()).toMatchObject({ completed: 0, failed: 1 });
    }, 20_000);

    test('a processor exception after Job.discard cannot fail or complete the retired generation', async () => {
      harness = await startHarness('worker-discard-then-throw', mode);
      const queue = harness.queue(`discard-then-throw-${mode}`);
      const worker = harness.worker(
        queue.name,
        (job) => {
          job.discard();
          throw new Error('must not replace the explicit discard outcome');
        },
        { concurrency: 1, lockDuration: 60_000 }
      );
      const signals = collectWorkerSignals(worker);

      const job = await queue.add('discard-then-throw', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed');
      await worker.close();
      expect(signals).toEqual({ errors: [], completed: [], failed: [] });
      expect(await queue.getJobCountsAsync()).toMatchObject({ completed: 0, failed: 1 });
    }, 20_000);

    test('graceful close waits for the fire-and-forget discard settlement', async () => {
      harness = await startHarness('worker-discard-close', mode);
      const queue = harness.queue(`discard-close-${mode}`);
      const manager = harness.brokerManager();
      const originalDiscard = manager.discard.bind(manager);
      const entered = deferred();
      const release = deferred();
      manager.discard = async (id, token) => {
        entered.resolve();
        await release.promise;
        return originalDiscard(id, token);
      };
      const worker = harness.worker(
        queue.name,
        (job) => {
          job.discard();
          return 'must-wait-for-discard';
        },
        { concurrency: 1, lockDuration: 60_000 }
      );
      const signals = collectWorkerSignals(worker);

      const job = await queue.add('discard-close', {}, { attempts: 1, durable: true });
      await entered.promise;
      let closed = false;
      const closing = worker.close().then(() => {
        closed = true;
      });
      try {
        await Bun.sleep(75);
        expect(closed).toBe(false);
      } finally {
        release.resolve();
      }
      await closing;
      manager.discard = originalDiscard;
      expect(await queue.getJobState(job.id)).toBe('failed');
      expect(signals).toEqual({ errors: [], completed: [], failed: [] });
    }, 20_000);

    test('duplicate Job.discard calls share one broker transition', async () => {
      harness = await startHarness('worker-discard-duplicate', mode);
      const queue = harness.queue(`discard-duplicate-${mode}`);
      const manager = harness.brokerManager();
      const originalDiscard = manager.discard.bind(manager);
      let discardCalls = 0;
      manager.discard = async (id, token) => {
        discardCalls++;
        return originalDiscard(id, token);
      };
      const worker = harness.worker(
        queue.name,
        (job) => {
          job.discard();
          job.discard();
          return 'must-not-ack';
        },
        { concurrency: 1, lockDuration: 60_000 }
      );
      const signals = collectWorkerSignals(worker);

      const job = await queue.add('discard-duplicate', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed');
      await worker.close();
      manager.discard = originalDiscard;
      expect(discardCalls).toBe(1);
      expect(signals).toEqual({ errors: [], completed: [], failed: [] });
    }, 20_000);

    test('an authoritative discard no-op suppresses automatic terminal commands without noise', async () => {
      harness = await startHarness('worker-discard-ignored', mode);
      const queue = harness.queue(`discard-ignored-${mode}`);
      const manager = harness.brokerManager();
      const originalDiscard = manager.discard.bind(manager);
      manager.discard = async () => false;
      const worker = harness.worker(
        queue.name,
        (job) => {
          job.discard();
          return 'must-not-ack';
        },
        { concurrency: 1, lockDuration: 60_000 }
      );
      const signals = collectWorkerSignals(worker);

      const job = await queue.add('discard-ignored', {}, { attempts: 1, durable: true });
      await worker.close();
      manager.discard = originalDiscard;
      expect(await queue.getJobState(job.id)).not.toBe('completed');
      expect(await queue.getJobState(job.id)).not.toBe('failed');
      expect(signals).toEqual({ errors: [], completed: [], failed: [] });
      expect(await queue.getJobCountsAsync()).toMatchObject({ completed: 0, failed: 0 });
    }, 20_000);

    test('a discard transport or engine rejection emits one Worker error and no terminal event', async () => {
      harness = await startHarness('worker-discard-error', mode);
      const queue = harness.queue(`discard-error-${mode}`);
      const manager = harness.brokerManager();
      const originalDiscard = manager.discard.bind(manager);
      manager.discard = async () => {
        throw new Error('injected discard rejection');
      };
      const worker = harness.worker(
        queue.name,
        (job) => {
          job.discard();
          return 'must-not-ack';
        },
        { concurrency: 1, lockDuration: 60_000 }
      );
      const signals = collectWorkerSignals(worker);

      const job = await queue.add('discard-error', {}, { attempts: 1, durable: true });
      await waitUntil(() => signals.errors.length === 1, 'discard Worker error');
      await worker.close();
      manager.discard = originalDiscard;
      expect(signals.completed).toEqual([]);
      expect(signals.failed).toEqual([]);
      expect(signals.errors).toHaveLength(1);
      expect(signals.errors[0]).toMatchObject({
        message: 'injected discard rejection',
        context: 'discard',
        jobId: job.id,
      });
      expect(await queue.getJobCountsAsync()).toMatchObject({ completed: 0, failed: 0 });
    }, 20_000);
  });
}
