/**
 * Executable proof for /guide/worker/events/
 * ("Worker Events: completed, failed, stalled").
 *
 * Every row of the documented event table is observed with its exact callback
 * signature.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import type { Job } from '../../src/client';
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
  describe(`worker guide · events [${mode}]`, () => {
    test('ready, active, completed and drained fire with the documented payloads', async () => {
      harness = await startHarness('worker-events', mode);
      const queue = harness.queue<{ value: number }>('events');
      const seen: string[] = [];
      let activeJob: Job<{ value: number }> | null = null;
      let completed: { job: Job<{ value: number }>; result: unknown } | null = null;

      const worker = harness.worker<{ value: number }, { doubled: number }>(
        queue.name,
        async (job) => ({ doubled: job.data.value * 2 })
      );
      worker.on('ready', () => seen.push('ready'));
      worker.on('active', (job) => {
        seen.push('active');
        activeJob = job;
      });
      worker.on('completed', (job, result) => {
        seen.push('completed');
        completed = { job, result };
      });
      worker.on('drained', () => seen.push('drained'));

      const job = await queue.add('double', { value: 21 }, { durable: true });
      await waitForState(queue, job.id, 'completed', 10_000);
      await waitUntil(() => seen.includes('completed'), 'the completed event');

      expect(seen).toContain('ready');
      expect(seen).toContain('active');
      expect((activeJob as unknown as Job<{ value: number }>).id).toBe(job.id);
      const done = completed as unknown as { job: Job<{ value: number }>; result: unknown };
      expect(done.job.id).toBe(job.id);
      expect(done.result).toEqual({ doubled: 42 });
      await waitUntil(() => seen.includes('drained'), 'the drained event', 10_000);
    }, 30_000);

    test('failed hands the job and the Error to the listener', async () => {
      harness = await startHarness('worker-events', mode);
      const queue = harness.queue('failing');
      const failures: Array<{ id: string; message: string }> = [];

      const worker = harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      worker.on('failed', (job, error) => {
        failures.push({ id: job.id, message: error.message });
      });

      const job = await queue.add('doomed', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 15_000);
      await waitUntil(() => failures.length === 1, 'the failed event');

      expect(failures[0].id).toBe(job.id);
      expect(failures[0].message).toContain('boom');
    }, 30_000);

    test('broker timeouts suppress late contradictory terminal events', async () => {
      harness = await startHarness('worker-events', mode);
      const queue = harness.queue<{ fail: boolean }>('timeout-authority');
      const completed: string[] = [];
      const failed: string[] = [];
      const errors: Error[] = [];
      const worker = harness.worker<{ fail: boolean }, string>(
        queue.name,
        async (job) => {
          await Bun.sleep(500);
          if (job.data.fail) throw new Error('late processor failure');
          return 'late processor result';
        },
        { concurrency: 2, heartbeatInterval: 25 }
      );
      worker.on('completed', (job) => completed.push(job.id));
      worker.on('failed', (job) => failed.push(job.id));
      worker.on('error', (error) => errors.push(error));

      const success = await queue.add(
        'late-success',
        { fail: false },
        { attempts: 1, timeout: 100, durable: true }
      );
      const failure = await queue.add(
        'late-failure',
        { fail: true },
        { attempts: 1, timeout: 100, durable: true }
      );
      await Promise.all([
        waitForState(queue, success.id, 'failed', 10_000),
        waitForState(queue, failure.id, 'failed', 10_000),
      ]);
      await Bun.sleep(700);

      expect(completed).toEqual([]);
      expect(failed).toEqual([]);
      expect(errors).toEqual([]);
    }, 20_000);

    test('progress and log carry the job plus the reported value', async () => {
      harness = await startHarness('worker-events', mode);
      const queue = harness.queue('reporting');
      const progress: Array<{ id: string | null; value: number }> = [];
      const logs: Array<{ id: string; message: string }> = [];

      const worker = harness.worker(queue.name, async (job) => {
        await job.updateProgress(50);
        await job.log('step 1');
        return true;
      });
      worker.on('progress', (job, value) => {
        progress.push({ id: job?.id ?? null, value });
      });
      worker.on('log', (job, message) => {
        logs.push({ id: job.id, message });
      });

      const job = await queue.add('reporting', {}, { durable: true });
      await waitForState(queue, job.id, 'completed', 10_000);
      await waitUntil(() => progress.length > 0 && logs.length > 0, 'progress and log events');

      expect(progress[0]).toEqual({ id: job.id, value: 50 });
      expect(logs[0]).toEqual({ id: job.id, message: 'step 1' });
    }, 20_000);

    test('cancelled reports the job id and reason', async () => {
      harness = await startHarness('worker-events', mode);
      const queue = harness.queue('cancelling');
      const cancelled: Array<{ jobId: string; reason: string }> = [];
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      const worker = harness.worker(queue.name, async () => {
        await held;
        return true;
      });
      worker.on('cancelled', (payload) => cancelled.push(payload));
      worker.on('error', () => undefined);
      worker.on('failed', () => undefined);

      const job = await queue.add('cancel-me', {}, { durable: true });
      await waitForState(queue, job.id, 'active', 10_000);
      expect(worker.cancelJob(job.id, 'operator request')).toBe(true);

      await waitUntil(() => cancelled.length === 1, 'the cancelled event');
      expect(cancelled[0]).toEqual({ jobId: job.id, reason: 'operator request' });
      release();
    }, 20_000);

    test('closed fires when the worker shuts down', async () => {
      harness = await startHarness('worker-events', mode);
      const queue = harness.queue('closing');
      let closed = 0;

      const worker = harness.worker(queue.name, async () => true);
      worker.on('closed', () => closed++);
      await worker.close();

      await waitUntil(() => closed === 1, 'the closed event');
      expect(worker.isClosed()).toBe(true);
    }, 20_000);

    test('stalled is delivered in this mode with a job id and a reason', async () => {
      harness = await startHarness('worker-events', mode);
      const queue = harness.queue('stalling');
      const stalled: Array<{ jobId: string; reason: string }> = [];

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 20,
        maxStalls: 5,
        gracePeriod: 0,
      });
      const worker = harness.worker(
        queue.name,
        async () => await new Promise<never>(() => undefined),
        { heartbeatInterval: 0, useLocks: true }
      );
      worker.on('error', () => undefined);
      worker.on('stalled', (jobId, reason) => stalled.push({ jobId, reason }));

      const job = await queue.add('stall', {}, { durable: true });
      await waitUntil(
        () => stalled.some((entry) => entry.jobId === job.id),
        'the stalled event',
        30_000
      );
      expect(stalled[0].reason).toBeString();
    }, 60_000);

    test('off removes a listener', async () => {
      harness = await startHarness('worker-events', mode);
      const queue = harness.queue('unsubscribing');
      let calls = 0;
      const handler = (): void => {
        calls++;
      };

      const worker = harness.worker(queue.name, async () => true);
      worker.on('completed', handler);
      const first = await queue.add('a', {}, { durable: true });
      await waitForState(queue, first.id, 'completed', 10_000);
      await waitUntil(() => calls === 1, 'the first completed event');

      worker.off('completed', handler);
      const second = await queue.add('b', {}, { durable: true });
      await waitForState(queue, second.id, 'completed', 10_000);
      await Bun.sleep(200);

      expect(calls).toBe(1);
    }, 30_000);

    test('skipStalledCheck silences the listener without stopping recovery', async () => {
      harness = await startHarness('worker-events', mode);
      const queue = harness.queue('skip-stalled');
      const stalled: string[] = [];

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 20,
        maxStalls: 5,
        gracePeriod: 0,
      });
      const worker = harness.worker(
        queue.name,
        async () => await new Promise<never>(() => undefined),
        { heartbeatInterval: 0, useLocks: true, skipStalledCheck: true }
      );
      worker.on('error', () => undefined);
      worker.on('stalled', (jobId) => stalled.push(jobId));

      const job = await queue.add('stall', {}, { durable: true });
      await waitForState(queue, job.id, 'active', 10_000);

      // Broker-side recovery still runs: the job leaves the active state.
      await waitUntil(
        async () => (await queue.getJobState(job.id)) !== 'active',
        'broker-side stall recovery',
        30_000
      );
      expect(stalled).toEqual([]);
    }, 60_000);
  });
}
