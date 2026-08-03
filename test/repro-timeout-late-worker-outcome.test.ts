/**
 * Regression: a processor result that arrives after the broker has already
 * finalized the job as timed out must not be reported as a completion or as a
 * transport/ACK error. The broker timeout remains the authoritative outcome in
 * both embedded and TCP mode.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { SandboxedWorker } from '../src/client';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
} from './docs-guide-support';

let harness: CoreE2eHarness | null = null;
let sandboxedWorker: SandboxedWorker | null = null;

afterEach(async () => {
  await sandboxedWorker?.stop(true).catch(() => undefined);
  sandboxedWorker = null;
  await closeHarness(harness);
  harness = null;
});

for (const mode of MODES) {
  describe(`late worker outcome after timeout [${mode}]`, () => {
    test('keeps the timeout authoritative without false completion or ACK error', async () => {
      harness = await startHarness('timeout-late-outcome', mode);
      const queue = harness.queue('jobs');
      const completed: string[] = [];
      const errors: Error[] = [];
      let finishHandler!: () => void;
      const handlerFinished = new Promise<void>((resolve) => {
        finishHandler = resolve;
      });

      const worker = harness.worker(queue.name, async (job) => {
        await Bun.sleep(250);
        finishHandler();
        return { late: job.id };
      });
      worker.on('completed', (job) => completed.push(job.id));
      worker.on('error', (error) => errors.push(error));

      const job = await queue.add(
        'slow',
        { value: 1 },
        { timeout: 60, attempts: 1, durable: true }
      );

      await waitForState(queue, job.id, 'failed', 5_000);
      await handlerFinished;
      // The TCP ACK batcher used to retry this authoritative rejection for
      // 100 + 200 + 400 ms before surfacing an `ack-stale` error.
      await Bun.sleep(900);

      expect(await queue.getJobState(job.id)).toBe('failed');
      expect((await queue.getJob(job.id))?.failedReason).toMatch(/timeout/i);
      expect(completed).toEqual([]);
      expect(errors).toEqual([]);
      expect(await queue.getCompletedCount()).toBe(0);
      expect(await queue.getFailedCount()).toBe(1);
    }, 15_000);

    test('keeps the same authority for a late SandboxedWorker result', async () => {
      harness = await startHarness('timeout-late-sandboxed-outcome', mode);
      const queue = harness.queue('jobs');
      const processorPath = join(harness.dataDir, 'late-processor.ts');
      writeFileSync(
        processorPath,
        `export default async (job: { id: string }) => {
  await Bun.sleep(250);
  return { late: job.id };
};\n`
      );
      const completed: string[] = [];
      const errors: Error[] = [];
      sandboxedWorker = new SandboxedWorker(queue.name, {
        processor: processorPath,
        timeout: 5_000,
        ...(harness.mode === 'tcp' ? { connection: harness.connection() } : {}),
      });
      sandboxedWorker.on('completed', (job) => completed.push(job.id));
      sandboxedWorker.on('error', (error) => errors.push(error));
      await sandboxedWorker.start();

      const job = await queue.add(
        'slow-sandboxed',
        { value: 1 },
        { timeout: 60, attempts: 1, durable: true }
      );

      await waitForState(queue, job.id, 'failed', 5_000);
      await Bun.sleep(900);

      expect(await queue.getJobState(job.id)).toBe('failed');
      expect((await queue.getJob(job.id))?.failedReason).toMatch(/timeout/i);
      expect(completed).toEqual([]);
      expect(errors).toEqual([]);
      expect(await queue.getCompletedCount()).toBe(0);
      expect(await queue.getFailedCount()).toBe(1);
    }, 15_000);

    test('suppresses a processor failure that arrives after the broker timeout', async () => {
      harness = await startHarness('timeout-late-failure', mode);
      const queue = harness.queue('jobs');
      const completed: string[] = [];
      const failed: string[] = [];
      const errors: Error[] = [];
      let finishHandler!: () => void;
      const handlerFinished = new Promise<void>((resolve) => {
        finishHandler = resolve;
      });
      const worker = harness.worker(queue.name, async () => {
        await Bun.sleep(250);
        finishHandler();
        throw new Error('late processor failure');
      });
      worker.on('completed', (job) => completed.push(job.id));
      worker.on('failed', (job) => failed.push(job.id));
      worker.on('error', (error) => errors.push(error));

      const job = await queue.add('slow-failure', {}, { timeout: 60, attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 5_000);
      await handlerFinished;
      await Bun.sleep(300);

      expect((await queue.getJob(job.id))?.failedReason).toMatch(/timeout/i);
      expect(completed).toEqual([]);
      expect(failed).toEqual([]);
      expect(errors).toEqual([]);
      expect(await queue.getCompletedCount()).toBe(0);
      expect(await queue.getFailedCount()).toBe(1);
    }, 15_000);

    test('suppresses a sandboxed failure that arrives after the broker timeout', async () => {
      harness = await startHarness('timeout-late-sandboxed-failure', mode);
      const queue = harness.queue('jobs');
      const processorPath = join(harness.dataDir, 'late-failing-processor.ts');
      writeFileSync(
        processorPath,
        `export default async () => {
  await Bun.sleep(250);
  throw new Error('late sandboxed failure');
};\n`
      );
      const completed: string[] = [];
      const failed: string[] = [];
      const errors: Error[] = [];
      sandboxedWorker = new SandboxedWorker(queue.name, {
        processor: processorPath,
        timeout: 5_000,
        ...(harness.mode === 'tcp' ? { connection: harness.connection() } : {}),
      });
      sandboxedWorker.on('completed', (job) => completed.push(job.id));
      sandboxedWorker.on('failed', (job) => failed.push(job.id));
      sandboxedWorker.on('error', (error) => errors.push(error));
      await sandboxedWorker.start();

      const job = await queue.add(
        'slow-sandboxed-failure',
        {},
        { timeout: 60, attempts: 1, durable: true }
      );
      await waitForState(queue, job.id, 'failed', 5_000);
      await Bun.sleep(600);

      expect((await queue.getJob(job.id))?.failedReason).toMatch(/timeout/i);
      expect(completed).toEqual([]);
      expect(failed).toEqual([]);
      expect(errors).toEqual([]);
      expect(await queue.getCompletedCount()).toBe(0);
      expect(await queue.getFailedCount()).toBe(1);
    }, 15_000);

    test('suppresses a late manual moveToFailed transition', async () => {
      harness = await startHarness('timeout-late-manual-failure', mode);
      const queue = harness.queue('jobs');
      const failed: string[] = [];
      const completed: string[] = [];
      const errors: Error[] = [];
      const worker = harness.worker(queue.name, async (job) => {
        await Bun.sleep(250);
        await job.moveToFailed(new Error('late manual failure'));
        return 'ignored';
      });
      worker.on('failed', (job) => failed.push(job.id));
      worker.on('completed', (job) => completed.push(job.id));
      worker.on('error', (error) => errors.push(error));

      const job = await queue.add(
        'manual-failure',
        {},
        { timeout: 60, attempts: 1, durable: true }
      );
      await waitForState(queue, job.id, 'failed', 5_000);
      await Bun.sleep(600);

      expect((await queue.getJob(job.id))?.failedReason).toMatch(/timeout/i);
      expect(failed).toEqual([]);
      expect(completed).toEqual([]);
      expect(errors).toEqual([]);
    }, 15_000);

    test('suppresses a local sandbox timeout after the broker timeout won', async () => {
      harness = await startHarness('timeout-late-local-sandbox-timeout', mode);
      const queue = harness.queue('jobs');
      const processorPath = join(harness.dataDir, 'never-finishing-processor.ts');
      writeFileSync(
        processorPath,
        `export default async () => {
  await new Promise(() => undefined);
};\n`
      );
      const failed: string[] = [];
      const completed: string[] = [];
      const errors: Error[] = [];
      sandboxedWorker = new SandboxedWorker(queue.name, {
        processor: processorPath,
        timeout: 250,
        ...(harness.mode === 'tcp' ? { connection: harness.connection() } : {}),
      });
      sandboxedWorker.on('failed', (job) => failed.push(job.id));
      sandboxedWorker.on('completed', (job) => completed.push(job.id));
      sandboxedWorker.on('error', (error) => errors.push(error));
      await sandboxedWorker.start();

      const job = await queue.add(
        'never-finishes',
        {},
        { timeout: 60, attempts: 1, durable: true }
      );
      await waitForState(queue, job.id, 'failed', 5_000);
      await Bun.sleep(700);

      expect((await queue.getJob(job.id))?.failedReason).toMatch(/timeout/i);
      expect(failed).toEqual([]);
      expect(completed).toEqual([]);
      expect(errors).toEqual([]);
    }, 15_000);
  });
}
