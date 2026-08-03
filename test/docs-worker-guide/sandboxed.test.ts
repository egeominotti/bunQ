/**
 * Executable proof for /guide/worker/sandboxed/
 * ("SandboxedWorker: Isolated Job Processing").
 *
 * The page documents an experimental Bun-only feature; every example here runs
 * a real Bun Worker thread against a real broker.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SandboxedWorker } from '../../src/client';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
  waitUntil,
} from '../docs-guide-support';

let harness: CoreE2eHarness | null = null;
const scratchDirs: string[] = [];
const workers: SandboxedWorker[] = [];

afterEach(async () => {
  for (const worker of workers.splice(0)) await worker.stop(true).catch(() => undefined);
  await closeHarness(harness);
  harness = null;
  for (const dir of scratchDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

/** Write a processor module exactly in the documented shape. */
function processorFile(source: string): string {
  const dir = mkdtempSync(join(tmpdir(), 'bunqueue-docs-sandboxed-'));
  scratchDirs.push(dir);
  const path = join(dir, 'processor.ts');
  writeFileSync(path, source);
  return path;
}

function track(worker: SandboxedWorker): SandboxedWorker {
  workers.push(worker);
  return worker;
}

const DOUBLING_PROCESSOR = `
export default async (job: {
  id: string;
  data: any;
  queue: string;
  attempts: number;
  parentId?: string;
  progress: (value: number) => void;
  log: (message: string) => void;
  fail: (error: string | Error) => void;
}) => {
  job.progress(50);
  job.log('halfway');
  const doubled = job.data.value * 2;
  job.progress(100);
  return { doubled, seen: { id: job.id, queue: job.queue, attempts: job.attempts } };
};
`;

for (const mode of MODES) {
  describe(`worker guide · SandboxedWorker [${mode}]`, () => {
    test('a processor module runs in a Bun Worker thread and returns its result', async () => {
      harness = await startHarness('sandboxed', mode);
      const queue = harness.queue<{ value: number }>('cpu-intensive');
      const results: unknown[] = [];

      const worker = track(
        new SandboxedWorker(queue.name, {
          processor: processorFile(DOUBLING_PROCESSOR),
          concurrency: 2,
          timeout: 60_000,
          maxMemory: 256,
          ...(harness.mode === 'tcp' ? { connection: harness.connection() } : {}),
        })
      );
      worker.on('error', () => undefined);
      worker.on('completed', (_job, result) => results.push(result));
      await worker.start();

      const job = await queue.add('double', { value: 21 }, { durable: true });
      await waitUntil(() => results.length === 1, 'the sandboxed result', 30_000);

      expect(results[0]).toMatchObject({ doubled: 42 });
      expect((results[0] as { seen: { id: string } }).seen.id).toBe(job.id);
      await waitForState(queue, job.id, 'completed', 20_000);
      expect((await queue.getJob(job.id))?.returnvalue).toMatchObject({ doubled: 42 });
      expect((await queue.getJob(job.id))?.toJSON().returnvalue).toMatchObject({ doubled: 42 });
    }, 60_000);

    test('start, isRunning, getStats and stop follow the documented lifecycle', async () => {
      harness = await startHarness('sandboxed', mode);
      const queue = harness.queue('cpu-intensive');

      const worker = track(
        new SandboxedWorker(queue.name, {
          processor: processorFile(DOUBLING_PROCESSOR),
          concurrency: 3,
          ...(harness.mode === 'tcp' ? { connection: harness.connection() } : {}),
        })
      );
      worker.on('error', () => undefined);
      expect(worker.isRunning()).toBe(false);

      await worker.start();
      expect(worker.isRunning()).toBe(true);

      const stats = worker.getStats();
      expect(Object.keys(stats).sort()).toEqual(['busy', 'idle', 'recycled', 'restarts', 'total']);
      expect(stats.total).toBe(3);
      expect(stats.idle).toBe(3);
      expect(stats.busy).toBe(0);

      await worker.stop();
      expect(worker.isRunning()).toBe(false);
      expect(worker.getStats().total).toBe(0);
    }, 60_000);

    test('progress and log from the thread reach the broker', async () => {
      harness = await startHarness('sandboxed', mode);
      const queue = harness.queue<{ value: number }>('cpu-intensive');
      const progress: number[] = [];
      const logs: string[] = [];

      const worker = track(
        new SandboxedWorker(queue.name, {
          processor: processorFile(DOUBLING_PROCESSOR),
          ...(harness.mode === 'tcp' ? { connection: harness.connection() } : {}),
        })
      );
      worker.on('error', () => undefined);
      worker.on('progress', (_job, value) => progress.push(value));
      worker.on('log', (_job, message) => logs.push(message));
      await worker.start();

      const job = await queue.add('double', { value: 3 }, { durable: true });
      await waitForState(queue, job.id, 'completed', 30_000);
      await waitUntil(() => progress.length > 0 && logs.length > 0, 'progress and log events');

      expect(progress).toContain(50);
      expect(logs).toContain('halfway');
    }, 60_000);

    test('job.fail from the thread fails the job', async () => {
      harness = await startHarness('sandboxed', mode);
      const queue = harness.queue('cpu-intensive');
      const failures: string[] = [];

      const worker = track(
        new SandboxedWorker(queue.name, {
          processor: processorFile(`
export default async (job: { fail: (error: string) => void }) => {
  job.fail('rejected by the processor');
  return null;
};
`),
          ...(harness.mode === 'tcp' ? { connection: harness.connection() } : {}),
        })
      );
      worker.on('error', () => undefined);
      worker.on('failed', (_job, error) => failures.push(error.message));
      await worker.start();

      const job = await queue.add('doomed', {}, { attempts: 1, durable: true });
      await waitUntil(() => failures.length === 1, 'the sandboxed failure', 30_000);

      expect(failures[0]).toContain('rejected by the processor');
      await waitForState(queue, job.id, 'failed', 20_000);
    }, 60_000);

    test('the per-job timeout terminates a stuck thread', async () => {
      harness = await startHarness('sandboxed', mode);
      const queue = harness.queue('cpu-intensive');
      const failures: string[] = [];

      const worker = track(
        new SandboxedWorker(queue.name, {
          processor: processorFile(`
export default async () => {
  await new Promise(() => undefined);
};
`),
          timeout: 500,
          autoRestart: true,
          ...(harness.mode === 'tcp' ? { connection: harness.connection() } : {}),
        })
      );
      worker.on('error', () => undefined);
      worker.on('failed', (_job, error) => failures.push(error.message));
      await worker.start();

      await queue.add('stuck', {}, { attempts: 1, durable: true });
      await waitUntil(() => failures.length >= 1, 'the timeout failure', 30_000);

      expect(failures[0]).toMatch(/timed out|timeout/i);
    }, 60_000);

    test('it emits the eight documented events and none of the other three', async () => {
      harness = await startHarness('sandboxed', mode);
      const queue = harness.queue<{ value: number }>('cpu-intensive');
      const seen = new Set<string>();

      const worker = track(
        new SandboxedWorker(queue.name, {
          processor: processorFile(DOUBLING_PROCESSOR),
          ...(harness.mode === 'tcp' ? { connection: harness.connection() } : {}),
        })
      );
      for (const event of ['ready', 'active', 'completed', 'failed', 'progress', 'log', 'error']) {
        worker.on(event as 'ready', () => seen.add(event));
      }
      worker.on('closed', () => seen.add('closed'));
      for (const absent of ['stalled', 'drained', 'cancelled']) {
        worker.on(absent as 'ready', () => seen.add(absent));
      }

      await worker.start();
      const job = await queue.add('double', { value: 2 }, { durable: true });
      await waitForState(queue, job.id, 'completed', 30_000);
      await worker.stop();

      expect(seen.has('ready')).toBe(true);
      expect(seen.has('active')).toBe(true);
      expect(seen.has('completed')).toBe(true);
      expect(seen.has('progress')).toBe(true);
      expect(seen.has('log')).toBe(true);
      expect(seen.has('closed')).toBe(true);
      expect(seen.has('stalled')).toBe(false);
      expect(seen.has('drained')).toBe(false);
      expect(seen.has('cancelled')).toBe(false);
    }, 60_000);
  });
}
