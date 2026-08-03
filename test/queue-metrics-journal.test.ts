import { afterEach, describe, expect, setSystemTime, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Queue } from '../src/client';
import { EventType } from '../src/domain/types/queue';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
} from './docs-guide-support';

const MINUTE = 60_000;

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  setSystemTime();
  await closeHarness(harness);
  harness = null;
});

function reopenQueue<T>(active: CoreE2eHarness, name: string): Queue<T> {
  const queue = new Queue<T>(name, active.queueOptions());
  active.addCleanup(() => queue.close());
  return queue;
}

async function waitForStateOnRealClock(
  queue: Queue<unknown>,
  id: string,
  expected: string,
  timeoutMs = 10_000
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  let state = 'unknown';
  while (performance.now() < deadline) {
    state = await queue.getJobState(id);
    if (state === expected) return;
    await Bun.sleep(5);
  }
  throw new Error(`timed out waiting for ${id} to reach '${expected}' (last '${state}')`);
}

for (const mode of MODES) {
  describe(`queue metrics journal [${mode}]`, () => {
    test('keeps completed and failed minute buckets newest-first with inclusive pagination', async () => {
      harness = await startHarness('metrics-journal-series', mode);
      const queue = harness.queue<{ minute: number; fail?: boolean }>('primary');
      const other = harness.queue<{ minute: number }>('other');
      const epoch = Math.floor(Date.now() / MINUTE) * MINUTE;
      setSystemTime(new Date(epoch));

      harness.worker(queue.name, async (job) => {
        setSystemTime(new Date(epoch + job.data.minute * MINUTE + 1_000));
        if (job.data.fail) throw new Error('expected failure');
        return true;
      });
      harness.worker(other.name, async (job) => {
        setSystemTime(new Date(epoch + job.data.minute * MINUTE + 1_000));
        return true;
      });

      for (const minute of [0, 0, 1]) {
        const job = await queue.add(`completed-${minute}`, { minute }, { durable: true });
        await waitForStateOnRealClock(queue, job.id, 'completed');
      }
      for (const minute of [2, 2]) {
        const job = await queue.add(
          `failed-${minute}`,
          { minute, fail: true },
          { attempts: 1, durable: true }
        );
        await waitForStateOnRealClock(queue, job.id, 'failed');
      }
      const last = await queue.add('completed-3', { minute: 3 }, { durable: true });
      await waitForStateOnRealClock(queue, last.id, 'completed');
      const otherJob = await other.add('other', { minute: 4 }, { durable: true });
      await waitForStateOnRealClock(other, otherJob.id, 'completed');

      const all = await queue.getMetrics('completed', 0, -1);
      expect(all.meta.count).toBe(4);
      expect(all.count).toBe(4);
      expect(all.data).toEqual([1, 0, 1, 2]);

      const firstPage = await queue.getMetrics('completed', 0, 1);
      expect(firstPage.meta.count).toBe(4);
      expect(firstPage.count).toBe(4);
      expect(firstPage.data).toEqual([1, 0]);
      expect((await queue.getMetrics('completed', 2, -1)).data).toEqual([1, 2]);

      const failed = await queue.getMetrics('failed', 0, -1);
      expect(failed.meta.count).toBe(2);
      expect(failed.count).toBe(1);
      expect(failed.data).toEqual([2]);

      const isolated = await other.getMetrics('completed', 0, -1);
      expect(isolated.meta.count).toBe(1);
      expect(isolated.data).toEqual([1]);
    }, 40_000);

    test('trims only the selected queue and is idempotent after concurrent activity', async () => {
      harness = await startHarness('metrics-journal-trim', mode);
      const queue = harness.queue<{ index: number }>('primary');
      const other = harness.queue('other');
      harness.worker(queue.name, async () => true, { concurrency: 8 });
      harness.worker(other.name, async () => true);

      const jobs = await queue.addBulk(
        Array.from({ length: 32 }, (_, index) => ({
          name: `job-${index}`,
          data: { index },
          opts: { durable: true },
        }))
      );
      const otherJob = await other.add('other', {}, { durable: true });
      await Promise.all(jobs.map((job) => waitForState(queue, job.id, 'completed', 15_000)));
      await waitForState(other, otherJob.id, 'completed', 10_000);

      const metrics = await queue.getMetrics('completed', 0, -1);
      expect(metrics.meta.count).toBe(32);
      expect(metrics.data.reduce((sum, value) => sum + value, 0)).toBe(32);

      expect(await queue.trimEvents(5)).toBeGreaterThan(0);
      expect(await queue.trimEvents(5)).toBe(0);
      expect(await other.trimEvents(0)).toBeGreaterThan(0);
      expect(await queue.trimEvents(5)).toBe(0);
    }, 40_000);

    test('does not count a retry-attempt failure as a terminal failed job', async () => {
      harness = await startHarness('metrics-journal-retry', mode);
      const queue = harness.queue('retry');
      let attempts = 0;
      harness.worker(queue.name, async () => {
        attempts++;
        if (attempts === 1) throw new Error('retry me');
        return true;
      });

      const job = await queue.add('retry', {}, { attempts: 2, backoff: 1, durable: true });
      await waitForState(queue, job.id, 'completed', 10_000);

      expect((await queue.getMetrics('completed')).meta.count).toBe(1);
      expect((await queue.getMetrics('failed')).meta.count).toBe(0);
    }, 30_000);

    test('rejects invalid metric windows and trim lengths', async () => {
      harness = await startHarness('metrics-journal-validation', mode);
      const queue = harness.queue('validation');

      await expect(queue.getMetrics('completed', -1, -1)).rejects.toThrow(
        'start must be a non-negative safe integer'
      );
      await expect(queue.getMetrics('completed', 0, -2)).rejects.toThrow(
        'end must be a non-negative safe integer'
      );
      await expect(queue.trimEvents(-1)).rejects.toThrow(
        'maxLength must be a non-negative safe integer'
      );
    }, 20_000);

    test('persists metrics and trim state across restart, then obliterate removes both', async () => {
      harness = await startHarness('metrics-journal-restart', mode);
      const queue = harness.queue('durable');
      const queueName = queue.name;
      const worker = harness.worker(queueName, async () => true);

      const jobs = await queue.addBulk(
        Array.from({ length: 3 }, (_, index) => ({
          name: `job-${index}`,
          data: { index },
          opts: { durable: true },
        }))
      );
      for (const job of jobs) await waitForState(queue, job.id, 'completed', 10_000);
      expect((await queue.getMetrics('completed', 0, -1)).meta.count).toBe(3);
      expect(await queue.trimEvents(2)).toBeGreaterThan(0);
      await worker.close(true);

      await harness.restartBroker();
      const restarted = reopenQueue(harness, queueName);
      const persisted = await restarted.getMetrics('completed', 0, -1);
      expect(persisted.meta.count).toBe(3);
      expect(persisted.data.reduce((sum, value) => sum + value, 0)).toBe(3);
      expect(await restarted.trimEvents(2)).toBe(0);

      await restarted.obliterateAsync();
      expect(await restarted.getMetrics('completed', 0, -1)).toEqual({
        meta: { count: 0, prevTS: 0, prevCount: 0 },
        data: [],
        count: 0,
      });
      expect(await restarted.trimEvents(0)).toBe(0);

      await harness.restartBroker();
      const afterSecondRestart = reopenQueue(harness, queueName);
      expect((await afterSecondRestart.getMetrics('completed', 0, -1)).meta.count).toBe(0);
      expect(await afterSecondRestart.trimEvents(0)).toBe(0);
    }, 45_000);
  });
}

test('SQLite bounds each queue journal and metric window without resetting the total', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bunqueue-metrics-bounds-'));
  const storage = new SqliteStorage({ path: join(dir, 'queue.db') });
  try {
    const epoch = Math.floor(Date.now() / MINUTE) * MINUTE;
    for (let index = 0; index < 5; index++) {
      storage.recordQueueEvent(
        {
          eventType: EventType.Completed,
          queue: 'bounded',
          jobId: `job-${index}`,
          timestamp: epoch + index * MINUTE,
        },
        3,
        2
      );
    }

    expect(storage.countQueueEvents('bounded')).toBe(3);
    expect(storage.getQueueMetrics('bounded', 'completed', 2)).toEqual({
      meta: { count: 5, prevTS: epoch + 4 * MINUTE, prevCount: 1 },
      data: [1, 1],
    });
    expect(storage.trimQueueEvents('bounded', 2)).toBe(1);
    expect(storage.trimQueueEvents('bounded', 2)).toBe(0);
  } finally {
    storage.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
