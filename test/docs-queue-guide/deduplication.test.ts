/**
 * Executable proof for /guide/queue/deduplication/
 * ("Deduplication and Idempotent Job Adds").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../../src/application/queueManager';
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
  describe(`queue guide · deduplication [${mode}]`, () => {
    test('a repeated jobId returns the existing job instead of a duplicate', async () => {
      harness = await startHarness('deduplication', mode);
      const queue = harness.queue<{ orderId: number }>('orders');
      const id = `order-${crypto.randomUUID()}`;

      const job1 = await queue.add('process', { orderId: 123 }, { jobId: id, durable: true });
      const job2 = await queue.add('process', { orderId: 123 }, { jobId: id, durable: true });

      expect(job1.id).toBe(job2.id);
      expect(job1.id).toBe(id);
      expect(await queue.getJobCountsAsync()).toMatchObject({ waiting: 1 });
    });

    test('idempotency holds for waiting, delayed, prioritized and active generations', async () => {
      harness = await startHarness('deduplication', mode);
      const queue = harness.queue('orders');

      const cases: Array<[string, { delay?: number; priority?: number }, string]> = [
        ['waiting', {}, 'waiting'],
        ['delayed', { delay: 60_000 }, 'delayed'],
        ['prioritized', { priority: 5 }, 'prioritized'],
      ];
      for (const [label, opts, expectedState] of cases) {
        const id = `live-${label}-${crypto.randomUUID()}`;
        const first = await queue.add('process', { label }, { ...opts, jobId: id, durable: true });
        expect(await queue.getJobState(first.id)).toBe(expectedState);
        const second = await queue.add('process', { label }, { ...opts, jobId: id, durable: true });
        expect(second.id).toBe(first.id);
      }
    }, 20_000);

    test('idempotency also holds while the generation is active', async () => {
      harness = await startHarness('deduplication', mode);
      const queue = harness.queue('orders-active');
      const activeId = `live-active-${crypto.randomUUID()}`;
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      harness.worker(queue.name, async () => {
        await held;
        return true;
      });
      const active = await queue.add(
        'process',
        { label: 'active' },
        { jobId: activeId, durable: true }
      );
      await waitForState(queue, active.id, 'active');

      const duringActive = await queue.add(
        'process',
        { label: 'active' },
        { jobId: activeId, durable: true }
      );

      expect(duringActive.id).toBe(active.id);
      expect(await queue.getJobCountsAsync()).toMatchObject({ active: 1, waiting: 0 });
      release();
      await waitForState(queue, active.id, 'completed');
    }, 20_000);

    test('a custom id is broker-wide, so another queue cannot reuse it while live', async () => {
      harness = await startHarness('deduplication', mode);
      const orders = harness.queue('orders');
      const invoices = harness.queue('invoices');
      const id = `shared-${crypto.randomUUID()}`;

      const first = await orders.add('process', { from: 'orders' }, { jobId: id, durable: true });
      const second = await invoices.add(
        'process',
        { from: 'invoices' },
        { jobId: id, durable: true }
      );

      expect(second.id).toBe(first.id);
      expect(await invoices.getJobCountsAsync()).toMatchObject({ waiting: 0 });
      expect(await orders.getJobCountsAsync()).toMatchObject({ waiting: 1 });
    });

    test('a terminal id starts exactly one fresh generation on re-add', async () => {
      harness = await startHarness('deduplication', mode);
      const queue = harness.queue<{ run: number }>('orders');
      const id = `report-${crypto.randomUUID()}`;

      let processed = 0;
      harness.worker(queue.name, async () => {
        processed++;
        return true;
      });
      const first = await queue.add('report', { run: 1 }, { jobId: id, durable: true });
      await waitForState(queue, first.id, 'completed');

      const second = await queue.add('report', { run: 2 }, { jobId: id, durable: true });
      expect(second.id).toBe(id);
      await waitUntil(() => processed >= 2, 'the fresh generation to be processed', 10_000);

      // Exactly one fresh generation: a third add while it is live is idempotent.
      const third = await queue.add('report', { run: 3 }, { jobId: id, durable: true });
      expect(third.id).toBe(id);
    }, 20_000);

    test('deduplication ttl blocks duplicates inside the window and allows them after', async () => {
      harness = await startHarness('deduplication', mode);
      const queue = harness.queue('notifications');
      await queue.pauseAsync();
      const key = `notify-${crypto.randomUUID()}`;

      await queue.add(
        'notification',
        { userId: '123' },
        {
          deduplication: { id: key, ttl: 400 },
          durable: true,
        }
      );
      await queue.add(
        'notification',
        { userId: '123' },
        {
          deduplication: { id: key, ttl: 400 },
          durable: true,
        }
      );
      expect(await queue.countAsync()).toBe(1);

      await Bun.sleep(500);
      await queue.add(
        'notification',
        { userId: '123' },
        {
          deduplication: { id: key, ttl: 400 },
          durable: true,
        }
      );
      expect(await queue.countAsync()).toBe(2);
    }, 20_000);

    test('extend keeps the existing job and resets its ttl', async () => {
      harness = await startHarness('deduplication', mode);
      const queue = harness.queue('sync');
      await queue.pauseAsync();
      const key = `sync-${crypto.randomUUID()}`;

      const first = await queue.add(
        'sync-task',
        { action: 'sync' },
        {
          deduplication: { id: key, ttl: 400, extend: true },
          durable: true,
        }
      );
      await Bun.sleep(250);
      const second = await queue.add(
        'sync-task',
        { action: 'sync' },
        {
          deduplication: { id: key, ttl: 400, extend: true },
          durable: true,
        }
      );

      expect(second.id).toBe(first.id);
      expect(await queue.countAsync()).toBe(1);

      // The window was reset by the second add, so the original ttl has lapsed
      // without releasing the key.
      await Bun.sleep(250);
      const third = await queue.add(
        'sync-task',
        { action: 'sync' },
        {
          deduplication: { id: key, ttl: 400, extend: true },
          durable: true,
        }
      );
      expect(third.id).toBe(first.id);
      expect(await queue.countAsync()).toBe(1);
    }, 20_000);

    test('replace removes the pending job and inserts a new one with a new id', async () => {
      harness = await startHarness('deduplication', mode);
      const queue = harness.queue<{ data: string }>('latest');
      await queue.pauseAsync();
      const key = `data-job-${crypto.randomUUID()}`;

      const first = await queue.add(
        'latest-data',
        { data: 'old' },
        {
          deduplication: { id: key, ttl: 300_000, replace: true },
          durable: true,
        }
      );
      const second = await queue.add(
        'latest-data',
        { data: 'new' },
        {
          deduplication: { id: key, ttl: 300_000, replace: true },
          durable: true,
        }
      );

      expect(second.id).not.toBe(first.id);
      expect(await queue.countAsync()).toBe(1);
      expect(await queue.getJobCountsAsync()).toMatchObject({ paused: 1 });
      expect((await queue.getJob(second.id))?.data.data).toBe('new');
      expect(await queue.getDeduplicationJobId(key)).toBe(second.id);

      // The superseded job is no longer runnable: resuming delivers only the
      // replacement.
      const delivered: string[] = [];
      harness.worker<{ data: string }, boolean>(queue.name, async (job) => {
        delivered.push(job.data.data);
        return true;
      });
      await queue.resumeAsync();
      await waitForState(queue, second.id, 'completed');
      await Bun.sleep(250);
      expect(delivered).toEqual(['new']);
    }, 20_000);

    // A replacement retires both the in-memory generation and its persisted row.
    test('the replaced job is no longer reported by getJobState', async () => {
      harness = await startHarness('deduplication', mode);
      const queue = harness.queue<{ data: string }>('latest');
      await queue.pauseAsync();
      const key = `data-job-${crypto.randomUUID()}`;

      const first = await queue.add(
        'latest-data',
        { data: 'old' },
        { deduplication: { id: key, ttl: 300_000, replace: true }, durable: true }
      );
      await queue.add(
        'latest-data',
        { data: 'new' },
        { deduplication: { id: key, ttl: 300_000, replace: true }, durable: true }
      );

      expect(await queue.getJobState(first.id)).toBe('unknown');
    });

    test('replace removes dependency edges owned by a superseded waiting job', async () => {
      harness = await startHarness('deduplication', mode);
      const dependencies = harness.queue('dedup-dependencies');
      const queue = harness.queue<{ version: number }>('latest-dependent');
      await queue.pauseAsync();
      const key = `dependent-${crypto.randomUUID()}`;
      const dependency = await dependencies.add('dependency', {}, { durable: true });

      const first = await queue.add(
        'latest-data',
        { version: 1 },
        {
          dependsOn: [dependency.id],
          deduplication: { id: key, ttl: 300_000, replace: true },
          durable: true,
        }
      );
      const replacement = await queue.add(
        'latest-data',
        { version: 2 },
        {
          dependsOn: [dependency.id],
          deduplication: { id: key, ttl: 300_000, replace: true },
          durable: true,
        }
      );

      expect(await queue.getJobState(first.id)).toBe('unknown');
      expect(await queue.getJobState(replacement.id)).toBe('waiting-children');
      expect(await queue.getJobCountsAsync()).toMatchObject({ 'waiting-children': 1 });

      harness.worker(dependencies.name, async () => true);
      await waitForState(dependencies, dependency.id, 'completed');
      await waitForState(queue, replacement.id, 'waiting');
      expect(await queue.getJobState(first.id)).toBe('unknown');
      expect(await queue.getJobCountsAsync()).toMatchObject({
        paused: 1,
        'waiting-children': 0,
      });
    }, 20_000);

    test('replace leaves an already active job running and queues the new one', async () => {
      harness = await startHarness('deduplication', mode);
      const queue = harness.queue<{ data: string }>('latest-active');
      const key = `active-${crypto.randomUUID()}`;
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      harness.worker(queue.name, async () => {
        await held;
        return true;
      });
      const first = await queue.add(
        'latest-data',
        { data: 'old' },
        { deduplication: { id: key, ttl: 300_000, replace: true }, durable: true }
      );
      await waitForState(queue, first.id, 'active');

      const second = await queue.add(
        'latest-data',
        { data: 'new' },
        { deduplication: { id: key, ttl: 300_000, replace: true }, durable: true }
      );

      // The active generation is untouched; the replacement waits its turn.
      expect(second.id).not.toBe(first.id);
      expect(await queue.getJobState(first.id)).toBe('active');
      expect(await queue.getJobCountsAsync()).toMatchObject({ active: 1, waiting: 1 });

      release();
      await waitForState(queue, first.id, 'completed');
      await waitForState(queue, second.id, 'completed');
    }, 20_000);

    test('replace wins when both extend and replace are set', async () => {
      harness = await startHarness('deduplication', mode);
      const queue = harness.queue('latest');
      await queue.pauseAsync();
      const key = `both-${crypto.randomUUID()}`;

      const first = await queue.add(
        'latest-data',
        { data: 'old' },
        {
          deduplication: { id: key, ttl: 300_000, extend: true, replace: true },
          durable: true,
        }
      );
      const second = await queue.add(
        'latest-data',
        { data: 'new' },
        {
          deduplication: { id: key, ttl: 300_000, extend: true, replace: true },
          durable: true,
        }
      );

      expect(second.id).not.toBe(first.id);
      expect(await queue.countAsync()).toBe(1);
    });

    test('replace collapses same-key generations inside one addBulk call', async () => {
      harness = await startHarness('deduplication', mode);
      const queue = harness.queue<{ version: number }>('latest-bulk');
      await queue.pauseAsync();
      const key = `bulk-${crypto.randomUUID()}`;
      const pushedBefore = harness.brokerManager().getStats().totalPushed;

      const jobs = await queue.addBulk([
        {
          name: 'latest-data',
          data: { version: 1 },
          opts: {
            deduplication: { id: key, ttl: 300_000, replace: true },
            durable: true,
          },
        },
        {
          name: 'latest-data',
          data: { version: 2 },
          opts: {
            deduplication: { id: key, ttl: 300_000, replace: true },
            durable: true,
          },
        },
        {
          name: 'latest-data',
          data: { version: 3 },
          opts: {
            deduplication: { id: key, ttl: 300_000, replace: true },
            durable: true,
          },
        },
      ]);

      expect(new Set(jobs.map((job) => job.id)).size).toBe(3);
      expect(await queue.getJobState(jobs[0].id)).toBe('unknown');
      expect(await queue.getJobState(jobs[1].id)).toBe('unknown');
      expect(await queue.getJobState(jobs[2].id)).toBe('waiting');
      expect((await queue.getJob(jobs[2].id))?.data.version).toBe(3);
      expect(await queue.getJobCountsAsync()).toMatchObject({ paused: 1 });
      expect(await queue.getDeduplicationJobId(key)).toBe(jobs[2].id);
      expect(harness.brokerManager().getStats().totalPushed - pushedBefore).toBe(3n);
    });

    test('getDeduplicationJobId and removeDeduplicationKey work in this mode', async () => {
      harness = await startHarness('deduplication', mode);
      const queue = harness.queue('keys');
      await queue.pauseAsync();
      const key = `my-unique-key-${crypto.randomUUID()}`;

      const first = await queue.add(
        'task',
        { v: 1 },
        {
          deduplication: { id: key, ttl: 300_000 },
          durable: true,
        }
      );

      expect(await queue.getDeduplicationJobId(key)).toBe(first.id);

      await queue.removeDeduplicationKey(key);
      expect(await queue.getDeduplicationJobId(key)).toBeNull();

      const second = await queue.add(
        'task',
        { v: 2 },
        {
          deduplication: { id: key, ttl: 300_000 },
          durable: true,
        }
      );
      expect(second.id).not.toBe(first.id);
      expect(await queue.countAsync()).toBe(2);
    });

    test('job-level removeDeduplicationKey is generation-safe', async () => {
      harness = await startHarness('deduplication', mode);
      const queue = harness.queue('keys');
      await queue.pauseAsync();
      const key = `generation-${crypto.randomUUID()}`;

      const stale = await queue.add(
        'task',
        { v: 1 },
        {
          deduplication: { id: key, ttl: 300_000, replace: true },
          durable: true,
        }
      );
      const replacement = await queue.add(
        'task',
        { v: 2 },
        {
          deduplication: { id: key, ttl: 300_000, replace: true },
          durable: true,
        }
      );
      expect(replacement.id).not.toBe(stale.id);

      // The stale generation no longer owns the key.
      expect(await stale.removeDeduplicationKey()).toBe(false);
      expect(await queue.getDeduplicationJobId(key)).toBe(replacement.id);

      // The owning generation can release it.
      expect(await replacement.removeDeduplicationKey()).toBe(true);
      expect(await queue.getDeduplicationJobId(key)).toBeNull();
    });
  });
}

describe('queue guide · deduplication [durability]', () => {
  // "Last write wins" must remain true after persistence recovery.
  test('a replaced job is not resurrected by a broker restart', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'bunqueue-docs-dedup-restart-'));
    const dataPath = join(dataDir, 'queue.db');
    const queue = 'latest';
    const key = `data-job-${crypto.randomUUID()}`;
    let manager = new QueueManager({ dataPath });

    try {
      await manager.push(queue, {
        data: { name: 'latest-data', data: 'old' },
        uniqueKey: key,
        durable: true,
        dedup: { ttl: 300_000, replace: true },
      });
      await manager.push(queue, {
        data: { name: 'latest-data', data: 'new' },
        uniqueKey: key,
        durable: true,
        dedup: { ttl: 300_000, replace: true },
      });
      expect(manager.getQueueJobCounts(queue).waiting).toBe(1);

      await Bun.sleep(100);
      manager.shutdown();
      manager = new QueueManager({ dataPath });
      await waitUntil(
        () => manager.getQueueJobCounts(queue).waiting > 0,
        'recovery to restore the queue'
      );

      const delivered: unknown[] = [];
      for (let i = 0; i < 3; i++) {
        const job = await manager.pull(queue, 200);
        if (!job) break;
        delivered.push((job.data as { data: unknown }).data);
      }
      expect(delivered).toEqual(['new']);
    } finally {
      manager.shutdown();
      rmSync(dataDir, { recursive: true, force: true });
    }
  }, 30_000);
});
