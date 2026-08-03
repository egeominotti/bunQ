/**
 * Executable proof for /guide/worker/options/ ("WorkerOptions Reference").
 *
 * Every row of the reference table is checked: the documented default, and the
 * documented behaviour when the option is set.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Worker } from '../../src/client';
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

interface WorkerInternals {
  opts: Record<string, unknown>;
  tcpPool?: { size?: number } | null;
}

function options(worker: unknown): Record<string, unknown> {
  return (worker as unknown as WorkerInternals).opts;
}

for (const mode of MODES) {
  describe(`worker guide · options reference [${mode}]`, () => {
    test('the documented defaults are the resolved defaults', async () => {
      harness = await startHarness('worker-options', mode);
      const queue = harness.queue('defaults');
      const worker = harness.worker(queue.name, async () => true);
      const opts = options(worker);

      expect(opts.concurrency).toBe(1);
      expect(opts.autorun).toBe(true);
      expect(opts.heartbeatInterval).toBe(10_000);
      expect(opts.batchSize).toBe(10);
      expect(opts.pollTimeout).toBe(0);
      expect(opts.useLocks).toBe(true);
      expect(opts.lockDuration).toBe(30_000);
      expect(opts.maxStalledCount).toBe(1);
      expect(opts.skipStalledCheck).toBe(false);
      expect(opts.skipLockRenewal).toBe(false);
      expect(opts.drainDelay).toBe(50);
      expect(opts.removeOnComplete ?? false).toBe(false);
      expect(opts.removeOnFail ?? false).toBe(false);
      expect(opts.limiter ?? null).toBeNull();
    });

    test('batchSize is capped at 1000 and pollTimeout at 30000', async () => {
      harness = await startHarness('worker-options', mode);
      const queue = harness.queue('bounds');

      const worker = harness.worker(queue.name, async () => true, {
        batchSize: 5_000,
        pollTimeout: 120_000,
      });
      const opts = options(worker);

      expect(opts.batchSize).toBe(1_000);
      expect(opts.pollTimeout).toBe(30_000);
    });

    test('a non-boolean removeOnComplete keeps the job, as documented', async () => {
      harness = await startHarness('worker-options', mode);
      const queue = harness.queue('remove-on-complete-count');

      harness.worker(queue.name, async () => true, { removeOnComplete: 10 });
      const kept = await queue.add('kept', {}, { durable: true });
      await waitForState(queue, kept.id, 'completed', 10_000);
      await Bun.sleep(300);

      expect(await queue.getJob(kept.id)).not.toBeNull();
    }, 30_000);

    // Documented: "removeOnComplete | boolean | false | Auto-remove completed
    // jobs. Only `true` is honored". The worker only flips the flag on its local
    // copy of the job, so over TCP the broker never sees it.
    test('removeOnComplete: true on the worker removes the finished job', async () => {
      harness = await startHarness('worker-options', mode);
      const queue = harness.queue('remove-on-complete');

      harness.worker(queue.name, async () => true, { removeOnComplete: true });
      const removed = await queue.add('gone', {}, { durable: true });

      await waitUntil(
        async () => (await queue.getJob(removed.id)) === null,
        'the job to be removed',
        10_000
      );
    }, 30_000);

    test('removeOnFail: true on the worker removes the failed job', async () => {
      harness = await startHarness('worker-options', mode);
      const queue = harness.queue('remove-on-fail');

      const worker = harness.worker(
        queue.name,
        async () => {
          throw new Error('boom');
        },
        { removeOnFail: true }
      );
      worker.on('failed', () => undefined);
      const dropped = await queue.add('gone', {}, { attempts: 1, durable: true });

      await waitUntil(
        async () => (await queue.getJob(dropped.id)) === null,
        'the failed job to be removed',
        15_000
      );
    }, 30_000);

    test('skipLockRenewal stops this worker renewing its lease', async () => {
      harness = await startHarness('worker-options', mode);
      const queue = harness.queue('skip-renewal');

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 50,
        maxStalls: 10,
        gracePeriod: 0,
      });
      const worker = harness.worker(
        queue.name,
        async () => await new Promise<never>(() => undefined),
        { heartbeatInterval: 50, skipLockRenewal: true, useLocks: true }
      );
      worker.on('error', () => undefined);

      const job = await queue.add('long', {}, { attempts: 10, durable: true });
      await waitForState(queue, job.id, 'active', 10_000);
      await waitUntil(
        async () => (await queue.getJobState(job.id)) !== 'active',
        'the un-renewed job to be recovered',
        30_000
      );
    }, 60_000);

    test('maxStalledCount is accepted but does not change broker recovery', async () => {
      harness = await startHarness('worker-options', mode);
      const queue = harness.queue('max-stalled');

      // Worker asks for 99 stalls; the queue policy allows exactly one.
      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 20,
        maxStalls: 1,
        gracePeriod: 0,
      });
      const worker = harness.worker(
        queue.name,
        async () => await new Promise<never>(() => undefined),
        { heartbeatInterval: 0, useLocks: true, maxStalledCount: 99 }
      );
      worker.on('error', () => undefined);

      const job = await queue.add('silent', {}, { attempts: 10, durable: true });
      await waitForState(queue, job.id, 'failed', 40_000);
      expect((await queue.getDlqAsync()).find((entry) => entry.job.id === job.id)?.reason).toBe(
        'stalled'
      );
    }, 60_000);

    test('the limiter groupKey form caps concurrency per group', async () => {
      harness = await startHarness('worker-options', mode);
      const queue = harness.queue<{ tenant: string }>('grouped');
      await queue.pauseAsync();

      const inFlight = new Map<string, number>();
      const peak = new Map<string, number>();
      let done = 0;
      harness.worker<{ tenant: string }, boolean>(
        queue.name,
        async (job) => {
          const tenant = job.data.tenant;
          const current = (inFlight.get(tenant) ?? 0) + 1;
          inFlight.set(tenant, current);
          peak.set(tenant, Math.max(peak.get(tenant) ?? 0, current));
          await Bun.sleep(60);
          inFlight.set(tenant, current - 1);
          done++;
          return true;
        },
        { concurrency: 6, limiter: { max: 1, duration: 1_000, groupKey: 'tenant' } }
      );
      for (let i = 0; i < 8; i++) {
        await queue.add('task', { tenant: i % 2 === 0 ? 'a' : 'b' }, { durable: true });
      }
      await queue.resumeAsync();

      await waitUntil(() => done === 8, 'every grouped job to finish', 30_000);
      expect(peak.get('a')).toBe(1);
      expect(peak.get('b')).toBe(1);
    }, 60_000);

    test('prefixKey must match the producing queue', async () => {
      harness = await startHarness('worker-options', mode);
      const name = `prefixed-${crypto.randomUUID().slice(0, 8)}`;
      const queue = harness.queue(name, { prefixKey: 'dev:' });
      const seen: string[] = [];

      const worker = new Worker(
        queue.name,
        async (job) => {
          seen.push(job.queueName);
          return true;
        },
        harness.workerOptions({ prefixKey: 'dev:' })
      );
      harness.addCleanup(() => worker.close(true));

      const job = await queue.add('task', {}, { durable: true });
      await waitForState(queue, job.id, 'completed', 10_000);
      expect(seen).toEqual([`dev:${queue.name}`]);
    }, 20_000);
  });
}

describe('worker guide · options reference [tcp pool sizing]', () => {
  test('poolSize defaults to min(concurrency, 8) and can be overridden', async () => {
    harness = await startHarness('worker-options', 'tcp');
    const queue = harness.queue('pooled');
    const connection = harness.connection();

    const build = (concurrency: number, poolSize?: number): Worker => {
      const worker = new Worker(queue.name, async () => true, {
        embedded: false,
        connection: { host: connection.host, port: connection.port, poolSize },
        concurrency,
        autorun: false,
      });
      harness?.addCleanup(() => worker.close(true));
      return worker;
    };
    const poolOf = (worker: Worker): number =>
      ((worker as unknown as { tcp: { clients: unknown[] } }).tcp.clients ?? []).length;

    expect(poolOf(build(3))).toBe(3);
    expect(poolOf(build(32))).toBe(8);
    expect(poolOf(build(32, 2))).toBe(2);
  }, 30_000);
});
