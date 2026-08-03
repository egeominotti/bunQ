/**
 * Executable proof for /guide/stall-detection/
 * ("Stall Detection: Auto-Recover Unresponsive Jobs").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Queue } from '../../src/client';
import { QueueManager } from '../../src/application/queueManager';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  queueEvents,
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
  describe(`worker guide · stall detection in depth [${mode}]`, () => {
    test('the documented defaults are the shipped stall policy', async () => {
      harness = await startHarness('stall-detection', mode);
      const queue = harness.queue('defaults');

      const config = await queue.getStallConfigAsync();

      expect(config.enabled).toBe(true);
      expect(config.stallInterval).toBe(30_000);
      expect(config.maxStalls).toBe(3);
      expect(config.gracePeriod).toBe(5_000);
    });

    test('setStallConfig writes the policy and reads it back', async () => {
      harness = await startHarness('stall-detection', mode);
      const queue = harness.queue('configured');

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 300_000,
        maxStalls: 2,
        gracePeriod: 60_000,
      });

      expect(await queue.getStallConfigAsync()).toMatchObject({
        enabled: true,
        stallInterval: 300_000,
        maxStalls: 2,
        gracePeriod: 60_000,
      });
    });

    test('the policy is server-side state, shared by every client of the queue', async () => {
      harness = await startHarness('stall-detection', mode);
      const name = `shared-policy-${crypto.randomUUID().slice(0, 8)}`;
      const first = new Queue(name, harness.queueOptions());
      const second = new Queue(name, harness.queueOptions());
      const otherQueue = new Queue(`${name}-other`, harness.queueOptions());
      harness.addCleanup(() => first.close());
      harness.addCleanup(() => second.close());
      harness.addCleanup(() => otherQueue.close());

      await first.setStallConfigAsync({ stallInterval: 111_000, maxStalls: 4 });

      // A different client of the same queue reads the same policy...
      expect(await second.getStallConfigAsync()).toMatchObject({
        stallInterval: 111_000,
        maxStalls: 4,
      });
      // ...while another queue keeps the defaults.
      expect((await otherQueue.getStallConfigAsync()).stallInterval).toBe(30_000);
    });

    test('disabling stall detection stops recovery entirely', async () => {
      harness = await startHarness('stall-detection', mode);
      const queue = harness.queue('disabled');

      await queue.setStallConfigAsync({
        enabled: false,
        stallInterval: 20,
        maxStalls: 1,
        gracePeriod: 0,
      });
      const worker = harness.worker(
        queue.name,
        async () => await new Promise<never>(() => undefined),
        { heartbeatInterval: 0, useLocks: true }
      );
      worker.on('error', () => undefined);

      const job = await queue.add('silent', {}, { durable: true });
      await waitForState(queue, job.id, 'active', 10_000);
      await Bun.sleep(2_000);

      expect(await queue.getJobState(job.id)).toBe('active');
    }, 30_000);

    test('gracePeriod protects a job right after it starts', async () => {
      harness = await startHarness('stall-detection', mode);
      const queue = harness.queue('grace');

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 20,
        maxStalls: 1,
        gracePeriod: 60_000,
      });
      const worker = harness.worker(
        queue.name,
        async () => await new Promise<never>(() => undefined),
        { heartbeatInterval: 0, useLocks: true }
      );
      worker.on('error', () => undefined);

      const job = await queue.add('silent', {}, { durable: true });
      await waitForState(queue, job.id, 'active', 10_000);
      await Bun.sleep(2_000);

      expect(await queue.getJobState(job.id)).toBe('active');
    }, 30_000);

    test('a stalled job is retried and eventually lands in the DLQ as stalled', async () => {
      harness = await startHarness('stall-detection', mode);
      const queue = harness.queue('recovering');

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 20,
        maxStalls: 1,
        gracePeriod: 0,
      });
      const worker = harness.worker(
        queue.name,
        async () => await new Promise<never>(() => undefined),
        { heartbeatInterval: 0, useLocks: true, concurrency: 1 }
      );
      worker.on('error', () => undefined);

      const job = await queue.add('silent', {}, { attempts: 10, durable: true });
      await waitForState(queue, job.id, 'failed', 40_000);

      const entry = (await queue.getDlqAsync()).find((item) => item.job.id === job.id);
      expect(entry?.reason).toBe('stalled');
      expect((await queue.getDlqStatsAsync()).byReason.stalled).toBeGreaterThanOrEqual(1);
      expect((await queue.getDlqAsync({ reason: 'stalled' })).length).toBeGreaterThanOrEqual(1);
    }, 60_000);

    test('QueueEvents delivers the stalled event', async () => {
      harness = await startHarness('stall-detection', mode);
      const queue = harness.queue('events');
      const events = queueEvents(harness, queue.name);
      const stalled: string[] = [];
      events.on('stalled', ({ jobId }) => stalled.push(jobId));
      await events.waitUntilReady();

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

      const job = await queue.add('silent', {}, { durable: true });
      await waitUntil(() => stalled.includes(job.id), 'the QueueEvents stalled event', 30_000);
    }, 60_000);

    test('updateProgress resets the stall timer like a heartbeat', async () => {
      harness = await startHarness('stall-detection', mode);
      const queue = harness.queue('progress-heartbeat');

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 400,
        maxStalls: 2,
        gracePeriod: 0,
      });
      const stalled: string[] = [];
      const worker = harness.worker(
        queue.name,
        async (job) => {
          for (let step = 0; step < 20; step++) {
            await Bun.sleep(100);
            await job.updateProgress(step * 5);
          }
          return true;
        },
        // Heartbeats are off: only updateProgress keeps the job alive.
        { heartbeatInterval: 0, useLocks: true }
      );
      worker.on('error', () => undefined);
      worker.on('stalled', (jobId: string) => stalled.push(jobId));

      const job = await queue.add('progressing', {}, { durable: true });
      await waitForState(queue, job.id, 'completed', 30_000);

      expect(stalled).toEqual([]);
    }, 60_000);
  });
}

describe('worker guide · stall detection [durability]', () => {
  test('a custom stall policy survives a broker restart', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'bunqueue-docs-stall-restart-'));
    const dataPath = join(dataDir, 'queue.db');
    const name = 'video-processing';
    let manager = new QueueManager({ dataPath });

    try {
      manager.setStallConfig(name, {
        enabled: true,
        stallInterval: 300_000,
        maxStalls: 2,
        gracePeriod: 60_000,
      });
      await manager.push(name, { data: { name: 'job' }, durable: true });
      await Bun.sleep(100);
      manager.shutdown();

      manager = new QueueManager({ dataPath });
      await waitUntil(
        () => manager.getStallConfig(name).stallInterval === 300_000,
        'the stall policy to be restored'
      );
      expect(manager.getStallConfig(name)).toMatchObject({
        enabled: true,
        stallInterval: 300_000,
        maxStalls: 2,
        gracePeriod: 60_000,
      });
    } finally {
      manager.shutdown();
      rmSync(dataDir, { recursive: true, force: true });
    }
  }, 30_000);
});
