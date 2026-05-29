/**
 * E2E Edge Case Tests for bunqueue
 * Covers webhook delivery, rate limiting, batch ops, and queue control sequences
 */
import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { unlink } from 'fs/promises';

const TEST_DB = './test-e2e-edge-cases.db';

async function cleanup() {
  for (const f of [TEST_DB, `${TEST_DB}-wal`, `${TEST_DB}-shm`]) {
    if (await Bun.file(f).exists()) await unlink(f);
  }
}

function setup() {
  let manager: QueueManager;
  beforeEach(async () => {
    await cleanup();
    manager = new QueueManager({ dataPath: TEST_DB, validateWebhookUrls: false });
  });
  afterEach(async () => {
    manager.shutdown();
    await cleanup();
  });
  return { get mgr() { return manager; } };
}

describe('E2E Edge Cases', () => {
  describe('1. Webhook delivery + retry', () => {
    const ctx = setup();

    test('webhook tracks successCount after delivery', async () => {
      // Use a local server (not an external service like httpbin.org) so the
      // assertion is deterministic and CI never flakes on network conditions.
      const server = Bun.serve({ port: 0, fetch: () => new Response('ok', { status: 200 }) });
      try {
        const url = `http://localhost:${server.port}/hook`;
        const wh = ctx.mgr.webhookManager.add(url, ['job.completed'], 'wh-q');
        expect(wh.successCount).toBe(0);
        expect(wh.failureCount).toBe(0);

        await ctx.mgr.webhookManager.trigger('job.completed', 'job-1', 'wh-q');
        // Delivery is fire-and-forget; poll until it records a result.
        const deadline = Date.now() + 5000;
        while (ctx.mgr.webhookManager.get(wh.id)!.successCount === 0 && Date.now() < deadline) {
          await Bun.sleep(10);
        }

        const updated = ctx.mgr.webhookManager.get(wh.id)!;
        expect(updated.successCount).toBe(1);
        expect(updated.failureCount).toBe(0);
      } finally {
        server.stop(true);
      }
    }, 10000);

    test('webhook tracks failureCount for invalid URL', async () => {
      const wh = ctx.mgr.webhookManager.add(
        'http://localhost:1/nonexistent', ['job.failed'], 'wh-fail-q'
      );
      await ctx.mgr.webhookManager.trigger('job.failed', 'job-2', 'wh-fail-q');
      await Bun.sleep(8000);

      const updated = ctx.mgr.webhookManager.get(wh.id)!;
      expect(updated.failureCount).toBe(1);
      expect(updated.successCount).toBe(0);
    }, 15000);

    test('disabled webhook does not trigger', async () => {
      const wh = ctx.mgr.webhookManager.add(
        'http://localhost:1/nonexistent', ['job.completed'], 'wh-off-q'
      );
      ctx.mgr.webhookManager.setEnabled(wh.id, false);
      await ctx.mgr.webhookManager.trigger('job.completed', 'job-3', 'wh-off-q');
      await Bun.sleep(500);

      const updated = ctx.mgr.webhookManager.get(wh.id)!;
      expect(updated.successCount).toBe(0);
      expect(updated.failureCount).toBe(0);
    });

    test('webhook filters by queue correctly', async () => {
      const wh = ctx.mgr.webhookManager.add(
        'http://localhost:1/nonexistent', ['job.completed'], 'specific-queue'
      );
      await ctx.mgr.webhookManager.trigger('job.completed', 'job-4', 'other-queue');
      await Bun.sleep(500);

      const updated = ctx.mgr.webhookManager.get(wh.id)!;
      expect(updated.successCount).toBe(0);
      expect(updated.failureCount).toBe(0);
    });
  });

  describe('2. Rate limiting edge cases', () => {
    const ctx = setup();

    test('rate limit restricts pulls beyond the limit', async () => {
      ctx.mgr.setRateLimit('rate-q', 2);
      for (let i = 0; i < 5; i++) await ctx.mgr.push('rate-q', { data: { index: i } });

      const j1 = await ctx.mgr.pull('rate-q', 100);
      const j2 = await ctx.mgr.pull('rate-q', 100);
      const j3 = await ctx.mgr.pull('rate-q', 100);

      expect(j1).toBeDefined();
      expect(j2).toBeDefined();
      expect(j3).toBeNull();
    });

    test('clearRateLimit allows processing to resume', async () => {
      ctx.mgr.setRateLimit('rate-clr-q', 1);
      await ctx.mgr.push('rate-clr-q', { data: { a: 1 } });
      await ctx.mgr.push('rate-clr-q', { data: { a: 2 } });

      const j1 = await ctx.mgr.pull('rate-clr-q', 100);
      expect(j1).toBeDefined();
      expect(await ctx.mgr.pull('rate-clr-q', 100)).toBeNull();

      await ctx.mgr.ack(j1!.id, {});
      ctx.mgr.clearRateLimit('rate-clr-q');

      expect(await ctx.mgr.pull('rate-clr-q', 100)).toBeDefined();
    });

    test('concurrency limit 0 blocks all pulls, restoring allows pulls', async () => {
      ctx.mgr.setConcurrency('conc0-q', 0);
      await ctx.mgr.push('conc0-q', { data: { v: 1 } });
      await ctx.mgr.push('conc0-q', { data: { v: 2 } });

      expect(await ctx.mgr.pull('conc0-q', 100)).toBeNull();

      ctx.mgr.setConcurrency('conc0-q', 5);
      expect(await ctx.mgr.pull('conc0-q', 100)).toBeDefined();
    });

    test('rate limit and concurrency limit work independently', async () => {
      ctx.mgr.setRateLimit('dual-q', 3);
      ctx.mgr.setConcurrency('dual-q', 2);
      for (let i = 0; i < 5; i++) await ctx.mgr.push('dual-q', { data: { i } });

      const j1 = await ctx.mgr.pull('dual-q', 100);
      const j2 = await ctx.mgr.pull('dual-q', 100);
      expect(j1).toBeDefined();
      expect(j2).toBeDefined();
      // Concurrency limit (2) reached even though rate limit (3) has room
      expect(await ctx.mgr.pull('dual-q', 100)).toBeNull();
    });
  });

  describe('3. Batch operations under load', () => {
    const ctx = setup();

    test('pushBatch creates 150 jobs and all are retrievable', async () => {
      const inputs = Array.from({ length: 150 }, (_, i) => ({ data: { index: i } }));
      const ids = await ctx.mgr.pushBatch('batch-q', inputs);

      expect(ids.length).toBe(150);
      expect(new Set(ids.map(String)).size).toBe(150);
      expect(ctx.mgr.count('batch-q')).toBe(150);
    });

    test('pullBatch returns jobs in priority order', async () => {
      await ctx.mgr.push('bord-q', { data: { name: 'low' }, priority: 1 });
      await ctx.mgr.push('bord-q', { data: { name: 'high' }, priority: 10 });
      await ctx.mgr.push('bord-q', { data: { name: 'medium' }, priority: 5 });

      const jobs = await ctx.mgr.pullBatch('bord-q', 3, 100);
      expect(jobs.length).toBe(3);
      expect(jobs.map((j) => (j.data as { name: string }).name)).toEqual(['high', 'medium', 'low']);
    });

    test('pullBatch returns available jobs when fewer than requested', async () => {
      await ctx.mgr.push('bpart-q', { data: { a: 1 } });
      await ctx.mgr.push('bpart-q', { data: { a: 2 } });
      expect((await ctx.mgr.pullBatch('bpart-q', 10, 100)).length).toBe(2);
    });

    test('pushBatch + pullBatch round-trip preserves priority order', async () => {
      const inputs = Array.from({ length: 100 }, (_, i) => ({ data: { idx: i }, priority: i }));
      await ctx.mgr.pushBatch('brt-q', inputs);

      const allPulled = [];
      let batch = await ctx.mgr.pullBatch('brt-q', 25, 100);
      while (batch.length > 0) {
        allPulled.push(...batch);
        batch = await ctx.mgr.pullBatch('brt-q', 25, 100);
      }
      expect(allPulled.length).toBe(100);
      for (let i = 1; i < allPulled.length; i++) {
        expect(allPulled[i - 1].priority).toBeGreaterThanOrEqual(allPulled[i].priority);
      }
    });
  });

  describe('4. Queue operations sequence', () => {
    const ctx = setup();

    test('pause prevents pulls, resume restores them', async () => {
      await ctx.mgr.push('pause-q', { data: { msg: 'before pause' } });
      ctx.mgr.pause('pause-q');
      expect(ctx.mgr.isPaused('pause-q')).toBe(true);
      expect(await ctx.mgr.pull('pause-q', 100)).toBeNull();

      ctx.mgr.resume('pause-q');
      expect(ctx.mgr.isPaused('pause-q')).toBe(false);
      const job = await ctx.mgr.pull('pause-q', 100);
      expect(job).toBeDefined();
      expect((job!.data as { msg: string }).msg).toBe('before pause');
    });

    test('jobs added while paused become available after resume', async () => {
      ctx.mgr.pause('padd-q');
      await ctx.mgr.push('padd-q', { data: { added: 'while paused 1' } });
      await ctx.mgr.push('padd-q', { data: { added: 'while paused 2' } });
      expect(ctx.mgr.count('padd-q')).toBe(2);
      expect(await ctx.mgr.pull('padd-q', 100)).toBeNull();

      ctx.mgr.resume('padd-q');
      expect(await ctx.mgr.pull('padd-q', 100)).toBeDefined();
      expect(await ctx.mgr.pull('padd-q', 100)).toBeDefined();
    });

    test('drain removes all waiting jobs', async () => {
      for (let i = 0; i < 10; i++) await ctx.mgr.push('drain-q', { data: { i } });
      expect(ctx.mgr.count('drain-q')).toBe(10);

      expect(ctx.mgr.drain('drain-q')).toBe(10);
      expect(ctx.mgr.count('drain-q')).toBe(0);
      expect(await ctx.mgr.pull('drain-q', 100)).toBeNull();
    });

    test('full lifecycle: push -> pause -> add -> resume -> drain', async () => {
      await ctx.mgr.push('life-q', { data: { phase: 'initial' } });
      ctx.mgr.pause('life-q');
      expect(ctx.mgr.isPaused('life-q')).toBe(true);

      await ctx.mgr.push('life-q', { data: { phase: 'paused' } });
      expect(ctx.mgr.count('life-q')).toBe(2);
      expect(await ctx.mgr.pull('life-q', 100)).toBeNull();

      ctx.mgr.resume('life-q');
      const pulled = await ctx.mgr.pull('life-q', 100);
      expect(pulled).toBeDefined();
      await ctx.mgr.ack(pulled!.id, {});

      expect(ctx.mgr.drain('life-q')).toBe(1);
      expect(ctx.mgr.count('life-q')).toBe(0);
      expect(await ctx.mgr.pull('life-q', 100)).toBeNull();
    });

    test('obliterate fully removes queue data', async () => {
      for (let i = 0; i < 5; i++) await ctx.mgr.push('obl-q', { data: { i } });
      expect(ctx.mgr.count('obl-q')).toBe(5);

      ctx.mgr.obliterate('obl-q');
      expect(ctx.mgr.count('obl-q')).toBe(0);
      expect(await ctx.mgr.pull('obl-q', 100)).toBeNull();
    });
  });
});
