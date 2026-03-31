/**
 * Rate Limiting Enforcement Tests (Embedded Mode)
 * Tests rate limit + concurrency enforcement via QueueManager pull operations.
 */
import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('Rate Limiting Enforcement', () => {
  let qm: QueueManager;

  beforeEach(() => { qm = new QueueManager(); });
  afterEach(() => { qm.shutdown(); });

  async function pushJobs(queue: string, count: number) {
    for (let i = 0; i < count; i++) {
      await qm.push(queue, { data: { index: i } });
    }
  }

  async function pullAll(queue: string, max: number) {
    const pulled = [];
    for (let i = 0; i < max; i++) {
      const job = await qm.pull(queue, 0);
      if (job) pulled.push(job);
    }
    return pulled;
  }

  // ============ Rate Limiting ============

  describe('setRateLimit and pull enforcement', () => {
    test('allows pulls up to the rate limit', async () => {
      await pushJobs('rl-basic', 5);
      qm.setRateLimit('rl-basic', 3);
      expect((await pullAll('rl-basic', 5)).length).toBe(3);
    });

    test('returns null once rate limit tokens are exhausted', async () => {
      await pushJobs('rl-block', 5);
      qm.setRateLimit('rl-block', 2);
      expect(await qm.pull('rl-block', 0)).not.toBeNull();
      expect(await qm.pull('rl-block', 0)).not.toBeNull();
      expect(await qm.pull('rl-block', 0)).toBeNull();
    });
  });

  describe('rate limit resets after the window expires', () => {
    test('allows pulls again after token refill', async () => {
      await pushJobs('rl-reset', 10);
      qm.setRateLimit('rl-reset', 2); // capacity=2, refillRate=2/sec
      await qm.pull('rl-reset', 0);
      await qm.pull('rl-reset', 0);
      expect(await qm.pull('rl-reset', 0)).toBeNull();
      // Wait for ~1.2 tokens to refill (600ms * 2 tokens/sec)
      await Bun.sleep(600);
      expect(await qm.pull('rl-reset', 0)).not.toBeNull();
    });
  });

  describe('rate limit with different max values', () => {
    test('limit of 1 allows exactly 1 pull', async () => {
      await pushJobs('rl-1', 3);
      qm.setRateLimit('rl-1', 1);
      expect(await qm.pull('rl-1', 0)).not.toBeNull();
      expect(await qm.pull('rl-1', 0)).toBeNull();
    });

    test('limit of 5 allows exactly 5 pulls', async () => {
      await pushJobs('rl-5', 10);
      qm.setRateLimit('rl-5', 5);
      expect((await pullAll('rl-5', 10)).length).toBe(5);
    });

    test('limit of 10 allows exactly 10 pulls', async () => {
      await pushJobs('rl-10', 15);
      qm.setRateLimit('rl-10', 10);
      expect((await pullAll('rl-10', 15)).length).toBe(10);
    });
  });

  describe('rate limit with different duration windows', () => {
    test('small capacity refills quickly', async () => {
      await pushJobs('rl-fast', 5);
      qm.setRateLimit('rl-fast', 1); // 1 token/sec
      expect(await qm.pull('rl-fast', 0)).not.toBeNull();
      expect(await qm.pull('rl-fast', 0)).toBeNull();
      await Bun.sleep(1100);
      expect(await qm.pull('rl-fast', 0)).not.toBeNull();
    });

    test('larger capacity: partial refill yields partial pulls', async () => {
      await pushJobs('rl-slow', 10);
      qm.setRateLimit('rl-slow', 4); // 4 tokens/sec
      for (let i = 0; i < 4; i++) {
        expect(await qm.pull('rl-slow', 0)).not.toBeNull();
      }
      expect(await qm.pull('rl-slow', 0)).toBeNull();
      // After 300ms: ~1.2 tokens refilled
      await Bun.sleep(300);
      expect(await qm.pull('rl-slow', 0)).not.toBeNull();
      expect(await qm.pull('rl-slow', 0)).toBeNull();
    });
  });

  describe('clearRateLimit removes the limit', () => {
    test('allows unlimited pulls after clearing', async () => {
      await pushJobs('rl-clear', 10);
      qm.setRateLimit('rl-clear', 2);
      await qm.pull('rl-clear', 0);
      await qm.pull('rl-clear', 0);
      expect(await qm.pull('rl-clear', 0)).toBeNull();
      qm.clearRateLimit('rl-clear');
      // 10 - 2 already pulled = 8 remaining
      expect((await pullAll('rl-clear', 10)).length).toBe(8);
    });
  });

  describe('rate limit per-queue isolation', () => {
    test('queue A rate limit does not affect queue B', async () => {
      await pushJobs('rl-qA', 5);
      await pushJobs('rl-qB', 5);
      qm.setRateLimit('rl-qA', 1);
      expect(await qm.pull('rl-qA', 0)).not.toBeNull();
      expect(await qm.pull('rl-qA', 0)).toBeNull();
      expect((await pullAll('rl-qB', 5)).length).toBe(5);
    });

    test('different queues can have different rate limits', async () => {
      await pushJobs('rl-iso-x', 10);
      await pushJobs('rl-iso-y', 10);
      qm.setRateLimit('rl-iso-x', 2);
      qm.setRateLimit('rl-iso-y', 5);
      expect((await pullAll('rl-iso-x', 10)).length).toBe(2);
      expect((await pullAll('rl-iso-y', 10)).length).toBe(5);
    });
  });

  // ============ Concurrency Limiting ============

  describe('setConcurrency limits concurrent active jobs', () => {
    test('blocks pulls when concurrency limit is reached', async () => {
      await pushJobs('cc-basic', 5);
      qm.setConcurrency('cc-basic', 2);
      expect(await qm.pull('cc-basic', 0)).not.toBeNull();
      expect(await qm.pull('cc-basic', 0)).not.toBeNull();
      expect(await qm.pull('cc-basic', 0)).toBeNull();
    });

    test('allows more pulls after acking active jobs', async () => {
      await pushJobs('cc-ack', 5);
      qm.setConcurrency('cc-ack', 1);
      const job1 = await qm.pull('cc-ack', 0);
      expect(job1).not.toBeNull();
      expect(await qm.pull('cc-ack', 0)).toBeNull();
      await qm.ack(job1!.id);
      expect(await qm.pull('cc-ack', 0)).not.toBeNull();
    });
  });

  describe('clearConcurrency removes concurrency limit', () => {
    test('allows unlimited concurrent pulls after clearing', async () => {
      await pushJobs('cc-clear', 5);
      qm.setConcurrency('cc-clear', 1);
      const job1 = await qm.pull('cc-clear', 0);
      expect(job1).not.toBeNull();
      expect(await qm.pull('cc-clear', 0)).toBeNull();
      qm.clearConcurrency('cc-clear');
      // 5 - 1 already pulled = 4 remaining
      expect((await pullAll('cc-clear', 5)).length).toBe(4);
    });
  });

  // ============ Combined Limits ============

  describe('concurrency + rate limit together', () => {
    test('both limits apply; concurrency stricter wins', async () => {
      await pushJobs('combo', 10);
      qm.setRateLimit('combo', 5);
      qm.setConcurrency('combo', 2);
      const job1 = await qm.pull('combo', 0);
      const job2 = await qm.pull('combo', 0);
      expect(job1).not.toBeNull();
      expect(job2).not.toBeNull();
      expect(await qm.pull('combo', 0)).toBeNull();
      // Ack one job to free concurrency slot
      await qm.ack(job1!.id);
      expect(await qm.pull('combo', 0)).not.toBeNull();
    });

    test('rate limit blocks even when concurrency has room', async () => {
      await pushJobs('combo-rl', 10);
      qm.setRateLimit('combo-rl', 1);
      qm.setConcurrency('combo-rl', 5);
      expect(await qm.pull('combo-rl', 0)).not.toBeNull();
      expect(await qm.pull('combo-rl', 0)).toBeNull();
    });
  });

  // ============ Batch Pull ============

  describe('rate limit with batch pull (PULLB)', () => {
    test('respects rate limit in batch pull', async () => {
      await pushJobs('rl-batch', 10);
      qm.setRateLimit('rl-batch', 3);
      expect((await qm.pullBatch('rl-batch', 10, 0)).length).toBe(3);
    });

    test('respects concurrency limit in batch pull', async () => {
      await pushJobs('cc-batch', 10);
      qm.setConcurrency('cc-batch', 4);
      expect((await qm.pullBatch('cc-batch', 10, 0)).length).toBe(4);
    });

    test('respects both limits in batch pull (stricter wins)', async () => {
      await pushJobs('combo-batch', 10);
      qm.setRateLimit('combo-batch', 5);
      qm.setConcurrency('combo-batch', 3);
      expect((await qm.pullBatch('combo-batch', 10, 0)).length).toBe(3);
    });

    test('batch pull returns correct jobs within rate limit', async () => {
      await pushJobs('rl-batch-data', 5);
      qm.setRateLimit('rl-batch-data', 2);
      const jobs = await qm.pullBatch('rl-batch-data', 5, 0);
      expect(jobs.length).toBe(2);
      expect(jobs[0].data).toEqual({ index: 0 });
      expect(jobs[1].data).toEqual({ index: 1 });
    });

    test('subsequent batch pulls blocked when rate limit exhausted', async () => {
      await pushJobs('rl-batch-seq', 10);
      qm.setRateLimit('rl-batch-seq', 3);
      expect((await qm.pullBatch('rl-batch-seq', 5, 0)).length).toBe(3);
      expect((await qm.pullBatch('rl-batch-seq', 5, 0)).length).toBe(0);
    });
  });
});
