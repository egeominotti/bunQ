/**
 * TTL and Expiration Tests
 * Tests for job TTL, timeout, and expiration handling
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('TTL and Expiration', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('Job TTL', () => {
    test('should set TTL on job', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        ttl: 5000,
      });

      expect(job.ttl).toBe(5000);
    });

    test('should not pull expired TTL job', async () => {
      // Push job with very short TTL (already expired)
      const job = await qm.push('test', {
        data: { msg: 'test' },
        ttl: 1, // 1ms TTL
      });

      // Wait for TTL to expire
      await Bun.sleep(10);

      // Job should not be pullable (expired)
      const pulled = await qm.pull('test', 0);
      expect(pulled).toBeNull();
    });

    test('should pull job within TTL', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        ttl: 60000, // 60 seconds
      });

      const pulled = await qm.pull('test', 0);
      expect(pulled?.id).toBe(job.id);
    });

    test('should respect TTL with delayed jobs', async () => {
      // Push delayed job with TTL shorter than delay
      const job = await qm.push('test', {
        data: { msg: 'test' },
        delay: 100,
        ttl: 50, // TTL expires before delay
      });

      // Wait for delay to pass
      await Bun.sleep(150);

      // Job should be expired (TTL < delay)
      const pulled = await qm.pull('test', 0);
      expect(pulled).toBeNull();
    });
  });

  describe('Job Timeout', () => {
    test('should set timeout on job', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        timeout: 30000,
      });

      expect(job.timeout).toBe(30000);
    });

    test('should track job with timeout', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        timeout: 5000,
      });

      const pulled = await qm.pull('test', 0);
      expect(pulled?.timeout).toBe(5000);
    });
  });

  describe('Stall Detection', () => {
    test('should set stall timeout on job', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        stallTimeout: 10000,
      });

      expect(job.stallTimeout).toBe(10000);
    });

    test('should track lastHeartbeat', async () => {
      const job = await qm.push('test', { data: { msg: 'test' } });

      expect(job.lastHeartbeat).toBeDefined();
      expect(job.lastHeartbeat).toBeGreaterThan(0);
    });
  });

  describe('removeOnComplete', () => {
    test('should remove job on completion when flag is set', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        removeOnComplete: true,
      });

      // Pull and ack
      await qm.pull('test', 0);
      await qm.ack(job.id, { result: 'done' });

      // Job should be removed
      const fetched = await qm.getJob(job.id);
      expect(fetched).toBeNull();
    });

    test('should keep job on completion when flag is false', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        removeOnComplete: false,
      });

      // Pull and ack
      await qm.pull('test', 0);
      await qm.ack(job.id, { result: 'done' });

      // Job should still exist (in completed state)
      const stats = qm.getStats();
      expect(stats.completed).toBeGreaterThan(0);
    });
  });

  describe('removeOnFail', () => {
    test('should remove job on failure when flag is set', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        removeOnFail: true,
        maxAttempts: 1,
      });

      // Pull and fail
      await qm.pull('test', 0);
      await qm.fail(job.id, 'error');

      // Job should be removed (not in DLQ when removeOnFail is true)
      const dlq = qm.getDlq('test');
      const inDlq = dlq.some((j) => j.id === job.id);
      expect(inDlq).toBe(false);
    });

    test('should move job to DLQ when removeOnFail is false', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        removeOnFail: false,
        maxAttempts: 1,
      });

      // Pull and fail
      await qm.pull('test', 0);
      await qm.fail(job.id, 'error');

      // Job should be in DLQ
      const dlq = qm.getDlq('test');
      expect(dlq.length).toBeGreaterThan(0);
    });
  });
});

describe('Delayed Jobs', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should not pull delayed job before delay expires', async () => {
    await qm.push('test', {
      data: { msg: 'delayed' },
      delay: 1000,
    });

    const pulled = await qm.pull('test', 0);
    expect(pulled).toBeNull();
  });

  test('should pull delayed job after delay expires', async () => {
    const job = await qm.push('test', {
      data: { msg: 'delayed' },
      delay: 50,
    });

    // Wait for delay
    await Bun.sleep(100);

    const pulled = await qm.pull('test', 0);
    expect(pulled?.id).toBe(job.id);
  });

  test('should promote delayed job to waiting', async () => {
    const job = await qm.push('test', {
      data: { msg: 'delayed' },
      delay: 60000, // Long delay
    });

    // Job should not be pullable
    let pulled = await qm.pull('test', 0);
    expect(pulled).toBeNull();

    // Promote the job
    const promoted = await qm.promote(job.id);
    expect(promoted).toBe(true);

    // Now it should be pullable
    pulled = await qm.pull('test', 0);
    expect(pulled?.id).toBe(job.id);
  });

  test('should move active job to delayed', async () => {
    const job = await qm.push('test', { data: { msg: 'test' } });

    // Pull the job (make it active)
    const pulled = await qm.pull('test', 0);
    expect(pulled?.id).toBe(job.id);

    // Move to delayed
    const moved = await qm.moveToDelayed(job.id, 1000);
    expect(moved).toBe(true);

    // Job should not be pullable now
    const pullAgain = await qm.pull('test', 0);
    expect(pullAgain).toBeNull();
  });
});
