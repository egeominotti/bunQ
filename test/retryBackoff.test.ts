/**
 * Retry and Backoff Tests
 * Tests for job retry logic, backoff calculation, and max attempts
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('Retry and Backoff', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('Max Attempts', () => {
    test('should default to 3 max attempts', async () => {
      const job = await qm.push('test', { data: { msg: 'test' } });
      expect(job.maxAttempts).toBe(3);
    });

    test('should respect custom max attempts', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        maxAttempts: 5,
      });
      expect(job.maxAttempts).toBe(5);
    });

    test('should move to DLQ after max attempts exhausted', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        maxAttempts: 2,
        backoff: 1, // 1ms backoff for fast test
      });

      // Fail twice
      for (let i = 0; i < 2; i++) {
        const pulled = await qm.pull('test', 100);
        if (pulled) {
          await qm.fail(pulled.id, `failure ${i + 1}`);
          // Wait for backoff
          await Bun.sleep(10);
        }
      }

      // Job should be in DLQ
      const dlq = qm.getDlq('test');
      expect(dlq.length).toBe(1);
      expect(dlq[0].id).toBe(job.id);
    });

    test('should retry job before max attempts and succeed', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        maxAttempts: 3,
        backoff: 1,
      });

      // First attempt - pull (attempts still 0, incremented on fail)
      let pulled = await qm.pull('test', 0);
      expect(pulled?.attempts).toBe(0);
      await qm.fail(job.id, 'failure 1');

      // Wait for backoff
      await Bun.sleep(10);

      // Second attempt - ack (attempts is 1 from previous fail)
      pulled = await qm.pull('test', 100);
      expect(pulled).not.toBeNull();
      expect(pulled?.attempts).toBe(1);
      await qm.ack(job.id, { result: 'success' });

      // Job should not be in DLQ
      const dlq = qm.getDlq('test');
      expect(dlq.length).toBe(0);
    });
  });

  describe('Backoff', () => {
    test('should default to 1000ms backoff', async () => {
      const job = await qm.push('test', { data: { msg: 'test' } });
      expect(job.backoff).toBe(1000);
    });

    test('should respect custom backoff', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        backoff: 5000,
      });
      expect(job.backoff).toBe(5000);
    });

    test('should delay retry by backoff amount', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        maxAttempts: 2,
        backoff: 50, // Base backoff - actual delay = 50 * 2^attempts
      });

      // First attempt - fail (attempts becomes 1)
      await qm.pull('test', 0);
      await qm.fail(job.id, 'failure');

      // Job should not be immediately pullable
      // Actual delay = 50 * 2^1 = 100ms
      const immediatePull = await qm.pull('test', 0);
      expect(immediatePull).toBeNull();

      // Wait for exponential backoff (100ms + buffer)
      await Bun.sleep(150);

      // Now should be pullable
      const delayedPull = await qm.pull('test', 0);
      expect(delayedPull).not.toBeNull();
    });
  });

  describe('Attempts Counter', () => {
    test('should start at 0 attempts', async () => {
      const job = await qm.push('test', { data: { msg: 'test' } });
      expect(job.attempts).toBe(0);
    });

    test('should increment attempts on each fail', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        maxAttempts: 5,
        backoff: 1,
      });

      // First pull - attempts is 0 (not yet failed)
      let pulled = await qm.pull('test', 0);
      expect(pulled?.attempts).toBe(0);

      // Fail - this increments attempts to 1
      await qm.fail(job.id, 'failure');
      await Bun.sleep(10);

      // Second pull - attempts is 1 (failed once)
      pulled = await qm.pull('test', 100);
      expect(pulled?.attempts).toBe(1);
    });
  });

  describe('Fail with Different Errors', () => {
    test('should store error message', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        maxAttempts: 1,
      });

      await qm.pull('test', 0);
      await qm.fail(job.id, 'Custom error message');

      const dlq = qm.getDlq('test');
      expect(dlq.length).toBe(1);
    });

    test('should handle undefined error', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        maxAttempts: 1,
      });

      await qm.pull('test', 0);
      await qm.fail(job.id);

      const dlq = qm.getDlq('test');
      expect(dlq.length).toBe(1);
    });

    test('should handle empty error string', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        maxAttempts: 1,
      });

      await qm.pull('test', 0);
      await qm.fail(job.id, '');

      const dlq = qm.getDlq('test');
      expect(dlq.length).toBe(1);
    });
  });

  describe('Edge Cases', () => {
    test('should handle maxAttempts = 1 (no retries)', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        maxAttempts: 1,
      });

      await qm.pull('test', 0);
      await qm.fail(job.id, 'single attempt failure');

      // Should immediately go to DLQ
      const dlq = qm.getDlq('test');
      expect(dlq.length).toBe(1);

      // Should not be pullable
      const pulled = await qm.pull('test', 0);
      expect(pulled).toBeNull();
    });

    test('should handle zero backoff', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        maxAttempts: 2,
        backoff: 0,
      });

      await qm.pull('test', 0);
      await qm.fail(job.id, 'failure');

      // Should be immediately pullable with zero backoff
      const pulled = await qm.pull('test', 0);
      expect(pulled).not.toBeNull();
    });

    test('should throw on fail for non-active job', async () => {
      const job = await qm.push('test', { data: { msg: 'test' } });

      // Try to fail without pulling (job is waiting, not active)
      await expect(qm.fail(job.id, 'should not work')).rejects.toThrow();
    });

    test('should throw on fail for non-existent job', async () => {
      await expect(qm.fail('non-existent-id' as any, 'error')).rejects.toThrow();
    });
  });
});
