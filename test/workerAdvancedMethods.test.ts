/**
 * Worker Advanced Methods Tests - BullMQ v5 Compatible
 * Tests for rateLimit, startStalledCheckTimer, delay, isRateLimited
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker } from '../src/client';
import { shutdownManager } from '../src/client';

describe('Worker Advanced Methods - BullMQ v5', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('worker-advanced-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  describe('rateLimit', () => {
    test('worker.rateLimit should apply rate limiting', async () => {
      const worker = new Worker(
        'worker-advanced-test',
        async () => ({ done: true }),
        { embedded: true, autorun: false }
      );

      // Apply rate limit
      worker.rateLimit(1000);

      // Check if rate limited
      expect(worker.isRateLimited()).toBe(true);

      await worker.close();
    });

    test('worker.rateLimit with 0 should not apply rate limit', async () => {
      const worker = new Worker(
        'worker-advanced-test',
        async () => ({ done: true }),
        { embedded: true, autorun: false }
      );

      // Apply rate limit with 0
      worker.rateLimit(0);

      // Should not be rate limited
      expect(worker.isRateLimited()).toBe(false);

      await worker.close();
    });

    test('worker.isRateLimited should return false when not rate limited', async () => {
      const worker = new Worker(
        'worker-advanced-test',
        async () => ({ done: true }),
        { embedded: true, autorun: false }
      );

      // Should not be rate limited initially
      expect(worker.isRateLimited()).toBe(false);

      await worker.close();
    });

    test('worker.isRateLimited should return false after rate limit expires', async () => {
      const worker = new Worker(
        'worker-advanced-test',
        async () => ({ done: true }),
        { embedded: true, autorun: false }
      );

      // Apply short rate limit
      worker.rateLimit(50);
      expect(worker.isRateLimited()).toBe(true);

      // Wait for rate limit to expire
      await Bun.sleep(100);

      expect(worker.isRateLimited()).toBe(false);

      await worker.close();
    });
  });

  describe('startStalledCheckTimer', () => {
    test('worker.startStalledCheckTimer should be a function', async () => {
      const worker = new Worker(
        'worker-advanced-test',
        async () => ({ done: true }),
        { embedded: true, autorun: false }
      );

      expect(typeof worker.startStalledCheckTimer).toBe('function');

      await worker.close();
    });

    test('worker.startStalledCheckTimer should not throw', async () => {
      const worker = new Worker(
        'worker-advanced-test',
        async () => ({ done: true }),
        { embedded: true, autorun: false }
      );

      // Should not throw
      await expect(worker.startStalledCheckTimer()).resolves.toBeUndefined();

      await worker.close();
    });
  });

  describe('delay', () => {
    test('worker.delay should be a function', async () => {
      const worker = new Worker(
        'worker-advanced-test',
        async () => ({ done: true }),
        { embedded: true, autorun: false }
      );

      expect(typeof worker.delay).toBe('function');

      await worker.close();
    });

    test('worker.delay with 0 should resolve immediately', async () => {
      const worker = new Worker(
        'worker-advanced-test',
        async () => ({ done: true }),
        { embedded: true, autorun: false }
      );

      const start = Date.now();
      await worker.delay(0);
      const elapsed = Date.now() - start;

      // Should be near instant (< 50ms)
      expect(elapsed).toBeLessThan(50);

      await worker.close();
    });

    test('worker.delay should delay for specified time', async () => {
      const worker = new Worker(
        'worker-advanced-test',
        async () => ({ done: true }),
        { embedded: true, autorun: false }
      );

      const start = Date.now();
      await worker.delay(100);
      const elapsed = Date.now() - start;

      // Should be at least 100ms
      expect(elapsed).toBeGreaterThanOrEqual(90);

      await worker.close();
    });

    test('worker.delay should be abortable', async () => {
      const worker = new Worker(
        'worker-advanced-test',
        async () => ({ done: true }),
        { embedded: true, autorun: false }
      );

      const abortController = new AbortController();

      // Start delay and abort after 50ms
      const delayPromise = worker.delay(1000, abortController);

      setTimeout(() => abortController.abort(), 50);

      // Should reject with abort error
      await expect(delayPromise).rejects.toThrow('Delay aborted');

      await worker.close();
    });
  });
});
