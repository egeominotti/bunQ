/**
 * Rate Limiter Tests
 * Protocol-level rate limiting
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { ProtocolRateLimiter } from '../src/infrastructure/server/rateLimiter';

describe('ProtocolRateLimiter', () => {
  let limiter: ProtocolRateLimiter;

  beforeEach(() => {
    limiter = new ProtocolRateLimiter({
      windowMs: 1000,
      maxRequests: 5,
      cleanupIntervalMs: 0, // Disable auto cleanup for tests
    });
  });

  afterEach(() => {
    limiter.stop();
  });

  describe('isAllowed', () => {
    test('should allow requests within limit', () => {
      for (let i = 0; i < 5; i++) {
        expect(limiter.isAllowed('client1')).toBe(true);
      }
    });

    test('should block requests exceeding limit', () => {
      for (let i = 0; i < 5; i++) {
        limiter.isAllowed('client1');
      }
      expect(limiter.isAllowed('client1')).toBe(false);
    });

    test('should track clients separately', () => {
      for (let i = 0; i < 5; i++) {
        limiter.isAllowed('client1');
      }
      expect(limiter.isAllowed('client1')).toBe(false);
      expect(limiter.isAllowed('client2')).toBe(true);
    });

    test('should reset after window expires', async () => {
      for (let i = 0; i < 5; i++) {
        limiter.isAllowed('client1');
      }
      expect(limiter.isAllowed('client1')).toBe(false);

      // Wait for window to expire
      await Bun.sleep(1100);

      expect(limiter.isAllowed('client1')).toBe(true);
    });
  });

  describe('getRemaining', () => {
    test('should return full limit for new client', () => {
      expect(limiter.getRemaining('new-client')).toBe(5);
    });

    test('should decrease as requests are made', () => {
      limiter.isAllowed('client1');
      expect(limiter.getRemaining('client1')).toBe(4);

      limiter.isAllowed('client1');
      expect(limiter.getRemaining('client1')).toBe(3);
    });

    test('should return 0 when limit is exhausted', () => {
      for (let i = 0; i < 5; i++) {
        limiter.isAllowed('client1');
      }
      expect(limiter.getRemaining('client1')).toBe(0);
    });
  });

  describe('removeClient', () => {
    test('should remove client tracking', () => {
      for (let i = 0; i < 5; i++) {
        limiter.isAllowed('client1');
      }
      expect(limiter.isAllowed('client1')).toBe(false);

      limiter.removeClient('client1');
      expect(limiter.isAllowed('client1')).toBe(true);
    });
  });

  describe('default config', () => {
    test('should use default values', () => {
      const defaultLimiter = new ProtocolRateLimiter();
      // Default is 10000 requests per 60 seconds
      for (let i = 0; i < 10000; i++) {
        expect(defaultLimiter.isAllowed('client')).toBe(true);
      }
      // Next request should be rejected (limit reached)
      expect(defaultLimiter.isAllowed('client')).toBe(false);
      defaultLimiter.stop();
    });
  });

  describe('cleanup', () => {
    test('should clean up old entries', async () => {
      const cleaningLimiter = new ProtocolRateLimiter({
        windowMs: 100,
        maxRequests: 5,
        cleanupIntervalMs: 50,
      });

      cleaningLimiter.isAllowed('client1');
      cleaningLimiter.isAllowed('client2');

      // Wait for cleanup
      await Bun.sleep(200);

      // After cleanup, clients should have full quota again
      expect(cleaningLimiter.getRemaining('client1')).toBe(5);
      expect(cleaningLimiter.getRemaining('client2')).toBe(5);

      cleaningLimiter.stop();
    });
  });
});
