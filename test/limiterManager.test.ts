/**
 * LimiterManager Unit Tests
 * Rate limiting and concurrency control for queues
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { LimiterManager } from '../src/domain/queue/limiterManager';

describe('LimiterManager', () => {
  let manager: LimiterManager;

  beforeEach(() => {
    manager = new LimiterManager();
  });

  // ============ Rate Limiting ============

  describe('setRateLimit', () => {
    test('should set rate limit for a queue', () => {
      manager.setRateLimit('emails', 10);
      const state = manager.getState('emails');
      expect(state.rateLimit).toBe(10);
    });

    test('should overwrite existing rate limit', () => {
      manager.setRateLimit('emails', 10);
      manager.setRateLimit('emails', 20);
      const state = manager.getState('emails');
      expect(state.rateLimit).toBe(20);
    });

    test('should create queue state if it does not exist', () => {
      manager.setRateLimit('new-queue', 5);
      const state = manager.getState('new-queue');
      expect(state.name).toBe('new-queue');
      expect(state.rateLimit).toBe(5);
    });
  });

  describe('clearRateLimit', () => {
    test('should remove rate limit from a queue', () => {
      manager.setRateLimit('emails', 10);
      manager.clearRateLimit('emails');
      const state = manager.getState('emails');
      expect(state.rateLimit).toBeNull();
    });

    test('should allow unlimited requests after clearing', () => {
      manager.setRateLimit('emails', 2);

      // Exhaust the rate limit
      manager.tryAcquireRateLimit('emails');
      manager.tryAcquireRateLimit('emails');
      expect(manager.tryAcquireRateLimit('emails')).toBe(false);

      // Clear the limit
      manager.clearRateLimit('emails');

      // Now should be unlimited
      expect(manager.tryAcquireRateLimit('emails')).toBe(true);
    });

    test('should not throw when clearing non-existent rate limit', () => {
      expect(() => manager.clearRateLimit('nonexistent')).not.toThrow();
    });

    test('should not affect concurrency limit when clearing rate limit', () => {
      manager.setRateLimit('emails', 10);
      manager.setConcurrency('emails', 5);
      manager.clearRateLimit('emails');

      const state = manager.getState('emails');
      expect(state.rateLimit).toBeNull();
      expect(state.concurrencyLimit).toBe(5);
    });
  });

  describe('tryAcquireRateLimit', () => {
    test('should return true when no rate limit is set', () => {
      expect(manager.tryAcquireRateLimit('unlimited')).toBe(true);
    });

    test('should allow first N requests within limit', () => {
      manager.setRateLimit('emails', 3);

      expect(manager.tryAcquireRateLimit('emails')).toBe(true);
      expect(manager.tryAcquireRateLimit('emails')).toBe(true);
      expect(manager.tryAcquireRateLimit('emails')).toBe(true);
    });

    test('should reject request N+1 when limit is exhausted', () => {
      manager.setRateLimit('emails', 3);

      // Consume all 3 tokens
      manager.tryAcquireRateLimit('emails');
      manager.tryAcquireRateLimit('emails');
      manager.tryAcquireRateLimit('emails');

      // 4th should fail
      expect(manager.tryAcquireRateLimit('emails')).toBe(false);
    });

    test('should allow requests again after tokens refill', async () => {
      // RateLimiter uses token bucket: capacity=5, refillRate=5 tokens/sec
      // So after 1 second, all 5 tokens are refilled
      manager.setRateLimit('emails', 5);

      // Exhaust all tokens
      for (let i = 0; i < 5; i++) {
        manager.tryAcquireRateLimit('emails');
      }
      expect(manager.tryAcquireRateLimit('emails')).toBe(false);

      // Wait for token refill (refillRate = capacity per second)
      // With capacity=5, after ~250ms we should have about 1.25 tokens
      await Bun.sleep(300);

      expect(manager.tryAcquireRateLimit('emails')).toBe(true);
    });

    test('should handle limit of 1', () => {
      manager.setRateLimit('singleton', 1);

      expect(manager.tryAcquireRateLimit('singleton')).toBe(true);
      expect(manager.tryAcquireRateLimit('singleton')).toBe(false);
    });
  });

  // ============ Concurrency Limiting ============

  describe('setConcurrency', () => {
    test('should set concurrency limit for a queue', () => {
      manager.setConcurrency('emails', 5);
      const state = manager.getState('emails');
      expect(state.concurrencyLimit).toBe(5);
    });

    test('should update existing concurrency limit', () => {
      manager.setConcurrency('emails', 5);
      manager.setConcurrency('emails', 10);
      const state = manager.getState('emails');
      expect(state.concurrencyLimit).toBe(10);
    });

    test('should create queue state if it does not exist', () => {
      manager.setConcurrency('new-queue', 3);
      const state = manager.getState('new-queue');
      expect(state.name).toBe('new-queue');
      expect(state.concurrencyLimit).toBe(3);
    });

    test('should preserve active count when updating limit', () => {
      manager.setConcurrency('emails', 5);

      // Acquire 3 slots
      manager.tryAcquireConcurrency('emails');
      manager.tryAcquireConcurrency('emails');
      manager.tryAcquireConcurrency('emails');

      // Reduce limit to 2 (active is already 3, above new limit)
      manager.setConcurrency('emails', 2);

      // Should not be able to acquire more since active (3) >= limit (2)
      expect(manager.tryAcquireConcurrency('emails')).toBe(false);

      // After releasing one, active = 2, still >= limit (2)
      manager.releaseConcurrency('emails');
      expect(manager.tryAcquireConcurrency('emails')).toBe(false);

      // After releasing another, active = 1, now < limit (2)
      manager.releaseConcurrency('emails');
      expect(manager.tryAcquireConcurrency('emails')).toBe(true);
    });
  });

  describe('clearConcurrency', () => {
    test('should remove concurrency limit from a queue', () => {
      manager.setConcurrency('emails', 5);
      manager.clearConcurrency('emails');
      const state = manager.getState('emails');
      expect(state.concurrencyLimit).toBeNull();
    });

    test('should allow unlimited concurrency after clearing', () => {
      manager.setConcurrency('emails', 1);

      manager.tryAcquireConcurrency('emails');
      expect(manager.tryAcquireConcurrency('emails')).toBe(false);

      manager.clearConcurrency('emails');

      // Now should be unlimited
      expect(manager.tryAcquireConcurrency('emails')).toBe(true);
    });

    test('should not throw when clearing non-existent concurrency limit', () => {
      expect(() => manager.clearConcurrency('nonexistent')).not.toThrow();
    });

    test('should not affect rate limit when clearing concurrency', () => {
      manager.setRateLimit('emails', 10);
      manager.setConcurrency('emails', 5);
      manager.clearConcurrency('emails');

      const state = manager.getState('emails');
      expect(state.rateLimit).toBe(10);
      expect(state.concurrencyLimit).toBeNull();
    });
  });

  describe('tryAcquireConcurrency', () => {
    test('should return true when no concurrency limit is set', () => {
      expect(manager.tryAcquireConcurrency('unlimited')).toBe(true);
    });

    test('should allow acquiring up to the limit', () => {
      manager.setConcurrency('emails', 3);

      expect(manager.tryAcquireConcurrency('emails')).toBe(true);
      expect(manager.tryAcquireConcurrency('emails')).toBe(true);
      expect(manager.tryAcquireConcurrency('emails')).toBe(true);
    });

    test('should reject when limit is reached', () => {
      manager.setConcurrency('emails', 3);

      manager.tryAcquireConcurrency('emails');
      manager.tryAcquireConcurrency('emails');
      manager.tryAcquireConcurrency('emails');

      expect(manager.tryAcquireConcurrency('emails')).toBe(false);
    });

    test('should handle limit of 1', () => {
      manager.setConcurrency('singleton', 1);

      expect(manager.tryAcquireConcurrency('singleton')).toBe(true);
      expect(manager.tryAcquireConcurrency('singleton')).toBe(false);
    });
  });

  describe('releaseConcurrency', () => {
    test('should allow re-acquiring after release', () => {
      manager.setConcurrency('emails', 1);

      manager.tryAcquireConcurrency('emails');
      expect(manager.tryAcquireConcurrency('emails')).toBe(false);

      manager.releaseConcurrency('emails');
      expect(manager.tryAcquireConcurrency('emails')).toBe(true);
    });

    test('should not go below zero active count', () => {
      manager.setConcurrency('emails', 2);

      // Release without acquiring should not cause negative active count
      manager.releaseConcurrency('emails');
      manager.releaseConcurrency('emails');

      // Should still be able to acquire the full limit
      expect(manager.tryAcquireConcurrency('emails')).toBe(true);
      expect(manager.tryAcquireConcurrency('emails')).toBe(true);
      expect(manager.tryAcquireConcurrency('emails')).toBe(false);
    });

    test('should not throw when releasing non-existent queue', () => {
      expect(() => manager.releaseConcurrency('nonexistent')).not.toThrow();
    });

    test('should handle multiple acquire-release cycles', () => {
      manager.setConcurrency('emails', 2);

      // Cycle 1
      manager.tryAcquireConcurrency('emails');
      manager.tryAcquireConcurrency('emails');
      expect(manager.tryAcquireConcurrency('emails')).toBe(false);
      manager.releaseConcurrency('emails');
      manager.releaseConcurrency('emails');

      // Cycle 2
      expect(manager.tryAcquireConcurrency('emails')).toBe(true);
      expect(manager.tryAcquireConcurrency('emails')).toBe(true);
      expect(manager.tryAcquireConcurrency('emails')).toBe(false);
    });
  });

  // ============ Queue Management ============

  describe('deleteQueue', () => {
    test('should remove all limits for a queue', () => {
      manager.setRateLimit('emails', 10);
      manager.setConcurrency('emails', 5);
      manager.pause('emails');

      manager.deleteQueue('emails');

      // Rate limit should be gone (returns true = no limiter)
      expect(manager.tryAcquireRateLimit('emails')).toBe(true);
      // Concurrency limit should be gone (returns true = no limiter)
      expect(manager.tryAcquireConcurrency('emails')).toBe(true);
      // Queue state should be recreated as fresh (not paused)
      expect(manager.isPaused('emails')).toBe(false);
    });

    test('should remove queue from queue names list', () => {
      manager.setRateLimit('emails', 10);
      manager.setConcurrency('notifications', 5);

      expect(manager.getQueueNames()).toContain('emails');

      manager.deleteQueue('emails');

      expect(manager.getQueueNames()).not.toContain('emails');
      expect(manager.getQueueNames()).toContain('notifications');
    });

    test('should not throw when deleting non-existent queue', () => {
      expect(() => manager.deleteQueue('nonexistent')).not.toThrow();
    });

    test('should not affect other queues', () => {
      manager.setRateLimit('emails', 10);
      manager.setConcurrency('emails', 5);
      manager.setRateLimit('notifications', 20);
      manager.setConcurrency('notifications', 10);

      manager.deleteQueue('emails');

      const notifState = manager.getState('notifications');
      expect(notifState.rateLimit).toBe(20);
      expect(notifState.concurrencyLimit).toBe(10);
    });
  });

  // ============ State Management ============

  describe('getState', () => {
    test('should create default state for new queue', () => {
      const state = manager.getState('new-queue');
      expect(state.name).toBe('new-queue');
      expect(state.paused).toBe(false);
      expect(state.rateLimit).toBeNull();
      expect(state.concurrencyLimit).toBeNull();
      expect(state.activeCount).toBe(0);
    });

    test('should return same state object on repeated calls', () => {
      const state1 = manager.getState('emails');
      const state2 = manager.getState('emails');
      expect(state1).toBe(state2);
    });
  });

  describe('isPaused / pause / resume', () => {
    test('should return false for non-existent queue', () => {
      expect(manager.isPaused('nonexistent')).toBe(false);
    });

    test('should pause a queue', () => {
      manager.pause('emails');
      expect(manager.isPaused('emails')).toBe(true);
    });

    test('should resume a paused queue', () => {
      manager.pause('emails');
      manager.resume('emails');
      expect(manager.isPaused('emails')).toBe(false);
    });
  });

  describe('getQueueNames', () => {
    test('should return empty array initially', () => {
      expect(manager.getQueueNames()).toEqual([]);
    });

    test('should return names of all queues with state', () => {
      manager.getState('emails');
      manager.getState('notifications');
      manager.getState('payments');

      const names = manager.getQueueNames();
      expect(names).toContain('emails');
      expect(names).toContain('notifications');
      expect(names).toContain('payments');
      expect(names.length).toBe(3);
    });
  });

  describe('getStateMap', () => {
    test('should return the underlying map', () => {
      manager.getState('emails');
      const stateMap = manager.getStateMap();
      expect(stateMap).toBeInstanceOf(Map);
      expect(stateMap.has('emails')).toBe(true);
    });
  });

  // ============ Multiple Queues ============

  describe('multiple queues with different limits', () => {
    test('should track rate limits independently per queue', () => {
      manager.setRateLimit('emails', 2);
      manager.setRateLimit('notifications', 3);

      // Exhaust emails
      manager.tryAcquireRateLimit('emails');
      manager.tryAcquireRateLimit('emails');
      expect(manager.tryAcquireRateLimit('emails')).toBe(false);

      // Notifications should still work
      expect(manager.tryAcquireRateLimit('notifications')).toBe(true);
      expect(manager.tryAcquireRateLimit('notifications')).toBe(true);
      expect(manager.tryAcquireRateLimit('notifications')).toBe(true);
      expect(manager.tryAcquireRateLimit('notifications')).toBe(false);
    });

    test('should track concurrency limits independently per queue', () => {
      manager.setConcurrency('emails', 1);
      manager.setConcurrency('notifications', 2);

      // Fill emails
      manager.tryAcquireConcurrency('emails');
      expect(manager.tryAcquireConcurrency('emails')).toBe(false);

      // Notifications should still have capacity
      expect(manager.tryAcquireConcurrency('notifications')).toBe(true);
      expect(manager.tryAcquireConcurrency('notifications')).toBe(true);
      expect(manager.tryAcquireConcurrency('notifications')).toBe(false);
    });

    test('should allow different limit types on same queue', () => {
      manager.setRateLimit('emails', 100);
      manager.setConcurrency('emails', 2);

      const state = manager.getState('emails');
      expect(state.rateLimit).toBe(100);
      expect(state.concurrencyLimit).toBe(2);
    });
  });

  // ============ Edge Cases ============

  describe('edge cases', () => {
    test('rate limit of 0 should reject all requests', () => {
      manager.setRateLimit('blocked', 0);
      expect(manager.tryAcquireRateLimit('blocked')).toBe(false);
    });

    test('concurrency limit of 0 should reject all requests', () => {
      manager.setConcurrency('blocked', 0);
      expect(manager.tryAcquireConcurrency('blocked')).toBe(false);
    });

    test('very large rate limit should allow many requests', () => {
      manager.setRateLimit('bulk', 100000);
      for (let i = 0; i < 1000; i++) {
        expect(manager.tryAcquireRateLimit('bulk')).toBe(true);
      }
    });

    test('very large concurrency limit should allow many acquires', () => {
      manager.setConcurrency('bulk', 100000);
      for (let i = 0; i < 1000; i++) {
        expect(manager.tryAcquireConcurrency('bulk')).toBe(true);
      }
    });

    test('setting rate limit after prior exhaustion resets tokens', () => {
      manager.setRateLimit('emails', 1);
      manager.tryAcquireRateLimit('emails');
      expect(manager.tryAcquireRateLimit('emails')).toBe(false);

      // Setting a new rate limit creates a fresh RateLimiter
      manager.setRateLimit('emails', 1);
      expect(manager.tryAcquireRateLimit('emails')).toBe(true);
    });

    test('setting concurrency limit after acquiring preserves a fresh limiter', () => {
      manager.setConcurrency('emails', 2);
      manager.tryAcquireConcurrency('emails');
      manager.tryAcquireConcurrency('emails');
      expect(manager.tryAcquireConcurrency('emails')).toBe(false);

      // Updating the limit on existing limiter (uses setLimit)
      manager.setConcurrency('emails', 3);
      // Active count is still 2, limit is now 3, so one more should succeed
      expect(manager.tryAcquireConcurrency('emails')).toBe(true);
      expect(manager.tryAcquireConcurrency('emails')).toBe(false);
    });

    test('deleting a queue then re-creating limits works cleanly', () => {
      manager.setRateLimit('emails', 2);
      manager.setConcurrency('emails', 1);
      manager.tryAcquireRateLimit('emails');
      manager.tryAcquireConcurrency('emails');

      manager.deleteQueue('emails');

      // Re-create limits
      manager.setRateLimit('emails', 5);
      manager.setConcurrency('emails', 3);

      const state = manager.getState('emails');
      expect(state.rateLimit).toBe(5);
      expect(state.concurrencyLimit).toBe(3);

      // Fresh state - all tokens/slots available
      for (let i = 0; i < 5; i++) {
        expect(manager.tryAcquireRateLimit('emails')).toBe(true);
      }
      expect(manager.tryAcquireRateLimit('emails')).toBe(false);

      for (let i = 0; i < 3; i++) {
        expect(manager.tryAcquireConcurrency('emails')).toBe(true);
      }
      expect(manager.tryAcquireConcurrency('emails')).toBe(false);
    });

    test('pausing a queue does not affect rate or concurrency limits', () => {
      manager.setRateLimit('emails', 5);
      manager.setConcurrency('emails', 3);
      manager.pause('emails');

      // Limits still function even when paused
      expect(manager.tryAcquireRateLimit('emails')).toBe(true);
      expect(manager.tryAcquireConcurrency('emails')).toBe(true);
      expect(manager.isPaused('emails')).toBe(true);
    });

    test('release concurrency on queue with cleared limiter is no-op', () => {
      manager.setConcurrency('emails', 2);
      manager.tryAcquireConcurrency('emails');
      manager.clearConcurrency('emails');

      // Should not throw
      expect(() => manager.releaseConcurrency('emails')).not.toThrow();
    });

    test('queue state survives rate limit clear and concurrency clear', () => {
      manager.setRateLimit('emails', 10);
      manager.setConcurrency('emails', 5);
      manager.pause('emails');

      manager.clearRateLimit('emails');
      manager.clearConcurrency('emails');

      const state = manager.getState('emails');
      expect(state.paused).toBe(true);
      expect(state.rateLimit).toBeNull();
      expect(state.concurrencyLimit).toBeNull();
      // Queue state still exists
      expect(manager.getQueueNames()).toContain('emails');
    });
  });
});
