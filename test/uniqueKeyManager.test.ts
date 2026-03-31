/**
 * UniqueKeyManager Unit Tests
 * Tests all methods: register, registerWithTtl, getEntry, isAvailable,
 * release, extendTtl, cleanExpired, clearQueue, getMap
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { UniqueKeyManager } from '../src/domain/queue/uniqueKeyManager';
import type { JobId } from '../src/domain/types/job';

const jobId = (id: string) => id as JobId;
const sleep = (ms: number) => Bun.sleep(ms);

describe('UniqueKeyManager', () => {
  let manager: UniqueKeyManager;

  beforeEach(() => {
    manager = new UniqueKeyManager();
  });

  // ──────────────────────────────────────────
  // register (without TTL)
  // ──────────────────────────────────────────
  describe('register', () => {
    test('registers a unique key without TTL', () => {
      manager.register('q1', 'key-a', jobId('job-1'));

      const entry = manager.getEntry('q1', 'key-a');
      expect(entry).not.toBeNull();
      expect(entry!.jobId).toBe('job-1');
      expect(entry!.expiresAt).toBeNull();
      expect(entry!.registeredAt).toBeGreaterThan(0);
    });

    test('key without TTL never expires', async () => {
      manager.register('q1', 'key-a', jobId('job-1'));

      // Even after a short wait the key should still be present
      await sleep(50);
      const entry = manager.getEntry('q1', 'key-a');
      expect(entry).not.toBeNull();
      expect(entry!.jobId).toBe('job-1');
    });

    test('re-registering the same key updates it', () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      manager.register('q1', 'key-a', jobId('job-2'));

      const entry = manager.getEntry('q1', 'key-a');
      expect(entry).not.toBeNull();
      expect(entry!.jobId).toBe('job-2');
    });
  });

  // ──────────────────────────────────────────
  // registerWithTtl
  // ──────────────────────────────────────────
  describe('registerWithTtl', () => {
    test('registers a key with TTL', () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 5000);

      const entry = manager.getEntry('q1', 'key-a');
      expect(entry).not.toBeNull();
      expect(entry!.jobId).toBe('job-1');
      expect(entry!.expiresAt).toBeGreaterThan(Date.now());
    });

    test('registers a key with undefined TTL (no expiry)', () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), undefined);

      const entry = manager.getEntry('q1', 'key-a');
      expect(entry).not.toBeNull();
      expect(entry!.expiresAt).toBeNull();
    });

    test('key with short TTL expires', async () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 30);

      // Immediately should still be there
      expect(manager.getEntry('q1', 'key-a')).not.toBeNull();

      await sleep(60);

      // After TTL passes, getEntry should return null
      expect(manager.getEntry('q1', 'key-a')).toBeNull();
    });

    test('re-registering with TTL updates existing entry', () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 5000);
      manager.registerWithTtl('q1', 'key-a', jobId('job-2'), 10000);

      const entry = manager.getEntry('q1', 'key-a');
      expect(entry).not.toBeNull();
      expect(entry!.jobId).toBe('job-2');
    });

    test('creates queue map on first registration', () => {
      expect(manager.getMap().has('new-queue')).toBe(false);

      manager.registerWithTtl('new-queue', 'key-a', jobId('job-1'), 1000);

      expect(manager.getMap().has('new-queue')).toBe(true);
    });
  });

  // ──────────────────────────────────────────
  // getEntry
  // ──────────────────────────────────────────
  describe('getEntry', () => {
    test('returns null for non-existent queue', () => {
      expect(manager.getEntry('no-queue', 'key-a')).toBeNull();
    });

    test('returns null for non-existent key in existing queue', () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      expect(manager.getEntry('q1', 'key-b')).toBeNull();
    });

    test('returns entry for valid non-expired key', () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 5000);

      const entry = manager.getEntry('q1', 'key-a');
      expect(entry).not.toBeNull();
      expect(entry!.jobId).toBe('job-1');
    });

    test('returns null for expired key and auto-cleans it', async () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 30);

      await sleep(60);

      // Should return null
      expect(manager.getEntry('q1', 'key-a')).toBeNull();

      // The key should have been removed from the internal map
      const queueMap = manager.getMap().get('q1');
      expect(queueMap?.has('key-a')).toBeFalsy();
    });

    test('returns entry with correct registeredAt timestamp', () => {
      const before = Date.now();
      manager.register('q1', 'key-a', jobId('job-1'));
      const after = Date.now();

      const entry = manager.getEntry('q1', 'key-a');
      expect(entry!.registeredAt).toBeGreaterThanOrEqual(before);
      expect(entry!.registeredAt).toBeLessThanOrEqual(after);
    });
  });

  // ──────────────────────────────────────────
  // isAvailable
  // ──────────────────────────────────────────
  describe('isAvailable', () => {
    test('returns true for non-existent queue', () => {
      expect(manager.isAvailable('no-queue', 'key-a')).toBe(true);
    });

    test('returns true for non-existent key', () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      expect(manager.isAvailable('q1', 'key-b')).toBe(true);
    });

    test('returns false for registered non-expired key', () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      expect(manager.isAvailable('q1', 'key-a')).toBe(false);
    });

    test('returns true for expired key and auto-cleans it', async () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 30);

      await sleep(60);

      expect(manager.isAvailable('q1', 'key-a')).toBe(true);

      // Verify it was cleaned from the map
      const queueMap = manager.getMap().get('q1');
      expect(queueMap?.has('key-a')).toBeFalsy();
    });

    test('returns false for key with TTL that has not expired', () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 5000);
      expect(manager.isAvailable('q1', 'key-a')).toBe(false);
    });
  });

  // ──────────────────────────────────────────
  // release (remove)
  // ──────────────────────────────────────────
  describe('release', () => {
    test('removes an existing key', () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      manager.release('q1', 'key-a');

      expect(manager.getEntry('q1', 'key-a')).toBeNull();
      expect(manager.isAvailable('q1', 'key-a')).toBe(true);
    });

    test('is a no-op for non-existent key', () => {
      // Should not throw
      manager.release('q1', 'key-a');
    });

    test('is a no-op for non-existent queue', () => {
      // Should not throw
      manager.release('no-queue', 'key-a');
    });

    test('only removes the specified key, not others', () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      manager.register('q1', 'key-b', jobId('job-2'));

      manager.release('q1', 'key-a');

      expect(manager.getEntry('q1', 'key-a')).toBeNull();
      expect(manager.getEntry('q1', 'key-b')).not.toBeNull();
    });
  });

  // ──────────────────────────────────────────
  // extendTtl
  // ──────────────────────────────────────────
  describe('extendTtl', () => {
    test('returns false for non-existent key', () => {
      expect(manager.extendTtl('q1', 'key-a', 5000)).toBe(false);
    });

    test('returns false for non-existent queue', () => {
      expect(manager.extendTtl('no-queue', 'key-a', 5000)).toBe(false);
    });

    test('returns true for existing key and extends TTL', () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 1000);

      const result = manager.extendTtl('q1', 'key-a', 10000);
      expect(result).toBe(true);

      const entry = manager.getEntry('q1', 'key-a');
      expect(entry).not.toBeNull();
      // The new expiresAt should be approximately now + 10000
      expect(entry!.expiresAt).toBeGreaterThan(Date.now() + 5000);
    });

    test('extends TTL on a key that originally had no TTL', () => {
      manager.register('q1', 'key-a', jobId('job-1'));

      // Originally no expiry
      expect(manager.getEntry('q1', 'key-a')!.expiresAt).toBeNull();

      const result = manager.extendTtl('q1', 'key-a', 5000);
      expect(result).toBe(true);

      // Now has an expiration
      expect(manager.getEntry('q1', 'key-a')!.expiresAt).not.toBeNull();
    });

    test('prevents expiration when TTL is extended', async () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 50);

      // Extend before it expires
      await sleep(20);
      manager.extendTtl('q1', 'key-a', 5000);

      // Wait past original expiry
      await sleep(60);

      // Should still be alive due to extended TTL
      expect(manager.getEntry('q1', 'key-a')).not.toBeNull();
    });
  });

  // ──────────────────────────────────────────
  // cleanExpired
  // ──────────────────────────────────────────
  describe('cleanExpired', () => {
    test('returns 0 when no keys exist', () => {
      expect(manager.cleanExpired()).toBe(0);
    });

    test('returns 0 when no keys are expired', () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      manager.registerWithTtl('q1', 'key-b', jobId('job-2'), 10000);

      expect(manager.cleanExpired()).toBe(0);
    });

    test('removes expired entries and returns count', async () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 30);
      manager.registerWithTtl('q1', 'key-b', jobId('job-2'), 30);
      manager.register('q1', 'key-c', jobId('job-3')); // no TTL

      await sleep(60);

      const cleaned = manager.cleanExpired();
      expect(cleaned).toBe(2);

      // Non-expiring key should remain
      expect(manager.getEntry('q1', 'key-c')).not.toBeNull();
    });

    test('cleans expired entries across multiple queues', async () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 30);
      manager.registerWithTtl('q2', 'key-b', jobId('job-2'), 30);
      manager.registerWithTtl('q3', 'key-c', jobId('job-3'), 10000); // not expired

      await sleep(60);

      const cleaned = manager.cleanExpired();
      expect(cleaned).toBe(2);

      // The long-TTL key should remain
      expect(manager.getEntry('q3', 'key-c')).not.toBeNull();
    });

    test('does not remove keys without TTL', async () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      manager.register('q1', 'key-b', jobId('job-2'));

      await sleep(50);

      expect(manager.cleanExpired()).toBe(0);
      expect(manager.getEntry('q1', 'key-a')).not.toBeNull();
      expect(manager.getEntry('q1', 'key-b')).not.toBeNull();
    });
  });

  // ──────────────────────────────────────────
  // clearQueue
  // ──────────────────────────────────────────
  describe('clearQueue', () => {
    test('removes all keys for a specific queue', () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      manager.register('q1', 'key-b', jobId('job-2'));
      manager.register('q2', 'key-c', jobId('job-3'));

      manager.clearQueue('q1');

      expect(manager.getEntry('q1', 'key-a')).toBeNull();
      expect(manager.getEntry('q1', 'key-b')).toBeNull();
      // Other queue unaffected
      expect(manager.getEntry('q2', 'key-c')).not.toBeNull();
    });

    test('is a no-op for non-existent queue', () => {
      // Should not throw
      manager.clearQueue('no-queue');
    });

    test('queue map is fully removed', () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      manager.clearQueue('q1');

      expect(manager.getMap().has('q1')).toBe(false);
    });
  });

  // ──────────────────────────────────────────
  // getMap
  // ──────────────────────────────────────────
  describe('getMap', () => {
    test('returns empty map when no keys registered', () => {
      const map = manager.getMap();
      expect(map.size).toBe(0);
    });

    test('returns map with correct structure', () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      manager.register('q2', 'key-b', jobId('job-2'));

      const map = manager.getMap();
      expect(map.size).toBe(2);
      expect(map.has('q1')).toBe(true);
      expect(map.has('q2')).toBe(true);
      expect(map.get('q1')!.size).toBe(1);
      expect(map.get('q2')!.size).toBe(1);
    });
  });

  // ──────────────────────────────────────────
  // Size tracking (via getMap)
  // ──────────────────────────────────────────
  describe('size tracking', () => {
    test('tracks total keys across queues', () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      manager.register('q1', 'key-b', jobId('job-2'));
      manager.register('q2', 'key-c', jobId('job-3'));

      const map = manager.getMap();
      let total = 0;
      for (const queueKeys of map.values()) {
        total += queueKeys.size;
      }
      expect(total).toBe(3);
    });

    test('size decreases after release', () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      manager.register('q1', 'key-b', jobId('job-2'));

      manager.release('q1', 'key-a');

      expect(manager.getMap().get('q1')!.size).toBe(1);
    });

    test('size decreases after cleanExpired', async () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 30);
      manager.registerWithTtl('q1', 'key-b', jobId('job-2'), 30);
      manager.register('q1', 'key-c', jobId('job-3'));

      await sleep(60);

      manager.cleanExpired();

      expect(manager.getMap().get('q1')!.size).toBe(1);
    });

    test('re-registering same key does not increase size', () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      manager.register('q1', 'key-a', jobId('job-2'));

      expect(manager.getMap().get('q1')!.size).toBe(1);
    });
  });

  // ──────────────────────────────────────────
  // Multi-queue isolation
  // ──────────────────────────────────────────
  describe('multi-queue isolation', () => {
    test('same key in different queues are independent', () => {
      manager.register('q1', 'shared-key', jobId('job-1'));
      manager.register('q2', 'shared-key', jobId('job-2'));

      const entry1 = manager.getEntry('q1', 'shared-key');
      const entry2 = manager.getEntry('q2', 'shared-key');

      expect(entry1!.jobId).toBe('job-1');
      expect(entry2!.jobId).toBe('job-2');
    });

    test('releasing key in one queue does not affect another', () => {
      manager.register('q1', 'shared-key', jobId('job-1'));
      manager.register('q2', 'shared-key', jobId('job-2'));

      manager.release('q1', 'shared-key');

      expect(manager.getEntry('q1', 'shared-key')).toBeNull();
      expect(manager.getEntry('q2', 'shared-key')).not.toBeNull();
    });

    test('clearing one queue does not affect another', () => {
      manager.register('q1', 'key-a', jobId('job-1'));
      manager.register('q2', 'key-b', jobId('job-2'));

      manager.clearQueue('q1');

      expect(manager.getMap().has('q1')).toBe(false);
      expect(manager.getEntry('q2', 'key-b')).not.toBeNull();
    });

    test('isAvailable is scoped to queue', () => {
      manager.register('q1', 'key-a', jobId('job-1'));

      expect(manager.isAvailable('q1', 'key-a')).toBe(false);
      expect(manager.isAvailable('q2', 'key-a')).toBe(true);
    });

    test('extendTtl is scoped to queue', () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 1000);

      expect(manager.extendTtl('q1', 'key-a', 5000)).toBe(true);
      expect(manager.extendTtl('q2', 'key-a', 5000)).toBe(false);
    });
  });

  // ──────────────────────────────────────────
  // Edge cases
  // ──────────────────────────────────────────
  describe('edge cases', () => {
    test('TTL of 0 expires immediately', async () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 0);

      // TTL of 0 means expiresAt = now, which is <= now, so expired
      await sleep(5);
      expect(manager.getEntry('q1', 'key-a')).toBeNull();
    });

    test('handles empty string queue name', () => {
      manager.register('', 'key-a', jobId('job-1'));
      expect(manager.getEntry('', 'key-a')).not.toBeNull();
    });

    test('handles empty string key', () => {
      manager.register('q1', '', jobId('job-1'));
      expect(manager.getEntry('q1', '')).not.toBeNull();
    });

    test('many keys in one queue', () => {
      for (let i = 0; i < 100; i++) {
        manager.register('q1', `key-${i}`, jobId(`job-${i}`));
      }

      expect(manager.getMap().get('q1')!.size).toBe(100);

      // Spot check
      expect(manager.getEntry('q1', 'key-0')!.jobId).toBe('job-0');
      expect(manager.getEntry('q1', 'key-99')!.jobId).toBe('job-99');
    });

    test('many queues', () => {
      for (let i = 0; i < 50; i++) {
        manager.register(`queue-${i}`, 'key-a', jobId(`job-${i}`));
      }

      expect(manager.getMap().size).toBe(50);
    });

    test('cleanExpired after all keys have expired leaves empty maps', async () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 30);
      manager.registerWithTtl('q1', 'key-b', jobId('job-2'), 30);

      await sleep(60);

      manager.cleanExpired();

      // The queue map entry still exists but is empty
      const queueMap = manager.getMap().get('q1');
      expect(queueMap).toBeDefined();
      expect(queueMap!.size).toBe(0);
    });

    test('getEntry auto-cleans expired keys on access', async () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 30);

      await sleep(60);

      // Access triggers cleanup
      manager.getEntry('q1', 'key-a');

      // Verify the key is gone from the internal map
      expect(manager.getMap().get('q1')!.has('key-a')).toBe(false);
    });

    test('isAvailable auto-cleans expired keys on access', async () => {
      manager.registerWithTtl('q1', 'key-a', jobId('job-1'), 30);

      await sleep(60);

      // Access triggers cleanup
      manager.isAvailable('q1', 'key-a');

      // Verify the key is gone from the internal map
      expect(manager.getMap().get('q1')!.has('key-a')).toBe(false);
    });
  });
});
