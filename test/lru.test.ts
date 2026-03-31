/**
 * LRU Cache Tests
 * LRUMap, LRUSet, and TTLMap implementations
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { LRUMap, LRUSet, TTLMap } from '../src/shared/lru';

describe('LRUMap', () => {
  describe('basic operations', () => {
    test('should set and get values', () => {
      const cache = new LRUMap<string, number>(10);
      cache.set('a', 1);
      cache.set('b', 2);

      expect(cache.get('a')).toBe(1);
      expect(cache.get('b')).toBe(2);
    });

    test('should return undefined for missing keys', () => {
      const cache = new LRUMap<string, number>(10);
      expect(cache.get('missing')).toBeUndefined();
    });

    test('should check key existence', () => {
      const cache = new LRUMap<string, number>(10);
      cache.set('a', 1);

      expect(cache.has('a')).toBe(true);
      expect(cache.has('b')).toBe(false);
    });

    test('should delete keys', () => {
      const cache = new LRUMap<string, number>(10);
      cache.set('a', 1);

      expect(cache.delete('a')).toBe(true);
      expect(cache.has('a')).toBe(false);
      expect(cache.delete('a')).toBe(false);
    });

    test('should clear all entries', () => {
      const cache = new LRUMap<string, number>(10);
      cache.set('a', 1);
      cache.set('b', 2);
      cache.clear();

      expect(cache.size).toBe(0);
      expect(cache.has('a')).toBe(false);
    });
  });

  describe('LRU eviction', () => {
    test('should evict oldest entry when full', () => {
      const cache = new LRUMap<string, number>(3);
      cache.set('a', 1);
      cache.set('b', 2);
      cache.set('c', 3);
      cache.set('d', 4); // Should evict 'a'

      expect(cache.has('a')).toBe(false);
      expect(cache.has('b')).toBe(true);
      expect(cache.has('c')).toBe(true);
      expect(cache.has('d')).toBe(true);
    });

    test('should update access order on get', () => {
      const cache = new LRUMap<string, number>(3);
      cache.set('a', 1);
      cache.set('b', 2);
      cache.set('c', 3);

      cache.get('a'); // Access 'a', making it most recent

      cache.set('d', 4); // Should evict 'b' (oldest now)

      expect(cache.has('a')).toBe(true);
      expect(cache.has('b')).toBe(false);
      expect(cache.has('c')).toBe(true);
      expect(cache.has('d')).toBe(true);
    });

    test('should update access order on set existing key', () => {
      const cache = new LRUMap<string, number>(3);
      cache.set('a', 1);
      cache.set('b', 2);
      cache.set('c', 3);

      cache.set('a', 10); // Update 'a', making it most recent

      cache.set('d', 4); // Should evict 'b'

      expect(cache.has('a')).toBe(true);
      expect(cache.get('a')).toBe(10);
      expect(cache.has('b')).toBe(false);
    });

    test('should call onEvict callback', () => {
      const evicted: Array<[string, number]> = [];
      const cache = new LRUMap<string, number>(2, (k, v) => evicted.push([k, v]));

      cache.set('a', 1);
      cache.set('b', 2);
      cache.set('c', 3); // Evicts 'a'

      expect(evicted).toEqual([['a', 1]]);
    });
  });

  describe('iteration', () => {
    test('should iterate keys', () => {
      const cache = new LRUMap<string, number>(10);
      cache.set('a', 1);
      cache.set('b', 2);

      expect([...cache.keys()]).toEqual(['a', 'b']);
    });

    test('should iterate values', () => {
      const cache = new LRUMap<string, number>(10);
      cache.set('a', 1);
      cache.set('b', 2);

      expect([...cache.values()]).toEqual([1, 2]);
    });

    test('should iterate entries', () => {
      const cache = new LRUMap<string, number>(10);
      cache.set('a', 1);
      cache.set('b', 2);

      expect([...cache.entries()]).toEqual([
        ['a', 1],
        ['b', 2],
      ]);
    });

    test('should support forEach', () => {
      const cache = new LRUMap<string, number>(10);
      cache.set('a', 1);
      cache.set('b', 2);

      const results: Array<[number, string]> = [];
      cache.forEach((v, k) => results.push([v, k]));

      expect(results).toEqual([
        [1, 'a'],
        [2, 'b'],
      ]);
    });
  });
});

describe('LRUSet', () => {
  describe('basic operations', () => {
    test('should add and check values', () => {
      const set = new LRUSet<string>(10);
      set.add('a');
      set.add('b');

      expect(set.has('a')).toBe(true);
      expect(set.has('b')).toBe(true);
      expect(set.has('c')).toBe(false);
    });

    test('should delete values', () => {
      const set = new LRUSet<string>(10);
      set.add('a');

      expect(set.delete('a')).toBe(true);
      expect(set.has('a')).toBe(false);
    });

    test('should clear all values', () => {
      const set = new LRUSet<string>(10);
      set.add('a');
      set.add('b');
      set.clear();

      expect(set.size).toBe(0);
    });
  });

  describe('LRU eviction', () => {
    test('should evict oldest when full', () => {
      const set = new LRUSet<string>(3);
      set.add('a');
      set.add('b');
      set.add('c');
      set.add('d'); // Evicts 'a'

      expect(set.has('a')).toBe(false);
      expect(set.has('d')).toBe(true);
    });

    test('should update access order on re-add', () => {
      const set = new LRUSet<string>(3);
      set.add('a');
      set.add('b');
      set.add('c');
      set.add('a'); // Re-add 'a', making it most recent

      set.add('d'); // Should evict 'b'

      expect(set.has('a')).toBe(true);
      expect(set.has('b')).toBe(false);
    });

    test('should call onEvict callback', () => {
      const evicted: string[] = [];
      const set = new LRUSet<string>(2, (v) => evicted.push(v));

      set.add('a');
      set.add('b');
      set.add('c'); // Evicts 'a'

      expect(evicted).toEqual(['a']);
    });
  });

  describe('iteration', () => {
    test('should iterate values', () => {
      const set = new LRUSet<string>(10);
      set.add('a');
      set.add('b');

      expect([...set.values()]).toEqual(['a', 'b']);
    });

    test('should support for-of', () => {
      const set = new LRUSet<string>(10);
      set.add('a');
      set.add('b');

      expect([...set]).toEqual(['a', 'b']);
    });
  });
});

describe('TTLMap', () => {
  let ttlMap: TTLMap<string, number>;

  afterEach(() => {
    ttlMap?.stop();
  });

  describe('basic operations', () => {
    test('should set and get values', () => {
      ttlMap = new TTLMap<string, number>(10000, 60000);
      ttlMap.set('a', 1);
      ttlMap.set('b', 2);

      expect(ttlMap.get('a')).toBe(1);
      expect(ttlMap.get('b')).toBe(2);
    });

    test('should return undefined for missing keys', () => {
      ttlMap = new TTLMap<string, number>(10000, 60000);
      expect(ttlMap.get('missing')).toBeUndefined();
    });

    test('should check key existence', () => {
      ttlMap = new TTLMap<string, number>(10000, 60000);
      ttlMap.set('a', 1);

      expect(ttlMap.has('a')).toBe(true);
      expect(ttlMap.has('b')).toBe(false);
    });

    test('should delete keys', () => {
      ttlMap = new TTLMap<string, number>(10000, 60000);
      ttlMap.set('a', 1);

      expect(ttlMap.delete('a')).toBe(true);
      expect(ttlMap.has('a')).toBe(false);
    });

    test('should clear all entries', () => {
      ttlMap = new TTLMap<string, number>(10000, 60000);
      ttlMap.set('a', 1);
      ttlMap.set('b', 2);
      ttlMap.clear();

      expect(ttlMap.size).toBe(0);
    });
  });

  describe('TTL expiration', () => {
    test('should expire entries after TTL', async () => {
      ttlMap = new TTLMap<string, number>(50, 60000); // 50ms TTL
      ttlMap.set('a', 1);

      expect(ttlMap.get('a')).toBe(1);

      await Bun.sleep(100);

      expect(ttlMap.get('a')).toBeUndefined();
    });

    test('should support custom TTL per entry', async () => {
      ttlMap = new TTLMap<string, number>(10000, 60000); // Default 10s
      ttlMap.set('short', 1, 50); // 50ms TTL
      ttlMap.set('long', 2, 5000); // 5s TTL

      await Bun.sleep(100);

      expect(ttlMap.get('short')).toBeUndefined();
      expect(ttlMap.get('long')).toBe(2);
    });

    test('should handle has() with expired entries', async () => {
      ttlMap = new TTLMap<string, number>(50, 60000);
      ttlMap.set('a', 1);

      expect(ttlMap.has('a')).toBe(true);

      await Bun.sleep(100);

      expect(ttlMap.has('a')).toBe(false);
    });
  });

  describe('cleanup', () => {
    test('should stop cleanup interval', () => {
      ttlMap = new TTLMap<string, number>(1000, 100);
      ttlMap.stop();
      // Should not throw or cause issues
      ttlMap.set('a', 1);
      expect(ttlMap.get('a')).toBe(1);
    });
  });
});
