/**
 * BoundedSet and BoundedMap Tests
 * FIFO eviction with batch eviction (10% of maxSize)
 */

import { describe, test, expect } from 'bun:test';
import { BoundedSet, BoundedMap } from '../src/shared/lru';

describe('BoundedSet', () => {
  describe('basic operations', () => {
    test('add(value) should add a value to the set', () => {
      const set = new BoundedSet<string>(10);
      set.add('a');
      expect(set.size).toBe(1);
    });

    test('has(value) should return true for existing values', () => {
      const set = new BoundedSet<string>(10);
      set.add('a');
      expect(set.has('a')).toBe(true);
    });

    test('has(value) should return false for non-existing values', () => {
      const set = new BoundedSet<string>(10);
      expect(set.has('missing')).toBe(false);
    });

    test('delete(value) should remove a value and return true', () => {
      const set = new BoundedSet<string>(10);
      set.add('a');
      expect(set.delete('a')).toBe(true);
      expect(set.has('a')).toBe(false);
      expect(set.size).toBe(0);
    });

    test('delete(value) should return false for non-existing values', () => {
      const set = new BoundedSet<string>(10);
      expect(set.delete('missing')).toBe(false);
    });

    test('size should reflect the number of elements', () => {
      const set = new BoundedSet<number>(10);
      expect(set.size).toBe(0);
      set.add(1);
      expect(set.size).toBe(1);
      set.add(2);
      expect(set.size).toBe(2);
      set.add(3);
      expect(set.size).toBe(3);
    });

    test('clear() should remove all elements', () => {
      const set = new BoundedSet<string>(10);
      set.add('a');
      set.add('b');
      set.add('c');
      set.clear();
      expect(set.size).toBe(0);
      expect(set.has('a')).toBe(false);
      expect(set.has('b')).toBe(false);
      expect(set.has('c')).toBe(false);
    });

    test('values() should return an iterator over all values', () => {
      const set = new BoundedSet<string>(10);
      set.add('a');
      set.add('b');
      set.add('c');
      expect([...set.values()]).toEqual(['a', 'b', 'c']);
    });

    test('Symbol.iterator should allow for-of iteration', () => {
      const set = new BoundedSet<string>(10);
      set.add('x');
      set.add('y');
      const result: string[] = [];
      for (const val of set) {
        result.push(val);
      }
      expect(result).toEqual(['x', 'y']);
    });

    test('Symbol.iterator should allow spread syntax', () => {
      const set = new BoundedSet<number>(10);
      set.add(10);
      set.add(20);
      set.add(30);
      expect([...set]).toEqual([10, 20, 30]);
    });
  });

  describe('duplicate handling', () => {
    test('duplicate adds should not increase size', () => {
      const set = new BoundedSet<string>(10);
      set.add('a');
      set.add('a');
      set.add('a');
      expect(set.size).toBe(1);
    });

    test('duplicate adds should not trigger eviction', () => {
      const evicted: string[] = [];
      const set = new BoundedSet<string>(3, (v) => evicted.push(v));
      set.add('a');
      set.add('b');
      set.add('c');
      // Re-adding existing values should not evict
      set.add('a');
      set.add('b');
      set.add('c');
      expect(set.size).toBe(3);
      expect(evicted).toEqual([]);
    });
  });

  describe('batch eviction', () => {
    test('should not evict when adding exactly maxSize elements', () => {
      const evicted: number[] = [];
      const set = new BoundedSet<number>(5, (v) => evicted.push(v));
      for (let i = 0; i < 5; i++) {
        set.add(i);
      }
      expect(set.size).toBe(5);
      expect(evicted).toEqual([]);
    });

    test('should trigger batch eviction when adding maxSize + 1 elements', () => {
      const evicted: number[] = [];
      const set = new BoundedSet<number>(10, (v) => evicted.push(v));
      for (let i = 0; i < 10; i++) {
        set.add(i);
      }
      expect(evicted).toEqual([]);

      // Adding the 11th element triggers eviction of 10% = 1 item
      set.add(10);
      expect(evicted).toEqual([0]); // FIFO: oldest (first inserted) is evicted
      expect(set.has(0)).toBe(false);
      expect(set.has(10)).toBe(true);
      expect(set.size).toBe(10);
    });

    test('should evict 10% of maxSize items (batch eviction)', () => {
      const evicted: number[] = [];
      const set = new BoundedSet<number>(20, (v) => evicted.push(v));
      for (let i = 0; i < 20; i++) {
        set.add(i);
      }
      expect(evicted).toEqual([]);

      // Adding the 21st triggers eviction of 10% of 20 = 2 items
      set.add(20);
      expect(evicted).toEqual([0, 1]);
      expect(set.has(0)).toBe(false);
      expect(set.has(1)).toBe(false);
      expect(set.has(2)).toBe(true);
      expect(set.has(20)).toBe(true);
      // After evicting 2 and adding 1: 20 - 2 + 1 = 19
      expect(set.size).toBe(19);
    });

    test('should evict oldest items first (FIFO order)', () => {
      const evicted: number[] = [];
      const set = new BoundedSet<number>(10, (v) => evicted.push(v));
      for (let i = 0; i < 10; i++) {
        set.add(i);
      }

      set.add(100);
      // 10% of 10 = 1, so evicts 0 (the oldest)
      expect(evicted).toEqual([0]);

      // Fill back up to trigger another eviction
      // Current size: 10 (items 1-9 + 100). Adding another triggers eviction.
      set.add(101);
      expect(evicted).toEqual([0, 1]);
    });

    test('onEvict callback should be called with each evicted value', () => {
      const evicted: string[] = [];
      const set = new BoundedSet<string>(5, (v) => evicted.push(v));
      // evictBatchSize = max(1, floor(5 * 0.1)) = max(1, 0) = 1
      for (let i = 0; i < 5; i++) {
        set.add(`item-${i}`);
      }

      set.add('overflow');
      expect(evicted).toEqual(['item-0']);
    });

    test('should evict minimum 1 item even when 10% rounds to 0', () => {
      // maxSize=5, 10% of 5 = 0.5, floor = 0, but max(1, 0) = 1
      const evicted: number[] = [];
      const set = new BoundedSet<number>(5, (v) => evicted.push(v));
      for (let i = 0; i < 5; i++) {
        set.add(i);
      }
      set.add(5);
      expect(evicted.length).toBe(1);
      expect(evicted[0]).toBe(0);
    });

    test('should handle multiple rounds of eviction', () => {
      const evicted: number[] = [];
      const set = new BoundedSet<number>(10, (v) => evicted.push(v));

      // Fill to capacity
      for (let i = 0; i < 10; i++) set.add(i);

      // First overflow: evicts item 0
      set.add(10);
      expect(evicted).toEqual([0]);
      expect(set.size).toBe(10);

      // Second overflow: evicts item 1
      set.add(11);
      expect(evicted).toEqual([0, 1]);
      expect(set.size).toBe(10);

      // Third overflow: evicts item 2
      set.add(12);
      expect(evicted).toEqual([0, 1, 2]);
      expect(set.size).toBe(10);
    });
  });

  describe('edge cases', () => {
    test('maxSize = 1 should work correctly', () => {
      const evicted: string[] = [];
      const set = new BoundedSet<string>(1, (v) => evicted.push(v));

      set.add('a');
      expect(set.size).toBe(1);
      expect(set.has('a')).toBe(true);
      expect(evicted).toEqual([]);

      set.add('b');
      expect(set.has('a')).toBe(false);
      expect(set.has('b')).toBe(true);
      expect(set.size).toBe(1);
      expect(evicted).toEqual(['a']);
    });

    test('maxSize = 0 should evict immediately on add', () => {
      // evictBatchSize = max(1, floor(0 * 0.1)) = 1
      // With maxSize=0, cache.size (0) >= maxSize (0) is true on first add
      // but there's nothing to evict from an empty set, so it just adds
      const set = new BoundedSet<string>(0);
      set.add('a');
      // The item is added since evictBatch on empty set removes nothing
      // then next add triggers eviction again
      expect(set.size).toBe(1);

      set.add('b');
      // size (1) >= maxSize (0), so evict batch runs, evicts 'a', then adds 'b'
      expect(set.has('a')).toBe(false);
      expect(set.has('b')).toBe(true);
      expect(set.size).toBe(1);
    });

    test('operations on an empty set should not throw', () => {
      const set = new BoundedSet<string>(10);
      expect(set.size).toBe(0);
      expect(set.has('a')).toBe(false);
      expect(set.delete('a')).toBe(false);
      expect([...set.values()]).toEqual([]);
      expect([...set]).toEqual([]);
      set.clear(); // Should not throw
      expect(set.size).toBe(0);
    });

    test('should work with various value types', () => {
      const numSet = new BoundedSet<number>(10);
      numSet.add(42);
      expect(numSet.has(42)).toBe(true);

      const objSet = new BoundedSet<object>(10);
      const obj = { key: 'value' };
      objSet.add(obj);
      expect(objSet.has(obj)).toBe(true);
    });

    test('delete followed by add should work correctly', () => {
      const set = new BoundedSet<string>(3);
      set.add('a');
      set.add('b');
      set.add('c');
      set.delete('b');
      expect(set.size).toBe(2);
      set.add('d');
      expect(set.size).toBe(3);
      expect(set.has('a')).toBe(true);
      expect(set.has('b')).toBe(false);
      expect(set.has('c')).toBe(true);
      expect(set.has('d')).toBe(true);
    });
  });
});

describe('BoundedMap', () => {
  describe('basic operations', () => {
    test('set(key, value) and get(key) should store and retrieve values', () => {
      const map = new BoundedMap<string, number>(10);
      map.set('a', 1);
      map.set('b', 2);
      expect(map.get('a')).toBe(1);
      expect(map.get('b')).toBe(2);
    });

    test('get(key) should return undefined for non-existing keys', () => {
      const map = new BoundedMap<string, number>(10);
      expect(map.get('missing')).toBeUndefined();
    });

    test('has(key) should return true for existing keys', () => {
      const map = new BoundedMap<string, number>(10);
      map.set('a', 1);
      expect(map.has('a')).toBe(true);
    });

    test('has(key) should return false for non-existing keys', () => {
      const map = new BoundedMap<string, number>(10);
      expect(map.has('missing')).toBe(false);
    });

    test('delete(key) should remove the entry and return true', () => {
      const map = new BoundedMap<string, number>(10);
      map.set('a', 1);
      expect(map.delete('a')).toBe(true);
      expect(map.has('a')).toBe(false);
      expect(map.get('a')).toBeUndefined();
      expect(map.size).toBe(0);
    });

    test('delete(key) should return false for non-existing keys', () => {
      const map = new BoundedMap<string, number>(10);
      expect(map.delete('missing')).toBe(false);
    });

    test('size should reflect the number of entries', () => {
      const map = new BoundedMap<string, number>(10);
      expect(map.size).toBe(0);
      map.set('a', 1);
      expect(map.size).toBe(1);
      map.set('b', 2);
      expect(map.size).toBe(2);
      map.set('c', 3);
      expect(map.size).toBe(3);
    });

    test('clear() should remove all entries', () => {
      const map = new BoundedMap<string, number>(10);
      map.set('a', 1);
      map.set('b', 2);
      map.set('c', 3);
      map.clear();
      expect(map.size).toBe(0);
      expect(map.has('a')).toBe(false);
      expect(map.has('b')).toBe(false);
      expect(map.has('c')).toBe(false);
    });
  });

  describe('iteration', () => {
    test('keys() should return an iterator over all keys', () => {
      const map = new BoundedMap<string, number>(10);
      map.set('a', 1);
      map.set('b', 2);
      map.set('c', 3);
      expect([...map.keys()]).toEqual(['a', 'b', 'c']);
    });

    test('values() should return an iterator over all values', () => {
      const map = new BoundedMap<string, number>(10);
      map.set('a', 1);
      map.set('b', 2);
      map.set('c', 3);
      expect([...map.values()]).toEqual([1, 2, 3]);
    });

    test('entries() should return an iterator over [key, value] pairs', () => {
      const map = new BoundedMap<string, number>(10);
      map.set('a', 1);
      map.set('b', 2);
      expect([...map.entries()]).toEqual([
        ['a', 1],
        ['b', 2],
      ]);
    });

    test('forEach(callback) should call the callback for each entry', () => {
      const map = new BoundedMap<string, number>(10);
      map.set('x', 10);
      map.set('y', 20);
      const results: Array<[number, string]> = [];
      map.forEach((value, key) => results.push([value, key]));
      expect(results).toEqual([
        [10, 'x'],
        [20, 'y'],
      ]);
    });

    test('Symbol.iterator should allow for-of iteration', () => {
      const map = new BoundedMap<string, number>(10);
      map.set('a', 1);
      map.set('b', 2);
      const result: Array<[string, number]> = [];
      for (const entry of map) {
        result.push(entry);
      }
      expect(result).toEqual([
        ['a', 1],
        ['b', 2],
      ]);
    });

    test('Symbol.iterator should allow spread syntax', () => {
      const map = new BoundedMap<string, number>(10);
      map.set('a', 1);
      map.set('b', 2);
      expect([...map]).toEqual([
        ['a', 1],
        ['b', 2],
      ]);
    });
  });

  describe('updating existing keys', () => {
    test('updating an existing key should overwrite the value', () => {
      const map = new BoundedMap<string, number>(10);
      map.set('a', 1);
      map.set('a', 99);
      expect(map.get('a')).toBe(99);
      expect(map.size).toBe(1);
    });

    test('updating an existing key should not trigger eviction', () => {
      const evicted: Array<[string, number]> = [];
      const map = new BoundedMap<string, number>(3, (k, v) => evicted.push([k, v]));
      map.set('a', 1);
      map.set('b', 2);
      map.set('c', 3);
      // Map is full. Updating existing keys should not evict.
      map.set('a', 10);
      map.set('b', 20);
      map.set('c', 30);
      expect(evicted).toEqual([]);
      expect(map.size).toBe(3);
      expect(map.get('a')).toBe(10);
      expect(map.get('b')).toBe(20);
      expect(map.get('c')).toBe(30);
    });

    test('updating an existing key should not increase size', () => {
      const map = new BoundedMap<string, number>(5);
      map.set('a', 1);
      map.set('b', 2);
      map.set('a', 100);
      expect(map.size).toBe(2);
    });
  });

  describe('batch eviction', () => {
    test('should not evict when adding exactly maxSize entries', () => {
      const evicted: Array<[number, string]> = [];
      const map = new BoundedMap<number, string>(5, (k, v) => evicted.push([k, v]));
      for (let i = 0; i < 5; i++) {
        map.set(i, `val-${i}`);
      }
      expect(map.size).toBe(5);
      expect(evicted).toEqual([]);
    });

    test('should trigger batch eviction when adding maxSize + 1 entries', () => {
      const evicted: Array<[number, string]> = [];
      const map = new BoundedMap<number, string>(10, (k, v) => evicted.push([k, v]));
      for (let i = 0; i < 10; i++) {
        map.set(i, `val-${i}`);
      }
      expect(evicted).toEqual([]);

      // Adding the 11th triggers eviction of 10% = 1 item
      map.set(10, 'val-10');
      expect(evicted).toEqual([[0, 'val-0']]);
      expect(map.has(0)).toBe(false);
      expect(map.has(10)).toBe(true);
      expect(map.size).toBe(10);
    });

    test('should evict 10% of maxSize entries (batch eviction)', () => {
      const evicted: Array<[number, string]> = [];
      const map = new BoundedMap<number, string>(20, (k, v) => evicted.push([k, v]));
      for (let i = 0; i < 20; i++) {
        map.set(i, `val-${i}`);
      }
      expect(evicted).toEqual([]);

      // Adding the 21st triggers eviction of 10% of 20 = 2 entries
      map.set(20, 'val-20');
      expect(evicted).toEqual([
        [0, 'val-0'],
        [1, 'val-1'],
      ]);
      expect(map.has(0)).toBe(false);
      expect(map.has(1)).toBe(false);
      expect(map.has(2)).toBe(true);
      expect(map.has(20)).toBe(true);
      // After evicting 2 and adding 1: 20 - 2 + 1 = 19
      expect(map.size).toBe(19);
    });

    test('should evict oldest entries first (FIFO order)', () => {
      const evicted: Array<[number, string]> = [];
      const map = new BoundedMap<number, string>(10, (k, v) => evicted.push([k, v]));
      for (let i = 0; i < 10; i++) {
        map.set(i, `val-${i}`);
      }

      map.set(100, 'val-100');
      expect(evicted).toEqual([[0, 'val-0']]);

      map.set(101, 'val-101');
      expect(evicted).toEqual([
        [0, 'val-0'],
        [1, 'val-1'],
      ]);
    });

    test('onEvict callback should be called with each evicted key-value pair', () => {
      const evicted: Array<[string, number]> = [];
      // evictBatchSize = max(1, floor(5 * 0.1)) = 1
      const map = new BoundedMap<string, number>(5, (k, v) => evicted.push([k, v]));
      for (let i = 0; i < 5; i++) {
        map.set(`key-${i}`, i);
      }

      map.set('overflow', 99);
      expect(evicted).toEqual([['key-0', 0]]);
    });

    test('should evict minimum 1 entry even when 10% rounds to 0', () => {
      // maxSize=5, 10% of 5 = 0.5, floor = 0, but max(1, 0) = 1
      const evicted: Array<[number, number]> = [];
      const map = new BoundedMap<number, number>(5, (k, v) => evicted.push([k, v]));
      for (let i = 0; i < 5; i++) {
        map.set(i, i * 10);
      }
      map.set(5, 50);
      expect(evicted.length).toBe(1);
      expect(evicted[0]).toEqual([0, 0]);
    });

    test('should handle multiple rounds of eviction', () => {
      const evicted: Array<[number, number]> = [];
      const map = new BoundedMap<number, number>(10, (k, v) => evicted.push([k, v]));

      for (let i = 0; i < 10; i++) map.set(i, i * 10);

      map.set(10, 100);
      expect(evicted).toEqual([[0, 0]]);
      expect(map.size).toBe(10);

      map.set(11, 110);
      expect(evicted).toEqual([
        [0, 0],
        [1, 10],
      ]);
      expect(map.size).toBe(10);

      map.set(12, 120);
      expect(evicted).toEqual([
        [0, 0],
        [1, 10],
        [2, 20],
      ]);
      expect(map.size).toBe(10);
    });

    test('larger batch eviction with maxSize=100 evicts 10 items', () => {
      const evicted: Array<[number, number]> = [];
      const map = new BoundedMap<number, number>(100, (k, v) => evicted.push([k, v]));
      for (let i = 0; i < 100; i++) {
        map.set(i, i);
      }
      expect(map.size).toBe(100);

      map.set(100, 100);
      // 10% of 100 = 10 items evicted
      expect(evicted.length).toBe(10);
      for (let i = 0; i < 10; i++) {
        expect(evicted[i]).toEqual([i, i]);
        expect(map.has(i)).toBe(false);
      }
      expect(map.has(10)).toBe(true);
      expect(map.has(100)).toBe(true);
      // 100 - 10 + 1 = 91
      expect(map.size).toBe(91);
    });
  });

  describe('edge cases', () => {
    test('maxSize = 1 should work correctly', () => {
      const evicted: Array<[string, number]> = [];
      const map = new BoundedMap<string, number>(1, (k, v) => evicted.push([k, v]));

      map.set('a', 1);
      expect(map.size).toBe(1);
      expect(map.get('a')).toBe(1);
      expect(evicted).toEqual([]);

      map.set('b', 2);
      expect(map.has('a')).toBe(false);
      expect(map.has('b')).toBe(true);
      expect(map.get('b')).toBe(2);
      expect(map.size).toBe(1);
      expect(evicted).toEqual([['a', 1]]);
    });

    test('maxSize = 0 should evict immediately on set of new key', () => {
      const map = new BoundedMap<string, number>(0);
      map.set('a', 1);
      // First add: size (0) >= maxSize (0) triggers evictBatch on empty map (nothing to evict)
      // then adds 'a'
      expect(map.size).toBe(1);

      map.set('b', 2);
      // size (1) >= maxSize (0) triggers evictBatch, evicts 'a', then adds 'b'
      expect(map.has('a')).toBe(false);
      expect(map.has('b')).toBe(true);
      expect(map.size).toBe(1);
    });

    test('operations on an empty map should not throw', () => {
      const map = new BoundedMap<string, number>(10);
      expect(map.size).toBe(0);
      expect(map.get('a')).toBeUndefined();
      expect(map.has('a')).toBe(false);
      expect(map.delete('a')).toBe(false);
      expect([...map.keys()]).toEqual([]);
      expect([...map.values()]).toEqual([]);
      expect([...map.entries()]).toEqual([]);
      expect([...map]).toEqual([]);
      const results: Array<[number, string]> = [];
      map.forEach((v, k) => results.push([v, k]));
      expect(results).toEqual([]);
      map.clear(); // Should not throw
      expect(map.size).toBe(0);
    });

    test('should work with various key/value types', () => {
      const numMap = new BoundedMap<number, string>(10);
      numMap.set(42, 'answer');
      expect(numMap.get(42)).toBe('answer');

      const objMap = new BoundedMap<string, object>(10);
      const obj = { nested: true };
      objMap.set('key', obj);
      expect(objMap.get('key')).toBe(obj);
    });

    test('delete followed by set should work correctly', () => {
      const map = new BoundedMap<string, number>(3);
      map.set('a', 1);
      map.set('b', 2);
      map.set('c', 3);
      map.delete('b');
      expect(map.size).toBe(2);
      map.set('d', 4);
      expect(map.size).toBe(3);
      expect(map.has('a')).toBe(true);
      expect(map.has('b')).toBe(false);
      expect(map.has('c')).toBe(true);
      expect(map.has('d')).toBe(true);
    });

    test('set same key multiple times at capacity should not evict', () => {
      const evicted: Array<[string, number]> = [];
      const map = new BoundedMap<string, number>(2, (k, v) => evicted.push([k, v]));
      map.set('a', 1);
      map.set('b', 2);
      // At capacity, but updating existing key
      map.set('a', 10);
      map.set('a', 20);
      map.set('b', 30);
      expect(evicted).toEqual([]);
      expect(map.size).toBe(2);
    });
  });
});
