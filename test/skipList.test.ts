/**
 * SkipList Tests
 * Probabilistic sorted data structure with O(log n) operations
 */

import { describe, test, expect } from 'bun:test';
import { SkipList } from '../src/shared/skipList';

/** Helper: create a numeric skip list */
function numericList(equals?: (a: number, b: number) => boolean): SkipList<number> {
  return new SkipList<number>((a, b) => a - b, 16, 0.5, equals);
}

/** Helper: create a skip list with object values */
interface Task {
  id: string;
  priority: number;
  name: string;
}

function taskList(withEquals = false): SkipList<Task> {
  const compare = (a: Task, b: Task) => a.priority - b.priority;
  const equals = withEquals ? (a: Task, b: Task) => a.id === b.id : undefined;
  return new SkipList<Task>(compare, 16, 0.5, equals);
}

describe('SkipList', () => {
  // ──────────────────────────────────────────────────────────────────────────
  // Constructor & Properties
  // ──────────────────────────────────────────────────────────────────────────
  describe('constructor and properties', () => {
    test('should create an empty skip list', () => {
      const list = numericList();
      expect(list.size).toBe(0);
      expect(list.isEmpty).toBe(true);
    });

    test('should report correct size after insertions', () => {
      const list = numericList();
      list.insert(10);
      list.insert(20);
      list.insert(30);
      expect(list.size).toBe(3);
      expect(list.isEmpty).toBe(false);
    });

    test('should work with custom maxLevel and probability', () => {
      const list = new SkipList<number>((a, b) => a - b, 4, 0.25);
      for (let i = 0; i < 100; i++) {
        list.insert(i);
      }
      expect(list.size).toBe(100);
      expect(list.toArray()).toEqual(Array.from({ length: 100 }, (_, i) => i));
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // insert
  // ──────────────────────────────────────────────────────────────────────────
  describe('insert', () => {
    test('should insert a single element and return true', () => {
      const list = numericList();
      expect(list.insert(5)).toBe(true);
      expect(list.size).toBe(1);
      expect(list.first()).toBe(5);
    });

    test('should maintain sorted order', () => {
      const list = numericList();
      list.insert(30);
      list.insert(10);
      list.insert(20);
      list.insert(5);
      list.insert(25);
      expect(list.toArray()).toEqual([5, 10, 20, 25, 30]);
    });

    test('should allow duplicates when no equals function is provided', () => {
      const list = numericList();
      expect(list.insert(5)).toBe(true);
      expect(list.insert(5)).toBe(true);
      expect(list.insert(5)).toBe(true);
      expect(list.size).toBe(3);
      expect(list.toArray()).toEqual([5, 5, 5]);
    });

    test('should reject duplicates when equals function is provided', () => {
      const list = numericList((a, b) => a === b);
      expect(list.insert(5)).toBe(true);
      expect(list.insert(5)).toBe(false);
      expect(list.size).toBe(1);
    });

    test('should allow same comparator result but different equality', () => {
      const list = taskList(true);
      const t1: Task = { id: 'a', priority: 1, name: 'Task A' };
      const t2: Task = { id: 'b', priority: 1, name: 'Task B' };
      const t3: Task = { id: 'a', priority: 1, name: 'Task A duplicate' };

      expect(list.insert(t1)).toBe(true);
      expect(list.insert(t2)).toBe(true); // same priority, different id
      expect(list.insert(t3)).toBe(false); // same id as t1 => duplicate
      expect(list.size).toBe(2);
    });

    test('should insert in descending order correctly', () => {
      const list = numericList();
      for (let i = 100; i >= 1; i--) {
        list.insert(i);
      }
      const arr = list.toArray();
      expect(arr.length).toBe(100);
      for (let i = 0; i < 100; i++) {
        expect(arr[i]).toBe(i + 1);
      }
    });

    test('should insert in ascending order correctly', () => {
      const list = numericList();
      for (let i = 1; i <= 100; i++) {
        list.insert(i);
      }
      expect(list.toArray()).toEqual(Array.from({ length: 100 }, (_, i) => i + 1));
    });

    test('should handle negative numbers', () => {
      const list = numericList();
      list.insert(-5);
      list.insert(0);
      list.insert(-10);
      list.insert(5);
      list.insert(-1);
      expect(list.toArray()).toEqual([-10, -5, -1, 0, 5]);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // delete
  // ──────────────────────────────────────────────────────────────────────────
  describe('delete', () => {
    test('should delete an existing element and return true', () => {
      const list = numericList();
      list.insert(10);
      list.insert(20);
      list.insert(30);

      expect(list.delete(20)).toBe(true);
      expect(list.size).toBe(2);
      expect(list.toArray()).toEqual([10, 30]);
    });

    test('should return false when deleting from empty list', () => {
      const list = numericList();
      expect(list.delete(5)).toBe(false);
    });

    test('should return false when element does not exist', () => {
      const list = numericList();
      list.insert(10);
      list.insert(30);
      expect(list.delete(20)).toBe(false);
      expect(list.size).toBe(2);
    });

    test('should delete the first element', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      list.insert(3);

      expect(list.delete(1)).toBe(true);
      expect(list.first()).toBe(2);
      expect(list.size).toBe(2);
    });

    test('should delete the last element', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      list.insert(3);

      expect(list.delete(3)).toBe(true);
      expect(list.toArray()).toEqual([1, 2]);
    });

    test('should delete the only element', () => {
      const list = numericList();
      list.insert(42);

      expect(list.delete(42)).toBe(true);
      expect(list.size).toBe(0);
      expect(list.isEmpty).toBe(true);
      expect(list.first()).toBeNull();
    });

    test('should delete one of many duplicates (no equals)', () => {
      const list = numericList();
      list.insert(5);
      list.insert(5);
      list.insert(5);

      expect(list.delete(5)).toBe(true);
      expect(list.size).toBe(2);
      expect(list.toArray()).toEqual([5, 5]);
    });

    test('should handle repeated delete of all elements', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      list.insert(3);

      expect(list.delete(1)).toBe(true);
      expect(list.delete(2)).toBe(true);
      expect(list.delete(3)).toBe(true);
      expect(list.size).toBe(0);
      expect(list.isEmpty).toBe(true);
    });

    test('should maintain order after multiple deletes', () => {
      const list = numericList();
      for (let i = 1; i <= 10; i++) list.insert(i);

      list.delete(3);
      list.delete(7);
      list.delete(1);
      list.delete(10);

      expect(list.toArray()).toEqual([2, 4, 5, 6, 8, 9]);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // deleteWhere
  // ──────────────────────────────────────────────────────────────────────────
  describe('deleteWhere', () => {
    test('should delete first matching element and return its value', () => {
      const list = taskList();
      list.insert({ id: 'a', priority: 1, name: 'A' });
      list.insert({ id: 'b', priority: 2, name: 'B' });
      list.insert({ id: 'c', priority: 3, name: 'C' });

      const deleted = list.deleteWhere((t) => t.id === 'b');
      expect(deleted).not.toBeNull();
      expect(deleted!.id).toBe('b');
      expect(list.size).toBe(2);
    });

    test('should return null when no match', () => {
      const list = taskList();
      list.insert({ id: 'a', priority: 1, name: 'A' });

      const deleted = list.deleteWhere((t) => t.id === 'z');
      expect(deleted).toBeNull();
      expect(list.size).toBe(1);
    });

    test('should return null on empty list', () => {
      const list = numericList();
      expect(list.deleteWhere((v) => v === 1)).toBeNull();
    });

    test('should only delete the first match', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      list.insert(3);
      list.insert(4);

      // predicate matches both 2 and 4 (even numbers), but only first is deleted
      const deleted = list.deleteWhere((v) => v % 2 === 0);
      expect(deleted).toBe(2);
      expect(list.size).toBe(3);
      expect(list.toArray()).toEqual([1, 3, 4]);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // find
  // ──────────────────────────────────────────────────────────────────────────
  describe('find', () => {
    test('should find an existing element', () => {
      const list = numericList();
      list.insert(10);
      list.insert(20);
      list.insert(30);

      expect(list.find(20)).toBe(20);
    });

    test('should return null for non-existing element', () => {
      const list = numericList();
      list.insert(10);
      list.insert(30);

      expect(list.find(20)).toBeNull();
    });

    test('should return null on empty list', () => {
      const list = numericList();
      expect(list.find(1)).toBeNull();
    });

    test('should find first element', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      list.insert(3);
      expect(list.find(1)).toBe(1);
    });

    test('should find last element', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      list.insert(3);
      expect(list.find(3)).toBe(3);
    });

    test('should find object by comparator match', () => {
      const list = taskList();
      const task: Task = { id: 'x', priority: 5, name: 'Find me' };
      list.insert(task);
      list.insert({ id: 'y', priority: 10, name: 'Other' });

      // find works by comparator, so any task with priority 5 matches
      const found = list.find({ id: 'ignored', priority: 5, name: 'ignored' });
      expect(found).not.toBeNull();
      expect(found!.id).toBe('x');
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // has
  // ──────────────────────────────────────────────────────────────────────────
  describe('has', () => {
    test('should return true for existing element', () => {
      const list = numericList();
      list.insert(42);
      expect(list.has(42)).toBe(true);
    });

    test('should return false for non-existing element', () => {
      const list = numericList();
      list.insert(42);
      expect(list.has(99)).toBe(false);
    });

    test('should return false on empty list', () => {
      const list = numericList();
      expect(list.has(1)).toBe(false);
    });

    test('should return false after element is deleted', () => {
      const list = numericList();
      list.insert(10);
      list.delete(10);
      expect(list.has(10)).toBe(false);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // first
  // ──────────────────────────────────────────────────────────────────────────
  describe('first', () => {
    test('should return null on empty list', () => {
      const list = numericList();
      expect(list.first()).toBeNull();
    });

    test('should return the minimum element', () => {
      const list = numericList();
      list.insert(30);
      list.insert(10);
      list.insert(20);
      expect(list.first()).toBe(10);
    });

    test('should not remove the element', () => {
      const list = numericList();
      list.insert(5);
      expect(list.first()).toBe(5);
      expect(list.first()).toBe(5);
      expect(list.size).toBe(1);
    });

    test('should update after deleting first element', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      list.insert(3);

      list.delete(1);
      expect(list.first()).toBe(2);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // shift
  // ──────────────────────────────────────────────────────────────────────────
  describe('shift', () => {
    test('should return null on empty list', () => {
      const list = numericList();
      expect(list.shift()).toBeNull();
    });

    test('should remove and return the first element', () => {
      const list = numericList();
      list.insert(10);
      list.insert(20);
      list.insert(30);

      expect(list.shift()).toBe(10);
      expect(list.size).toBe(2);
      expect(list.first()).toBe(20);
    });

    test('should drain list when called repeatedly', () => {
      const list = numericList();
      list.insert(3);
      list.insert(1);
      list.insert(2);

      expect(list.shift()).toBe(1);
      expect(list.shift()).toBe(2);
      expect(list.shift()).toBe(3);
      expect(list.shift()).toBeNull();
      expect(list.size).toBe(0);
    });

    test('should work with single element', () => {
      const list = numericList();
      list.insert(99);
      expect(list.shift()).toBe(99);
      expect(list.isEmpty).toBe(true);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // rangeUntil
  // ──────────────────────────────────────────────────────────────────────────
  describe('rangeUntil', () => {
    test('should return empty array on empty list', () => {
      const list = numericList();
      expect(list.rangeUntil(100)).toEqual([]);
    });

    test('should return all elements up to maxValue (inclusive)', () => {
      const list = numericList();
      for (let i = 1; i <= 10; i++) list.insert(i);

      expect(list.rangeUntil(5)).toEqual([1, 2, 3, 4, 5]);
    });

    test('should return all elements when maxValue exceeds max', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      list.insert(3);
      expect(list.rangeUntil(100)).toEqual([1, 2, 3]);
    });

    test('should return empty when maxValue is less than min', () => {
      const list = numericList();
      list.insert(10);
      list.insert(20);
      expect(list.rangeUntil(5)).toEqual([]);
    });

    test('should respect limit parameter', () => {
      const list = numericList();
      for (let i = 1; i <= 10; i++) list.insert(i);

      expect(list.rangeUntil(10, 3)).toEqual([1, 2, 3]);
    });

    test('should return fewer than limit if not enough elements', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);

      expect(list.rangeUntil(100, 10)).toEqual([1, 2]);
    });

    test('should handle limit of 0', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      // limit=0: first element is pushed, then result.length (1) >= 0 triggers break
      // so we get the first matching element
      expect(list.rangeUntil(100, 0)).toEqual([1]);
    });

    test('should handle boundary value exactly matching an element', () => {
      const list = numericList();
      list.insert(10);
      list.insert(20);
      list.insert(30);

      expect(list.rangeUntil(20)).toEqual([10, 20]);
    });

    test('should handle boundary value between elements', () => {
      const list = numericList();
      list.insert(10);
      list.insert(20);
      list.insert(30);

      // 15 is between 10 and 20, comparator of 20 vs 15 is >0, so only 10
      expect(list.rangeUntil(15)).toEqual([10]);
    });

    test('should work with objects', () => {
      const list = taskList();
      list.insert({ id: 'a', priority: 1, name: 'A' });
      list.insert({ id: 'b', priority: 5, name: 'B' });
      list.insert({ id: 'c', priority: 10, name: 'C' });

      const result = list.rangeUntil({ id: '', priority: 5, name: '' });
      expect(result.length).toBe(2);
      expect(result[0].id).toBe('a');
      expect(result[1].id).toBe('b');
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // takeWhile
  // ──────────────────────────────────────────────────────────────────────────
  describe('takeWhile', () => {
    test('should return empty array on empty list', () => {
      const list = numericList();
      expect(list.takeWhile(() => true)).toEqual([]);
    });

    test('should take elements while predicate is true', () => {
      const list = numericList();
      for (let i = 1; i <= 10; i++) list.insert(i);

      expect(list.takeWhile((v) => v <= 5)).toEqual([1, 2, 3, 4, 5]);
    });

    test('should stop at first false', () => {
      const list = numericList();
      list.insert(2);
      list.insert(4);
      list.insert(5); // odd, stops here
      list.insert(6);
      list.insert(8);

      expect(list.takeWhile((v) => v % 2 === 0)).toEqual([2, 4]);
    });

    test('should return all elements if predicate always true', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      list.insert(3);

      expect(list.takeWhile(() => true)).toEqual([1, 2, 3]);
    });

    test('should return empty if predicate immediately false', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);

      expect(list.takeWhile(() => false)).toEqual([]);
    });

    test('should respect limit parameter', () => {
      const list = numericList();
      for (let i = 1; i <= 10; i++) list.insert(i);

      expect(list.takeWhile(() => true, 3)).toEqual([1, 2, 3]);
    });

    test('should respect both predicate and limit', () => {
      const list = numericList();
      for (let i = 1; i <= 10; i++) list.insert(i);

      // predicate matches up to 7, but limit is 3
      expect(list.takeWhile((v) => v <= 7, 3)).toEqual([1, 2, 3]);
    });

    test('should handle limit larger than matching elements', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      list.insert(10); // predicate stops here

      expect(list.takeWhile((v) => v < 5, 100)).toEqual([1, 2]);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // removeAll
  // ──────────────────────────────────────────────────────────────────────────
  describe('removeAll', () => {
    test('should remove all matching elements and return them', () => {
      const list = numericList();
      for (let i = 1; i <= 10; i++) list.insert(i);

      const removed = list.removeAll((v) => v % 2 === 0);
      expect(removed.sort((a, b) => a - b)).toEqual([2, 4, 6, 8, 10]);
      expect(list.size).toBe(5);
      expect(list.toArray()).toEqual([1, 3, 5, 7, 9]);
    });

    test('should return empty array when nothing matches', () => {
      const list = numericList();
      list.insert(1);
      list.insert(3);
      list.insert(5);

      const removed = list.removeAll((v) => v % 2 === 0);
      expect(removed).toEqual([]);
      expect(list.size).toBe(3);
    });

    test('should return empty array on empty list', () => {
      const list = numericList();
      const removed = list.removeAll(() => true);
      expect(removed).toEqual([]);
    });

    test('should remove all elements when predicate always true', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      list.insert(3);

      const removed = list.removeAll(() => true);
      expect(removed.sort((a, b) => a - b)).toEqual([1, 2, 3]);
      expect(list.size).toBe(0);
      expect(list.isEmpty).toBe(true);
    });

    test('should handle removing objects by predicate', () => {
      const list = taskList();
      list.insert({ id: 'a', priority: 1, name: 'keep' });
      list.insert({ id: 'b', priority: 2, name: 'remove' });
      list.insert({ id: 'c', priority: 3, name: 'keep' });
      list.insert({ id: 'd', priority: 4, name: 'remove' });

      const removed = list.removeAll((t) => t.name === 'remove');
      expect(removed.length).toBe(2);
      expect(removed.map((t) => t.id).sort()).toEqual(['b', 'd']);
      expect(list.size).toBe(2);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // clear
  // ──────────────────────────────────────────────────────────────────────────
  describe('clear', () => {
    test('should clear all elements', () => {
      const list = numericList();
      for (let i = 0; i < 50; i++) list.insert(i);

      list.clear();
      expect(list.size).toBe(0);
      expect(list.isEmpty).toBe(true);
      expect(list.first()).toBeNull();
      expect(list.toArray()).toEqual([]);
    });

    test('should be safe to call on empty list', () => {
      const list = numericList();
      list.clear();
      expect(list.size).toBe(0);
    });

    test('should allow insertions after clear', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      list.clear();

      list.insert(10);
      list.insert(20);
      expect(list.size).toBe(2);
      expect(list.toArray()).toEqual([10, 20]);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // toArray
  // ──────────────────────────────────────────────────────────────────────────
  describe('toArray', () => {
    test('should return empty array for empty list', () => {
      const list = numericList();
      expect(list.toArray()).toEqual([]);
    });

    test('should return elements in sorted order', () => {
      const list = numericList();
      list.insert(50);
      list.insert(10);
      list.insert(30);
      list.insert(20);
      list.insert(40);
      expect(list.toArray()).toEqual([10, 20, 30, 40, 50]);
    });

    test('should return a new array each time', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      const a1 = list.toArray();
      const a2 = list.toArray();
      expect(a1).toEqual(a2);
      expect(a1).not.toBe(a2); // different references
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // values() generator / iteration
  // ──────────────────────────────────────────────────────────────────────────
  describe('values (iterator)', () => {
    test('should iterate in sorted order', () => {
      const list = numericList();
      list.insert(30);
      list.insert(10);
      list.insert(20);

      const result: number[] = [];
      for (const v of list.values()) {
        result.push(v);
      }
      expect(result).toEqual([10, 20, 30]);
    });

    test('should yield nothing for empty list', () => {
      const list = numericList();
      const result: number[] = [];
      for (const v of list.values()) {
        result.push(v);
      }
      expect(result).toEqual([]);
    });

    test('should support spread operator via values()', () => {
      const list = numericList();
      list.insert(3);
      list.insert(1);
      list.insert(2);
      expect([...list.values()]).toEqual([1, 2, 3]);
    });

    test('should support Array.from via values()', () => {
      const list = numericList();
      list.insert(5);
      list.insert(3);
      list.insert(7);
      expect(Array.from(list.values())).toEqual([3, 5, 7]);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // Custom compare function behavior
  // ──────────────────────────────────────────────────────────────────────────
  describe('custom compare function', () => {
    test('should sort in descending order with reversed comparator', () => {
      const descList = new SkipList<number>((a, b) => b - a);
      descList.insert(1);
      descList.insert(3);
      descList.insert(2);
      expect(descList.toArray()).toEqual([3, 2, 1]);
      expect(descList.first()).toBe(3);
    });

    test('should sort strings alphabetically', () => {
      const strList = new SkipList<string>((a, b) => a.localeCompare(b));
      strList.insert('banana');
      strList.insert('apple');
      strList.insert('cherry');
      expect(strList.toArray()).toEqual(['apple', 'banana', 'cherry']);
    });

    test('should sort strings by length', () => {
      const strList = new SkipList<string>((a, b) => a.length - b.length);
      strList.insert('cc');
      strList.insert('a');
      strList.insert('bbb');
      expect(strList.toArray()).toEqual(['a', 'cc', 'bbb']);
    });

    test('should work with multi-field object comparator', () => {
      interface Item {
        priority: number;
        timestamp: number;
      }
      const list = new SkipList<Item>((a, b) => {
        if (a.priority !== b.priority) return a.priority - b.priority;
        return a.timestamp - b.timestamp;
      });

      list.insert({ priority: 2, timestamp: 100 });
      list.insert({ priority: 1, timestamp: 200 });
      list.insert({ priority: 2, timestamp: 50 });
      list.insert({ priority: 1, timestamp: 100 });

      const arr = list.toArray();
      expect(arr[0]).toEqual({ priority: 1, timestamp: 100 });
      expect(arr[1]).toEqual({ priority: 1, timestamp: 200 });
      expect(arr[2]).toEqual({ priority: 2, timestamp: 50 });
      expect(arr[3]).toEqual({ priority: 2, timestamp: 100 });
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // Duplicate handling with equals function
  // ──────────────────────────────────────────────────────────────────────────
  describe('duplicate handling with equals', () => {
    test('should detect duplicates across nodes with same comparator result', () => {
      const list = taskList(true); // equals: (a, b) => a.id === b.id
      list.insert({ id: 'x', priority: 5, name: 'first' });
      list.insert({ id: 'y', priority: 5, name: 'second' });
      list.insert({ id: 'x', priority: 5, name: 'duplicate' });

      expect(list.size).toBe(2);
    });

    test('should allow re-insert after delete with equals', () => {
      const list = numericList((a, b) => a === b);
      list.insert(10);
      expect(list.insert(10)).toBe(false);

      list.delete(10);
      expect(list.insert(10)).toBe(true);
      expect(list.size).toBe(1);
    });

    test('should handle many items with same comparator value but different equals', () => {
      const list = new SkipList<{ key: number; val: string }>(
        (a, b) => a.key - b.key,
        16,
        0.5,
        (a, b) => a.key === b.key && a.val === b.val
      );

      list.insert({ key: 1, val: 'a' });
      list.insert({ key: 1, val: 'b' });
      list.insert({ key: 1, val: 'c' });
      list.insert({ key: 1, val: 'a' }); // duplicate
      list.insert({ key: 1, val: 'b' }); // duplicate

      expect(list.size).toBe(3);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // Edge cases: empty list operations
  // ──────────────────────────────────────────────────────────────────────────
  describe('empty list edge cases', () => {
    test('all read operations should be safe on empty list', () => {
      const list = numericList();
      expect(list.size).toBe(0);
      expect(list.isEmpty).toBe(true);
      expect(list.first()).toBeNull();
      expect(list.shift()).toBeNull();
      expect(list.find(1)).toBeNull();
      expect(list.has(1)).toBe(false);
      expect(list.delete(1)).toBe(false);
      expect(list.deleteWhere(() => true)).toBeNull();
      expect(list.rangeUntil(100)).toEqual([]);
      expect(list.takeWhile(() => true)).toEqual([]);
      expect(list.toArray()).toEqual([]);
      expect(list.removeAll(() => true)).toEqual([]);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // Edge cases: single element
  // ──────────────────────────────────────────────────────────────────────────
  describe('single element edge cases', () => {
    test('operations on single-element list', () => {
      const list = numericList();
      list.insert(42);

      expect(list.size).toBe(1);
      expect(list.first()).toBe(42);
      expect(list.find(42)).toBe(42);
      expect(list.has(42)).toBe(true);
      expect(list.rangeUntil(42)).toEqual([42]);
      expect(list.rangeUntil(41)).toEqual([]);
      expect(list.takeWhile((v) => v === 42)).toEqual([42]);
      expect(list.toArray()).toEqual([42]);
    });

    test('shift on single-element list leaves it empty', () => {
      const list = numericList();
      list.insert(7);
      expect(list.shift()).toBe(7);
      expect(list.isEmpty).toBe(true);
    });

    test('removeAll on single-element list', () => {
      const list = numericList();
      list.insert(7);
      const removed = list.removeAll((v) => v === 7);
      expect(removed).toEqual([7]);
      expect(list.isEmpty).toBe(true);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // Large list (stress test)
  // ──────────────────────────────────────────────────────────────────────────
  describe('large list stress tests', () => {
    test('should handle 1000 random insertions and maintain sorted order', () => {
      const list = numericList();
      const values: number[] = [];
      for (let i = 0; i < 1000; i++) {
        const v = Math.floor(Math.random() * 100000);
        values.push(v);
        list.insert(v);
      }

      expect(list.size).toBe(1000);
      const arr = list.toArray();
      for (let i = 1; i < arr.length; i++) {
        expect(arr[i]).toBeGreaterThanOrEqual(arr[i - 1]);
      }
    });

    test('should handle 500 insertions then 500 deletions', () => {
      const list = numericList();
      const values: number[] = [];
      for (let i = 0; i < 500; i++) {
        values.push(i);
        list.insert(i);
      }

      expect(list.size).toBe(500);

      // Delete even numbers
      for (let i = 0; i < 500; i += 2) {
        expect(list.delete(i)).toBe(true);
      }

      expect(list.size).toBe(250);
      const arr = list.toArray();
      for (const v of arr) {
        expect(v % 2).toBe(1);
      }
    });

    test('should drain large list via shift', () => {
      const list = numericList();
      for (let i = 0; i < 200; i++) list.insert(i);

      let prev = -1;
      let count = 0;
      let val = list.shift();
      while (val !== null) {
        expect(val).toBeGreaterThan(prev);
        prev = val;
        count++;
        val = list.shift();
      }
      expect(count).toBe(200);
      expect(list.isEmpty).toBe(true);
    });

    test('should handle interleaved insert and delete', () => {
      const list = numericList();

      // Insert 1-100
      for (let i = 1; i <= 100; i++) list.insert(i);

      // Delete odds
      for (let i = 1; i <= 100; i += 2) list.delete(i);
      expect(list.size).toBe(50);

      // Insert 101-200
      for (let i = 101; i <= 200; i++) list.insert(i);
      expect(list.size).toBe(150);

      // Verify sorted order
      const arr = list.toArray();
      for (let i = 1; i < arr.length; i++) {
        expect(arr[i]).toBeGreaterThanOrEqual(arr[i - 1]);
      }
    });

    test('rangeUntil with limit on large list', () => {
      const list = numericList();
      for (let i = 1; i <= 1000; i++) list.insert(i);

      const result = list.rangeUntil(500, 10);
      expect(result.length).toBe(10);
      expect(result).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });

    test('removeAll on large list', () => {
      const list = numericList();
      for (let i = 1; i <= 100; i++) list.insert(i);

      const removed = list.removeAll((v) => v > 50);
      expect(removed.length).toBe(50);
      expect(list.size).toBe(50);
      expect(list.toArray()).toEqual(Array.from({ length: 50 }, (_, i) => i + 1));
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // Combined operations
  // ──────────────────────────────────────────────────────────────────────────
  describe('combined operations', () => {
    test('insert, find, delete, find again', () => {
      const list = numericList();
      list.insert(10);
      expect(list.find(10)).toBe(10);
      list.delete(10);
      expect(list.find(10)).toBeNull();
    });

    test('clear then re-populate', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      list.insert(3);
      list.clear();

      list.insert(100);
      list.insert(200);
      expect(list.toArray()).toEqual([100, 200]);
      expect(list.size).toBe(2);
    });

    test('shift then insert maintains order', () => {
      const list = numericList();
      list.insert(10);
      list.insert(20);
      list.insert(30);

      list.shift(); // removes 10
      list.insert(5);
      list.insert(25);

      expect(list.toArray()).toEqual([5, 20, 25, 30]);
    });

    test('deleteWhere followed by rangeUntil', () => {
      const list = taskList();
      list.insert({ id: 'a', priority: 1, name: 'A' });
      list.insert({ id: 'b', priority: 2, name: 'B' });
      list.insert({ id: 'c', priority: 3, name: 'C' });
      list.insert({ id: 'd', priority: 4, name: 'D' });

      list.deleteWhere((t) => t.id === 'b');

      const range = list.rangeUntil({ id: '', priority: 3, name: '' });
      expect(range.length).toBe(2);
      expect(range[0].id).toBe('a');
      expect(range[1].id).toBe('c');
    });

    test('removeAll then insert new elements', () => {
      const list = numericList();
      for (let i = 1; i <= 10; i++) list.insert(i);

      list.removeAll((v) => v <= 5);
      expect(list.size).toBe(5);

      list.insert(0);
      list.insert(3);
      expect(list.toArray()).toEqual([0, 3, 6, 7, 8, 9, 10]);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // isEmpty property
  // ──────────────────────────────────────────────────────────────────────────
  describe('isEmpty', () => {
    test('should be true for new list', () => {
      expect(numericList().isEmpty).toBe(true);
    });

    test('should be false after insert', () => {
      const list = numericList();
      list.insert(1);
      expect(list.isEmpty).toBe(false);
    });

    test('should be true after removing all elements', () => {
      const list = numericList();
      list.insert(1);
      list.delete(1);
      expect(list.isEmpty).toBe(true);
    });

    test('should be true after clear', () => {
      const list = numericList();
      list.insert(1);
      list.insert(2);
      list.clear();
      expect(list.isEmpty).toBe(true);
    });
  });
});
