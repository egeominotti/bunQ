/**
 * MinHeap Tests
 * Generic min-heap data structure used by cron scheduler
 */

import { describe, test, expect } from 'bun:test';
import { MinHeap } from '../src/shared/minHeap';

describe('MinHeap', () => {
  describe('basic operations', () => {
    test('should create empty heap', () => {
      const heap = new MinHeap<number>((a, b) => a - b);
      expect(heap.size).toBe(0);
      expect(heap.isEmpty).toBe(true);
    });

    test('should push and pop single element', () => {
      const heap = new MinHeap<number>((a, b) => a - b);
      heap.push(5);

      expect(heap.size).toBe(1);
      expect(heap.isEmpty).toBe(false);
      expect(heap.pop()).toBe(5);
      expect(heap.isEmpty).toBe(true);
    });

    test('should maintain min-heap property', () => {
      const heap = new MinHeap<number>((a, b) => a - b);
      heap.push(5);
      heap.push(3);
      heap.push(7);
      heap.push(1);
      heap.push(9);

      expect(heap.pop()).toBe(1);
      expect(heap.pop()).toBe(3);
      expect(heap.pop()).toBe(5);
      expect(heap.pop()).toBe(7);
      expect(heap.pop()).toBe(9);
    });

    test('should handle duplicate values', () => {
      const heap = new MinHeap<number>((a, b) => a - b);
      heap.push(3);
      heap.push(3);
      heap.push(1);
      heap.push(3);

      expect(heap.pop()).toBe(1);
      expect(heap.pop()).toBe(3);
      expect(heap.pop()).toBe(3);
      expect(heap.pop()).toBe(3);
    });
  });

  describe('peek', () => {
    test('should return undefined for empty heap', () => {
      const heap = new MinHeap<number>((a, b) => a - b);
      expect(heap.peek()).toBeUndefined();
    });

    test('should return minimum without removing', () => {
      const heap = new MinHeap<number>((a, b) => a - b);
      heap.push(5);
      heap.push(3);
      heap.push(7);

      expect(heap.peek()).toBe(3);
      expect(heap.size).toBe(3);
      expect(heap.peek()).toBe(3); // Still 3
    });
  });

  describe('buildFrom', () => {
    test('should build heap from array', () => {
      const heap = new MinHeap<number>((a, b) => a - b);
      heap.buildFrom([5, 3, 7, 1, 9, 2, 8]);

      expect(heap.size).toBe(7);
      expect(heap.pop()).toBe(1);
      expect(heap.pop()).toBe(2);
      expect(heap.pop()).toBe(3);
    });

    test('should handle empty array', () => {
      const heap = new MinHeap<number>((a, b) => a - b);
      heap.buildFrom([]);

      expect(heap.size).toBe(0);
      expect(heap.isEmpty).toBe(true);
    });

    test('should replace existing content', () => {
      const heap = new MinHeap<number>((a, b) => a - b);
      heap.push(100);
      heap.push(200);
      heap.buildFrom([1, 2, 3]);

      expect(heap.size).toBe(3);
      expect(heap.pop()).toBe(1);
    });
  });

  describe('removeWhere', () => {
    test('should remove matching element', () => {
      const heap = new MinHeap<number>((a, b) => a - b);
      heap.push(5);
      heap.push(3);
      heap.push(7);

      const removed = heap.removeWhere((x) => x === 5);
      expect(removed).toBe(5);
      expect(heap.size).toBe(2);
    });

    test('should return undefined if no match', () => {
      const heap = new MinHeap<number>((a, b) => a - b);
      heap.push(5);
      heap.push(3);

      const removed = heap.removeWhere((x) => x === 99);
      expect(removed).toBeUndefined();
      expect(heap.size).toBe(2);
    });

    test('should maintain heap property after removal', () => {
      const heap = new MinHeap<number>((a, b) => a - b);
      heap.push(1);
      heap.push(5);
      heap.push(3);
      heap.push(7);
      heap.push(2);

      heap.removeWhere((x) => x === 3);

      expect(heap.pop()).toBe(1);
      expect(heap.pop()).toBe(2);
      expect(heap.pop()).toBe(5);
      expect(heap.pop()).toBe(7);
    });
  });

  describe('clear', () => {
    test('should clear all elements', () => {
      const heap = new MinHeap<number>((a, b) => a - b);
      heap.push(1);
      heap.push(2);
      heap.push(3);

      heap.clear();

      expect(heap.size).toBe(0);
      expect(heap.isEmpty).toBe(true);
      expect(heap.peek()).toBeUndefined();
    });
  });

  describe('with objects', () => {
    interface Task {
      id: string;
      priority: number;
    }

    test('should work with custom comparison', () => {
      const heap = new MinHeap<Task>((a, b) => a.priority - b.priority);

      heap.push({ id: 'low', priority: 10 });
      heap.push({ id: 'high', priority: 1 });
      heap.push({ id: 'medium', priority: 5 });

      expect(heap.pop()?.id).toBe('high');
      expect(heap.pop()?.id).toBe('medium');
      expect(heap.pop()?.id).toBe('low');
    });

    test('should remove object by predicate', () => {
      const heap = new MinHeap<Task>((a, b) => a.priority - b.priority);

      heap.push({ id: 'a', priority: 1 });
      heap.push({ id: 'b', priority: 2 });
      heap.push({ id: 'c', priority: 3 });

      const removed = heap.removeWhere((t) => t.id === 'b');
      expect(removed?.id).toBe('b');
      expect(heap.size).toBe(2);
    });
  });

  describe('stress test', () => {
    test('should handle many elements', () => {
      const heap = new MinHeap<number>((a, b) => a - b);
      const count = 1000;

      // Push in random order
      const values = Array.from({ length: count }, () => Math.floor(Math.random() * 10000));
      for (const v of values) {
        heap.push(v);
      }

      expect(heap.size).toBe(count);

      // Pop should give sorted order
      let prev = -Infinity;
      while (!heap.isEmpty) {
        const val = heap.pop()!;
        expect(val).toBeGreaterThanOrEqual(prev);
        prev = val;
      }
    });
  });
});
