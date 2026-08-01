import {
  collectSkipRange,
  createSkipHead,
  iterateSkipValues,
  randomSkipLevel,
  removeMatchingSkipValues,
} from './skipListNodes';
import type { SkipNode } from './types/skipList';

/**
 * Skip List with O(log n) operations
 * - insert: O(log n)
 * - delete: O(log n)
 * - search: O(log n)
 * - range query: O(log n + k) where k = results
 */
export class SkipList<T> {
  private readonly maxLevel: number;
  private readonly probability: number;
  private readonly compare: (a: T, b: T) => number;
  private readonly equals?: (a: T, b: T) => boolean;
  private head: SkipNode<T>;
  private level: number = 0;
  private _size: number = 0;

  /**
   * Create a skip list
   * @param compare Comparator function (negative if a < b, positive if a > b, 0 if equal)
   * @param maxLevel Maximum level (default 16, supports ~65k elements efficiently)
   * @param probability Level promotion probability (default 0.5)
   * @param equals Optional equality function for duplicate detection. If provided,
   *               duplicates where equals returns true are skipped during insert.
   *               If not provided, multiple entries with same comparator result are allowed.
   */
  constructor(
    compare: (a: T, b: T) => number,
    maxLevel: number = 16,
    probability: number = 0.5,
    equals?: (a: T, b: T) => boolean
  ) {
    this.compare = compare;
    this.maxLevel = maxLevel;
    this.probability = probability;
    this.equals = equals;
    this.head = createSkipHead<T>(maxLevel);
  }

  /** Current number of elements */
  get size(): number {
    return this._size;
  }

  /** Check if empty */
  get isEmpty(): boolean {
    return this._size === 0;
  }

  /** Generate random level for new node */
  private randomLevel(): number {
    return randomSkipLevel(this.probability, this.maxLevel);
  }

  /**
   * Insert value - O(log n)
   * If an equals function was provided to the constructor, duplicates
   * (where equals returns true) are skipped. Otherwise, multiple entries
   * with the same comparator result are allowed (sorted multiset behavior).
   * @returns true if inserted, false if duplicate was skipped
   */
  insert(value: T): boolean {
    const update: Array<SkipNode<T> | null> = [];
    for (let i = 0; i <= this.maxLevel; i++) {
      update.push(null);
    }
    let current = this.head;

    // Find insert position at each level
    for (let i = this.level; i >= 0; i--) {
      let fwd = current.forward[i];
      while (fwd !== null && this.compare(fwd.value, value) < 0) {
        current = fwd;
        fwd = current.forward[i];
      }
      update[i] = current;
    }

    // Check for duplicate if equals function is provided
    if (this.equals) {
      // Check all nodes with same comparator result for true equality
      let node = current.forward[0];
      while (node !== null && this.compare(node.value, value) === 0) {
        if (this.equals(node.value, value)) {
          return false; // True duplicate found - skip insertion
        }
        node = node.forward[0];
      }
    }

    // Generate level for new node
    const newLevel = this.randomLevel();

    // If new level is higher than current, update head
    if (newLevel > this.level) {
      for (let i = this.level + 1; i <= newLevel; i++) {
        update[i] = this.head;
      }
      this.level = newLevel;
    }

    // Create new node
    const newNodeForward: Array<SkipNode<T> | null> = [];
    for (let i = 0; i <= newLevel; i++) {
      newNodeForward.push(null);
    }
    const newNode: SkipNode<T> = {
      value,
      forward: newNodeForward,
    };

    // Insert at each level
    for (let i = 0; i <= newLevel; i++) {
      const updateNode = update[i];
      if (updateNode) {
        newNode.forward[i] = updateNode.forward[i];
        updateNode.forward[i] = newNode;
      }
    }

    this._size++;
    return true;
  }

  /**
   * Delete value - O(log n)
   * @returns true if found and deleted
   */
  delete(value: T): boolean {
    const update: Array<SkipNode<T> | null> = [];
    for (let i = 0; i <= this.maxLevel; i++) {
      update.push(null);
    }
    let current = this.head;

    // Find node at each level
    for (let i = this.level; i >= 0; i--) {
      let fwd = current.forward[i];
      while (fwd !== null && this.compare(fwd.value, value) < 0) {
        current = fwd;
        fwd = current.forward[i];
      }
      update[i] = current;
    }

    // Check if found
    const target = current.forward[0];
    if (target === null || this.compare(target.value, value) !== 0) {
      return false;
    }

    // Remove from each level
    for (let i = 0; i <= this.level; i++) {
      const updateNode = update[i];
      if (updateNode?.forward[i] === target) {
        updateNode.forward[i] = target.forward[i];
      } else {
        break;
      }
    }

    // Reduce level if necessary
    while (this.level > 0 && this.head.forward[this.level] === null) {
      this.level--;
    }

    this._size--;
    return true;
  }

  /**
   * Delete by predicate - O(n) worst case, but typically O(log n + k)
   * Useful when you need to match by a subset of fields
   */
  deleteWhere(predicate: (value: T) => boolean): T | null {
    let current = this.head.forward[0];
    while (current !== null) {
      if (predicate(current.value)) {
        this.delete(current.value);
        return current.value;
      }
      current = current.forward[0];
    }
    return null;
  }

  /**
   * Find exact value - O(log n)
   */
  find(value: T): T | null {
    let current = this.head;

    for (let i = this.level; i >= 0; i--) {
      let fwd = current.forward[i];
      while (fwd !== null && this.compare(fwd.value, value) < 0) {
        current = fwd;
        fwd = current.forward[i];
      }
    }

    const target = current.forward[0];
    if (target !== null && this.compare(target.value, value) === 0) {
      return target.value;
    }
    return null;
  }

  /**
   * Check if value exists - O(log n)
   */
  has(value: T): boolean {
    return this.find(value) !== null;
  }

  /**
   * Get first (minimum) element - O(1)
   */
  first(): T | null {
    return this.head.forward[0]?.value ?? null;
  }

  /**
   * Remove and return first element - O(log n)
   */
  shift(): T | null {
    const first = this.head.forward[0];
    if (first === null) return null;
    this.delete(first.value);
    return first.value;
  }

  /**
   * Range query: get all values where comparator returns <= 0
   * O(log n + k) where k = number of results
   * @param maxValue Upper bound (inclusive)
   * @param limit Maximum number of results
   */
  rangeUntil(maxValue: T, limit?: number): T[] {
    return collectSkipRange(
      this.head.forward[0],
      (value) => this.compare(value, maxValue) <= 0,
      limit
    );
  }

  /**
   * Get all values where predicate is true, starting from first
   * Stops at first false (assumes sorted order matches predicate)
   * O(k) where k = matching results
   */
  takeWhile(predicate: (value: T) => boolean, limit?: number): T[] {
    return collectSkipRange(this.head.forward[0], predicate, limit);
  }

  /**
   * Iterate all values in order
   */
  *values(): Generator<T> {
    yield* iterateSkipValues(this.head.forward[0]);
  }

  /**
   * Convert to array
   */
  toArray(): T[] {
    return Array.from(this.values());
  }

  /**
   * Clear all elements
   */
  clear(): void {
    this.head = createSkipHead<T>(this.maxLevel);
    this.level = 0;
    this._size = 0;
  }

  /**
   * Filter and remove elements matching predicate
   * Returns removed elements
   * O(n) but removes in-place
   */
  removeAll(predicate: (value: T) => boolean): T[] {
    return removeMatchingSkipValues(this.head.forward[0], predicate, (value) => {
      this.delete(value);
    });
  }
}
