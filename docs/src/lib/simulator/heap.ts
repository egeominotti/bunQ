// 4-ary min-heap — same branching factor as bunqueue's PriorityQueue.
// Shallower than a binary heap, so sift-down touches fewer levels.

export class MinHeap<T> {
  private items: T[] = [];

  constructor(private readonly less: (a: T, b: T) => boolean) {}

  get size(): number {
    return this.items.length;
  }

  peek(): T | undefined {
    return this.items[0];
  }

  push(value: T): void {
    this.items.push(value);
    this.siftUp(this.items.length - 1);
  }

  pop(): T | undefined {
    const { items } = this;
    if (items.length === 0) return undefined;
    const top = items[0];
    const last = items.pop() as T;
    if (items.length > 0) {
      items[0] = last;
      this.siftDown(0);
    }
    return top;
  }

  clear(): T[] {
    const removed = this.items;
    this.items = [];
    return removed;
  }

  toArray(): T[] {
    return [...this.items];
  }

  private siftUp(i: number): void {
    const { items, less } = this;
    const value = items[i];
    while (i > 0) {
      const parent = (i - 1) >> 2;
      if (!less(value, items[parent])) break;
      items[i] = items[parent];
      i = parent;
    }
    items[i] = value;
  }

  private siftDown(i: number): void {
    const { items, less } = this;
    const n = items.length;
    const value = items[i];
    for (;;) {
      const first = (i << 2) + 1;
      if (first >= n) break;
      let best = first;
      const end = Math.min(first + 4, n);
      for (let c = first + 1; c < end; c++) {
        if (less(items[c], items[best])) best = c;
      }
      if (!less(items[best], value)) break;
      items[i] = items[best];
      i = best;
    }
    items[i] = value;
  }
}
