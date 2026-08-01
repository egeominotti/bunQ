import type { SkipNode } from './types/skipList';

export function randomSkipLevel(probability: number, maxLevel: number): number {
  let level = 0;
  while (Math.random() < probability && level < maxLevel) level++;
  return level;
}

export function createSkipHead<T>(maxLevel: number): SkipNode<T> {
  const forward: Array<SkipNode<T> | null> = [];
  for (let i = 0; i <= maxLevel; i++) forward.push(null);
  return { value: null as unknown as T, forward };
}

export function collectSkipRange<T>(
  first: SkipNode<T> | null,
  predicate: (value: T) => boolean,
  limit?: number
): T[] {
  const result: T[] = [];
  let current = first;
  while (current !== null && predicate(current.value)) {
    result.push(current.value);
    if (limit !== undefined && result.length >= limit) break;
    current = current.forward[0];
  }
  return result;
}

export function* iterateSkipValues<T>(first: SkipNode<T> | null): Generator<T> {
  let current = first;
  while (current !== null) {
    yield current.value;
    current = current.forward[0];
  }
}

export function removeMatchingSkipValues<T>(
  first: SkipNode<T> | null,
  predicate: (value: T) => boolean,
  remove: (value: T) => void
): T[] {
  const removed: T[] = [];
  let current = first;
  while (current !== null) {
    const next = current.forward[0];
    if (predicate(current.value)) {
      removed.push(current.value);
      remove(current.value);
    }
    current = next;
  }
  return removed;
}
