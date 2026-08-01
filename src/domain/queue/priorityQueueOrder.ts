import type { HeapEntry } from '../types/priorityQueue';

/** Priority, LIFO/FIFO, run time, then UUIDv7 ordering. */
export function comparePriorityEntries(a: HeapEntry, b: HeapEntry): number {
  if (a.priority !== b.priority) return b.priority - a.priority;

  if (a.lifo && b.lifo) {
    if (b.jobId > a.jobId) return 1;
    if (b.jobId < a.jobId) return -1;
    return 0;
  }

  if (a.runAt !== b.runAt) return a.runAt - b.runAt;
  if (a.jobId < b.jobId) return -1;
  if (a.jobId > b.jobId) return 1;
  return 0;
}
