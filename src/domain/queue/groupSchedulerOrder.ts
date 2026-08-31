import { copyGroupFifoOrder } from '../job/groupFifoOrder';
import type { Job } from '../types/job';
import type { HeapEntry } from '../types/priorityQueue';

export function orderedGroupClone(job: Job, runAt = job.runAt): Job {
  const clone = { ...job, lifo: false, runAt };
  copyGroupFifoOrder(job, clone);
  return clone;
}

export function nextGroupCheckAt(job: Job): number {
  if (job.ttl === null) return job.runAt;
  return Math.min(job.runAt, job.createdAt + job.ttl + 1);
}

function compareOrder(left: bigint, right: bigint): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

export function compareGroupFifoEntries(left: HeapEntry, right: HeapEntry): number {
  if (left.priority !== right.priority) return left.priority - right.priority;
  const order = compareOrder(
    left.groupFifoOrder ?? left.generation,
    right.groupFifoOrder ?? right.generation
  );
  if (order !== 0) return order;
  return left.jobId < right.jobId ? -1 : left.jobId > right.jobId ? 1 : 0;
}

export function compareGroupWakeEntries(left: HeapEntry, right: HeapEntry): number {
  if (left.runAt !== right.runAt) return left.runAt - right.runAt;
  if (left.groupFifoOrder !== undefined && right.groupFifoOrder !== undefined) {
    const order = compareOrder(left.groupFifoOrder, right.groupFifoOrder);
    if (order !== 0) return order;
  }
  return left.jobId < right.jobId ? -1 : left.jobId > right.jobId ? 1 : 0;
}
