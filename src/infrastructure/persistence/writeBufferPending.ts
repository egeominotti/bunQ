import type { Job, JobId } from '../../domain/types/job';
import { setBufferedJobState, type PersistedJobState } from './sqliteSerializer';

export type PendingBuffers = readonly [active: Job[], flushing: Job[]];

export function findPendingJob(buffers: PendingBuffers, jobId: JobId): Job | null {
  for (const buffer of buffers) {
    for (const job of buffer) if (job.id === jobId) return job;
  }
  return null;
}

export function setPendingJobState(
  buffers: PendingBuffers,
  jobId: JobId,
  state: PersistedJobState
): boolean {
  let found = false;
  for (const buffer of buffers) {
    for (const job of buffer) {
      if (job.id !== jobId) continue;
      setBufferedJobState(job, state);
      found = true;
    }
  }
  return found;
}

export function listPendingJobs(buffers: PendingBuffers, queue?: string): Job[] {
  const jobs = buffers[0].concat(buffers[1]);
  return queue === undefined ? jobs : jobs.filter((job) => job.queue === queue);
}

export function removePendingJob(buffers: PendingBuffers, jobId: JobId): [Job[], Job[]] {
  return buffers.map((buffer) => buffer.filter((job) => job.id !== jobId)) as [Job[], Job[]];
}

export function removePendingQueue(
  buffers: PendingBuffers,
  queue: string
): { buffers: [Job[], Job[]]; removed: JobId[] } {
  const removed = new Set<JobId>();
  const next = buffers.map((buffer) =>
    buffer.filter((job) => {
      if (job.queue !== queue) return true;
      removed.add(job.id);
      return false;
    })
  ) as [Job[], Job[]];
  return { buffers: next, removed: Array.from(removed) };
}
