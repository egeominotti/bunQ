import type { Job } from '../types/job';

/** Return the most recent persisted processor failure for a job. */
export function lastFailedReason(job: Pick<Job, 'timeline'>): string | undefined {
  for (let index = job.timeline.length - 1; index >= 0; index--) {
    const entry = job.timeline[index];
    if (entry.state === 'failed' && entry.error) return entry.error;
  }
  return undefined;
}
