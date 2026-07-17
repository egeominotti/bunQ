/**
 * Cleanup shared ownership state when a management operation claims an active
 * job before its worker can ACK or FAIL it.
 */

import type { JobId, JobLock } from '../../domain/types/job';

export interface JobClaimContext {
  jobLocks: Map<JobId, JobLock>;
  clientJobs: Map<string, Set<JobId>>;
}

/**
 * Release the live lease and detach the job from its TCP client owner.
 *
 * Map/Set deletion is intentionally idempotent: management commands can race
 * with disconnect cleanup, but ownership must be removed exactly once from the
 * observable state.
 */
export function releaseClaimedJobOwnership(jobId: JobId, ctx: JobClaimContext): void {
  ctx.jobLocks.delete(jobId);

  for (const [clientId, jobs] of ctx.clientJobs) {
    if (!jobs.delete(jobId)) continue;
    if (jobs.size === 0) ctx.clientJobs.delete(clientId);
  }
}
