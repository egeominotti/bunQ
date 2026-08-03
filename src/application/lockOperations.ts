/**
 * Lock Operations - Job lock creation, verification, renewal
 */

import type { JobId, JobLock, LockToken } from '../domain/types/job';
import { createJobLock, isLockExpired, renewLock, DEFAULT_LOCK_TTL } from '../domain/types/job';
import type { LockContext } from './types';

/**
 * Create a lock for a job when it's pulled for processing.
 * @returns The lock token, or null if job not in processing
 */
export function createLock(
  jobId: JobId,
  owner: string,
  ctx: LockContext,
  ttl: number = DEFAULT_LOCK_TTL
): LockToken | null {
  const loc = ctx.jobIndex.get(jobId);
  if (loc?.type !== 'processing') return null;

  const existing = ctx.jobLocks.get(jobId);
  if (existing) {
    const job = ctx.processingShards[loc.shardIdx].get(jobId);
    const isNewGeneration =
      job?.startedAt !== null && job?.startedAt !== undefined && job.startedAt > existing.createdAt;
    if (!isNewGeneration) return null;

    // Stall retry deliberately leaves the previous lease as a stale-outcome
    // guard. Once that job is pulled again, replace the lease and detach the
    // old connection before the new connection registers ownership.
    ctx.jobLocks.delete(jobId);
    for (const [clientId, jobs] of ctx.clientJobs) {
      if (!jobs.delete(jobId)) continue;
      if (jobs.size === 0) ctx.clientJobs.delete(clientId);
    }
  }

  const lock = createJobLock(jobId, owner, ttl);
  ctx.jobLocks.set(jobId, lock);
  return lock.token;
}

/**
 * Verify that a token is valid for a job.
 * @returns true if token matches the active lock
 */
export function verifyLock(jobId: JobId, token: string, ctx: LockContext): boolean {
  const lock = ctx.jobLocks.get(jobId);
  if (!lock) return false;
  if (lock.token !== token) return false;
  if (isLockExpired(lock)) return false;
  return true;
}

/**
 * Renew a lock with the given token.
 * @returns true if renewal succeeded, false if token invalid or lock expired
 */
export function renewJobLock(
  jobId: JobId,
  token: string,
  ctx: LockContext,
  newTtl?: number
): boolean {
  const lock = ctx.jobLocks.get(jobId);
  if (!lock) return false;
  if (lock.token !== token) return false;
  if (isLockExpired(lock)) {
    // Lock already expired, remove it
    ctx.jobLocks.delete(jobId);
    return false;
  }

  renewLock(lock, newTtl);

  // Also update lastHeartbeat on the job (for legacy stall detection compatibility)
  const loc = ctx.jobIndex.get(jobId);
  if (loc?.type === 'processing') {
    const job = ctx.processingShards[loc.shardIdx].get(jobId);
    if (job) job.lastHeartbeat = Date.now();
  }

  return true;
}

/**
 * Renew locks for multiple jobs (batch operation).
 * @returns Array of jobIds that were successfully renewed
 */
export function renewJobLockBatch(
  items: Array<{ id: JobId; token: string; ttl?: number }>,
  ctx: LockContext
): string[] {
  const renewed: string[] = [];
  for (const item of items) {
    if (renewJobLock(item.id, item.token, ctx, item.ttl)) {
      renewed.push(String(item.id));
    }
  }
  return renewed;
}

/**
 * Release a lock when job is completed or failed.
 * Should be called by ACK/FAIL operations.
 */
export function releaseLock(jobId: JobId, ctx: LockContext, token?: string): boolean {
  const lock = ctx.jobLocks.get(jobId);
  if (!lock) return true; // No lock to release

  // If token provided, verify it matches
  if (token && lock.token !== token) {
    return false;
  }

  ctx.jobLocks.delete(jobId);
  return true;
}

/**
 * Get lock info for a job (for debugging/monitoring).
 */
export function getLockInfo(jobId: JobId, ctx: LockContext): JobLock | null {
  return ctx.jobLocks.get(jobId) ?? null;
}
