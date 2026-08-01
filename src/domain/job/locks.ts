import type { JobId, JobLock } from '../types/jobs/model';
import { DEFAULT_LOCK_TTL } from './constants';
import { generateLockToken } from './ids';

export function createJobLock(
  jobId: JobId,
  owner: string,
  ttl: number = DEFAULT_LOCK_TTL,
  now: number = Date.now()
): JobLock {
  return {
    jobId,
    token: generateLockToken(),
    owner,
    createdAt: now,
    expiresAt: now + ttl,
    lastRenewalAt: now,
    renewalCount: 0,
    ttl,
  };
}

export function isLockExpired(lock: JobLock, now: number = Date.now()): boolean {
  return now >= lock.expiresAt;
}

export function renewLock(lock: JobLock, newTtl?: number, now: number = Date.now()): void {
  const ttl = newTtl ?? lock.ttl;
  lock.expiresAt = now + ttl;
  lock.lastRenewalAt = now;
  lock.renewalCount++;
}
