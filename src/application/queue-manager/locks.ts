import type { JobId, JobLock, LockToken } from '../../domain/types/job';
import { DEFAULT_LOCK_TTL } from '../../domain/types/job';
import * as lockMgr from '../lockManager';
import { QueueManagerAck } from './ack';

export class QueueManagerLocks extends QueueManagerAck {
  jobHeartbeat(jobId: JobId, token?: string): boolean {
    const location = this.jobIndex.get(jobId);
    if (location?.type !== 'processing') return false;
    if (token) {
      return lockMgr.renewJobLock(jobId, token, this.contextFactory.getLockContext());
    }
    const job = this.processingShards[location.shardIdx].get(jobId);
    if (!job) return false;
    job.lastHeartbeat = Date.now();
    return true;
  }

  jobHeartbeatBatch(jobIds: JobId[], tokens?: string[]): number {
    let count = 0;
    for (let index = 0; index < jobIds.length; index++) {
      if (this.jobHeartbeat(jobIds[index], tokens?.[index])) count++;
    }
    return count;
  }

  removeLock(jobId: JobId): void {
    this.contextFactory.getLockContext().jobLocks.delete(jobId);
  }

  createLock(jobId: JobId, owner: string, ttl = DEFAULT_LOCK_TTL): LockToken | null {
    return lockMgr.createLock(jobId, owner, this.contextFactory.getLockContext(), ttl);
  }

  verifyLock(jobId: JobId, token: string): boolean {
    return lockMgr.verifyLock(jobId, token, this.contextFactory.getLockContext());
  }

  renewJobLock(jobId: JobId, token: string, newTtl?: number): boolean {
    return lockMgr.renewJobLock(jobId, token, this.contextFactory.getLockContext(), newTtl);
  }

  renewJobLockBatch(items: Array<{ id: JobId; token: string; ttl?: number }>): string[] {
    return lockMgr.renewJobLockBatch(items, this.contextFactory.getLockContext());
  }

  releaseLock(jobId: JobId, token?: string): boolean {
    return lockMgr.releaseLock(jobId, this.contextFactory.getLockContext(), token);
  }

  getLockInfo(jobId: JobId): JobLock | null {
    return lockMgr.getLockInfo(jobId, this.contextFactory.getLockContext());
  }

  registerClientJob(clientId: string, jobId: JobId): void {
    lockMgr.registerClientJob(clientId, jobId, this.contextFactory.getLockContext());
  }

  unregisterClientJob(clientId: string | undefined, jobId: JobId): void {
    lockMgr.unregisterClientJob(clientId, jobId, this.contextFactory.getLockContext());
  }

  releaseClientJobs(clientId: string): Promise<number> {
    return lockMgr.releaseClientJobs(clientId, this.contextFactory.getLockContext());
  }

  forceReleaseClientJobs(clientId: string): number {
    return lockMgr.forceReleaseClientJobs(clientId, this.contextFactory.getLockContext());
  }
}
