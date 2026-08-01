import type { Job as InternalJob } from '../../../domain/types/job';
import { sendHeartbeat } from '../workerHeartbeat';
import { WorkerLifecycle } from './lifecycle';

export abstract class WorkerBuffer<T = unknown, R = unknown> extends WorkerLifecycle<T, R> {
  protected registerPulledJobs(items: Array<{ job: InternalJob; token: string | null }>): void {
    for (const pulledItem of items) {
      const jobIdStr = String(pulledItem.job.id);
      this.pulledJobIds.add(jobIdStr);
      if (this.opts.useLocks && pulledItem.token) {
        this.jobTokens.set(jobIdStr, pulledItem.token);
      }
    }
    if (this.opts.useLocks && items.length > 0 && this.tcpPool && this.tcpPool.getPoolSize() > 1) {
      void sendHeartbeat(this.getHeartbeatDeps());
    }
  }

  protected getBufferedJob(): { job: InternalJob; token: string | null } | null {
    if (this.pendingJobsHead >= this.pendingJobs.length) return null;
    const item = this.pendingJobs[this.pendingJobsHead++];
    if (this.pendingJobsHead > 500 && this.pendingJobsHead >= this.pendingJobs.length / 2) {
      this.pendingJobs = this.pendingJobs.slice(this.pendingJobsHead);
      this.pendingJobsHead = 0;
    }
    return item;
  }

  protected requeueItem(item: { job: InternalJob; token: string | null }): void {
    if (this.pendingJobsHead > 0) {
      this.pendingJobs[--this.pendingJobsHead] = item;
    } else {
      this.pendingJobs.unshift(item);
    }
  }

  protected getNextEligibleJob(): { job: InternalJob; token: string | null } | null {
    if (this.pendingJobsHead >= this.pendingJobs.length) return null;
    if (!this.groupLimiter) return this.getBufferedJob();

    const start = this.pendingJobsHead;
    const end = this.pendingJobs.length;
    for (let index = start; index < end; index++) {
      const item = this.pendingJobs[index];
      if (this.groupLimiter.canProcess(item.job)) {
        this.pendingJobs[index] = this.pendingJobs[this.pendingJobsHead];
        this.pendingJobsHead++;
        if (this.pendingJobsHead > 500 && this.pendingJobsHead >= this.pendingJobs.length / 2) {
          this.pendingJobs = this.pendingJobs.slice(this.pendingJobsHead);
          this.pendingJobsHead = 0;
        }
        return item;
      }
    }
    return null;
  }
}
