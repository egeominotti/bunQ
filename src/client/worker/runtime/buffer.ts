import { sendHeartbeat } from '../workerHeartbeat';
import { WorkerLifecycle } from './lifecycle';
import type { PulledJob, WorkerDelivery } from './state';

export abstract class WorkerBuffer<T = unknown, R = unknown> extends WorkerLifecycle<T, R> {
  protected registerPulledJobs(items: PulledJob[]): WorkerDelivery[] {
    const deliveries = items.map((item) => this.trackDelivery(item));
    if (this.opts.useLocks && items.length > 0 && this.tcpPool && this.tcpPool.getPoolSize() > 1) {
      void sendHeartbeat(this.getHeartbeatDeps());
    }
    return deliveries;
  }

  protected getBufferedJob(): WorkerDelivery | null {
    if (this.pendingJobsHead >= this.pendingJobs.length) return null;
    const item = this.pendingJobs[this.pendingJobsHead++];
    if (this.pendingJobsHead > 500 && this.pendingJobsHead >= this.pendingJobs.length / 2) {
      this.pendingJobs = this.pendingJobs.slice(this.pendingJobsHead);
      this.pendingJobsHead = 0;
    }
    return item;
  }

  protected requeueItem(item: WorkerDelivery): void {
    if (this.pendingJobsHead > 0) {
      this.pendingJobs[--this.pendingJobsHead] = item;
    } else {
      this.pendingJobs.unshift(item);
    }
  }

  protected getNextEligibleJob(): WorkerDelivery | null {
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
