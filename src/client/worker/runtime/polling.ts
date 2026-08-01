import type { Job as InternalJob } from '../../../domain/types/job';
import { WORKER_CONSTANTS } from '../constants';
import { pullEmbedded, pullTcp, type PullConfig } from '../workerPull';
import { WorkerBuffer } from './buffer';

export abstract class WorkerPolling<T = unknown, R = unknown> extends WorkerBuffer<T, R> {
  protected poll(): void {
    if (!this.running || this._closing) return;

    if (this.activeJobs >= this.opts.concurrency) {
      this.pollTimer = setTimeout(() => {
        this.poll();
      }, 10);
      return;
    }

    if (!this.rateLimiter.canProcessWithinLimit()) {
      const waitTime = this.rateLimiter.getTimeUntilNextSlot();
      this.pollTimer = setTimeout(
        () => {
          this.poll();
        },
        Math.max(waitTime, 10)
      );
      return;
    }

    void this.tryProcess();
  }

  protected async tryProcess(): Promise<void> {
    if (!this.running || this._closing) return;

    try {
      let item = this.getNextEligibleJob();
      if (!item) {
        const items = await this.doPullBatch();
        if (!this.running || this._closing) return;
        if (items.length > 0) {
          this.registerPulledJobs(items);
          if (this.pendingJobsHead >= this.pendingJobs.length) {
            this.pendingJobs = items;
            this.pendingJobsHead = 0;
          } else {
            this.pendingJobs = this.pendingJobs.slice(this.pendingJobsHead).concat(items);
            this.pendingJobsHead = 0;
          }
          item = this.getNextEligibleJob();
        }
      }

      if (item) {
        if (this.activeJobs >= this.opts.concurrency) {
          this.requeueItem(item);
          this.pollTimer = setTimeout(() => {
            this.poll();
          }, 10);
          return;
        }
        this.consecutiveErrors = 0;
        this.startJob(item.job, item.token);
      } else {
        const hasBuffered = this.pendingJobsHead < this.pendingJobs.length;
        if (hasBuffered && this.groupLimiter) {
          this.pollTimer = setTimeout(() => {
            this.poll();
          }, 10);
          return;
        }
        const now = Date.now();
        if (now - this.lastDrainedEmit > 1000) {
          this.lastDrainedEmit = now;
          this.emit('drained');
        }
        const waitTime = this.opts.pollTimeout > 0 ? 10 : this.opts.drainDelay;
        this.pollTimer = setTimeout(() => {
          this.poll();
        }, waitTime);
      }
    } catch (error) {
      if (!this.running) return;
      this.handlePullError(error);
    }
  }

  protected async doPullBatch(): Promise<Array<{ job: InternalJob; token: string | null }>> {
    const groupBlockedBuffer =
      this.groupLimiter !== null && this.pendingJobsHead < this.pendingJobs.length;
    const leased = groupBlockedBuffer ? this.activeJobs : this.pulledJobIds.size;
    const slots = this.opts.concurrency - leased - this.pendingPull;
    const batchSize = Math.min(this.opts.batchSize, slots, 1000);
    if (batchSize <= 0) return [];

    const config = this.getPullConfig();
    this.pendingPull += batchSize;
    try {
      return this.embedded
        ? await pullEmbedded(config, batchSize)
        : await pullTcp(config, this.tcp as NonNullable<typeof this.tcp>, batchSize, this._closing);
    } finally {
      this.pendingPull -= batchSize;
    }
  }

  protected handlePullError(errorValue: unknown): void {
    this.consecutiveErrors++;
    const error = errorValue instanceof Error ? errorValue : new Error(String(errorValue));
    this.emit(
      'error',
      Object.assign(error, {
        queue: this.name,
        consecutiveErrors: this.consecutiveErrors,
        context: 'pull',
      })
    );
    const backoffMs = Math.min(
      WORKER_CONSTANTS.BASE_BACKOFF_MS * Math.pow(2, this.consecutiveErrors - 1),
      WORKER_CONSTANTS.MAX_BACKOFF_MS
    );
    this.pollTimer = setTimeout(() => {
      this.poll();
    }, backoffMs);
  }

  protected abstract startJob(job: InternalJob, token: string | null): void;
  protected abstract getPullConfig(): PullConfig;
}
