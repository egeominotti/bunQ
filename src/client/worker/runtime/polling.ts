import { WORKER_CONSTANTS } from '../constants';
import { pullEmbedded, pullTcp, type PullConfig } from '../workerPull';
import { WorkerBuffer } from './buffer';
import type { PulledJob, WorkerDelivery } from './state';

export abstract class WorkerPolling<T = unknown, R = unknown> extends WorkerBuffer<T, R> {
  protected poll(): void {
    this.clearPollTimer();
    if (!this.running || this._closing) return;

    if (this.activeJobs >= this.opts.concurrency) {
      this.schedulePoll(10);
      return;
    }

    if (!this.rateLimiter.canProcessWithinLimit()) {
      this.scheduleRateLimitPoll();
      return;
    }

    void this.tryProcess();
  }

  protected scheduleProcessing(): void {
    if (!this.running || this._closing || this.processingScheduled) return;
    this.processingScheduled = true;
    setImmediate(() => {
      this.processingScheduled = false;
      void this.tryProcess();
    });
  }

  protected async tryProcess(): Promise<void> {
    if (!this.running || this._closing) return;

    try {
      let item = this.getNextEligibleJob();
      if (!item) {
        if (!this.rateLimiter.canProcessWithinLimit()) {
          this.scheduleRateLimitPoll();
          return;
        }
        const pulledItems = await this.doPullBatch();
        if (!this.running || this._closing) return;
        if (pulledItems.length > 0) {
          const items = this.registerPulledJobs(pulledItems);
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
          this.schedulePoll(10);
          return;
        }
        this.consecutiveErrors = 0;
        if (!this.startJob(item)) {
          this.requeueItem(item);
          this.scheduleRateLimitPoll();
        }
      } else {
        const hasBuffered = this.pendingJobsHead < this.pendingJobs.length;
        if (hasBuffered && this.groupLimiter) {
          this.schedulePoll(10);
          return;
        }
        const now = Date.now();
        if (now - this.lastDrainedEmit > 1000) {
          this.lastDrainedEmit = now;
          this.emit('drained');
        }
        const waitTime = this.opts.pollTimeout > 0 ? 10 : this.opts.drainDelay;
        this.schedulePoll(waitTime);
      }
    } catch (error) {
      if (!this.running) return;
      this.handlePullError(error);
    }
  }

  protected async doPullBatch(): Promise<PulledJob[]> {
    const groupBlockedBuffer =
      this.groupLimiter !== null && this.pendingJobsHead < this.pendingJobs.length;
    const leased = groupBlockedBuffer ? this.activeJobs : this.pulledJobIds.size;
    const slots = this.opts.concurrency - leased - this.pendingPull;
    const rateSlots = this.rateLimiter.getAvailableSlots() - this.pendingPull;
    const batchSize = Math.min(this.opts.batchSize, slots, rateSlots, 1000);
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

  private scheduleRateLimitPoll(): void {
    const waitTime = this.rateLimiter.getTimeUntilNextSlot();
    this.schedulePoll(Math.max(waitTime, 10));
  }

  private schedulePoll(delay: number): void {
    if (!this.running || this._closing || this.closed) return;
    const deadline = Date.now() + delay;
    if (this.pollTimer !== null && this.pollDeadline !== null && this.pollDeadline <= deadline) {
      return;
    }
    this.clearPollTimer();

    const timer = setTimeout(() => {
      if (this.pollTimer !== timer) return;
      this.pollTimer = null;
      this.pollDeadline = null;
      this.poll();
    }, delay);
    this.pollTimer = timer;
    this.pollDeadline = deadline;
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
    this.schedulePoll(backoffMs);
  }

  protected abstract startJob(delivery: WorkerDelivery): boolean;
  protected abstract getPullConfig(): PullConfig;
}
