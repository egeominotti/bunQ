import type { Job, JobId } from '../../domain/types/job';
import type { BatchInsertManager } from './batchInsert';
import type {
  BatchInsertResult,
  CriticalErrorCallback,
  WriteBufferErrorCallback,
} from './types/batch';
import type { PersistedJobState } from './sqliteSerializer';
import {
  findPendingJob,
  listPendingJobs,
  removePendingJob,
  removePendingQueue,
  setPendingJobState,
} from './writeBufferPending';

export class WriteBuffer {
  private activeBuffer: Job[] = [];
  private flushBuffer: Job[] = [];
  private flushing = false;
  private stopped = false;
  private timer: ReturnType<typeof setInterval> | null = null;
  private readonly batchManager: BatchInsertManager;
  private readonly bufferSize: number;
  private readonly onError: WriteBufferErrorCallback;
  private readonly onCriticalError?: CriticalErrorCallback;
  private retryCount = 0;
  private currentBackoffMs = 100;
  private readonly initialBackoffMs = 100;
  private readonly maxBackoffMs = 30000;
  private readonly maxRetries = 10;
  private lastError: Error | null = null;
  private backoffTimer: ReturnType<typeof setTimeout> | null = null;
  private autoFlushDeferrals = 0;

  constructor(
    batchManager: BatchInsertManager,
    bufferSize: number,
    flushIntervalMs: number,
    onError: WriteBufferErrorCallback,
    onCriticalError?: CriticalErrorCallback
  ) {
    this.batchManager = batchManager;
    this.bufferSize = bufferSize;
    this.onError = onError;
    this.onCriticalError = onCriticalError;
    this.timer = setInterval(() => {
      if (this.stopped || this.backoffTimer) return;
      this.flushBestEffort();
    }, flushIntervalMs);
  }

  add(job: Job): void {
    this.activeBuffer.push(job);
    if (this.autoFlushDeferrals === 0 && this.activeBuffer.length >= this.bufferSize) {
      this.flushIfReady();
    }
  }

  addBatch(jobs: Job[]): void {
    for (const job of jobs) this.activeBuffer.push(job);
    if (this.autoFlushDeferrals === 0 && this.activeBuffer.length >= this.bufferSize) {
      this.flushIfReady();
    }
  }

  withDeferredAutoFlush<T>(operation: () => T): T {
    this.autoFlushDeferrals++;
    let result: T | undefined;
    let operationFailed = false;
    let operationError: unknown;
    try {
      result = operation();
    } catch (error) {
      operationFailed = true;
      operationError = error;
    } finally {
      this.autoFlushDeferrals--;
    }
    let flushFailed = false;
    let flushError: unknown;
    if (
      this.autoFlushDeferrals === 0 &&
      !this.stopped &&
      this.activeBuffer.length >= this.bufferSize
    ) {
      try {
        this.flushIfReady();
      } catch (error) {
        flushFailed = true;
        flushError = error;
      }
    }
    if (operationFailed && flushFailed) {
      throw new AggregateError(
        [operationError, flushError],
        'Batch admission and buffer flush failed'
      );
    }
    if (operationFailed) throw operationError;
    if (flushFailed) throw flushError;
    return result as T;
  }

  flush(): number {
    if (this.stopped || this.flushing) return 0;
    if (this.activeBuffer.length === 0) return 0;
    this.flushing = true;
    this.flushBuffer = this.activeBuffer;
    this.activeBuffer = [];
    const jobCount = this.flushBuffer.length;

    try {
      let result: BatchInsertResult;
      try {
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- defensive against a non-conforming test double
        result = this.batchManager.insertJobsBatch(this.flushBuffer) ?? {
          transient: [],
          conflicts: [],
        };
      } catch (error) {
        result = {
          transient: this.flushBuffer,
          conflicts: [],
          error: error instanceof Error ? error : new Error(String(error)),
        };
      }
      const { transient, conflicts, error } = result;
      this.flushBuffer = [];

      if (conflicts.length > 0) {
        this.onError(error ?? new Error('Constraint violation'), conflicts.length);
      }

      if (transient.length > 0) {
        const flushError = error ?? new Error('Write buffer flush failed');
        this.lastError = flushError;
        this.retryCount++;
        this.activeBuffer = transient.concat(this.activeBuffer);

        if (this.retryCount >= this.maxRetries) {
          const lostJobs = [...this.activeBuffer];
          this.activeBuffer = [];
          this.clearBackoffRetry();
          if (this.onCriticalError) {
            this.onCriticalError(lostJobs, flushError, this.retryCount);
          }
          this.onError(flushError, lostJobs.length, {
            retryCount: this.retryCount,
            nextBackoffMs: 0,
            maxRetries: this.maxRetries,
          });
          this.resetRetryState();
        } else {
          const nextBackoffMs = Math.min(this.currentBackoffMs * 2, this.maxBackoffMs);
          this.onError(flushError, transient.length, {
            retryCount: this.retryCount,
            nextBackoffMs,
            maxRetries: this.maxRetries,
          });
          this.scheduleBackoffRetry();
        }
        return jobCount - transient.length - conflicts.length;
      }

      this.resetRetryState();
      return jobCount - conflicts.length;
    } finally {
      this.flushing = false;
    }
  }

  flushIfReady(): number {
    return this.backoffTimer ? 0 : this.flush();
  }

  private scheduleBackoffRetry(): void {
    this.clearBackoffRetry();
    this.currentBackoffMs = Math.min(this.currentBackoffMs * 2, this.maxBackoffMs);
    this.backoffTimer = setTimeout(() => {
      this.backoffTimer = null;
      this.flushBestEffort();
    }, this.currentBackoffMs);
  }

  get pendingCount(): number {
    return this.activeBuffer.length + this.flushBuffer.length;
  }
  removePending(jobId: string): void {
    [this.activeBuffer, this.flushBuffer] = removePendingJob(
      [this.activeBuffer, this.flushBuffer],
      jobId as JobId
    );
    if (this.pendingCount === 0) this.resetRetryState();
  }
  removePendingForQueue(queue: string): JobId[] {
    const result = removePendingQueue([this.activeBuffer, this.flushBuffer], queue);
    [this.activeBuffer, this.flushBuffer] = result.buffers;
    if (this.pendingCount === 0) this.resetRetryState();
    return result.removed;
  }
  hasPending(jobId: string): boolean {
    return this.getPendingJob(jobId as JobId) !== null;
  }

  getPendingJob(jobId: JobId): Job | null {
    return findPendingJob([this.activeBuffer, this.flushBuffer], jobId);
  }

  setPendingState(jobId: JobId, state: PersistedJobState): boolean {
    return setPendingJobState([this.activeBuffer, this.flushBuffer], jobId, state);
  }

  getPendingJobs(queue?: string): Job[] {
    return listPendingJobs([this.activeBuffer, this.flushBuffer], queue);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    if (this.pendingCount > 0) {
      this.flushBestEffort();
    }
    this.stopped = true;
    this.clearBackoffRetry();
    this.reportLostJobs();
  }

  async stopGracefully(timeoutMs = 5000): Promise<number> {
    this.clearBackoffRetry();
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    const pending = this.pendingCount;
    if (pending === 0) {
      this.stopped = true;
      return 0;
    }
    return new Promise<number>((resolve) => {
      const timeout = setTimeout(() => {
        this.stopped = true;
        this.reportLostJobs();
        resolve(-1);
      }, timeoutMs);
      try {
        const flushed = this.flush();
        clearTimeout(timeout);
        this.stopped = true;
        if (this.pendingCount > 0) this.reportLostJobs();
        resolve(flushed);
      } catch {
        this.clearBackoffRetry();
        clearTimeout(timeout);
        this.stopped = true;
        this.reportLostJobs();
        resolve(0);
      }
    });
  }

  private clearBackoffRetry(): void {
    if (this.backoffTimer) clearTimeout(this.backoffTimer);
    this.backoffTimer = null;
  }

  private resetRetryState(): void {
    this.clearBackoffRetry();
    this.retryCount = 0;
    this.currentBackoffMs = this.initialBackoffMs;
    this.lastError = null;
  }

  private flushBestEffort(): void {
    try {
      this.flush();
    } catch {
      // Flush callbacks already receive persistence failures.
    }
  }

  private reportLostJobs(): void {
    const remaining = this.activeBuffer.concat(this.flushBuffer);
    if (remaining.length > 0 && this.onCriticalError) {
      this.onCriticalError(
        remaining,
        this.lastError ?? new Error('Flush failed during shutdown'),
        this.retryCount
      );
      this.activeBuffer = [];
      this.flushBuffer = [];
    }
  }

  getRetryState(): { retryCount: number; currentBackoffMs: number; lastError: Error | null } {
    return {
      retryCount: this.retryCount,
      currentBackoffMs: this.currentBackoffMs,
      lastError: this.lastError,
    };
  }
}
