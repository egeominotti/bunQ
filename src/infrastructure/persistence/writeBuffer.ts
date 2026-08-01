import type { Job } from '../../domain/types/job';
import type { BatchInsertManager } from './batchInsert';
import type {
  BatchInsertResult,
  CriticalErrorCallback,
  WriteBufferErrorCallback,
} from './types/batch';

/** Double-buffered job insert queue with bounded exponential retry. */
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
      try {
        this.flush();
      } catch {
        // Error already handled by flush.
      }
    }, flushIntervalMs);
  }

  add(job: Job): void {
    this.activeBuffer.push(job);
    if (this.activeBuffer.length >= this.bufferSize) this.flush();
  }

  addBatch(jobs: Job[]): void {
    for (const job of jobs) this.activeBuffer.push(job);
    if (this.activeBuffer.length >= this.bufferSize) this.flush();
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
          if (this.onCriticalError) {
            this.onCriticalError(lostJobs, flushError, this.retryCount);
          }
          this.onError(flushError, lostJobs.length, {
            retryCount: this.retryCount,
            nextBackoffMs: 0,
            maxRetries: this.maxRetries,
          });
          this.retryCount = 0;
          this.currentBackoffMs = this.initialBackoffMs;
          this.lastError = null;
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

      this.retryCount = 0;
      this.currentBackoffMs = this.initialBackoffMs;
      this.lastError = null;
      return jobCount - conflicts.length;
    } finally {
      this.flushing = false;
    }
  }

  private scheduleBackoffRetry(): void {
    if (this.backoffTimer) clearTimeout(this.backoffTimer);
    this.currentBackoffMs = Math.min(this.currentBackoffMs * 2, this.maxBackoffMs);
    this.backoffTimer = setTimeout(() => {
      this.backoffTimer = null;
      try {
        this.flush();
      } catch {
        // Error already handled by flush.
      }
    }, this.currentBackoffMs);
  }

  get pendingCount(): number {
    return this.activeBuffer.length + this.flushBuffer.length;
  }

  removePending(jobId: string): void {
    const activeIndex = this.activeBuffer.findIndex((job) => job.id === jobId);
    if (activeIndex !== -1) this.activeBuffer.splice(activeIndex, 1);
    const flushIndex = this.flushBuffer.findIndex((job) => job.id === jobId);
    if (flushIndex !== -1) this.flushBuffer.splice(flushIndex, 1);
  }

  hasPending(jobId: string): boolean {
    for (const job of this.activeBuffer) if (job.id === jobId) return true;
    for (const job of this.flushBuffer) if (job.id === jobId) return true;
    return false;
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    if (this.pendingCount > 0) {
      try {
        this.flush();
      } catch {
        // Remaining jobs are reported below.
      }
    }
    this.stopped = true;
    if (this.backoffTimer) {
      clearTimeout(this.backoffTimer);
      this.backoffTimer = null;
    }
    this.reportLostJobs();
  }

  async stopGracefully(timeoutMs = 5000): Promise<number> {
    if (this.backoffTimer) {
      clearTimeout(this.backoffTimer);
      this.backoffTimer = null;
    }
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

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
        if (this.backoffTimer) {
          clearTimeout(this.backoffTimer);
          this.backoffTimer = null;
        }
        clearTimeout(timeout);
        this.stopped = true;
        this.reportLostJobs();
        resolve(0);
      }
    });
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
