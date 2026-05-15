/**
 * SQLite Batch Operations
 * High-performance batch insert with prepared statement caching
 */

import type { Database } from 'bun:sqlite';
import type { Job } from '../../domain/types/job';
import { pack } from './sqliteSerializer';

/** Batch insert manager with prepared statement caching */
export class BatchInsertManager {
  private readonly db: Database;
  private readonly cache = new Map<number, ReturnType<Database['prepare']>>();

  constructor(db: Database) {
    this.db = db;
  }

  /** Insert batch of jobs using multi-row INSERT for 50-100x speedup */
  insertJobsBatch(jobs: Job[]): void {
    if (jobs.length === 0) return;

    const now = Date.now();
    const COLS_PER_ROW = 24;
    // SQLite has a limit of ~999 variables, so batch in chunks
    const MAX_ROWS_PER_INSERT = Math.floor(999 / COLS_PER_ROW);

    this.db.transaction(() => {
      for (let offset = 0; offset < jobs.length; offset += MAX_ROWS_PER_INSERT) {
        const chunk = jobs.slice(offset, offset + MAX_ROWS_PER_INSERT);
        this.insertJobsChunk(chunk, now);
      }
    })();
  }

  /** Get or create cached prepared statement for batch insert */
  private getBatchInsertStmt(size: number): ReturnType<Database['prepare']> {
    let stmt = this.cache.get(size);
    if (!stmt) {
      const rowPlaceholder =
        '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
      const placeholders = Array(size).fill(rowPlaceholder).join(', ');
      const sql = `INSERT INTO jobs (
        id, queue, data, priority, created_at, run_at, attempts, max_attempts,
        backoff, ttl, timeout, unique_key, custom_id, depends_on, parent_id,
        children_ids, tags, state, lifo, group_id, remove_on_complete, remove_on_fail, stall_timeout, timeline
      ) VALUES ${placeholders}`;
      stmt = this.db.prepare(sql);
      // Cache statements for common batch sizes (1-100)
      if (size <= 100) {
        this.cache.set(size, stmt);
      }
    }
    return stmt;
  }

  /** Insert a chunk of jobs with single multi-row INSERT */
  private insertJobsChunk(jobs: Job[], now: number): void {
    const stmt = this.getBatchInsertStmt(jobs.length);

    // Flatten all values
    const values: unknown[] = [];
    for (const job of jobs) {
      values.push(
        job.id,
        job.queue,
        pack(job.data),
        job.priority,
        job.createdAt,
        job.runAt,
        job.attempts,
        job.maxAttempts,
        job.backoff,
        job.ttl,
        job.timeout,
        job.uniqueKey,
        job.customId,
        job.dependsOn.length > 0 ? pack(job.dependsOn) : null,
        job.parentId,
        job.childrenIds.length > 0 ? pack(job.childrenIds) : null,
        job.tags.length > 0 ? pack(job.tags) : null,
        job.runAt > now ? 'delayed' : 'waiting',
        job.lifo ? 1 : 0,
        job.groupId,
        job.removeOnComplete ? 1 : 0,
        job.removeOnFail ? 1 : 0,
        job.stallTimeout,
        job.timeline.length > 0 ? pack(job.timeline) : null
      );
    }

    stmt.run(...(values as (string | number | bigint | null | Uint8Array)[]));
  }
}

/** Extended error callback with retry information */
export type WriteBufferErrorCallback = (
  err: Error,
  jobCount: number,
  retryInfo?: { retryCount: number; nextBackoffMs: number; maxRetries: number }
) => void;

/** Callback when jobs are moved to dead letter after max retries */
export type CriticalErrorCallback = (jobs: Job[], lastError: Error, totalAttempts: number) => void;

/** Write buffer for batching inserts with double-buffering for atomic swap */
export class WriteBuffer {
  /** Active buffer for new jobs */
  private activeBuffer: Job[] = [];
  /** Flush buffer being written to disk */
  private flushBuffer: Job[] = [];
  /** Lock to prevent concurrent flushes */
  private flushing = false;
  /** Flag to prevent operations after stop */
  private stopped = false;
  private timer: ReturnType<typeof setInterval> | null = null;
  private readonly batchManager: BatchInsertManager;
  private readonly bufferSize: number;
  private readonly onError: WriteBufferErrorCallback;
  private readonly onCriticalError?: CriticalErrorCallback;

  /** Retry state for exponential backoff */
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

    // Auto-flush timer
    this.timer = setInterval(() => {
      // Skip if stopped or in backoff mode
      if (this.stopped || this.backoffTimer) return;

      try {
        this.flush();
      } catch {
        // Error already handled in flush
      }
    }, flushIntervalMs);
  }

  /** Add job to buffer */
  add(job: Job): void {
    this.activeBuffer.push(job);
    if (this.activeBuffer.length >= this.bufferSize) {
      this.flush();
    }
  }

  /** Add multiple jobs to buffer */
  addBatch(jobs: Job[]): void {
    for (const job of jobs) {
      this.activeBuffer.push(job);
    }
    if (this.activeBuffer.length >= this.bufferSize) {
      this.flush();
    }
  }

  /** Flush buffer to disk using double-buffering. Returns number of jobs flushed. */
  flush(): number {
    // Prevent flush after stop or concurrent flushes
    if (this.stopped || this.flushing) return 0;
    if (this.activeBuffer.length === 0) return 0;

    this.flushing = true;

    // Atomic swap: move active to flush buffer
    this.flushBuffer = this.activeBuffer;
    this.activeBuffer = [];

    const jobCount = this.flushBuffer.length;

    try {
      this.batchManager.insertJobsBatch(this.flushBuffer);
      this.flushBuffer = []; // Clear after successful write

      // Reset retry state on success
      this.retryCount = 0;
      this.currentBackoffMs = this.initialBackoffMs;
      this.lastError = null;

      return jobCount;
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      this.lastError = error;
      this.retryCount++;

      // On failure, prepend failed jobs back to active buffer
      // This preserves order: failed jobs first, then new jobs
      this.activeBuffer = this.flushBuffer.concat(this.activeBuffer);
      this.flushBuffer = [];

      // Check if we've exceeded max retries
      if (this.retryCount >= this.maxRetries) {
        // Move jobs to dead letter / emit critical error
        const lostJobs = [...this.activeBuffer];
        this.activeBuffer = [];

        if (this.onCriticalError) {
          this.onCriticalError(lostJobs, error, this.retryCount);
        }

        // Also call onError with retry info for logging
        this.onError(error, lostJobs.length, {
          retryCount: this.retryCount,
          nextBackoffMs: 0, // No more retries
          maxRetries: this.maxRetries,
        });

        // Reset retry state
        this.retryCount = 0;
        this.currentBackoffMs = this.initialBackoffMs;
        this.lastError = null;

        throw err;
      }

      // Calculate next backoff with exponential increase
      const nextBackoffMs = Math.min(this.currentBackoffMs * 2, this.maxBackoffMs);

      // Call error callback with retry information
      this.onError(error, jobCount, {
        retryCount: this.retryCount,
        nextBackoffMs: nextBackoffMs,
        maxRetries: this.maxRetries,
      });

      // Schedule backoff retry
      this.scheduleBackoffRetry();

      throw err;
    } finally {
      this.flushing = false;
    }
  }

  /** Schedule a retry with exponential backoff */
  private scheduleBackoffRetry(): void {
    // Clear any existing backoff timer
    if (this.backoffTimer) {
      clearTimeout(this.backoffTimer);
    }

    // Update backoff for next attempt
    this.currentBackoffMs = Math.min(this.currentBackoffMs * 2, this.maxBackoffMs);

    this.backoffTimer = setTimeout(() => {
      this.backoffTimer = null;
      try {
        this.flush();
      } catch {
        // Error already handled in flush
      }
    }, this.currentBackoffMs);
  }

  /** Get pending job count (includes both buffers) */
  get pendingCount(): number {
    return this.activeBuffer.length + this.flushBuffer.length;
  }

  /**
   * Remove any pending buffered inserts for a given jobId.
   * Prevents a buffered insert from flushing to disk after an immediate deleteJob,
   * which would leave an orphan row with stale state.
   */
  removePending(jobId: string): void {
    const i = this.activeBuffer.findIndex((j) => j.id === jobId);
    if (i !== -1) this.activeBuffer.splice(i, 1);
    const j = this.flushBuffer.findIndex((j) => j.id === jobId);
    if (j !== -1) this.flushBuffer.splice(j, 1);
  }

  /**
   * Check whether a job is still sitting in the WriteBuffer (not yet persisted).
   * Used by state-transition writes (markActive/Completed/Failed) to decide
   * whether they need to flush the buffer first — otherwise their UPDATE
   * would silently match 0 rows and the state change would be lost when the
   * buffered INSERT eventually writes with the original state.
   */
  hasPending(jobId: string): boolean {
    for (const j of this.activeBuffer) if (j.id === jobId) return true;
    for (const j of this.flushBuffer) if (j.id === jobId) return true;
    return false;
  }

  /** Stop auto-flush timer and flush pending jobs */
  stop(): void {
    // Clear auto-flush timer
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    // Flush any pending jobs before stopping
    if (this.pendingCount > 0) {
      try {
        this.flush();
      } catch {
        // Flush failed during shutdown - jobs will be reported as lost below
      }
    }

    // Mark as stopped to prevent any future flush attempts
    this.stopped = true;

    // Clear any backoff timer (pre-existing or scheduled by failed flush)
    if (this.backoffTimer) {
      clearTimeout(this.backoffTimer);
      this.backoffTimer = null;
    }

    // Report any jobs still stuck in buffers as lost
    this.reportLostJobs();
  }

  /**
   * Graceful shutdown with timeout.
   * Attempts to flush pending jobs within the specified timeout.
   * @param timeoutMs Maximum time to wait for flush (default: 5000ms)
   * @returns Promise resolving to number of jobs flushed, or -1 if timed out
   */
  async stopGracefully(timeoutMs = 5000): Promise<number> {
    // Clear backoff timer
    if (this.backoffTimer) {
      clearTimeout(this.backoffTimer);
      this.backoffTimer = null;
    }

    // Clear auto-flush timer
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    const pending = this.pendingCount;
    if (pending === 0) {
      this.stopped = true;
      return 0;
    }

    // Try to flush with timeout
    return new Promise<number>((resolve) => {
      const timeout = setTimeout(() => {
        this.stopped = true;
        this.reportLostJobs();
        resolve(-1); // Timed out
      }, timeoutMs);

      try {
        const flushed = this.flush();
        clearTimeout(timeout);
        this.stopped = true;
        resolve(flushed);
      } catch {
        // Flush failed - clear any backoff timer that flush() scheduled
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

  /** Report any remaining buffered jobs as lost via onCriticalError */
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

  /** Get current retry state (for monitoring) */
  getRetryState(): { retryCount: number; currentBackoffMs: number; lastError: Error | null } {
    return {
      retryCount: this.retryCount,
      currentBackoffMs: this.currentBackoffMs,
      lastError: this.lastError,
    };
  }
}
