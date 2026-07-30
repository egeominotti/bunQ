/**
 * SQLite Batch Operations
 * High-performance batch insert with prepared statement caching
 */

import type { Database } from 'bun:sqlite';
import type { Job } from '../../domain/types/job';
import { pack, persistedInitialState, persistedStallCount } from './sqliteSerializer';

const COLS_PER_ROW = 29;
// SQLite has a limit of ~999 variables, so batch in chunks
const MAX_ROWS_PER_INSERT = Math.floor(999 / COLS_PER_ROW);

/**
 * A constraint violation (e.g. a duplicate `jobs.id` PRIMARY KEY) is PERMANENT
 * for that row — retrying never succeeds. It must be isolated from the rest of
 * the batch, otherwise one bad row poisons the whole atomic flush and drops
 * unrelated valid jobs (data loss, #92-class). Everything else (disk I/O, busy,
 * full) is treated as transient and retried.
 */
function isConstraintError(err: Error): boolean {
  const code = (err as { code?: string }).code ?? '';
  return code.startsWith('SQLITE_CONSTRAINT') || /constraint failed/i.test(err.message);
}

/** Outcome of a batch insert after isolating per-row failures. */
export interface BatchInsertResult {
  /** Jobs that hit a transient (non-constraint) error — caller should retry. */
  transient: Job[];
  /** Jobs rejected by a permanent constraint (e.g. duplicate id) — drop, never retry. */
  conflicts: Job[];
  /** The originating error from the fast-path failure, for logging. */
  error?: Error;
}

/** Batch insert manager with prepared statement caching */
export class BatchInsertManager {
  private readonly db: Database;
  private readonly cache = new Map<number, ReturnType<Database['prepare']>>();

  constructor(db: Database) {
    this.db = db;
  }

  /**
   * Insert a batch of jobs using a single multi-row INSERT (50-100x speedup).
   * On the common path every row succeeds and an empty result is returned.
   *
   * If the atomic batch fails (e.g. a single duplicate `jobs.id`), the rows are
   * re-inserted ONE AT A TIME so a single bad row can no longer drop the rest:
   * valid jobs persist, constraint violations are isolated as `conflicts` (drop,
   * never retry — they would poison every future flush), and any transient
   * failures are returned so the caller can retry just those. Never throws.
   */
  insertJobsBatch(jobs: Job[]): BatchInsertResult {
    if (jobs.length === 0) return { transient: [], conflicts: [] };

    const now = Date.now();

    try {
      this.db.transaction(() => {
        for (let offset = 0; offset < jobs.length; offset += MAX_ROWS_PER_INSERT) {
          const chunk = jobs.slice(offset, offset + MAX_ROWS_PER_INSERT);
          this.insertJobsChunk(chunk, now);
        }
      })();
      return { transient: [], conflicts: [] };
    } catch (err) {
      const batchError = err instanceof Error ? err : new Error(String(err));
      return this.insertRowByRow(jobs, now, batchError);
    }
  }

  /** Fallback path: insert each job independently, isolating per-row failures. */
  private insertRowByRow(jobs: Job[], now: number, batchError: Error): BatchInsertResult {
    const transient: Job[] = [];
    const conflicts: Job[] = [];
    for (const job of jobs) {
      try {
        // Single-row INSERT, auto-committed: succeeds/fails independently.
        this.insertJobsChunk([job], now);
      } catch (e) {
        const err = e instanceof Error ? e : new Error(String(e));
        if (isConstraintError(err)) conflicts.push(job);
        else transient.push(job);
      }
    }
    return { transient, conflicts, error: batchError };
  }

  /** Get or create cached prepared statement for batch insert */
  private getBatchInsertStmt(size: number): ReturnType<Database['prepare']> {
    let stmt = this.cache.get(size);
    if (!stmt) {
      const rowPlaceholder =
        '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
      const placeholders = Array(size).fill(rowPlaceholder).join(', ');
      // ON CONFLICT(id) DO UPDATE (upsert): a colliding id overwrites in place instead
      // of failing the whole flush. Without it, one stale ORPHAN row (left when
      // obliterate()/a flush raced an in-flight insert) makes the batch throw UNIQUE,
      // and reportLostJobs() then drops EVERY innocent job batched in the same flush
      // window. Per-row conflict resolution isolates the collision; new ids stay a
      // plain INSERT (zero extra cost). jobs has no triggers/FKs → no cascade.
      const sql = `INSERT INTO jobs (
        id, queue, data, priority, created_at, run_at, attempts, max_attempts,
        backoff, ttl, timeout, unique_key, custom_id, depends_on, parent_id,
        children_ids, tags, state, lifo, group_id, remove_on_complete, remove_on_fail,
        fail_parent_on_failure, remove_dependency_on_failure,
        continue_parent_on_failure, ignore_dependency_on_failure,
        stall_timeout, stall_count, timeline
      ) VALUES ${placeholders}
      ON CONFLICT(id) DO UPDATE SET
        queue=excluded.queue, data=excluded.data, priority=excluded.priority,
        created_at=excluded.created_at, run_at=excluded.run_at, attempts=excluded.attempts,
        max_attempts=excluded.max_attempts, backoff=excluded.backoff, ttl=excluded.ttl,
        timeout=excluded.timeout, unique_key=excluded.unique_key, custom_id=excluded.custom_id,
        depends_on=excluded.depends_on, parent_id=excluded.parent_id, children_ids=excluded.children_ids,
        tags=excluded.tags, state=excluded.state, lifo=excluded.lifo, group_id=excluded.group_id,
        remove_on_complete=excluded.remove_on_complete, remove_on_fail=excluded.remove_on_fail,
        fail_parent_on_failure=excluded.fail_parent_on_failure,
        remove_dependency_on_failure=excluded.remove_dependency_on_failure,
        continue_parent_on_failure=excluded.continue_parent_on_failure,
        ignore_dependency_on_failure=excluded.ignore_dependency_on_failure,
        stall_timeout=excluded.stall_timeout, stall_count=excluded.stall_count,
        timeline=excluded.timeline,
        started_at=excluded.started_at, completed_at=excluded.completed_at,
        progress=excluded.progress, progress_msg=excluded.progress_msg,
        last_heartbeat=excluded.last_heartbeat, stacktrace=excluded.stacktrace`;
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
        persistedInitialState(job, now),
        job.lifo ? 1 : 0,
        job.groupId,
        job.removeOnComplete ? 1 : 0,
        job.removeOnFail ? 1 : 0,
        job.failParentOnFailure ? 1 : 0,
        job.removeDependencyOnFailure ? 1 : 0,
        job.continueParentOnFailure ? 1 : 0,
        job.ignoreDependencyOnFailure ? 1 : 0,
        job.stallTimeout,
        persistedStallCount(job),
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

  /**
   * Flush buffer to disk using double-buffering. Returns number of jobs persisted.
   *
   * Per-row isolation (see BatchInsertManager.insertJobsBatch): valid jobs are
   * persisted even if a sibling row violates a constraint. Constraint conflicts
   * (e.g. duplicate id) are dropped+reported and NEVER re-buffered — re-buffering
   * them would poison every future flush and silently drop unrelated valid jobs
   * (the #92-class data-loss bug). Transient failures are re-buffered and retried
   * with exponential backoff, exactly as before.
   */
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
      // BatchInsertManager.insertJobsBatch isolates per-row failures and never
      // throws. Stay defensive: a manager that throws (or returns nothing) is
      // treated as a transient failure of the whole batch, preserving the
      // re-buffer/retry/critical-loss semantics.
      let result: BatchInsertResult;
      try {
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- defensive against a non-conforming batch manager (e.g. a test double returning undefined)
        result = this.batchManager.insertJobsBatch(this.flushBuffer) ?? {
          transient: [],
          conflicts: [],
        };
      } catch (err) {
        result = {
          transient: this.flushBuffer,
          conflicts: [],
          error: err instanceof Error ? err : new Error(String(err)),
        };
      }
      const { transient, conflicts, error } = result;
      this.flushBuffer = [];

      // Permanent constraint conflicts: drop + report once, never retry.
      if (conflicts.length > 0) {
        this.onError(error ?? new Error('Constraint violation'), conflicts.length);
      }

      // Transient failures: re-buffer just those and retry with backoff.
      if (transient.length > 0) {
        const error2 = error ?? new Error('Write buffer flush failed');
        this.lastError = error2;
        this.retryCount++;
        // Prepend failed jobs back to active buffer (failed first, then new).
        this.activeBuffer = transient.concat(this.activeBuffer);

        if (this.retryCount >= this.maxRetries) {
          const lostJobs = [...this.activeBuffer];
          this.activeBuffer = [];
          if (this.onCriticalError) this.onCriticalError(lostJobs, error2, this.retryCount);
          this.onError(error2, lostJobs.length, {
            retryCount: this.retryCount,
            nextBackoffMs: 0,
            maxRetries: this.maxRetries,
          });
          this.retryCount = 0;
          this.currentBackoffMs = this.initialBackoffMs;
          this.lastError = null;
        } else {
          const nextBackoffMs = Math.min(this.currentBackoffMs * 2, this.maxBackoffMs);
          this.onError(error2, transient.length, {
            retryCount: this.retryCount,
            nextBackoffMs,
            maxRetries: this.maxRetries,
          });
          this.scheduleBackoffRetry();
        }
        return jobCount - transient.length - conflicts.length;
      }

      // Success (or success-modulo-dropped-conflicts): reset retry state.
      this.retryCount = 0;
      this.currentBackoffMs = this.initialBackoffMs;
      this.lastError = null;
      return jobCount - conflicts.length;
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
        // flush() no longer throws on transient failure (it re-buffers); report
        // anything that could not be persisted so it isn't silently lost.
        if (this.pendingCount > 0) this.reportLostJobs();
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
