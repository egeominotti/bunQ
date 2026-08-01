/**
 * SQLite Storage Implementation
 * Persistence layer using Bun's native SQLite
 * Uses MessagePack for ~2-3x faster serialization than JSON
 */

import { Database } from 'bun:sqlite';
import type { Job, JobId, JobTimelineEntry } from '../../domain/types/job';
import type { FlowFailureMode, FlowFailureRecord } from '../../domain/types/flow';
import type { StallConfig } from '../../domain/types/stall';
import type { CronJob } from '../../domain/types/cron';
import {
  type DlqConfig,
  type DlqEntry,
  FailureReason,
  createDlqEntry,
} from '../../domain/types/dlq';
import { PRAGMA_SETTINGS, SCHEMA, MIGRATION_TABLE, SCHEMA_VERSION, MIGRATIONS } from './schema';
import {
  prepareStatements,
  type StatementName,
  type DbJob,
  type DbCron,
  type DbQueueState,
} from './statements';
import {
  pack,
  persistedInitialState,
  persistedStallCount,
  reconstructDlqEntry,
  rowToJob,
  unpack,
} from './sqliteSerializer';
import { BatchInsertManager, WriteBuffer } from './sqliteBatch';
import { storageLog } from '../../shared/logger';
import {
  DependencyCompletionStore,
  type DependencyCompletionRecord,
} from './dependencyCompletionStore';

/** Critical-loss callback: invoked when WriteBuffer drops jobs after exhausting retries. */
export type SqliteCriticalLossCallback = (jobs: Job[], lastError: Error, attempts: number) => void;

/** Record of a critical job loss event (jobs dropped after max retries). */
export interface SqliteCriticalLoss {
  jobs: Job[];
  error: string;
  attempts: number;
  at: number;
}

/** SQLite configuration */
export interface SqliteConfig {
  path: string;
  walMode?: boolean;
  synchronous?: 'OFF' | 'NORMAL' | 'FULL';
  cacheSize?: number;
  /** Write buffer size (default: 100) */
  writeBufferSize?: number;
  /** Write buffer flush interval in ms (default: 50) */
  writeBufferFlushMs?: number;
  /** Callback invoked when WriteBuffer drops jobs after exhausting retries. */
  onCriticalLoss?: SqliteCriticalLossCallback;
}

/** Check if an error is a SQLITE_FULL (disk full) error */
function isSqliteFullError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const msg = err.message;
  return msg.includes('SQLITE_FULL') || msg.includes('database or disk is full');
}

/**
 * SQLite Storage class with write buffering for high throughput
 */
export class SqliteStorage {
  private readonly db: Database;
  private readonly statements: Map<StatementName, ReturnType<Database['prepare']>>;
  private readonly batchManager: BatchInsertManager;
  private readonly writeBuffer: WriteBuffer;
  private readonly dependencyCompletionStore: DependencyCompletionStore;
  private _diskFull = false;
  private _lastDiskFullError: string | null = null;
  private _lastDiskFullAt: number | null = null;
  private readonly _criticalLosses: SqliteCriticalLoss[] = [];
  private readonly _onCriticalLoss?: SqliteCriticalLossCallback;
  /** Cap on retained critical-loss records to prevent unbounded growth */
  private static readonly MAX_RETAINED_LOSSES = 100;

  constructor(config: SqliteConfig) {
    this.db = new Database(config.path, { create: true });
    // PRAGMA settings are performance/optimization hints, not correctness
    // requirements. A transient filesystem IOERR (e.g. SQLITE_IOERR_FSTAT from
    // `mmap_size` calling fstat() on the fd during a restart/cleanup race) must
    // NOT propagate out of the constructor — in a deferred/async context Bun
    // reports it as an "Unhandled error between tests" and tears down CI.
    try {
      this.db.run(PRAGMA_SETTINGS);
    } catch (err) {
      storageLog.error('Failed to apply PRAGMA settings', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
    this.migrate();
    this.dependencyCompletionStore = new DependencyCompletionStore(this.db);
    this.statements = prepareStatements(this.db);
    this._onCriticalLoss = config.onCriticalLoss;

    // Initialize batch manager and write buffer
    this.batchManager = new BatchInsertManager(this.db);
    this.writeBuffer = new WriteBuffer(
      this.batchManager,
      config.writeBufferSize ?? 100,
      config.writeBufferFlushMs ?? 10,
      (err, jobCount) => {
        if (isSqliteFullError(err)) {
          this.setDiskFull(err.message);
        }
        // A constraint violation (e.g. duplicate jobs.id) is a PERMANENT per-row
        // rejection that the WriteBuffer isolated and dropped — sibling valid jobs
        // in the same flush were still persisted. Log it distinctly from a
        // transient flush failure (and never route it to the DLQ, which would
        // resurrect a duplicate).
        if (/constraint failed/i.test(err.message)) {
          storageLog.error('Write buffer rejected jobs (constraint violation, dropped)', {
            rejectedJobCount: jobCount,
            error: err.message,
          });
        } else {
          storageLog.error('Write buffer flush failed', {
            jobCount,
            error: err.message,
            diskFull: this._diskFull,
          });
        }
      },
      (jobs, lastError, attempts) => {
        this.handleCriticalLoss(jobs, lastError, attempts);
      }
    );
  }

  /**
   * Default handler for WriteBuffer critical loss. Logs every dropped job so
   * ops can recover from logs, retains the last MAX_RETAINED_LOSSES records
   * for programmatic inspection, and forwards to a user-provided callback.
   *
   * Without this handler the dropped jobs would be silently discarded by
   * sqliteBatch.ts:209-223 (no callback = no recovery path).
   */
  private handleCriticalLoss(jobs: Job[], lastError: Error, attempts: number): void {
    storageLog.error('CRITICAL: WriteBuffer dropped jobs after exhausting retries', {
      lostJobCount: jobs.length,
      attempts,
      error: lastError.message,
      diskFull: this._diskFull,
    });
    for (const job of jobs) {
      storageLog.error('Lost job (recover from this log if needed)', {
        id: String(job.id),
        queue: job.queue,
        customId: job.customId,
        priority: job.priority,
        createdAt: job.createdAt,
        attempts: job.attempts,
        // Data preview (truncated to avoid log spam from huge payloads)
        dataPreview: this.previewJobData(job.data),
      });
    }
    this._criticalLosses.push({
      jobs,
      error: lastError.message,
      attempts,
      at: Date.now(),
    });
    // Bound retention
    while (this._criticalLosses.length > SqliteStorage.MAX_RETAINED_LOSSES) {
      this._criticalLosses.shift();
    }
    // Durably persist each lost job to the DLQ table so it survives a restart
    // and is recoverable via the normal DLQ path (getDlqEntry / loadDlq).
    // saveDlqEntry() writes directly via the prepared `insertDlq` statement
    // (NOT through the WriteBuffer that just exhausted its retries), so this
    // cannot recurse back into the failing flush path.
    for (const job of jobs) {
      try {
        this.saveDlqEntry(createDlqEntry(job, FailureReason.Unknown, lastError.message));
      } catch (err) {
        storageLog.error('Failed to persist lost job to DLQ', {
          id: String(job.id),
          queue: job.queue,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
    // Forward to user-supplied callback if provided
    if (this._onCriticalLoss) {
      try {
        this._onCriticalLoss(jobs, lastError, attempts);
      } catch (err) {
        storageLog.error('onCriticalLoss callback threw', {
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
  }

  /** Truncate job data preview to keep logs readable. */
  private previewJobData(data: unknown): string {
    try {
      const s = JSON.stringify(data);
      return s.length > 500 ? s.slice(0, 500) + '...[truncated]' : s;
    } catch {
      return '[unserializable]';
    }
  }

  /** Retrieve retained critical-loss records (most recent last). */
  getCriticalLosses(): readonly SqliteCriticalLoss[] {
    return this._criticalLosses;
  }

  /** Clear retained critical-loss records (e.g. after operator acknowledgement). */
  clearCriticalLosses(): void {
    this._criticalLosses.length = 0;
  }

  /** Mark disk as full and log */
  private setDiskFull(message: string): void {
    if (!this._diskFull) {
      storageLog.error('DISK FULL: SQLite cannot write, persistence degraded', { error: message });
    }
    this._diskFull = true;
    this._lastDiskFullError = message;
    this._lastDiskFullAt = Date.now();
  }

  /** Execute a write operation with SQLITE_FULL detection */
  private safeWrite(fn: () => void): void {
    try {
      fn();
      // Clear disk full flag on successful write
      if (this._diskFull) {
        this._diskFull = false;
        this._lastDiskFullError = null;
        storageLog.info('Disk full condition cleared - writes succeeding again');
      }
    } catch (err) {
      if (isSqliteFullError(err)) {
        this.setDiskFull(err instanceof Error ? err.message : String(err));
      }
      throw err;
    }
  }

  /** Check if storage is in disk-full state */
  get diskFull(): boolean {
    return this._diskFull;
  }

  /** Get disk full error details */
  getDiskFullStatus(): { diskFull: boolean; error: string | null; since: number | null } {
    return {
      diskFull: this._diskFull,
      error: this._lastDiskFullError,
      since: this._lastDiskFullAt,
    };
  }

  /** Flush write buffer to disk. Returns number of jobs flushed. */
  flushWriteBuffer(): number {
    const flushed = this.writeBuffer.flush();
    const pending = this.writeBuffer.pendingCount;
    if (pending > 0) {
      throw new Error(
        `Cannot create a consistent backup while ${pending} write${pending === 1 ? '' : 's'} remains buffered`
      );
    }
    return flushed;
  }

  private migrate(): void {
    this.db.run(MIGRATION_TABLE);
    const currentVersion =
      this.db.query<{ version: number }, []>('SELECT MAX(version) as version FROM migrations').get()
        ?.version ?? 0;

    if (currentVersion < SCHEMA_VERSION) {
      this.db.run(SCHEMA);
      // Apply incremental migrations for existing databases
      for (const [ver, sql] of Object.entries(MIGRATIONS)) {
        const v = Number(ver);
        if (v > currentVersion && v > 1) {
          try {
            this.db.run(sql);
          } catch {
            // Column/index may already exist from SCHEMA creation
          }
        }
      }
      this.db
        .prepare('INSERT INTO migrations (version, applied_at) VALUES (?, ?)')
        .run(SCHEMA_VERSION, Date.now());
    }
  }

  // ============ Job Operations ============

  /**
   * Insert job using write buffer for better throughput.
   * @param job The job to insert
   * @param durable If true, bypasses write buffer and writes immediately to disk
   */
  insertJob(job: Job, durable?: boolean): void {
    if (durable) {
      this.insertJobImmediate(job);
      return;
    }
    this.writeBuffer.add(job);
  }

  /** Insert job immediately (bypass buffer) */
  insertJobImmediate(job: Job): void {
    this.safeWrite(() => {
      this.runInsertJobStmt(job);
    });
  }

  /** Run the insertJob statement for one job (no safeWrite/transaction wrapper). */
  private runInsertJobStmt(job: Job): void {
    this.statements
      .get('insertJob')!
      .run(
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
        persistedInitialState(job),
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

  /**
   * Ensure a job's buffered INSERT has been written to disk before issuing a
   * state-mutating UPDATE. Without this, markActive/markCompleted's UPDATE
   * would silently match 0 rows and the state change would be overwritten
   * when the buffered INSERT eventually fires with the original
   * 'waiting'/'delayed' state baked at insert time.
   */
  private flushIfBuffered(jobId: JobId): void {
    if (this.writeBuffer.hasPending(String(jobId))) {
      try {
        this.writeBuffer.flush();
      } catch {
        // Flush errors are already routed through onError/onCriticalLoss.
        // Proceed to UPDATE — if the row really isn't there the UPDATE will
        // no-op and the caller's in-memory state stays authoritative until
        // the next successful flush or recovery cycle.
      }
    }
  }

  markActive(jobId: JobId, startedAt: number, timeline?: JobTimelineEntry[]): void {
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      this.statements
        .get('updateJobState')!
        .run('active', startedAt, timeline && timeline.length > 0 ? pack(timeline) : null, jobId);
    });
  }

  /** Persist a manually parked parent so recovery does not requeue it. */
  markWaitingChildren(jobId: JobId, timeline?: JobTimelineEntry[]): void {
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      this.db
        .prepare('UPDATE jobs SET state = ?, started_at = NULL, timeline = ? WHERE id = ?')
        .run('waiting-children', timeline && timeline.length > 0 ? pack(timeline) : null, jobId);
    });
  }

  markCompleted(jobId: JobId, completedAt: number, timeline?: JobTimelineEntry[]): void {
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      this.statements
        .get('completeJob')!
        .run(
          'completed',
          completedAt,
          timeline && timeline.length > 0 ? pack(timeline) : null,
          jobId
        );
    });
  }

  markFailed(job: Job, error: string | null): void {
    this.flushIfBuffered(job.id);
    this.safeWrite(() => {
      this.statements.get('insertDlq')!.run(job.id, job.queue, pack({ job, error }), Date.now());
    });
  }

  /** Save DLQ entry with full metadata */
  saveDlqEntry(entry: DlqEntry): void {
    this.safeWrite(() => {
      this.statements
        .get('insertDlq')!
        .run(entry.job.id, entry.job.queue, pack(entry), entry.enteredAt);
    });
  }

  /**
   * Commit a terminal child outcome, its optional DLQ row, and its flow outbox
   * record together. Recovery therefore never observes only half the transition.
   */
  commitFailedJob(
    jobId: JobId,
    entry: DlqEntry | null,
    flowFailure: FlowFailureRecord | null
  ): void {
    this.writeBuffer.removePending(jobId);
    this.safeWrite(() => {
      const tx = this.db.transaction(() => {
        if (entry) {
          this.statements
            .get('insertDlq')!
            .run(entry.job.id, entry.job.queue, pack(entry), entry.enteredAt);
        }
        if (flowFailure) {
          this.db
            .prepare(
              `INSERT INTO flow_failures
               (parent_id, child_id, child_queue, mode, error, created_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(parent_id, child_id) DO UPDATE SET
                 child_queue=excluded.child_queue, mode=excluded.mode,
                 error=excluded.error, created_at=excluded.created_at`
            )
            .run(
              flowFailure.parentId,
              flowFailure.childId,
              flowFailure.childQueue,
              flowFailure.mode,
              flowFailure.error,
              flowFailure.createdAt
            );
        }
        this.statements.get('deleteJob')!.run(jobId);
        this.statements.get('deleteJobResult')!.run(jobId);
      });
      tx();
    });
  }

  /** Delete DLQ entry by job ID */
  deleteDlqEntry(jobId: JobId): void {
    this.safeWrite(() => {
      this.statements.get('deleteDlqEntry')!.run(jobId);
    });
  }

  /** Clear all DLQ entries for a queue */
  clearDlqQueue(queue: string): void {
    this.safeWrite(() => {
      this.statements.get('clearDlqQueue')!.run(queue);
    });
  }

  /**
   * Permanently purge terminal DLQ jobs in one transaction.
   * Pending buffered inserts are removed first so a purged job cannot be
   * written back after the transaction commits.
   */
  purgeDlqEntries(
    queue: string,
    dlqJobIds: readonly JobId[],
    terminalJobIds: readonly JobId[],
    clearQueue: boolean
  ): void {
    for (const jobId of terminalJobIds) this.writeBuffer.removePending(jobId);

    this.safeWrite(() => {
      const deleteDlq = this.statements.get('deleteDlqEntryForQueue')!;
      const clearDlq = this.statements.get('clearDlqQueue')!;
      const deleteJob = this.statements.get('deleteJob')!;
      const deleteResult = this.statements.get('deleteJobResult')!;
      const tx = this.db.transaction(() => {
        if (clearQueue) {
          clearDlq.run(queue);
        } else {
          for (const jobId of dlqJobIds) deleteDlq.run(jobId, queue);
        }
        for (const jobId of terminalJobIds) {
          deleteJob.run(jobId);
          deleteResult.run(jobId);
        }
      });
      tx();
    });
  }

  /** Load all DLQ entries */
  loadDlq(): Map<string, DlqEntry[]> {
    interface DbDlqRow {
      job_id: string;
      queue: string;
      entry: Uint8Array;
      entered_at: number;
    }
    const rows = this.statements.get('loadDlq')!.all() as DbDlqRow[];
    const result = new Map<string, DlqEntry[]>();

    for (const row of rows) {
      const entry = unpack<DlqEntry | null>(row.entry, null, `loadDlq:${row.job_id}`);
      if (!entry?.job) continue;

      const reconstructedEntry = reconstructDlqEntry(entry);

      let queueEntries = result.get(row.queue);
      if (!queueEntries) {
        queueEntries = [];
        result.set(row.queue, queueEntries);
      }
      queueEntries.push(reconstructedEntry);
    }

    return result;
  }

  updateForRetry(job: Job): void {
    this.safeWrite(() => {
      this.db
        .prepare(
          'UPDATE jobs SET attempts = ?, stall_count = ?, run_at = ?, state = ?, timeline = ?, stacktrace = ? WHERE id = ?'
        )
        .run(
          job.attempts,
          persistedStallCount(job),
          job.runAt,
          'waiting',
          job.timeline.length > 0 ? pack(job.timeline) : null,
          job.stacktrace && job.stacktrace.length > 0 ? pack(job.stacktrace) : null,
          job.id
        );
    });
  }

  deleteJob(jobId: JobId): void {
    this.writeBuffer.removePending(jobId);
    this.safeWrite(() => {
      // Atomic cascade: job row + result must succeed or fail together so
      // cleanAsync cannot leave orphan job_results rows (issue #84).
      // DLQ is NOT cascaded here: moveFailedJobToDlq() intentionally calls
      // saveDlqEntry() then deleteJob() to drop the jobs row while keeping
      // the DLQ entry. Callers that want DLQ cleanup (e.g. cleanFailed)
      // explicitly call deleteDlqEntry() before deleteJob().
      const deleteStmt = this.statements.get('deleteJob')!;
      const deleteResultStmt = this.statements.get('deleteJobResult')!;
      const tx = this.db.transaction((id: JobId) => {
        deleteStmt.run(id);
        deleteResultStmt.run(id);
        this.db.prepare('DELETE FROM flow_failures WHERE parent_id = ?').run(id);
      });
      tx(jobId);
    });
  }

  /** Update a job's data blob (e.g. after adding __parentId) */
  updateJobData(jobId: JobId, data: unknown): void {
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      this.db.prepare('UPDATE jobs SET data = ? WHERE id = ?').run(pack(data), jobId);
    });
  }

  /** Clear a released deduplication key so recovery cannot resurrect it. */
  clearJobUniqueKey(jobId: JobId): void {
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      this.db.prepare('UPDATE jobs SET unique_key = NULL WHERE id = ?').run(jobId);
    });
  }

  /** Persist priority ordering, including its LIFO tie-break, across recovery. */
  updateJobPriority(jobId: JobId, priority: number, lifo: boolean): void {
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      this.db
        .prepare('UPDATE jobs SET priority = ?, lifo = ? WHERE id = ?')
        .run(priority, lifo ? 1 : 0, jobId);
    });
  }

  /** Persist progress and its heartbeat as one active-job mutation. */
  updateJobProgress(
    jobId: JobId,
    progress: number,
    message: string | null,
    lastHeartbeat: number
  ): void {
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      this.statements.get('updateJobProgress')!.run(progress, message, lastHeartbeat, jobId);
    });
  }

  /**
   * Persist a delay change (moveToDelayed / changeDelay) so the new `run_at`
   * survives a restart. Without this the delay lives only in the in-memory heap:
   * recovery reloads the stale on-disk `run_at` and the job is immediately
   * pullable again. State is re-derived from `run_at` (future → 'delayed'),
   * mirroring the insert convention; `started_at` is cleared because a job moved
   * back to the queue from `active` is no longer running.
   */
  updateRunAt(jobId: JobId, runAt: number): void {
    // Flush a still-buffered insert first (like markActive/markCompleted/markFailed)
    // so the UPDATE lands on a real row — otherwise the delay would be lost across a
    // restart when the job was added (non-durable) and re-delayed within the same
    // ~10ms write-buffer window.
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      const state = runAt > Date.now() ? 'delayed' : 'waiting';
      this.db
        .prepare('UPDATE jobs SET run_at = ?, state = ?, started_at = NULL WHERE id = ?')
        .run(runAt, state, jobId);
    });
  }

  /** Update a job's children_ids blob and parent_id */
  updateJobChildrenIds(jobId: JobId, childrenIds: JobId[]): void {
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      this.db
        .prepare('UPDATE jobs SET children_ids = ? WHERE id = ?')
        .run(childrenIds.length > 0 ? pack(childrenIds) : null, jobId);
    });
  }

  /** Persist both sides of a legacy parent link in one SQLite transaction. */
  updateFlowLink(
    child: Pick<Job, 'id' | 'parentId' | 'data'>,
    parent: Pick<Job, 'id' | 'childrenIds' | 'dependsOn' | 'data'>,
    parentState: 'waiting-children' | 'waiting' | 'prioritized' | 'delayed'
  ): void {
    this.flushIfBuffered(child.id);
    this.flushIfBuffered(parent.id);
    this.safeWrite(() => {
      const tx = this.db.transaction(() => {
        this.db
          .prepare('UPDATE jobs SET parent_id = ?, data = ? WHERE id = ?')
          .run(child.parentId, pack(child.data), child.id);
        this.db
          .prepare(
            'UPDATE jobs SET children_ids = ?, depends_on = ?, data = ?, state = ? WHERE id = ?'
          )
          .run(
            parent.childrenIds.length > 0 ? pack(parent.childrenIds) : null,
            parent.dependsOn.length > 0 ? pack(parent.dependsOn) : null,
            pack(parent.data),
            parentState,
            parent.id
          );
      });
      tx();
    });
  }

  /**
   * Persist a legacy SDK parent-id backpatch without changing parent state.
   *
   * A fast child can finish before the SDK sends UpdateParent. Completed jobs
   * remain in `jobs`, while failed jobs live only in the serialized DLQ entry.
   * The failure outbox is re-keyed in the same transaction so a crash between
   * this write and in-memory propagation still targets the real parent.
   */
  backpatchFlowChild(
    child: Pick<Job, 'id' | 'parentId' | 'data'>,
    previousParentId: JobId | null
  ): void {
    this.flushIfBuffered(child.id);
    this.safeWrite(() => {
      const tx = this.db.transaction(() => {
        let persisted =
          this.db
            .prepare('UPDATE jobs SET parent_id = ?, data = ? WHERE id = ?')
            .run(child.parentId, pack(child.data), child.id).changes > 0;

        const dlqRows = this.db
          .query<{ id: number; entry: Uint8Array }, [string]>(
            'SELECT id, entry FROM dlq WHERE job_id = ?'
          )
          .all(String(child.id));
        for (const row of dlqRows) {
          const entry = unpack<DlqEntry | null>(
            row.entry,
            null,
            `backpatchFlowChild:${String(child.id)}`
          );
          if (!entry?.job) continue;
          const updated = {
            ...entry,
            job: { ...entry.job, parentId: child.parentId, data: child.data },
          };
          this.db.prepare('UPDATE dlq SET entry = ? WHERE id = ?').run(pack(updated), row.id);
          persisted = true;
        }

        if (!persisted) {
          throw new Error(`Flow child is no longer persisted: ${String(child.id)}`);
        }

        if (previousParentId && child.parentId && previousParentId !== child.parentId) {
          const failure = this.db
            .query<
              {
                child_queue: string;
                mode: string;
                error: string;
                created_at: number;
              },
              [string, string]
            >(
              `SELECT child_queue, mode, error, created_at
               FROM flow_failures
               WHERE parent_id = ? AND child_id = ?`
            )
            .get(String(previousParentId), String(child.id));
          if (failure) {
            this.db
              .prepare(
                `INSERT INTO flow_failures
                 (parent_id, child_id, child_queue, mode, error, created_at)
                 VALUES (?, ?, ?, ?, ?, ?)
                 ON CONFLICT(parent_id, child_id) DO UPDATE SET
                   child_queue=excluded.child_queue, mode=excluded.mode,
                   error=excluded.error, created_at=excluded.created_at`
              )
              .run(
                child.parentId,
                child.id,
                failure.child_queue,
                failure.mode,
                failure.error,
                failure.created_at
              );
            this.db
              .prepare('DELETE FROM flow_failures WHERE parent_id = ? AND child_id = ?')
              .run(previousParentId, child.id);
          }
        }
      });
      tx();
    });
  }

  /** Persist both sides of a removed parent/child relationship atomically. */
  removeFlowLink(
    child: Pick<Job, 'id' | 'parentId' | 'data'>,
    parent: Pick<Job, 'id' | 'childrenIds' | 'dependsOn' | 'data' | 'runAt'>,
    parentState: 'waiting-children' | 'waiting' | 'prioritized'
  ): void {
    this.flushIfBuffered(child.id);
    this.flushIfBuffered(parent.id);
    this.safeWrite(() => {
      const tx = this.db.transaction(() => {
        this.db
          .prepare('UPDATE jobs SET parent_id = ?, data = ? WHERE id = ?')
          .run(child.parentId, pack(child.data), child.id);
        this.db
          .prepare(
            `UPDATE jobs
             SET children_ids = ?, depends_on = ?, data = ?, run_at = ?, state = ?
             WHERE id = ?`
          )
          .run(
            parent.childrenIds.length > 0 ? pack(parent.childrenIds) : null,
            parent.dependsOn.length > 0 ? pack(parent.dependsOn) : null,
            pack(parent.data),
            parent.runAt,
            parentState,
            parent.id
          );
        this.db
          .prepare('DELETE FROM flow_failures WHERE parent_id = ? AND child_id = ?')
          .run(parent.id, child.id);
      });
      tx();
    });
  }

  /** Record terminal child propagation before applying its parent-side effect. */
  saveFlowFailure(record: FlowFailureRecord): void {
    this.safeWrite(() => {
      this.db
        .prepare(
          `INSERT INTO flow_failures
           (parent_id, child_id, child_queue, mode, error, created_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(parent_id, child_id) DO UPDATE SET
             child_queue=excluded.child_queue, mode=excluded.mode,
             error=excluded.error, created_at=excluded.created_at`
        )
        .run(
          record.parentId,
          record.childId,
          record.childQueue,
          record.mode,
          record.error,
          record.createdAt
        );
    });
  }

  loadFlowFailures(): FlowFailureRecord[] {
    const rows = this.db
      .query<
        {
          parent_id: string;
          child_id: string;
          child_queue: string;
          mode: string;
          error: string;
          created_at: number;
        },
        []
      >('SELECT * FROM flow_failures ORDER BY created_at, parent_id, child_id')
      .all();
    return rows.map((row) => ({
      parentId: row.parent_id as JobId,
      childId: row.child_id as JobId,
      childQueue: row.child_queue,
      mode: row.mode as FlowFailureMode,
      error: row.error,
      createdAt: row.created_at,
    }));
  }

  deleteFlowFailure(parentId: JobId, childId?: JobId): void {
    this.safeWrite(() => {
      if (childId) {
        this.db
          .prepare('DELETE FROM flow_failures WHERE parent_id = ? AND child_id = ?')
          .run(parentId, childId);
      } else {
        this.db.prepare('DELETE FROM flow_failures WHERE parent_id = ?').run(parentId);
      }
    });
  }

  /** Persist dependency removal/promotion as one parent-side recovery checkpoint. */
  updateFlowParentResolution(
    job: Pick<Job, 'id' | 'dependsOn' | 'runAt' | 'priority' | 'timeline'>,
    stateOverride?: 'waiting-children' | 'waiting' | 'prioritized' | 'delayed'
  ): void {
    this.flushIfBuffered(job.id);
    const state =
      stateOverride ??
      (job.dependsOn.length > 0
        ? 'waiting-children'
        : job.runAt > Date.now()
          ? 'delayed'
          : job.priority > 0
            ? 'prioritized'
            : 'waiting');
    this.safeWrite(() => {
      this.db
        .prepare('UPDATE jobs SET depends_on = ?, run_at = ?, state = ?, timeline = ? WHERE id = ?')
        .run(
          job.dependsOn.length > 0 ? pack(job.dependsOn) : null,
          job.runAt,
          state,
          job.timeline.length > 0 ? pack(job.timeline) : null,
          job.id
        );
    });
  }

  getJob(id: JobId): Job | null {
    const row = this.statements.get('getJob')!.get(id) as DbJob | null;
    return row ? rowToJob(row) : null;
  }

  storeResult(jobId: JobId, result: unknown): void {
    this.safeWrite(() => {
      this.statements.get('insertResult')!.run(jobId, pack(result), Date.now());
    });
  }

  getResult(jobId: JobId): unknown {
    const row = this.statements.get('getResult')!.get(jobId) as { result: Uint8Array } | null;
    return row ? unpack(row.result, null, `getResult:${jobId}`) : null;
  }

  /** Check if a job result exists (for dependency checking during recovery) */
  hasResult(jobId: JobId): boolean {
    const row = this.db
      .query<{ job_id: string }, [string]>('SELECT job_id FROM job_results WHERE job_id = ?')
      .get(String(jobId));
    return row !== null;
  }

  /** Check if a job has a DLQ entry (used for state/job fallback after restart) */
  hasDlqEntry(jobId: JobId): boolean {
    const row = this.db
      .query<{ job_id: string }, [string]>('SELECT job_id FROM dlq WHERE job_id = ? LIMIT 1')
      .get(String(jobId));
    return row !== null;
  }

  /** Get latest DLQ entry for a job (used for getJob fallback after restart) */
  getDlqEntry(jobId: JobId): DlqEntry | null {
    const row = this.db
      .query<{ entry: Uint8Array }, [string]>(
        'SELECT entry FROM dlq WHERE job_id = ? ORDER BY entered_at DESC LIMIT 1'
      )
      .get(String(jobId));
    if (!row) return null;
    const entry = unpack<DlqEntry | null>(row.entry, null, `getDlqEntry:${String(jobId)}`);
    return entry?.job ? reconstructDlqEntry(entry) : null;
  }

  /** Load all DLQ job IDs (used by recovery to skip stale active rows) */
  loadDlqJobIds(): Set<JobId> {
    const rows = this.db.query<{ job_id: string }, []>('SELECT job_id FROM dlq').all();
    return new Set(rows.map((r) => r.job_id as JobId));
  }

  /** Get the persisted `state` column for a job. Returns null if the row is missing. */
  getJobStateRaw(jobId: JobId): string | null {
    const row = this.db
      .query<{ state: string }, [string]>('SELECT state FROM jobs WHERE id = ?')
      .get(String(jobId));
    return row?.state ?? null;
  }

  /** Load all completed job IDs (for dependency recovery) */
  loadCompletedJobIds(): Set<JobId> {
    // `jobs.state` is the completion authority. A result row can exist while its
    // job is still active if a process dies between the two legacy writes, so it
    // must never release a dependency by itself.
    const ids = new Set<JobId>();
    const stateRows = this.db
      .query<{ id: string }, []>("SELECT id FROM jobs WHERE state = 'completed'")
      .all();
    for (const r of stateRows) ids.add(r.id as JobId);
    for (const record of this.dependencyCompletionStore.load()) ids.add(record.jobId);
    return ids;
  }

  /** Atomically replace a removed completed job with payload-free dependency evidence. */
  commitRemovedCompletion(
    job: Pick<Job, 'id' | 'queue'>,
    retentionLimit: number,
    pinned: boolean,
    completedAt: number = Date.now()
  ): void {
    this.writeBuffer.removePending(job.id);
    this.safeWrite(() => {
      this.dependencyCompletionStore.commit(
        { jobId: job.id, queue: job.queue, completedAt, pinned },
        retentionLimit
      );
    });
  }

  loadDependencyCompletions(): DependencyCompletionRecord[] {
    return this.dependencyCompletionStore.load();
  }

  pinDependencyCompletions(jobIds: Iterable<JobId>): void {
    this.safeWrite(() => {
      this.dependencyCompletionStore.pin(jobIds);
    });
  }

  unpinDependencyCompletions(
    jobIds: Iterable<JobId>,
    retentionLimit: number
  ): DependencyCompletionRecord[] {
    let records: DependencyCompletionRecord[] = [];
    this.safeWrite(() => {
      records = this.dependencyCompletionStore.unpin(jobIds, retentionLimit);
    });
    return records;
  }

  reconcileDependencyCompletionPins(
    referenced: ReadonlySet<JobId>,
    retentionLimit: number
  ): DependencyCompletionRecord[] {
    let records: DependencyCompletionRecord[] = [];
    this.safeWrite(() => {
      records = this.dependencyCompletionStore.reconcilePins(referenced, retentionLimit);
    });
    return records;
  }

  deleteDependencyCompletion(jobId: JobId): boolean {
    let deleted = false;
    this.safeWrite(() => {
      deleted = this.dependencyCompletionStore.delete(jobId);
    });
    return deleted;
  }

  deleteDependencyCompletionsForQueue(queue: string): JobId[] {
    let deleted: JobId[] = [];
    this.safeWrite(() => {
      deleted = this.dependencyCompletionStore.deleteForQueue(queue);
    });
    return deleted;
  }

  // ============ Bulk Operations ============

  /**
   * Insert batch of jobs. By default the jobs go through the write buffer.
   * When `durable` is true they bypass the buffer and are written to disk
   * immediately (matching single-push durable semantics) — used for addBulk
   * jobs flagged `durable: true`.
   */
  insertJobsBatch(jobs: Job[], durable?: boolean): void {
    if (durable) {
      // Atomic immediate write: every row hits disk in ONE transaction, so a
      // mid-batch failure rolls back the whole batch (no partial on-disk state)
      // — bypassing the write buffer to honor the durable contract.
      this.safeWrite(() => {
        const tx = this.db.transaction((batch: Job[]) => {
          for (const job of batch) this.runInsertJobStmt(job);
        });
        tx(jobs);
      });
      return;
    }
    this.writeBuffer.addBatch(jobs);
  }

  // ============ Query Operations ============

  /**
   * Query jobs by queue with optional state filter and pagination.
   * This is the raw persisted-state query. Logical waiting/prioritized/delayed
   * views should use queryJobsByLogicalStates().
   */
  queryJobs(
    queue: string,
    options: { state?: string; states?: string[]; limit: number; offset: number; asc: boolean }
  ): Job[] {
    const order = options.asc ? 'ASC' : 'DESC';
    let rows: DbJob[];

    if (options.states && options.states.length > 0) {
      const placeholders = options.states.map(() => '?').join(',');
      rows = this.db
        .query<DbJob, (string | number)[]>(
          `SELECT * FROM jobs WHERE queue = ? AND state IN (${placeholders}) ORDER BY created_at ${order}, id ${order} LIMIT ? OFFSET ?`
        )
        .all(queue, ...options.states, options.limit, options.offset);
    } else if (options.state) {
      rows = this.db
        .query<DbJob, [string, string, number, number]>(
          `SELECT * FROM jobs WHERE queue = ? AND state = ? ORDER BY created_at ${order}, id ${order} LIMIT ? OFFSET ?`
        )
        .all(queue, options.state, options.limit, options.offset);
    } else {
      rows = this.db
        .query<DbJob, [string, number, number]>(
          `SELECT * FROM jobs WHERE queue = ? ORDER BY created_at ${order}, id ${order} LIMIT ? OFFSET ?`
        )
        .all(queue, options.limit, options.offset);
    }

    return rows.map((row) => rowToJob(row));
  }

  /**
   * Query public/logical job states before applying pagination.
   *
   * SQLite persists pending jobs as waiting/delayed, while the public state is
   * time-dependent: a delayed row becomes waiting or prioritized once run_at
   * is reached. Keeping these predicates in SQL makes OFFSET count the logical
   * result rather than the unfiltered persisted rows.
   */
  queryJobsByLogicalStates(
    queue: string,
    states: string[],
    options: { limit: number; offset: number; asc: boolean; now: number }
  ): Job[] {
    const uniqueStates = Array.from(new Set(states));
    if (uniqueStates.length === 0) return [];

    const predicates: string[] = [];
    const predicateParams: (string | number)[] = [];
    const requested = new Set(uniqueStates);

    if (requested.has('waiting')) {
      predicates.push(
        "(state IN ('waiting', 'prioritized', 'delayed') AND run_at <= ? AND priority <= 0)"
      );
      predicateParams.push(options.now);
    }
    if (requested.has('prioritized')) {
      predicates.push(
        "(state IN ('waiting', 'prioritized', 'delayed') AND run_at <= ? AND priority > 0)"
      );
      predicateParams.push(options.now);
    }
    if (requested.has('delayed')) {
      predicates.push("(state IN ('waiting', 'prioritized', 'delayed') AND run_at > ?)");
      predicateParams.push(options.now);
    }

    const persistedStates = uniqueStates.filter(
      (state) => state !== 'waiting' && state !== 'prioritized' && state !== 'delayed'
    );
    if (persistedStates.length > 0) {
      predicates.push(`state IN (${persistedStates.map(() => '?').join(',')})`);
      predicateParams.push(...persistedStates);
    }

    const order = options.asc ? 'ASC' : 'DESC';
    const rows = this.db
      .query<DbJob, (string | number)[]>(
        `SELECT * FROM jobs INDEXED BY idx_jobs_queue_created
         WHERE queue = ? AND (${predicates.join(' OR ')})
         ORDER BY created_at ${order}, id ${order}
         LIMIT ? OFFSET ?`
      )
      .all(queue, ...predicateParams, options.limit, options.offset);

    return rows.map((row) => rowToJob(row));
  }

  /**
   * Load pending jobs with pagination for efficient recovery.
   * Orders by priority (desc), run_at (asc), and id (asc) so page boundaries
   * remain deterministic when scheduling fields compare equal.
   * @param limit Max jobs to return (default: 10000)
   * @param offset Skip first N jobs (default: 0)
   */
  loadPendingJobs(limit: number = 10000, offset: number = 0): Job[] {
    const rows = this.db
      .query<DbJob, [number, number]>(
        "SELECT * FROM jobs WHERE state IN ('waiting', 'prioritized', 'waiting-children', 'delayed') ORDER BY priority DESC, run_at ASC, id ASC LIMIT ? OFFSET ?"
      )
      .all(limit, offset);
    return rows.map((row) => rowToJob(row));
  }

  /**
   * Load active jobs with pagination.
   * @param limit Max jobs to return (default: 10000)
   * @param offset Skip first N jobs (default: 0)
   */
  loadActiveJobs(limit: number = 10000, offset: number = 0): Job[] {
    const rows = this.db
      .query<DbJob, [number, number]>(
        "SELECT * FROM jobs WHERE state = 'active' ORDER BY started_at ASC LIMIT ? OFFSET ?"
      )
      .all(limit, offset);
    return rows.map((row) => rowToJob(row));
  }

  /**
   * Load completed jobs with pagination.
   * Uses job_results join to get only jobs that were successfully completed.
   * @param limit Max jobs to return (default: 10000)
   * @param offset Skip first N jobs (default: 0)
   */
  loadCompletedJobs(limit: number = 10000, offset: number = 0): Job[] {
    const rows = this.db
      .query<DbJob, [number, number]>(
        "SELECT * FROM jobs WHERE state = 'completed' ORDER BY completed_at DESC LIMIT ? OFFSET ?"
      )
      .all(limit, offset);
    return rows.map((row) => rowToJob(row));
  }

  /**
   * Count pending jobs (for pagination)
   */
  countPendingJobs(): number {
    const result = this.db
      .query<{ count: number }, []>(
        "SELECT COUNT(*) as count FROM jobs WHERE state IN ('waiting', 'prioritized', 'waiting-children', 'delayed')"
      )
      .get();
    return result?.count ?? 0;
  }

  /**
   * Count active jobs (for pagination)
   */
  countActiveJobs(): number {
    const result = this.db
      .query<{ count: number }, []>("SELECT COUNT(*) as count FROM jobs WHERE state = 'active'")
      .get();
    return result?.count ?? 0;
  }

  // ============ Cron Operations ============

  saveCron(cron: CronJob): void {
    this.safeWrite(() => {
      this.statements
        .get('insertCron')!
        .run(
          cron.name,
          cron.queue,
          pack(cron.data),
          cron.schedule,
          cron.repeatEvery,
          cron.priority,
          cron.nextRun,
          cron.executions,
          cron.maxLimit,
          cron.timezone,
          cron.uniqueKey,
          cron.dedup ? pack(cron.dedup) : null,
          cron.skipMissedOnRestart ? 1 : 0,
          cron.skipIfNoWorker ? 1 : 0,
          cron.preventOverlap ? 1 : 0,
          cron.jobOptions ? pack(cron.jobOptions) : null
        );
    });
  }

  loadCronJobs(): CronJob[] {
    const rows = this.db.query<DbCron, []>('SELECT * FROM cron_jobs').all();
    return rows.map((row) => ({
      name: row.name,
      queue: row.queue,
      data: unpack(row.data, {}, `loadCronJobs:${row.name}`),
      schedule: row.schedule,
      repeatEvery: row.repeat_every,
      priority: row.priority,
      timezone: row.timezone,
      nextRun: row.next_run,
      executions: row.executions,
      maxLimit: row.max_limit,
      uniqueKey: row.unique_key ?? null,
      dedup: row.dedup
        ? (unpack(row.dedup, null, `loadCronDedup:${row.name}`) as CronJob['dedup'])
        : null,
      skipMissedOnRestart: row.skip_missed_on_restart === 1,
      skipIfNoWorker: row.skip_if_no_worker === 1,
      preventOverlap: row.prevent_overlap === 1,
      jobOptions: row.job_options
        ? (unpack(row.job_options, null, `loadCronJobOptions:${row.name}`) as CronJob['jobOptions'])
        : null,
    }));
  }

  deleteCron(name: string): void {
    this.safeWrite(() => {
      this.db.prepare('DELETE FROM cron_jobs WHERE name = ?').run(name);
    });
  }

  /** Update cron job execution state (executions count and next run time) */
  updateCron(name: string, executions: number, nextRun: number): void {
    this.safeWrite(() => {
      this.statements.get('updateCron')!.run(executions, nextRun, name);
    });
  }

  // ============ Queue Control-State (#100) ============

  /**
   * Persist a queue's control-state (paused / rate-limit / concurrency) so it
   * survives a server restart. Write-through on every pause/resume/limit change.
   */
  saveQueueState(
    name: string,
    state: {
      paused: boolean;
      rateLimit: number | null;
      concurrencyLimit: number | null;
      rateLimitDuration?: number | null;
      rateLimitExpiresAt?: number | null;
      stallConfig?: StallConfig | null;
      dlqConfig?: DlqConfig | null;
    }
  ): void {
    this.safeWrite(() => {
      this.statements
        .get('upsertQueueState')!
        .run(
          name,
          state.paused ? 1 : 0,
          state.rateLimit,
          state.concurrencyLimit,
          state.rateLimitDuration ?? null,
          state.rateLimitExpiresAt ?? null,
          state.stallConfig === null || state.stallConfig === undefined
            ? null
            : state.stallConfig.enabled
              ? 1
              : 0,
          state.stallConfig?.stallInterval ?? null,
          state.stallConfig?.maxStalls ?? null,
          state.stallConfig?.gracePeriod ?? null,
          state.dlqConfig ? pack(state.dlqConfig) : null
        );
    });
  }

  /** Load all persisted queue control-state rows (used by recover() on boot). */
  loadQueueState(): Array<{
    name: string;
    paused: boolean;
    rateLimit: number | null;
    concurrencyLimit: number | null;
    rateLimitDuration: number | null;
    rateLimitExpiresAt: number | null;
    stallConfig: StallConfig | null;
    dlqConfig: DlqConfig | null;
  }> {
    const rows = this.statements.get('loadQueueState')!.all() as DbQueueState[];
    return rows.map((row) => ({
      name: row.name,
      paused: row.paused === 1,
      rateLimit: row.rate_limit,
      concurrencyLimit: row.concurrency_limit,
      rateLimitDuration: row.rate_limit_duration ?? null,
      rateLimitExpiresAt: row.rate_limit_expires_at ?? null,
      stallConfig:
        row.stall_enabled === null ||
        row.stall_interval === null ||
        row.max_stalls === null ||
        row.stall_grace_period === null
          ? null
          : {
              enabled: row.stall_enabled === 1,
              stallInterval: row.stall_interval,
              maxStalls: row.max_stalls,
              gracePeriod: row.stall_grace_period,
            },
      dlqConfig: unpack<DlqConfig | null>(row.dlq_config, null, `loadQueueDlqConfig:${row.name}`),
    }));
  }

  /** Drop a queue's persisted control-state (e.g. on obliterate). */
  deleteQueueState(name: string): void {
    this.safeWrite(() => {
      this.statements.get('deleteQueueState')!.run(name);
    });
  }

  // ============ Utilities ============

  close(): void {
    this.writeBuffer.stop();

    try {
      this.writeBuffer.flush();
    } catch (err) {
      storageLog.error('Failed to flush write buffer on close', {
        bufferedJobs: this.writeBuffer.pendingCount,
        error: err instanceof Error ? err.message : String(err),
      });
    }

    // WAL checkpoint before close to prevent stale locks on restart
    try {
      this.db.run('PRAGMA wal_checkpoint(TRUNCATE)');
    } catch (err) {
      storageLog.error('WAL checkpoint failed on close', {
        error: err instanceof Error ? err.message : String(err),
      });
    }

    this.db.close();
  }

  getSize(): number {
    const file = Bun.file(this.db.filename);
    return file.size;
  }
}
