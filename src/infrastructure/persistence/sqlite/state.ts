import { Database } from 'bun:sqlite';
import { createDlqEntry, FailureReason, type DlqEntry } from '../../../domain/types/dlq';
import type { Job, JobId } from '../../../domain/types/job';
import { storageLog } from '../../../shared/logger';
import { BatchInsertManager, WriteBuffer } from '../sqliteBatch';
import { DependencyCompletionStore } from '../dependencyCompletionStore';
import { PRAGMA_SETTINGS } from '../schema';
import { assertSupportedSqliteSchema, migrateSqliteDatabase } from '../sqliteMigration';
import { persistedJobStateForWrite } from '../sqliteSerializer';
import { prepareStatements, type StatementName } from '../statements';
import type { SqliteConfig, SqliteCriticalLoss, SqliteCriticalLossCallback } from '../types/sqlite';
import { SqliteTelemetryStore } from './telemetryStore';

function isSqliteFullError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  return (
    error.message.includes('SQLITE_FULL') || error.message.includes('database or disk is full')
  );
}

/** Owns the database, prepared statements, buffering, and storage health state. */
export abstract class SqliteState {
  protected readonly db: Database;
  protected readonly statements: Map<StatementName, ReturnType<Database['prepare']>>;
  protected readonly batchManager: BatchInsertManager;
  protected readonly writeBuffer: WriteBuffer;
  protected readonly dependencyCompletionStore: DependencyCompletionStore;
  protected readonly telemetryStore: SqliteTelemetryStore;
  protected _diskFull = false;
  protected _lastDiskFullError: string | null = null;
  protected _lastDiskFullAt: number | null = null;
  protected readonly _criticalLosses: SqliteCriticalLoss[] = [];
  protected readonly _onCriticalLoss?: SqliteCriticalLossCallback;
  private static readonly MAX_RETAINED_LOSSES = 100;

  protected abstract saveDlqEntry(entry: DlqEntry): void;

  constructor(config: SqliteConfig) {
    this.db = new Database(config.path, { create: true });
    this._onCriticalLoss = config.onCriticalLoss;
    try {
      assertSupportedSqliteSchema(this.db);
      try {
        this.db.run(PRAGMA_SETTINGS);
      } catch (error) {
        storageLog.error('Failed to apply PRAGMA settings', {
          error: error instanceof Error ? error.message : String(error),
        });
      }
      migrateSqliteDatabase(this.db, config.path);
      this.dependencyCompletionStore = new DependencyCompletionStore(this.db);
      this.statements = prepareStatements(this.db);
      this.telemetryStore = new SqliteTelemetryStore(this.db);
      this.batchManager = new BatchInsertManager(this.db);
      this.writeBuffer = new WriteBuffer(
        this.batchManager,
        config.writeBufferSize ?? 100,
        config.writeBufferFlushMs ?? 10,
        (error, jobCount) => {
          if (isSqliteFullError(error)) this.setDiskFull(error.message);
          if (/constraint failed/i.test(error.message)) {
            storageLog.error('Write buffer rejected jobs (constraint violation, dropped)', {
              rejectedJobCount: jobCount,
              error: error.message,
            });
          } else {
            storageLog.error('Write buffer flush failed', {
              jobCount,
              error: error.message,
              diskFull: this._diskFull,
            });
          }
        },
        (jobs, lastError, attempts) => {
          this.handleCriticalLoss(jobs, lastError, attempts);
        }
      );
    } catch (error) {
      try {
        this.db.close();
      } catch {
        // Preserve the initialization error.
      }
      throw error;
    }
  }

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
        dataPreview: this.previewJobData(job.data),
      });
    }
    this._criticalLosses.push({
      jobs,
      error: lastError.message,
      attempts,
      at: Date.now(),
    });
    while (this._criticalLosses.length > SqliteState.MAX_RETAINED_LOSSES) {
      this._criticalLosses.shift();
    }
    for (const job of jobs) {
      try {
        this.saveDlqEntry(createDlqEntry(job, FailureReason.Unknown, lastError.message));
      } catch (error) {
        storageLog.error('Failed to persist lost job to DLQ', {
          id: String(job.id),
          queue: job.queue,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }
    if (this._onCriticalLoss) {
      try {
        this._onCriticalLoss(jobs, lastError, attempts);
      } catch (error) {
        storageLog.error('onCriticalLoss callback threw', {
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }

  private previewJobData(data: unknown): string {
    try {
      const serialized = JSON.stringify(data);
      return serialized.length > 500 ? serialized.slice(0, 500) + '...[truncated]' : serialized;
    } catch {
      return '[unserializable]';
    }
  }

  getCriticalLosses(): readonly SqliteCriticalLoss[] {
    return this._criticalLosses;
  }

  clearCriticalLosses(): void {
    this._criticalLosses.length = 0;
  }

  private setDiskFull(message: string): void {
    if (!this._diskFull) {
      storageLog.error('DISK FULL: SQLite cannot write, persistence degraded', { error: message });
    }
    this._diskFull = true;
    this._lastDiskFullError = message;
    this._lastDiskFullAt = Date.now();
  }

  protected safeWrite(operation: () => void): void {
    try {
      operation();
      if (this._diskFull) {
        this._diskFull = false;
        this._lastDiskFullError = null;
        storageLog.info('Disk full condition cleared - writes succeeding again');
      }
    } catch (error) {
      if (isSqliteFullError(error)) {
        this.setDiskFull(error instanceof Error ? error.message : String(error));
      }
      throw error;
    }
  }

  get diskFull(): boolean {
    return this._diskFull;
  }

  getDiskFullStatus(): { diskFull: boolean; error: string | null; since: number | null } {
    return {
      diskFull: this._diskFull,
      error: this._lastDiskFullError,
      since: this._lastDiskFullAt,
    };
  }

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

  withDeferredBufferFlush<T>(operation: () => T): T {
    return this.writeBuffer.withDeferredAutoFlush(operation);
  }

  getBufferedJobs(queue?: string): Job[] {
    return this.writeBuffer.getPendingJobs(queue);
  }

  getBufferedJob(jobId: JobId): Job | null {
    return this.writeBuffer.getPendingJob(jobId);
  }

  getBufferedJobState(jobId: JobId): string | null {
    const job = this.writeBuffer.getPendingJob(jobId);
    return job ? persistedJobStateForWrite(job) : null;
  }
}
