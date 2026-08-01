import type { Database } from 'bun:sqlite';
import type { Job } from '../../domain/types/job';
import { pack, persistedInitialState, persistedStallCount } from './sqliteSerializer';
import type { BatchInsertResult } from './types/batch';

const COLUMNS_PER_ROW = 29;
const MAX_ROWS_PER_INSERT = Math.floor(999 / COLUMNS_PER_ROW);

function isConstraintError(error: Error): boolean {
  const code = (error as { code?: string }).code ?? '';
  return code.startsWith('SQLITE_CONSTRAINT') || /constraint failed/i.test(error.message);
}

/** Multi-row SQLite insert manager with per-row failure isolation. */
export class BatchInsertManager {
  private readonly db: Database;
  private readonly cache = new Map<number, ReturnType<Database['prepare']>>();

  constructor(db: Database) {
    this.db = db;
  }

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
    } catch (error) {
      const batchError = error instanceof Error ? error : new Error(String(error));
      return this.insertRowByRow(jobs, now, batchError);
    }
  }

  private insertRowByRow(jobs: Job[], now: number, batchError: Error): BatchInsertResult {
    const transient: Job[] = [];
    const conflicts: Job[] = [];
    for (const job of jobs) {
      try {
        this.insertJobsChunk([job], now);
      } catch (error) {
        const rowError = error instanceof Error ? error : new Error(String(error));
        if (isConstraintError(rowError)) conflicts.push(job);
        else transient.push(job);
      }
    }
    return { transient, conflicts, error: batchError };
  }

  private getBatchInsertStmt(size: number): ReturnType<Database['prepare']> {
    let statement = this.cache.get(size);
    if (!statement) {
      const rowPlaceholder =
        '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
      const placeholders = Array(size).fill(rowPlaceholder).join(', ');
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
        timeline=excluded.timeline, dlq_retry_state=excluded.dlq_retry_state,
        started_at=excluded.started_at, completed_at=excluded.completed_at,
        progress=excluded.progress, progress_msg=excluded.progress_msg,
        last_heartbeat=excluded.last_heartbeat, stacktrace=excluded.stacktrace`;
      statement = this.db.prepare(sql);
      if (size <= 100) this.cache.set(size, statement);
    }
    return statement;
  }

  private insertJobsChunk(jobs: Job[], now: number): void {
    const statement = this.getBatchInsertStmt(jobs.length);
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
    statement.run(...(values as (string | number | bigint | null | Uint8Array)[]));
  }
}
