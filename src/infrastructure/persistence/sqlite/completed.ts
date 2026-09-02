import type { Job, JobId } from '../../../domain/types/job';
import { rowToJob } from '../sqliteSerializer';
import type { DbJob } from '../statements';
import { SqliteRecords } from './records';

interface CompletedCandidate {
  id: string;
  queue: string;
  retained_at: number;
}

export interface CompletedJobRemoval {
  jobId: JobId;
  queue: string;
}

const SCAN_BATCH_SIZE = 500;
const FIRST_TIMESTAMP = Number.MIN_SAFE_INTEGER;

/** SQLite-authoritative cleanup and exact retained-completion counts. */
export abstract class SqliteCompleted extends SqliteRecords {
  getCompletedJob(jobId: JobId): Job | null {
    const row = this.db
      .query<DbJob, [string]>("SELECT * FROM jobs WHERE id = ? AND state = 'completed'")
      .get(String(jobId));
    return row ? rowToJob(row) : null;
  }

  cleanCompletedJobs(
    queue: string | null,
    completedBefore: number,
    limit: number,
    isProtected: (jobId: JobId) => boolean = () => false
  ): CompletedJobRemoval[] {
    const normalizedLimit = Number.isFinite(limit) ? Math.max(0, Math.trunc(limit)) : 0;
    if (normalizedLimit === 0 || !Number.isFinite(completedBefore)) return [];

    let removed: CompletedJobRemoval[] = [];
    this.safeWrite(() => {
      removed = this.db.transaction(() => {
        const candidates = this.selectCompletedCandidates(
          queue,
          completedBefore,
          normalizedLimit,
          isProtected
        );
        const deleteJob = this.db.prepare(
          `DELETE FROM jobs
           WHERE id = ? AND state = 'completed'
             AND COALESCE(completed_at, created_at) <= ?`
        );
        const deleteResult = this.statements.get('deleteJobResult')!;
        const deleteFlowFailures = this.db.prepare(
          'DELETE FROM flow_failures WHERE parent_id = ? OR child_id = ?'
        );
        const hasDlqGeneration = this.db.prepare(
          'SELECT 1 AS present FROM dlq WHERE job_id = ? LIMIT 1'
        );
        const deleted: CompletedJobRemoval[] = [];
        for (const candidate of candidates) {
          const jobId = candidate.id as JobId;
          const result = deleteJob.run(jobId, completedBefore) as { changes: number };
          if (result.changes === 0) continue;
          if (!hasDlqGeneration.get(jobId)) {
            deleteResult.run(jobId);
            deleteFlowFailures.run(jobId, jobId);
          }
          deleted.push({ jobId, queue: candidate.queue });
        }
        return deleted;
      })();
    });
    return removed;
  }

  countCompletedJobs(): number {
    return (
      this.db
        .query<{ count: number }, []>(
          'SELECT COALESCE(SUM(count), 0) AS count FROM completed_job_counts'
        )
        .get()?.count ?? 0
    );
  }

  countCompletedJobsByQueue(queueNames: Iterable<string>): Map<string, number> {
    const requested = Array.from(new Set(queueNames));
    const counts = new Map<string, number>();
    for (let offset = 0; offset < requested.length; offset += SCAN_BATCH_SIZE) {
      const chunk = requested.slice(offset, offset + SCAN_BATCH_SIZE);
      const placeholders = chunk.map(() => '?').join(', ');
      const rows = this.db
        .query<{ queue: string; count: number }, string[]>(
          `SELECT queue, count FROM completed_job_counts WHERE queue IN (${placeholders})`
        )
        .all(...chunk);
      for (const row of rows) counts.set(row.queue, row.count);
    }
    return counts;
  }

  loadCompletedQueueNames(): string[] {
    return this.db
      .query<{ queue: string }, []>('SELECT queue FROM completed_job_counts ORDER BY queue')
      .all()
      .map((row) => row.queue);
  }

  loadCompletedJobsForRetry(
    queue: string,
    completedBefore: number,
    cursorTimestamp: number,
    cursorId: string,
    limit: number
  ): Job[] {
    const normalizedLimit = Number.isFinite(limit) ? Math.max(0, Math.trunc(limit)) : 0;
    if (normalizedLimit === 0 || !Number.isFinite(completedBefore)) return [];
    const rows = this.db
      .query<DbJob & { retained_at: number }, (string | number)[]>(
        `SELECT *, COALESCE(completed_at, created_at) AS retained_at
         FROM jobs INDEXED BY idx_jobs_completed_retention
         WHERE state = 'completed' AND queue = ?
           AND COALESCE(completed_at, created_at) <= ?
           AND (COALESCE(completed_at, created_at), id) > (?, ?)
         ORDER BY COALESCE(completed_at, created_at), id
         LIMIT ?`
      )
      .all(queue, completedBefore, cursorTimestamp, cursorId, normalizedLimit);
    return rows.map((row) => rowToJob(row));
  }

  private selectCompletedCandidates(
    queue: string | null,
    completedBefore: number,
    limit: number,
    isProtected: (jobId: JobId) => boolean
  ): CompletedCandidate[] {
    const selected: CompletedCandidate[] = [];
    let cursorTimestamp = FIRST_TIMESTAMP;
    let cursorId = '';

    while (selected.length < limit) {
      const rows = this.completedCandidatePage(queue, completedBefore, cursorTimestamp, cursorId);
      if (rows.length === 0) break;
      for (const row of rows) {
        cursorTimestamp = row.retained_at;
        cursorId = row.id;
        const jobId = row.id as JobId;
        if (!isProtected(jobId)) selected.push(row);
        if (selected.length === limit) break;
      }
      if (rows.length < SCAN_BATCH_SIZE) break;
    }
    return selected;
  }

  private completedCandidatePage(
    queue: string | null,
    completedBefore: number,
    cursorTimestamp: number,
    cursorId: string
  ): CompletedCandidate[] {
    const queuePredicate = queue === null ? '' : 'AND queue = ?';
    const indexName =
      queue === null ? 'idx_jobs_completed_retention_global' : 'idx_jobs_completed_retention';
    const parameters =
      queue === null
        ? [completedBefore, cursorTimestamp, cursorId, SCAN_BATCH_SIZE]
        : [completedBefore, queue, cursorTimestamp, cursorId, SCAN_BATCH_SIZE];
    return this.db
      .query<CompletedCandidate, (string | number)[]>(
        `SELECT id, queue, COALESCE(completed_at, created_at) AS retained_at
         FROM jobs INDEXED BY ${indexName}
         WHERE state = 'completed'
           AND COALESCE(completed_at, created_at) <= ?
           ${queuePredicate}
           AND (COALESCE(completed_at, created_at), id) > (?, ?)
         ORDER BY COALESCE(completed_at, created_at), id
         LIMIT ?`
      )
      .all(...parameters);
  }
}
