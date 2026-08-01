import { getDlqRetryState } from '../../../domain/types/dlq';
import type { Job, JobId } from '../../../domain/types/job';
import { pack, persistedStallCount } from '../sqliteSerializer';
import { SqliteJobs } from './jobs';

/** Non-terminal job field and scheduling mutations. */
export abstract class SqliteMutations extends SqliteJobs {
  updateForRetry(job: Job): void {
    const dlqRetryState = getDlqRetryState(job);
    this.safeWrite(() => {
      this.db
        .prepare(
          'UPDATE jobs SET attempts = ?, stall_count = ?, run_at = ?, state = ?, timeline = ?, stacktrace = ?, dlq_retry_state = ? WHERE id = ?'
        )
        .run(
          job.attempts,
          persistedStallCount(job),
          job.runAt,
          'waiting',
          job.timeline.length > 0 ? pack(job.timeline) : null,
          job.stacktrace && job.stacktrace.length > 0 ? pack(job.stacktrace) : null,
          dlqRetryState ? pack(dlqRetryState) : null,
          job.id
        );
    });
  }

  deleteJob(jobId: JobId): void {
    this.writeBuffer.removePending(jobId);
    this.safeWrite(() => {
      const deleteStatement = this.statements.get('deleteJob')!;
      const deleteResultStatement = this.statements.get('deleteJobResult')!;
      const transaction = this.db.transaction((id: JobId) => {
        deleteStatement.run(id);
        deleteResultStatement.run(id);
        this.db.prepare('DELETE FROM flow_failures WHERE parent_id = ?').run(id);
      });
      transaction(jobId);
    });
  }

  updateJobData(jobId: JobId, data: unknown): void {
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      this.db.prepare('UPDATE jobs SET data = ? WHERE id = ?').run(pack(data), jobId);
    });
  }

  clearJobUniqueKey(jobId: JobId): void {
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      this.db.prepare('UPDATE jobs SET unique_key = NULL WHERE id = ?').run(jobId);
    });
  }

  updateJobPriority(jobId: JobId, priority: number, lifo: boolean): void {
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      this.db
        .prepare('UPDATE jobs SET priority = ?, lifo = ? WHERE id = ?')
        .run(priority, lifo ? 1 : 0, jobId);
    });
  }

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

  updateRunAt(jobId: JobId, runAt: number): void {
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      const state = runAt > Date.now() ? 'delayed' : 'waiting';
      this.db
        .prepare('UPDATE jobs SET run_at = ?, state = ?, started_at = NULL WHERE id = ?')
        .run(runAt, state, jobId);
    });
  }

  updateJobChildrenIds(jobId: JobId, childrenIds: JobId[]): void {
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      this.db
        .prepare('UPDATE jobs SET children_ids = ? WHERE id = ?')
        .run(childrenIds.length > 0 ? pack(childrenIds) : null, jobId);
    });
  }
}
