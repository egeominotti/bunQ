import { getDlqRetryState } from '../../../domain/types/dlq';
import type { Job, JobId } from '../../../domain/types/job';
import type { DependencyCompletionRecord } from '../dependencyCompletionStore';
import type { DurableAdmissionMetadata } from '../types/admission';
import { pack, persistedStallCount } from '../sqliteSerializer';
import { SqliteJobs } from './jobs';
import {
  DELETE_QUEUE_FLOW_FAILURES_SQL,
  DELETE_QUEUE_JOB_RESULTS_FROM_DLQ_SQL,
  DELETE_QUEUE_JOB_RESULTS_FROM_JOBS_SQL,
} from './queueDeletionSql';

export interface QueueDeletionResult {
  bufferedJobIds: JobId[];
  dependencyCompletions: DependencyCompletionRecord[];
}

/** Non-terminal job field and scheduling mutations. */
export abstract class SqliteMutations extends SqliteJobs {
  updateForRetry(job: Job): void {
    this.writeBuffer.setPendingState(job.id, job.runAt > Date.now() ? 'delayed' : 'waiting');
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

  /** Atomically move a completed job back to waiting and retire its old result. */
  requeueCompletedJob(job: Job): void {
    this.flushIfBuffered(job.id);
    const dlqRetryState = getDlqRetryState(job);
    this.safeWrite(() => {
      const transaction = this.db.transaction(() => {
        const result = this.db
          .prepare(
            `UPDATE jobs SET
               attempts = ?, stall_count = ?, run_at = ?, state = 'waiting',
               started_at = NULL, completed_at = NULL, progress = ?,
               progress_msg = ?, last_heartbeat = ?, timeline = ?,
               dlq_retry_state = ?
             WHERE id = ? AND state = 'completed'`
          )
          .run(
            job.attempts,
            persistedStallCount(job),
            job.runAt,
            job.progress,
            job.progressMessage,
            job.lastHeartbeat,
            job.timeline.length > 0 ? pack(job.timeline) : null,
            dlqRetryState ? pack(dlqRetryState) : null,
            job.id
          );
        if (result.changes === 0) {
          throw new Error(`Completed job is not persisted: ${String(job.id)}`);
        }
        this.statements.get('deleteJobResult')!.run(job.id);
      });
      transaction();
    });
  }

  deleteJob(jobId: JobId): void {
    this.writeBuffer.removePending(jobId);
    this.safeWrite(() => {
      const transaction = this.db.transaction((id: JobId) => {
        this.runDeleteJobRows(id);
      });
      transaction(jobId);
    });
  }

  /** Commit every durable queue-owned deletion before removing buffered jobs. */
  deleteJobsForQueue(
    queue: string,
    referencedDependencyIds: ReadonlySet<JobId>,
    dependencyRetentionLimit: number
  ): QueueDeletionResult {
    let dependencyCompletions: DependencyCompletionRecord[] = [];
    this.safeWrite(() => {
      this.db.transaction(() => {
        this.db.prepare(DELETE_QUEUE_JOB_RESULTS_FROM_JOBS_SQL).run(queue, queue);
        this.db.prepare(DELETE_QUEUE_JOB_RESULTS_FROM_DLQ_SQL).run(queue, queue, queue);
        this.db
          .prepare(DELETE_QUEUE_FLOW_FAILURES_SQL)
          .run(queue, queue, queue, queue, queue, queue, queue, queue);
        this.db.prepare('DELETE FROM jobs WHERE queue = ?').run(queue);
        this.db.prepare('DELETE FROM dlq WHERE queue = ?').run(queue);
        this.db.prepare('DELETE FROM dependency_completions WHERE queue = ?').run(queue);
        dependencyCompletions = this.dependencyCompletionStore.reconcilePinsInTransaction(
          referencedDependencyIds,
          dependencyRetentionLimit
        );
        this.telemetryStore.deleteQueueTelemetryRows(queue);
        this.db.prepare('DELETE FROM queue_state WHERE name = ?').run(queue);
        this.db.prepare('DELETE FROM group_state WHERE queue = ?').run(queue);
      })();
    });
    this.telemetryStore.markQueueTelemetryCleared(queue);
    return {
      bufferedJobIds: this.writeBuffer.removePendingForQueue(queue),
      dependencyCompletions,
    };
  }

  replaceJob(
    oldJobId: JobId,
    newJob: Job,
    durable?: boolean,
    admission?: DurableAdmissionMetadata
  ): void {
    if (!durable) {
      this.safeWrite(() => {
        const transaction = this.db.transaction((id: JobId) => {
          this.runDeleteJobRows(id);
          this.runAdmissionMetadata(admission);
        });
        transaction(oldJobId);
      });
      this.writeBuffer.removePending(oldJobId);
      this.finalizeAdmissionMetadata(admission);
      this.writeBuffer.add(newJob);
      return;
    }

    this.safeWrite(() => {
      const transaction = this.db.transaction(() => {
        this.runDeleteJobRows(oldJobId);
        this.runAdmissionMetadata(admission);
        this.runInsertJobStmt(newJob);
      });
      transaction();
    });
    this.writeBuffer.removePending(oldJobId);
    this.finalizeAdmissionMetadata(admission);
  }

  transferActiveDedupJob(
    oldJobId: JobId,
    newJob: Job,
    durable?: boolean,
    admission?: DurableAdmissionMetadata
  ): void {
    if (!durable) {
      this.safeWrite(() => {
        const transaction = this.db.transaction(() => {
          this.runClearActiveDedupKey(oldJobId);
          this.runAdmissionMetadata(admission);
        });
        transaction();
      });
      this.finalizeAdmissionMetadata(admission);
      this.writeBuffer.add(newJob);
      return;
    }

    this.safeWrite(() => {
      const transaction = this.db.transaction(() => {
        this.runClearActiveDedupKey(oldJobId);
        this.runAdmissionMetadata(admission);
        this.runInsertJobStmt(newJob);
      });
      transaction();
    });
    this.finalizeAdmissionMetadata(admission);
  }

  protected runDeleteJobRows(jobId: JobId): void {
    this.statements.get('deleteJob')!.run(jobId);
    this.statements.get('deleteJobResult')!.run(jobId);
    this.db.prepare('DELETE FROM flow_failures WHERE parent_id = ?').run(jobId);
  }

  protected runClearActiveDedupKey(jobId: JobId): void {
    const result = this.db
      .prepare("UPDATE jobs SET unique_key = NULL WHERE id = ? AND state = 'active'")
      .run(jobId);
    if (result.changes === 0) {
      throw new Error(`Active deduplication owner is not persisted: ${String(jobId)}`);
    }
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
    const state = runAt > Date.now() ? 'delayed' : 'waiting';
    this.writeBuffer.setPendingState(jobId, state);
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
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
