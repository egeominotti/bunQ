import { getDlqRetryState } from '../../../domain/types/dlq';
import {
  assertWellFormedJobId,
  type Job,
  type JobId,
  type JobTimelineEntry,
} from '../../../domain/types/job';
import { encodeJobOptions } from '../jobOptionsBlob';
import { pack, persistedJobStateForWrite, persistedStallCount } from '../sqliteSerializer';
import type { DurableAdmissionMetadata } from '../types/admission';
import type { ActiveJobWrite, CompletedJobWrite } from '../types/sqlite';
import { SqliteAdmission } from './admission';

/** Buffered admission and lifecycle state persistence. */
export abstract class SqliteJobLifecycle extends SqliteAdmission {
  insertJob(job: Job, durable?: boolean, admission?: DurableAdmissionMetadata): void {
    assertWellFormedJobId(job.id);
    if (durable) {
      this.insertJobImmediate(job, admission);
      return;
    }
    this.commitBufferedAdmissionMetadata(admission);
    this.writeBuffer.add(job);
  }

  insertJobImmediate(job: Job, admission?: DurableAdmissionMetadata): void {
    if (!this.hasAdmissionMetadata(admission)) {
      this.safeWrite(() => this.runInsertJobStmt(job));
      return;
    }
    this.safeWrite(() => {
      const transaction = this.db.transaction(() => {
        this.runAdmissionMetadata(admission);
        this.runInsertJobStmt(job);
      });
      transaction();
    });
    this.finalizeAdmissionMetadata(admission);
  }

  protected runInsertJobStmt(job: Job): void {
    assertWellFormedJobId(job.id);
    const dlqRetryState = getDlqRetryState(job);
    const state = persistedJobStateForWrite(job);
    this.statements
      .get('insertJob')!
      .run(
        job.id,
        job.queue,
        job.name,
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
        state,
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
        job.timeline.length > 0 ? pack(job.timeline) : null,
        dlqRetryState ? pack(dlqRetryState) : null,
        encodeJobOptions(job),
        job.startedAt,
        job.completedAt,
        state === 'completed' ? 100 : (job.progress ?? 0),
        job.progressMessage ?? null,
        job.lastHeartbeat ?? job.createdAt,
        job.stacktrace ? pack(job.stacktrace) : null
      );
  }

  protected flushIfBuffered(jobId: JobId): void {
    if (!this.writeBuffer.hasPending(String(jobId))) return;
    try {
      this.writeBuffer.flushIfReady();
    } catch {
      // Flush errors are already reported by the buffer callbacks.
    }
  }

  private flushPendingBatch(): void {
    try {
      this.writeBuffer.flushIfReady();
    } catch {
      // Flush errors are already reported by the buffer callbacks.
    }
  }

  markActive(jobId: JobId, startedAt: number, timeline?: JobTimelineEntry[]): void {
    this.writeBuffer.setPendingState(jobId, 'active');
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      this.statements
        .get('updateJobState')!
        .run('active', startedAt, timeline && timeline.length > 0 ? pack(timeline) : null, jobId);
    });
  }

  markActiveBatch(updates: readonly ActiveJobWrite[]): void {
    if (updates.length === 0) return;
    let hasPending = false;
    for (const update of updates) {
      hasPending = this.writeBuffer.setPendingState(update.jobId, 'active') || hasPending;
    }
    if (hasPending) this.flushPendingBatch();
    const rows = updates.map((update) => ({
      ...update,
      timeline: update.timeline?.length ? pack(update.timeline) : null,
    }));
    this.safeWrite(() => {
      this.db.transaction((batch: typeof rows) => {
        const statement = this.statements.get('updateJobState')!;
        for (const row of batch) statement.run('active', row.startedAt, row.timeline, row.jobId);
      })(rows);
    });
  }

  markWaitingChildren(jobId: JobId, timeline?: JobTimelineEntry[]): void {
    this.writeBuffer.setPendingState(jobId, 'waiting-children');
    this.flushIfBuffered(jobId);
    this.safeWrite(() => {
      this.db
        .prepare('UPDATE jobs SET state = ?, started_at = NULL, timeline = ? WHERE id = ?')
        .run('waiting-children', timeline && timeline.length > 0 ? pack(timeline) : null, jobId);
    });
  }

  markCompleted(jobId: JobId, completedAt: number, timeline?: JobTimelineEntry[]): void {
    this.writeBuffer.setPendingState(jobId, 'completed');
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

  markCompletedBatch(updates: readonly CompletedJobWrite[]): void {
    if (updates.length === 0) return;
    let hasPending = false;
    for (const update of updates) {
      hasPending = this.writeBuffer.setPendingState(update.jobId, 'completed') || hasPending;
    }
    if (hasPending) this.flushPendingBatch();
    const rows = updates.map((update) => ({
      ...update,
      timeline: update.timeline?.length ? pack(update.timeline) : null,
    }));
    this.safeWrite(() => {
      this.db.transaction((batch: typeof rows) => {
        const statement = this.statements.get('completeJob')!;
        for (const row of batch)
          statement.run('completed', row.completedAt, row.timeline, row.jobId);
      })(rows);
    });
  }
}
