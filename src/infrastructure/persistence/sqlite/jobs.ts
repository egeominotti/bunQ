import type { DlqEntry } from '../../../domain/types/dlq';
import { getDlqRetryState } from '../../../domain/types/dlq';
import type { FlowFailureRecord } from '../../../domain/types/flow';
import type { Job, JobId, JobTimelineEntry } from '../../../domain/types/job';
import {
  pack,
  persistedInitialState,
  persistedStallCount,
  reconstructDlqEntry,
  unpack,
} from '../sqliteSerializer';
import { SqliteState } from './state';

/** Job insertion, state transitions, and DLQ persistence. */
export abstract class SqliteJobs extends SqliteState {
  insertJob(job: Job, durable?: boolean): void {
    if (durable) {
      this.insertJobImmediate(job);
      return;
    }
    this.writeBuffer.add(job);
  }

  insertJobImmediate(job: Job): void {
    this.safeWrite(() => {
      this.runInsertJobStmt(job);
    });
  }

  protected runInsertJobStmt(job: Job): void {
    const dlqRetryState = getDlqRetryState(job);
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
        job.timeline.length > 0 ? pack(job.timeline) : null,
        dlqRetryState ? pack(dlqRetryState) : null
      );
  }

  protected flushIfBuffered(jobId: JobId): void {
    if (this.writeBuffer.hasPending(String(jobId))) {
      try {
        this.writeBuffer.flush();
      } catch {
        // Flush errors are already reported by the buffer callbacks.
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

  saveDlqEntry(entry: DlqEntry): void {
    this.safeWrite(() => {
      this.statements
        .get('insertDlq')!
        .run(entry.job.id, entry.job.queue, pack(entry), entry.enteredAt);
    });
  }

  commitFailedJob(
    jobId: JobId,
    entry: DlqEntry | null,
    flowFailure: FlowFailureRecord | null
  ): void {
    this.writeBuffer.removePending(jobId);
    this.safeWrite(() => {
      const transaction = this.db.transaction(() => {
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
      transaction();
    });
  }

  deleteDlqEntry(jobId: JobId): void {
    this.safeWrite(() => {
      this.statements.get('deleteDlqEntry')!.run(jobId);
    });
  }

  requeueDlqJob(job: Job): void {
    this.writeBuffer.removePending(job.id);
    this.safeWrite(() => {
      const transaction = this.db.transaction(() => {
        this.statements.get('deleteDlqEntry')!.run(job.id);
        this.runInsertJobStmt(job);
      });
      transaction();
    });
  }

  clearDlqQueue(queue: string): void {
    this.safeWrite(() => {
      this.statements.get('clearDlqQueue')!.run(queue);
    });
  }

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
      const transaction = this.db.transaction(() => {
        if (clearQueue) clearDlq.run(queue);
        else for (const jobId of dlqJobIds) deleteDlq.run(jobId, queue);
        for (const jobId of terminalJobIds) {
          deleteJob.run(jobId);
          deleteResult.run(jobId);
        }
      });
      transaction();
    });
  }

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
}
