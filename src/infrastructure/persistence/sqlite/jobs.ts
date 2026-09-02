import type { DlqEntry } from '../../../domain/types/dlq';
import type { FlowFailureRecord } from '../../../domain/types/flow';
import type { Job, JobId } from '../../../domain/types/job';
import { pack, reconstructDlqEntry, unpack } from '../sqliteSerializer';
import { SqliteJobLifecycle } from './jobLifecycle';

/** DLQ persistence layered on buffered job lifecycle writes. */
export abstract class SqliteJobs extends SqliteJobLifecycle {
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
      const deleteFlowFailures = this.db.prepare('DELETE FROM flow_failures WHERE parent_id = ?');
      const transaction = this.db.transaction(() => {
        if (clearQueue) clearDlq.run(queue);
        else for (const jobId of dlqJobIds) deleteDlq.run(jobId, queue);
        for (const jobId of terminalJobIds) {
          deleteJob.run(jobId);
          deleteResult.run(jobId);
          deleteFlowFailures.run(jobId);
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
