import type { DlqEntry } from '../../../domain/types/dlq';
import { assertWellFormedJobId, type Job, type JobId } from '../../../domain/types/job';
import type { DependencyCompletionRecord } from '../dependencyCompletionStore';
import { pack, reconstructDlqEntry, rowToJob, unpack } from '../sqliteSerializer';
import type { DbJob } from '../statements';
import { SqliteFlows } from './flows';

/** Direct job/result lookup and payload-free dependency completion records. */
export abstract class SqliteRecords extends SqliteFlows {
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

  hasResult(jobId: JobId): boolean {
    const row = this.db
      .query<{ job_id: string }, [string]>('SELECT job_id FROM job_results WHERE job_id = ?')
      .get(String(jobId));
    return row !== null;
  }

  hasDlqEntry(jobId: JobId): boolean {
    const row = this.db
      .query<{ job_id: string }, [string]>('SELECT job_id FROM dlq WHERE job_id = ? LIMIT 1')
      .get(String(jobId));
    return row !== null;
  }

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

  loadDlqJobIds(): Set<JobId> {
    const rows = this.db.query<{ job_id: string }, []>('SELECT job_id FROM dlq').all();
    return new Set(rows.map((row) => row.job_id as JobId));
  }

  getJobStateRaw(jobId: JobId): string | null {
    const row = this.db
      .query<{ state: string }, [string]>('SELECT state FROM jobs WHERE id = ?')
      .get(String(jobId));
    return row?.state ?? null;
  }

  loadCompletedJobIds(requested: Iterable<JobId>): Set<JobId> {
    const requestedIds = [...new Set(requested)];
    const ids = new Set<JobId>();
    for (let offset = 0; offset < requestedIds.length; offset += 500) {
      const chunk = requestedIds.slice(offset, offset + 500);
      const placeholders = chunk.map(() => '?').join(',');
      const rows = this.db
        .query<{ id: string }, string[]>(
          `SELECT id FROM jobs WHERE state = 'completed' AND id IN (${placeholders})`
        )
        .all(...chunk);
      for (const row of rows) ids.add(row.id as JobId);
    }
    return ids;
  }

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

  insertJobsBatch(jobs: Job[], durable?: boolean): void {
    for (const job of jobs) assertWellFormedJobId(job.id);
    if (durable) {
      this.safeWrite(() => {
        const transaction = this.db.transaction((batch: Job[]) => {
          for (const job of batch) this.runInsertJobStmt(job);
        });
        transaction(jobs);
      });
      return;
    }
    this.writeBuffer.addBatch(jobs);
  }
}
