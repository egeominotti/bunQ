import type { Database } from 'bun:sqlite';
import type { JobId } from '../../domain/types/job';

export interface DependencyCompletionRecord {
  jobId: JobId;
  queue: string;
  completedAt: number;
  pinned: boolean;
}

/**
 * SQLite operations for payload-free `removeOnComplete` completion evidence.
 * The owner supplies write-error handling and buffered-job coordination.
 */
export class DependencyCompletionStore {
  private readonly insert: ReturnType<Database['prepare']>;
  private readonly deleteJob: ReturnType<Database['prepare']>;
  private readonly deleteResult: ReturnType<Database['prepare']>;
  private readonly deleteFlowFailures: ReturnType<Database['prepare']>;
  private readonly pruneThrough: ReturnType<Database['prepare']>;
  private readonly maxSequence: ReturnType<Database['prepare']>;
  private readonly loadAll: ReturnType<Database['prepare']>;
  private readonly deleteOne: ReturnType<Database['prepare']>;
  private readonly pinOne: ReturnType<Database['prepare']>;
  private readonly unpinOne: ReturnType<Database['prepare']>;
  private readonly unpinAll: ReturnType<Database['prepare']>;
  private readonly loadQueue: ReturnType<Database['prepare']>;
  private readonly deleteQueue: ReturnType<Database['prepare']>;

  constructor(private readonly db: Database) {
    this.insert = db.prepare(
      `INSERT INTO dependency_completions (job_id, queue, completed_at, pinned)
       VALUES (?, ?, ?, ?)
       ON CONFLICT(job_id) DO UPDATE SET
         queue=excluded.queue,
         completed_at=excluded.completed_at,
         pinned=MAX(dependency_completions.pinned, excluded.pinned)
       RETURNING sequence`
    );
    this.deleteJob = db.prepare('DELETE FROM jobs WHERE id = ?');
    this.deleteResult = db.prepare('DELETE FROM job_results WHERE job_id = ?');
    this.deleteFlowFailures = db.prepare('DELETE FROM flow_failures WHERE parent_id = ?');
    this.pruneThrough = db.prepare(
      'DELETE FROM dependency_completions WHERE pinned = 0 AND sequence <= ?'
    );
    this.maxSequence = db.prepare(
      'SELECT COALESCE(MAX(sequence), 0) AS sequence FROM dependency_completions'
    );
    this.loadAll = db.prepare(
      'SELECT job_id, queue, completed_at, pinned FROM dependency_completions ORDER BY sequence'
    );
    this.deleteOne = db.prepare(
      'DELETE FROM dependency_completions WHERE job_id = ? AND pinned = 0'
    );
    this.pinOne = db.prepare('UPDATE dependency_completions SET pinned = 1 WHERE job_id = ?');
    this.unpinOne = db.prepare('UPDATE dependency_completions SET pinned = 0 WHERE job_id = ?');
    this.unpinAll = db.prepare('UPDATE dependency_completions SET pinned = 0');
    this.loadQueue = db.prepare(
      'SELECT job_id FROM dependency_completions WHERE queue = ? ORDER BY sequence'
    );
    this.deleteQueue = db.prepare('DELETE FROM dependency_completions WHERE queue = ?');
  }

  commit(record: DependencyCompletionRecord, retentionLimit: number): void {
    const limit = Math.max(1, Math.trunc(retentionLimit));
    this.db.transaction(() => {
      const inserted = this.insert.get(
        record.jobId,
        record.queue,
        record.completedAt,
        record.pinned ? 1 : 0
      ) as { sequence: number } | null;
      if (!inserted) throw new Error(`Failed to persist completion for ${String(record.jobId)}`);
      this.pruneThrough.run(inserted.sequence - limit);
      this.deleteJob.run(record.jobId);
      this.deleteResult.run(record.jobId);
      this.deleteFlowFailures.run(record.jobId);
    })();
  }

  load(): DependencyCompletionRecord[] {
    return this.loadRecords();
  }

  private loadRecords(): DependencyCompletionRecord[] {
    const rows = this.loadAll.all() as Array<{
      job_id: string;
      queue: string;
      completed_at: number;
      pinned: number;
    }>;
    return rows.map((row) => ({
      jobId: row.job_id as JobId,
      queue: row.queue,
      completedAt: row.completed_at,
      pinned: row.pinned === 1,
    }));
  }

  pin(jobIds: Iterable<JobId>): void {
    this.db.transaction(() => {
      for (const jobId of jobIds) this.pinOne.run(jobId);
    })();
  }

  unpin(jobIds: Iterable<JobId>, retentionLimit: number): DependencyCompletionRecord[] {
    return this.db.transaction(() => {
      for (const jobId of jobIds) this.unpinOne.run(jobId);
      this.prune(retentionLimit);
      return this.loadRecords();
    })();
  }

  reconcilePins(
    referenced: ReadonlySet<JobId>,
    retentionLimit: number
  ): DependencyCompletionRecord[] {
    return this.db.transaction(() => {
      this.unpinAll.run();
      for (const jobId of referenced) this.pinOne.run(jobId);
      this.prune(retentionLimit);
      return this.loadRecords();
    })();
  }

  private prune(retentionLimit: number): void {
    const limit = Math.max(1, Math.trunc(retentionLimit));
    const latest = this.maxSequence.get() as { sequence: number };
    this.pruneThrough.run(latest.sequence - limit);
  }

  delete(jobId: JobId): boolean {
    const result = this.deleteOne.run(jobId) as { changes: number };
    return result.changes > 0;
  }

  deleteForQueue(queue: string): JobId[] {
    return this.db.transaction(() => {
      const rows = this.loadQueue.all(queue) as Array<{ job_id: string }>;
      this.deleteQueue.run(queue);
      return rows.map((row) => row.job_id as JobId);
    })();
  }
}
