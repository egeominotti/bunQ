import type { DlqEntry } from '../../../domain/types/dlq';
import type { FlowFailureMode, FlowFailureRecord } from '../../../domain/types/flow';
import type { Job, JobId } from '../../../domain/types/job';
import { pack, unpack } from '../sqliteSerializer';
import { SqliteMutations } from './mutations';

/** Atomic persistence for parent/child relationships and failure outbox rows. */
export abstract class SqliteFlows extends SqliteMutations {
  updateFlowLink(
    child: Pick<Job, 'id' | 'parentId' | 'data'>,
    parent: Pick<Job, 'id' | 'childrenIds' | 'dependsOn' | 'data'>,
    parentState: 'waiting-children' | 'waiting' | 'prioritized' | 'delayed'
  ): void {
    this.flushIfBuffered(child.id);
    this.flushIfBuffered(parent.id);
    this.safeWrite(() => {
      const transaction = this.db.transaction(() => {
        this.db
          .prepare('UPDATE jobs SET parent_id = ?, data = ? WHERE id = ?')
          .run(child.parentId, pack(child.data), child.id);
        this.db
          .prepare(
            'UPDATE jobs SET children_ids = ?, depends_on = ?, data = ?, state = ? WHERE id = ?'
          )
          .run(
            parent.childrenIds.length > 0 ? pack(parent.childrenIds) : null,
            parent.dependsOn.length > 0 ? pack(parent.dependsOn) : null,
            pack(parent.data),
            parentState,
            parent.id
          );
      });
      transaction();
    });
  }

  backpatchFlowChild(
    child: Pick<Job, 'id' | 'parentId' | 'data'>,
    previousParentId: JobId | null
  ): void {
    this.flushIfBuffered(child.id);
    this.safeWrite(() => {
      const transaction = this.db.transaction(() => {
        let persisted =
          this.db
            .prepare('UPDATE jobs SET parent_id = ?, data = ? WHERE id = ?')
            .run(child.parentId, pack(child.data), child.id).changes > 0;

        const dlqRows = this.db
          .query<{ id: number; entry: Uint8Array }, [string]>(
            'SELECT id, entry FROM dlq WHERE job_id = ?'
          )
          .all(String(child.id));
        for (const row of dlqRows) {
          const entry = unpack<DlqEntry | null>(
            row.entry,
            null,
            `backpatchFlowChild:${String(child.id)}`
          );
          if (!entry?.job) continue;
          const updated = {
            ...entry,
            job: { ...entry.job, parentId: child.parentId, data: child.data },
          };
          this.db.prepare('UPDATE dlq SET entry = ? WHERE id = ?').run(pack(updated), row.id);
          persisted = true;
        }

        if (!persisted) {
          throw new Error(`Flow child is no longer persisted: ${String(child.id)}`);
        }

        if (previousParentId && child.parentId && previousParentId !== child.parentId) {
          const failure = this.db
            .query<
              { child_queue: string; mode: string; error: string; created_at: number },
              [string, string]
            >(
              `SELECT child_queue, mode, error, created_at
               FROM flow_failures
               WHERE parent_id = ? AND child_id = ?`
            )
            .get(String(previousParentId), String(child.id));
          if (failure) {
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
                child.parentId,
                child.id,
                failure.child_queue,
                failure.mode,
                failure.error,
                failure.created_at
              );
            this.db
              .prepare('DELETE FROM flow_failures WHERE parent_id = ? AND child_id = ?')
              .run(previousParentId, child.id);
          }
        }
      });
      transaction();
    });
  }

  removeFlowLink(
    child: Pick<Job, 'id' | 'parentId' | 'data'>,
    parent: Pick<Job, 'id' | 'childrenIds' | 'dependsOn' | 'data' | 'runAt'>,
    parentState: 'waiting-children' | 'waiting' | 'prioritized'
  ): void {
    this.flushIfBuffered(child.id);
    this.flushIfBuffered(parent.id);
    this.safeWrite(() => {
      const transaction = this.db.transaction(() => {
        this.db
          .prepare('UPDATE jobs SET parent_id = ?, data = ? WHERE id = ?')
          .run(child.parentId, pack(child.data), child.id);
        this.db
          .prepare(
            `UPDATE jobs
             SET children_ids = ?, depends_on = ?, data = ?, run_at = ?, state = ?
             WHERE id = ?`
          )
          .run(
            parent.childrenIds.length > 0 ? pack(parent.childrenIds) : null,
            parent.dependsOn.length > 0 ? pack(parent.dependsOn) : null,
            pack(parent.data),
            parent.runAt,
            parentState,
            parent.id
          );
        this.db
          .prepare('DELETE FROM flow_failures WHERE parent_id = ? AND child_id = ?')
          .run(parent.id, child.id);
      });
      transaction();
    });
  }

  saveFlowFailure(record: FlowFailureRecord): void {
    this.safeWrite(() => {
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
          record.parentId,
          record.childId,
          record.childQueue,
          record.mode,
          record.error,
          record.createdAt
        );
    });
  }

  loadFlowFailures(): FlowFailureRecord[] {
    const rows = this.db
      .query<
        {
          parent_id: string;
          child_id: string;
          child_queue: string;
          mode: string;
          error: string;
          created_at: number;
        },
        []
      >('SELECT * FROM flow_failures ORDER BY created_at, parent_id, child_id')
      .all();
    return rows.map((row) => ({
      parentId: row.parent_id as JobId,
      childId: row.child_id as JobId,
      childQueue: row.child_queue,
      mode: row.mode as FlowFailureMode,
      error: row.error,
      createdAt: row.created_at,
    }));
  }

  deleteFlowFailure(parentId: JobId, childId?: JobId): void {
    this.safeWrite(() => {
      if (childId) {
        this.db
          .prepare('DELETE FROM flow_failures WHERE parent_id = ? AND child_id = ?')
          .run(parentId, childId);
      } else {
        this.db.prepare('DELETE FROM flow_failures WHERE parent_id = ?').run(parentId);
      }
    });
  }

  updateFlowParentResolution(
    job: Pick<Job, 'id' | 'dependsOn' | 'runAt' | 'priority' | 'timeline'>,
    stateOverride?: 'waiting-children' | 'waiting' | 'prioritized' | 'delayed'
  ): void {
    this.flushIfBuffered(job.id);
    const state =
      stateOverride ??
      (job.dependsOn.length > 0
        ? 'waiting-children'
        : job.runAt > Date.now()
          ? 'delayed'
          : job.priority > 0
            ? 'prioritized'
            : 'waiting');
    this.safeWrite(() => {
      this.db
        .prepare('UPDATE jobs SET depends_on = ?, run_at = ?, state = ?, timeline = ? WHERE id = ?')
        .run(
          job.dependsOn.length > 0 ? pack(job.dependsOn) : null,
          job.runAt,
          state,
          job.timeline.length > 0 ? pack(job.timeline) : null,
          job.id
        );
    });
  }
}
