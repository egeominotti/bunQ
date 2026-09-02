import type { Job } from '../../../domain/types/job';
import { rowToJob } from '../sqliteSerializer';
import type { DbJob } from '../statements';
import { SqliteCompleted } from './completed';

/** Persisted and logical-state job queries used by recovery and public pagination. */
export abstract class SqliteQueries extends SqliteCompleted {
  queryJobs(
    queue: string,
    options: { state?: string; states?: string[]; limit: number; offset: number; asc: boolean }
  ): Job[] {
    const order = options.asc ? 'ASC' : 'DESC';
    let rows: DbJob[];

    if (options.states && options.states.length > 0) {
      const placeholders = options.states.map(() => '?').join(',');
      rows = this.db
        .query<DbJob, (string | number)[]>(
          `SELECT * FROM jobs WHERE queue = ? AND state IN (${placeholders}) ORDER BY created_at ${order}, id ${order} LIMIT ? OFFSET ?`
        )
        .all(queue, ...options.states, options.limit, options.offset);
    } else if (options.state) {
      rows = this.db
        .query<DbJob, [string, string, number, number]>(
          `SELECT * FROM jobs WHERE queue = ? AND state = ? ORDER BY created_at ${order}, id ${order} LIMIT ? OFFSET ?`
        )
        .all(queue, options.state, options.limit, options.offset);
    } else {
      rows = this.db
        .query<DbJob, [string, number, number]>(
          `SELECT * FROM jobs WHERE queue = ? ORDER BY created_at ${order}, id ${order} LIMIT ? OFFSET ?`
        )
        .all(queue, options.limit, options.offset);
    }

    return rows.map((row) => rowToJob(row));
  }

  queryJobsByLogicalStates(
    queue: string,
    states: string[],
    options: { limit: number; offset: number; asc: boolean; now: number }
  ): Job[] {
    const uniqueStates = Array.from(new Set(states));
    if (uniqueStates.length === 0) return [];

    const predicates: string[] = [];
    const predicateParams: (string | number)[] = [];
    const requested = new Set(uniqueStates);

    if (requested.has('waiting')) {
      predicates.push(
        "(state IN ('waiting', 'prioritized', 'delayed') AND run_at <= ? AND priority <= 0)"
      );
      predicateParams.push(options.now);
    }
    if (requested.has('prioritized')) {
      predicates.push(
        "(state IN ('waiting', 'prioritized', 'delayed') AND run_at <= ? AND priority > 0)"
      );
      predicateParams.push(options.now);
    }
    if (requested.has('delayed')) {
      predicates.push("(state IN ('waiting', 'prioritized', 'delayed') AND run_at > ?)");
      predicateParams.push(options.now);
    }

    const persistedStates = uniqueStates.filter(
      (state) => state !== 'waiting' && state !== 'prioritized' && state !== 'delayed'
    );
    if (persistedStates.length > 0) {
      predicates.push(`state IN (${persistedStates.map(() => '?').join(',')})`);
      predicateParams.push(...persistedStates);
    }

    const order = options.asc ? 'ASC' : 'DESC';
    const rows = this.db
      .query<DbJob, (string | number)[]>(
        `SELECT * FROM jobs INDEXED BY idx_jobs_queue_created
         WHERE queue = ? AND (${predicates.join(' OR ')})
         ORDER BY created_at ${order}, id ${order}
         LIMIT ? OFFSET ?`
      )
      .all(queue, ...predicateParams, options.limit, options.offset);

    return rows.map((row) => rowToJob(row));
  }

  loadPendingJobs(limit: number = 10000, offset: number = 0): Job[] {
    const rows = this.db
      .query<DbJob, [number, number]>(
        "SELECT * FROM jobs WHERE state IN ('waiting', 'prioritized', 'waiting-children', 'delayed') ORDER BY priority DESC, run_at ASC, id ASC LIMIT ? OFFSET ?"
      )
      .all(limit, offset);
    return rows.map((row) => rowToJob(row));
  }

  loadActiveJobs(limit: number = 10000, offset: number = 0): Job[] {
    const rows = this.db
      .query<DbJob, [number, number]>(
        "SELECT * FROM jobs WHERE state = 'active' ORDER BY started_at ASC LIMIT ? OFFSET ?"
      )
      .all(limit, offset);
    return rows.map((row) => rowToJob(row));
  }

  loadCompletedJobs(limit: number = 10000, offset: number = 0): Job[] {
    const rows = this.db
      .query<DbJob, [number, number]>(
        "SELECT * FROM jobs WHERE state = 'completed' ORDER BY completed_at DESC, id DESC LIMIT ? OFFSET ?"
      )
      .all(limit, offset);
    return rows.map((row) => rowToJob(row));
  }

  countPendingJobs(): number {
    const result = this.db
      .query<{ count: number }, []>(
        "SELECT COUNT(*) as count FROM jobs WHERE state IN ('waiting', 'prioritized', 'waiting-children', 'delayed')"
      )
      .get();
    return result?.count ?? 0;
  }

  countActiveJobs(): number {
    const result = this.db
      .query<{ count: number }, []>("SELECT COUNT(*) as count FROM jobs WHERE state = 'active'")
      .get();
    return result?.count ?? 0;
  }
}
