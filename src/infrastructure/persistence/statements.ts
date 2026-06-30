/**
 * Prepared SQLite statements
 * Pre-compiled SQL for better performance
 */

import type { Database } from 'bun:sqlite';

/** Statement names */
export type StatementName =
  | 'insertJob'
  | 'updateJobState'
  | 'completeJob'
  | 'deleteJob'
  | 'deleteJobResult'
  | 'getJob'
  | 'insertResult'
  | 'getResult'
  | 'insertDlq'
  | 'loadDlq'
  | 'deleteDlqEntry'
  | 'clearDlqQueue'
  | 'insertCron'
  | 'updateCron'
  | 'upsertQueueState'
  | 'loadQueueState'
  | 'deleteQueueState';

/** SQL statements */
export const SQL_STATEMENTS: Record<StatementName, string> = {
  // ON CONFLICT(id) DO UPDATE (upsert): a brand-new id is a plain INSERT (zero extra
  // cost); a colliding id overwrites the existing row in place. This makes durable
  // inserts idempotent against ORPHAN rows — a jobs row that outlived its in-memory
  // tracking when obliterate() or a buffer flush raced an in-flight insert — without
  // any per-push delete on the hot path. jobs has no triggers/FKs, so DO UPDATE has
  // no cascade side effects (unlike REPLACE = DELETE+INSERT).
  insertJob: `
    INSERT INTO jobs (
      id, queue, data, priority, created_at, run_at, attempts,
      max_attempts, backoff, ttl, timeout, unique_key, custom_id,
      depends_on, parent_id, children_ids, tags, state, lifo, group_id,
      remove_on_complete, remove_on_fail, stall_timeout, timeline
    ) VALUES (
      ?, ?, ?, ?, ?, ?, ?,
      ?, ?, ?, ?, ?, ?,
      ?, ?, ?, ?, ?, ?, ?,
      ?, ?, ?, ?
    )
    ON CONFLICT(id) DO UPDATE SET
      queue=excluded.queue, data=excluded.data, priority=excluded.priority,
      created_at=excluded.created_at, run_at=excluded.run_at, attempts=excluded.attempts,
      max_attempts=excluded.max_attempts, backoff=excluded.backoff, ttl=excluded.ttl,
      timeout=excluded.timeout, unique_key=excluded.unique_key, custom_id=excluded.custom_id,
      depends_on=excluded.depends_on, parent_id=excluded.parent_id, children_ids=excluded.children_ids,
      tags=excluded.tags, state=excluded.state, lifo=excluded.lifo, group_id=excluded.group_id,
      remove_on_complete=excluded.remove_on_complete, remove_on_fail=excluded.remove_on_fail,
      stall_timeout=excluded.stall_timeout, timeline=excluded.timeline,
      -- Per-execution columns are NOT in the INSERT column list, so excluded.<col>
      -- resolves to each column's DEFAULT (NULL / 0) — i.e. the fresh-job value. Reset
      -- them too, otherwise an upsert over an ORPHAN row would leave a brand-new job
      -- carrying the prior life's progress=100 / completed_at / stacktrace etc.
      started_at=excluded.started_at, completed_at=excluded.completed_at,
      progress=excluded.progress, progress_msg=excluded.progress_msg,
      last_heartbeat=excluded.last_heartbeat, stacktrace=excluded.stacktrace
  `,

  updateJobState: 'UPDATE jobs SET state = ?, started_at = ?, timeline = ? WHERE id = ?',

  completeJob:
    'UPDATE jobs SET state = ?, completed_at = ?, progress = 100, timeline = ? WHERE id = ?',

  deleteJob: 'DELETE FROM jobs WHERE id = ?',

  deleteJobResult: 'DELETE FROM job_results WHERE job_id = ?',

  getJob: 'SELECT * FROM jobs WHERE id = ?',

  insertResult:
    'INSERT OR REPLACE INTO job_results (job_id, result, completed_at) VALUES (?, ?, ?)',

  getResult: 'SELECT result FROM job_results WHERE job_id = ?',

  insertDlq: 'INSERT INTO dlq (job_id, queue, entry, entered_at) VALUES (?, ?, ?, ?)',

  loadDlq: 'SELECT * FROM dlq ORDER BY entered_at',

  deleteDlqEntry: 'DELETE FROM dlq WHERE job_id = ?',

  clearDlqQueue: 'DELETE FROM dlq WHERE queue = ?',

  insertCron: `
    INSERT OR REPLACE INTO cron_jobs
    (name, queue, data, schedule, repeat_every, priority, next_run, executions, max_limit, timezone, unique_key, dedup, skip_missed_on_restart, skip_if_no_worker, prevent_overlap, job_options)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `,

  updateCron: 'UPDATE cron_jobs SET executions = ?, next_run = ? WHERE name = ?',

  // Queue control-state persistence (#100): paused / rate-limit / concurrency.
  upsertQueueState:
    'INSERT OR REPLACE INTO queue_state (name, paused, rate_limit, concurrency_limit) VALUES (?, ?, ?, ?)',

  loadQueueState: 'SELECT name, paused, rate_limit, concurrency_limit FROM queue_state',

  deleteQueueState: 'DELETE FROM queue_state WHERE name = ?',
};

/** Prepare all statements */
export function prepareStatements(
  db: Database
): Map<StatementName, ReturnType<Database['prepare']>> {
  const statements = new Map<StatementName, ReturnType<Database['prepare']>>();

  for (const [name, sql] of Object.entries(SQL_STATEMENTS)) {
    statements.set(name as StatementName, db.prepare(sql));
  }

  return statements;
}

/** Database row type for jobs (BLOB fields are Uint8Array from bun:sqlite) */
export interface DbJob {
  id: string;
  queue: string;
  data: Uint8Array; // MessagePack BLOB
  priority: number;
  created_at: number;
  run_at: number;
  started_at: number | null;
  completed_at: number | null;
  attempts: number;
  max_attempts: number;
  backoff: number;
  ttl: number | null;
  timeout: number | null;
  unique_key: string | null;
  custom_id: string | null;
  depends_on: Uint8Array | null; // MessagePack BLOB
  parent_id: string | null;
  children_ids: Uint8Array | null; // MessagePack BLOB
  tags: Uint8Array | null; // MessagePack BLOB
  state: string;
  lifo: number;
  group_id: string | null;
  progress: number | null;
  progress_msg: string | null;
  remove_on_complete: number;
  remove_on_fail: number;
  stall_timeout: number | null;
  last_heartbeat: number | null;
  timeline: Uint8Array | null;
  stacktrace: Uint8Array | null; // MessagePack BLOB (#74)
}

/** Database row type for cron jobs */
export interface DbCron {
  name: string;
  queue: string;
  data: Uint8Array; // MessagePack BLOB
  schedule: string | null;
  repeat_every: number | null;
  priority: number;
  next_run: number;
  executions: number;
  max_limit: number | null;
  timezone: string | null;
  unique_key: string | null;
  dedup: Uint8Array | null; // MessagePack BLOB
  skip_missed_on_restart: number;
  skip_if_no_worker: number;
  prevent_overlap: number;
  job_options: Uint8Array | null; // MessagePack BLOB
}

/** Database row type for queue control-state (#100) */
export interface DbQueueState {
  name: string;
  paused: number;
  rate_limit: number | null;
  concurrency_limit: number | null;
}
