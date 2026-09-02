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
  | 'updateJobProgress'
  | 'deleteJob'
  | 'deleteJobResult'
  | 'getJob'
  | 'insertResult'
  | 'getResult'
  | 'insertDlq'
  | 'loadDlq'
  | 'deleteDlqEntry'
  | 'deleteDlqEntryForQueue'
  | 'clearDlqQueue'
  | 'deleteDependencyCompletionForAdmission'
  | 'deleteFlowFailuresForAdmission'
  | 'pinDependencyCompletionForAdmission'
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
  // any per-push delete on the hot path. jobs has no foreign keys; DO UPDATE avoids
  // REPLACE's DELETE+INSERT semantics while the completion-count UPDATE triggers
  // intentionally account for state/queue transitions.
  insertJob: `
    INSERT INTO jobs (
      id, queue, name, data, priority, created_at, run_at, attempts,
      max_attempts, backoff, ttl, timeout, unique_key, custom_id,
      depends_on, parent_id, children_ids, tags, state, lifo, group_id,
      remove_on_complete, remove_on_fail, fail_parent_on_failure,
      remove_dependency_on_failure, continue_parent_on_failure,
      ignore_dependency_on_failure, stall_timeout, stall_count, timeline,
      dlq_retry_state, extended_options, started_at, completed_at, progress,
      progress_msg, last_heartbeat, stacktrace
    ) VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?,
      ?, ?, ?, ?, ?, ?,
      ?, ?, ?, ?, ?, ?, ?,
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
      ?, ?, ?, ?, ?, ?
    )
    ON CONFLICT(id) DO UPDATE SET
      queue=excluded.queue, name=excluded.name, data=excluded.data, priority=excluded.priority,
      created_at=excluded.created_at, run_at=excluded.run_at, attempts=excluded.attempts,
      max_attempts=excluded.max_attempts, backoff=excluded.backoff, ttl=excluded.ttl,
      timeout=excluded.timeout, unique_key=excluded.unique_key, custom_id=excluded.custom_id,
      depends_on=excluded.depends_on, parent_id=excluded.parent_id, children_ids=excluded.children_ids,
      tags=excluded.tags, state=excluded.state, lifo=excluded.lifo, group_id=excluded.group_id,
      remove_on_complete=excluded.remove_on_complete, remove_on_fail=excluded.remove_on_fail,
      fail_parent_on_failure=excluded.fail_parent_on_failure,
      remove_dependency_on_failure=excluded.remove_dependency_on_failure,
      continue_parent_on_failure=excluded.continue_parent_on_failure,
      ignore_dependency_on_failure=excluded.ignore_dependency_on_failure,
      stall_timeout=excluded.stall_timeout, stall_count=excluded.stall_count,
      timeline=excluded.timeline, dlq_retry_state=excluded.dlq_retry_state,
      extended_options=excluded.extended_options,
      -- Fresh admissions supply the normal NULL/0 execution values, while a
      -- delayed WriteBuffer retry supplies the Job's current lifecycle values.
      -- Either form must overwrite an ORPHAN generation completely.
      started_at=excluded.started_at, completed_at=excluded.completed_at,
      progress=excluded.progress, progress_msg=excluded.progress_msg,
      last_heartbeat=excluded.last_heartbeat, stacktrace=excluded.stacktrace
  `,

  updateJobState: 'UPDATE jobs SET state = ?, started_at = ?, timeline = ? WHERE id = ?',

  completeJob:
    'UPDATE jobs SET state = ?, completed_at = ?, progress = 100, timeline = ?, dlq_retry_state = NULL WHERE id = ?',

  updateJobProgress:
    'UPDATE jobs SET progress = ?, progress_msg = ?, last_heartbeat = ? WHERE id = ?',

  deleteJob: 'DELETE FROM jobs WHERE id = ?',

  deleteJobResult: 'DELETE FROM job_results WHERE job_id = ?',

  getJob: 'SELECT * FROM jobs WHERE id = ?',

  insertResult:
    'INSERT OR REPLACE INTO job_results (job_id, result, completed_at) VALUES (?, ?, ?)',

  getResult: 'SELECT result FROM job_results WHERE job_id = ?',

  insertDlq: 'INSERT INTO dlq (job_id, queue, entry, entered_at) VALUES (?, ?, ?, ?)',

  loadDlq: 'SELECT * FROM dlq ORDER BY entered_at',

  deleteDlqEntry: 'DELETE FROM dlq WHERE job_id = ?',

  deleteDlqEntryForQueue: 'DELETE FROM dlq WHERE job_id = ? AND queue = ?',

  clearDlqQueue: 'DELETE FROM dlq WHERE queue = ?',

  deleteDependencyCompletionForAdmission: 'DELETE FROM dependency_completions WHERE job_id = ?',

  deleteFlowFailuresForAdmission: 'DELETE FROM flow_failures WHERE parent_id = ? OR child_id = ?',

  pinDependencyCompletionForAdmission:
    'UPDATE dependency_completions SET pinned = 1 WHERE job_id = ?',

  insertCron: `
    INSERT OR REPLACE INTO cron_jobs
    (name, queue, job_name, data, schedule, repeat_every, priority, next_run, executions, max_limit, timezone, unique_key, dedup, skip_missed_on_restart, skip_if_no_worker, prevent_overlap, job_options)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `,

  updateCron: 'UPDATE cron_jobs SET executions = ?, next_run = ? WHERE name = ?',

  // Queue control-state persistence (#100): paused / rate-limit / concurrency.
  upsertQueueState:
    'INSERT OR REPLACE INTO queue_state (name, paused, rate_limit, concurrency_limit, rate_limit_duration, rate_limit_expires_at, stall_enabled, stall_interval, max_stalls, stall_grace_period, dlq_config) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',

  loadQueueState:
    'SELECT name, paused, rate_limit, concurrency_limit, rate_limit_duration, rate_limit_expires_at, stall_enabled, stall_interval, max_stalls, stall_grace_period, dlq_config FROM queue_state',

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
  name: string | null;
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
  fail_parent_on_failure: number;
  remove_dependency_on_failure: number;
  continue_parent_on_failure: number;
  ignore_dependency_on_failure: number;
  stall_timeout: number | null;
  last_heartbeat: number | null;
  stall_count: number;
  timeline: Uint8Array | null;
  stacktrace: Uint8Array | null; // MessagePack BLOB (#74)
  dlq_retry_state: Uint8Array | null;
  extended_options: Uint8Array | null;
}

/** Database row type for cron jobs */
export interface DbCron {
  name: string;
  queue: string;
  job_name: string | null;
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
  rate_limit_duration: number | null;
  rate_limit_expires_at: number | null;
  stall_enabled: number | null;
  stall_interval: number | null;
  max_stalls: number | null;
  stall_grace_period: number | null;
  dlq_config: Uint8Array | null;
}
