import { DEPENDENCY_COMPLETION_SCHEMA } from './dependencyCompletionSchema';
import { SCHEMA } from './schema';

export type MigrationSql = string | readonly string[];

/** All SQLite migrations in order. Arrays preserve atomic statement boundaries. */
export const MIGRATIONS: Record<number, MigrationSql> = {
  1: SCHEMA,
  // Migration 5: Add performance indexes for high-throughput operations
  5: `
CREATE INDEX IF NOT EXISTS idx_dlq_entered_at ON dlq(entered_at);
CREATE INDEX IF NOT EXISTS idx_jobs_state_started
    ON jobs(state, started_at) WHERE state = 'active';
CREATE INDEX IF NOT EXISTS idx_jobs_group_id
    ON jobs(group_id) WHERE group_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_pending_priority
    ON jobs(queue, state, priority DESC, run_at ASC) WHERE state IN ('waiting', 'delayed');
`,
  // Migration 6: Add deduplication fields to cron_jobs
  6: [
    'ALTER TABLE cron_jobs ADD COLUMN unique_key TEXT;',
    'ALTER TABLE cron_jobs ADD COLUMN dedup BLOB;',
  ],
  // Migration 7: Add timeline blob to jobs
  7: 'ALTER TABLE jobs ADD COLUMN timeline BLOB;',
  // Migration 8: Add skipMissedOnRestart to cron_jobs
  8: 'ALTER TABLE cron_jobs ADD COLUMN skip_missed_on_restart INTEGER NOT NULL DEFAULT 0;',
  // Migration 9: Add skipIfNoWorker to cron_jobs
  9: 'ALTER TABLE cron_jobs ADD COLUMN skip_if_no_worker INTEGER NOT NULL DEFAULT 0;',
  // Migration 10: Add preventOverlap to cron_jobs (default 1 = enabled)
  10: 'ALTER TABLE cron_jobs ADD COLUMN prevent_overlap INTEGER NOT NULL DEFAULT 1;',
  // Migration 11: Index for completed-job recovery ordering (issue #84)
  11: `
CREATE INDEX IF NOT EXISTS idx_jobs_completed_order
    ON jobs(completed_at DESC) WHERE state = 'completed';
`,
  // Migration 12: Add per-cron job options (retry/cleanup policy, issue #86)
  12: 'ALTER TABLE cron_jobs ADD COLUMN job_options BLOB;',
  // Migration 13: Persist the last failure's stacktrace on jobs (issue #74)
  13: 'ALTER TABLE jobs ADD COLUMN stacktrace BLOB;',
  // Migration 14: Stable, index-backed getJobs ordering and pagination
  14: `
CREATE INDEX IF NOT EXISTS idx_jobs_queue_created
    ON jobs(queue, created_at, id);
CREATE INDEX IF NOT EXISTS idx_jobs_queue_state_created
    ON jobs(queue, state, created_at, id);
`,
  // Migrations 15-16: rate-limit window and TTL auto-expiry on queue_state.
  15: 'ALTER TABLE queue_state ADD COLUMN rate_limit_duration INTEGER;',
  16: 'ALTER TABLE queue_state ADD COLUMN rate_limit_expires_at INTEGER;',
  // Migration 17: preserve the cumulative stall bound across restarts.
  17: 'ALTER TABLE jobs ADD COLUMN stall_count INTEGER NOT NULL DEFAULT 0;',
  // Migrations 18-21: persist the complete per-queue stall policy.
  18: 'ALTER TABLE queue_state ADD COLUMN stall_enabled INTEGER;',
  19: 'ALTER TABLE queue_state ADD COLUMN stall_interval INTEGER;',
  20: 'ALTER TABLE queue_state ADD COLUMN max_stalls INTEGER;',
  21: 'ALTER TABLE queue_state ADD COLUMN stall_grace_period INTEGER;',
  // Migration 22: Persist the complete per-queue DLQ policy atomically.
  22: 'ALTER TABLE queue_state ADD COLUMN dlq_config BLOB;',
  // Migrations 23-26: flow failure policy must survive active-job recovery.
  23: 'ALTER TABLE jobs ADD COLUMN fail_parent_on_failure INTEGER NOT NULL DEFAULT 0;',
  24: 'ALTER TABLE jobs ADD COLUMN remove_dependency_on_failure INTEGER NOT NULL DEFAULT 0;',
  25: 'ALTER TABLE jobs ADD COLUMN continue_parent_on_failure INTEGER NOT NULL DEFAULT 0;',
  26: 'ALTER TABLE jobs ADD COLUMN ignore_dependency_on_failure INTEGER NOT NULL DEFAULT 0;',
  27: `
CREATE TABLE IF NOT EXISTS flow_failures (
    parent_id TEXT NOT NULL,
    child_id TEXT NOT NULL,
    child_queue TEXT NOT NULL,
    mode TEXT NOT NULL,
    error TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (parent_id, child_id)
);
CREATE INDEX IF NOT EXISTS idx_flow_failures_parent ON flow_failures(parent_id);

DROP INDEX IF EXISTS idx_jobs_run_at;
CREATE INDEX idx_jobs_run_at
    ON jobs(run_at) WHERE state IN ('waiting', 'prioritized', 'waiting-children', 'delayed');

DROP INDEX IF EXISTS idx_jobs_pending_priority;
CREATE INDEX idx_jobs_pending_priority
    ON jobs(queue, state, priority DESC, run_at ASC) WHERE state IN ('waiting', 'prioritized', 'waiting-children', 'delayed');
`,
  28: DEPENDENCY_COMPLETION_SCHEMA,
  29: 'ALTER TABLE dependency_completions ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0;',
  // Preserve bounded DLQ auto-retry metadata while the job is live.
  30: 'ALTER TABLE jobs ADD COLUMN dlq_retry_state BLOB;',
  // Persist operation names outside user payloads.
  31: 'ALTER TABLE jobs ADD COLUMN name TEXT;',
  32: 'ALTER TABLE cron_jobs ADD COLUMN job_name TEXT;',
  33: `
CREATE TABLE IF NOT EXISTS queue_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue TEXT NOT NULL,
    event_type TEXT NOT NULL,
    job_id TEXT NOT NULL,
    occurred_at INTEGER NOT NULL,
    payload BLOB
);
CREATE INDEX IF NOT EXISTS idx_queue_events_queue_id
    ON queue_events(queue, id DESC);
CREATE TABLE IF NOT EXISTS queue_metrics_meta (
    queue TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('completed', 'failed')),
    total_count INTEGER NOT NULL DEFAULT 0,
    prev_ts INTEGER NOT NULL DEFAULT 0,
    prev_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (queue, type)
);
CREATE TABLE IF NOT EXISTS queue_metric_buckets (
    queue TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('completed', 'failed')),
    minute INTEGER NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (queue, type, minute)
);
CREATE INDEX IF NOT EXISTS idx_queue_metric_buckets_window
    ON queue_metric_buckets(queue, type, minute DESC);
`,
  // Persist repeat-chain and advanced per-generation job policies.
  34: 'ALTER TABLE jobs ADD COLUMN extended_options BLOB;',
  // Persist per-group rate and concurrency overrides.
  35: `
CREATE TABLE IF NOT EXISTS group_state (
    queue TEXT NOT NULL,
    group_id TEXT NOT NULL,
    rate_limit INTEGER,
    rate_duration INTEGER,
    concurrency_limit INTEGER,
    PRIMARY KEY (queue, group_id)
);
`,
};
