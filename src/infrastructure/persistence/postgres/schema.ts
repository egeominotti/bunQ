import { POSTGRES_EVENT_JOURNAL_SCHEMA } from './eventJournalSchema';

export const POSTGRES_SCHEMA_VERSION = 15;

/** PostgreSQL schema for the database-authoritative distributed queue engine. */
export const POSTGRES_SCHEMA = `
CREATE TABLE IF NOT EXISTS bunqueue_schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS bunqueue_jobs (
  namespace TEXT NOT NULL,
  id TEXT NOT NULL,
  queue TEXT NOT NULL,
  payload BYTEA NOT NULL,
  state TEXT NOT NULL CHECK (
    state IN ('waiting', 'prioritized', 'delayed', 'waiting-children',
              'active', 'completed', 'failed')
  ),
  priority INTEGER NOT NULL DEFAULT 0,
  lifo BOOLEAN NOT NULL DEFAULT FALSE,
  run_at BIGINT NOT NULL,
  created_at BIGINT NOT NULL,
  started_at BIGINT,
  completed_at BIGINT,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  ttl BIGINT,
  timeout BIGINT,
  unique_key TEXT,
  unique_expires_at BIGINT,
  custom_id TEXT,
  group_id TEXT,
  parent_id TEXT,
  lease_owner TEXT,
  lease_broker_id TEXT,
  lease_token TEXT,
  lease_until BIGINT,
  lease_renewals INTEGER NOT NULL DEFAULT 0,
  result BYTEA,
  dlq_entry BYTEA,
  dlq_retry_state BYTEA,
  error TEXT,
  failure_reason TEXT,
  version BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (namespace, id)
);

CREATE INDEX IF NOT EXISTS bunqueue_jobs_ready_idx
  ON bunqueue_jobs(namespace, queue, priority DESC, run_at ASC, id ASC)
  WHERE state IN ('waiting', 'prioritized', 'delayed');
CREATE INDEX IF NOT EXISTS bunqueue_jobs_state_idx
  ON bunqueue_jobs(namespace, queue, state, created_at, id);
CREATE INDEX IF NOT EXISTS bunqueue_jobs_lease_idx
  ON bunqueue_jobs(namespace, lease_until)
  WHERE state = 'active';
CREATE INDEX IF NOT EXISTS bunqueue_jobs_parent_idx
  ON bunqueue_jobs(namespace, parent_id)
  WHERE parent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS bunqueue_jobs_group_ready_idx
  ON bunqueue_jobs(namespace, queue, group_id, priority DESC, run_at ASC, id ASC)
  WHERE group_id IS NOT NULL AND state IN ('waiting', 'prioritized', 'delayed');
CREATE INDEX IF NOT EXISTS bunqueue_jobs_group_active_idx
  ON bunqueue_jobs(namespace, queue, group_id, lease_until)
  WHERE group_id IS NOT NULL AND state = 'active';
CREATE INDEX IF NOT EXISTS bunqueue_jobs_lifo_ready_idx
  ON bunqueue_jobs(namespace, queue, priority DESC, id DESC)
  WHERE group_id IS NULL AND lifo AND state IN ('waiting', 'prioritized', 'delayed');
CREATE INDEX IF NOT EXISTS bunqueue_jobs_ttl_pending_idx
  ON bunqueue_jobs(namespace, queue, created_at, id)
  WHERE ttl IS NOT NULL AND state IN ('waiting', 'prioritized', 'delayed');
CREATE UNIQUE INDEX IF NOT EXISTS bunqueue_jobs_live_unique_key_idx
  ON bunqueue_jobs(namespace, queue, unique_key)
  WHERE unique_key IS NOT NULL
    AND state IN ('waiting', 'prioritized', 'delayed', 'waiting-children', 'active');

ALTER TABLE bunqueue_jobs
  ADD COLUMN IF NOT EXISTS unique_expires_at BIGINT;
ALTER TABLE bunqueue_jobs
  ADD COLUMN IF NOT EXISTS lease_broker_id TEXT;
ALTER TABLE bunqueue_jobs
  ADD COLUMN IF NOT EXISTS lease_renewals INTEGER NOT NULL DEFAULT 0;
ALTER TABLE bunqueue_jobs
  ADD COLUMN IF NOT EXISTS dlq_retry_state BYTEA;

CREATE TABLE IF NOT EXISTS bunqueue_dependencies (
  namespace TEXT NOT NULL,
  job_id TEXT NOT NULL,
  dependency_id TEXT NOT NULL,
  PRIMARY KEY (namespace, job_id, dependency_id)
);
CREATE INDEX IF NOT EXISTS bunqueue_dependencies_dependency_idx
  ON bunqueue_dependencies(namespace, dependency_id, job_id);

CREATE TABLE IF NOT EXISTS bunqueue_flow_failures (
  namespace TEXT NOT NULL,
  parent_id TEXT NOT NULL,
  child_id TEXT NOT NULL,
  child_queue TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('fail', 'remove', 'ignore', 'continue')),
  error TEXT NOT NULL,
  created_at BIGINT NOT NULL,
  PRIMARY KEY (namespace, parent_id, child_id)
);
CREATE INDEX IF NOT EXISTS bunqueue_flow_failures_parent_idx
  ON bunqueue_flow_failures(namespace, parent_id, mode);

CREATE TABLE IF NOT EXISTS bunqueue_repeat_links (
  namespace TEXT NOT NULL,
  original_id TEXT NOT NULL,
  successor_id TEXT NOT NULL,
  PRIMARY KEY (namespace, original_id)
);
CREATE INDEX IF NOT EXISTS bunqueue_repeat_links_successor_idx
  ON bunqueue_repeat_links(namespace, successor_id);

CREATE TABLE IF NOT EXISTS bunqueue_job_logs (
  id BIGSERIAL PRIMARY KEY,
  namespace TEXT NOT NULL,
  job_id TEXT NOT NULL,
  timestamp BIGINT NOT NULL,
  level TEXT NOT NULL CHECK (level IN ('info', 'warn', 'error')),
  message TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS bunqueue_job_logs_job_idx
  ON bunqueue_job_logs(namespace, job_id, id);

CREATE TABLE IF NOT EXISTS bunqueue_crons (
  namespace TEXT NOT NULL,
  name TEXT NOT NULL,
  payload BYTEA NOT NULL,
  next_run BIGINT NOT NULL,
  executions INTEGER NOT NULL DEFAULT 0,
  max_limit INTEGER,
  updated_at BIGINT NOT NULL,
  PRIMARY KEY (namespace, name)
);
CREATE INDEX IF NOT EXISTS bunqueue_crons_due_idx
  ON bunqueue_crons(namespace, next_run, name);

CREATE TABLE IF NOT EXISTS bunqueue_workers (
  namespace TEXT NOT NULL,
  id TEXT NOT NULL,
  broker_id TEXT NOT NULL,
  client_id TEXT,
  queues TEXT[] NOT NULL,
  payload BYTEA NOT NULL,
  last_seen BIGINT NOT NULL,
  PRIMARY KEY (namespace, id)
);
CREATE INDEX IF NOT EXISTS bunqueue_workers_queue_idx
  ON bunqueue_workers USING GIN(queues);
CREATE INDEX IF NOT EXISTS bunqueue_workers_broker_idx
  ON bunqueue_workers(namespace, broker_id, client_id);

CREATE TABLE IF NOT EXISTS bunqueue_metric_buckets (
  namespace TEXT NOT NULL,
  queue TEXT NOT NULL,
  metric_type TEXT NOT NULL CHECK (metric_type IN ('completed', 'failed')),
  minute BIGINT NOT NULL,
  count INTEGER NOT NULL,
  PRIMARY KEY (namespace, queue, metric_type, minute)
);
CREATE TABLE IF NOT EXISTS bunqueue_metric_totals (
  namespace TEXT NOT NULL,
  queue TEXT NOT NULL,
  metric_type TEXT NOT NULL CHECK (metric_type IN ('completed', 'failed')),
  total_count BIGINT NOT NULL,
  prev_ts BIGINT NOT NULL,
  prev_count INTEGER NOT NULL,
  PRIMARY KEY (namespace, queue, metric_type)
);

CREATE TABLE IF NOT EXISTS bunqueue_completions (
  namespace TEXT NOT NULL,
  job_id TEXT NOT NULL,
  queue TEXT NOT NULL,
  completed_at BIGINT NOT NULL,
  result BYTEA,
  PRIMARY KEY (namespace, job_id)
);
CREATE INDEX IF NOT EXISTS bunqueue_completions_queue_idx
  ON bunqueue_completions(namespace, queue, job_id);
CREATE INDEX IF NOT EXISTS bunqueue_completions_recent_idx
  ON bunqueue_completions(namespace, completed_at DESC, job_id DESC);

CREATE TABLE IF NOT EXISTS bunqueue_queue_state (
  namespace TEXT NOT NULL,
  queue TEXT NOT NULL,
  paused BOOLEAN NOT NULL DEFAULT FALSE,
  rate_limit INTEGER,
  rate_duration_ms BIGINT,
  rate_window_started_at BIGINT,
  rate_expires_at BIGINT,
  rate_count INTEGER NOT NULL DEFAULT 0,
  concurrency_limit INTEGER,
  stall_config BYTEA,
  dlq_config BYTEA,
  PRIMARY KEY (namespace, queue)
);

ALTER TABLE bunqueue_queue_state
  ADD COLUMN IF NOT EXISTS rate_expires_at BIGINT;

INSERT INTO bunqueue_queue_state (namespace, queue)
SELECT DISTINCT namespace, queue FROM bunqueue_jobs
ON CONFLICT (namespace, queue) DO NOTHING;

CREATE TABLE IF NOT EXISTS bunqueue_events (
  id BIGSERIAL PRIMARY KEY,
  namespace TEXT NOT NULL,
  queue TEXT NOT NULL,
  event_type TEXT NOT NULL,
  job_id TEXT NOT NULL,
  occurred_at BIGINT NOT NULL,
  payload BYTEA
);
CREATE INDEX IF NOT EXISTS bunqueue_events_namespace_id_idx
  ON bunqueue_events(namespace, id);
CREATE INDEX IF NOT EXISTS bunqueue_events_queue_id_idx
  ON bunqueue_events(namespace, queue, id DESC);

CREATE TABLE IF NOT EXISTS bunqueue_event_prune_watermarks (
  namespace TEXT NOT NULL,
  queue TEXT NOT NULL,
  source_event_id BIGINT NOT NULL,
  pruned_through BIGINT NOT NULL,
  PRIMARY KEY (namespace, queue, source_event_id)
);
CREATE INDEX IF NOT EXISTS bunqueue_event_prune_watermarks_namespace_queue_idx
  ON bunqueue_event_prune_watermarks(namespace, queue, pruned_through DESC);

${POSTGRES_EVENT_JOURNAL_SCHEMA}

CREATE TABLE IF NOT EXISTS bunqueue_brokers (
  namespace TEXT NOT NULL,
  broker_id TEXT NOT NULL,
  started_at BIGINT NOT NULL,
  heartbeat_at BIGINT NOT NULL,
  PRIMARY KEY (namespace, broker_id)
);
`;

export const POSTGRES_EVENT_CHANNEL = 'bunqueue_jobs_changed';
