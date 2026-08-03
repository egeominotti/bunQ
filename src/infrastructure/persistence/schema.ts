import { DEPENDENCY_COMPLETION_SCHEMA } from './dependencyCompletionSchema';
/** SQLite PRAGMA settings for optimal performance */
export const PRAGMA_SETTINGS = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -64000;
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 268435456;
PRAGMA page_size = 4096;
PRAGMA busy_timeout = 5000;
`;
/** Main schema creation. */
export const SCHEMA = `
-- Jobs table (using UUIDv7 for job IDs)
-- Uses BLOB for data fields (MessagePack serialization for ~2-3x faster than JSON)
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    name TEXT,
    data BLOB NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    run_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    backoff INTEGER NOT NULL DEFAULT 1000,
    ttl INTEGER,
    timeout INTEGER,
    unique_key TEXT,
    custom_id TEXT,
    depends_on BLOB,
    parent_id TEXT,
    children_ids BLOB,
    tags BLOB,
    state TEXT NOT NULL DEFAULT 'waiting',
    lifo INTEGER NOT NULL DEFAULT 0,
    group_id TEXT,
    progress INTEGER DEFAULT 0,
    progress_msg TEXT,
    remove_on_complete INTEGER DEFAULT 0,
    remove_on_fail INTEGER DEFAULT 0,
    fail_parent_on_failure INTEGER NOT NULL DEFAULT 0,
    remove_dependency_on_failure INTEGER NOT NULL DEFAULT 0,
    continue_parent_on_failure INTEGER NOT NULL DEFAULT 0,
    ignore_dependency_on_failure INTEGER NOT NULL DEFAULT 0,
    stall_timeout INTEGER,
    last_heartbeat INTEGER,
    stall_count INTEGER NOT NULL DEFAULT 0,
    timeline BLOB,
    stacktrace BLOB,
    dlq_retry_state BLOB,
    extended_options BLOB
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_jobs_queue_state
    ON jobs(queue, state);
-- Stable createdAt/id pagination for unfiltered and logical-state queue views
CREATE INDEX IF NOT EXISTS idx_jobs_queue_created
    ON jobs(queue, created_at, id);
CREATE INDEX IF NOT EXISTS idx_jobs_queue_state_created
    ON jobs(queue, state, created_at, id);
CREATE INDEX IF NOT EXISTS idx_jobs_run_at
    ON jobs(run_at) WHERE state IN ('waiting', 'prioritized', 'waiting-children', 'delayed');
CREATE INDEX IF NOT EXISTS idx_jobs_unique
    ON jobs(queue, unique_key) WHERE unique_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_custom_id
    ON jobs(custom_id) WHERE custom_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_parent
    ON jobs(parent_id) WHERE parent_id IS NOT NULL;

-- Durable outbox/state for terminal child failure propagation.
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

${DEPENDENCY_COMPLETION_SCHEMA}

-- Job results storage (BLOB for MessagePack)
CREATE TABLE IF NOT EXISTS job_results (
    job_id TEXT PRIMARY KEY,
    result BLOB,
    completed_at INTEGER NOT NULL
);

-- Dead letter queue (BLOB for MessagePack - stores full DlqEntry)
CREATE TABLE IF NOT EXISTS dlq (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    queue TEXT NOT NULL,
    entry BLOB NOT NULL,
    entered_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dlq_queue ON dlq(queue);
CREATE INDEX IF NOT EXISTS idx_dlq_job_id ON dlq(job_id);
CREATE INDEX IF NOT EXISTS idx_dlq_entered_at ON dlq(entered_at);

-- Performance indexes for high-throughput operations
-- Stall detection: runs every 5s, needs fast lookup of active jobs by started_at
CREATE INDEX IF NOT EXISTS idx_jobs_state_started
    ON jobs(state, started_at) WHERE state = 'active';

-- Group operations: fast lookup by group_id
CREATE INDEX IF NOT EXISTS idx_jobs_group_id
    ON jobs(group_id) WHERE group_id IS NOT NULL;

-- Pending jobs: compound index for priority-ordered retrieval
CREATE INDEX IF NOT EXISTS idx_jobs_pending_priority
    ON jobs(queue, state, priority DESC, run_at ASC) WHERE state IN ('waiting', 'prioritized', 'waiting-children', 'delayed');

-- Completed jobs: index for recovery ordering (issue #84)
CREATE INDEX IF NOT EXISTS idx_jobs_completed_order
    ON jobs(completed_at DESC) WHERE state = 'completed';

-- Cron jobs (BLOB for MessagePack)
CREATE TABLE IF NOT EXISTS cron_jobs (
    name TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    job_name TEXT,
    data BLOB NOT NULL,
    schedule TEXT,
    repeat_every INTEGER,
    priority INTEGER NOT NULL DEFAULT 0,
    next_run INTEGER NOT NULL,
    executions INTEGER NOT NULL DEFAULT 0,
    max_limit INTEGER,
    timezone TEXT,
    unique_key TEXT,
    dedup BLOB,
    skip_missed_on_restart INTEGER NOT NULL DEFAULT 0,
    skip_if_no_worker INTEGER NOT NULL DEFAULT 0,
    prevent_overlap INTEGER NOT NULL DEFAULT 1,
    job_options BLOB
);

-- Queue state persistence (optional)
CREATE TABLE IF NOT EXISTS queue_state (
    name TEXT PRIMARY KEY,
    paused INTEGER NOT NULL DEFAULT 0,
    rate_limit INTEGER,
    concurrency_limit INTEGER,
    rate_limit_duration INTEGER,
    rate_limit_expires_at INTEGER,
    stall_enabled INTEGER,
    stall_interval INTEGER,
    max_stalls INTEGER,
    stall_grace_period INTEGER,
    dlq_config BLOB
);

-- Per-queue lifecycle event journal. Entries are kept newest-first by id and
-- bounded by QueueManagerConfig.maxQueueEvents when they are appended.
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

-- Cumulative terminal counters are separate from bounded minute buckets so
-- pruning old data points never resets meta.count.
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
`;
/** Migration version table */
export const MIGRATION_TABLE = `
CREATE TABLE IF NOT EXISTS migrations (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
);
`;
/** Current schema version */
export const SCHEMA_VERSION = 34;
