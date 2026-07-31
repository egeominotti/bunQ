/**
 * Durable, payload-free evidence for completed jobs removed by
 * `removeOnComplete`. Unreferenced rows follow the bounded FIFO retention
 * window; rows owned by live dependency edges remain pinned until checkpoint.
 */
export const DEPENDENCY_COMPLETION_SCHEMA = `
CREATE TABLE IF NOT EXISTS dependency_completions (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL UNIQUE,
    queue TEXT NOT NULL,
    completed_at INTEGER NOT NULL,
    pinned INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_dependency_completions_queue
    ON dependency_completions(queue);
`;
