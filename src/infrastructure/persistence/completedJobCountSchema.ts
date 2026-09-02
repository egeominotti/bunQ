/** Exact retained-completion counts maintained by the jobs lifecycle itself. */
export const COMPLETED_JOB_COUNTS_TABLE = `
CREATE TABLE IF NOT EXISTS completed_job_counts (
    queue TEXT PRIMARY KEY,
    count INTEGER NOT NULL CHECK (count >= 0)
);
`;

export const COMPLETED_JOB_COUNT_TRIGGERS = `
CREATE TRIGGER IF NOT EXISTS trg_jobs_completed_insert
AFTER INSERT ON jobs
WHEN NEW.state = 'completed'
BEGIN
    INSERT INTO completed_job_counts(queue, count) VALUES (NEW.queue, 1)
    ON CONFLICT(queue) DO UPDATE SET count = count + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_jobs_completed_leave
AFTER UPDATE OF state, queue ON jobs
WHEN OLD.state = 'completed'
 AND (NEW.state <> 'completed' OR NEW.queue <> OLD.queue)
BEGIN
    UPDATE completed_job_counts SET count = count - 1 WHERE queue = OLD.queue;
    DELETE FROM completed_job_counts WHERE queue = OLD.queue AND count = 0;
END;

CREATE TRIGGER IF NOT EXISTS trg_jobs_completed_enter
AFTER UPDATE OF state, queue ON jobs
WHEN NEW.state = 'completed'
 AND (OLD.state <> 'completed' OR NEW.queue <> OLD.queue)
BEGIN
    INSERT INTO completed_job_counts(queue, count) VALUES (NEW.queue, 1)
    ON CONFLICT(queue) DO UPDATE SET count = count + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_jobs_completed_delete
AFTER DELETE ON jobs
WHEN OLD.state = 'completed'
BEGIN
    UPDATE completed_job_counts SET count = count - 1 WHERE queue = OLD.queue;
    DELETE FROM completed_job_counts WHERE queue = OLD.queue AND count = 0;
END;
`;

export const COMPLETED_JOB_RETENTION_INDEX = `
CREATE INDEX IF NOT EXISTS idx_jobs_completed_retention
    ON jobs(queue, COALESCE(completed_at, created_at), id)
    WHERE state = 'completed';

CREATE INDEX IF NOT EXISTS idx_jobs_completed_retention_global
    ON jobs(COALESCE(completed_at, created_at), id)
    WHERE state = 'completed';
`;

/** Backfill existing rows before lifecycle triggers become authoritative. */
export const COMPLETED_JOB_MAINTENANCE_MIGRATION = `
DROP INDEX IF EXISTS idx_jobs_completed_order;
CREATE INDEX idx_jobs_completed_order
    ON jobs(completed_at DESC, id DESC) WHERE state = 'completed';
${COMPLETED_JOB_RETENTION_INDEX}
${COMPLETED_JOB_COUNTS_TABLE}
DELETE FROM completed_job_counts;
INSERT INTO completed_job_counts(queue, count)
SELECT queue, COUNT(*) FROM jobs WHERE state = 'completed' GROUP BY queue;
${COMPLETED_JOB_COUNT_TRIGGERS}
`;
