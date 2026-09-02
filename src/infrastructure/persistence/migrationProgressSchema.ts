/** Durable cursor for resumable, bounded SQLite data migrations. */
export const MIGRATION_PROGRESS_TABLE = `
CREATE TABLE IF NOT EXISTS migration_progress (
    version INTEGER NOT NULL,
    phase TEXT NOT NULL,
    last_key TEXT,
    processed_rows INTEGER NOT NULL DEFAULT 0,
    processed_bytes INTEGER NOT NULL DEFAULT 0,
    total_rows INTEGER NOT NULL DEFAULT 0,
    started_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (version, phase)
);
`;
