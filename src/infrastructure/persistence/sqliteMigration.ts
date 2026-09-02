import type { Database } from 'bun:sqlite';
import { storageLog } from '../../shared/logger';
import {
  migrateLegacyCronJobNames,
  migrateLegacyJobNames,
  type LegacyNameMigrationProgress,
} from './legacyNameMigration';
import { MIGRATION_PROGRESS_TABLE } from './migrationProgressSchema';
import { MIGRATIONS, type MigrationSql } from './migrations';
import { MIGRATION_TABLE, SCHEMA, SCHEMA_VERSION } from './schema';

function isIdempotentMigrationError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  return (
    /^duplicate column name: [A-Za-z_][A-Za-z0-9_]*$/i.test(error.message) ||
    /^(?:index|table|trigger) [A-Za-z_][A-Za-z0-9_]* already exists$/i.test(error.message)
  );
}

function migrationTableExists(db: Database): boolean {
  return (
    db
      .query<{ name: string }, []>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'migrations'"
      )
      .get() !== null
  );
}

function jobsTableExists(db: Database): boolean {
  return (
    db
      .query<{ name: string }, []>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'jobs'"
      )
      .get() !== null
  );
}

export function readSqliteSchemaVersion(db: Database): number {
  if (!migrationTableExists(db)) return 0;
  return (
    db.query<{ version: number }, []>('SELECT MAX(version) AS version FROM migrations').get()
      ?.version ?? 0
  );
}

export function assertSupportedSqliteSchema(db: Database): number {
  const version = readSqliteSchemaVersion(db);
  if (version > SCHEMA_VERSION) {
    throw new Error(
      `SQLite schema version ${version} is newer than supported version ${SCHEMA_VERSION}`
    );
  }
  return version;
}

function databaseBytes(db: Database): number {
  const pageCount =
    db.query<{ page_count: number }, []>('PRAGMA page_count').get()?.page_count ?? 0;
  const pageSize = db.query<{ page_size: number }, []>('PRAGMA page_size').get()?.page_size ?? 0;
  return pageCount * pageSize;
}

function applySql(db: Database, sql: MigrationSql): void {
  const statements = typeof sql === 'string' ? [sql] : sql;
  db.transaction(() => {
    for (const statement of statements) {
      try {
        db.run(statement);
      } catch (error) {
        if (!isIdempotentMigrationError(error)) throw error;
      }
    }
  })();
}

function markVersionComplete(db: Database, version: number): void {
  db.transaction(() => {
    db.prepare('INSERT OR IGNORE INTO migrations(version, applied_at) VALUES (?, ?)').run(
      version,
      Date.now()
    );
    db.prepare('DELETE FROM migration_progress WHERE version = ?').run(version);
  })();
}

function progressLogger(): (progress: LegacyNameMigrationProgress) => void {
  let first = true;
  let lastLoggedAt = 0;
  return (progress) => {
    const now = Date.now();
    if (!first && !progress.complete && now - lastLoggedAt < 5_000) return;
    first = false;
    lastLoggedAt = now;
    storageLog.infoToStderr('SQLite migration progress', {
      version: progress.version,
      phase: progress.phase,
      table: progress.table,
      processedRows: progress.processedRows,
      totalRows: progress.totalRows,
      processedBytes: progress.processedBytes,
      batchRows: progress.batchRows,
      batchBytes: progress.batchBytes,
      elapsedMs: progress.elapsedMs,
      complete: progress.complete,
    });
  };
}

function runDataMigration(db: Database, version: number): void {
  if (version === 31) migrateLegacyJobNames(db, progressLogger());
  if (version === 32) migrateLegacyCronJobNames(db, progressLogger());
}

function initializeDatabase(db: Database): void {
  db.transaction(() => {
    db.run(SCHEMA);
    db.prepare('INSERT INTO migrations(version, applied_at) VALUES (?, ?)').run(
      SCHEMA_VERSION,
      Date.now()
    );
  })();
}

/** Apply every pending SQLite migration in restart-safe, bounded phases. */
export function migrateSqliteDatabase(db: Database, path: string): void {
  db.run(MIGRATION_TABLE);
  db.run(MIGRATION_PROGRESS_TABLE);
  const currentVersion = assertSupportedSqliteSchema(db);
  if (currentVersion === SCHEMA_VERSION) return;
  if (currentVersion === 0 && !jobsTableExists(db)) {
    initializeDatabase(db);
    return;
  }

  const startedAt = Date.now();
  const resumeRows =
    db.query<{ count: number }, []>('SELECT COUNT(*) AS count FROM migration_progress').get()
      ?.count ?? 0;
  storageLog.infoToStderr('Starting SQLite migration', {
    path,
    fromVersion: currentVersion,
    toVersion: SCHEMA_VERSION,
    databaseBytes: databaseBytes(db),
    resuming: resumeRows > 0,
    interruptionPolicy: 'restart with the same or newer bunqueue version; do not downgrade',
  });

  let failedVersion = currentVersion;
  try {
    if (currentVersion === 0) {
      failedVersion = 1;
      storageLog.infoToStderr('Applying SQLite migration step', {
        version: 1,
        targetVersion: SCHEMA_VERSION,
      });
      applySql(db, SCHEMA);
      markVersionComplete(db, 1);
      storageLog.infoToStderr('Completed SQLite migration step', { version: 1 });
    } else {
      storageLog.infoToStderr('Ensuring SQLite base schema', {
        fromVersion: currentVersion,
        targetVersion: SCHEMA_VERSION,
      });
      applySql(db, SCHEMA);
      storageLog.infoToStderr('Ensured SQLite base schema', { fromVersion: currentVersion });
    }
    const versions = Object.keys(MIGRATIONS)
      .map(Number)
      .filter((version) => version > currentVersion && version > 1)
      .sort((left, right) => left - right);
    for (const version of versions) {
      failedVersion = version;
      storageLog.infoToStderr('Applying SQLite migration step', {
        version,
        targetVersion: SCHEMA_VERSION,
      });
      applySql(db, MIGRATIONS[version]);
      runDataMigration(db, version);
      markVersionComplete(db, version);
      storageLog.infoToStderr('Completed SQLite migration step', { version });
    }
    db.run('PRAGMA wal_checkpoint(TRUNCATE)');
    storageLog.infoToStderr('Completed SQLite migration', {
      fromVersion: currentVersion,
      toVersion: SCHEMA_VERSION,
      durationMs: Date.now() - startedAt,
      databaseBytes: databaseBytes(db),
    });
  } catch (error) {
    storageLog.error('SQLite migration failed', {
      fromVersion: currentVersion,
      toVersion: SCHEMA_VERSION,
      failedVersion,
      durationMs: Date.now() - startedAt,
      resumable: true,
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}
