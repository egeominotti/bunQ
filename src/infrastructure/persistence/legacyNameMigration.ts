import type { Database } from 'bun:sqlite';
import { normalizeLegacyJobPayload } from '../../domain/job/payload';
import { decodeMessagePack, encodeMessagePack } from '../../shared/msgpack';

interface LegacyNameRow {
  id: string;
  data: Uint8Array;
}

interface LegacyNameCandidate {
  id: string;
  bytes: number;
}

interface MigrationProgressRow {
  last_key: string | null;
  processed_rows: number;
  processed_bytes: number;
  total_rows: number;
  started_at: number;
}

export interface LegacyNameMigrationProgress {
  version: 31 | 32;
  phase: 'job-names' | 'cron-job-names';
  table: 'jobs' | 'cron_jobs';
  processedRows: number;
  processedBytes: number;
  totalRows: number;
  batchRows: number;
  batchBytes: number;
  elapsedMs: number;
  complete: boolean;
}

const MIGRATION_BATCH_SIZE = 500;
const MIGRATION_BATCH_BYTES = 8 * 1024 * 1024;

function splitLegacyBlob(row: LegacyNameRow): { name: string; data: Uint8Array } {
  try {
    const decoded = decodeMessagePack(row.data);
    const normalized = normalizeLegacyJobPayload({ data: decoded });
    return {
      name: normalized.name,
      data: normalized.legacy ? encodeMessagePack(normalized.data) : row.data,
    };
  } catch {
    // Preserve an undecodable blob byte-for-byte. Its existing read-path error
    // handling remains authoritative; the migration only supplies a safe name.
    return { name: 'default', data: row.data };
  }
}

function boundedBatch(candidates: LegacyNameCandidate[]): LegacyNameCandidate[] {
  const batch: LegacyNameCandidate[] = [];
  let bytes = 0;
  for (const candidate of candidates) {
    if (batch.length > 0 && bytes + candidate.bytes > MIGRATION_BATCH_BYTES) break;
    batch.push(candidate);
    bytes += candidate.bytes;
    if (bytes >= MIGRATION_BATCH_BYTES) break;
  }
  return batch;
}

function migrateLegacyNames(
  db: Database,
  options: {
    version: 31 | 32;
    phase: 'job-names' | 'cron-job-names';
    table: 'jobs' | 'cron_jobs';
    idColumn: 'id' | 'name';
    nameColumn: 'name' | 'job_name';
    onProgress?: (progress: LegacyNameMigrationProgress) => void;
  }
): LegacyNameMigrationProgress {
  const { version, phase, table, idColumn, nameColumn, onProgress } = options;
  const countRemaining = db.query<{ count: number }, []>(
    `SELECT COUNT(*) AS count FROM ${table} WHERE ${nameColumn} IS NULL`
  );
  const now = Date.now();
  const existing = db
    .query<MigrationProgressRow, [number, string]>(
      `SELECT last_key, processed_rows, processed_bytes, total_rows, started_at
       FROM migration_progress WHERE version = ? AND phase = ?`
    )
    .get(version, phase);
  let progress: MigrationProgressRow = existing ?? {
    last_key: null,
    processed_rows: 0,
    processed_bytes: 0,
    total_rows: countRemaining.get()?.count ?? 0,
    started_at: now,
  };
  if (!existing) {
    db.prepare(
      `INSERT INTO migration_progress
       (version, phase, last_key, processed_rows, processed_bytes, total_rows, started_at, updated_at)
       VALUES (?, ?, NULL, 0, 0, ?, ?, ?)`
    ).run(version, phase, progress.total_rows, now, now);
  }

  const selectFirst = db.query<LegacyNameCandidate, [number]>(
    `SELECT ${idColumn} AS id, LENGTH(data) AS bytes
     FROM ${table} WHERE ${nameColumn} IS NULL
     ORDER BY ${idColumn} LIMIT ?`
  );
  const selectAfter = db.query<LegacyNameCandidate, [string, number]>(
    `SELECT ${idColumn} AS id, LENGTH(data) AS bytes
     FROM ${table} WHERE ${nameColumn} IS NULL AND ${idColumn} > ?
     ORDER BY ${idColumn} LIMIT ?`
  );
  const selectBlob = db.query<LegacyNameRow, [string]>(
    `SELECT ${idColumn} AS id, data FROM ${table}
     WHERE ${idColumn} = ? AND ${nameColumn} IS NULL`
  );
  const update = db.prepare(
    `UPDATE ${table} SET ${nameColumn} = ?, data = ?
     WHERE ${idColumn} = ? AND ${nameColumn} IS NULL`
  );
  const saveProgress = db.prepare(
    `UPDATE migration_progress
     SET last_key = ?, processed_rows = ?, processed_bytes = ?, updated_at = ?
     WHERE version = ? AND phase = ?`
  );

  while (true) {
    const candidates = progress.last_key
      ? selectAfter.all(progress.last_key, MIGRATION_BATCH_SIZE)
      : selectFirst.all(MIGRATION_BATCH_SIZE);
    const batch = boundedBatch(candidates);
    if (batch.length === 0) break;
    let batchRows = 0;
    let batchBytes = 0;
    const nextKey = batch.at(-1)!.id;
    db.transaction(() => {
      for (const candidate of batch) {
        const row = selectBlob.get(candidate.id);
        if (!row) continue;
        const migrated = splitLegacyBlob(row);
        const result = update.run(migrated.name, migrated.data, row.id) as { changes: number };
        if (result.changes > 0) {
          batchRows++;
          batchBytes += candidate.bytes;
        }
      }
      progress = {
        ...progress,
        last_key: nextKey,
        processed_rows: progress.processed_rows + batchRows,
        processed_bytes: progress.processed_bytes + batchBytes,
      };
      saveProgress.run(
        progress.last_key,
        progress.processed_rows,
        progress.processed_bytes,
        Date.now(),
        version,
        phase
      );
    })();
    onProgress?.({
      version,
      phase,
      table,
      processedRows: progress.processed_rows,
      processedBytes: progress.processed_bytes,
      totalRows: progress.total_rows,
      batchRows,
      batchBytes,
      elapsedMs: Date.now() - progress.started_at,
      complete: false,
    });
  }

  const remaining = countRemaining.get()?.count ?? 0;
  if (remaining > 0) {
    throw new Error(`${remaining} ${table} rows remain after the ${phase} migration`);
  }
  const completed = {
    version,
    phase,
    table,
    processedRows: progress.processed_rows,
    processedBytes: progress.processed_bytes,
    totalRows: progress.total_rows,
    batchRows: 0,
    batchBytes: 0,
    elapsedMs: Date.now() - progress.started_at,
    complete: true,
  } satisfies LegacyNameMigrationProgress;
  onProgress?.(completed);
  return completed;
}

/** Backfill pre-v31 job payload envelopes after adding jobs.name. */
export function migrateLegacyJobNames(
  db: Database,
  onProgress?: (progress: LegacyNameMigrationProgress) => void
): LegacyNameMigrationProgress {
  return migrateLegacyNames(db, {
    version: 31,
    phase: 'job-names',
    table: 'jobs',
    idColumn: 'id',
    nameColumn: 'name',
    onProgress,
  });
}

/** Backfill pre-v32 cron payload envelopes after adding cron_jobs.job_name. */
export function migrateLegacyCronJobNames(
  db: Database,
  onProgress?: (progress: LegacyNameMigrationProgress) => void
): LegacyNameMigrationProgress {
  return migrateLegacyNames(db, {
    version: 32,
    phase: 'cron-job-names',
    table: 'cron_jobs',
    idColumn: 'name',
    nameColumn: 'job_name',
    onProgress,
  });
}
