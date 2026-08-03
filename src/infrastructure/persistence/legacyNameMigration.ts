import type { Database } from 'bun:sqlite';
import { normalizeLegacyJobPayload } from '../../domain/job/payload';
import { decodeMessagePack, encodeMessagePack } from '../../shared/msgpack';

interface LegacyNameRow {
  id: string;
  data: Uint8Array;
}

const MIGRATION_BATCH_SIZE = 500;

function splitLegacyBlob(row: LegacyNameRow): {
  name: string;
  data: Uint8Array;
} {
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

function migrateLegacyNames(
  db: Database,
  table: 'jobs' | 'cron_jobs',
  idColumn: 'id' | 'name',
  nameColumn: 'name' | 'job_name'
): void {
  const select = db.query<LegacyNameRow, []>(
    `SELECT ${idColumn} AS id, data FROM ${table} WHERE ${nameColumn} IS NULL LIMIT ${MIGRATION_BATCH_SIZE}`
  );
  const update = db.prepare(
    `UPDATE ${table} SET ${nameColumn} = ?, data = ? WHERE ${idColumn} = ? AND ${nameColumn} IS NULL`
  );
  const applyBatch = db.transaction((rows: LegacyNameRow[]) => {
    for (const row of rows) {
      const migrated = splitLegacyBlob(row);
      update.run(migrated.name, migrated.data, row.id);
    }
  });

  while (true) {
    const rows = select.all();
    if (rows.length === 0) return;
    applyBatch(rows);
  }
}

/** Backfill pre-v31 job payload envelopes after adding jobs.name. */
export function migrateLegacyJobNames(db: Database): void {
  migrateLegacyNames(db, 'jobs', 'id', 'name');
}

/** Backfill pre-v32 cron payload envelopes after adding cron_jobs.job_name. */
export function migrateLegacyCronJobNames(db: Database): void {
  migrateLegacyNames(db, 'cron_jobs', 'name', 'job_name');
}
