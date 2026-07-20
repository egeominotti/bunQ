/**
 * SQLite snapshot creation, validation, and restore file transitions.
 */

import { Database } from 'bun:sqlite';
import { rename, rm } from 'node:fs/promises';
import { backupLog } from '../../shared/logger';

const SIDECAR_SUFFIXES = ['-wal', '-shm', '-journal'] as const;

function uniqueSiblingPath(databasePath: string, purpose: string): string {
  return `${databasePath}.${purpose}-${Date.now()}-${crypto.randomUUID()}.tmp`;
}

function quoteSqliteString(value: string): string {
  return value.replaceAll("'", "''");
}

async function removeIfPresent(path: string): Promise<void> {
  await rm(path, { force: true });
}

/** Remove a database candidate and any sidecars SQLite may have created for it. */
export async function removeDatabaseArtifacts(databasePath: string): Promise<void> {
  await Promise.all([
    removeIfPresent(databasePath),
    ...SIDECAR_SUFFIXES.map((suffix) => removeIfPresent(`${databasePath}${suffix}`)),
  ]);
}

/** Validate all SQLite database pages. */
export function verifyDatabaseIntegrity(databasePath: string): void {
  let db: Database | null = null;
  try {
    db = new Database(databasePath, { readonly: true });
    const result = db.query('PRAGMA integrity_check').get() as { integrity_check: string } | null;
    const status = result?.integrity_check ?? '';
    if (status !== 'ok') {
      throw new Error(`Database integrity check failed: ${status || 'unknown error'}`);
    }
  } catch (error) {
    if (error instanceof Error && error.message.includes('integrity check failed')) {
      throw error;
    }
    throw new Error('Database integrity check failed: corrupt or invalid database', {
      cause: error,
    });
  } finally {
    try {
      db?.close();
    } catch {
      // The failed open or check may already have closed the handle.
    }
  }
}

/**
 * Produce a transactionally consistent, standalone SQLite snapshot.
 *
 * VACUUM INTO reads through SQLite rather than copying the live main file, so
 * committed WAL frames are included even when an older reader blocks a WAL
 * checkpoint.
 */
export async function createConsistentSnapshot(databasePath: string): Promise<string> {
  const snapshotPath = uniqueSiblingPath(databasePath, 'backup-snapshot');
  let db: Database | null = null;
  try {
    db = new Database(databasePath, { readonly: true });
    db.exec('PRAGMA busy_timeout = 30000');
    db.exec(`VACUUM INTO '${quoteSqliteString(snapshotPath)}'`);
    db.close();
    db = null;
    verifyDatabaseIntegrity(snapshotPath);
    return snapshotPath;
  } catch (error) {
    try {
      db?.close();
    } catch {
      // The failed snapshot may already have closed the handle.
    }
    await removeDatabaseArtifacts(snapshotPath);
    throw error;
  }
}

/**
 * Install an already validated candidate while quarantining stale sidecars.
 *
 * The live main database is not touched until all sidecars have moved. If a
 * pre-swap operation fails, moved sidecars are restored before returning.
 */
export async function installDatabaseCandidate(
  candidatePath: string,
  databasePath: string
): Promise<void> {
  const quarantineId = `${Date.now()}-${crypto.randomUUID()}`;
  const movedSidecars: Array<{ live: string; quarantine: string }> = [];

  try {
    for (const suffix of SIDECAR_SUFFIXES) {
      const live = `${databasePath}${suffix}`;
      if (!(await Bun.file(live).exists())) {
        continue;
      }
      const quarantine = `${live}.restore-quarantine-${quarantineId}`;
      await rename(live, quarantine);
      movedSidecars.push({ live, quarantine });
    }

    await rename(candidatePath, databasePath);
  } catch (error) {
    const rollbackErrors: string[] = [];
    for (const moved of movedSidecars.reverse()) {
      try {
        await rename(moved.quarantine, moved.live);
      } catch (rollbackError) {
        rollbackErrors.push(String(rollbackError));
      }
    }
    if (rollbackErrors.length > 0) {
      throw new Error(`Restore failed and sidecar rollback failed: ${rollbackErrors.join('; ')}`, {
        cause: error,
      });
    }
    throw error;
  }

  const cleanup = await Promise.allSettled([
    ...movedSidecars.map(({ quarantine }) => removeIfPresent(quarantine)),
    ...SIDECAR_SUFFIXES.map((suffix) => removeIfPresent(`${candidatePath}${suffix}`)),
  ]);
  const failures = cleanup.filter(
    (result): result is PromiseRejectedResult => result.status === 'rejected'
  );
  if (failures.length > 0) {
    backupLog.warn('Restore committed but quarantine cleanup was incomplete', {
      errors: failures.map(({ reason }) => String(reason)),
    });
  }
}
