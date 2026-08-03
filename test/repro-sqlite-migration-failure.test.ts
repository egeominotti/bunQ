import { describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import { MIGRATION_TABLE, SCHEMA, SCHEMA_VERSION } from '../src/infrastructure/persistence/schema';

interface MigrationResult {
  firstError: string | null;
  versionAfterFailure: number;
  secondError: string | null;
  columns: string[];
  versionAfterRetry: number;
}

describe('SQLite migration failure recovery', () => {
  test('a failed migration is not recorded and is retried on reopen', async () => {
    const databasePath = join(tmpdir(), `bunqueue-migration-failure-${crypto.randomUUID()}.db`);
    const fixture = join(import.meta.dir, 'fixtures', 'sqlite-migration-failure.ts');
    try {
      const child = Bun.spawn([process.execPath, fixture, databasePath], {
        stdout: 'pipe',
        stderr: 'pipe',
        env: { ...process.env, BUNQUEUE_EMBEDDED: '1' },
      });
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      expect(exitCode, stderr).toBe(0);
      const line = stdout
        .trim()
        .split('\n')
        .findLast((entry) => entry.startsWith('{'));
      expect(line).toBeDefined();
      const result = JSON.parse(line!) as MigrationResult;

      expect(result.firstError).not.toBeNull();
      expect(result.firstError ?? '').toContain('migration_failure_injection');
      expect(result.versionAfterFailure).toBe(33);
      expect(result.secondError).toBeNull();
      expect(result.columns).toContain('extended_options');
      expect(result.versionAfterRetry).toBe(SCHEMA_VERSION);
    } finally {
      for (const suffix of ['', '-wal', '-shm']) {
        const file = databasePath + suffix;
        if (existsSync(file)) unlinkSync(file);
      }
    }
  });

  test('an already-applied column is accepted and the version is recorded', () => {
    const databasePath = join(tmpdir(), `bunqueue-migration-idempotent-${crypto.randomUUID()}.db`);
    try {
      const legacy = new Database(databasePath, { create: true });
      legacy.run(SCHEMA);
      legacy.run(MIGRATION_TABLE);
      legacy.run('INSERT INTO migrations(version, applied_at) VALUES (33, 0)');
      legacy.close();

      new SqliteStorage({ path: databasePath }).close();
      const migrated = new Database(databasePath, { readonly: true });
      const version =
        migrated
          .query<{ version: number }, []>('SELECT MAX(version) AS version FROM migrations')
          .get()?.version ?? 0;
      migrated.close();
      expect(version).toBe(SCHEMA_VERSION);
    } finally {
      for (const suffix of ['', '-wal', '-shm']) {
        const file = databasePath + suffix;
        if (existsSync(file)) unlinkSync(file);
      }
    }
  });

  test('a partially applied multi-statement migration resumes at its missing column', () => {
    const databasePath = join(tmpdir(), `bunqueue-migration-partial-v6-${crypto.randomUUID()}.db`);
    try {
      const legacy = new Database(databasePath, { create: true });
      legacy.run(SCHEMA.replace(',\n    dedup BLOB', ''));
      legacy.run(MIGRATION_TABLE);
      legacy.run('INSERT INTO migrations(version, applied_at) VALUES (5, 0)');
      legacy.close();

      new SqliteStorage({ path: databasePath }).close();
      const migrated = new Database(databasePath, { readonly: true });
      const columns = migrated
        .query<{ name: string }, []>('PRAGMA table_info(cron_jobs)')
        .all()
        .map((row) => row.name);
      const version =
        migrated
          .query<{ version: number }, []>('SELECT MAX(version) AS version FROM migrations')
          .get()?.version ?? 0;
      migrated.close();
      expect(columns).toContain('unique_key');
      expect(columns).toContain('dedup');
      expect(version).toBe(SCHEMA_VERSION);
    } finally {
      for (const suffix of ['', '-wal', '-shm']) {
        const file = databasePath + suffix;
        if (existsSync(file)) unlinkSync(file);
      }
    }
  });
});
