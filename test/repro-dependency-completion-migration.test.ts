import { expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, unlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';

test('schema 28 completion evidence gains conservative pin ownership', () => {
  const path = join(tmpdir(), `bunqueue-completion-v29-${crypto.randomUUID()}.db`);
  const legacy = new Database(path, { create: true });
  legacy.run(`
    CREATE TABLE migrations (
      version INTEGER PRIMARY KEY,
      applied_at INTEGER NOT NULL
    );
    CREATE TABLE dependency_completions (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id TEXT NOT NULL UNIQUE,
      queue TEXT NOT NULL,
      completed_at INTEGER NOT NULL
    );
    INSERT INTO dependency_completions(job_id, queue, completed_at)
      VALUES ('legacy-proof', 'legacy-source', 123);
    INSERT INTO migrations(version, applied_at) VALUES (28, 0);
  `);
  legacy.close();

  const storage = new SqliteStorage({ path });
  storage.close();
  const migrated = new Database(path, { readonly: true });
  try {
    const row = migrated
      .query<{ pinned: number }, []>(
        "SELECT pinned FROM dependency_completions WHERE job_id = 'legacy-proof'"
      )
      .get();
    const version = migrated
      .query<{ version: number }, []>('SELECT MAX(version) AS version FROM migrations')
      .get();
    expect(row?.pinned).toBe(0);
    expect(version?.version).toBe(29);
  } finally {
    migrated.close();
    for (const suffix of ['', '-wal', '-shm']) {
      const file = `${path}${suffix}`;
      if (existsSync(file)) unlinkSync(file);
    }
  }
});
