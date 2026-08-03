import { Database } from 'bun:sqlite';
import { MIGRATIONS } from '../../src/infrastructure/persistence/migrations';
import { SqliteStorage } from '../../src/infrastructure/persistence/sqlite';
import { MIGRATION_TABLE, SCHEMA } from '../../src/infrastructure/persistence/schema';

const databasePath = process.argv[2];
if (!databasePath) throw new Error('Database path is required');

const legacy = new Database(databasePath, { create: true });
legacy.run(SCHEMA.replace(',\n    extended_options BLOB\n', '\n'));
legacy.run(MIGRATION_TABLE);
legacy.run('INSERT INTO migrations(version, applied_at) VALUES (33, 0)');
legacy.close();

const migration = MIGRATIONS[34];
MIGRATIONS[34] = 'SELECT * FROM migration_failure_injection;';
let firstError: string | null = null;
try {
  new SqliteStorage({ path: databasePath }).close();
} catch (error) {
  firstError = error instanceof Error ? error.message : String(error);
} finally {
  MIGRATIONS[34] = migration;
}

const afterFailure = new Database(databasePath);
const versionAfterFailure =
  afterFailure
    .query<{ version: number }, []>('SELECT MAX(version) AS version FROM migrations')
    .get()?.version ?? 0;
afterFailure.close();

let secondError: string | null = null;
try {
  new SqliteStorage({ path: databasePath }).close();
} catch (error) {
  secondError = error instanceof Error ? error.message : String(error);
}

const afterRetry = new Database(databasePath, { readonly: true });
const columns = afterRetry
  .query<{ name: string }, []>('PRAGMA table_info(jobs)')
  .all()
  .map((row) => row.name);
const versionAfterRetry =
  afterRetry.query<{ version: number }, []>('SELECT MAX(version) AS version FROM migrations').get()
    ?.version ?? 0;
afterRetry.close();

console.log(
  JSON.stringify({ firstError, versionAfterFailure, secondError, columns, versionAfterRetry })
);
