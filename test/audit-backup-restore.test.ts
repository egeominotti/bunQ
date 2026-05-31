/**
 * AUDIT — CLAIM M3: Non-atomic restore clobbers the live database.
 *
 * src/infrastructure/backup/s3BackupOperations.ts (restoreBackup, ~320-324):
 *
 *     // Write to database path
 *     await Bun.write(config.databasePath, data);
 *
 *     // Verify restored database integrity
 *     await verifyDatabaseIntegrity(config.databasePath);
 *
 * The integrity check (PRAGMA integrity_check) runs AFTER the data is written
 * directly onto the live DB path. There is no temp-file-then-atomic-rename.
 * So a backup that survives the pre-write guards (checksum matches + valid
 * SQLite header) but is structurally corrupt will overwrite the live DB BEFORE
 * the integrity check fails -> the original is clobbered even though restore
 * returns success:false.
 *
 * CORRECT/SAFE behavior asserted here: when the downloaded backup fails
 * validation, the EXISTING live database file is left UNCHANGED (restore writes
 * to a temp file and only swaps on success).
 *
 * If the original DB content survives a failed restore -> M3 REFUTED (GREEN).
 * If the original DB is overwritten/corrupted -> M3 CONFIRMED (RED).
 *
 * TEST ONLY — does not modify src/.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Database } from 'bun:sqlite';
import { unlink } from 'fs/promises';
import type { S3BackupConfig, BackupMetadata } from '../src/infrastructure/backup/s3BackupConfig';
import { DEFAULTS } from '../src/infrastructure/backup/s3BackupConfig';
import { restoreBackup } from '../src/infrastructure/backup/s3BackupOperations';
import type { S3Client } from 'bun';

// --- Minimal in-memory mock S3 client (mirrors test/s3Backup.test.ts) ---
function createMockS3Client() {
  const storage = new Map<string, Uint8Array>();
  const client = {
    file(key: string) {
      return {
        async write(data: Uint8Array | string, _opts?: { type?: string }) {
          storage.set(
            key,
            typeof data === 'string' ? new TextEncoder().encode(data) : new Uint8Array(data)
          );
        },
        async exists() {
          return storage.has(key);
        },
        async arrayBuffer() {
          const d = storage.get(key);
          if (!d) throw new Error(`Not found: ${key}`);
          return d.buffer.slice(d.byteOffset, d.byteOffset + d.byteLength);
        },
        async json() {
          const d = storage.get(key);
          if (!d) throw new Error(`Not found: ${key}`);
          return JSON.parse(new TextDecoder().decode(d));
        },
      };
    },
    async delete(key: string) {
      storage.delete(key);
    },
    async list(opts: { prefix?: string; maxKeys?: number }) {
      const contents: { key: string; size: number; lastModified: Date }[] = [];
      for (const [k, v] of storage) {
        if (opts.prefix && !k.startsWith(opts.prefix)) continue;
        contents.push({ key: k, size: v.byteLength, lastModified: new Date() });
      }
      return { contents };
    },
    _storage: storage,
  };
  return client as unknown as S3Client & { _storage: Map<string, Uint8Array> };
}

const TMP = Bun.env.TMPDIR ?? '/tmp';
let tempFiles: string[] = [];

function tmpPath(name: string): string {
  const p = `${TMP}/bunqueue-m3-${name}-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
  tempFiles.push(p);
  return p;
}

function cfg(overrides: Partial<S3BackupConfig> = {}): S3BackupConfig {
  return {
    enabled: true,
    accessKeyId: 'test-key',
    secretAccessKey: 'test-secret',
    bucket: 'test-bucket',
    region: 'us-east-1',
    intervalMs: DEFAULTS.intervalMs,
    retention: DEFAULTS.retention,
    prefix: DEFAULTS.prefix,
    databasePath: '/tmp/test.db',
    ...overrides,
  };
}

function sha256(data: Uint8Array): string {
  const h = new Bun.CryptoHasher('sha256');
  h.update(data);
  return h.digest('hex');
}

async function gzip(data: Uint8Array): Promise<Uint8Array> {
  const stream = new Blob([data as unknown as BlobPart])
    .stream()
    .pipeThrough(new CompressionStream('gzip'));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

/**
 * Build a payload that:
 *  - begins with a valid "SQLite format 3\0" header (passes the header guard)
 *  - is structurally CORRUPT (fails PRAGMA integrity_check)
 * Built from a real SQLite file with mangled page contents so the SQLite header
 * stays intact but the b-tree pages are garbage.
 */
async function buildCorruptButHeaderValidPayload(): Promise<Uint8Array> {
  const seed = tmpPath('seed');
  const db = new Database(seed);
  db.exec('PRAGMA journal_mode=DELETE'); // single-file, no WAL sidecar
  db.run('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)');
  for (let i = 0; i < 50; i++) db.run('INSERT INTO t VALUES (?, ?)', [i, `row-${i}`]);
  db.close();

  const bytes = new Uint8Array(await Bun.file(seed).arrayBuffer());
  // Header is the first 100 bytes of a SQLite file. Leave the magic string +
  // header intact, but corrupt the b-tree page data so integrity_check fails.
  // Overwrite a swath well past the header with garbage.
  for (let i = 200; i < bytes.length; i++) {
    bytes[i] = (bytes[i] + 0x5a + i) & 0xff;
  }
  return bytes;
}

describe('AUDIT M3: restore must be atomic (live DB unchanged on failed restore)', () => {
  afterEach(async () => {
    for (const f of tempFiles) {
      try { await unlink(f); } catch { /* ok */ }
      try { await unlink(f + '-wal'); } catch { /* ok */ }
      try { await unlink(f + '-shm'); } catch { /* ok */ }
    }
    tempFiles = [];
  });

  test('failed restore (integrity check) leaves existing live database UNCHANGED', async () => {
    const client = createMockS3Client();

    // 1) Create a VALID live database with known content.
    const dbPath = tmpPath('live');
    const live = new Database(dbPath);
    live.run('CREATE TABLE original (id INTEGER PRIMARY KEY, data TEXT)');
    live.run("INSERT INTO original VALUES (1, 'must-survive')");
    live.close();

    const beforeBytes = new Uint8Array(await Bun.file(dbPath).arrayBuffer());
    const beforeChecksum = sha256(beforeBytes);

    // 2) Stage a backup in S3 that passes BOTH pre-write guards
    //    (checksum matches the payload + valid SQLite header) but is
    //    structurally corrupt, so PRAGMA integrity_check (run AFTER the write)
    //    fails.
    const corruptPayload = await buildCorruptButHeaderValidPayload();

    // sanity: header guard would pass
    const header = new TextDecoder().decode(corruptPayload.slice(0, 16));
    expect(header.startsWith('SQLite format 3')).toBe(true);

    const key = `${DEFAULTS.prefix}bunqueue-corrupt.db`;
    await (client.file(key) as any).write(await gzip(corruptPayload));

    // metadata checksum MATCHES the payload -> checksum guard passes
    const meta: BackupMetadata = {
      timestamp: new Date().toISOString(),
      version: 'test',
      size: corruptPayload.byteLength,
      compressedSize: 0,
      checksum: sha256(corruptPayload),
      compressed: true,
    };
    await (client.file(`${key}.meta.json`) as any).write(JSON.stringify(meta));

    // 3) Attempt restore onto the live DB path.
    const result = await restoreBackup(key, cfg({ databasePath: dbPath }), client);

    // Restore must NOT succeed (payload is corrupt).
    expect(result.success).toBe(false);

    // 4) THE CORE ASSERTION: the original live DB file must be intact.
    //    (Atomic restore = write to temp, swap only on success.)
    //    Note: verifyDatabaseIntegrity unlinks the file it just wrote on
    //    failure; if the live path was the write target, the original is gone.
    const stillExists = await Bun.file(dbPath).exists();
    expect(stillExists).toBe(true); // original must not be deleted

    const afterBytes = new Uint8Array(await Bun.file(dbPath).arrayBuffer());
    const afterChecksum = sha256(afterBytes);

    // Original bytes must be byte-for-byte identical.
    expect(afterChecksum).toBe(beforeChecksum);

    // And it must still be a readable DB with the original row.
    const reopened = new Database(dbPath);
    const row = reopened.query('SELECT data FROM original WHERE id = 1').get() as
      | { data: string }
      | null;
    reopened.close();
    expect(row?.data).toBe('must-survive');
  });
});
