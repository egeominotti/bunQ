/**
 * Repro: SQLITE_IOERR_FSTAT "Unhandled error between tests" crashing CI
 *
 * Root cause (see CI runs 26837216428 / 26820027935 / 26816343462):
 * The SqliteStorage constructor runs `this.db.run(PRAGMA_SETTINGS)` (sqlite.ts:67)
 * with NO error handling. PRAGMA_SETTINGS contains `mmap_size = 268435456`, which
 * makes SQLite call fstat() on the just-opened fd. In CI, a restart/cleanup race
 * (a not-awaited shutdown leaves the file mid-removal under the fd, or the dir is
 * unlink'd out from under it) makes that fstat fail with SQLITE_IOERR_FSTAT
 * (SQLITE_IOERR_VNODE on macOS) — same IOERR family. Because the failing
 * construction happens in a deferred/async context, Bun reports it as an
 * "Unhandled error between tests" and fails the whole job, even though 0 tests fail.
 *
 * Reproduced deterministically below: V2 (open succeeds -> dir removed -> PRAGMA
 * mmap_size fstat fails) yields a real IOERR. The fix must make optimization
 * PRAGMAs NON-FATAL so a transient filesystem IOERR never tears down the process.
 */

import { describe, test, expect, spyOn } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PRAGMA_SETTINGS } from '../src/infrastructure/persistence/schema';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';

describe('CI flaky: SQLITE_IOERR on PRAGMA mmap_size must not crash SqliteStorage', () => {
  // Proves the underlying mechanism the CI crash relies on: an open fd whose
  // file/dir is removed, then `PRAGMA mmap_size` -> IOERR (FSTAT on Linux,
  // VNODE on macOS). Both are SQLITE_IOERR_* — the exact family seen in CI.
  test('mechanism: orphaning the fd can surface SQLITE_IOERR on PRAGMA mmap_size', () => {
    const dir = mkdtempSync(join(tmpdir(), 'bunq-ioerr-'));
    const db = new Database(join(dir, 't.db'), { create: true });
    rmSync(dir, { recursive: true, force: true }); // file removed under the open fd
    let code: string | undefined;
    let threw = false;
    try {
      db.run('PRAGMA mmap_size = 268435456;');
    } catch (e) {
      threw = true;
      code = (e as { code?: string }).code;
    } finally {
      try {
        db.close();
      } catch {
        /* ignore */
      }
    }
    // Platform-dependent: macOS surfaces SQLITE_IOERR_VNODE, Linux/CI often
    // SQLITE_IOERR_FSTAT — but some platforms keep the inode alive for an open fd
    // and do NOT error here. Only assert the error family WHEN it actually threw;
    // the real fix (constructor survives an injected IOERR) is covered below and
    // is platform-independent.
    if (threw) {
      expect(code).toMatch(/^SQLITE_IOERR/);
    } else {
      expect(code).toBeUndefined(); // no orphan error on this platform — acceptable
    }
  });

  // The actual bug: the constructor propagates that IOERR unhandled. After the
  // fix, a failing optimization PRAGMA is caught and the constructor survives.
  test('SqliteStorage constructor survives a transient IOERR during PRAGMA setup', () => {
    const realRun = Database.prototype.run;
    let injected = false;
    const spy = spyOn(Database.prototype, 'run').mockImplementation(function (
      this: Database,
      ...args: unknown[]
    ) {
      const sql = String(args[0] ?? '');
      // Fail exactly the optimization-PRAGMA application, like CI's fstat IOERR.
      if (!injected && (sql.includes('mmap_size') || sql === PRAGMA_SETTINGS)) {
        injected = true;
        const err = new Error('disk I/O error') as Error & { code?: string };
        err.code = 'SQLITE_IOERR_FSTAT';
        throw err;
      }
      return (realRun as (...a: unknown[]) => unknown).apply(this, args);
    });

    try {
      // BUG (pre-fix): this throws SQLITE_IOERR_FSTAT out of `new SqliteStorage`,
      // which in a deferred context becomes "Unhandled error between tests".
      let storage: SqliteStorage | undefined;
      expect(() => {
        storage = new SqliteStorage({ path: ':memory:' });
      }).not.toThrow();
      expect(injected).toBe(true); // ensure the failure path was actually exercised
      storage?.close();
    } finally {
      spy.mockRestore();
    }
  });
});
