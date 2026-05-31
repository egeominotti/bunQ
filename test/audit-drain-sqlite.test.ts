/**
 * AUDIT TEST: queue.drain() must delete jobs from SQLite too (embedded mode)
 *
 * BUG (suspected): In embedded mode, `queue.drain()` removes jobs from the
 * in-memory jobIndex and zeroes the queue counts, but does NOT delete the rows
 * from SQLite (unlike obliterate/clean which call storage.deleteJob).
 *
 * As a result, after drain() the drained jobs "resurrect" from the stale SQLite
 * row:
 *   - getJobState(id) returns 'waiting'   (resolveStateFromStorage fallback)
 *   - getJob(id) returns the job          (storage.getJob fallback)
 *   - getWaiting() is non-empty           (queryJobs reads SQLite when storage set)
 *
 * Root cause:
 *   - src/application/operations/queueControl.ts drainQueue (~43-51): deletes
 *     only from jobIndex, never calls storage.deleteJob.
 *   - src/application/operations/queryOperations.ts getJobState (~154-155):
 *     falls back to resolveStateFromStorage which reads the stale SQLite row.
 *
 * This test asserts the CORRECT behavior, so it FAILS (RED) on the current
 * buggy code and would PASS (GREEN) once drain also deletes the SQLite rows.
 *
 * IMPORTANT: uses a FILE-backed SQLite db (NOT :memory:) so the persistence is
 * real — the bug is specifically about stale rows surviving in the on-disk db.
 */

import { describe, test, expect, afterEach, beforeAll } from 'bun:test';
import { existsSync, rmSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Queue, shutdownManager } from '../src/client';

// Unique file-backed db under /tmp so SQLite persistence is real (not :memory:).
const DB_DIR = mkdtempSync(join(tmpdir(), 'bunq-drain-'));
const DB_PATH = join(DB_DIR, 'drain.db');

function cleanupDbFiles(): void {
  for (const suffix of ['', '-wal', '-shm']) {
    const f = DB_PATH + suffix;
    if (existsSync(f)) {
      try {
        rmSync(f);
      } catch {
        /* ignore */
      }
    }
  }
  try {
    rmSync(DB_DIR, { recursive: true, force: true });
  } catch {
    /* ignore */
  }
}

describe('AUDIT: drain() must delete rows from SQLite (embedded)', () => {
  beforeAll(() => {
    // Ensure a fresh manager singleton bound to the FILE db.
    shutdownManager();
    Bun.env.BUNQUEUE_DATA_PATH = DB_PATH;
  });

  afterEach(() => {
    shutdownManager();
  });

  test('drain() removes jobs everywhere including SQLite (no resurrection)', async () => {
    const queue = new Queue('drain-sqlite-audit', {
      embedded: true,
      dataPath: DB_PATH,
    });

    // Clean slate.
    queue.obliterate();

    // Add 3 jobs, NO worker consuming them — they sit in 'waiting'.
    const j1 = await queue.add('task', { n: 1 });
    const j2 = await queue.add('task', { n: 2 });
    const j3 = await queue.add('task', { n: 3 });
    const ids = [j1.id, j2.id, j3.id];

    // Force the write buffer to flush so the rows really hit the file db.
    // durable add bypasses the 10ms write buffer; if drain only cleared the
    // in-memory state this guarantees the SQLite rows exist to resurrect from.
    const j4 = await queue.add('task', { n: 4 }, { durable: true });
    ids.push(j4.id);

    // Give the write buffer a moment to flush the buffered (non-durable) jobs.
    await Bun.sleep(50);

    // Sanity: before drain they are all visible as waiting.
    const beforeCounts = queue.getJobCounts();
    expect(beforeCounts.waiting).toBe(4);
    for (const id of ids) {
      expect(await queue.getJobState(id)).toBe('waiting');
    }

    // ---- ACT ----
    queue.drain(); // sync/void in embedded mode

    // Poll a couple of times to be deterministic (let any async settle).
    let counts = queue.getJobCounts();
    let waiting = await queue.getWaitingAsync(0, 100);
    let states = await Promise.all(ids.map((id) => queue.getJobState(id)));
    let jobs = await Promise.all(ids.map((id) => queue.getJob(id)));

    for (let attempt = 0; attempt < 3; attempt++) {
      const stillThere =
        counts.waiting !== 0 ||
        waiting.length !== 0 ||
        states.some((s) => s === 'waiting') ||
        jobs.some((j) => j !== null);
      if (!stillThere) break;
      await Bun.sleep(60);
      counts = queue.getJobCounts();
      waiting = await queue.getWaitingAsync(0, 100);
      states = await Promise.all(ids.map((id) => queue.getJobState(id)));
      jobs = await Promise.all(ids.map((id) => queue.getJob(id)));
    }

    // ---- ASSERT CORRECT BEHAVIOR ----
    // 1. Counts: drain zeroes waiting (in-memory). This already passes today.
    expect(counts.waiting).toBe(0);

    // 2. getWaiting must be empty — currently RESURRECTS from SQLite rows.
    expect(waiting.length).toBe(0);

    // 3. getJobState must NOT report 'waiting' — currently resurrects to 'waiting'.
    for (let i = 0; i < ids.length; i++) {
      expect(states[i]).not.toBe('waiting');
    }

    // 4. getJob must be null for every drained job — currently returns the job.
    for (let i = 0; i < ids.length; i++) {
      expect(jobs[i]).toBeNull();
    }

    queue.close();
  });
});

// Final filesystem cleanup on process exit.
process.on('exit', cleanupDbFiles);
