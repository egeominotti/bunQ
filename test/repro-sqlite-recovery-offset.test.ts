/**
 * Regression coverage for startup recovery pagination over mutating SQLite sets.
 */

import { describe, expect, test } from 'bun:test';
import type { Database } from 'bun:sqlite';
import { randomUUID } from 'node:crypto';
import { unlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import { pack } from '../src/infrastructure/persistence/sqliteSerializer';

const RECOVERY_BATCH_SIZE = 10_000;
const ACTIVE_JOB_COUNT = RECOVERY_BATCH_SIZE + 1;
const ACTIVE_QUEUE = 'recovery-offset';
const CORRUPT_PENDING_QUEUE = 'recovery-offset-corrupt-pending';
// map16 header claiming five entries, truncated before the first entry.
const CORRUPT_DEPENDS_ON = new Uint8Array([0xde, 0x00, 0x05]);

function removeDbFiles(dbPath: string): void {
  for (const suffix of ['', '-wal', '-shm']) {
    try {
      unlinkSync(dbPath + suffix);
    } catch {
      // File may not exist if SQLite did not create a WAL sidecar.
    }
  }
}

/** Seed active rows in one transaction so boundary tests remain fast and deterministic. */
function seedActiveJobs(dbPath: string, count: number): void {
  const storage = new SqliteStorage({ path: dbPath });
  const db = (storage as unknown as { db: Database }).db;
  const now = Date.now();
  const data = pack({ source: 'recovery-offset-regression' });
  const insert = db.prepare(`
    INSERT INTO jobs (
      id, queue, data, priority, created_at, run_at, started_at,
      attempts, max_attempts, backoff, state
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertAll = db.transaction(() => {
    for (let i = 0; i < count; i++) {
      insert.run(
        `active-${String(i).padStart(5, '0')}`,
        ACTIVE_QUEUE,
        data,
        0,
        now + i,
        now,
        now,
        0,
        3,
        1_000,
        'active'
      );
    }
  });

  insertAll();
  storage.close();
}

/** Seed one corrupt pending row first, followed by healthy rows in stable runAt order. */
function seedPendingJobsWithFirstCorrupt(dbPath: string, count: number): void {
  const storage = new SqliteStorage({ path: dbPath });
  const db = (storage as unknown as { db: Database }).db;
  const now = Date.now();
  const data = pack({ source: 'pending-recovery-offset-regression' });
  const insert = db.prepare(`
    INSERT INTO jobs (
      id, queue, data, priority, created_at, run_at, attempts,
      max_attempts, backoff, state, depends_on
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertAll = db.transaction(() => {
    for (let i = 0; i < count; i++) {
      insert.run(
        `pending-${String(i).padStart(5, '0')}`,
        CORRUPT_PENDING_QUEUE,
        data,
        0,
        now + i,
        now + i,
        0,
        3,
        1_000,
        'waiting',
        i === 0 ? CORRUPT_DEPENDS_ON : null
      );
    }
  });

  insertAll();
  storage.close();
}

function readyCount(manager: QueueManager, queue: string): number {
  const counts = manager.getQueueJobCounts(queue);
  return counts.waiting + counts.prioritized + counts.delayed;
}

describe('SQLite startup recovery pagination', () => {
  for (const activeCount of [ACTIVE_JOB_COUNT, RECOVERY_BATCH_SIZE * 2]) {
    // Seeding + recovering 20k rows takes >5s (the default per-test budget) on
    // slow CI runners; the 10k case alone measured ~2.6s there.
    test(`recovers all ${activeCount.toLocaleString('en-US')} active jobs exactly once`, () => {
      const dbPath = join(tmpdir(), `bunqueue-recovery-offset-${randomUUID()}.db`);
      let manager: QueueManager | null = null;

      try {
        seedActiveJobs(dbPath, activeCount);

        // QueueManager construction runs the real startup recovery against SQLite.
        manager = new QueueManager({ dataPath: dbPath });
        const storage = (manager as unknown as { storage: SqliteStorage }).storage;
        const memory = manager.getMemoryStats();

        // Every interrupted active job is a normal retry here. Besides leaving no
        // active rows, Phase 1 + Phase 2 must produce exactly one logical queue
        // entry and one queued-counter increment per persisted job.
        expect({
          active: storage.countActiveJobs(),
          pending: storage.countPendingJobs(),
          ready: readyCount(manager, ACTIVE_QUEUE),
          indexed: memory.jobIndex,
          queuedCounter: memory.queuedTotal,
        }).toEqual({
          active: 0,
          pending: activeCount,
          ready: activeCount,
          indexed: activeCount,
          queuedCounter: activeCount,
        });
      } finally {
        manager?.shutdown();
        removeDbFiles(dbPath);
      }
    }, 20_000);
  }

  test('enqueues one recovered active job and increments delayed counters only once', () => {
    const dbPath = join(tmpdir(), `bunqueue-recovery-once-${randomUUID()}.db`);
    let manager: QueueManager | null = null;

    try {
      seedActiveJobs(dbPath, 1);
      manager = new QueueManager({ dataPath: dbPath });
      const memory = manager.getMemoryStats();

      expect({
        ready: readyCount(manager, ACTIVE_QUEUE),
        queuedCounter: memory.queuedTotal,
        delayedHeapEntries: memory.delayedHeapTotal,
      }).toEqual({
        ready: 1,
        queuedCounter: 1,
        delayedHeapEntries: 1,
      });
    } finally {
      manager?.shutdown();
      removeDbFiles(dbPath);
    }
  });

  test('a corrupt pending row cannot shift a healthy job past the next page boundary', () => {
    const dbPath = join(tmpdir(), `bunqueue-pending-offset-${randomUUID()}.db`);
    let manager: QueueManager | null = null;

    try {
      seedPendingJobsWithFirstCorrupt(dbPath, ACTIVE_JOB_COUNT);
      manager = new QueueManager({ dataPath: dbPath });
      const storage = (manager as unknown as { storage: SqliteStorage }).storage;
      const memory = manager.getMemoryStats();

      // One corrupt row is quarantined; every one of the remaining 10,000 rows
      // must still be represented in memory even though the scan crossed a page.
      expect({
        pending: storage.countPendingJobs(),
        ready: readyCount(manager, CORRUPT_PENDING_QUEUE),
        queuedCounter: memory.queuedTotal,
        indexedIncludingDlq: memory.jobIndex,
        dlq: manager.getDlqEntries(CORRUPT_PENDING_QUEUE).length,
      }).toEqual({
        pending: RECOVERY_BATCH_SIZE,
        ready: RECOVERY_BATCH_SIZE,
        queuedCounter: RECOVERY_BATCH_SIZE,
        indexedIncludingDlq: ACTIVE_JOB_COUNT,
        dlq: 1,
      });
    } finally {
      manager?.shutdown();
      removeDbFiles(dbPath);
    }
  });

  test('pending recovery pages use id as a deterministic final tie-breaker', () => {
    const dbPath = join(tmpdir(), `bunqueue-pending-order-${randomUUID()}.db`);
    const storage = new SqliteStorage({ path: dbPath });

    try {
      const db = (storage as unknown as { db: Database }).db;
      const now = Date.now();
      const data = pack({ source: 'pending-order-regression' });
      const insert = db.prepare(`
        INSERT INTO jobs (
          id, queue, data, priority, created_at, run_at, attempts,
          max_attempts, backoff, state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);

      // Reverse insertion order exposes reliance on SQLite rowid for rows whose
      // priority and run_at compare equal.
      for (const id of ['pending-c', 'pending-b', 'pending-a']) {
        insert.run(id, 'pending-order', data, 0, now, now, 0, 3, 1_000, 'waiting');
      }

      expect(storage.loadPendingJobs(10, 0).map((job) => job.id)).toEqual([
        'pending-a',
        'pending-b',
        'pending-c',
      ]);
    } finally {
      storage.close();
      removeDbFiles(dbPath);
    }
  });
});
