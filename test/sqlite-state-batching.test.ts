import { afterEach, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { createJob, jobId, type JobId, type JobTimelineEntry } from '../src/domain/types/job';
import { SqliteStorage, type SqliteConfig } from '../src/infrastructure/persistence/sqlite';

interface ActiveWrite {
  jobId: JobId;
  startedAt: number;
  timeline?: JobTimelineEntry[];
}

interface CompletedWrite {
  jobId: JobId;
  completedAt: number;
  timeline?: JobTimelineEntry[];
}

type BatchStorage = SqliteStorage & {
  markActiveBatch?: (updates: readonly ActiveWrite[]) => void;
  markCompletedBatch?: (updates: readonly CompletedWrite[]) => void;
};

const directories: string[] = [];

afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

function createStorage(config: Partial<SqliteConfig> = {}): {
  storage: SqliteStorage;
  db: Database;
} {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-state-batch-test-'));
  directories.push(directory);
  const storage = new SqliteStorage({ path: join(directory, 'queue.db'), ...config });
  return { storage, db: (storage as unknown as { db: Database }).db };
}

function stateSnapshot(db: Database): unknown {
  return db
    .query<
      {
        id: string;
        state: string;
        started_at: number | null;
        completed_at: number | null;
        progress: number;
        timeline: Uint8Array | null;
        dlq_retry_state: Uint8Array | null;
      },
      []
    >(
      `SELECT id, state, started_at, completed_at, progress, timeline, dlq_retry_state
       FROM jobs ORDER BY id`
    )
    .all()
    .map((row) => ({
      ...row,
      timeline: row.timeline ? Array.from(row.timeline) : null,
      dlq_retry_state: row.dlq_retry_state ? Array.from(row.dlq_retry_state) : null,
    }));
}

function insertIdenticalJobs(storage: SqliteStorage, ids: readonly JobId[], now: number): void {
  for (const id of ids) {
    storage.insertJobImmediate(createJob(id, 'state-batch', { data: { id } }, now));
  }
}

describe('SQLite lifecycle state batching', () => {
  test('matches scalar active and completed rows exactly', () => {
    const scalar = createStorage();
    const batched = createStorage();
    const ids = Array.from({ length: 6 }, (_, index) => jobId(`equivalence-${index}`));
    const createdAt = 1_800_000_000_000;
    const startedAt = createdAt + 100;
    const completedAt = createdAt + 200;
    const active = ids.map((id, index) => ({
      jobId: id,
      startedAt: startedAt + index,
      timeline: [{ state: 'active', timestamp: startedAt + index }] as JobTimelineEntry[],
    }));
    const completed = ids.map((id, index) => ({
      jobId: id,
      completedAt: completedAt + index,
      timeline: [
        { state: 'active', timestamp: startedAt + index },
        { state: 'completed', timestamp: completedAt + index },
      ] as JobTimelineEntry[],
    }));

    try {
      insertIdenticalJobs(scalar.storage, ids, createdAt);
      insertIdenticalJobs(batched.storage, ids, createdAt);
      for (const update of active) {
        scalar.storage.markActive(update.jobId, update.startedAt, update.timeline);
      }

      const candidate = batched.storage as BatchStorage;
      expect(candidate.markActiveBatch).toBeFunction();
      candidate.markActiveBatch!(active);
      expect(stateSnapshot(batched.db)).toEqual(stateSnapshot(scalar.db));

      for (const update of completed) {
        scalar.storage.markCompleted(update.jobId, update.completedAt, update.timeline);
      }
      expect(candidate.markCompletedBatch).toBeFunction();
      candidate.markCompletedBatch!(completed);
      expect(stateSnapshot(batched.db)).toEqual(stateSnapshot(scalar.db));
    } finally {
      scalar.storage.close();
      batched.storage.close();
    }
  });

  test('flushes buffered jobs before applying either transition batch', () => {
    const { storage } = createStorage({ writeBufferSize: 1_000, writeBufferFlushMs: 60_000 });
    const candidate = storage as BatchStorage;
    const ids = [jobId('buffered-1'), jobId('buffered-2')];
    const startedAt = 1_800_000_001_000;
    const completedAt = startedAt + 100;

    try {
      for (const id of ids) storage.insertJob(createJob(id, 'buffered', { data: { id } }));
      expect(candidate.markActiveBatch).toBeFunction();
      candidate.markActiveBatch!(
        ids.map((id) => ({
          jobId: id,
          startedAt,
          timeline: [{ state: 'active', timestamp: startedAt }],
        }))
      );
      expect(ids.map((id) => storage.getJobStateRaw(id))).toEqual(['active', 'active']);

      expect(candidate.markCompletedBatch).toBeFunction();
      candidate.markCompletedBatch!(
        ids.map((id) => ({
          jobId: id,
          completedAt,
          timeline: [
            { state: 'active', timestamp: startedAt },
            { state: 'completed', timestamp: completedAt },
          ],
        }))
      );
      expect(ids.map((id) => storage.getJobStateRaw(id))).toEqual(['completed', 'completed']);
    } finally {
      storage.close();
    }
  });

  test('rolls back every row when either state batch is rejected', () => {
    const { storage, db } = createStorage();
    const candidate = storage as BatchStorage;
    const ids = [jobId('rollback-good-1'), jobId('rollback-bad'), jobId('rollback-good-2')];
    insertIdenticalJobs(storage, ids, Date.now());
    db.run(`CREATE TRIGGER reject_bad_active
      BEFORE UPDATE ON jobs
      WHEN NEW.state = 'active' AND NEW.id = 'rollback-bad'
      BEGIN
        SELECT RAISE(ABORT, 'rejected active transition');
      END`);

    try {
      expect(candidate.markActiveBatch).toBeFunction();
      expect(() =>
        candidate.markActiveBatch!(
          ids.map((id) => ({ jobId: id, startedAt: Date.now(), timeline: [] }))
        )
      ).toThrow('rejected active transition');
      expect(ids.map((id) => storage.getJobStateRaw(id))).toEqual([
        'waiting',
        'waiting',
        'waiting',
      ]);

      db.run('DROP TRIGGER reject_bad_active');
      for (const id of ids) storage.markActive(id, Date.now(), []);
      db.run(`CREATE TRIGGER reject_bad_completed
        BEFORE UPDATE ON jobs
        WHEN NEW.state = 'completed' AND NEW.id = 'rollback-bad'
        BEGIN
          SELECT RAISE(ABORT, 'rejected completed transition');
        END`);
      expect(candidate.markCompletedBatch).toBeFunction();
      expect(() =>
        candidate.markCompletedBatch!(
          ids.map((id) => ({ jobId: id, completedAt: Date.now(), timeline: [] }))
        )
      ).toThrow('rejected completed transition');
      expect(ids.map((id) => storage.getJobStateRaw(id))).toEqual(['active', 'active', 'active']);
    } finally {
      storage.close();
    }
  });

  test('routes public pull and ack batches once and falls back to scalar writes', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-state-routing-test-'));
    directories.push(directory);
    const manager = new QueueManager({ dataPath: join(directory, 'queue.db') });
    const storage = (manager as unknown as { storage: BatchStorage }).storage;
    const originalActiveBatch = storage.markActiveBatch?.bind(storage);
    const originalCompletedBatch = storage.markCompletedBatch?.bind(storage);
    const originalActive = storage.markActive.bind(storage);
    const originalCompleted = storage.markCompleted.bind(storage);
    let activeBatchCalls = 0;
    let completedBatchCalls = 0;
    let activeScalarCalls = 0;
    let completedScalarCalls = 0;

    try {
      expect(originalActiveBatch).toBeFunction();
      expect(originalCompletedBatch).toBeFunction();
      storage.markActiveBatch = () => {
        activeBatchCalls++;
        throw new Error('force active scalar fallback');
      };
      storage.markCompletedBatch = () => {
        completedBatchCalls++;
        throw new Error('force completed scalar fallback');
      };
      storage.markActive = (...args) => {
        activeScalarCalls++;
        originalActive(...args);
      };
      storage.markCompleted = (...args) => {
        completedScalarCalls++;
        originalCompleted(...args);
      };

      const ids = await manager.pushBatch(
        'routing',
        Array.from({ length: 8 }, (_, index) => ({ data: { index }, durable: true }))
      );
      const pulled = await manager.pullBatch('routing', ids.length);
      await manager.ackBatch(pulled.map((job) => job.id));

      expect(pulled).toHaveLength(8);
      expect(activeBatchCalls).toBe(1);
      expect(completedBatchCalls).toBe(1);
      expect(activeScalarCalls).toBe(8);
      expect(completedScalarCalls).toBe(8);
      expect(ids.map((id) => storage.getJobStateRaw(id))).toEqual(Array(8).fill('completed'));

      activeBatchCalls = 0;
      completedBatchCalls = 0;
      activeScalarCalls = 0;
      completedScalarCalls = 0;
      storage.markActiveBatch = (updates) => {
        activeBatchCalls++;
        originalActiveBatch!(updates);
      };
      storage.markCompletedBatch = (updates) => {
        completedBatchCalls++;
        originalCompletedBatch!(updates);
      };

      const directIds = await manager.pushBatch(
        'direct-routing',
        Array.from({ length: 8 }, (_, index) => ({ data: { index }, durable: true }))
      );
      const directPulled = await manager.pullBatch('direct-routing', directIds.length);
      await manager.ackBatch(directPulled.map((job) => job.id));

      expect(activeBatchCalls).toBe(1);
      expect(completedBatchCalls).toBe(1);
      expect(activeScalarCalls).toBe(0);
      expect(completedScalarCalls).toBe(0);
    } finally {
      manager.shutdown();
    }
  });
});
