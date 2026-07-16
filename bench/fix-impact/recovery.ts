import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  elapsedMs,
  managerStorage,
  padded,
  removeDatabase,
  storageDb,
  type Harness,
} from './harness';
import type { QueueManagerLike } from './types';

function seedActiveJobs(harness: Harness, dbPath: string, count: number): void {
  const { SqliteStorage, pack } = harness.runtime;
  const storage = new SqliteStorage({ path: dbPath });
  const db = storageDb(storage);
  const now = Date.now();
  const data = pack({ benchmark: 'recovery' });
  const insert = db.prepare(`
    INSERT INTO jobs (
      id, queue, data, priority, created_at, run_at, started_at,
      attempts, max_attempts, backoff, state
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  db.transaction(() => {
    for (let i = 0; i < count; i++) {
      insert.run(
        padded('active-', i),
        'bench-recovery',
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
  })();
  storage.close();
}

export function benchmarkRecovery(harness: Harness): void {
  const { QueueManager } = harness.runtime;
  const { recoveryJobs, recoverySamples } = harness.profile;
  const samples: number[] = [];
  const observations: Array<Record<string, number>> = [];

  for (let sample = 0; sample < recoverySamples; sample++) {
    const dbPath = join(
      tmpdir(),
      `bunqueue-bench-recovery-${harness.label}-${crypto.randomUUID()}.db`
    );
    let manager: QueueManagerLike | null = null;
    try {
      seedActiveJobs(harness, dbPath, recoveryJobs);
      const startedAt = Bun.nanoseconds();
      manager = new QueueManager({ dataPath: dbPath });
      samples.push(elapsedMs(startedAt));

      const storage = managerStorage(manager);
      const counts = manager.getQueueJobCounts('bench-recovery');
      const memory = manager.getMemoryStats();
      observations.push({
        activeRows: storage.countActiveJobs(),
        pendingRows: storage.countPendingJobs(),
        readyJobs: counts.waiting + counts.prioritized + counts.delayed,
        indexedJobs: memory.jobIndex,
        queuedCounter: memory.queuedTotal,
      });
    } finally {
      manager?.shutdown();
      removeDatabase(dbPath);
    }
  }

  const allComplete = observations.every(
    (observation) =>
      observation.activeRows === 0 &&
      observation.pendingRows === recoveryJobs &&
      observation.readyJobs === recoveryJobs &&
      observation.indexedJobs === recoveryJobs &&
      observation.queuedCounter === recoveryJobs
  );
  harness.addResult({
    id: 'sqlite-recovery-active',
    category: 'recovery',
    operation: 'QueueManager startup recovery',
    workload: { activeJobs: recoveryJobs },
    samples,
    correctness: { allComplete, observations },
    comparable: true,
    note: 'Timing includes opening SQLite and rebuilding all in-memory indexes.',
  });
}
