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

export async function benchmarkInMemoryGetJobs(harness: Harness): Promise<void> {
  const { QueueManager } = harness.runtime;
  const { memoryQueryJobs, querySamples } = harness.profile;
  const manager = new QueueManager();
  const queue = 'bench-getjobs-memory';
  const base = Date.now() - memoryQueryJobs - 10_000;
  try {
    await manager.pushBatch(
      queue,
      Array.from({ length: memoryQueryJobs }, (_, index) => ({
        data: { index },
        timestamp: base + index,
      }))
    );
    const query = () =>
      manager.getJobs(queue, { state: 'waiting', start: 0, end: 100, asc: false });
    const first = query();
    const samples: number[] = [];
    for (let i = 0; i < querySamples; i++) {
      const startedAt = Bun.nanoseconds();
      const page = query();
      samples.push(elapsedMs(startedAt));
      harness.sink ^= page.length;
    }
    const indexes = first.map((job) => (job.data as { index: number }).index);
    harness.addResult({
      id: 'getjobs-memory-desc-page',
      category: 'query',
      operation: 'getJobs descending first page',
      workload: { jobs: memoryQueryJobs, pageSize: 100 },
      samples,
      correctness: {
        count: first.length,
        firstIndex: indexes[0],
        lastIndex: indexes.at(-1),
        expectedFirstIndex: memoryQueryJobs - 1,
        expectedLastIndex: memoryQueryJobs - 100,
        exactPage:
          first.length === 100 &&
          indexes[0] === memoryQueryJobs - 1 &&
          indexes.at(-1) === memoryQueryJobs - 100,
      },
      comparable: false,
      note: 'Latency is not comparable when exactPage=false: the old path truncates before sorting.',
    });
  } finally {
    manager.shutdown();
  }
}

function seedSqlQueryJobs(harness: Harness, dbPath: string): void {
  const { SqliteStorage, pack } = harness.runtime;
  const { sqlNormalJobs, sqlPriorityJobs } = harness.profile;
  const storage = new SqliteStorage({ path: dbPath });
  const db = storageDb(storage);
  const base = Date.now() - sqlNormalJobs - sqlPriorityJobs - 10_000;
  const data = pack({ benchmark: 'sqlite-query' });
  const insert = db.prepare(`
    INSERT INTO jobs (
      id, queue, data, priority, created_at, run_at, attempts,
      max_attempts, backoff, state
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  db.transaction(() => {
    for (let i = 0; i < sqlNormalJobs; i++) {
      insert.run(
        padded('sql-normal-', i),
        'bench-getjobs-sql',
        data,
        0,
        base + i,
        base + i,
        0,
        3,
        1_000,
        'waiting'
      );
    }
    for (let i = 0; i < sqlPriorityJobs; i++) {
      insert.run(
        padded('sql-priority-', i),
        'bench-getjobs-sql',
        data,
        10,
        base + sqlNormalJobs + i,
        base + sqlNormalJobs + i,
        0,
        3,
        1_000,
        'waiting'
      );
    }
  })();
  storage.close();
}

function benchmarkSqlPriority(harness: Harness, manager: QueueManagerLike): void {
  const { sqlNormalJobs, sqlPriorityJobs, querySamples } = harness.profile;
  const pageSize = Math.min(100, sqlPriorityJobs);
  const query = () =>
    manager.getJobs('bench-getjobs-sql', {
      state: 'prioritized',
      start: 0,
      end: pageSize,
      asc: true,
    });
  const first = query();
  const samples: number[] = [];
  for (let i = 0; i < querySamples; i++) {
    const startedAt = Bun.nanoseconds();
    const page = query();
    samples.push(elapsedMs(startedAt));
    harness.sink ^= page.length;
  }
  harness.addResult({
    id: 'getjobs-sql-prioritized',
    category: 'query',
    operation: 'getJobs sparse prioritized page',
    workload: { normalJobsBeforePriority: sqlNormalJobs, priorityJobs: sqlPriorityJobs, pageSize },
    samples,
    correctness: {
      count: first.length,
      allPrioritized: first.every((job) => job.priority > 0),
      fullPage: first.length === pageSize,
    },
    comparable: false,
    note: 'Latency is not comparable when fullPage=false: the old path returns an incomplete page.',
  });
}

function benchmarkSqlDeepPage(harness: Harness, manager: QueueManagerLike): void {
  const { sqlNormalJobs, sqlPriorityJobs, querySamples } = harness.profile;
  const pageSize = Math.min(100, sqlPriorityJobs);
  const query = () =>
    manager.getJobs('bench-getjobs-sql', {
      start: sqlNormalJobs,
      end: sqlNormalJobs + pageSize,
      asc: true,
    });
  const first = query();
  const samples: number[] = [];
  for (let i = 0; i < querySamples; i++) {
    const startedAt = Bun.nanoseconds();
    const page = query();
    samples.push(elapsedMs(startedAt));
    harness.sink ^= page.length;
  }
  const plans = storageDb(managerStorage(manager))
    .query(
      'EXPLAIN QUERY PLAN SELECT id FROM jobs WHERE queue = ? ' +
        'ORDER BY created_at ASC, id ASC LIMIT ? OFFSET ?'
    )
    .all('bench-getjobs-sql', pageSize, sqlNormalJobs) as Array<{ detail: string }>;
  const planDetails = plans.map((row) => row.detail);
  harness.addResult({
    id: 'getjobs-sql-deep-page',
    category: 'query',
    operation: 'getJobs unfiltered deep page',
    workload: { totalJobs: sqlNormalJobs + sqlPriorityJobs, offset: sqlNormalJobs, pageSize },
    samples,
    correctness: {
      count: first.length,
      expectedFirstId: padded('sql-priority-', 0),
      firstId: first[0]?.id,
      exactPage: first.length === pageSize && first[0]?.id === padded('sql-priority-', 0),
      usesTemporarySort: planDetails.some((detail) => detail.includes('TEMP B-TREE')),
      queryPlan: planDetails,
    },
    comparable: true,
  });
}

export function benchmarkSqlGetJobs(harness: Harness): void {
  const dbPath = join(tmpdir(), `bunqueue-bench-query-${harness.label}-${crypto.randomUUID()}.db`);
  let manager: QueueManagerLike | null = null;
  try {
    seedSqlQueryJobs(harness, dbPath);
    manager = new harness.runtime.QueueManager({ dataPath: dbPath });
    benchmarkSqlPriority(harness, manager);
    benchmarkSqlDeepPage(harness, manager);
  } finally {
    manager?.shutdown();
    removeDatabase(dbPath);
  }
}
