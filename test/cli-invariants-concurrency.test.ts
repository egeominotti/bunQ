import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { rmSync } from 'node:fs';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { jobId } from '../src/domain/types/job';
import { parseSingleJson, runCli } from './cli-invariants/runtimeHarness';

const DB_PATH = `/tmp/bunqueue-cli-concurrency-${process.pid}-${Date.now()}.db`;
let manager: QueueManager;
let server: TcpServer;
let port: number;

function persistedJobCount(): number {
  const db = new Database(DB_PATH, { readonly: true });
  try {
    return (db.query('SELECT COUNT(*) AS count FROM jobs').get() as { count: number }).count;
  } finally {
    db.close();
  }
}

function stableStats() {
  const { uptime: _uptime, ...stats } = manager.getStats();
  return stats;
}

beforeAll(() => {
  manager = new QueueManager({ dataPath: DB_PATH });
  server = createTcpServer(manager, { port: 0, hostname: '127.0.0.1' });
  port = server.server.port;
});

afterAll(() => {
  server.stop();
  manager.shutdown();
  for (const suffix of ['', '-wal', '-shm']) {
    try {
      rmSync(DB_PATH + suffix);
    } catch {
      // already removed
    }
  }
});

describe('CLI concurrency, idempotence and rollback invariants', () => {
  test('32 concurrent custom-ID pushes create one durable job', async () => {
    const queue = `cli-idempotent-${process.pid}`;
    const customId = `same-id-${process.pid}`;
    const beforePushed = manager.getStats().totalPushed;
    const results = await Promise.all(
      Array.from({ length: 32 }, () =>
        runCli(['push', queue, '{"same":true}', '--job-id', customId, '--json'], port, 10_000)
      )
    );

    const ids = results.map((result) => {
      expect(result.timedOut).toBe(false);
      expect(result.exitCode).toBe(0);
      return (parseSingleJson(result) as { id: string }).id;
    });
    expect(new Set(ids)).toEqual(new Set([customId]));
    expect(manager.getQueueJobCounts(queue).waiting).toBe(1);
    expect(manager.getStats().totalPushed - beforePushed).toBe(1n);
    expect(persistedJobCount()).toBe(1);
  });

  test('32 concurrent parse failures roll back without touching broker or SQLite', async () => {
    const beforeStats = stableStats();
    const beforeRows = persistedJobCount();
    const results = await Promise.all(
      Array.from({ length: 32 }, (_, index) =>
        runCli(['push', `invalid-${index}`, '{bad', '--json'], port, 10_000)
      )
    );

    for (const result of results) {
      expect(result.timedOut).toBe(false);
      expect(result.exitCode).toBe(2);
      expect(parseSingleJson(result)).toMatchObject({ ok: false });
    }
    expect(stableStats()).toEqual(beforeStats);
    expect(persistedJobCount()).toBe(beforeRows);
  });

  test('parallel duplicate acknowledgements complete exactly once', async () => {
    const queue = `cli-ack-once-${process.pid}`;
    const pushed = await manager.push(queue, { data: {}, durable: true });
    expect((await manager.pull(queue))?.id).toBe(pushed.id);
    const beforeCompleted = manager.getStats().totalCompleted;
    const results = await Promise.all(
      Array.from({ length: 16 }, () => runCli(['ack', pushed.id, '--json'], port, 10_000))
    );

    expect(results.filter((result) => result.exitCode === 0).length).toBeGreaterThanOrEqual(1);
    for (const result of results) {
      expect(result.timedOut).toBe(false);
      parseSingleJson(result);
    }
    expect(await manager.getJobState(jobId(pushed.id))).toBe('completed');
    expect(manager.getStats().totalCompleted - beforeCompleted).toBe(1n);
    expect(manager.getQueueJobCounts(queue)).toMatchObject({ active: 0, completed: 1 });
  });

  test('16 interrupted long-polls leave no consumers or claimed jobs', async () => {
    const queue = `cli-abort-storm-${process.pid}`;
    const pulls = await Promise.all(
      Array.from({ length: 16 }, () =>
        runCli(['pull', queue, '--timeout', '10000', '--json'], port, 150)
      )
    );
    expect(pulls.every((result) => result.timedOut)).toBe(true);
    const disconnectDeadline = Date.now() + 2_000;
    while (server.getConnectionCount() !== 0 && Date.now() < disconnectDeadline) {
      await Bun.sleep(5);
    }
    expect(server.getConnectionCount()).toBe(0);

    const jobs = await Promise.all(
      Array.from({ length: 16 }, (_, index) =>
        manager.push(queue, { data: { index }, durable: true })
      )
    );
    expect(manager.getQueueJobCounts(queue)).toMatchObject({ waiting: 16, active: 0 });
    for (const job of jobs) {
      expect(await manager.getJobState(job.id)).toBe('waiting');
    }
  });
});
