import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { rmSync } from 'node:fs';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { jobId } from '../src/domain/types/job';
import { parseSingleJson, runCli } from './cli-invariants/runtimeHarness';

const DB_PATH = `/tmp/bunqueue-cli-invariants-${process.pid}-${Date.now()}.db`;
const READ_ONLY_QUEUE = `cli-read-${process.pid}`;

let manager: QueueManager;
let server: TcpServer;
let port: number;

function databaseSnapshot(): Record<string, unknown[]> {
  const db = new Database(DB_PATH, { readonly: true });
  try {
    const tables = ['jobs', 'job_results', 'dlq', 'cron_jobs', 'queue_state'];
    return Object.fromEntries(
      tables.map((table) => {
        const rows = db.query(`SELECT * FROM ${table} ORDER BY rowid`).all();
        return [table, rows];
      })
    );
  } finally {
    db.close();
  }
}

function observableStats() {
  const stats = manager.getStats();
  return {
    waiting: stats.waiting,
    active: stats.active,
    delayed: stats.delayed,
    completed: stats.completed,
    failed: stats.failed,
    dlq: stats.dlq,
    totalPushed: stats.totalPushed,
    totalPulled: stats.totalPulled,
    totalCompleted: stats.totalCompleted,
    totalFailed: stats.totalFailed,
  };
}

beforeAll(async () => {
  manager = new QueueManager({ dataPath: DB_PATH });
  server = createTcpServer(manager, { port: 0, hostname: '127.0.0.1' });
  port = server.server.port;

  const waiting = await manager.push(READ_ONLY_QUEUE, {
    data: { fixture: true },
    durable: true,
    jobId: 'cli-read-waiting',
  });
  const completed = await manager.push(READ_ONLY_QUEUE, {
    data: { completed: true },
    durable: true,
    jobId: 'cli-read-completed',
  });
  const pulled = await manager.pull(READ_ONLY_QUEUE);
  expect(pulled?.id).toBe(waiting.id);
  await manager.ack(waiting.id, { done: true });
  expect(completed.id).toBeDefined();
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

describe('CLI invariant register: live process contracts', () => {
  test('I2: success and every error layer emit one JSON document', async () => {
    const cases = [
      await runCli(['ping', '--json'], port),
      await runCli(['job', 'get', 'missing', '--json'], port),
      await runCli(['push', 'q', '{bad', '--json'], port),
      await runCli(['unknown-command', '--json'], port),
      await runCli(['ping', '--json'], 1),
    ];

    for (const result of cases) {
      expect(result.timedOut).toBe(false);
      expect(() => parseSingleJson(result)).not.toThrow();
    }
  });

  test('I6: parse failures never mutate broker or SQLite state', async () => {
    const beforeDb = databaseSnapshot();
    const beforeStats = observableStats();
    const invalidCommands = [
      ['push', 'q'],
      ['push', 'q', '{bad'],
      ['queue', 'jobs', 'q', '--limit', '-1'],
      ['cron', 'add', 'c', '-q', 'q', '-d', '{}', '-e', '0'],
      ['webhook', 'add', 'file:///tmp/h', '-e', 'job.completed'],
      ['unknown-command'],
    ];

    for (const args of invalidCommands) {
      const result = await runCli([...args, '--json'], port);
      expect(result.exitCode).not.toBe(0);
      expect(result.timedOut).toBe(false);
      parseSingleJson(result);
    }

    expect(observableStats()).toEqual(beforeStats);
    expect(databaseSnapshot()).toEqual(beforeDb);
  });

  test('I7: every read-only CLI command preserves state and persistence', async () => {
    const beforeDb = databaseSnapshot();
    const beforeStats = observableStats();
    const commands = [
      ['job', 'get', 'cli-read-waiting'],
      ['job', 'state', 'cli-read-waiting'],
      ['job', 'result', 'cli-read-waiting'],
      ['job', 'logs', 'cli-read-waiting'],
      ['job', 'wait', 'cli-read-waiting', '--timeout', '10'],
      ['queue', 'list'],
      ['queue', 'count', READ_ONLY_QUEUE],
      ['queue', 'jobs', READ_ONLY_QUEUE],
      ['queue', 'paused', READ_ONLY_QUEUE],
      ['dlq', 'list', READ_ONLY_QUEUE],
      ['cron', 'list'],
      ['worker', 'list'],
      ['webhook', 'list'],
      ['stats'],
      ['metrics'],
      ['health'],
      ['ping'],
    ];

    for (const args of commands) {
      const result = await runCli([...args, '--json'], port);
      expect(result.timedOut).toBe(false);
      parseSingleJson(result);
    }

    expect(observableStats()).toEqual(beforeStats);
    expect(databaseSnapshot()).toEqual(beforeDb);
  });

  test('I8: direct API and CLI lifecycles have equivalent observable state', async () => {
    const apiQueue = `api-parity-${process.pid}`;
    const cliQueue = `cli-parity-${process.pid}`;
    const apiJob = await manager.push(apiQueue, {
      data: { value: 42 },
      priority: 7,
      maxAttempts: 2,
      durable: true,
    });
    const cliPush = await runCli(
      ['push', cliQueue, '{"value":42}', '--priority', '7', '--max-attempts', '2', '--json'],
      port
    );
    const cliId = (parseSingleJson(cliPush) as { id: string }).id;

    const apiPulled = await manager.pull(apiQueue);
    const cliPull = await runCli(['pull', cliQueue, '--json'], port);
    const cliPulled = (parseSingleJson(cliPull) as { job: { id: string } }).job;
    expect(apiPulled?.id).toBe(apiJob.id);
    expect(cliPulled.id).toBe(cliId);

    await manager.ack(apiJob.id, { done: true });
    parseSingleJson(await runCli(['ack', cliId, '--result', '{"done":true}', '--json'], port));

    const apiStored = await manager.getJob(apiJob.id);
    const cliStored = await manager.getJob(jobId(cliId));
    expect(cliStored?.data).toEqual(apiStored?.data);
    expect(cliStored?.priority).toBe(apiStored?.priority);
    expect(cliStored?.maxAttempts).toBe(apiStored?.maxAttempts);
    expect(await manager.getJobState(jobId(cliId))).toBe(await manager.getJobState(apiJob.id));
    expect(manager.getResult(jobId(cliId))).toEqual(manager.getResult(apiJob.id));
    expect(manager.getQueueJobCounts(cliQueue)).toEqual(manager.getQueueJobCounts(apiQueue));
  });
});
