/**
 * Issue #74 regression — "Stacktrace not included in `failed` worker event"
 * https://github.com/egeominotti/bunqueue/issues/74#issuecomment-4689465834
 *
 * Originally fixed in 2.6.110, reported regressed in 2.8.7 by @arthurvanl.
 * Reporter runs TCP mode with a cron-scheduled job (upsertJobScheduler),
 * removeOnComplete/removeOnFail defaults, stall+DLQ auto-retry disabled.
 *
 * These tests cover every path the reporter's repro touches:
 *  1. TCP worker, plain add() job that throws
 *  2. TCP worker, retry attempt (2nd failure of same job)
 *  3. TCP worker, job created by upsertJobScheduler (cron path)
 *  4. TCP worker, removeOnFail: true (reporter's default job options)
 *  5. embedded worker (control — covered by bug-74 test, kept as baseline)
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { unlink } from 'fs/promises';
import { Queue, Worker, shutdownManager } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

const PORT = 17911;
const DB = './test-repro-issue74.db';

async function cleanupDb() {
  for (const f of [DB, `${DB}-wal`, `${DB}-shm`]) {
    if (await Bun.file(f).exists()) await unlink(f);
  }
}

let qm: QueueManager;
let server: TcpServer;
const open: Array<{ close: () => Promise<void> | void }> = [];

const TCP_CONN = { host: '127.0.0.1', port: PORT };

function tcpQueue<T = unknown>(name: string): Queue<T> {
  const q = new Queue<T>(name, {
    embedded: false,
    connection: TCP_CONN,
    autoBatch: { enabled: false },
  });
  open.push(q);
  return q;
}

interface FailedCapture {
  failedReason: string | undefined;
  stacktrace: string[] | null | undefined;
}

function captureFailed(worker: Worker): Promise<FailedCapture> {
  const { promise, resolve } = Promise.withResolvers<FailedCapture>();
  worker.on('failed', (job) => {
    resolve({
      failedReason: job?.failedReason,
      stacktrace: job?.stacktrace,
    });
  });
  return promise;
}

function expectStacktrace(result: FailedCapture, message: string) {
  expect(result.failedReason).toBe(message);
  expect(result.stacktrace).toBeDefined();
  expect(result.stacktrace).not.toBeNull();
  expect(Array.isArray(result.stacktrace)).toBe(true);
  expect(result.stacktrace!.length).toBeGreaterThan(0);
  expect(result.stacktrace![0]).toContain(message);
}

beforeAll(async () => {
  shutdownManager();
  await cleanupDb();
  qm = new QueueManager();
  server = createTcpServer(qm, { port: PORT, hostname: '127.0.0.1' });
});

afterAll(async () => {
  for (const o of open) await o.close();
  server.stop();
  qm.shutdown();
  shutdownManager();
  await cleanupDb();
});

describe('Issue #74 regression - stacktrace in failed event (TCP mode)', () => {
  test('1. plain add(): failed event includes stacktrace', async () => {
    const q = tcpQueue('i74-plain');
    const worker = new Worker(
      'i74-plain',
      async () => {
        throw new Error('plain boom');
      },
      { embedded: false, connection: TCP_CONN }
    );
    open.push(worker);
    const failed = captureFailed(worker);
    worker.run();

    await q.add('job', { v: 1 }, { attempts: 1 });
    const result = await failed;
    expectStacktrace(result, 'plain boom');
  }, 15000);

  test('2. retry attempt: second failure still includes stacktrace', async () => {
    const q = tcpQueue('i74-retry');
    const captures: FailedCapture[] = [];
    const { promise, resolve } = Promise.withResolvers<void>();

    const worker = new Worker(
      'i74-retry',
      async () => {
        throw new Error('retry boom');
      },
      { embedded: false, connection: TCP_CONN }
    );
    open.push(worker);
    worker.on('failed', (job) => {
      captures.push({ failedReason: job?.failedReason, stacktrace: job?.stacktrace });
      if (captures.length >= 2) resolve();
    });
    worker.run();

    await q.add('job', { v: 1 }, { attempts: 2, backoff: 50 });
    await promise;

    for (const c of captures) {
      expectStacktrace(c, 'retry boom');
    }
  }, 20000);

  test('3. cron job via upsertJobScheduler: failed event includes stacktrace', async () => {
    const q = tcpQueue('i74-cron');
    const worker = new Worker(
      'i74-cron',
      async () => {
        throw new Error('cron boom');
      },
      { embedded: false, connection: TCP_CONN }
    );
    open.push(worker);
    const failed = captureFailed(worker);
    worker.run();

    // every: 200ms — repeat job created server-side, same path as reporter's cron
    await q.upsertJobScheduler('i74-sched', { every: 200 });

    const result = await failed;
    await q.removeJobScheduler('i74-sched');
    expectStacktrace(result, 'cron boom');
  }, 20000);

  test('4. removeOnFail: true (reporter default opts): stacktrace still present', async () => {
    const q = tcpQueue('i74-rof');
    const worker = new Worker(
      'i74-rof',
      async () => {
        throw new Error('rof boom');
      },
      { embedded: false, connection: TCP_CONN }
    );
    open.push(worker);
    const failed = captureFailed(worker);
    worker.run();

    await q.add('job', { v: 1 }, { attempts: 1, removeOnFail: true, removeOnComplete: true });
    const result = await failed;
    expectStacktrace(result, 'rof boom');
  }, 15000);
});

describe('Issue #74 regression - embedded baseline', () => {
  test('5. embedded worker: failed event includes stacktrace', async () => {
    const q = new Queue('i74-emb', { embedded: true, dataPath: DB });
    open.push(q);
    const worker = new Worker(
      'i74-emb',
      async () => {
        throw new Error('emb boom');
      },
      { embedded: true }
    );
    open.push(worker);
    const failed = captureFailed(worker);
    worker.run();

    await q.add('job', { v: 1 }, { attempts: 1 });
    const result = await failed;
    expectStacktrace(result, 'emb boom');
  }, 15000);
});
