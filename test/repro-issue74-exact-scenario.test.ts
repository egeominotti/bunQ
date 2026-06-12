/**
 * Issue #74 regression — exact 1:1 replica of @arthurvanl's repro
 * https://github.com/egeominotti/bunqueue/issues/74#issuecomment-4689465834
 *
 * Faithful to the reporter's setup:
 *  - TCP server with AUTH_TOKENS
 *  - Queue + Worker with token auth, defaultJobOptions { removeOnComplete, removeOnFail }
 *  - setStallConfig({ enabled: false }) + setDlqConfig({ autoRetry: false, maxAutoRetries: 0 })
 *  - upsertJobScheduler with cron pattern, timezone, preventOverlap, skipIfNoWorker
 *  - processor throws synchronously (imported helper, like their sum.ts)
 *  - worker.on('failed') reads job.stacktrace
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { unlink } from 'fs/promises';
import { Queue, Worker, shutdownManager } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

const PORT = 17913;
const DB = './test-repro-issue74-exact.db';
const BQ_TOKEN = 'test-token-74';

async function cleanupDb() {
  for (const f of [DB, `${DB}-wal`, `${DB}-shm`]) {
    if (await Bun.file(f).exists()) await unlink(f);
  }
}

// Reporter's sum.ts — throws synchronously from an imported function
const sum = (_a: number, _b: number): number => {
  throw new Error('ok');
};

let qm: QueueManager;
let server: TcpServer;
const open: Array<{ close: () => Promise<void> | void }> = [];

const CONNECTION = { host: '127.0.0.1', port: PORT, token: BQ_TOKEN };

const DEFAULT_JOB_OPTS = {
  removeOnComplete: true,
  removeOnFail: true,
};

const DEFAULT_QUEUE_OPTS = {
  connection: CONNECTION,
  embedded: false,
  defaultJobOptions: DEFAULT_JOB_OPTS,
};

beforeAll(async () => {
  await cleanupDb();
  qm = new QueueManager();
  server = createTcpServer(qm, {
    port: PORT,
    hostname: '127.0.0.1',
    authTokens: [BQ_TOKEN],
  });
});

afterAll(async () => {
  for (const o of open) await o.close();
  server.stop();
  qm.shutdown();
  shutdownManager();
  await cleanupDb();
});

describe('Issue #74 - exact reporter scenario (TCP + auth + cron + preventOverlap)', () => {
  test('cron-scheduled job failure carries stacktrace in failed event', async () => {
    const failures: Array<{
      failedReason: string | undefined;
      stacktrace: string[] | null | undefined;
    }> = [];
    const { promise, resolve } = Promise.withResolvers<void>();

    const worker = new Worker(
      'testing-74',
      async (job) => {
        sum(1, 1); // throws 'ok' synchronously, like the reporter's sum.ts
        await job.moveToCompleted(''); // never reached
      },
      DEFAULT_QUEUE_OPTS
    );
    open.push(worker);

    worker.on('failed', (job) => {
      failures.push({
        failedReason: job?.failedReason,
        stacktrace: job?.stacktrace,
      });
      resolve();
    });
    worker.run();

    const queue = new Queue('testing-74', DEFAULT_QUEUE_OPTS);
    open.push(queue);

    // Reporter's disableQueueFeatures()
    queue.setStallConfig({ enabled: false });
    queue.setDlqConfig({ autoRetry: false, maxAutoRetries: 0 });

    // Reporter uses pattern '* * * * *' (60s tick — too slow for a test).
    // every: 300 exercises the same Cron command + spawn path with identical
    // options (preventOverlap dedup, skipIfNoWorker, jobOptions from defaults).
    await queue.upsertJobScheduler('start-new-test-job-74', {
      every: 300,
      timezone: 'Europe/Amsterdam',
      preventOverlap: true,
      skipIfNoWorker: true,
    });

    await promise;
    await queue.removeJobScheduler('start-new-test-job-74');

    const first = failures[0];
    expect(first.failedReason).toBe('ok');
    // The regression: stacktrace must be a non-empty string[] on the failed event
    expect(first.stacktrace).toBeDefined();
    expect(first.stacktrace).not.toBeNull();
    expect(Array.isArray(first.stacktrace)).toBe(true);
    expect(first.stacktrace!.length).toBeGreaterThan(0);
    expect(first.stacktrace![0]).toContain('ok');
    expect(first.stacktrace!.join('\n')).toContain('sum');
  }, 30000);
});
