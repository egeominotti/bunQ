/**
 * Issue #74 follow-up — explicit job.moveToFailed(err) must carry the stacktrace
 * https://github.com/egeominotti/bunqueue/issues/74#issuecomment-4706287391
 *
 * The 2.8.11 fix wired the stack only through the NATURAL-throw path
 * (handleJobFailure): it computes the stack lines, sends them on FAIL so the
 * server persists them, and populates `job.stacktrace` on the local `failed`
 * event. The EXPLICIT path — a processor that catches and calls
 * `await job.moveToFailed(err)` — went through createMoveToFailedHandler /
 * handleManualMove, neither of which touched the stack. @arthurvanl's repro
 * (CASE 1) showed:
 *
 *   failed event: job.stacktrace        → null   (manual moveToFailed)
 *   failed event: job.stacktrace        → [...]  (natural throw, control)
 *
 * Both paths must behave identically: the manual moveToFailed() stack must
 * appear on the local event AND be persisted server-side (getJob/DLQ).
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { unlink } from 'fs/promises';
import { Queue, Worker, shutdownManager } from '../src/client';
import type { Job } from '../src/client/types';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

const PORT = 17923;
const DB = './test-repro-issue74-mtf.db';

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

function embQueue<T = unknown>(name: string): Queue<T> {
  const q = new Queue<T>(name, { embedded: true, dataPath: DB });
  open.push(q);
  return q;
}

/**
 * Worker whose processor catches a thrown Error and reports it via the EXPLICIT
 * `await job.moveToFailed(err)` API (the pattern from the bug report). Resolves
 * with the public job object from the first `failed` event.
 */
function manualFailWorker(
  queueName: string,
  opts: { embedded: boolean }
): { worker: Worker; failed: Promise<Job> } {
  const { promise, resolve } = Promise.withResolvers<Job>();
  const worker = new Worker(
    queueName,
    async (job) => {
      const err = new Error('manual boom');
      try {
        throw err;
      } catch (caught) {
        const syncError = caught instanceof Error ? caught : new Error(String(caught));
        await job.moveToFailed(syncError);
      }
    },
    opts.embedded ? { embedded: true } : { embedded: false, connection: TCP_CONN }
  );
  open.push(worker);
  worker.on('failed', (job) => resolve(job as Job));
  worker.run();
  return { worker, failed: promise };
}

beforeAll(shutdownManager);
beforeAll(async () => {
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

describe('Issue #74 - moveToFailed() carries the stacktrace', () => {
  test('embedded: local failed event exposes the stacktrace', async () => {
    const q = embQueue('i74mtf-emb-event');
    const { failed } = manualFailWorker('i74mtf-emb-event', { embedded: true });

    await q.add('job', { v: 1 }, { attempts: 3, backoff: 60_000 });
    const job = await failed;

    expect(job.failedReason ?? '').toContain('manual boom');
    // THE BUG: was null on the manual moveToFailed() path.
    expect(Array.isArray(job.stacktrace)).toBe(true);
    expect(job.stacktrace!.length).toBeGreaterThan(0);
    expect(job.stacktrace![0]).toContain('manual boom');
  }, 20000);

  test('embedded: getJob() persists the stacktrace server-side', async () => {
    const q = embQueue('i74mtf-emb-persist');
    const { worker, failed } = manualFailWorker('i74mtf-emb-persist', { embedded: true });

    const added = await q.add('job', { v: 1 }, { attempts: 3, backoff: 60_000 });
    await failed;
    await worker.close();

    const fetched = await q.getJob(added.id);
    expect(fetched).not.toBeNull();
    expect(Array.isArray(fetched!.stacktrace)).toBe(true);
    expect(fetched!.stacktrace!.length).toBeGreaterThan(0);
    expect(fetched!.stacktrace![0]).toContain('manual boom');
  }, 20000);

  test('TCP: local failed event exposes the stacktrace (reporter CASE 1)', async () => {
    const q = tcpQueue('i74mtf-tcp-event');
    const { failed } = manualFailWorker('i74mtf-tcp-event', { embedded: false });

    await q.add('job', { v: 1 }, { attempts: 3, backoff: 60_000 });
    const job = await failed;

    expect(job.failedReason ?? '').toContain('manual boom');
    expect(Array.isArray(job.stacktrace)).toBe(true);
    expect(job.stacktrace!.length).toBeGreaterThan(0);
    expect(job.stacktrace![0]).toContain('manual boom');
  }, 20000);

  test('TCP: getJob() persists the stacktrace server-side', async () => {
    const q = tcpQueue('i74mtf-tcp-persist');
    const { worker, failed } = manualFailWorker('i74mtf-tcp-persist', { embedded: false });

    const added = await q.add('job', { v: 1 }, { attempts: 3, backoff: 60_000 });
    await failed;
    await worker.close();

    const fetched = await q.getJob(added.id);
    expect(fetched).not.toBeNull();
    expect(Array.isArray(fetched!.stacktrace)).toBe(true);
    expect(fetched!.stacktrace!.length).toBeGreaterThan(0);
    expect(fetched!.stacktrace![0]).toContain('manual boom');
  }, 20000);
});
