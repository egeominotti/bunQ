/**
 * Issue #74 follow-up — stacktrace must survive beyond the local worker event
 * https://github.com/egeominotti/bunqueue/issues/74#issuecomment-4689465834
 *
 * The 2.6.110 fix populated job.stacktrace ONLY on the in-process `failed`
 * event object. The stack is never sent to the server (FAIL carries just
 * `error: message`), the domain Job has no stacktrace field, and jobProxy
 * hardcodes `stacktrace: null`. Net effect, as reported by @arthurvanl
 * ("I need the stacktrace"):
 *
 *   - queue.getJob(id).stacktrace        → always null   (TCP + embedded)
 *   - DLQ entries' job.stacktrace        → always null
 *   - any process other than the worker  → no stack, ever
 *
 * BullMQ persists job.stacktrace server-side. These tests pin that behavior.
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { unlink } from 'fs/promises';
import { Queue, Worker, shutdownManager } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

const PORT = 17915;
const DB = './test-repro-issue74-persist.db';

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

/** Run a worker until the job has failed `times` times, then settle. */
function failTimes(
  queueName: string,
  times: number,
  opts: { embedded: boolean }
): { worker: Worker; done: Promise<void> } {
  const { promise, resolve } = Promise.withResolvers<void>();
  let count = 0;
  const worker = new Worker(
    queueName,
    async () => {
      throw new Error('persist boom');
    },
    opts.embedded ? { embedded: true } : { embedded: false, connection: TCP_CONN }
  );
  open.push(worker);
  worker.on('failed', () => {
    count++;
    if (count >= times) resolve();
  });
  worker.run();
  return { worker, done: promise };
}

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

describe('Issue #74 - stacktrace persisted server-side', () => {
  test('TCP: getJob() exposes stacktrace while job awaits retry', async () => {
    const q = tcpQueue('i74p-tcp-retry');
    const { worker, done } = failTimes('i74p-tcp-retry', 1, { embedded: false });

    const job = await q.add('job', { v: 1 }, { attempts: 3, backoff: 60_000 });
    await done;
    await worker.close(); // stop pulling; job sits in delayed/waiting for 60s backoff

    const fetched = await q.getJob(job.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.failedReason ?? '').toContain('persist boom');
    expect(Array.isArray(fetched!.stacktrace)).toBe(true);
    expect(fetched!.stacktrace!.length).toBeGreaterThan(0);
    expect(fetched!.stacktrace![0]).toContain('persist boom');
  }, 20000);

  test('embedded: getJob() exposes stacktrace while job awaits retry', async () => {
    const q = embQueue('i74p-emb-retry');
    const { worker, done } = failTimes('i74p-emb-retry', 1, { embedded: true });

    const job = await q.add('job', { v: 1 }, { attempts: 3, backoff: 60_000 });
    await done;
    await worker.close();

    const fetched = await q.getJob(job.id);
    expect(fetched).not.toBeNull();
    expect(Array.isArray(fetched!.stacktrace)).toBe(true);
    expect(fetched!.stacktrace!.length).toBeGreaterThan(0);
    expect(fetched!.stacktrace![0]).toContain('persist boom');
  }, 20000);

  test('embedded: DLQ entry carries the stacktrace after attempts exhausted', async () => {
    const q = embQueue('i74p-emb-dlq');
    const { done } = failTimes('i74p-emb-dlq', 1, { embedded: true });

    await q.add('job', { v: 1 }, { attempts: 1 });
    await done;

    // DLQ insert happens in the same fail path; poll briefly for it
    let entries = q.getDlq();
    for (let i = 0; i < 50 && entries.length === 0; i++) {
      await Bun.sleep(100);
      entries = q.getDlq();
    }
    expect(entries.length).toBeGreaterThan(0);
    const entry = entries[0];
    expect(Array.isArray(entry.job.stacktrace)).toBe(true);
    expect(entry.job.stacktrace!.length).toBeGreaterThan(0);
    expect(entry.job.stacktrace![0]).toContain('persist boom');
  }, 20000);

  test('TCP: stackTraceLimit caps the persisted stacktrace lines', async () => {
    const q = tcpQueue('i74p-tcp-limit');
    const { worker, done } = failTimes('i74p-tcp-limit', 1, { embedded: false });

    const job = await q.add(
      'job',
      { v: 1 },
      { attempts: 3, backoff: 60_000, stackTraceLimit: 3 }
    );
    await done;
    await worker.close();

    const fetched = await q.getJob(job.id);
    expect(fetched).not.toBeNull();
    expect(Array.isArray(fetched!.stacktrace)).toBe(true);
    expect(fetched!.stacktrace!.length).toBeGreaterThan(0);
    expect(fetched!.stacktrace!.length).toBeLessThanOrEqual(3);
  }, 20000);
});
