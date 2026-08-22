/**
 * queue.forward() — store-and-forward from a local (edge) queue to a remote
 * bunqueue server. The edge pattern: jobs buffer locally in SQLite, a
 * Forwarder drains them to the central server when the uplink is healthy;
 * remote failures leave jobs local (retry → DLQ), nothing is lost.
 *
 * Idempotency: forwarded jobs carry a deterministic remote jobId
 * (`fwd:<localQueue>:<localJobId>`) so a re-forward after a crash or retry
 * never duplicates the job on the remote server.
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { unlink } from 'fs/promises';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { Queue, shutdownManager } from '../src/client';

const REMOTE_PORT = 18871;
const DB = './test-forwarder.db';

let qm: QueueManager;
let server: TcpServer;
const open: Array<{ close: () => Promise<void> | void }> = [];

async function cleanupDb() {
  for (const f of [DB, `${DB}-wal`, `${DB}-shm`]) {
    if (await Bun.file(f).exists()) await unlink(f);
  }
}

/** Local edge queue (embedded SQLite) */
function localQueue<T = unknown>(name: string): Queue<T> {
  const q = new Queue<T>(name, { embedded: true, dataPath: DB });
  open.push(q);
  return q;
}

/** Handle to the remote server-side queue, for assertions */
function remoteQueue<T = unknown>(name: string): Queue<T> {
  const q = new Queue<T>(name, {
    embedded: false,
    connection: { host: '127.0.0.1', port: REMOTE_PORT },
    autoBatch: { enabled: false },
  });
  open.push(q);
  return q;
}

async function waitFor(cond: () => Promise<boolean> | boolean, timeoutMs = 8000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await cond()) return;
    await Bun.sleep(50);
  }
}

beforeAll(async () => {
  shutdownManager();
  await cleanupDb();
  qm = new QueueManager();
  server = createTcpServer(qm, { port: REMOTE_PORT, hostname: '127.0.0.1' });
  localQueue('fwd-bootstrap'); // bind shared embedded manager to our test DB
});

afterAll(async () => {
  for (const c of open) await c.close();
  server.stop();
  qm.shutdown();
  shutdownManager();
  await cleanupDb();
});

describe('queue.forward() basics', () => {
  test('drains local jobs to the remote queue, preserving name and data', async () => {
    const local = localQueue<{ n: number }>('fwd-basic');
    const remote = remoteQueue<{ n: number }>('fwd-basic');

    for (let i = 0; i < 5; i++) {
      await local.add('reading', { n: i });
    }

    const fwd = local.forward({ to: { host: '127.0.0.1', port: REMOTE_PORT } });
    open.push(fwd);

    await waitFor(async () => (await remote.getJobCountsAsync()).waiting >= 5);
    const waiting = await remote.getWaitingAsync<{ n: number }>();
    expect(waiting.length).toBe(5);
    expect(new Set(waiting.map((j) => j.data.n))).toEqual(new Set([0, 1, 2, 3, 4]));
    expect(waiting.every((j) => j.name === 'reading')).toBe(true);

    // Local queue fully drained (jobs completed locally after remote accept)
    const counts = local.getJobCounts();
    expect(counts.waiting).toBe(0);
    expect(counts.active).toBe(0);
  });

  test('queue option maps to a different remote queue name', async () => {
    const local = localQueue<{ v: string }>('fwd-src');
    const remote = remoteQueue<{ v: string }>('fwd-central');

    await local.add('evt', { v: 'mapped' });
    const fwd = local.forward({
      to: { host: '127.0.0.1', port: REMOTE_PORT },
      queue: 'fwd-central',
    });
    open.push(fwd);

    await waitFor(async () => (await remote.getJobCountsAsync()).waiting >= 1);
    const waiting = await remote.getWaitingAsync<{ v: string }>();
    expect(waiting.length).toBe(1);
    expect(waiting[0].data.v).toBe('mapped');
  });

  test('forwarded jobs carry a deterministic jobId → re-push is deduped remotely', async () => {
    const local = localQueue<{ k: number }>('fwd-idem');
    const remote = remoteQueue<{ k: number }>('fwd-idem');

    const job = await local.add('evt', { k: 1 });
    const fwd = local.forward({ to: { host: '127.0.0.1', port: REMOTE_PORT } });
    open.push(fwd);

    await waitFor(async () => (await remote.getJobCountsAsync()).waiting >= 1);

    // Simulate a duplicate forward (crash-replay): same deterministic jobId
    await remote.add('evt', { k: 1 }, { jobId: `fwd:fwd-idem:${job.id}` });
    await Bun.sleep(150);
    const counts = await remote.getJobCountsAsync();
    expect(counts.waiting).toBe(1); // deduped, not 2
  });

  test('emits "forwarded" per job', async () => {
    const local = localQueue<{ x: number }>('fwd-events');
    const forwarded: string[] = [];

    await local.add('evt', { x: 1 });
    const fwd = local.forward({ to: { host: '127.0.0.1', port: REMOTE_PORT } });
    open.push(fwd);
    fwd.on('forwarded', (info: { name: string }) => forwarded.push(info.name));

    await waitFor(() => forwarded.length >= 1);
    expect(forwarded).toEqual(['evt']);
  });
});

describe('queue.forward() TCP source → TCP remote', () => {
  test('forwards between two distinct servers', async () => {
    // Second server acts as the "source" broker (e.g. an on-prem instance)
    const srcQm = new QueueManager();
    const srcServer = createTcpServer(srcQm, { port: 18872, hostname: '127.0.0.1' });
    try {
      const source = new Queue<{ s: number }>('fwd-tcp2tcp', {
        embedded: false,
        connection: { host: '127.0.0.1', port: 18872 },
        autoBatch: { enabled: false },
      });
      const remote = remoteQueue<{ s: number }>('fwd-tcp2tcp');

      await source.add('evt', { s: 7 });
      const fwd = source.forward({ to: { host: '127.0.0.1', port: REMOTE_PORT } });

      await waitFor(async () => (await remote.getJobCountsAsync()).waiting >= 1);
      const waiting = await remote.getWaitingAsync<{ s: number }>();
      expect(waiting.length).toBe(1);
      expect(waiting[0].data.s).toBe(7);

      await fwd.close();
      await source.close();
    } finally {
      srcServer.stop();
      srcQm.shutdown();
    }
  });
});

describe('queue.forward() failure handling', () => {
  test('remote unreachable → jobs stay local (retry/DLQ), nothing lost', async () => {
    const local = localQueue<{ id: number }>('fwd-down');

    for (let i = 0; i < 3; i++) {
      await local.add('evt', { id: i }, { attempts: 2, backoff: 50 });
    }

    // Port with no listener; short timeouts so the test stays fast
    const fwd = local.forward({
      to: {
        host: '127.0.0.1',
        port: 18879,
        commandTimeout: 300,
        // Don't retry the dead link forever during the test
        // oxlint-disable-next-line typescript/no-explicit-any -- exercise a deliberately incomplete connection
      } as any,
    });
    open.push(fwd);

    const errors: Error[] = [];
    fwd.on('error', (err: Error) => errors.push(err));

    // Wait until all attempts are burned
    await waitFor(() => {
      const c = local.getJobCounts();
      return c.waiting + c.active + c.delayed === 0;
    }, 12000);

    // Nothing lost: every job is accounted for locally (DLQ after attempts)
    const counts = local.getJobCounts();
    const dlq = local.getDlq();
    expect(dlq.length + counts.waiting + counts.delayed + counts.active).toBe(3);
    expect(counts.completed).toBe(0); // nothing falsely marked as forwarded
  }, 20000);

  test('close() resolves cleanly and stops forwarding', async () => {
    const local = localQueue<{ z: number }>('fwd-close');
    const fwd = local.forward({ to: { host: '127.0.0.1', port: REMOTE_PORT } });

    await fwd.close(); // must not hang
    await local.add('evt', { z: 1 });
    await Bun.sleep(300);

    // Forwarder stopped: job remains local
    expect(local.getJobCounts().waiting).toBe(1);
  });
});
