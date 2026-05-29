/**
 * Issue #88 — Created job has wrong priority (and other options not reflected).
 * https://github.com/egeominotti/bunqueue/issues/88
 *
 *   const job = await queue.add(name, data, { priority: 10 })
 *   job.priority // returned 0 instead of 10
 *
 * Root cause: in TCP mode the returned Job is built by createJobProxy /
 * createSimpleJob, which hardcode priority=0, delay=0, opts={} and never
 * reflect the options passed to add()/addBulk(). Embedded add() uses
 * toPublicJob (correct), but embedded addBulk() uses createSimpleJob (wrong).
 *
 * These tests assert the returned-job contract across ALL FOUR paths
 * (embedded add, embedded addBulk, TCP add, TCP addBulk) AND that the
 * options are actually forwarded to the server/manager (round-trip).
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { unlink } from 'fs/promises';
import { Queue, shutdownManager } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

const PORT = 17898;
const DB = './test-issue88.db';

async function cleanupDb() {
  for (const f of [DB, `${DB}-wal`, `${DB}-shm`]) {
    if (await Bun.file(f).exists()) await unlink(f);
  }
}

let qm: QueueManager;
let server: TcpServer;
let tcpQueue: Queue;
let embQueue: Queue;

beforeAll(async () => {
  await cleanupDb();
  // TCP server backed by an in-memory manager
  qm = new QueueManager();
  server = createTcpServer(qm, { port: PORT, hostname: '127.0.0.1' });
  tcpQueue = new Queue('issue88-tcp', {
    // test/preload.ts sets BUNQUEUE_EMBEDDED=1, so force TCP explicitly
    embedded: false,
    connection: { host: '127.0.0.1', port: PORT },
    // disable auto-batch so add() exercises the single-add path directly too
    autoBatch: { enabled: false },
  });
  embQueue = new Queue('issue88-emb', { embedded: true, dataPath: DB });
});

afterAll(async () => {
  await tcpQueue.close();
  await embQueue.close();
  server.stop();
  qm.shutdown();
  shutdownManager();
  await cleanupDb();
});

describe('Issue #88 — returned job reflects options (TCP add)', () => {
  test('priority is reflected (the reported bug)', async () => {
    const job = await tcpQueue.add('task', { x: 1 }, { priority: 10 });
    expect(job.priority).toBe(10);
  });

  test('delay is reflected', async () => {
    const job = await tcpQueue.add('task', { x: 2 }, { delay: 5000 });
    expect(job.delay).toBe(5000);
  });

  test('attempts is reflected in opts', async () => {
    const job = await tcpQueue.add('task', { x: 3 }, { attempts: 7 });
    expect(job.opts.attempts).toBe(7);
  });
});

describe('Issue #88 — returned job reflects options (TCP addBulk)', () => {
  test('priority per job is reflected', async () => {
    const jobs = await tcpQueue.addBulk([
      { name: 'a', data: { x: 1 }, opts: { priority: 3 } },
      { name: 'b', data: { x: 2 }, opts: { priority: 8 } },
    ]);
    expect(jobs[0].priority).toBe(3);
    expect(jobs[1].priority).toBe(8);
  });
});

describe('Issue #88 — returned job reflects options (embedded add)', () => {
  test('priority is reflected', async () => {
    const job = await embQueue.add('task', { x: 1 }, { priority: 10 });
    expect(job.priority).toBe(10);
  });
});

describe('Issue #88 — returned job reflects options (embedded addBulk)', () => {
  test('priority per job is reflected', async () => {
    const jobs = await embQueue.addBulk([
      { name: 'a', data: { x: 1 }, opts: { priority: 4 } },
      { name: 'b', data: { x: 2 }, opts: { priority: 9 } },
    ]);
    expect(jobs[0].priority).toBe(4);
    expect(jobs[1].priority).toBe(9);
  });
});

describe('Issue #88 — options are forwarded to the server (round-trip)', () => {
  test('TCP: priority reaches the server (counts per priority)', async () => {
    const q = new Queue('issue88-fwd-tcp', {
      embedded: false,
      connection: { host: '127.0.0.1', port: PORT },
      autoBatch: { enabled: false },
    });
    try {
      await q.add('task', { x: 1 }, { priority: 5 });
      // TCP queues use the async count variants (sync ones are embedded-only).
      const counts = await q.getCountsPerPriorityAsync();
      expect(counts[5]).toBe(1);
    } finally {
      await q.close();
    }
  });

  test('TCP: getJob reflects stored priority/delay/opts', async () => {
    const q = new Queue('issue88-getjob-tcp', {
      embedded: false,
      connection: { host: '127.0.0.1', port: PORT },
      autoBatch: { enabled: false },
    });
    try {
      const added = await q.add('task', { x: 1 }, { priority: 6, delay: 4000, attempts: 5 });
      const fetched = await q.getJob(added.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.priority).toBe(6);
      expect(fetched!.delay).toBe(4000);
      expect(fetched!.opts.attempts).toBe(5);
    } finally {
      await q.close();
    }
  });

  test('embedded: getJob reflects stored priority', async () => {
    const added = await embQueue.add('task', { x: 1 }, { priority: 7 });
    const fetched = await embQueue.getJob(added.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.priority).toBe(7);
  });

  test('TCP: deduplication is forwarded (duplicate is collapsed)', async () => {
    const q = new Queue('issue88-dedup-tcp', {
      embedded: false,
      connection: { host: '127.0.0.1', port: PORT },
      autoBatch: { enabled: false },
    });
    try {
      const j1 = await q.add('task', { x: 1 }, { deduplication: { id: 'dup-key' } });
      const j2 = await q.add('task', { x: 2 }, { deduplication: { id: 'dup-key' } });
      // Same dedup id while first is still waiting → second returns the same job id.
      expect(j2.id).toBe(j1.id);
    } finally {
      await q.close();
    }
  });
});
