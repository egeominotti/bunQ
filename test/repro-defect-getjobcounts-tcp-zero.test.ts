/**
 * REPRO — slug: getjobcounts-tcp-zero
 *
 * DEFECT: Queue.getJobCounts() returns hardcoded zeros in TCP mode.
 *
 * src/client/queue/operations/counts.ts getJobCounts() (sync) returns
 * {waiting:0, active:0, ...} hardcoded for the !ctx.embedded (TCP) branch. The
 * async sibling getJobCountsAsync() works for TCP (it sends GetJobCounts and
 * reflects the real server-side counts). Queue.getJobCounts() delegates to the
 * SYNC one, so a TCP user calling queue.getJobCounts() silently gets all zeros
 * while the real counts are non-zero.
 *
 * This is a REAL in-process TCP server (QueueManager + createTcpServer) with a
 * Queue client over a TCP connection (embedded:false). We push N waiting jobs
 * and M delayed jobs, then assert the CORRECT post-fix behavior:
 *   - (await queue.getJobCounts()).waiting === N
 *   - (await queue.getJobCounts()).delayed === M
 * RED today: getJobCounts() resolves to all zeros.
 *
 * Control: getJobCountsAsync() already returns the real counts (passes today),
 * proving the server-side numbers are correct and the bug is purely the sync
 * path returning hardcoded zeros.
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { Queue } from '../src/client';

interface Harness {
  qm: QueueManager;
  server: TcpServer;
  port: number;
  queues: Queue<unknown>[];
}

let harness: Harness | null = null;

function startServer(): Harness {
  const qm = new QueueManager(); // in-memory, no dataPath
  const server = createTcpServer(qm, { hostname: '127.0.0.1', port: 0 });
  const port = server.server.port;
  const h: Harness = { qm, server, port, queues: [] };
  harness = h;
  return h;
}

function makeQueue<T = unknown>(h: Harness, name: string): Queue<T> {
  const q = new Queue<T>(name, {
    embedded: false,
    connection: { host: '127.0.0.1', port: h.port },
    autoBatch: { enabled: false },
  });
  h.queues.push(q as Queue<unknown>);
  return q;
}

afterEach(async () => {
  if (harness) {
    for (const q of harness.queues) {
      try {
        await q.close();
      } catch {
        /* ignore */
      }
    }
    try {
      harness.server.stop();
    } catch {
      /* ignore */
    }
    try {
      harness.qm.shutdown();
    } catch {
      /* ignore */
    }
    harness = null;
  }
  await Bun.sleep(50);
});

describe('REPRO getjobcounts-tcp-zero', () => {
  it('Queue.getJobCounts() must return the REAL counts over TCP, not hardcoded zeros (RED today: all zeros)', async () => {
    const h = startServer();
    const QUEUE = 'q-getjobcounts-tcp';
    const N = 25;
    const M = 7; // delayed jobs

    const queue = makeQueue<{ n: number }>(h, QUEUE);

    for (let i = 0; i < N; i++) {
      await queue.add('task', { n: i });
    }
    for (let i = 0; i < M; i++) {
      await queue.add('later', { n: 1000 + i }, { delay: 60000 });
    }

    // Control: the async path already reflects the real counts over TCP.
    const asyncCounts = await queue.getJobCountsAsync();
    expect(asyncCounts.waiting).toBe(N);
    expect(asyncCounts.delayed).toBe(M);

    // THE LOAD-BEARING ASSERTION — asserts CORRECT (post-fix) behavior.
    // getJobCounts() must surface the same real counts in TCP mode. RED today:
    // the sync TCP branch returns hardcoded zeros, so waiting/delayed === 0.
    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(N);
    expect(counts.delayed).toBe(M);
    expect(counts.active).toBe(0);
    expect(counts.completed).toBe(0);
    expect(counts.failed).toBe(0);
  }, 15000);
});
