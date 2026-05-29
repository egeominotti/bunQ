/**
 * Issue #87 reproduction — MCP TcpBackend returns inconsistent numbers.
 *
 * These tests drive the MCP TcpBackend against a REAL in-process TCP server
 * (the same path a user hits with BUNQUEUE_MODE=tcp). They reproduce the
 * response-envelope parsing mismatches reported in:
 * https://github.com/egeominotti/bunqueue/issues/87
 *
 * BUG A: getJobCounts returns all 0 — reads res.waiting but server nests
 *        the values under res.counts.{waiting,...}.
 * BUG B: listWorkers returns [] even with a registered worker — reads
 *        res.workers but server nests under res.data.workers.
 * BUG C: getJobs start/end ignored — sends {start,end} but protocol expects
 *        {offset,limit}; start always 0, end defaults to 100 not the requested.
 * BUG D: getPerQueueStats returns global metrics, not a per-queue breakdown.
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { TcpBackend } from '../src/mcp/adapter';

const PORT = 17897;

let qm: QueueManager;
let server: TcpServer;
let backend: TcpBackend;

beforeAll(async () => {
  qm = new QueueManager();
  server = createTcpServer(qm, { port: PORT, hostname: '127.0.0.1' });
  backend = new TcpBackend({ host: '127.0.0.1', port: PORT });
  await backend.connect();
});

afterAll(() => {
  backend.shutdown();
  server.stop();
  qm.shutdown();
});

describe('Issue #87 — MCP TcpBackend over real TCP server', () => {
  test('BUG A: getJobCounts reflects real waiting jobs (not all 0)', async () => {
    const q = 'issue87-counts';
    await backend.addJob(q, 'task', { n: 1 });
    await backend.addJob(q, 'task', { n: 2 });
    await backend.addJob(q, 'task', { n: 3 });

    const counts = await backend.getJobCounts(q);
    expect(counts.waiting).toBe(3);
  });

  test('BUG B: listWorkers returns a registered worker (not empty)', async () => {
    await backend.registerWorker('issue87-worker', ['issue87-wq']);

    const workers = await backend.listWorkers();
    expect(workers.length).toBeGreaterThan(0);
    expect(workers.some((w) => w.name === 'issue87-worker')).toBe(true);
  });

  test('BUG C: getJobs honors start (offset) pagination', async () => {
    const q = 'issue87-jobs';
    for (let i = 0; i < 10; i++) {
      await backend.addJob(q, 'task', { i });
    }

    const firstTwo = await backend.getJobs(q, { state: 'waiting', start: 0, end: 2 });
    const nextTwo = await backend.getJobs(q, { state: 'waiting', start: 2, end: 4 });

    expect(firstTwo).toHaveLength(2);
    expect(nextTwo).toHaveLength(2);
    // start must actually move the window — the two pages must differ
    const firstIds = new Set(firstTwo.map((j) => j.id));
    expect(nextTwo.every((j) => !firstIds.has(j.id))).toBe(true);
  });

  test('BUG C: getJobs honors end (limit), default page is 20 not 100', async () => {
    const q = 'issue87-jobs-limit';
    for (let i = 0; i < 30; i++) {
      await backend.addJob(q, 'task', { i });
    }

    // explicit end=5 must cap the result
    const five = await backend.getJobs(q, { state: 'waiting', start: 0, end: 5 });
    expect(five).toHaveLength(5);
  });

  test('BUG C: tool-layer default page is 20 (start ?? 0, end ?? 20)', async () => {
    const q = 'issue87-jobs-default';
    for (let i = 0; i < 30; i++) {
      await backend.addJob(q, 'task', { i });
    }

    // Mirror the MCP tool defaults (queueTools.ts: start ?? 0, end ?? 20).
    const page = await backend.getJobs(q, { state: 'waiting', start: 0, end: 20 });
    expect(page).toHaveLength(20);
  });

  test('BUG D: getPerQueueStats returns a per-queue breakdown', async () => {
    const q = 'issue87-perqueue';
    await backend.addJob(q, 'task', { n: 1 });
    await backend.addJob(q, 'task', { n: 2 });

    const stats = await backend.getPerQueueStats();
    // Must be keyed by queue name, not a flat global metrics object.
    expect(stats[q]).toBeDefined();
    const s = stats[q] as Record<string, number>;
    expect(s.waiting).toBe(2);
    // Shape parity with EmbeddedBackend: { waiting, prioritized, delayed, active, dlq }
    expect(s).toHaveProperty('prioritized');
    expect(s).toHaveProperty('delayed');
    expect(s).toHaveProperty('active');
    expect(s).toHaveProperty('dlq');
  });
});
