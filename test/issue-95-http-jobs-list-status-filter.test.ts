/**
 * Issue #95 — HTTP GET /queues/:name/jobs/list ignores the `status` filter.
 *
 * https://github.com/egeominotti/bunqueue/issues/95
 *
 * The HTTP list handler (src/infrastructure/server/httpRouteQueues.ts) reads
 * ONLY the `state` query param. A dashboard calling `?status=failed` (the param
 * the reporter used) gets `state=undefined` → GetJobs with no filter → the WHOLE
 * queue back, every state mixed in. TCP `getJobsAsync({ state })` filters
 * correctly because it sends `state` directly.
 *
 * This test drives the real route function (so query-string parsing is
 * exercised) against a queue seeded with a known mix of states, and checks the
 * filter both ways:
 *   - `?status=<state>` — RED before the fix (returns all 14, mixed states).
 *   - `?state=<state>`  — pins down whether the canonical param already works.
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { routeQueueRoutes } from '../src/infrastructure/server/httpRouteQueues';
import { handleCommand } from '../src/infrastructure/server/handler';
import type { HandlerContext } from '../src/infrastructure/server/types';

let qm: QueueManager | null = null;

afterEach(async () => {
  qm?.shutdown();
  qm = null;
  await Bun.sleep(30);
});

const QUEUE = 'q95';
const CORS = new Set<string>();

function ctxFor(manager: QueueManager): HandlerContext {
  return {
    queueManager: manager,
    authTokens: new Set<string>(),
    authenticated: false,
    clientId: 'test-issue-95',
  };
}

/** Drive the real HTTP route for /queues/:q/jobs/list?<param>=<value>. */
async function listHttp(
  ctx: HandlerContext,
  param: 'status' | 'state',
  value: string
): Promise<Array<{ id: string; state: string }>> {
  const path = `/queues/${QUEUE}/jobs/list`;
  const url = `http://localhost:6790${path}?${param}=${value}&limit=100&offset=0`;
  const req = new Request(url, { method: 'GET' });
  const res = await routeQueueRoutes(req, path, 'GET', ctx, CORS);
  if (!res) throw new Error('route did not match /jobs/list');
  const body = (await res.json()) as { jobs?: Array<{ id: string; state: string }> };
  return body.jobs ?? [];
}

/** Seed the reporter's exact mix: 3 failed, 2 completed, 4 waiting, 5 delayed. */
async function seed(manager: QueueManager): Promise<void> {
  // 3 failed (maxAttempts 1 → fail is terminal)
  for (let i = 0; i < 3; i++) {
    await manager.push(QUEUE, { data: { kind: 'fail', i }, maxAttempts: 1 });
    const job = await manager.pull(QUEUE);
    if (!job) throw new Error('expected a job to fail');
    await manager.fail(job.id, 'intentional');
  }
  // 2 completed
  for (let i = 0; i < 2; i++) {
    await manager.push(QUEUE, { data: { kind: 'ok', i }, maxAttempts: 1 });
    const job = await manager.pull(QUEUE);
    if (!job) throw new Error('expected a job to complete');
    await manager.ack(job.id, { done: true });
  }
  // 4 waiting
  for (let i = 0; i < 4; i++) await manager.push(QUEUE, { data: { kind: 'wait', i } });
  // 5 delayed (10 min out)
  for (let i = 0; i < 5; i++) await manager.push(QUEUE, { data: { kind: 'delay', i }, delay: 600_000 });
}

const EXPECTED: Record<string, number> = { failed: 3, completed: 2, waiting: 4, delayed: 5 };

describe('Issue #95: HTTP /jobs/list status filter', () => {
  it('seeds the expected state mix (sanity)', async () => {
    qm = new QueueManager();
    const ctx = ctxFor(qm);
    await seed(qm);

    const counts = (await handleCommand({ cmd: 'GetJobCounts', queue: QUEUE }, ctx)) as {
      counts: Record<string, number>;
    };
    expect(counts.counts.failed).toBe(3);
    expect(counts.counts.completed).toBe(2);
    expect(counts.counts.waiting).toBe(4);
    expect(counts.counts.delayed).toBe(5);
  });

  it('?status=<state> returns ONLY jobs in that state (the #95 bug)', async () => {
    qm = new QueueManager();
    const ctx = ctxFor(qm);
    await seed(qm);

    for (const [state, expected] of Object.entries(EXPECTED)) {
      const jobs = await listHttp(ctx, 'status', state);
      const wrongState = jobs.filter((j) => j.state !== state).map((j) => j.state);
      // BUG: ?status= is ignored → all 14 jobs come back with mixed states.
      expect({ state, count: jobs.length, wrongState }).toEqual({
        state,
        count: expected,
        wrongState: [],
      });
    }
  });

  it('?state=<state> returns ONLY jobs in that state (canonical param)', async () => {
    qm = new QueueManager();
    const ctx = ctxFor(qm);
    await seed(qm);

    for (const [state, expected] of Object.entries(EXPECTED)) {
      const jobs = await listHttp(ctx, 'state', state);
      const wrongState = jobs.filter((j) => j.state !== state).map((j) => j.state);
      expect({ state, count: jobs.length, wrongState }).toEqual({
        state,
        count: expected,
        wrongState: [],
      });
    }
  });

  it('supports multiple states: comma-separated and repeated params', async () => {
    qm = new QueueManager();
    const ctx = ctxFor(qm);
    await seed(qm);

    const path = `/queues/${QUEUE}/jobs/list`;
    const call = async (qs: string) => {
      const req = new Request(`http://localhost:6790${path}?${qs}&limit=100`, { method: 'GET' });
      const res = await routeQueueRoutes(req, path, 'GET', ctx, CORS);
      const body = (await res!.json()) as { jobs?: Array<{ state: string }> };
      return body.jobs ?? [];
    };

    // status comma-separated: failed(3) + completed(2) = 5
    const commaStatus = await call('status=failed,completed');
    expect(commaStatus.length).toBe(5);
    expect(commaStatus.every((j) => j.state === 'failed' || j.state === 'completed')).toBe(true);

    // status repeated: failed(3) + waiting(4) = 7
    const repeated = await call('status=failed&status=waiting');
    expect(repeated.length).toBe(7);
    expect(repeated.every((j) => j.state === 'failed' || j.state === 'waiting')).toBe(true);

    // state comma-separated still works too (canonical): waiting(4) + delayed(5) = 9
    const commaState = await call('state=waiting,delayed');
    expect(commaState.length).toBe(9);
  });
});
