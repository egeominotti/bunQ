/**
 * API audit follow-up (issue #95 class): HTTP routes silently dropping params
 * that the underlying command / TCP SDK already support.
 *
 * Verified + fixed:
 *   - POST /jobs/:id/ack   dropped `token` (lock ownership)
 *   - POST /jobs/:id/fail  dropped `token`
 *   - PUT  /jobs/:id/priority dropped `lifo` (tie-break, BullMQ v5 / #90)
 *   - POST /crons          dropped immediately / skipIfNoWorker / preventOverlap / jobOptions
 *
 * Each test spies the QueueManager method the route ultimately calls and asserts
 * the param actually arrives. The spy is tolerant of the call throwing (we only
 * care that the route forwarded the arg).
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { routeJobRoutes } from '../src/infrastructure/server/httpRouteJobs';
import { routeResourceRoutes } from '../src/infrastructure/server/httpRouteResources';
import type { HandlerContext } from '../src/infrastructure/server/types';

let qm: QueueManager | null = null;
afterEach(async () => {
  qm?.shutdown();
  qm = null;
  await Bun.sleep(30);
});

const CORS = new Set<string>();
function ctxFor(m: QueueManager): HandlerContext {
  return { queueManager: m, authTokens: new Set<string>(), authenticated: false, clientId: 't' };
}

/** Record every call's args; delegate to original, swallowing any throw. */
function spy(obj: Record<string, unknown>, key: string): unknown[][] {
  const calls: unknown[][] = [];
  const orig = (obj[key] as (...a: unknown[]) => unknown).bind(obj);
  obj[key] = async (...args: unknown[]) => {
    calls.push(args);
    try {
      return await orig(...args);
    } catch {
      return undefined;
    }
  };
  return calls;
}

async function post(ctx: HandlerContext, path: string, body: unknown, method = 'POST') {
  const req = new Request(`http://localhost:6790${path}`, {
    method,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { req, path, method };
}

describe('HTTP param-forwarding audit (#95 class)', () => {
  it('POST /jobs/:id/ack forwards token', async () => {
    qm = new QueueManager();
    const ctx = ctxFor(qm);
    await qm.push('q', { data: { x: 1 } });
    const job = await qm.pull('q');
    const calls = spy(qm as unknown as Record<string, unknown>, 'ack');

    const { req, path, method } = await post(ctx, `/jobs/${job!.id}/ack`, {
      result: { ok: true },
      token: 'TKN-ack',
    });
    await routeJobRoutes(req, path, method, ctx, CORS);

    expect(calls.length).toBe(1);
    expect(calls[0][2]).toBe('TKN-ack'); // (jobId, result, token)
  });

  it('POST /jobs/:id/fail forwards token', async () => {
    qm = new QueueManager();
    const ctx = ctxFor(qm);
    await qm.push('q', { data: { x: 1 } });
    const job = await qm.pull('q');
    const calls = spy(qm as unknown as Record<string, unknown>, 'fail');

    const { req, path, method } = await post(ctx, `/jobs/${job!.id}/fail`, {
      error: 'boom',
      token: 'TKN-fail',
    });
    await routeJobRoutes(req, path, method, ctx, CORS);

    expect(calls.length).toBe(1);
    expect(calls[0][2]).toBe('TKN-fail'); // (jobId, error, token)
  });

  it('PUT /jobs/:id/priority forwards lifo', async () => {
    qm = new QueueManager();
    const ctx = ctxFor(qm);
    const pushed = await qm.push('q', { data: { x: 1 } });
    const calls = spy(qm as unknown as Record<string, unknown>, 'changePriority');

    const { req, path, method } = await post(
      ctx,
      `/jobs/${pushed.id}/priority`,
      { priority: 5, lifo: true },
      'PUT'
    );
    await routeJobRoutes(req, path, method, ctx, CORS);

    expect(calls.length).toBe(1);
    expect(calls[0][1]).toBe(5); // priority
    expect(calls[0][2]).toBe(true); // lifo
  });

  it('POST /crons forwards immediately / skipIfNoWorker / preventOverlap / jobOptions', async () => {
    qm = new QueueManager();
    const ctx = ctxFor(qm);
    const calls = spy(qm as unknown as Record<string, unknown>, 'addCron');

    const { req, path, method } = await post(ctx, `/crons`, {
      name: 'c1',
      queue: 'q',
      data: { hi: 1 },
      schedule: '* * * * *',
      immediately: true,
      skipIfNoWorker: true,
      preventOverlap: true,
      jobOptions: { attempts: 3, backoff: 1000 },
    });
    await routeResourceRoutes(req, path, method, ctx, CORS);

    expect(calls.length).toBe(1);
    const input = calls[0][0] as Record<string, unknown>;
    expect(input.immediately).toBe(true);
    expect(input.skipIfNoWorker).toBe(true);
    expect(input.preventOverlap).toBe(true);
    expect(input.jobOptions).toEqual({ attempts: 3, backoff: 1000 });
  });
});
