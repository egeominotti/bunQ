/**
 * Config endpoints must not silently break on non-numeric input.
 *
 * Before: `{ stallInterval: 'abc' }` was stored verbatim; later numeric
 * comparisons saw NaN and stall detection was silently disabled for the queue.
 * Now: numeric strings are coerced, non-numeric garbage is dropped (the manager
 * keeps its default), and explicit single values (RateLimit/SetConcurrency
 * limit) reject non-finite input instead of storing NaN.
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { handleCommand } from '../src/infrastructure/server/handler';
import type { HandlerContext } from '../src/infrastructure/server/types';

let qm: QueueManager | null = null;
afterEach(async () => {
  qm?.shutdown();
  qm = null;
  await Bun.sleep(20);
});
function ctxFor(m: QueueManager): HandlerContext {
  return { queueManager: m, authTokens: new Set<string>(), authenticated: false, clientId: 't' };
}

describe('config validation audit', () => {
  it('sanitizes PostgreSQL diagnostics returned by asynchronous handlers', async () => {
    const leaked =
      'duplicate key value violates unique constraint bunqueue_jobs_pkey (SQLSTATE 23505) at postgres.internal:5432';
    const manager = {
      setRateLimitDurable: async () => {
        throw Object.assign(new Error(leaked), { code: '23505' });
      },
      emitDashboardEvent: () => undefined,
    } as unknown as QueueManager;
    const response = await handleCommand(
      { cmd: 'RateLimit', queue: 'postgres-errors', limit: 1 } as never,
      ctxFor(manager)
    );
    expect(response).toMatchObject({ ok: false, error: 'Internal server error' });
    expect(JSON.stringify(response)).not.toContain('23505');
    expect(JSON.stringify(response)).not.toContain('postgres.internal');
  });

  it('stall-config: garbage string is dropped, default kept (detection stays valid)', async () => {
    qm = new QueueManager();
    const ctx = ctxFor(qm);
    await handleCommand(
      {
        cmd: 'SetStallConfig',
        queue: 'q',
        config: { stallInterval: 'abc', maxStalls: 'xyz' },
      } as never,
      ctx
    );
    const cfg = qm.getStallConfig('q') as { stallInterval: number; maxStalls: number };
    expect(Number.isFinite(cfg.stallInterval)).toBe(true); // NOT NaN
    expect(Number.isFinite(cfg.maxStalls)).toBe(true);
  });

  it('stall-config: numeric string is coerced to a number', async () => {
    qm = new QueueManager();
    const ctx = ctxFor(qm);
    await handleCommand(
      { cmd: 'SetStallConfig', queue: 'q', config: { stallInterval: '45000' } } as never,
      ctx
    );
    expect((qm.getStallConfig('q') as { stallInterval: number }).stallInterval).toBe(45000);
  });

  it('dlq-config: numeric string coerced, garbage dropped', async () => {
    qm = new QueueManager();
    const ctx = ctxFor(qm);
    await handleCommand(
      { cmd: 'SetDlqConfig', queue: 'q', config: { maxEntries: '500', maxAge: 'nope' } } as never,
      ctx
    );
    const cfg = qm.getDlqConfig('q') as { maxEntries: number; maxAge: number | null };
    expect(cfg.maxEntries).toBe(500);
    expect(cfg.maxAge === null || Number.isFinite(cfg.maxAge)).toBe(true); // not NaN
  });

  it('RateLimit / SetConcurrency reject a non-finite limit (no silent NaN)', async () => {
    qm = new QueueManager();
    const ctx = ctxFor(qm);
    const r1 = await handleCommand({ cmd: 'RateLimit', queue: 'q', limit: 'abc' } as never, ctx);
    expect(r1.ok).toBe(false);
    const r2 = await handleCommand(
      { cmd: 'SetConcurrency', queue: 'q', limit: 'abc' } as never,
      ctx
    );
    expect(r2.ok).toBe(false);
    // valid numeric string still works
    const r3 = await handleCommand({ cmd: 'RateLimit', queue: 'q', limit: '100' } as never, ctx);
    expect(r3.ok).toBe(true);
  });
});
