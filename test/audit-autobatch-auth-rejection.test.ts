/**
 * AUDIT — Auto-batcher swallows server auth rejection (TCP client SDK)
 *
 * BUG: When the server requires auth (authTokens configured) and a TCP client
 * connects WITHOUT a valid token, a `queue.add()` routed through the default
 * AUTO-BATCHER resolves to `undefined` instead of REJECTING. The server
 * correctly rejects the PUSHB with 'Not authenticated' (and does NOT persist
 * the job — this is NOT an auth bypass), but `addBulk()` in
 * src/client/queue/operations/add.ts does `if (!response.ok) return []`,
 * swallowing the rejection. The AddBatcher then resolves the queued caller
 * with `jobs[i] === undefined`, so the caller believes the add succeeded.
 *
 * Correct behavior (asserted here, RED on buggy code):
 *   - batched add() REJECTS with an Error mentioning auth / not authenticated
 *
 * Controls (must pass before AND after the fix):
 *   (a) durable add (bypasses the batcher → single PUSH) DOES throw on auth
 *       failure (already correct via add.ts ~line 168)
 *   (b) the server did NOT persist the job (server-side queue count == 0),
 *       proving this is an error-propagation defect, not an auth bypass.
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { Queue } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

// Random high port to avoid collisions with other suites / shared pools.
const PORT = 28000 + Math.floor(Math.random() * 9000);
const AUTH_TOKEN = 'secret123';

let qm: QueueManager;
let server: TcpServer;
const open: Array<{ close: () => Promise<void> | void }> = [];

// Ensure the client is genuinely tokenless: resolveToken() falls back to these
// env vars, which would otherwise trigger an Auth handshake and connect-time
// failure (masking the real bug). We want a CONNECTED-but-UNAUTHENTICATED client.
let savedBq: string | undefined;
let savedBunqueue: string | undefined;

/** Tokenless TCP queue with the DEFAULT auto-batcher (enabled). */
function tokenlessBatched<T = unknown>(name: string): Queue<T> {
  const q = new Queue<T>(name, {
    embedded: false,
    connection: { host: '127.0.0.1', port: PORT }, // NO token
    // autoBatch left at default (enabled) — this is the path under test.
  });
  open.push(q);
  return q;
}

beforeAll(() => {
  savedBq = Bun.env.BQ_TOKEN;
  savedBunqueue = Bun.env.BUNQUEUE_TOKEN;
  delete Bun.env.BQ_TOKEN;
  delete Bun.env.BUNQUEUE_TOKEN;

  qm = new QueueManager();
  // Server REQUIRES auth: any tokenless client is connected but not authenticated.
  server = createTcpServer(qm, {
    port: PORT,
    hostname: '127.0.0.1',
    authTokens: [AUTH_TOKEN],
  });
});

afterAll(async () => {
  for (const q of open) await q.close();
  server.stop();
  qm.shutdown();
  if (savedBq !== undefined) Bun.env.BQ_TOKEN = savedBq;
  if (savedBunqueue !== undefined) Bun.env.BUNQUEUE_TOKEN = savedBunqueue;
});

describe('auto-batcher must propagate server auth rejection', () => {
  // LOAD-BEARING: on buggy code this resolves to `undefined`; correct behavior
  // is to REJECT. This is the assertion that flips RED→GREEN with the fix.
  test('batched add() REJECTS (does not resolve undefined) on auth failure', async () => {
    const queue = tokenlessBatched('audit-autobatch-auth');

    let threw = false;
    let resolvedValue: unknown = '<<unset>>';
    try {
      resolvedValue = await queue.add('task', { x: 1 });
    } catch (err) {
      threw = true;
      const msg = err instanceof Error ? err.message : String(err);
      // Server rejection text is 'Not authenticated'.
      expect(msg.toLowerCase()).toMatch(/auth|not authenticated/);
    }

    // The defect: it resolves to `undefined` instead of rejecting.
    expect(resolvedValue).toBe('<<unset>>'); // i.e. never resolved
    expect(threw).toBe(true);
  });

  // CONTROL (a): durable add bypasses the batcher (single PUSH) and ALREADY
  // throws correctly on auth failure — passes before and after the fix.
  test('CONTROL: durable add() throws on auth failure (bypasses batcher)', async () => {
    const queue = tokenlessBatched('audit-autobatch-auth-durable');

    let threw = false;
    try {
      await queue.add('task', { x: 2 }, { durable: true });
    } catch (err) {
      threw = true;
      const msg = err instanceof Error ? err.message : String(err);
      expect(msg.toLowerCase()).toMatch(/auth|not authenticated/);
    }
    expect(threw).toBe(true);
  });

  // CONTROL (b): prove this is NOT an auth bypass — the server never persisted
  // either job. Read the server-side QueueManager directly (in-process).
  test('CONTROL: server did NOT persist any job (not an auth bypass)', () => {
    const batched = qm.getQueueJobCounts('audit-autobatch-auth');
    const durable = qm.getQueueJobCounts('audit-autobatch-auth-durable');

    const total = (c: { waiting: number; prioritized: number; active: number; delayed: number }) =>
      c.waiting + c.prioritized + c.active + c.delayed;

    expect(total(batched)).toBe(0);
    expect(total(durable)).toBe(0);
  });
});
