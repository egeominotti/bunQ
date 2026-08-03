/**
 * REPRO — moveToDelayed is broken over TCP (works embedded).
 *
 * Run: bun test test/repro-movetodelayed-tcp.test.ts
 *
 * Two compounding defects make Queue.moveJobToDelayed / job.moveToDelayed a silent
 * failure over TCP:
 *   1. The client sends `{ cmd:'MoveToDelayed', id, timestamp }` (jobMove.ts) but the
 *      command type (command.ts MoveToDelayedCommand) and handler (advanced.ts) only
 *      know `delay`. So `delay` arrives undefined → for an active job
 *      runAt = now + undefined = NaN → it is re-queued as `waiting`, not `delayed`.
 *   2. The server op moveJobToDelayed (jobManagement.ts) early-returns false for any
 *      non-`processing` job, so a WAITING job is a silent no-op (the embedded client
 *      special-cases waiting jobs via changeWaitingDelay; the TCP path does not).
 * The client TCP path returns void and never checks the server `.ok`, so callers get
 * no error in either case.
 *
 * Asserts the CORRECT behavior → RED on current code, GREEN once the TCP path moves
 * waiting/active jobs to `delayed`. Spawns its own server; DOES NOT touch src/.
 */

import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Queue, Worker } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

let dataDir = '';
let manager: QueueManager | null = null;
let server: TcpServer | null = null;
let port = 0;

describe('REPRO: moveToDelayed over TCP', () => {
  beforeAll(async () => {
    dataDir = mkdtempSync(join(tmpdir(), 'bunqueue-move-delayed-tcp-'));
    manager = new QueueManager({ dataPath: join(dataDir, 'queue.db') });
    server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
    port = server.server.port;
  });

  afterAll(async () => {
    server?.stop();
    manager?.shutdown();
    rmSync(dataDir, { recursive: true, force: true });
  });

  test('moveJobToDelayed on a WAITING job moves it to delayed (TCP)', async () => {
    const q = new Queue('mtd-waiting', {
      connection: { host: '127.0.0.1', port },
      embedded: false,
    });
    try {
      const job = await q.add('j', { x: 1 });
      expect(await q.getJobState(job.id)).toBe('waiting');

      await q.moveJobToDelayed(job.id, Date.now() + 60_000);
      await Bun.sleep(250);

      expect(await q.getJobState(job.id)).toBe('delayed'); // RED: stays 'waiting'
    } finally {
      await q.close();
    }
  }, 45000);

  test('moveJobToDelayed on an ACTIVE job delays it, not re-queues as waiting (TCP)', async () => {
    const q = new Queue('mtd-active', {
      connection: { host: '127.0.0.1', port },
      embedded: false,
    });
    let release!: () => void;
    const block = new Promise<void>((r) => {
      release = r;
    });
    let active = false;
    let activeToken: string | undefined;
    const w = new Worker(
      'mtd-active',
      async (activeJob) => {
        active = true;
        activeToken = activeJob.token;
        await block;
        return { ok: true };
      },
      { connection: { host: '127.0.0.1', port }, embedded: false, concurrency: 1 }
    );
    try {
      const job = await q.add('j', { x: 2 });
      const t0 = Date.now();
      while (!active && Date.now() - t0 < 5000) await Bun.sleep(10);
      expect(active).toBe(true);
      expect(activeToken).toBeString();

      await q.moveJobToDelayed(job.id, Date.now() + 60_000, activeToken);
      await Bun.sleep(250);
      expect(await q.getJobState(job.id)).toBe('delayed');

      // Force-close abandons the processor's obsolete outcome before releasing it.
      await w.close(true);
      release();
    } finally {
      release();
      await w.close(true);
      await q.close();
    }
  }, 45000);
});
