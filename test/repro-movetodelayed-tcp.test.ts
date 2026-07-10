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
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Queue } from '../src/client';

const PORT = 17321;
const HTTP = 17322;
const DB = join(tmpdir(), `bunq-mtd-tcp-${process.pid}.db`);
// biome-ignore lint/suspicious/noExplicitAny: Bun.Subprocess
let proc: any;

// 60s: CI runners cold-start the real server binary much slower than a dev machine
async function waitPort(port: number, timeoutMs = 60000) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    try {
      const s = await Bun.connect({ hostname: '127.0.0.1', port, socket: { data() {} } });
      s.end();
      return;
    } catch {
      await Bun.sleep(100);
    }
  }
  throw new Error(`server not ready on :${port}`);
}
function cleanDb() {
  for (const suffix of ['', '-wal', '-shm']) {
    const f = `${DB}${suffix}`;
    if (existsSync(f)) rmSync(f, { force: true });
  }
}

describe('REPRO: moveToDelayed over TCP', () => {
  beforeAll(async () => {
    cleanDb();
    proc = Bun.spawn([process.execPath, 'run', 'src/main.ts'], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        BUNQUEUE_EMBEDDED: '',
        TCP_PORT: String(PORT),
        HTTP_PORT: String(HTTP),
        BUNQUEUE_DATA_PATH: DB,
        LOG_LEVEL: 'error',
      },
      stdout: 'ignore',
      stderr: 'ignore',
    });
    await waitPort(PORT);
  }, 70000);

  afterAll(async () => {
    proc?.kill();
    await Bun.sleep(150);
    cleanDb();
  });

  test('moveJobToDelayed on a WAITING job moves it to delayed (TCP)', async () => {
    const q = new Queue('mtd-waiting', { connection: { host: '127.0.0.1', port: PORT }, embedded: false });
    const job = await q.add('j', { x: 1 });
    expect(await q.getJobState(job.id)).toBe('waiting');

    await q.moveJobToDelayed(job.id, Date.now() + 60_000);
    await Bun.sleep(250);

    expect(await q.getJobState(job.id)).toBe('delayed'); // RED: stays 'waiting'
    await q.close();
  }, 45000);

  test('moveJobToDelayed on an ACTIVE job delays it, not re-queues as waiting (TCP)', async () => {
    const q = new Queue('mtd-active', { connection: { host: '127.0.0.1', port: PORT }, embedded: false });
    const job = await q.add('j', { x: 2 });
    // Pull it active via a short-lived worker that holds it.
    const { Worker } = await import('../src/client');
    let release!: () => void;
    const block = new Promise<void>((r) => {
      release = r;
    });
    let active = false;
    const w = new Worker(
      'mtd-active',
      async () => {
        active = true;
        await block;
        return { ok: true };
      },
      { connection: { host: '127.0.0.1', port: PORT }, embedded: false, concurrency: 1 }
    );
    const t0 = Date.now();
    while (!active && Date.now() - t0 < 5000) await Bun.sleep(10);
    expect(active).toBe(true);

    await q.moveJobToDelayed(job.id, Date.now() + 60_000);
    await Bun.sleep(250);
    const state = await q.getJobState(job.id);
    release();
    await w.close();
    await q.close();

    // RED: state comes back 'waiting' (delay dropped → NaN runAt) instead of 'delayed'.
    expect(state).toBe('delayed');
  }, 45000);
});
