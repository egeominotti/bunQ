/**
 * Test: Issue #111 over TCP — upsertJobScheduler must forward `limit` -> maxLimit
 * and surface it back through SchedulerInfo on the server (TCP) path too.
 *
 * Spawns its own server (embedded:false); DOES NOT touch src/.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Queue } from '../src/client';

const PORT = 17631;
const HTTP = 17632;
const DB = join(tmpdir(), `bunq-111-tcp-${process.pid}.db`);
// biome-ignore lint/suspicious/noExplicitAny: Bun.Subprocess
let proc: any;

async function waitPort(port: number, timeoutMs = 15000) {
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

describe('Issue #111 over TCP: scheduler limit', () => {
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
  });

  afterAll(async () => {
    proc?.kill();
    await Bun.sleep(150);
    cleanDb();
  });

  test('limit round-trips through Cron/CronList over TCP', async () => {
    const q = new Queue('issue-111-tcp', {
      connection: { host: '127.0.0.1', port: PORT },
      embedded: false,
    });

    const info = await q.upsertJobScheduler(
      'sched-tcp-limit',
      { every: 60_000, limit: 7 },
      { name: 'job', data: { a: 1 } }
    );
    // upsert result echoes the requested cap immediately.
    expect(info?.limit).toBe(7);

    // Read back over the wire (CronList) — server must have persisted max_limit.
    const all = await q.getJobSchedulers();
    const found = all.find((s) => s.id === 'sched-tcp-limit');
    expect(found?.limit).toBe(7);

    const one = await q.getJobScheduler('sched-tcp-limit');
    expect(one?.limit).toBe(7);

    await q.close();
  });

  test('no limit -> undefined over TCP (no regression)', async () => {
    const q = new Queue('issue-111-tcp-nolimit', {
      connection: { host: '127.0.0.1', port: PORT },
      embedded: false,
    });
    await q.upsertJobScheduler('sched-tcp-nolimit', { every: 60_000 }, { data: {} });
    const one = await q.getJobScheduler('sched-tcp-nolimit');
    expect(one?.limit).toBeUndefined();
    await q.close();
  });
});
