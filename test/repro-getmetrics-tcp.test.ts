/**
 * REPRO — queue.getMetrics('completed'|'failed') over TCP always returns count: 0.
 *
 * Run: bun test test/repro-getmetrics-tcp.test.ts
 *
 * The TCP `Metrics` handler (src/infrastructure/server/handlers/management.ts:
 * handleMetrics) returns resp.metrics({ totalCompleted, totalFailed, ... }) →
 * response shape { ok, metrics: { totalCompleted, totalFailed, ... } }. But the
 * client (src/client/queue/workers.ts: getMetrics) reads `response.stats.completed`
 * / `response.stats.dlq` — a `stats` key that does not exist on a Metrics response.
 * So the count is always 0 regardless of how many jobs completed/failed. Embedded
 * mode reads getStats().completed and is unaffected.
 *
 * Asserts the CORRECT behavior → RED on current code (0), GREEN once workers.ts reads
 * response.metrics.totalCompleted / .totalFailed. Spawns its own server; no src/ edits.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Queue, Worker } from '../src/client';

const PORT = 17411;
const HTTP = 17412;
const DB = join(tmpdir(), `bunq-getmetrics-tcp-${process.pid}.db`);
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

describe('REPRO: getMetrics over TCP returns 0', () => {
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
    await waitPort(HTTP);
  });

  afterAll(async () => {
    proc?.kill();
    await Bun.sleep(150);
    cleanDb();
  });

  test("getMetrics('completed') reflects processed jobs over TCP", async () => {
    const q = new Queue('gm', { connection: { host: '127.0.0.1', port: PORT }, embedded: false });
    const N = 25;
    for (let i = 0; i < N; i++) await q.add('t', { i });

    let done = 0;
    const w = new Worker('gm', async () => { done++; return { ok: true }; }, {
      connection: { host: '127.0.0.1', port: PORT },
      embedded: false,
      concurrency: 5,
    });
    const t0 = Date.now();
    while (done < N && Date.now() - t0 < 15000) await Bun.sleep(50);
    await Bun.sleep(500); // let server-side totalCompleted settle
    await w.close();

    expect(done).toBe(N);

    const m = await q.getMetrics('completed');
    // RED today: count is 0 (client reads non-existent response.stats.completed).
    expect(m.meta.count).toBeGreaterThanOrEqual(N);

    await q.close();
  });
});
