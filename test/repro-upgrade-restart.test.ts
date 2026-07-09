/**
 * UPGRADE / RESTART (category 7) — graceful restarts and data continuity.
 *
 * A 24/7 service is restarted for deploys constantly. Unlike a crash (SIGKILL),
 * a graceful shutdown (SIGTERM) must lose NOTHING — including buffered
 * (non-durable) jobs, because the shutdown path flushes the write buffer and
 * checkpoints the WAL before exit (src/infrastructure/persistence/sqlite.ts
 * close() → writeBuffer.flush() + wal_checkpoint(TRUNCATE)). These tests assert:
 *
 *   - GRACEFUL RESTART LOSES NO BUFFERED JOB: jobs pushed WITHOUT durable:true
 *     (which may be lost in the ~10ms window on a hard crash) survive a SIGTERM
 *     restart intact.
 *   - FULL STATE ROUND-TRIPS: waiting jobs, completed jobs + their results, a
 *     paused queue, and a DLQ entry all survive a graceful restart.
 *   - ROLLING RESTART UNDER LOAD: repeated SIGTERM+restart cycles amid pushes
 *     lose nothing cumulatively — a legacy DB reopens cleanly under the running
 *     code each cycle (recovery/migration path).
 */

import { afterAll, afterEach, describe, expect, it } from 'bun:test';
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { TcpClient } from '../src/client/tcp/client';

type Subprocess = ReturnType<typeof Bun.spawn>;

interface Server {
  proc: Subprocess;
  port: number;
  db: string;
}

let active: Server | null = null;
let activeClient: TcpClient | null = null;

function randomPort(): number {
  return 20000 + Math.floor(Math.random() * 20000);
}

/** A port confirmed free right now (bind+close), to avoid cross-test collisions. */
function getFreePort(): number {
  for (let i = 0; i < 50; i++) {
    const port = randomPort();
    try {
      const l = Bun.listen({ hostname: '127.0.0.1', port, socket: { data() {} } });
      l.stop();
      return port;
    } catch {
      /* taken — try another */
    }
  }
  throw new Error('no free port found');
}

async function waitPort(port: number, timeoutMs = 15000): Promise<void> {
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

async function spawnServer(db: string, port: number): Promise<Server> {
  const proc = Bun.spawn([process.execPath, 'run', 'src/main.ts'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      BUNQUEUE_EMBEDDED: '',
      TCP_PORT: String(port),
      HTTP_PORT: String(port + 1),
      BUNQUEUE_DATA_PATH: db,
      LOG_LEVEL: 'error',
      SHUTDOWN_TIMEOUT_MS: '5000',
    },
    stdout: 'ignore',
    stderr: 'ignore',
  });
  await waitPort(port);
  const s: Server = { proc, port, db };
  active = s;
  return s;
}

async function connect(port: number): Promise<TcpClient> {
  if (activeClient) {
    try {
      activeClient.close();
    } catch {
      /* ignore */
    }
  }
  const c = new TcpClient({
    host: '127.0.0.1',
    port,
    autoReconnect: false,
    pingInterval: 0,
    commandTimeout: 5000,
    connectTimeout: 5000,
  });
  await c.connect();
  activeClient = c;
  return c;
}

/** SIGTERM = graceful shutdown (flush write buffer + checkpoint WAL, then exit). */
async function gracefulStop(s: Server): Promise<void> {
  if (activeClient) {
    try {
      activeClient.close();
    } catch {
      /* ignore */
    }
    activeClient = null;
  }
  s.proc.kill('SIGTERM');
  await s.proc.exited;
  await Bun.sleep(150); // let the OS release the port
}

function cleanDb(db: string): void {
  for (const suffix of ['', '-wal', '-shm']) {
    const f = `${db}${suffix}`;
    if (existsSync(f)) rmSync(f, { force: true });
  }
}

async function stateOf(c: TcpClient, queue: string, id: string): Promise<string> {
  const st = await c.send({ cmd: 'GetState', queue, id });
  return String(st.state);
}

afterEach(async () => {
  if (activeClient) {
    try {
      activeClient.close();
    } catch {
      /* ignore */
    }
    activeClient = null;
  }
  if (active) {
    try {
      active.proc.kill(9);
      await active.proc.exited;
    } catch {
      /* ignore */
    }
    cleanDb(active.db);
    active = null;
  }
});

// Safety net for a mid-test timeout: never leave a spawned child holding a port.
afterAll(async () => {
  if (active) {
    try {
      active.proc.kill(9);
      await active.proc.exited;
    } catch {
      /* ignore */
    }
    cleanDb(active.db);
    active = null;
  }
});

describe('UPGRADE/RESTART — graceful restart continuity', () => {
  it('U1: buffered (non-durable) jobs survive a graceful SIGTERM restart — flush-on-shutdown loses nothing', async () => {
    const db = join(tmpdir(), `bunq-u1-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const QUEUE = 'u1';
    const N = 200;

    let s = await spawnServer(db, port);
    let c = await connect(port);

    // Push NON-durable jobs. Their ok returns before the disk write; only the
    // write-buffer flush (periodic OR on graceful shutdown) persists them.
    const ids: string[] = [];
    for (let i = 0; i < N; i++) {
      const p = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { i } }); // NOT durable
      expect(p.ok).toBe(true);
      ids.push(String(p.id));
    }

    // Graceful restart. The shutdown path MUST flush the buffer → no loss.
    await gracefulStop(s);
    s = await spawnServer(db, port);
    c = await connect(port);

    // Every buffered job survived.
    let present = 0;
    for (const id of ids) {
      if ((await stateOf(c, QUEUE, id)) !== 'unknown') present++;
    }
    expect(present).toBe(N);

    // And the queue drains cleanly post-restart.
    const drained = new Set<string>();
    const deadline = Date.now() + 10000;
    while (drained.size < N && Date.now() < deadline) {
      const pull = await c.send({ cmd: 'PULL', queue: QUEUE, timeout: 0 });
      const job = pull.job as { id: string } | null;
      if (!job?.id) {
        await Bun.sleep(20);
        continue;
      }
      drained.add(job.id);
      await c.send({ cmd: 'ACK', id: job.id });
    }
    expect(drained.size).toBe(N);
  }, 40000);

  it('U2: waiting + completed(+result) + paused + DLQ state all round-trip through a graceful restart', async () => {
    const db = join(tmpdir(), `bunq-u2-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();

    let s = await spawnServer(db, port);
    let c = await connect(port);

    // (a) a waiting durable job
    const waitId = String(
      (await c.send({ cmd: 'PUSH', queue: 'u2-wait', data: { kind: 'wait' }, durable: true })).id
    );

    // (b) a completed job with a stored result
    const doneId = String(
      (await c.send({ cmd: 'PUSH', queue: 'u2-done', data: { kind: 'done' }, durable: true })).id
    );
    const pull = await c.send({ cmd: 'PULL', queue: 'u2-done', owner: 'w', lockTtl: 30000 });
    await c.send({
      cmd: 'ACK',
      id: doneId,
      token: pull.token as string,
      result: { answer: 42 },
    });

    // (c) a paused queue with a job in it
    const pausedId = String(
      (await c.send({ cmd: 'PUSH', queue: 'u2-paused', data: { kind: 'paused' }, durable: true }))
        .id
    );
    await c.send({ cmd: 'Pause', queue: 'u2-paused' });

    // (d) a DLQ entry
    const dlqId = String(
      (await c.send({ cmd: 'PUSH', queue: 'u2-dlq', data: { kind: 'dlq' }, durable: true })).id
    );
    const dlqPull = await c.send({ cmd: 'PULL', queue: 'u2-dlq', owner: 'w', lockTtl: 30000 });
    await c.send({
      cmd: 'FAIL',
      id: dlqId,
      token: dlqPull.token as string,
      error: 'boom',
      unrecoverable: true,
    });

    await Bun.sleep(300); // let all state persist
    await gracefulStop(s);
    s = await spawnServer(db, port);
    c = await connect(port);

    // (a) waiting job still waiting
    expect(['waiting', 'prioritized', 'delayed']).toContain(await stateOf(c, 'u2-wait', waitId));
    // (b) completed job still completed, result intact
    expect(await stateOf(c, 'u2-done', doneId)).toBe('completed');
    const result = await c.send({ cmd: 'GetResult', queue: 'u2-done', id: doneId });
    expect((result.result as { answer: number })?.answer).toBe(42);
    // (c) paused queue still paused, its job held
    expect((await c.send({ cmd: 'IsPaused', queue: 'u2-paused' })).paused).toBe(true);
    expect(await stateOf(c, 'u2-paused', pausedId)).not.toBe('unknown');
    // (d) DLQ entry survived
    const dlq = await c.send({ cmd: 'Dlq', queue: 'u2-dlq', count: 100 });
    expect((dlq.jobs as unknown[]).some((j) => String((j as { id: string }).id) === dlqId)).toBe(
      true
    );
  }, 40000);

  it('U3: rolling restart — 8 graceful restart cycles amid durable pushes lose nothing cumulatively', async () => {
    const CYCLES = Number(process.env.ROLLING_CYCLES ?? 8);
    const db = join(tmpdir(), `bunq-u3-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const QUEUE = 'u3';
    const allIds: string[] = [];

    let s = await spawnServer(db, port);
    let c = await connect(port);

    for (let cycle = 0; cycle < CYCLES; cycle++) {
      // Push a batch (mix durable + buffered — graceful flush covers both).
      for (let i = 0; i < 8; i++) {
        const durable = i % 2 === 0;
        const p = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { cycle, i }, durable });
        if (p.ok) allIds.push(String(p.id));
      }
      // Complete a couple to exercise completed-state persistence across cycles.
      for (let i = 0; i < 2; i++) {
        const pull = await c.send({
          cmd: 'PULL',
          queue: QUEUE,
          owner: 'w',
          lockTtl: 30000,
          timeout: 0,
        });
        const job = pull.job as { id: string } | null;
        if (job?.id) await c.send({ cmd: 'ACK', id: job.id, token: pull.token as string });
      }

      // Graceful restart mid-load.
      await gracefulStop(s);
      s = await spawnServer(db, port);
      c = await connect(port);

      // Cumulative: nothing lost across the rolling restarts.
      for (const id of allIds) {
        expect(await stateOf(c, QUEUE, id)).not.toBe('unknown');
      }
    }

    // Everything ultimately drains (completed jobs stay completed; the rest run).
    const remaining = new Set<string>();
    for (const id of allIds) {
      if ((await stateOf(c, QUEUE, id)) !== 'completed') remaining.add(id);
    }
    const drained = new Set<string>();
    const deadline = Date.now() + 15000;
    while (drained.size < remaining.size && Date.now() < deadline) {
      const pull = await c.send({
        cmd: 'PULL',
        queue: QUEUE,
        owner: 'w',
        lockTtl: 30000,
        timeout: 0,
      });
      const job = pull.job as { id: string } | null;
      if (!job?.id) {
        await Bun.sleep(40);
        continue;
      }
      if (remaining.has(job.id)) drained.add(job.id);
      await c.send({ cmd: 'ACK', id: job.id, token: pull.token as string });
    }
    // No permanent orphan: every not-yet-completed job was drainable.
    expect(drained.size).toBe(remaining.size);
  }, 90000);
});
