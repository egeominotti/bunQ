/**
 * REPRO — "#111 class": client/handler silently drops a supported option.
 *
 * Three suspected drops, each asserted as the DESIRED behavior so the test is
 * RED today and turns GREEN when fixed:
 *
 * CLAIM A — TcpConnectionPool drops `maxInFlight` / `pipelining`.
 *   Queue (TCP mode) forwards connection.maxInFlight/pipelining into PoolOptions
 *   (src/client/queue/queue.ts:91-92 and :105-106). TcpConnectionPool RESOLVES
 *   them into this.options (src/client/tcpPool.ts:45-46) but OMITS both fields
 *   when constructing each TcpClient (src/client/tcpPool.ts:51-65), so every
 *   client falls back to DEFAULT_CONNECTION (maxInFlight: 100, pipelining: true
 *   — src/client/tcp/types.ts:97-98 via src/client/tcp/client.ts:108). A Queue
 *   configured with maxInFlight: 512 runs with a 100-wide window.
 *
 * CLAIM B — PUSHB skips the validation PUSH enforces.
 *   handlePush runs validateJobOptions + a dependsOn-existence gate
 *   (src/infrastructure/server/handlers/core.ts:31-57). handlePushBatch runs
 *   ONLY validateQueueName + per-job validateJobData
 *   (src/infrastructure/server/handlers/core.ts:102-117) — no option bounds,
 *   no dependsOn gate. A job PUSH rejects is silently accepted via PUSHB; a
 *   dependsOn on a nonexistent id lands in shard.waitingDeps forever
 *   (src/application/operations/push.ts:218-234) — a permanent orphan.
 *
 * CLAIM C — PULLB without owner hardcodes timeout 0 (long-poll dropped).
 *   Non-owner PULL honors cmd.timeout (core.ts:148 → pullJob long-polls,
 *   src/application/operations/pull.ts:211-249). Non-owner PULLB calls
 *   pullBatch(queue, count, 0) — cmd.timeout discarded
 *   (src/infrastructure/server/handlers/core.ts:187) — even though
 *   pullJobBatch fully supports long-poll (pull.ts:291-338) and the OWNER
 *   branch forwards cmd.timeout ?? 0 (core.ts:170-176).
 *
 * Harness: spawns the REAL server (pattern from repro-chaos-crash-recovery:
 * getFreePort, waitPort 60s budget, explicit per-test timeouts). Wire access
 * uses TcpClient.send() with raw command objects — the exact frames the server
 * parses; nothing in the client rewrites PUSH/PUSHB/PULL/PULLB payloads.
 */

import { afterAll, describe, expect, it } from 'bun:test';
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Queue } from '../src/client/queue/queue';
import { TcpClient } from '../src/client/tcp/client';

type Subprocess = ReturnType<typeof Bun.spawn>;

// ---------------------------------------------------------------------------
// Server harness (lazy singleton: one real-server spawn shared by claims B+C)
// ---------------------------------------------------------------------------

interface Server {
  proc: Subprocess;
  port: number;
  db: string;
}

let server: Server | null = null;
let serverPromise: Promise<Server> | null = null;
const clients: TcpClient[] = [];

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

// 60s: CI runners cold-start the real server much slower than a dev machine
async function waitPort(port: number, timeoutMs = 60000): Promise<void> {
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

async function ensureServer(): Promise<Server> {
  if (serverPromise) return serverPromise;
  serverPromise = (async () => {
    const db = join(tmpdir(), `bunq-optdrop-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const proc = Bun.spawn([process.execPath, 'run', 'src/main.ts'], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        BUNQUEUE_EMBEDDED: '',
        AUTH_TOKENS: '',
        TCP_PORT: String(port),
        HTTP_PORT: String(port + 1),
        BUNQUEUE_DATA_PATH: db,
        LOG_LEVEL: 'error',
      },
      stdout: 'ignore',
      stderr: 'ignore',
    });
    await waitPort(port);
    server = { proc, port, db };
    return server;
  })();
  return serverPromise;
}

/** Fresh connected raw client to the shared server. */
async function connect(): Promise<TcpClient> {
  const s = await ensureServer();
  const c = new TcpClient({
    host: '127.0.0.1',
    port: s.port,
    autoReconnect: false,
    pingInterval: 0,
    commandTimeout: 10000,
    connectTimeout: 5000,
  });
  await c.connect();
  clients.push(c);
  return c;
}

function cleanDb(db: string): void {
  for (const suffix of ['', '-wal', '-shm']) {
    const f = `${db}${suffix}`;
    if (existsSync(f)) rmSync(f, { force: true });
  }
}

afterAll(async () => {
  for (const c of clients) {
    try {
      c.close();
    } catch {
      /* ignore */
    }
  }
  clients.length = 0;
  if (server) {
    try {
      server.proc.kill(9);
      await server.proc.exited;
    } catch {
      /* ignore */
    }
    cleanDb(server.db);
    server = null;
  }
});

// ---------------------------------------------------------------------------
// CLAIM A — TcpConnectionPool drops maxInFlight / pipelining
// ---------------------------------------------------------------------------
// White-box on CLIENT internals (allowed — it is not a src/ modification):
// the pool's resolved options prove the value ARRIVED at the pool; the
// constructed TcpClient's options prove whether it was FORWARDED. No server
// or connection is needed: both objects are fully built in the constructors.

interface PoolInternals {
  options: { maxInFlight: number; pipelining: boolean };
  clients: Array<{ options?: { maxInFlight: number; pipelining: boolean } }>;
}

describe('CLAIM A — pool must forward maxInFlight/pipelining to its TcpClients (tcpPool.ts:51-65)', () => {
  it('a Queue configured with connection.maxInFlight: 512 gets TcpClients with a 512-wide window (not the default 100)', () => {
    // poolSize !== 4 forces the DEDICATED pool path (queue.ts:96-107), immune
    // to shared-pool contamination from other tests in this process.
    const q = new Queue('optdrop-a1', {
      embedded: false,
      autoBatch: { enabled: false },
      connection: { host: '127.0.0.1', port: getFreePort(), poolSize: 2, maxInFlight: 512 },
    });
    try {
      const pool = (q as unknown as { tcpPool: PoolInternals }).tcpPool;
      expect(pool).toBeTruthy();

      // Intake is correct: the pool itself resolved maxInFlight to 512
      // (tcpPool.ts:46) — this passes today and localizes the drop to the
      // pool→TcpClient boundary.
      expect(pool.options.maxInFlight).toBe(512);
      expect(pool.clients.length).toBe(2);

      // DESIRED: every constructed TcpClient runs with the configured window.
      // TODAY: TcpClient ctor never receives maxInFlight (tcpPool.ts:51-65) and
      // falls back to DEFAULT_CONNECTION.maxInFlight = 100 (tcp/types.ts:98),
      // which caps processQueue() pipelining (tcp/client.ts:422). → RED
      for (const client of pool.clients) {
        const effective = (client as unknown as { options: { maxInFlight: number } }).options
          .maxInFlight;
        console.log(`[CLAIM-A] configured maxInFlight=512, TcpClient effective=${effective}`);
        expect(effective).toBe(512);
      }
    } finally {
      q.close();
    }
  }, 10000);

  it('a Queue configured with connection.pipelining: false gets TcpClients with pipelining disabled (not the default true)', () => {
    const q = new Queue('optdrop-a2', {
      embedded: false,
      autoBatch: { enabled: false },
      connection: { host: '127.0.0.1', port: getFreePort(), poolSize: 2, pipelining: false },
    });
    try {
      const pool = (q as unknown as { tcpPool: PoolInternals }).tcpPool;
      expect(pool).toBeTruthy();

      // Intake is correct: the pool resolved pipelining to false (tcpPool.ts:45).
      expect(pool.options.pipelining).toBe(false);

      // DESIRED: the constructed clients honor pipelining: false.
      // TODAY: dropped at tcpPool.ts:51-65 → DEFAULT_CONNECTION.pipelining=true. → RED
      for (const client of pool.clients) {
        const effective = (client as unknown as { options: { pipelining: boolean } }).options
          .pipelining;
        console.log(`[CLAIM-A] configured pipelining=false, TcpClient effective=${effective}`);
        expect(effective).toBe(false);
      }
    } finally {
      q.close();
    }
  }, 10000);
});

// ---------------------------------------------------------------------------
// CLAIM B — PUSHB skips validateJobOptions + dependsOn existence gate
// ---------------------------------------------------------------------------

describe('CLAIM B — PUSHB must enforce the same validation as PUSH (handlers/core.ts:102-117 vs :31-57)', () => {
  it('an out-of-bounds option rejected by PUSH is also rejected by PUSHB', async () => {
    const c = await connect();
    const QUEUE = 'optdrop-b1';
    // priority bound is ±1_000_000 (protocol.ts:128). 2_000_000 is invalid.
    const BAD_PRIORITY = 2_000_000;

    // Baseline (GREEN today and after the fix): PUSH rejects it.
    const single = await c.send({
      cmd: 'PUSH',
      queue: QUEUE,
      data: { via: 'PUSH' },
      priority: BAD_PRIORITY,
    });
    expect(single.ok).toBe(false);
    expect(String(single.error)).toContain('priority');

    // The SAME job inside a PUSHB.
    const batch = await c.send({
      cmd: 'PUSHB',
      queue: QUEUE,
      jobs: [{ data: { via: 'PUSHB' }, priority: BAD_PRIORITY }],
    });

    if (batch.ok) {
      // Bug present — collect evidence: the invalid job was actually created.
      const ids = batch.ids as string[];
      const got = await c.send({ cmd: 'GetJob', queue: QUEUE, id: ids[0] });
      const prio = (got.job as { priority?: number } | null)?.priority;
      console.log(
        `[CLAIM-B] PUSH rejected priority=${BAD_PRIORITY} ("${String(single.error)}") but ` +
          `PUSHB accepted the identical job: id=${ids[0]}, stored priority=${prio}`
      );
    }

    // DESIRED: PUSHB rejects exactly like PUSH. TODAY: handlePushBatch never
    // calls validateJobOptions (core.ts:110-113) → ok:true with ids. → RED
    expect(batch.ok).toBe(false);
  }, 90000);

  it('a dependsOn referencing a nonexistent job, rejected by PUSH, is also rejected by PUSHB (instead of creating a forever-orphan)', async () => {
    const c = await connect();
    const QUEUE = 'optdrop-b2';
    const GHOST_DEP = 'no-such-job-id-12345';

    // Baseline (GREEN today and after the fix): PUSH gates dependsOn existence.
    const single = await c.send({
      cmd: 'PUSH',
      queue: QUEUE,
      data: { via: 'PUSH' },
      dependsOn: [GHOST_DEP],
    });
    expect(single.ok).toBe(false);
    expect(String(single.error)).toContain('Dependency job not found');

    // The SAME job inside a PUSHB.
    const batch = await c.send({
      cmd: 'PUSHB',
      queue: QUEUE,
      jobs: [{ data: { via: 'PUSHB' }, dependsOn: [GHOST_DEP] }],
    });

    if (batch.ok) {
      // Bug present — prove the orphan: the accepted job sits in
      // shard.waitingDeps (push.ts:226-228) on a dependency that can NEVER
      // complete, so it never becomes pullable (bounded poll), yet it exists.
      const ids = batch.ids as string[];
      const orphanId = ids[0];

      let pulled: unknown = null;
      const deadline = Date.now() + 1500; // > dependency processor interval (100ms)
      while (Date.now() < deadline) {
        const p = await c.send({ cmd: 'PULL', queue: QUEUE, timeout: 0 });
        if (p.job) {
          pulled = p.job;
          break;
        }
        await Bun.sleep(100);
      }
      const st = await c.send({ cmd: 'GetState', queue: QUEUE, id: orphanId });
      console.log(
        `[CLAIM-B] PUSH rejected dependsOn=[${GHOST_DEP}] ("${String(single.error)}") but ` +
          `PUSHB accepted it: id=${orphanId}, state=${String(st.state)}, ` +
          `pullable within 1.5s=${pulled !== null} (expected: never — permanent orphan)`
      );
    }

    // DESIRED: PUSHB runs the same dependsOn existence gate as PUSH
    // (core.ts:42-57). TODAY: handlePushBatch has no such gate → ok:true and
    // the job is stranded in waitingDeps forever. → RED
    expect(batch.ok).toBe(false);
  }, 90000);

  it('an intra-batch chain is accepted regardless of order in the array (auto-batch groups concurrent adds arbitrarily)', async () => {
    const c = await connect();
    const QUEUE = 'optdrop-b3';

    // FORWARD reference: the child appears BEFORE its parent in the batch.
    // The default-on auto-batcher can produce exactly this ordering from
    // Promise.all([add(child, {dependsOn:[p1]}), add(parent, {jobId:p1})]),
    // and the readiness layer resolves the chain once the batch is applied,
    // so the validation gate must accept it.
    const batch = await c.send({
      cmd: 'PUSHB',
      queue: QUEUE,
      jobs: [
        { data: { role: 'child' }, customId: 'b3-child', dependsOn: ['b3-parent'] },
        { data: { role: 'parent' }, customId: 'b3-parent' },
      ],
    });
    expect(batch.ok).toBe(true);
    expect((batch.ids as string[]).length).toBe(2);

    // Prove the chain actually resolves: pull+ack the parent, then the child
    // must become pullable (bounded poll, dependency flush is event-driven).
    const first = await c.send({ cmd: 'PULL', queue: QUEUE, timeout: 0 });
    expect(first.job).toBeTruthy();
    expect((first.job as { id: string }).id).toBe('b3-parent');
    await c.send({ cmd: 'ACK', id: 'b3-parent' });

    let child: unknown = null;
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      const p = await c.send({ cmd: 'PULL', queue: QUEUE, timeout: 0 });
      if (p.job) {
        child = p.job;
        break;
      }
      await Bun.sleep(50);
    }
    expect((child as { id: string } | null)?.id).toBe('b3-child');
  }, 90000);

  it('a self-referencing dependsOn is rejected (it can never resolve)', async () => {
    const c = await connect();
    const batch = await c.send({
      cmd: 'PUSHB',
      queue: 'optdrop-b4',
      jobs: [{ data: {}, customId: 'b4-self', dependsOn: ['b4-self'] }],
    });
    expect(batch.ok).toBe(false);
    expect(String(batch.error)).toContain('depend on itself');
  }, 90000);
});

// ---------------------------------------------------------------------------
// CLAIM C — non-owner PULLB hardcodes timeout 0 (long-poll dropped)
// ---------------------------------------------------------------------------

describe('CLAIM C — non-owner PULLB must honor its timeout like PULL does (handlers/core.ts:187)', () => {
  it('baseline: non-owner PULL {timeout:1500} on an empty queue long-polls ~1500ms', async () => {
    const c = await connect();
    const QUEUE = 'optdrop-c-baseline'; // never receives jobs

    const t0 = performance.now();
    const res = await c.send({ cmd: 'PULL', queue: QUEUE, timeout: 1500 });
    const elapsed = performance.now() - t0;

    console.log(`[CLAIM-C] PULL timeout=1500 (no owner) → ${Math.round(elapsed)}ms, job=null`);
    expect(res.ok).toBe(true);
    expect(res.job ?? null).toBeNull();
    // Long-poll honored (core.ts:148 → pullJob deadline loop, pull.ts:217-247).
    expect(elapsed).toBeGreaterThanOrEqual(1300);
  }, 90000);

  it('non-owner PULLB {count:5, timeout:1500} on an empty queue must also long-poll ~1500ms (not return immediately)', async () => {
    const c = await connect();
    const QUEUE = 'optdrop-c-nolock'; // never receives jobs

    const t0 = performance.now();
    const res = await c.send({ cmd: 'PULLB', queue: QUEUE, count: 5, timeout: 1500 });
    const elapsed = performance.now() - t0;

    console.log(
      `[CLAIM-C] PULLB count=5 timeout=1500 (no owner) → ${Math.round(elapsed)}ms, ` +
        `jobs=${(res.jobs as unknown[]).length}`
    );
    expect(res.ok).toBe(true);
    expect((res.jobs as unknown[]).length).toBe(0);

    // DESIRED: the non-owner branch forwards cmd.timeout to pullBatch, which
    // fully supports long-poll (pull.ts:291-338). TODAY: core.ts:187 hardcodes
    // `pullBatch(cmd.queue, cmd.count, 0)` → returns immediately. → RED
    expect(elapsed).toBeGreaterThanOrEqual(1300);
  }, 90000);

  it('control: owner PULLB {count:5, timeout:1500} long-polls ~1500ms (owner branch forwards cmd.timeout, core.ts:170-176)', async () => {
    const c = await connect();
    const QUEUE = 'optdrop-c-owner'; // never receives jobs

    const t0 = performance.now();
    const res = await c.send({
      cmd: 'PULLB',
      queue: QUEUE,
      count: 5,
      timeout: 1500,
      owner: 'repro-owner',
      lockTtl: 30000,
    });
    const elapsed = performance.now() - t0;

    console.log(
      `[CLAIM-C] PULLB count=5 timeout=1500 owner=repro-owner → ${Math.round(elapsed)}ms, ` +
        `jobs=${(res.jobs as unknown[]).length}`
    );
    expect(res.ok).toBe(true);
    expect((res.jobs as unknown[]).length).toBe(0);
    // GREEN today: proves the timeout machinery works for PULLB when the
    // owner branch forwards it — isolating the drop to the non-owner branch.
    expect(elapsed).toBeGreaterThanOrEqual(1300);
  }, 90000);
});
