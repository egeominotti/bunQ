/**
 * DURABILITY GAPS — adversarial suites closing two holes a reviewer identified.
 *
 * GROUP 1 — client disconnect with buffered ACKs (the SDK ackBatch path).
 * A worker that batches ACKs (ACKB) holds completed-but-unsent acknowledgments
 * in a client-side buffer. If the connection dies BEFORE the batch flushes,
 * those jobs must NOT be treated as completed by the server: they are
 * redelivered (via connection-release on close and/or lock expiry) and can be
 * completed by another connection. Jobs whose ACKB *did* reach the server stay
 * completed — they are never resurrected by the disconnect, by the release
 * path, or by a later lock-expiry sweep. Total completions == N, no job
 * completed twice, no job lost.
 *
 * GROUP 2 — WAL/db damage at startup after SIGKILL.
 * SQLite-by-design outcomes we pin down against the REAL server binary:
 *   - truncated -wal  → the valid WAL prefix is replayed: committed durable
 *     jobs up to the truncation point survive (a strict PREFIX of push order),
 *     everything after is gone, and nothing phantom appears.
 *   - garbaged -wal header → the whole WAL is discarded (invalid header ==
 *     no valid frames): checkpointed data in the main db is intact, WAL-only
 *     commits are lost, no invented data, server starts and serves.
 *   - corrupted main .db → the server must either refuse to start (non-zero
 *     exit) or start and demonstrably serve correct data — never wedge, never
 *     silently serve garbage.
 *
 * Harness: same pattern as test/repro-chaos-crash-recovery.test.ts — the REAL
 * server is spawned as a subprocess (src/main.ts) with an on-disk SQLite DB in
 * tmpdir. test/preload.ts forces embedded mode for in-process clients, so all
 * traffic here goes over real TCP: raw framed msgpack sockets (node:net, so we
 * can destroy() mid-write) or explicit TcpClient instances.
 */

import { afterAll, afterEach, describe, expect, it } from 'bun:test';
import {
  closeSync,
  existsSync,
  openSync,
  rmSync,
  statSync,
  truncateSync,
  writeSync,
} from 'node:fs';
import { createConnection as netConnect, type Socket as NetSocket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';
import { pack, unpack } from 'msgpackr';
import { FrameParser } from '../src/infrastructure/server/protocol';
import { TcpClient } from '../src/client/tcp/client';
import { AckBatcher } from '../src/client/worker/ackBatcher';

type Subprocess = ReturnType<typeof Bun.spawn>;

interface Server {
  proc: Subprocess;
  port: number;
  db: string;
}

/** Every state string GetState may legally return. Anything else is garbage. */
const VALID_STATES = new Set([
  'waiting',
  'prioritized',
  'delayed',
  'active',
  'paused',
  'completed',
  'failed',
  'waiting-children',
  'unknown',
]);

const servers: Server[] = [];
const rawConns: RawConn[] = [];
const tcpClients: TcpClient[] = [];

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

// 60s: CI runners cold-start the real server binary much slower than a dev machine
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

function serverEnv(db: string, port: number): Record<string, string> {
  return {
    ...process.env,
    BUNQUEUE_EMBEDDED: '',
    TCP_PORT: String(port),
    HTTP_PORT: String(port + 1),
    BUNQUEUE_DATA_PATH: db,
    LOG_LEVEL: 'error',
  } as Record<string, string>;
}

/** Spawn the real server against a given db + port. Waits until it accepts TCP. */
async function spawnServer(db: string, port: number): Promise<Server> {
  const proc = Bun.spawn([process.execPath, 'run', 'src/main.ts'], {
    cwd: process.cwd(),
    env: serverEnv(db, port),
    stdout: 'ignore',
    stderr: 'ignore',
  });
  await waitPort(port);
  const s: Server = { proc, port, db };
  servers.push(s);
  return s;
}

/**
 * Spawn where startup MAY legitimately fail (corrupted main db, W3). Resolves
 * once the server either accepts TCP or exits on its own. A server that does
 * neither within the window is wedged — that is always a failure.
 */
async function spawnServerTolerant(
  db: string,
  port: number,
  windowMs = 45000
): Promise<{ server: Server; started: boolean }> {
  const proc = Bun.spawn([process.execPath, 'run', 'src/main.ts'], {
    cwd: process.cwd(),
    env: serverEnv(db, port),
    stdout: 'ignore',
    stderr: 'ignore',
  });
  const s: Server = { proc, port, db };
  servers.push(s);
  const deadline = Date.now() + windowMs;
  while (Date.now() < deadline) {
    if (proc.exitCode !== null) return { server: s, started: false };
    try {
      const c = await Bun.connect({ hostname: '127.0.0.1', port, socket: { data() {} } });
      c.end();
      return { server: s, started: true };
    } catch {
      await Bun.sleep(150);
    }
  }
  throw new Error(`server on :${port} neither started nor exited within ${windowMs}ms (wedged)`);
}

/** SIGKILL the server and wait for the OS to reap it (no graceful shutdown). */
async function hardKill(s: Server): Promise<void> {
  s.proc.kill(9); // SIGKILL — violent, no flush, no WAL checkpoint
  await s.proc.exited;
  // Small settle so the OS releases the listening port for the restart.
  await Bun.sleep(200);
}

function cleanDb(db: string): void {
  for (const suffix of ['', '-wal', '-shm']) {
    const f = `${db}${suffix}`;
    if (existsSync(f)) rmSync(f, { force: true });
  }
}

// ---------------------------------------------------------------------------
// Raw framed-msgpack TCP client over node:net — gives us destroy() (hard RST
// semantics), partial-frame writes, and full control over what is or is not
// ever put on the wire (the "client-side buffered ACK" being simulated).
// ---------------------------------------------------------------------------

interface PendingReq {
  resolve: (r: Record<string, unknown>) => void;
  reject: (e: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

class RawConn {
  private readonly parser = new FrameParser();
  private readonly pending = new Map<string, PendingReq>();
  private seq = 0;
  private closed = false;

  private constructor(private readonly sock: NetSocket) {}

  static open(port: number): Promise<RawConn> {
    return new Promise((resolve, reject) => {
      let settled = false;
      const sock = netConnect({ host: '127.0.0.1', port });
      const conn = new RawConn(sock);
      sock.on('connect', () => {
        settled = true;
        rawConns.push(conn);
        resolve(conn);
      });
      sock.on('data', (chunk) => conn.onData(chunk));
      sock.on('error', (err) => {
        if (!settled) {
          settled = true;
          reject(err);
        }
        conn.failAll(err instanceof Error ? err : new Error(String(err)));
      });
      sock.on('close', () => {
        conn.closed = true;
        conn.failAll(new Error('socket closed'));
      });
    });
  }

  private onData(chunk: Buffer): void {
    for (const frame of this.parser.addData(chunk)) {
      const resp = unpack(frame) as Record<string, unknown>;
      const p = this.pending.get(String(resp['reqId']));
      if (p) {
        this.pending.delete(String(resp['reqId']));
        clearTimeout(p.timer);
        p.resolve(resp);
      }
    }
  }

  private failAll(err: Error): void {
    for (const p of this.pending.values()) {
      clearTimeout(p.timer);
      p.reject(err);
    }
    this.pending.clear();
  }

  /** Send one command as a framed msgpack message; resolve on its reqId echo. */
  send(cmd: Record<string, unknown>, timeoutMs = 10000): Promise<Record<string, unknown>> {
    if (this.closed) return Promise.reject(new Error('raw conn closed'));
    const reqId = `raw-${++this.seq}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(reqId);
        reject(new Error(`raw send timeout: ${String(cmd['cmd'])}`));
      }, timeoutMs);
      this.pending.set(reqId, { resolve, reject, timer });
      this.sock.write(FrameParser.frame(pack({ ...cmd, reqId })));
    });
  }

  /** Put arbitrary bytes on the wire (e.g. a deliberately partial frame). */
  writeRaw(bytes: Uint8Array): void {
    this.sock.write(bytes);
  }

  /** Hard-destroy the socket: no FIN handshake courtesy, pending requests die. */
  destroy(): void {
    this.closed = true;
    this.sock.destroy();
  }

  end(): void {
    this.closed = true;
    this.sock.end();
    this.sock.destroy();
  }
}

// ---------------------------------------------------------------------------
// Small wire helpers
// ---------------------------------------------------------------------------

async function stateOf(c: RawConn, queue: string, id: string): Promise<string> {
  const st = await c.send({ cmd: 'GetState', queue, id });
  return String(st['state']);
}

async function pushDurable(c: RawConn, queue: string, n: number, pad = ''): Promise<string[]> {
  const ids: string[] = [];
  for (let i = 0; i < n; i++) {
    const res = await c.send({ cmd: 'PUSH', queue, data: { i, pad }, durable: true });
    expect(res['ok']).toBe(true); // ok:true == the durable write reached disk
    ids.push(String(res['id']));
  }
  return ids;
}

/** Pull with lock until `want` distinct jobs are leased (or deadline). */
async function pullSet(
  c: RawConn,
  queue: string,
  owner: string,
  want: number,
  deadlineMs: number,
  lockTtl = 30000
): Promise<Map<string, string>> {
  const leased = new Map<string, string>(); // id -> token
  const deadline = Date.now() + deadlineMs;
  while (leased.size < want && Date.now() < deadline) {
    const pull = await c.send({ cmd: 'PULL', queue, owner, lockTtl, timeout: 0 });
    const job = pull['job'] as { id: string } | null;
    if (!job?.id) {
      await Bun.sleep(60);
      continue;
    }
    leased.set(job.id, String(pull['token']));
  }
  return leased;
}

// ---------------------------------------------------------------------------
// Lifecycle hygiene: never leak a spawned server / socket into the next test.
// ---------------------------------------------------------------------------

async function teardown(): Promise<void> {
  for (const c of rawConns.splice(0)) {
    try {
      c.destroy();
    } catch {
      /* already gone */
    }
  }
  for (const c of tcpClients.splice(0)) {
    try {
      c.close();
    } catch {
      /* already gone */
    }
  }
  for (const s of servers.splice(0)) {
    try {
      s.proc.kill(9);
      await s.proc.exited;
    } catch {
      /* already dead */
    }
    cleanDb(s.db);
  }
}

afterEach(async () => {
  await teardown();
});

// Safety net: if a test times out before its afterEach settles, don't leak a
// child server (port + tmpdir DB) into the next test file.
afterAll(async () => {
  await teardown();
});

// ===========================================================================
// GROUP 1 — buffered-ACK loss at client disconnect (the SDK ackBatch path)
// ===========================================================================

describe('DURABILITY — buffered ACKs lost at disconnect never produce a false complete', () => {
  it('D1: ACKB-flushed half stays completed; unflushed half is redelivered and completed exactly once', async () => {
    const db = join(tmpdir(), `bunq-d1-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const QUEUE = 'd1';
    const N = 10;

    await spawnServer(db, port);

    // --- Connection A: the "worker" whose ack buffer will be lost ---------
    const a = await RawConn.open(port);
    const ids = await pushDurable(a, QUEUE, N);

    // Lease all N with a SHORT lock so even without the connection-release
    // path, lock expiry must return unacked jobs to the queue.
    const leased = await pullSet(a, QUEUE, 'worker-A', N, 10000, 1500);
    expect(leased.size).toBe(N);

    const ackedHalf = ids.slice(0, N / 2);
    const bufferedHalf = ids.slice(N / 2); // NEVER sent — "lost client buffer"

    // One real ACKB for the first half only (ids+results+tokens, the exact
    // shape the SDK AckBatcher flushes).
    const ackb = await a.send({
      cmd: 'ACKB',
      ids: ackedHalf,
      results: ackedHalf.map((id) => ({ done: id })),
      tokens: ackedHalf.map((id) => leased.get(id) as string),
    });
    expect(ackb['ok']).toBe(true);
    for (const id of ackedHalf) {
      expect(await stateOf(a, QUEUE, id)).toBe('completed');
    }

    // The other half's ACKs are sitting in the client-side batch buffer when
    // the connection dies: they are simply never written. Hard-destroy.
    a.destroy();

    // --- Connection B: a second worker takes over --------------------------
    const b = await RawConn.open(port);

    // FLUSHED ACKS STICK: the disconnect (and its releaseClientJobs sweep)
    // must not resurrect jobs whose ACKB already reached the server.
    for (const id of ackedHalf) {
      expect(await stateOf(b, QUEUE, id)).toBe('completed');
      const r = await b.send({ cmd: 'GetResult', id });
      expect((r['result'] as { done: string } | null)?.done).toBe(id);
    }

    // UNFLUSHED ACKS REDELIVER: the buffered half comes back (connection
    // release on close and/or the 1.5s lock expiry) and ONLY the buffered
    // half — an acked job showing up here would be a double execution.
    const redelivered = await pullSet(b, QUEUE, 'worker-B', N / 2, 25000, 30000);
    expect(redelivered.size).toBe(N / 2);
    expect([...redelivered.keys()].sort()).toEqual([...bufferedHalf].sort());
    for (const [id, token] of redelivered) {
      const ack = await b.send({ cmd: 'ACK', id, token, result: { done: id } });
      expect(ack['ok']).toBe(true);
    }

    // LATE-RESURRECTION GUARD: wait out the lock-expiry sweep (5s interval)
    // plus the original 1.5s TTLs — the expired worker-A locks on the acked
    // half must not requeue completed jobs.
    await Bun.sleep(6500);
    const extra = await b.send({ cmd: 'PULL', queue: QUEUE, owner: 'worker-C', timeout: 0 });
    expect((extra['job'] as unknown) ?? null).toBeNull();

    // FINAL TALLY: all N completed exactly once, nothing pending, no phantom.
    for (const id of ids) {
      expect(await stateOf(b, QUEUE, id)).toBe('completed');
    }
    const count = await b.send({ cmd: 'Count', queue: QUEUE });
    expect(count['count']).toBe(0);
    const counts = await b.send({ cmd: 'GetJobCounts', queue: QUEUE });
    const cnt = counts['counts'] as Record<string, number>;
    const pendingTotal =
      (cnt['waiting'] ?? 0) +
      (cnt['prioritized'] ?? 0) +
      (cnt['delayed'] ?? 0) +
      (cnt['active'] ?? 0) +
      (cnt['paused'] ?? 0);
    expect(pendingTotal).toBe(0);
  }, 90000); // 1 server spawn + 6.5s deliberate lock-sweep wait

  it('D2: a partial ACKB frame (length prefix + half the body) then destroy is not an ACK; server survives, job redelivers', async () => {
    const db = join(tmpdir(), `bunq-d2-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const QUEUE = 'd2';

    const s = await spawnServer(db, port);

    const a = await RawConn.open(port);
    const [id] = await pushDurable(a, QUEUE, 1);
    const leased = await pullSet(a, QUEUE, 'worker-A', 1, 10000, 1500);
    const token = leased.get(id as string) as string;
    expect(token).toBeDefined();

    // Build a VALID ACKB frame, then write only the 4-byte length prefix and
    // the first half of the msgpack body — the client dies mid-write.
    const body = pack({
      cmd: 'ACKB',
      ids: [id],
      results: [{ done: true }],
      tokens: [token],
      reqId: 'never-answered',
    });
    const framed = FrameParser.frame(body);
    const partial = framed.subarray(0, 4 + Math.floor(body.length / 2));
    a.writeRaw(partial);
    await Bun.sleep(150); // let the server buffer the partial frame
    a.destroy(); // connection dies with the frame incomplete

    // Server did not crash and stays responsive on a fresh connection.
    await Bun.sleep(300);
    expect(s.proc.exitCode).toBeNull(); // still running
    const b = await RawConn.open(port);
    const ping = await b.send({ cmd: 'Ping' });
    expect(ping['ok']).toBe(true);

    // The partial frame was NOT interpreted as an ACK: the job is not
    // completed, and it redelivers (release-on-close / 1.5s lock expiry).
    expect(await stateOf(b, QUEUE, id as string)).not.toBe('completed');
    const redelivered = await pullSet(b, QUEUE, 'worker-B', 1, 25000, 30000);
    expect([...redelivered.keys()]).toEqual([id as string]);
    const ack = await b.send({
      cmd: 'ACK',
      id,
      token: redelivered.get(id as string),
      result: { done: true },
    });
    expect(ack['ok']).toBe(true);
    expect(await stateOf(b, QUEUE, id as string)).toBe('completed');
  }, 90000); // 1 server spawn

  it('D3: SDK-level — the real AckBatcher over a real TcpClient: unflushed batch lost at close() is redelivered, flushed batch sticks', async () => {
    const db = join(tmpdir(), `bunq-d3-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const QUEUE = 'd3';
    const N = 8;

    await spawnServer(db, port);

    // Worker #1's connection + its ACK batcher (the real SDK classes the
    // Worker wires together — batchSize/interval chosen so nothing flushes
    // unless we say so, exactly modeling "buffered at disconnect").
    const c1 = new TcpClient({
      host: '127.0.0.1',
      port,
      autoReconnect: false,
      pingInterval: 0,
      commandTimeout: 5000,
      connectTimeout: 5000,
    });
    await c1.connect();
    tcpClients.push(c1);
    const batcher = new AckBatcher({
      batchSize: 1000, // never auto-flush on size
      interval: 600000, // never auto-flush on time within the test
      embedded: false,
      maxRetries: 0,
    });
    batcher.setTcp(c1);

    try {
      const ids: string[] = [];
      for (let i = 0; i < N; i++) {
        const res = await c1.send({ cmd: 'PUSH', queue: QUEUE, data: { i }, durable: true });
        expect(res.ok).toBe(true);
        ids.push(String(res.id));
      }
      const tokens = new Map<string, string>();
      const deadline = Date.now() + 10000;
      while (tokens.size < N && Date.now() < deadline) {
        const pull = await c1.send({
          cmd: 'PULL',
          queue: QUEUE,
          owner: 'sdk-worker-1',
          lockTtl: 2000,
          timeout: 0,
        });
        const job = pull.job as { id: string } | null;
        if (job?.id) tokens.set(job.id, String(pull.token));
        else await Bun.sleep(50);
      }
      expect(tokens.size).toBe(N);

      const flushedHalf = ids.slice(0, N / 2);
      const bufferedHalf = ids.slice(N / 2);

      // Queue + flush the first half — a real ACKB goes over the wire.
      const flushedPromises = flushedHalf.map((id) =>
        batcher.queue(id, { via: 'ackb', id }, tokens.get(id))
      );
      await batcher.flush();
      await Promise.all(flushedPromises);

      // Queue the second half but NEVER flush: these live only in the
      // client-side buffer. (Their queue() promises can never settle once the
      // connection dies — do not await them.)
      for (const id of bufferedHalf) {
        void batcher.queue(id, { via: 'ackb', id }, tokens.get(id)).catch(() => {});
      }

      // The worker process "dies": connection closed with the batch unsent.
      c1.close();
      batcher.stop();

      // Worker #2 takes over on a fresh connection.
      const c2 = new TcpClient({
        host: '127.0.0.1',
        port,
        autoReconnect: false,
        pingInterval: 0,
        commandTimeout: 5000,
        connectTimeout: 5000,
      });
      await c2.connect();
      tcpClients.push(c2);

      // Flushed half: completed and stays completed.
      for (const id of flushedHalf) {
        const st = await c2.send({ cmd: 'GetState', queue: QUEUE, id });
        expect(st.state).toBe('completed');
      }

      // Buffered half: never completed — redelivered (release-on-close or the
      // 2s lock expiry) and ONLY those ids come back.
      const redelivered = new Map<string, string>();
      const deadline2 = Date.now() + 25000;
      while (redelivered.size < bufferedHalf.length && Date.now() < deadline2) {
        const pull = await c2.send({
          cmd: 'PULL',
          queue: QUEUE,
          owner: 'sdk-worker-2',
          lockTtl: 30000,
          timeout: 0,
        });
        const job = pull.job as { id: string } | null;
        if (!job?.id) {
          await Bun.sleep(60);
          continue;
        }
        redelivered.set(job.id, String(pull.token));
      }
      expect([...redelivered.keys()].sort()).toEqual([...bufferedHalf].sort());
      for (const [id, token] of redelivered) {
        const ack = await c2.send({ cmd: 'ACK', id, token });
        expect(ack.ok).toBe(true);
      }

      // Everything completed exactly once.
      for (const id of ids) {
        const st = await c2.send({ cmd: 'GetState', queue: QUEUE, id });
        expect(st.state).toBe('completed');
      }
      const count = await c2.send({ cmd: 'Count', queue: QUEUE });
      expect(count.count).toBe(0);
    } finally {
      batcher.stop(); // never leak the 600s ack timer
    }
  }, 90000); // 1 server spawn
});

// ===========================================================================
// GROUP 2 — WAL/db corruption or truncation at startup
// ===========================================================================

describe('DURABILITY — damaged WAL/db at startup: clean recovery or explicit refusal, never invented data', () => {
  it('W1: truncated -wal → the committed prefix survives (strict push-order prefix), no phantom jobs, queue drains', async () => {
    const db = join(tmpdir(), `bunq-w1-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const QUEUE = 'w1';
    const M = 100;

    const s1 = await spawnServer(db, port);
    const a = await RawConn.open(port);
    // Payload padding keeps job frames dominant in the WAL, so truncating at
    // half lands well inside the job-commit region (schema frames are at the
    // head and tiny by comparison).
    const ids = await pushDurable(a, QUEUE, M, 'x'.repeat(120));
    a.destroy();
    await hardKill(s1); // no graceful close → no wal_checkpoint → WAL keeps everything

    // Damage: chop the WAL to half its size. SQLite replays only the valid
    // prefix (frame checksums chain), so a suffix of commits vanishes. The
    // stale -shm is left in place deliberately — recovery must cope with it.
    const wal = `${db}-wal`;
    expect(existsSync(wal)).toBe(true);
    const walSize = statSync(wal).size;
    expect(walSize).toBeGreaterThan(1000); // durable commits really are in the WAL
    truncateSync(wal, Math.floor(walSize / 2));

    // Restart on the same damaged DB: must start and serve.
    await spawnServer(db, port);
    const b = await RawConn.open(port);
    expect((await b.send({ cmd: 'Ping' }))['ok']).toBe(true);

    // Survivors form a strict PREFIX of push order: durable pushes were
    // sequential (each awaited), so their WAL commits are ordered; a valid-
    // prefix replay can never resurrect job k+1 while dropping job k. SQLite
    // may auto-checkpoint every commit before the kill; in that valid case the
    // complete input is the surviving prefix even though the stale WAL tail is
    // truncated.
    const states: string[] = [];
    for (const id of ids) {
      states.push(await stateOf(b, QUEUE, id));
    }
    for (const st of states) {
      expect(VALID_STATES.has(st)).toBe(true); // no garbage states, ever
    }
    const firstUnknown = states.indexOf('unknown');
    expect(firstUnknown).not.toBe(0); // at least the earliest commits survive
    const known = firstUnknown === -1 ? ids : ids.slice(0, firstUnknown);
    // STRICT PREFIX: once one commit is gone, every later commit is gone too.
    // A known job AFTER the first unknown would mean SQLite (or the recovery
    // path) resurrected a commit past the valid WAL prefix — invented data.
    if (firstUnknown !== -1) {
      for (let i = firstUnknown; i < M; i++) {
        expect(states[i]).toBe('unknown');
      }
    }

    // Counts are consistent: nothing was ever acked, so pending == known.
    const counts = await b.send({ cmd: 'GetJobCounts', queue: QUEUE });
    const cnt = counts['counts'] as Record<string, number>;
    const pendingTotal =
      (cnt['waiting'] ?? 0) +
      (cnt['prioritized'] ?? 0) +
      (cnt['delayed'] ?? 0) +
      (cnt['active'] ?? 0) +
      (cnt['paused'] ?? 0);
    expect(pendingTotal).toBe(known.length); // no phantom, no loss within the prefix

    // The queue drains: every survivor is pullable and ackable, and ONLY
    // survivors come out (a pulled id outside the known prefix would be
    // phantom data invented from the damaged WAL).
    const drained = await pullSet(b, QUEUE, 'w1-drainer', known.length, 20000);
    expect(drained.size).toBe(known.length);
    const knownSet = new Set(known);
    for (const [id, token] of drained) {
      expect(knownSet.has(id)).toBe(true);
      const ack = await b.send({ cmd: 'ACK', id, token });
      expect(ack['ok']).toBe(true);
    }
    const after = await b.send({ cmd: 'Count', queue: QUEUE });
    expect(after['count']).toBe(0);
  }, 150000); // 2 server spawns

  it('W2: garbaged -wal header → WAL discarded by design: checkpointed data intact, no phantoms, server starts and serves', async () => {
    const db = join(tmpdir(), `bunq-w2-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const QUEUE = 'w2';

    // Phase 1: 25 durable jobs, SIGKILL, then checkpoint them into the MAIN
    // db from the test side (the server only checkpoints on graceful close,
    // which SIGKILL denies it).
    const s1 = await spawnServer(db, port);
    const a = await RawConn.open(port);
    const checkpointedIds = await pushDurable(a, QUEUE, 25);
    a.destroy();
    await hardKill(s1);
    {
      const d = new Database(db);
      d.exec('PRAGMA wal_checkpoint(TRUNCATE);'); // 25 jobs now live in the main db
      d.close();
    }

    // Phase 2: 15 more durable jobs — these live ONLY in the fresh WAL.
    const s2 = await spawnServer(db, port);
    const b = await RawConn.open(port);
    const walOnlyIds = await pushDurable(b, QUEUE, 15);
    b.destroy();
    await hardKill(s2);

    // Damage: stomp the first 100 bytes of the WAL with random garbage. An
    // invalid WAL header means SQLite finds zero valid frames and treats the
    // db as WAL-less — verified against bun:sqlite: checkpointed rows remain,
    // WAL-only rows are gone. The stale -shm is again left in place.
    const wal = `${db}-wal`;
    expect(existsSync(wal)).toBe(true);
    expect(statSync(wal).size).toBeGreaterThan(100);
    const junk = new Uint8Array(100);
    crypto.getRandomValues(junk);
    const fd = openSync(wal, 'r+');
    writeSync(fd, junk, 0, 100, 0);
    closeSync(fd);

    // Restart: must come up and serve.
    await spawnServer(db, port);
    const c = await RawConn.open(port);
    expect((await c.send({ cmd: 'Ping' }))['ok']).toBe(true);

    // Main-db content is intact: every checkpointed job is still known.
    for (const id of checkpointedIds) {
      const st = await stateOf(c, QUEUE, id);
      expect(st).not.toBe('unknown');
      expect(VALID_STATES.has(st)).toBe(true);
    }

    // WAL-only commits: lost WITH the WAL (SQLite by design — a durable
    // write's home is the WAL until a checkpoint; destroying the WAL header
    // destroys those commits). The server must report them cleanly as
    // unknown or, at most, as a valid state — never a garbage state, and
    // never data resurrected out of a corrupt WAL.
    let walOnlyKnown = 0;
    for (const id of walOnlyIds) {
      const st = await stateOf(c, QUEUE, id);
      expect(VALID_STATES.has(st)).toBe(true);
      if (st !== 'unknown') walOnlyKnown++;
    }

    // No phantoms: pending == exactly the known jobs (none were ever acked).
    const counts = await c.send({ cmd: 'GetJobCounts', queue: QUEUE });
    const cnt = counts['counts'] as Record<string, number>;
    const pendingTotal =
      (cnt['waiting'] ?? 0) +
      (cnt['prioritized'] ?? 0) +
      (cnt['delayed'] ?? 0) +
      (cnt['active'] ?? 0) +
      (cnt['paused'] ?? 0);
    expect(pendingTotal).toBe(checkpointedIds.length + walOnlyKnown);

    // And the instance is fully functional: a fresh push/pull/ack cycle works.
    const push = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { fresh: true }, durable: true });
    expect(push['ok']).toBe(true);
    const leased = await pullSet(c, QUEUE, 'w2-drainer', 1, 10000);
    expect(leased.size).toBe(1);
    const [freshLease] = [...leased.entries()].filter(([id]) => id === String(push['id']));
    // (The drainer may pull an old job first; drain until the fresh one shows.)
    let freshToken = freshLease?.[1];
    if (!freshToken) {
      const more = await pullSet(c, QUEUE, 'w2-drainer', pendingTotal, 20000);
      for (const [id, token] of [...leased, ...more]) {
        if (id === String(push['id'])) freshToken = token;
        else await c.send({ cmd: 'ACK', id, token });
      }
    }
    expect(freshToken).toBeDefined();
    const ack = await c.send({ cmd: 'ACK', id: String(push['id']), token: freshToken });
    expect(ack['ok']).toBe(true);
    expect(await stateOf(c, QUEUE, String(push['id']))).toBe('completed');
  }, 200000); // 3 server spawns

  it('W3: corrupted main .db → explicit refusal (non-zero exit) or a demonstrably working start; never a wedge, never garbage', async () => {
    const db = join(tmpdir(), `bunq-w3-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const QUEUE = 'w3';
    const M = 30;

    const s1 = await spawnServer(db, port);
    const a = await RawConn.open(port);
    const ids = await pushDurable(a, QUEUE, M, 'y'.repeat(150));
    a.destroy();
    await hardKill(s1);

    // Move the data into the main db (checkpoint), then flip 256 bytes in the
    // middle of the file — squarely inside real pages, past the header.
    {
      const d = new Database(db);
      d.exec('PRAGMA wal_checkpoint(TRUNCATE);');
      d.close();
    }
    const size = statSync(db).size;
    expect(size).toBeGreaterThan(4096);
    const junk = new Uint8Array(256).fill(0xff);
    const fd = openSync(db, 'r+');
    writeSync(fd, junk, 0, 256, Math.floor(size / 2));
    closeSync(fd);

    // Restart against the corrupted file. Both outcomes below are acceptable;
    // a hang (neither port nor exit) fails inside spawnServerTolerant.
    //
    // OBSERVED on the current build (documented per test contract): outcome B
    // — the server STARTS and serves. In the probed run all 30 pre-corruption
    // jobs were still reported 'waiting' and a fresh push/pull/ack cycle
    // completed. Why no error: SQLite pages carry NO checksums, so a byte
    // flip that lands inside cell PAYLOAD bytes (row data) is structurally
    // invisible — reads succeed, "database disk image is malformed" fires
    // only when the damage hits page/cell STRUCTURE (verified both ways with
    // bun:sqlite: structural hits throw on the first query that touches the
    // page). Outcome A (non-zero exit during the startup recovery scan) is
    // therefore also possible depending on where the damage lands; the test
    // accepts either. KNOWN LIMITATION (SQLite by design, not a bunqueue
    // defect): payload-region damage can silently alter a stored job's data
    // blob — detecting that would require application-level checksums over
    // job payloads.
    const { server, started } = await spawnServerTolerant(db, port);

    if (!started) {
      // Outcome A — explicit refusal. A clean exit code 0 here would be a
      // silent failure and is rejected.
      expect(server.proc.exitCode).not.toBe(0);
      expect(server.proc.exitCode).not.toBeNull();
      return;
    }

    // Outcome B — it started: then it must NOT serve garbage.
    const c = await RawConn.open(port);
    const ping = await c.send({ cmd: 'Ping' });
    expect(ping['ok']).toBe(true);

    // Every old id must resolve to a legal state ('unknown' is honest for
    // rows behind corrupted pages) — never a junk state, never a wire error
    // that kills the connection.
    for (const id of ids) {
      const st = await stateOf(c, QUEUE, id);
      expect(VALID_STATES.has(st)).toBe(true);
    }

    // And the server is genuinely operational, not a zombie: a full
    // push/pull/ack cycle on a FRESH queue succeeds end to end.
    const FRESH = 'w3-fresh';
    const push = await c.send({ cmd: 'PUSH', queue: FRESH, data: { ok: 1 }, durable: true });
    expect(push['ok']).toBe(true);
    const leased = await pullSet(c, FRESH, 'w3-worker', 1, 10000);
    expect([...leased.keys()]).toEqual([String(push['id'])]);
    const ack = await c.send({
      cmd: 'ACK',
      id: String(push['id']),
      token: leased.get(String(push['id'])),
    });
    expect(ack['ok']).toBe(true);
    expect(await stateOf(c, FRESH, String(push['id']))).toBe('completed');
  }, 150000); // 2 server spawns (the second may legitimately refuse)
});
