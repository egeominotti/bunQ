/**
 * Conformance runner: spawns a real bunqueue server + a client driver
 * (any language, JSON lines over stdio) and certifies the driver against
 * docs/protocol.md. Every result is double-checked through the runner's own
 * independent wire connection, so a driver cannot pass by being consistently
 * wrong about the protocol.
 *
 * Usage: bun runner.ts --driver "<command>"
 */

import { type ChildProcess, spawn } from 'node:child_process';
import { once } from 'node:events';
import { connect, type Socket } from 'node:net';
import { createInterface } from 'node:readline';
import { pack, unpack } from 'msgpackr';

const REPO_ROOT = new URL('../../', import.meta.url).pathname;

// ------------------------------------------------------------ wire client

/** Minimal independent verification client (deliberately not an SDK). */
class Wire {
  private socket!: Socket;
  private buffer = Buffer.alloc(0);
  private reqCounter = 0;

  async connect(port: number, token?: string): Promise<void> {
    this.socket = connect({ host: '127.0.0.1', port });
    await once(this.socket, 'connect');
    this.socket.on('data', (chunk) => {
      this.buffer = Buffer.concat([this.buffer, chunk]);
    });
    if (token) await this.call({ cmd: 'Auth', token });
  }

  async call(command: Record<string, unknown>): Promise<Record<string, unknown>> {
    const reqId = `conf-${++this.reqCounter}`;
    const frame = pack({ ...command, reqId });
    const header = Buffer.alloc(4);
    header.writeUInt32BE(frame.length);
    this.socket.write(Buffer.concat([header, frame]));
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      const response = this.tryRead();
      if (response) {
        if (response.reqId !== reqId) continue; // sequential: skip strays
        if (response.ok !== true) throw new Error(String(response.error ?? 'command failed'));
        return response;
      }
      await new Promise((r) => setTimeout(r, 5));
    }
    throw new Error('verification call timed out');
  }

  private tryRead(): Record<string, unknown> | null {
    if (this.buffer.length < 4) return null;
    const length = this.buffer.readUInt32BE(0);
    if (this.buffer.length < 4 + length) return null;
    const body = this.buffer.subarray(4, 4 + length);
    this.buffer = this.buffer.subarray(4 + length);
    return unpack(body) as Record<string, unknown>;
  }

  close(): void {
    this.socket?.destroy();
  }
}

// ------------------------------------------------------------ server + driver

async function freePort(): Promise<number> {
  const { createServer } = await import('node:net');
  return new Promise((resolve) => {
    const srv = createServer();
    srv.listen(0, '127.0.0.1', () => {
      const port = (srv.address() as { port: number }).port;
      srv.close(() => resolve(port));
    });
  });
}

async function startServer(extraEnv: Record<string, string> = {}) {
  const port = await freePort();
  const httpPort = await freePort();
  const dataDir = `${REPO_ROOT}.conformance-tmp-${Math.random().toString(36).slice(2, 8)}`;
  const { mkdirSync } = await import('node:fs');
  mkdirSync(dataDir, { recursive: true });
  const proc = spawn('bun', ['src/main.ts'], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      TCP_PORT: String(port),
      HTTP_PORT: String(httpPort),
      BUNQUEUE_DATA_PATH: `${dataDir}/bunq.db`,
      ...extraEnv,
    },
    stdio: 'ignore',
  });
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    try {
      const wire = new Wire();
      await wire.connect(port, extraEnv.AUTH_TOKENS?.split(',')[0]);
      return { port, proc, wire, dataDir };
    } catch {
      await new Promise((r) => setTimeout(r, 150));
    }
  }
  proc.kill();
  throw new Error('server did not start within 15s');
}

class Driver {
  private proc: ChildProcess;
  private pending = new Map<number, { resolve: (v: Record<string, unknown>) => void }>();
  private counter = 0;

  constructor(command: string) {
    this.proc = spawn('sh', ['-c', command], {
      cwd: new URL('.', import.meta.url).pathname,
      stdio: ['pipe', 'pipe', 'inherit'],
    });
    const rl = createInterface({ input: this.proc.stdout! });
    rl.on('line', (line) => {
      try {
        const message = JSON.parse(line) as { id: number };
        this.pending.get(message.id)?.resolve(message as unknown as Record<string, unknown>);
        this.pending.delete(message.id);
      } catch {
        /* non-JSON driver noise on stdout: ignore */
      }
    });
  }

  async op(op: string, fields: Record<string, unknown> = {}, timeoutMs = 30_000): Promise<Record<string, unknown>> {
    const id = ++this.counter;
    const answer = new Promise<Record<string, unknown>>((resolve, reject) => {
      this.pending.set(id, { resolve });
      setTimeout(() => {
        if (this.pending.delete(id)) reject(new Error(`driver op '${op}' timed out after ${timeoutMs}ms`));
      }, timeoutMs).unref?.();
    });
    this.proc.stdin!.write(`${JSON.stringify({ id, op, ...fields })}\n`);
    return answer;
  }

  /** Like op(), but throws when the driver answers ok: false. */
  async must(op: string, fields: Record<string, unknown> = {}, timeoutMs = 30_000): Promise<Record<string, unknown>> {
    const answer = await this.op(op, fields, timeoutMs);
    if (answer.ok !== true) throw new Error(`driver ${op} failed: ${String(answer.error ?? 'unknown')}`);
    return answer;
  }

  kill(): void {
    this.proc.kill();
  }
}

// ---------------------------------------------------------------- checks

type Check = { id: string; title: string; run: (d: Driver, w: Wire) => Promise<void> };

const q = (prefix: string) => `conf-${prefix}-${Math.random().toString(36).slice(2, 8)}`;

function assert(cond: unknown, message: string): asserts cond {
  if (!cond) throw new Error(message);
}

async function waitFor(fn: () => Promise<boolean>, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error('condition not reached in time');
}

const CHECKS: Check[] = [
  {
    id: 'C01',
    title: 'Hello protocol version matches the server',
    run: async (d, w) => {
      const server = await w.call({ cmd: 'Hello', protocolVersion: 2, capabilities: [] });
      const driver = await d.must('hello');
      assert(driver.protocolVersion === server.protocolVersion, `driver v${driver.protocolVersion} != server v${server.protocolVersion}`);
    },
  },
  {
    id: 'C02',
    title: 'job name travels inside data',
    run: async (d, w) => {
      const queue = q('c02');
      const { jobId: id } = await d.must('add', { queue, name: 'send-email', data: { to: 'a@b.c' } });
      await waitFor(async () => Boolean((await w.call({ cmd: 'GetJob', id }).catch(() => null))?.job));
      const raw = (await w.call({ cmd: 'GetJob', id })).job as { data: Record<string, unknown> };
      assert(raw.data.name === 'send-email', 'wire data.name missing (name must travel inside data)');
      assert(raw.data.to === 'a@b.c', 'user payload lost');
      // Collision rule (spec §5): a user `name` key overrides the job name.
      const { jobId: id2 } = await d.must('add', { queue, name: 'job-name', data: { name: 'user-name' } });
      await waitFor(async () => Boolean((await w.call({ cmd: 'GetJob', id: id2 }).catch(() => null))?.job));
      const raw2 = (await w.call({ cmd: 'GetJob', id: id2 })).job as { data: Record<string, unknown> };
      assert(raw2.data.name === 'user-name', `user data.name must win the collision, got ${raw2.data.name}`);
    },
  },
  {
    id: 'C03',
    title: 'int64 guard: big integers travel as float64, server stays healthy',
    run: async (d, w) => {
      const queue = q('c03');
      const { jobId: id } = await d.must('add', { queue, name: 't', data: { epochMs: 9_999_999_999_999, small: 42 } });
      await waitFor(async () => Boolean((await w.call({ cmd: 'GetJob', id }).catch(() => null))?.job));
      const raw = (await w.call({ cmd: 'GetJob', id })).job as { data: Record<string, unknown> };
      assert(Number(raw.data.epochMs) === 9_999_999_999_999, 'big int value corrupted');
      await w.call({ cmd: 'ListWorkers' }); // BigInt arithmetic crash class
      const pong = await w.call({ cmd: 'Ping' });
      assert((pong.data as { pong?: boolean })?.pong === true, 'server unhealthy after int64 traffic');
    },
  },
  {
    id: 'C04',
    title: 'custom jobId is idempotent on PUSH',
    run: async (d, w) => {
      const queue = q('c04');
      const first = await d.must('add', { queue, name: 't', data: { v: 1 }, opts: { jobId: 'conf-idem' } });
      const second = await d.must('add', { queue, name: 't', data: { v: 2 }, opts: { jobId: 'conf-idem' } });
      assert(first.jobId === second.jobId, 'same custom id must return the same job id');
      const count = await w.call({ cmd: 'Count', queue });
      assert(count.count === 1, `expected 1 job, server has ${count.count}`);
    },
  },
  {
    id: 'C05',
    title: 'PUSHB preserves custom ids (jobId -> customId rename)',
    run: async (d, w) => {
      const queue = q('c05');
      await d.must('addBulk', { queue, entries: [{ name: 't', data: { v: 1 }, opts: { jobId: 'conf-bulk-1' } }] });
      await waitFor(async () => {
        try {
          const r = await w.call({ cmd: 'GetJobByCustomId', queue, customId: 'conf-bulk-1' });
          return Boolean(r.job);
        } catch {
          return false;
        }
      });
    },
  },
  {
    id: 'C06',
    title: 'not-found lookups map to null, not errors',
    run: async (d) => {
      const answer = await d.must('getJob', { jobId: 'does-not-exist-conformance' });
      assert(answer.job === null, `missing job must answer null, got ${JSON.stringify(answer.job)}`);
      const byCustom = await d.must('getJobByCustomId', { queue: q('c06'), customId: 'nope' });
      assert(byCustom.job === null, 'missing customId must answer null');
      const sched = await d.must('getScheduler', { schedulerId: 'missing-sched-conf' });
      assert(sched.scheduler === null, 'missing scheduler must answer null');
    },
  },
  {
    id: 'C07',
    title: 'delayed job + promote',
    run: async (d, w) => {
      const queue = q('c07');
      const { jobId: id } = await d.must('add', { queue, name: 't', data: { x: 1 }, opts: { delay: 60_000 } });
      const state = await d.must('getState', { jobId: id });
      assert(state.state === 'delayed', `expected delayed, got ${state.state}`);
      await w.call({ cmd: 'Promote', id });
      await waitFor(async () => (await d.must('getState', { jobId: id })).state === 'waiting');
    },
  },
  {
    id: 'C08',
    title: 'process + ACK: result persisted, exactly one completion',
    run: async (d, w) => {
      const queue = q('c08');
      const { jobId: id } = await d.must('add', { queue, name: 'calc', data: { v: 21 } });
      await d.must('process', { queue, behavior: 'ok', result: { doubled: 42 }, until: { completed: 1 }, timeoutMs: 20_000 }, 30_000);
      const result = await w.call({ cmd: 'GetResult', id });
      assert(JSON.stringify(result.result) === JSON.stringify({ doubled: 42 }), 'result not persisted verbatim');
      const state = await w.call({ cmd: 'GetState', id });
      assert(state.state === 'completed', 'job not completed');
    },
  },
  {
    id: 'C09',
    title: 'retry semantics: transient failure, then success',
    run: async (d, w) => {
      const queue = q('c09');
      const { jobId: id } = await d.must('add', { queue, name: 'flaky', data: { x: 1 }, opts: { attempts: 3, backoff: 50 } });
      await d.must('process', { queue, behavior: 'failOnce', result: 'ok', until: { completed: 1 }, timeoutMs: 30_000 }, 45_000);
      const state = await w.call({ cmd: 'GetState', id });
      assert(state.state === 'completed', 'job must complete after a retried transient failure');
    },
  },
  {
    id: 'C10',
    title: 'unrecoverable failure skips retries into the DLQ',
    run: async (d, w) => {
      const queue = q('c10');
      await d.must('add', { queue, name: 'poison', data: { x: 1 }, opts: { attempts: 5 } });
      await d.must('process', { queue, behavior: 'unrecoverable', until: { dlq: 1 }, timeoutMs: 20_000 }, 30_000);
      const dlq = await w.call({ cmd: 'Dlq', queue });
      assert(Array.isArray(dlq.jobs) && dlq.jobs.length === 1, 'exactly one DLQ entry expected');
      const retried = await d.must('retryDlq', { queue });
      assert(retried.count === 1, 'retryDlq must report 1 re-queued entry');
    },
  },
  {
    id: 'C11',
    title: 'FAIL stack keeps the raise site within the persisted lines',
    run: async (d, w) => {
      const queue = q('c11');
      const { jobId: id } = await d.must('add', { queue, name: 'boom', data: {}, opts: { attempts: 1 } });
      await d.must('process', { queue, behavior: 'deepThrow', until: { failed: 1 }, timeoutMs: 20_000 }, 30_000);
      await waitFor(async () => (await w.call({ cmd: 'GetState', id })).state === 'failed');
      const raw = (await w.call({ cmd: 'GetJob', id })).job as { stacktrace?: string[] };
      const stack = raw.stacktrace ?? [];
      assert(stack.length > 0, 'stack must be persisted on FAIL');
      assert(stack.some((line) => line.includes('BOOM-CONFORMANCE')), `raise site truncated away: ${JSON.stringify(stack)}`);
    },
  },
  {
    id: 'C12',
    title: 'scheduler limit reaches the wire as maxLimit',
    run: async (d, w) => {
      const queue = q('c12');
      const schedulerId = q('sched');
      await d.must('upsertScheduler', { queue, schedulerId, repeat: { pattern: '0 9 * * *', limit: 3, tz: 'UTC' } });
      const cron = (await w.call({ cmd: 'CronGet', name: schedulerId })).cron as { maxLimit?: number };
      assert(Number(cron.maxLimit) === 3, `limit dropped: wire maxLimit = ${cron.maxLimit}`);
      const viaDriver = await d.must('getScheduler', { schedulerId });
      assert(viaDriver.scheduler !== null, 'driver must read its own scheduler');
      await d.must('removeScheduler', { schedulerId });
    },
  },
  {
    id: 'C13',
    title: 'WaitJob timeout beyond the server bound is clamped',
    run: async (d) => {
      const queue = q('c13');
      const { jobId: id } = await d.must('add', { queue, name: 't', data: { x: 1 } });
      await d.must('process', { queue, behavior: 'ok', result: { done: true }, until: { completed: 1 }, timeoutMs: 20_000 }, 30_000);
      const answer = await d.must('waitForJob', { jobId: id, timeoutMs: 700_000 });
      assert(JSON.stringify(answer.result) === JSON.stringify({ done: true }), 'waitForJob must clamp and resolve');
    },
  },
  {
    id: 'C14',
    title: 'worker batch size clamps to the server max (1000)',
    run: async (d, w) => {
      const queue = q('c14');
      const { jobId: id } = await d.must('add', { queue, name: 't', data: { x: 1 } });
      await d.must('process', { queue, behavior: 'ok', result: 'ok', batchSize: 5000, until: { completed: 1 }, timeoutMs: 20_000 }, 30_000);
      const state = await w.call({ cmd: 'GetState', id });
      assert(state.state === 'completed', 'an unclamped batchSize wedges the pull loop (PULLB count > 1000)');
    },
  },
  {
    id: 'C15',
    title: 'pause / isPaused / resume / drain',
    run: async (d, w) => {
      const queue = q('c15');
      await d.must('pause', { queue });
      assert((await d.must('isPaused', { queue })).paused === true, 'paused must read true');
      await d.must('resume', { queue });
      assert((await d.must('isPaused', { queue })).paused === false, 'resumed must read false');
      await d.must('addBulk', { queue, entries: [{ name: 'a', data: { i: 1 } }, { name: 'b', data: { i: 2 } }] });
      await waitFor(async () => (await w.call({ cmd: 'Count', queue })).count === 2);
      const drained = await d.must('drain', { queue });
      assert(drained.count === 2, `drain must report 2, got ${drained.count}`);
    },
  },
  {
    id: 'C16',
    title: 'unicode payload integrity',
    run: async (d, w) => {
      const queue = q('c16');
      const payload = { emoji: '🚀🔥', cjk: '以呂波', rtl: 'مرحبا' };
      const { jobId: id } = await d.must('add', { queue, name: 'uni', data: payload });
      await waitFor(async () => Boolean((await w.call({ cmd: 'GetJob', id }).catch(() => null))?.job));
      const raw = (await w.call({ cmd: 'GetJob', id })).job as { data: Record<string, unknown> };
      for (const key of Object.keys(payload)) {
        assert(raw.data[key] === payload[key as keyof typeof payload], `${key} corrupted`);
      }
    },
  },
];

// ------------------------------------------------------------------ main

async function main(): Promise<number> {
  const driverArg = process.argv.indexOf('--driver');
  const driverCommand = driverArg >= 0 ? process.argv[driverArg + 1] : null;
  if (!driverCommand) {
    console.error('usage: bun runner.ts --driver "<command>"');
    return 2;
  }

  let failed = 0;
  const server = await startServer();
  const driver = new Driver(driverCommand);
  try {
    await driver.must('connect', { host: '127.0.0.1', port: server.port });
    for (const check of CHECKS) {
      try {
        await check.run(driver, server.wire);
        console.log(`PASS ${check.id} ${check.title}`);
      } catch (error) {
        failed++;
        console.log(`FAIL ${check.id} ${check.title}: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
    await driver.op('close', {}, 5000).catch(() => {});
  } finally {
    driver.kill();
    server.wire.close();
    server.proc.kill();
    await new Promise((r) => setTimeout(r, 200));
    const { rmSync } = await import('node:fs');
    rmSync(server.dataDir, { recursive: true, force: true });
  }

  // C17 runs against a dedicated token-protected server.
  const authServer = await startServer({ AUTH_TOKENS: 'conformance-secret' });
  const authDriver = new Driver(driverCommand);
  try {
    await authDriver.must('connect', { host: '127.0.0.1', port: authServer.port, token: 'conformance-secret' });
    const queue = q('c17');
    await authDriver.must('add', { queue, name: 't', data: { x: 1 } });
    const count = await authServer.wire.call({ cmd: 'Count', queue });
    assert(count.count === 1, 'authenticated add must work');
    console.log('PASS C17 auth handshake on a token-protected server');
  } catch (error) {
    failed++;
    console.log(`FAIL C17 auth handshake: ${error instanceof Error ? error.message : String(error)}`);
  } finally {
    await authDriver.op('close', {}, 5000).catch(() => {});
    authDriver.kill();
    authServer.wire.close();
    authServer.proc.kill();
    await new Promise((r) => setTimeout(r, 200));
    const { rmSync } = await import('node:fs');
    rmSync(authServer.dataDir, { recursive: true, force: true });
  }

  const total = CHECKS.length + 1;
  console.log(`\n${total - failed}/${total} checks passed`);
  console.log(failed === 0 ? 'VERDICT: CONFORMANT' : 'VERDICT: NOT CONFORMANT');
  return failed === 0 ? 0 : 1;
}

process.exit(await main());
