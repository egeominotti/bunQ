/**
 * Legacy API test harness — runtime-neutral (Node ≥22, Bun, Deno ≥2).
 *
 * Provides a tiny test registry, assertions, wait helpers and real-server
 * management (spawns `bun src/main.ts` from the repo root on a random port).
 * Test modules import { test, ... } and register cases as a side effect;
 * an entrypoint then calls runSuite().
 */

import { type ChildProcess, spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { connect, createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { type Job, Queue, Worker } from '../dist/legacy.js';

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..', '..');

// ------------------------------------------------------------ registry

type TestFn = () => Promise<void>;
const tests: Array<{ name: string; fn: TestFn }> = [];

export function test(name: string, fn: TestFn): void {
  tests.push({ name, fn });
}

export function assert(cond: unknown, msg: string): asserts cond {
  if (!cond) throw new Error(`assertion failed: ${msg}`);
}

export function assertEq(actual: unknown, expected: unknown, msg: string): void {
  if (actual !== expected) {
    throw new Error(`${msg}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export async function waitFor(
  cond: () => boolean | Promise<boolean>,
  timeoutMs = 15_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await cond()) return;
    await sleep(100);
  }
  throw new Error('waitFor timed out');
}

// ------------------------------------------------------------ server

function portOpen(port: number): Promise<boolean> {
  return new Promise((resolvePort) => {
    const socket = connect({ host: '127.0.0.1', port }, () => {
      socket.destroy();
      resolvePort(true);
    });
    socket.on('error', () => resolvePort(false));
    socket.setTimeout(500, () => {
      socket.destroy();
      resolvePort(false);
    });
  });
}

/** Ask the OS for a genuinely free port (bind to 0, read, release). */
function freePort(): Promise<number> {
  return new Promise((res, rej) => {
    const srv = createServer();
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address() as { port: number };
      srv.close(() => res(port));
    });
    srv.on('error', rej);
  });
}

const tempDirs: string[] = [];

/** Best-effort removal of every temp data dir created by startServer. */
export function cleanTempDirs(): void {
  for (const dir of tempDirs.splice(0)) {
    try {
      rmSync(dir, { recursive: true, force: true });
    } catch {
      /* best effort */
    }
  }
}

export async function startServer(extraEnv: Record<string, string> = {}): Promise<{
  port: number;
  proc: ChildProcess;
}> {
  const port = await freePort();
  const dataDir = mkdtempSync(join(tmpdir(), 'bunqueue-ts-e2e-'));
  tempDirs.push(dataDir);
  const proc = spawn('bun', ['src/main.ts'], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      TCP_PORT: String(port),
      HTTP_PORT: '0',
      BUNQUEUE_DATA_PATH: join(dataDir, 'bunq.db'),
      ...extraEnv,
    },
    stdio: 'ignore',
  });
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    if (await portOpen(port)) return { port, proc };
    await sleep(100);
  }
  proc.kill();
  throw new Error('bunqueue server did not start within 15s');
}

// ------------------------------------------------------------ fixtures

let PORT = 0;
let uniq = 0;

export function getPort(): number {
  return PORT;
}

export function qname(prefix: string): string {
  uniq += 1;
  return `ts-e2e-${prefix}-${Date.now().toString(36)}-${uniq}`;
}

export function makeQueue<T = unknown>(prefix: string): Queue<T> {
  return new Queue<T>(qname(prefix), { host: '127.0.0.1', port: PORT });
}

export function namedQueue<T = unknown>(name: string): Queue<T> {
  return new Queue<T>(name, { host: '127.0.0.1', port: PORT });
}

export function makeWorker<T, R>(
  queueName: string,
  processor: (job: Job<T>) => R | Promise<R>,
  opts: Record<string, unknown> = {}
): Worker<T, R> {
  return new Worker<T, R>(queueName, processor, {
    host: '127.0.0.1',
    port: PORT,
    pollTimeoutMs: 300,
    ...opts,
  });
}

// ------------------------------------------------------------ runner

/** Start a server, run every registered test, print results, set exit code. */
export async function runSuite(): Promise<void> {
  const { proc, port } = await startServer();
  PORT = port;
  let failed = 0;
  try {
    for (const { name, fn } of tests) {
      const start = Date.now();
      try {
        await fn();
        console.log(`PASS ${name} (${Date.now() - start}ms)`);
      } catch (err) {
        failed += 1;
        console.error(`FAIL ${name}: ${err instanceof Error ? err.message : err}`);
      }
    }
  } finally {
    proc.kill();
    cleanTempDirs();
  }
  console.log(`\n${tests.length - failed}/${tests.length} passed`);
  process.exit(failed > 0 ? 1 : 0);
}

/** Raw PULL: returns the job id and its lock token (undefined on lingering-lock reuse). */
export async function pullOne(
  queue: InstanceType<typeof Queue>,
  owner: string
): Promise<{ id: string; token: string | undefined }> {
  const response = await queue.connection.call({
    cmd: 'PULL',
    queue: queue.name,
    owner,
    timeout: 2000,
  });
  const job = response.job as { id: string } | null;
  if (!job) throw new Error('expected a job from PULL');
  const token = response.token == null ? undefined : String(response.token);
  return { id: String(job.id), token };
}
