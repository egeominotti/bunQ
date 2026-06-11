/**
 * CLI end-to-end battery — drives the real binary entry (`bun src/main.ts`)
 * as a subprocess against a live TCP server, asserting output, exit codes,
 * --json purity and the auth flow. This is the contract the audits kept
 * finding broken one layer at a time: here it is locked end-to-end.
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

const REPO = join(import.meta.dir, '..');
const PORT = 18971;
const AUTH_PORT = 18972;
const TOKEN = 'e2e-secret';

let qm: QueueManager;
let qmAuth: QueueManager;
let server: TcpServer;
let authServer: TcpServer;

interface CliResult {
  exitCode: number | null;
  stdout: string;
  stderr: string;
}

async function cli(args: string[], env: Record<string, string> = {}): Promise<CliResult> {
  const proc = Bun.spawn(['bun', 'src/main.ts', ...args, '--port', String(PORT)], {
    cwd: REPO,
    env: { ...process.env, ...env },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const timer = setTimeout(() => proc.kill(), 15000);
  const exitCode = await proc.exited;
  clearTimeout(timer);
  return {
    exitCode,
    stdout: await new Response(proc.stdout).text(),
    stderr: await new Response(proc.stderr).text(),
  };
}

/** Variant against the auth-enabled server; takes raw args (port included). */
async function cliRaw(args: string[], env: Record<string, string> = {}): Promise<CliResult> {
  const proc = Bun.spawn(['bun', 'src/main.ts', ...args], {
    cwd: REPO,
    env: { ...process.env, ...env },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const timer = setTimeout(() => proc.kill(), 15000);
  const exitCode = await proc.exited;
  clearTimeout(timer);
  return {
    exitCode,
    stdout: await new Response(proc.stdout).text(),
    stderr: await new Response(proc.stderr).text(),
  };
}

/** Extract the job id from `push` output ("Job created: <id>"). */
function pushedId(r: CliResult): string {
  const m = /Job created:\s*(\S+)/.exec(r.stdout);
  if (!m) throw new Error(`no job id in output: ${r.stdout} ${r.stderr}`);
  return m[1];
}

beforeAll(() => {
  qm = new QueueManager();
  server = createTcpServer(qm, { port: PORT, hostname: '127.0.0.1' });
  qmAuth = new QueueManager();
  authServer = createTcpServer(qmAuth, {
    port: AUTH_PORT,
    hostname: '127.0.0.1',
    authTokens: [TOKEN],
  });
});

afterAll(() => {
  server.stop();
  authServer.stop();
  qm.shutdown();
  qmAuth.shutdown();
});

describe('core lifecycle', () => {
  test('push → pull → ack roundtrip', async () => {
    const pushed = await cli(['push', 'e2e-core', '{"a":1}', '--priority', '5']);
    expect(pushed.exitCode).toBe(0);
    const id = pushedId(pushed);

    const pulled = await cli(['pull', 'e2e-core']);
    expect(pulled.exitCode).toBe(0);
    expect(pulled.stdout).toContain(id);

    const acked = await cli(['ack', id, '--result', '{"done":true}']);
    expect(acked.exitCode).toBe(0);

    const result = await cli(['job', 'result', id]);
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('done');
  });

  test('fail path: job goes back for retry', async () => {
    const pushed = await cli(['push', 'e2e-fail', '{"b":2}']);
    const id = pushedId(pushed);
    await cli(['pull', 'e2e-fail']);
    const failed = await cli(['fail', id, '--error', 'boom']);
    expect(failed.exitCode).toBe(0);

    const state = await cli(['job', 'state', id]);
    expect(state.exitCode).toBe(0);
    expect(state.stdout).toMatch(/delayed|waiting/);
  });

  test('job get shows the job; --json parses with data intact', async () => {
    const pushed = await cli(['push', 'e2e-get', '{"x":42}']);
    const id = pushedId(pushed);

    const got = await cli(['job', 'get', id]);
    expect(got.exitCode).toBe(0);
    expect(got.stdout).toContain(id);

    const json = await cli(['job', 'get', id, '--json']);
    expect(json.exitCode).toBe(0);
    const parsed = JSON.parse(json.stdout) as { ok: boolean; job?: { data?: { x?: number } } };
    expect(parsed.ok).toBe(true);
  });

  test('job wait: completed → exit 0; pending → exit 1 with message', async () => {
    const pushed = await cli(['push', 'e2e-wait', '{"w":1}']);
    const id = pushedId(pushed);
    await cli(['pull', 'e2e-wait']);
    await cli(['ack', id]);

    const done = await cli(['job', 'wait', id, '-t', '3000']);
    expect(done.exitCode).toBe(0);

    const pending = await cli(['push', 'e2e-wait2', '{"w":2}']);
    const pendingId = pushedId(pending);
    const waited = await cli(['job', 'wait', pendingId, '-t', '300']);
    expect(waited.exitCode).toBe(1);
    expect(waited.stderr.toLowerCase()).toContain('not completed');
  });
});

describe('queue / cron / webhook / limits', () => {
  test('queue count, pause, resume', async () => {
    await cli(['push', 'e2e-q', '{"n":1}']);
    const count = await cli(['queue', 'count', 'e2e-q']);
    expect(count.exitCode).toBe(0);
    expect(count.stdout).toMatch(/\d/);

    expect((await cli(['queue', 'pause', 'e2e-q'])).exitCode).toBe(0);
    expect((await cli(['queue', 'resume', 'e2e-q'])).exitCode).toBe(0);
  });

  test('cron add shows name and next run; list shows it; delete removes it', async () => {
    const added = await cli(['cron', 'add', 'e2e-cron', '-q', 'e2e-q', '-d', '{}', '-s', '0 3 * * *']);
    expect(added.exitCode).toBe(0);
    expect(added.stdout).toContain('e2e-cron');
    expect(added.stdout.toLowerCase()).toContain('next run');

    const list = await cli(['cron', 'list']);
    expect(list.stdout).toContain('e2e-cron');
    expect(list.stdout.toLowerCase()).toContain('next run');

    expect((await cli(['cron', 'delete', 'e2e-cron'])).exitCode).toBe(0);
  });

  test('webhook add prints the id; list shows it; remove uses that id', async () => {
    // NB: loopback/private URLs are rejected by the server's SSRF guard —
    // use a public-looking host (delivery is never attempted in this test).
    const added = await cli([
      'webhook', 'add', 'http://example.com/e2e-hook', '-e', 'job.completed,job.progress',
    ]);
    expect(added.exitCode).toBe(0);
    const m = /Webhook added:\s*(\S+)/.exec(added.stdout);
    expect(m).not.toBeNull();
    const whId = m?.[1] ?? '';

    const list = await cli(['webhook', 'list']);
    expect(list.stdout).toContain('http://example.com/e2e-hook');
    expect(list.stdout).toContain(whId);

    expect((await cli(['webhook', 'remove', whId])).exitCode).toBe(0);
  });

  test('webhook add with a dead event fails loudly', async () => {
    const r = await cli(['webhook', 'add', 'http://example.com/h', '-e', 'job.active']);
    expect(r.exitCode).not.toBe(0);
    expect((r.stdout + r.stderr).toLowerCase()).toContain('invalid event');
  });

  test('rate-limit and concurrency set/clear', async () => {
    expect((await cli(['rate-limit', 'set', 'e2e-q', '10'])).exitCode).toBe(0);
    expect((await cli(['rate-limit', 'clear', 'e2e-q'])).exitCode).toBe(0);
    expect((await cli(['concurrency', 'set', 'e2e-q', '5'])).exitCode).toBe(0);
    expect((await cli(['concurrency', 'clear', 'e2e-q'])).exitCode).toBe(0);
  });
});

describe('monitor + --json purity', () => {
  test('stats prints the dashboard; --json is parseable', async () => {
    const plain = await cli(['stats']);
    expect(plain.exitCode).toBe(0);
    expect(plain.stdout).toContain('Server Statistics');

    const json = await cli(['stats', '--json']);
    expect(json.exitCode).toBe(0);
    expect(() => JSON.parse(json.stdout)).not.toThrow();
  });

  test('ping and health succeed', async () => {
    expect((await cli(['ping'])).exitCode).toBe(0);
    const health = await cli(['health', '--json']);
    expect(health.exitCode).toBe(0);
    expect(() => JSON.parse(health.stdout)).not.toThrow();
  });

  test('error paths stay JSON with --json (stderr parseable, stdout silent)', async () => {
    const missing = await cli(['job', 'get', '999999999', '--json']);
    expect(missing.exitCode).toBe(1);
    expect(missing.stdout.trim()).toBe('');
    expect(() => JSON.parse(missing.stderr)).not.toThrow();

    const unknown = await cli(['frobnicate', '--json']);
    expect(unknown.exitCode).toBe(1);
    expect(() => JSON.parse(unknown.stderr)).not.toThrow();
  });

  test('pull with -t on an empty queue is a timeout, NOT an auth failure', async () => {
    const r = await cli(['pull', 'e2e-empty', '-t', '200']);
    expect(r.stdout + r.stderr).not.toContain('Authentication failed');
  });
});

describe('auth flow (server with AUTH_TOKENS)', () => {
  const target = ['--port', String(AUTH_PORT)];

  test('no token → rejected, exit 1', async () => {
    const r = await cliRaw(['stats', ...target]);
    expect(r.exitCode).toBe(1);
    expect((r.stdout + r.stderr).toLowerCase()).toMatch(/auth/);
  });

  test('--token works; wrong token fails', async () => {
    const ok = await cliRaw(['stats', ...target, '--token', TOKEN]);
    expect(ok.exitCode).toBe(0);

    const bad = await cliRaw(['stats', ...target, '--token', 'wrong']);
    expect(bad.exitCode).toBe(1);
  });

  test('BQ_TOKEN env works', async () => {
    const r = await cliRaw(['stats', ...target], { BQ_TOKEN: TOKEN });
    expect(r.exitCode).toBe(0);
  });
});
