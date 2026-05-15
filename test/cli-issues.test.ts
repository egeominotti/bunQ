/**
 * Tests reproducing 4 CLI issues discovered in the v2.7.13 audit:
 *
 * 1. HIGH — `worker register` via CLI does not persist (worker is
 *    auto-unregistered when the one-shot TCP connection closes).
 * 2. MEDIUM — `pull` response is missing the `state` field, so
 *    `formatJob` displays "State: unknown" instead of "active".
 * 3. LOW — `job progress` on a non-active job returns an ambiguous error
 *    ("Job not found or not active") that conflates two distinct states.
 * 4. LOW — Client side of the CLI ignores env vars like `BUNQUEUE_TCP_PORT`
 *    / `BUNQUEUE_HOST`, only honoring `--port`/`--host` flags. Server side
 *    supports env vars; the asymmetry surprises users.
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import type { Subprocess } from 'bun';
import { spawn } from 'bun';

const CLI_PATH = `${import.meta.dir}/../src/cli/index.ts`;
const PROJECT_ROOT = `${import.meta.dir}/..`;
const TCP_PORT = 18901;
const HTTP_PORT = 18902;

let server: Subprocess | null = null;

async function waitForServer(port: number, maxMs = 8000): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < maxMs) {
    try {
      const r = await fetch(`http://localhost:${port}/health`);
      if (r.ok) return true;
    } catch {
      // not ready
    }
    await Bun.sleep(100);
  }
  return false;
}

async function runCli(
  args: string[],
  env: Record<string, string> = {}
): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  const proc = spawn(['bun', 'run', CLI_PATH, ...args], {
    cwd: PROJECT_ROOT,
    stdout: 'pipe',
    stderr: 'pipe',
    env: { ...process.env, ...env },
  });
  const stdout = await new Response(proc.stdout).text();
  const stderr = await new Response(proc.stderr).text();
  const exitCode = await proc.exited;
  return { exitCode, stdout, stderr };
}

describe('CLI v2.7.13 audit issues', () => {
  beforeAll(async () => {
    server = spawn(['bun', 'run', 'src/main.ts'], {
      cwd: PROJECT_ROOT,
      stdout: 'pipe',
      stderr: 'pipe',
      env: {
        ...process.env,
        TCP_PORT: String(TCP_PORT),
        HTTP_PORT: String(HTTP_PORT),
      },
    });
    const ready = await waitForServer(HTTP_PORT);
    if (!ready) throw new Error('Server failed to start');
  });

  afterAll(async () => {
    if (server) {
      server.kill('SIGKILL');
      await Promise.race([server.exited, Bun.sleep(2000)]);
      server = null;
    }
  });

  // ─────────── Bug 1: worker register does not persist ───────────

  test('issue#1 — worker register via CLI: list should show the worker after registration', async () => {
    const reg = await runCli([
      '--port',
      String(TCP_PORT),
      'worker',
      'register',
      'audit-worker-1',
      '-q',
      'audit-q',
    ]);

    expect(reg.exitCode).toBe(0);
    expect(reg.stdout).toMatch(/Worker registered/i);

    const list = await runCli(['--port', String(TCP_PORT), 'worker', 'list']);

    // Currently fails: server unregisters the worker when the first CLI
    // process disconnects, so the second CLI process sees "No workers".
    // Expected behavior (after fix): either persist the registration
    // OR the register command warns that one-shot CLI registration is
    // transient.
    const persisted = /audit-worker-1/.test(list.stdout);
    const warned = /transient|one-shot|disconnect|expire/i.test(reg.stdout + reg.stderr);
    expect(persisted || warned).toBe(true);
  });

  // ─────────── Bug 2: pull response missing state field ───────────

  test('issue#2 — pull output shows the job state (not "unknown")', async () => {
    const push = await runCli([
      '--port',
      String(TCP_PORT),
      'push',
      'audit-pull-q',
      JSON.stringify({ v: 42 }),
    ]);
    expect(push.exitCode).toBe(0);

    const pull = await runCli(['--port', String(TCP_PORT), 'pull', 'audit-pull-q']);
    expect(pull.exitCode).toBe(0);

    // The pulled job is now `active`. Output must reflect that.
    expect(pull.stdout).not.toMatch(/State:\s+unknown/);
    expect(pull.stdout).toMatch(/State:\s+active/);
  });

  // ─────────── Bug 3: ambiguous job-progress error ───────────

  test('issue#3 — job progress on a waiting job: error distinguishes "not active" from "not found"', async () => {
    const push = await runCli([
      '--port',
      String(TCP_PORT),
      'push',
      'audit-progress-q',
      JSON.stringify({ v: 1 }),
    ]);
    const idMatch = push.stdout.match(/Job created:\s+([\w-]+)/);
    expect(idMatch).not.toBeNull();
    const jobId = idMatch![1];

    // Try to set progress on a waiting (not active) job
    const progressWaiting = await runCli([
      '--port',
      String(TCP_PORT),
      'job',
      'progress',
      jobId,
      '50',
    ]);

    // Try to set progress on a non-existent job
    const progressMissing = await runCli([
      '--port',
      String(TCP_PORT),
      'job',
      'progress',
      'nonexistent-job-id-9999',
      '50',
    ]);

    // BUG: both return the same ambiguous message.
    // Expected (after fix): waiting-job error mentions "not active" or "state",
    // missing-job error mentions "not found" — distinct so the user can act.
    const waitingMsg = (progressWaiting.stdout + progressWaiting.stderr).toLowerCase();
    const missingMsg = (progressMissing.stdout + progressMissing.stderr).toLowerCase();

    const waitingMentionsState =
      waitingMsg.includes('not active') ||
      waitingMsg.includes('state') ||
      waitingMsg.includes('waiting');
    const missingMentionsNotFound =
      missingMsg.includes('not found') || missingMsg.includes('does not exist');

    expect(waitingMentionsState).toBe(true);
    expect(missingMentionsNotFound).toBe(true);
    // And the two messages MUST differ.
    expect(waitingMsg.trim()).not.toBe(missingMsg.trim());
  });

  // ─────────── Bug 4: env var support for client port ───────────

  test('issue#4 — BUNQUEUE_TCP_PORT env var routes the client to the right server', async () => {
    // No --port flag. Only env var. Should connect to TCP_PORT (18901), not default 6789.
    const result = await runCli(['stats'], { BUNQUEUE_TCP_PORT: String(TCP_PORT) });

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toMatch(/Server Statistics|waiting/i);
    expect(result.stderr).not.toMatch(/Failed to connect/);
  });

  // ─────────── Bug 5: `job delay` on nonexistent vs waiting — same error ───────────

  test('issue#5 — job delay on nonexistent job: error is distinct from "not active"', async () => {
    const push = await runCli([
      '--port',
      String(TCP_PORT),
      'push',
      'audit-delay-q',
      JSON.stringify({ v: 1 }),
    ]);
    const idMatch = push.stdout.match(/Job created:\s+([\w-]+)/);
    const jobId = idMatch![1];

    // job delay on a waiting (not active) job
    const delayWaiting = await runCli([
      '--port',
      String(TCP_PORT),
      'job',
      'delay',
      jobId,
      '5000',
    ]);
    const delayMissing = await runCli([
      '--port',
      String(TCP_PORT),
      'job',
      'delay',
      'nonexistent-xyz',
      '5000',
    ]);

    const waitingMsg = (delayWaiting.stdout + delayWaiting.stderr).toLowerCase();
    const missingMsg = (delayMissing.stdout + delayMissing.stderr).toLowerCase();

    const waitingMentionsState =
      waitingMsg.includes('not active') ||
      waitingMsg.includes('state') ||
      waitingMsg.includes('current state') ||
      waitingMsg.includes('ok'); // delay on waiting may succeed
    const missingMentionsNotFound =
      missingMsg.includes('not found') || missingMsg.includes('does not exist');

    expect(missingMentionsNotFound).toBe(true);
    // If delayWaiting failed, it must NOT have the same generic message as missing
    if (!waitingMsg.includes('ok')) {
      expect(waitingMentionsState).toBe(true);
      expect(waitingMsg.trim()).not.toBe(missingMsg.trim());
    }
  });

  // ─────────── Bug 6: queue clean shows "Created" instead of "Cleaned" ───────────

  test('issue#6 — queue clean: output uses correct verb (not "Created")', async () => {
    // Push and complete a job so clean has something to remove
    await runCli([
      '--port',
      String(TCP_PORT),
      'push',
      'audit-clean-q',
      JSON.stringify({ v: 1 }),
    ]);

    const result = await runCli([
      '--port',
      String(TCP_PORT),
      'queue',
      'clean',
      'audit-clean-q',
      '--grace',
      '0',
    ]);
    expect(result.exitCode).toBe(0);
    // BUG was: "Created 0 jobs: " — confusingly applied to clean operations.
    // After fix: queue/dlq scoped commands say "Cleaned N jobs" or just count.
    expect(result.stdout).not.toMatch(/Created \d+ jobs/);
  });

  // ─────────── Bug 8: -h / -v short flag aliases ───────────

  test('issue#8 — -h is an alias for --help', async () => {
    const result = await runCli(['-h']);
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toMatch(/USAGE|bunqueue/i);
    expect(result.stdout).not.toMatch(/Failed to start server/);
  });

  test('issue#8 — -v is an alias for --version', async () => {
    const result = await runCli(['-v']);
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toMatch(/bunqueue v\d/);
    expect(result.stdout).not.toMatch(/Failed to start server/);
  });

  // ─────────── Bug 7: `job result` shows literal "undefined" ───────────

  test('issue#9 — dlq retry does NOT print misleading "Cleaned" verb', async () => {
    const result = await runCli([
      '--port',
      String(TCP_PORT),
      'dlq',
      'retry',
      'audit-retry-q',
    ]);
    expect(result.exitCode).toBe(0);
    // Must never wrongly claim jobs were "Cleaned" — they were retried.
    expect(result.stdout).not.toMatch(/Cleaned \d+ jobs/);
  });

  test('issue#10 — TCP_PORT env var (server primary) is honored by the client', async () => {
    const result = await runCli(['stats'], { TCP_PORT: String(TCP_PORT) });
    expect(result.exitCode).toBe(0);
    expect(result.stderr).not.toMatch(/Failed to connect/);
  });

  test('issue#7 — job result with no result: clear message, not literal "undefined"', async () => {
    const push = await runCli([
      '--port',
      String(TCP_PORT),
      'push',
      'audit-result-q',
      JSON.stringify({ v: 1 }),
    ]);
    const idMatch = push.stdout.match(/Job created:\s+([\w-]+)/);
    const jobId = idMatch![1];

    const result = await runCli(['--port', String(TCP_PORT), 'job', 'result', jobId]);
    expect(result.exitCode).toBe(0);
    expect(result.stdout).not.toMatch(/Result:\s+undefined/);
    expect(result.stdout.toLowerCase()).toMatch(/no result|not completed|not available/);
  });
});
