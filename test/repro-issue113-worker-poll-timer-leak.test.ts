/**
 * Regression for GitHub issue #113.
 *
 * Every completed job used to create another self-perpetuating 10 ms poll
 * timer chain. Concurrent pull continuations could also arm more than one
 * replacement timer after both had passed poll's entry guard. The probe runs
 * in a child process so timer instrumentation cannot affect the test runner,
 * and exercises the public Queue/Worker API against a real TCP server.
 */
import { describe, expect, test } from 'bun:test';
import { join } from 'node:path';

const fixture = join(import.meta.dir, 'fixtures', 'issue113-worker-poll-timer-probe.ts');

describe('Issue #113 - Worker poll timer lifecycle', () => {
  test('concurrent scheduling owns one poll timer and retains its earliest deadline', async () => {
    const child = Bun.spawn([process.execPath, fixture], {
      cwd: join(import.meta.dir, '..'),
      env: { ...process.env, LOG_LEVEL: 'error' },
      stdout: 'pipe',
      stderr: 'pipe',
    });

    const exited = await Promise.race([
      child.exited.then((exitCode) => ({ exitCode })),
      Bun.sleep(15_000).then(() => undefined),
    ]);
    if (!exited) child.kill('SIGKILL');

    const [stdout, stderr] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ]);
    expect(exited?.exitCode, stderr).toBe(0);

    const result = JSON.parse(stdout.trim()) as {
      completed: number;
      expected: number;
      outstandingAfterDrain: number;
      outstandingAfterConcurrentScheduling: number;
      ownedDelaysAfterLaterSchedule: number[];
    };
    expect(result.completed).toBe(result.expected);
    expect(result.outstandingAfterDrain).toBeLessThanOrEqual(1);
    expect(result.outstandingAfterConcurrentScheduling).toBeLessThanOrEqual(1);
    expect(result.ownedDelaysAfterLaterSchedule).toEqual([25]);
  }, 20_000);
});
