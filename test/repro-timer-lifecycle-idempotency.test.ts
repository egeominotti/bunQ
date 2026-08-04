/**
 * Regression coverage for field-owned timer handles that used to be
 * overwritten by repeated lifecycle calls and became impossible to stop.
 */
import { describe, expect, test } from 'bun:test';
import { join } from 'node:path';

const fixture = join(import.meta.dir, 'fixtures', 'timer-lifecycle-idempotency-probe.ts');

async function runProbe(scenario: string): Promise<Record<string, number>> {
  const child = Bun.spawn([process.execPath, fixture, scenario], {
    cwd: join(import.meta.dir, '..'),
    env: { ...process.env, LOG_LEVEL: 'error' },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const exited = await Promise.race([
    child.exited.then((exitCode) => ({ exitCode })),
    Bun.sleep(5_000).then(() => undefined),
  ]);
  if (!exited) child.kill('SIGKILL');
  const [stdout, stderr] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  expect(exited?.exitCode, stderr).toBe(0);
  const resultLine = stdout
    .trim()
    .split('\n')
    .findLast((line) => line.startsWith('{'));
  if (!resultLine) throw new Error(`Probe did not emit JSON:\n${stdout}`);
  return JSON.parse(resultLine) as Record<string, number>;
}

describe('timer lifecycle idempotency', () => {
  test('CancellationManager owns and clears a single graceful-cancel timer', async () => {
    expect(await runProbe('cancellation')).toEqual({
      afterRepeatedCancel: 1,
      gracePeriodAfterLaterCancel: 60_000,
      afterEarlierCancel: 1,
      gracePeriodAfterEarlierCancel: 30_000,
      afterUnregister: 0,
      afterDestroy: 0,
    });
  });

  test('PriorityAger repeated start retains one interval and destroy clears it', async () => {
    expect(await runProbe('aging')).toEqual({
      afterRepeatedStart: 1,
      afterDestroy: 0,
      afterRestart: 1,
      afterRestartDestroy: 0,
    });
  });

  test('PriorityAger ignores a stale callback after destroy', async () => {
    expect(await runProbe('aging-stale')).toEqual({ queriesAfterDestroy: 0 });
  });

  test('S3BackupManager start and stop own exactly one scheduler pair', async () => {
    expect(await runProbe('backup')).toEqual({
      timeoutsAfterRepeatedStart: 1,
      intervalsAfterRepeatedStart: 1,
      timeoutsAfterStop: 0,
      intervalsAfterStop: 0,
      timeoutsAfterRestart: 1,
      intervalsAfterRestart: 1,
      timeoutsAfterRestartStop: 0,
      intervalsAfterRestartStop: 0,
      scheduledBackupsAfterStaleCallbacks: 0,
    });
  });

  test('CloudAgent start and stop own one snapshot chain and subscription', async () => {
    expect(await runProbe('cloud')).toEqual({
      timeoutsAfterRepeatedStart: 1,
      subscriptionsAfterRepeatedStart: 1,
      timeoutsAfterStop: 0,
      subscriptionsAfterStop: 0,
      timeoutsAfterRestartAttempt: 0,
      subscriptionsAfterRestartAttempt: 0,
      snapshotSendsAfterStaleTimer: 1,
    });
  });
});
