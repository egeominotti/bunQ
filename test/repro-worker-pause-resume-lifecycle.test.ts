import { describe, expect, test } from 'bun:test';
import { join } from 'node:path';

const repositoryRoot = join(import.meta.dir, '..');
const fixture = join(import.meta.dir, 'fixtures', 'worker-pause-resume-lifecycle.ts');

async function runLifecycleFixture(mode: 'embedded' | 'tcp'): Promise<void> {
  const child = Bun.spawn([process.execPath, fixture, mode], {
    cwd: repositoryRoot,
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const exited = await Promise.race([
    child.exited.then((exitCode) => ({ exitCode })),
    Bun.sleep(3_000).then(() => undefined),
  ]);
  if (!exited) child.kill('SIGKILL');
  const [stdout, stderr] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);

  expect(stderr).toBe('');
  expect(stdout).toBe('');
  expect(exited?.exitCode).toBe(0);
}

describe('Worker pause/resume lifecycle', () => {
  test('embedded closes every heartbeat timer and exits naturally', async () => {
    await runLifecycleFixture('embedded');
  }, 5_000);

  test('TCP closes every heartbeat timer and exits naturally', async () => {
    await runLifecycleFixture('tcp');
  }, 5_000);
});
