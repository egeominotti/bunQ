import { expect, test } from 'bun:test';
import { join } from 'node:path';

test('a stopped TCP broker exits after its protocol limiter handled a command', async () => {
  const fixture = join(import.meta.dir, 'fixtures', 'tcp-server-rate-limiter-lifecycle.ts');
  const child = Bun.spawn([process.execPath, fixture], {
    cwd: join(import.meta.dir, '..'),
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const exited = await Promise.race([
    child.exited.then((exitCode) => ({ exitCode })),
    Bun.sleep(2_000).then(() => undefined),
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
}, 4_000);
