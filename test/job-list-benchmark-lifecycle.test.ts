import { describe, expect, test } from 'bun:test';
import { join } from 'node:path';

describe('job-list benchmark lifecycle', () => {
  test('the benchmark releases the embedded manager and exits on its own', async () => {
    const child = Bun.spawn([process.execPath, 'run', 'bench/job-list-perf.ts'], {
      cwd: join(import.meta.dir, '..'),
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

    expect(exited?.exitCode).toBe(0);
    expect(stderr).toBe('');
    expect(stdout).toContain('--- 20000 total jobs');
  }, 30_000);
});
