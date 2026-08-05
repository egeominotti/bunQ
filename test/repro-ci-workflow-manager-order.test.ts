/**
 * Regression for the v2.8.58 Linux CI order where an in-memory Workflow Engine
 * left the process-wide embedded manager alive before a SQLite-backed workflow
 * suite claimed its own database.
 */
import { describe, expect, test } from 'bun:test';
import { join } from 'node:path';

const repositoryRoot = join(import.meta.dir, '..');

describe('workflow test manager isolation', () => {
  test('an in-memory workflow suite cannot poison a later SQLite workflow suite', async () => {
    const child = Bun.spawn(
      [
        process.execPath,
        'test',
        'test/workflow-loops.test.ts',
        'test/repro-model-workflow-timeout-reason.test.ts',
      ],
      {
        cwd: repositoryRoot,
        env: { ...process.env, BUNQUEUE_EMBEDDED: '1' },
        stdout: 'pipe',
        stderr: 'pipe',
      }
    );

    const completed = await Promise.race([
      child.exited.then((exitCode) => ({ exitCode })),
      Bun.sleep(15_000).then(() => undefined),
    ]);
    if (!completed) child.kill('SIGKILL');

    const [stdout, stderr] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ]);

    expect(completed, `${stdout}\n${stderr}`).toBeDefined();
    expect(completed?.exitCode, `${stdout}\n${stderr}`).toBe(0);
  }, 20_000);
});
