import { describe, expect, it } from 'bun:test';
import { resolve } from 'node:path';

const repositoryRoot = resolve(import.meta.dir, '..');

describe('publish build cron serialization regression', () => {
  it('compiles the exact TypeScript surface executed by bun publish', () => {
    const result = Bun.spawnSync([process.execPath, 'run', 'build:lib'], {
      cwd: repositoryRoot,
      stdout: 'pipe',
      stderr: 'pipe',
      timeout: 30_000,
    });
    const output = `${result.stdout.toString()}${result.stderr.toString()}`;

    if (result.exitCode !== 0) {
      throw new Error(`build:lib failed with exit ${result.exitCode}:\n${output}`);
    }

    expect(result.exitCode).toBe(0);
  });
});
