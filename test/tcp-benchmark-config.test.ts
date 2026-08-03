import { afterAll, describe, expect, test } from 'bun:test';
import { mkdtempSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { resolveServerRateLimit } from '../bench/tcp-bench';

const temporaryDirectories: string[] = [];

function freePortPair(): number {
  for (let attempt = 0; attempt < 20; attempt++) {
    const first = Bun.listen({
      hostname: '127.0.0.1',
      port: 0,
      socket: { data: () => undefined },
    });
    const port = first.port;
    first.stop(true);
    try {
      const second = Bun.listen({
        hostname: '127.0.0.1',
        port: port + 1,
        socket: { data: () => undefined },
      });
      second.stop(true);
      return port;
    } catch {
      // Try another adjacent pair.
    }
  }
  throw new Error('Unable to allocate adjacent benchmark ports');
}

afterAll(() => {
  for (const directory of temporaryDirectories) rmSync(directory, { recursive: true, force: true });
});

describe('TCP benchmark broker configuration', () => {
  test('the default broker limit cannot cap the default producer workload', () => {
    expect(resolveServerRateLimit(20_000)).toBeGreaterThan(20_001);
  });

  test('larger BENCH_N values expand the isolated broker limit', () => {
    expect(resolveServerRateLimit(50_000)).toBeGreaterThan(50_001);
  });

  test('an explicit protocol limit remains an intentional benchmark input', () => {
    expect(resolveServerRateLimit(50_000, '12345')).toBe(12_345);
  });

  test('a real default-derived broker limit completes a workload above the core safety cap', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-tcp-bench-test-'));
    temporaryDirectories.push(directory);
    const { RATE_LIMIT_MAX_REQUESTS: _limit, RATE_LIMIT_WINDOW_MS: _window, ...env } = process.env;
    const child = Bun.spawn([process.execPath, 'run', 'bench/tcp-bench.ts'], {
      cwd: join(import.meta.dir, '..'),
      env: {
        ...env,
        BENCH_N: '10050',
        BENCH_RUNS: '1',
        BENCH_PORT: String(freePortPair()),
        BENCH_TMP_DIR: directory,
      },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ]);
    expect(exitCode).toBe(0);
    expect(stderr).toBe('');
    expect(stdout).toContain('protocolRateLimit=11050/60000ms (derived)');
    expect(stdout).not.toContain('Rate limit exceeded');
    expect(readdirSync(directory)).toEqual([]);
  }, 60_000);
});
