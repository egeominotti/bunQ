import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const repositoryRoot = join(import.meta.dir, '..');

function source(path: string): string {
  return readFileSync(join(repositoryRoot, path), 'utf8');
}

describe('native benchmark runner integrity', () => {
  test('external TCP runners accept an isolated endpoint instead of hard-coding port 6789', () => {
    for (const path of ['bench/comprehensive.ts', 'bench/pushbulk-delta.ts']) {
      const contents = source(path);
      expect(contents).toContain('BENCH_HOST');
      expect(contents).toContain('BENCH_PORT');
      expect(contents).not.toContain('port: 6789');
    }
  });

  test('long-running benchmark entry points exit naturally and stay within 300 lines', () => {
    for (const path of [
      'bench/comprehensive.ts',
      'bench/pushbulk-delta.ts',
      'scripts/bench-tcp-batch-notify.ts',
    ]) {
      const contents = source(path);
      expect(contents).not.toContain('process.exit(');
      expect(contents).toContain('import.meta.main');
      expect(contents.split('\n').length).toBeLessThanOrEqual(301);
    }
  });

  test('processing benchmarks require accepted-ID and authoritative terminal completion', () => {
    for (const path of ['bench/comprehensive.ts', 'scripts/bench-tcp-batch-notify.ts']) {
      const contents = source(path);
      expect(contents).toContain('assertExactCompletion');
      expect(contents).toContain('assertExactDeliveries');
      expect(contents).toContain('getJobCounts');
      expect(contents).toContain('deadline');
    }
  });

  test('the self-hosted TCP benchmark binds a dynamic port', () => {
    const contents = source('scripts/bench-tcp-batch-notify.ts');
    expect(contents).toContain('port: 0');
    expect(contents).toContain('server.port');
  });

  test('the self-hosted benchmark retains every terminal job in its largest scenario', () => {
    const contents = source('scripts/bench-tcp-batch-notify.ts');
    expect(contents).toContain('maxCompletedJobs: MAX_SCENARIO_JOBS');
    expect(contents).toContain('const MAX_SCENARIO_JOBS = 100_000');
  });

  test('the comprehensive embedded campaign resets its shared manager between samples', () => {
    const contents = source('bench/comprehensive.ts');
    expect(contents).toContain("import { shutdownManager } from '../src/client/manager'");
    expect(contents).toContain(
      'await closeAll([worker, processQueue, bulkQueue, pushQueue]);\n        shutdownManager();'
    );
  });

  test('the comprehensive campaign exposes a durable-SQLite processing deadline', () => {
    const contents = source('bench/comprehensive.ts');
    expect(contents).toContain("positiveInteger('BENCH_TIMEOUT_MS', 600_000)");
    expect(contents).toContain('const PROCESS_TIMEOUT_MS =');
  });

  test('the push/bulk campaign shuts down its embedded singleton after reporting', () => {
    const contents = source('bench/pushbulk-delta.ts');
    expect(contents).toContain("import { shutdownManager } from '../src/client/manager'");
    expect(contents).toContain('shutdownManager();');
  });
});
