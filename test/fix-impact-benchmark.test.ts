import { describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

interface BenchmarkReport {
  metadata: { profile: string };
  results: Array<{
    id: string;
    samples: number[];
    distribution: { median: number; p95: number };
    correctness: Record<string, unknown>;
  }>;
}

describe('fix-impact benchmark harness', () => {
  test('smoke profile records timings and candidate correctness invariants', async () => {
    const root = resolve(import.meta.dir, '..');
    const output = join(tmpdir(), `bunqueue-fix-impact-smoke-${randomUUID()}.json`);
    const child = Bun.spawn(
      [
        process.execPath,
        'bench/fix-impact.ts',
        '--label=test-smoke',
        '--revision=worktree',
        `--source-root=${root}`,
        '--profile=smoke',
        `--output=${output}`,
      ],
      { cwd: root, stdout: 'pipe', stderr: 'pipe' }
    );

    try {
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      expect({ exitCode, stderr }).toEqual({ exitCode: 0, stderr: '' });
      expect(stdout).toContain('sqlite-recovery-active');

      const report = (await Bun.file(output).json()) as BenchmarkReport;
      expect(report.metadata.profile).toBe('smoke');
      expect(report.results.map((result) => result.id)).toEqual([
        'sqlite-recovery-active',
        'getjobs-memory-desc-page',
        'getjobs-sql-prioritized',
        'getjobs-sql-deep-page',
        'pull-group-head-of-line',
        'stats-global',
        'stats-queues-summary',
        'temporal-queue-lookup',
        'temporal-remove-by-id',
        'waiter-notify-batch',
        'delayed-heap-churn',
      ]);
      for (const result of report.results) {
        expect(result.samples.length).toBeGreaterThan(0);
        expect(result.distribution.median).toBeGreaterThanOrEqual(0);
        expect(result.distribution.p95).toBeGreaterThanOrEqual(result.distribution.median);
      }

      const correctness = Object.fromEntries(
        report.results.map((result) => [result.id, result.correctness])
      );
      expect(correctness['sqlite-recovery-active']?.allComplete).toBe(true);
      expect(correctness['getjobs-memory-desc-page']?.exactPage).toBe(true);
      expect(correctness['getjobs-sql-prioritized']?.fullPage).toBe(true);
      expect(correctness['getjobs-sql-deep-page']?.usesTemporarySort).toBe(false);
      expect(correctness['pull-group-head-of-line']?.eligibleJobReturnedEveryTime).toBe(true);
      expect(correctness['stats-queues-summary']?.priorityFieldPresent).toBe(true);
      expect(correctness['waiter-notify-batch']?.surplusCoalesced).toBe(true);
      expect(correctness['delayed-heap-churn']?.noRetainedStaleEntries).toBe(true);
    } finally {
      await rm(output, { force: true });
    }
  }, 30_000);
});
