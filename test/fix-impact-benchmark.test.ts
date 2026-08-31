import { describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { benchmarkMixedGroupPath } from '../bench/fix-impact/group-paths';
import { Harness } from '../bench/fix-impact/harness';
import { SMOKE_PROFILE, type RuntimeModules } from '../bench/fix-impact/types';

interface BenchmarkReport {
  metadata: { profile: string };
  results: Array<{
    id: string;
    samples: number[];
    distribution: { median: number; p95: number };
    correctness: Record<string, unknown>;
    comparable: boolean;
  }>;
}

describe('fix-impact benchmark harness', () => {
  test('rejects mixed-path samples with the right counts but the wrong exact order', async () => {
    class WrongOrderQueueManager {
      async pushBatch(_queue: string, inputs: Array<Record<string, unknown>>): Promise<string[]> {
        return inputs.map((_, index) => `job-${index}`);
      }

      async pullBatch(): Promise<
        Array<{
          id: string;
          priority: number;
          createdAt: number;
          data: unknown;
          groupId?: string;
        }>
      > {
        return [
          { id: 'plain-4', priority: 0, createdAt: 4, data: { grouped: false, index: 4 } },
          { id: 'plain-2', priority: 0, createdAt: 2, data: { grouped: false, index: 2 } },
          { id: 'plain-0', priority: 0, createdAt: 0, data: { grouped: false, index: 0 } },
          {
            id: 'tenant-0-later',
            priority: 0,
            createdAt: 5,
            data: { grouped: true, index: 5 },
            groupId: 'tenant-0',
          },
          {
            id: 'tenant-1-first',
            priority: 0,
            createdAt: 3,
            data: { grouped: true, index: 3 },
            groupId: 'tenant-1',
          },
        ];
      }

      shutdown(): void {}
    }

    const runtime = {
      QueueManager: WrongOrderQueueManager,
    } as unknown as RuntimeModules;
    const harness = new Harness(
      runtime,
      { ...SMOKE_PROFILE, mixedJobs: 6, mixedGroups: 2, queuePathSamples: 1 },
      'wrong-order'
    );

    await benchmarkMixedGroupPath(harness);

    for (const result of harness.results) {
      expect(result.correctness.allCorrect).toBe(false);
      expect(result.comparable).toBe(false);
    }
  });

  test('rejects an unknown benchmark selector instead of writing an empty success', async () => {
    const root = resolve(import.meta.dir, '..');
    const output = join(tmpdir(), `bunqueue-fix-impact-unknown-${randomUUID()}.json`);
    const child = Bun.spawn(
      [
        process.execPath,
        'bench/fix-impact.ts',
        '--only=does-not-exist',
        '--profile=smoke',
        `--output=${output}`,
      ],
      { cwd: root, stdout: 'pipe', stderr: 'pipe' }
    );

    try {
      const [exitCode, stderr] = await Promise.all([
        child.exited,
        new Response(child.stderr).text(),
      ]);
      expect(exitCode).not.toBe(0);
      expect(stderr).toContain('Unknown benchmark selector');
      expect(await Bun.file(output).exists()).toBe(false);
    } finally {
      await rm(output, { force: true });
    }
  });

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
        'queue-ungrouped-push-batch',
        'queue-ungrouped-pull-batch',
        'queue-mixed-groups-push-batch',
        'queue-mixed-groups-pull-batch',
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
      expect(correctness['queue-ungrouped-push-batch']?.allCorrect).toBe(true);
      expect(correctness['queue-ungrouped-pull-batch']?.allCorrect).toBe(true);
      expect(correctness['queue-mixed-groups-push-batch']?.allCorrect).toBe(true);
      expect(correctness['queue-mixed-groups-pull-batch']?.allCorrect).toBe(true);
      expect(correctness['stats-queues-summary']?.priorityFieldPresent).toBe(true);
      expect(correctness['waiter-notify-batch']?.surplusCoalesced).toBe(true);
      expect(correctness['delayed-heap-churn']?.noRetainedStaleEntries).toBe(true);
    } finally {
      await rm(output, { force: true });
    }
  }, 30_000);
});
