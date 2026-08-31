import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { basename, join } from 'node:path';

const ROOT = join(import.meta.dir, '..');

interface CoverageEntry {
  area: 'Queue' | 'Worker' | 'Cron' | 'DLQ';
  section: string;
  guide: string;
  tcp: string[];
  embedded: string[];
}

function scripts(mode: 'tcp' | 'embedded', ...names: string[]): string[] {
  return names.map((name) => `scripts/${mode}/${name}`);
}

const COVERAGE: CoverageEntry[] = [
  {
    area: 'Queue',
    section: 'Overview',
    guide: 'queue/index.mdx',
    tcp: scripts('tcp', 'test-basic-operations.ts'),
    embedded: scripts('embedded', 'test-basic-operations.ts'),
  },
  {
    area: 'Queue',
    section: 'Adding Jobs',
    guide: 'queue/adding-jobs.mdx',
    tcp: scripts('tcp', 'test-basic-operations.ts', 'test-batch-operations.ts'),
    embedded: scripts('embedded', 'test-basic-operations.ts', 'test-batch-operations.ts'),
  },
  {
    area: 'Queue',
    section: 'Deduplication',
    guide: 'queue/deduplication.mdx',
    tcp: scripts('tcp', 'test-unique-jobs.ts', 'test-dedup-tcp.ts'),
    embedded: scripts('embedded', 'test-unique-jobs.ts'),
  },
  {
    area: 'Queue',
    section: 'Querying Jobs',
    guide: 'queue/querying.mdx',
    tcp: scripts('tcp', 'test-query-operations.ts'),
    embedded: scripts('embedded', 'test-query-operations.ts'),
  },
  {
    area: 'Queue',
    section: 'Control & Maintenance',
    guide: 'queue/control.mdx',
    tcp: scripts('tcp', 'test-queue-control.ts', 'test-bullmq-queue-methods.ts'),
    embedded: scripts('embedded', 'test-queue-control.ts', 'test-bullmq-queue-methods.ts'),
  },
  {
    area: 'Queue',
    section: 'Progress, Logs & Dependencies',
    guide: 'queue/progress.mdx',
    tcp: scripts('tcp', 'test-job-progress.ts', 'test-job-dependencies.ts'),
    embedded: scripts('embedded', 'test-job-progress.ts', 'test-job-dependencies.ts'),
  },
  {
    area: 'Queue',
    section: 'Rate Limits & Concurrency',
    guide: 'queue/limits.mdx',
    tcp: scripts('tcp', 'test-rate-limiting.ts', 'test-concurrency.ts'),
    embedded: scripts('embedded', 'test-rate-limiting.ts', 'test-concurrency.ts'),
  },
  {
    area: 'Queue',
    section: 'Rate Limiting in Depth',
    guide: 'rate-limiting.mdx',
    tcp: scripts('tcp', 'test-rate-limit-window-parity.ts'),
    embedded: scripts('embedded', 'test-rate-limit-window-parity.ts'),
  },
  {
    area: 'Queue',
    section: 'Job Groups',
    guide: 'queue/job-groups.mdx',
    tcp: scripts('tcp', 'test-job-groups.ts'),
    embedded: scripts('embedded', 'test-job-groups.ts'),
  },
  {
    area: 'Queue',
    section: 'Queue Groups',
    guide: 'queue-group.md',
    tcp: scripts('tcp', 'test-queue-group.ts', 'test-queue-group-advanced.ts'),
    embedded: scripts('embedded', 'test-queue-group.ts'),
  },
  {
    area: 'Queue',
    section: 'Workers & Metrics',
    guide: 'queue/metrics.mdx',
    tcp: scripts('tcp', 'test-worker-management.ts', 'test-monitoring.ts'),
    embedded: scripts('embedded', 'test-worker-management.ts', 'test-monitoring.ts'),
  },
  {
    area: 'Queue',
    section: 'Namespaces & Batching',
    guide: 'queue/advanced.mdx',
    tcp: scripts('tcp', 'test-prefix-key-parity.ts', 'test-batch-operations.ts'),
    embedded: scripts('embedded', 'test-prefix-key-parity.ts', 'test-batch-operations.ts'),
  },
  {
    area: 'Queue',
    section: 'Job Options Reference',
    guide: 'queue/options.mdx',
    tcp: scripts('tcp', 'test-advanced-job-options.ts', 'test-backoff-strategies.ts'),
    embedded: scripts(
      'embedded',
      'test-advanced-job-options.ts',
      'test-bullmq-job-options.ts',
      'test-backoff-strategies.ts'
    ),
  },
  {
    area: 'Worker',
    section: 'Overview',
    guide: 'worker/index.mdx',
    tcp: scripts('tcp', 'test-basic-operations.ts'),
    embedded: scripts('embedded', 'test-basic-operations.ts'),
  },
  {
    area: 'Worker',
    section: 'Concurrency & Batching',
    guide: 'worker/concurrency.mdx',
    tcp: scripts('tcp', 'test-concurrency.ts', 'test-batch-operations.ts'),
    embedded: scripts('embedded', 'test-concurrency.ts', 'test-batch-operations.ts'),
  },
  {
    area: 'Worker',
    section: 'The Job Object',
    guide: 'worker/job-object.mdx',
    tcp: scripts('tcp', 'test-job-progress.ts', 'test-job-management.ts'),
    embedded: scripts('embedded', 'test-job-progress.ts', 'test-job-advanced-methods.ts'),
  },
  {
    area: 'Worker',
    section: 'Events',
    guide: 'worker/events.mdx',
    tcp: scripts('tcp', 'test-job-lifecycle-events.ts'),
    embedded: scripts('embedded', 'test-queue-events.ts', 'test-job-progress.ts'),
  },
  {
    area: 'Worker',
    section: 'Errors & Retries',
    guide: 'worker/errors.mdx',
    tcp: scripts('tcp', 'test-retry-backoff.ts', 'test-timeout.ts'),
    embedded: scripts('embedded', 'test-retry-backoff.ts', 'test-timeout.ts'),
  },
  {
    area: 'Worker',
    section: 'Lifecycle & Shutdown',
    guide: 'worker/lifecycle.mdx',
    tcp: scripts('tcp', 'test-worker-lifecycle-parity.ts', 'test-graceful-shutdown.ts'),
    embedded: scripts('embedded', 'test-worker-lifecycle-parity.ts'),
  },
  {
    area: 'Worker',
    section: 'Heartbeats & Locks',
    guide: 'worker/stalls.mdx',
    tcp: scripts('tcp', 'test-stall-detection.ts', 'test-long-running-timeout.ts'),
    embedded: scripts('embedded', 'test-stall-detection.ts', 'test-timeout.ts'),
  },
  {
    area: 'Worker',
    section: 'Stall Detection in Depth',
    guide: 'stall-detection.mdx',
    tcp: scripts('tcp', 'test-stall-detection.ts'),
    embedded: scripts('embedded', 'test-stall-detection.ts'),
  },
  {
    area: 'Worker',
    section: 'CPU-Intensive Workers',
    guide: 'cpu-intensive-workers.mdx',
    tcp: scripts('tcp', 'test-sandboxed-worker-advanced.ts', 'test-stall-detection.ts'),
    embedded: scripts('embedded', 'test-sandboxed-workers.ts', 'test-stall-detection.ts'),
  },
  {
    area: 'Worker',
    section: 'SandboxedWorker',
    guide: 'worker/sandboxed.mdx',
    tcp: scripts('tcp', 'test-sandboxed-workers.ts', 'test-sandboxed-worker-advanced.ts'),
    embedded: scripts('embedded', 'test-sandboxed-workers.ts'),
  },
  {
    area: 'Worker',
    section: 'Options Reference',
    guide: 'worker/options.mdx',
    tcp: scripts('tcp', 'test-concurrency.ts', 'test-worker-advanced.ts', 'test-timeout.ts'),
    embedded: scripts(
      'embedded',
      'test-concurrency.ts',
      'test-worker-advanced.ts',
      'test-timeout.ts'
    ),
  },
  {
    area: 'Cron',
    section: 'Overview',
    guide: 'cron/index.mdx',
    tcp: scripts('tcp', 'test-cron-jobs.ts', 'test-cron-server.ts'),
    embedded: scripts('embedded', 'test-cron-jobs.ts', 'test-cron-server.ts'),
  },
  {
    area: 'Cron',
    section: 'Recipes',
    guide: 'cron/recipes.mdx',
    tcp: scripts('tcp', 'test-cron-jobs.ts', 'test-cron-advanced.ts'),
    embedded: scripts('embedded', 'test-cron-jobs.ts', 'test-cron-event-driven.ts'),
  },
  {
    area: 'Cron',
    section: 'Job Schedulers (Queue API)',
    guide: 'queue/schedulers.mdx',
    tcp: scripts('tcp', 'test-cron-advanced.ts', 'test-bullmq-queue-methods.ts'),
    embedded: scripts('embedded', 'test-bullmq-queue-methods.ts'),
  },
  {
    area: 'Cron',
    section: 'Expressions & Options',
    guide: 'cron/reference.mdx',
    tcp: scripts('tcp', 'test-cron-server.ts'),
    embedded: scripts('embedded', 'test-cron-server.ts'),
  },
  {
    area: 'DLQ',
    section: 'Overview',
    guide: 'dlq/index.mdx',
    tcp: scripts('tcp', 'test-dlq.ts'),
    embedded: scripts('embedded', 'test-dlq.ts'),
  },
  {
    area: 'DLQ',
    section: 'Operations',
    guide: 'dlq/operations.mdx',
    tcp: scripts('tcp', 'test-advanced-dlq.ts', 'test-dlq-patterns.ts'),
    embedded: scripts('embedded', 'test-advanced-dlq.ts'),
  },
  {
    area: 'DLQ',
    section: 'From the Queue API',
    guide: 'queue/dlq.mdx',
    tcp: scripts('tcp', 'test-advanced-dlq.ts'),
    embedded: scripts('embedded', 'test-advanced-dlq.ts'),
  },
  {
    area: 'DLQ',
    section: 'Automatic Retry',
    guide: 'dlq/auto-retry.mdx',
    tcp: scripts('tcp', 'test-advanced-dlq.ts'),
    embedded: scripts('embedded', 'test-advanced-dlq.ts'),
  },
  {
    area: 'DLQ',
    section: 'Configuration',
    guide: 'dlq/configuration.mdx',
    tcp: scripts('tcp', 'test-advanced-dlq.ts'),
    embedded: scripts('embedded', 'test-advanced-dlq.ts'),
  },
  {
    area: 'DLQ',
    section: 'Reference',
    guide: 'dlq/reference.mdx',
    tcp: scripts('tcp', 'test-advanced-dlq.ts', 'test-timeout.ts'),
    embedded: scripts('embedded', 'test-advanced-dlq.ts', 'test-timeout.ts'),
  },
];

const EXPECTED_SECTIONS = {
  Queue: [
    'Overview',
    'Adding Jobs',
    'Deduplication',
    'Querying Jobs',
    'Control & Maintenance',
    'Progress, Logs & Dependencies',
    'Rate Limits & Concurrency',
    'Rate Limiting in Depth',
    'Job Groups',
    'Queue Groups',
    'Workers & Metrics',
    'Namespaces & Batching',
    'Job Options Reference',
  ],
  Worker: [
    'Overview',
    'Concurrency & Batching',
    'The Job Object',
    'Events',
    'Errors & Retries',
    'Lifecycle & Shutdown',
    'Heartbeats & Locks',
    'Stall Detection in Depth',
    'CPU-Intensive Workers',
    'SandboxedWorker',
    'Options Reference',
  ],
  Cron: ['Overview', 'Recipes', 'Job Schedulers (Queue API)', 'Expressions & Options'],
  DLQ: [
    'Overview',
    'Operations',
    'From the Queue API',
    'Automatic Retry',
    'Configuration',
    'Reference',
  ],
} as const;

const PARITY_CONTRACTS = [
  ['advanced-dlq-contract.ts', 'test-advanced-dlq.ts'],
  ['job-groups-contract.ts', 'test-job-groups.ts'],
  ['prefix-key-contract.ts', 'test-prefix-key-parity.ts'],
  ['queue-group-contract.ts', 'test-queue-group.ts'],
  ['rate-limit-window-contract.ts', 'test-rate-limit-window-parity.ts'],
  ['stall-detection-contract.ts', 'test-stall-detection.ts'],
  ['timeout-contract.ts', 'test-timeout.ts'],
  ['worker-lifecycle-contract.ts', 'test-worker-lifecycle-parity.ts'],
] as const;

describe('documented Queue, Worker, Cron, and DLQ feature coverage', () => {
  test('contains every requested documentation section exactly once', () => {
    for (const [area, expected] of Object.entries(EXPECTED_SECTIONS)) {
      expect(COVERAGE.filter((entry) => entry.area === area).map((entry) => entry.section)).toEqual(
        expected
      );
    }
    expect(new Set(COVERAGE.map((entry) => `${entry.area}:${entry.section}`)).size).toBe(
      COVERAGE.length
    );
  });

  for (const entry of COVERAGE) {
    test(`${entry.area} / ${entry.section} has discoverable TCP and embedded evidence`, () => {
      const guidePath = join(ROOT, 'docs/src/content/docs/guide', entry.guide);
      expect(Bun.file(guidePath).size).toBeGreaterThan(0);
      expect(entry.tcp.length).toBeGreaterThan(0);
      expect(entry.embedded.length).toBeGreaterThan(0);

      for (const testPath of [...entry.tcp, ...entry.embedded]) {
        expect(basename(testPath).startsWith('test-')).toBe(true);
        expect(Bun.file(join(ROOT, testPath)).size).toBeGreaterThan(0);
      }

      for (const testPath of entry.tcp) {
        const source = readFileSync(join(ROOT, testPath), 'utf8');
        expect(source).not.toMatch(/embedded:\s*true|BUNQUEUE_EMBEDDED\s*=\s*['"]1['"]/);
      }
      for (const testPath of entry.embedded) {
        const source = readFileSync(join(ROOT, testPath), 'utf8');
        expect(source).toMatch(
          /embedded:\s*true|run\w+Contract\('embedded'\)|BUNQUEUE_EMBEDDED.*1/
        );
      }
    });
  }

  test('every authoritative parity contract has symmetric runner wrappers', () => {
    for (const [contract, wrapper] of PARITY_CONTRACTS) {
      expect(Bun.file(join(ROOT, 'scripts/shared', contract)).size).toBeGreaterThan(0);
      const tcp = readFileSync(join(ROOT, 'scripts/tcp', wrapper), 'utf8');
      const embedded = readFileSync(join(ROOT, 'scripts/embedded', wrapper), 'utf8');
      expect(tcp).toContain("('tcp')");
      expect(embedded).toContain("('embedded')");
    }
  });
});
