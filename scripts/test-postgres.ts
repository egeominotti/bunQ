#!/usr/bin/env bun

const ROOT = `${import.meta.dir}/..`;

export const postgresTestProfiles = {
  smoke: [
    'test/postgres-config.test.ts',
    'test/postgres-lifecycle.test.ts',
    'test/postgres-multibroker.test.ts',
    'test/postgres-public-api-flow.test.ts',
    'test/postgres-public-api-queue-worker.test.ts',
    'test/postgres-schema-dedup-guard.test.ts',
    'test/postgres-schema-guard.test.ts',
  ],
  destruction: [
    'test/postgres-connection-recovery.test.ts',
    'test/postgres-core-transaction-retry.test.ts',
    'test/postgres-destructive-dependency-locking.test.ts',
    'test/postgres-dlq-generation-race.test.ts',
    'test/postgres-event-retention-races.test.ts',
    'test/postgres-fast-check-destruction.test.ts',
    'test/postgres-postcommit-shutdown-regression.test.ts',
    'test/postgres-process-diagnostics.test.ts',
    'test/postgres-process-output.test.ts',
    'test/postgres-public-api-extreme.test.ts',
    'test/postgres-queue-lifecycle-races.test.ts',
    'test/postgres-schema-dedup-guard.test.ts',
    'test/postgres-shutdown-admission-regressions.test.ts',
  ],
  pressure: [
    'test/postgres-ackb-lock-timeout.test.ts',
    'test/postgres-advisory-lock-collisions.test.ts',
    'test/postgres-batch-performance.test.ts',
    'test/postgres-concurrency-regressions.test.ts',
    'test/postgres-core-transaction-retry.test.ts',
    'test/postgres-fast-check-distributed.test.ts',
    'test/postgres-fast-check-events.test.ts',
    'test/postgres-flow-admission-batching.test.ts',
    'test/postgres-four-processes.test.ts',
    'test/postgres-metric-contention.test.ts',
    'test/postgres-process-diagnostics.test.ts',
    'test/postgres-process-output.test.ts',
    'test/postgres-public-api-extreme.test.ts',
    'test/postgres-ten-processes.test.ts',
  ],
  battle: ['test/postgres-*.test.ts'],
} as const;

function profileFiles(files: readonly string[]): readonly string[] {
  if (files.length !== 1 || !files[0].startsWith('--profile=')) return files;
  const profile = files[0].slice('--profile='.length) as keyof typeof postgresTestProfiles;
  const selected = postgresTestProfiles[profile];
  if (!selected) throw new Error(`Unknown PostgreSQL test profile: ${profile}`);
  return selected;
}

function expandTestFiles(files: readonly string[], root: string): string[] {
  const selected = profileFiles(files);
  const requested = selected.length > 0 ? selected : ['test/postgres-*.test.ts'];
  const expanded = requested.flatMap((file) => {
    if (!/[*?[{]/.test(file)) return [file];
    return [...new Bun.Glob(file).scanSync({ cwd: root, onlyFiles: true })];
  });
  if (expanded.length === 0) throw new Error('The PostgreSQL test command matched no test files');
  return [...new Set(expanded)].sort((left, right) => left.localeCompare(right));
}

export function postgresTestCommand(
  url: string | undefined,
  files: readonly string[],
  root = ROOT
): string[] {
  if (!url?.trim()) {
    throw new Error('BUNQUEUE_TEST_POSTGRES_URL is required for the PostgreSQL test command');
  }
  return [process.execPath, 'test', ...expandTestFiles(files, root)];
}

if (import.meta.main) {
  try {
    const command = postgresTestCommand(Bun.env.BUNQUEUE_TEST_POSTGRES_URL, Bun.argv.slice(2));
    const child = Bun.spawn(command, {
      cwd: ROOT,
      env: Bun.env,
      stdin: 'inherit',
      stdout: 'inherit',
      stderr: 'inherit',
    });
    process.exit(await child.exited);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}
