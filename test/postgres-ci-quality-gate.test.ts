import { expect, test } from 'bun:test';
import { postgresTestCommand, postgresTestProfiles } from '../scripts/test-postgres';

interface QualityGateJob {
  needs?: string[];
  steps?: Array<{ env?: Record<string, string>; run?: string }>;
}

test('the CI quality gate requires every PostgreSQL compatibility job', async () => {
  const workflow = Bun.YAML.parse(await Bun.file('.github/workflows/ci.yml').text()) as {
    jobs?: Record<string, QualityGateJob>;
  };
  const gate = workflow.jobs?.['quality-gate'];
  const step = gate?.steps?.[0];

  expect(gate?.needs).toContain('test-postgres');
  expect(step?.env?.POSTGRES_RESULT).toBe('${{ needs.test-postgres.result }}');
  expect(step?.run).toContain('"$POSTGRES_RESULT"');
});

test('the dedicated PostgreSQL commands cannot pass by skipping an absent database', async () => {
  const packageJson = (await Bun.file('package.json').json()) as {
    scripts: Record<string, string>;
  };
  expect(() => postgresTestCommand(undefined, [])).toThrow(
    'BUNQUEUE_TEST_POSTGRES_URL is required'
  );
  expect(() => postgresTestCommand('  ', [])).toThrow('BUNQUEUE_TEST_POSTGRES_URL is required');
  const command = postgresTestCommand('postgres://configured', []);
  expect(command.slice(0, 2)).toEqual([process.execPath, 'test']);
  expect(command.slice(2)).toContain('test/postgres-connection-recovery.test.ts');
  expect(command.slice(2).every((file) => !file.includes('*'))).toBe(true);
  expect(postgresTestCommand('postgres://configured', ['--profile=smoke']).slice(2)).toEqual(
    [...postgresTestProfiles.smoke].sort((left, right) => left.localeCompare(right))
  );
  expect(postgresTestProfiles.destruction).toContain('test/postgres-connection-recovery.test.ts');
  expect(postgresTestProfiles.destruction).toContain(
    'test/postgres-core-transaction-retry.test.ts'
  );
  expect(postgresTestProfiles.destruction).toContain('test/postgres-process-diagnostics.test.ts');
  expect(postgresTestProfiles.pressure).toContain('test/postgres-ackb-lock-timeout.test.ts');
  expect(postgresTestProfiles.pressure).toContain('test/postgres-advisory-lock-collisions.test.ts');
  expect(postgresTestProfiles.pressure).toContain('test/postgres-flow-admission-batching.test.ts');
  expect(postgresTestProfiles.pressure).toContain('test/postgres-process-diagnostics.test.ts');
  expect(postgresTestProfiles.pressure).toContain('test/postgres-ten-processes.test.ts');
  expect(() => postgresTestCommand('postgres://configured', ['--profile=unknown'])).toThrow(
    'Unknown PostgreSQL test profile'
  );
  expect(packageJson.scripts['test:postgres']).toBe('bun scripts/test-postgres.ts');
  expect(packageJson.scripts['test:postgres:pressure']).toContain(
    'BUNQUEUE_POSTGRES_TEN_BROKER_SOAK=1'
  );
  expect(packageJson.scripts['test:postgres:battle']).toContain('BUNQUEUE_POSTGRES_FC_RUNS=100');
  expect(packageJson.scripts['test:postgres:fast-check']).toContain('scripts/test-postgres.ts');
  expect(packageJson.scripts['test:postgres:ten-broker']).toContain('scripts/test-postgres.ts');
});
