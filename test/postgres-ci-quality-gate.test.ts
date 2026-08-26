import { expect, test } from 'bun:test';

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
