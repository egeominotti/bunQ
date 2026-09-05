import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

interface Step {
  run?: string;
  if?: string;
  'working-directory'?: string;
}

function steps(file: string, job: string): Step[] {
  const workflow = Bun.YAML.parse(
    readFileSync(resolve(import.meta.dir, '../.github/workflows', file), 'utf8')
  ) as {
    jobs: Record<string, { steps: Step[] }>;
  };
  return workflow.jobs[job].steps;
}

test('native CI builds ignored portable artifacts before unit tests', () => {
  const pipeline = steps('ci.yml', 'test');
  const unit = pipeline.findIndex((step) => /bun test\s/.test(step.run ?? ''));
  expect(unit).toBeGreaterThanOrEqual(0);
  const build = pipeline.findIndex((step) => step.run?.includes('bun run build:client'));
  expect(build).toBeGreaterThanOrEqual(0);
  expect(build).toBeLessThan(unit);
  expect(pipeline[build].if).toBeUndefined();
  expect(pipeline[build]['working-directory'] ?? '.').toBe('.');
  const install = pipeline.findIndex((step) => step.run?.includes('bun install --frozen-lockfile'));
  expect(install).toBeGreaterThanOrEqual(0);
  expect(install).toBeLessThan(build);
});

test('SDK publishing installs root build dependencies before SDK compilation', () => {
  const pipeline = steps('sdk-release.yml', 'publish');
  const build = pipeline.findIndex(
    (step) => step['working-directory'] === 'sdk/typescript' && step.run?.includes('bun run build')
  );
  expect(build).toBeGreaterThanOrEqual(0);
  const install = pipeline.findIndex(
    (step) =>
      (step['working-directory'] ?? '.') === '.' &&
      step.run?.includes('bun install --frozen-lockfile')
  );
  expect(install).toBeGreaterThanOrEqual(0);
  expect(install).toBeLessThan(build);
  expect(pipeline[install].if).toBeUndefined();
  expect(pipeline[install].run).toContain('--ignore-scripts');
});
