import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const sdkWorkflowPath = join(import.meta.dir, '../.github/workflows/sdk.yml');
const workflow = readFileSync(sdkWorkflowPath, 'utf8');
const securityWorkflow = readFileSync(
  join(import.meta.dir, '../.github/workflows/sdk-security.yml'),
  'utf8'
);
const typescriptHarnesses = [
  '../sdk/typescript/tests/harness.ts',
  '../sdk/typescript/tests/e2e-edge.ts',
  '../sdk/typescript/tests/workers/run.mjs',
].map((path) => readFileSync(join(import.meta.dir, path), 'utf8'));

test('Go protocol conformance selects the nested driver module', () => {
  expect(workflow).toContain('bun runner.ts --driver "go -C drivers/go run ."');
  expect(workflow).not.toContain('go run ./drivers/go');
});

test('scheduled SDK soaks raise the protocol request budget', () => {
  expect(workflow).toContain(
    "RATE_LIMIT_MAX_REQUESTS: ${{ github.event_name == 'schedule' && '1000000' || '10000' }}"
  );
});

test('weekly dependency advisories remain scheduled after the workflow split', () => {
  expect(securityWorkflow).toContain("cron: '0 2 * * 0'");
  expect(securityWorkflow).toContain('name: Weekly SDK dependency advisories');
});

test('TypeScript SDK harnesses let the OS allocate independent HTTP ports', () => {
  for (const harness of typescriptHarnesses) {
    expect(harness).toContain("HTTP_PORT: '0'");
    expect(harness).not.toContain('HTTP_PORT: String(port + 1)');
  }
});
