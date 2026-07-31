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

/**
 * The soak steps are gated on `RUN_SOAK` rather than on `schedule` directly, so a
 * fix to them can be exercised through `workflow_dispatch` instead of waiting for
 * Sunday. Both env values must then derive from the SAME condition: a soak run
 * that kept the push-level budget would measure the broker's anti-abuse throttle
 * instead of the client, which is how it failed the first time.
 */
const parsedWorkflow = Bun.YAML.parse(workflow) as {
  env: Record<string, string>;
  jobs: Record<
    string,
    { 'timeout-minutes'?: number; steps: { name?: string; if?: string; run?: string }[] }
  >;
};
const soakSteps = Object.entries(parsedWorkflow.jobs).flatMap(([job, { steps }]) =>
  steps.filter((step) => /soak|fuzz/i.test(step.name ?? '')).map((step) => ({ job, step }))
);

test('scheduled SDK soaks raise the protocol request budget', () => {
  const condition = "github.event_name == 'schedule' || inputs.run_soak";
  expect(parsedWorkflow.env.RUN_SOAK).toBe(`\${{ ${condition} }}`);
  expect(parsedWorkflow.env.RATE_LIMIT_MAX_REQUESTS).toBe(
    `\${{ (${condition}) && '1000000' || '10000' }}`
  );
});

test('a reusable caller can enable the same soak profile', () => {
  expect(parsedWorkflow.env.RUN_SOAK).toContain('inputs.run_soak');
  expect(parsedWorkflow.env.RUN_SOAK).not.toContain("github.event_name == 'workflow_dispatch'");
});

test('every soak and fuzz step is gated on the same RUN_SOAK flag', () => {
  expect(soakSteps.length).toBe(7); // six SDK soaks + Go native fuzzing
  for (const { job, step } of soakSteps) {
    expect(step.if, `${job}/${step.name}`).toContain("env.RUN_SOAK == 'true'");
  }
});

/**
 * `go test` panics at 10m and ExUnit kills a test at 60s, both far below the soak
 * profile: those two runners must derive an explicit bound from the soak duration,
 * and must refuse to run at all if the variable is unset or non-numeric (under
 * GitHub's default `bash -e` the arithmetic would otherwise silently yield a
 * 300-second bound, tighter than the default it replaces).
 */
test('the Go and Elixir soaks bound themselves from the soak duration', () => {
  const commands = Object.fromEntries(
    soakSteps
      .filter(({ job }) => job === 'go' || job === 'elixir')
      .map(({ job, step }) => [`${job}:${step.name}`, step.run ?? ''])
  );
  const soakCommands = Object.entries(commands).filter(([, run]) =>
    /go test -run '\^TestSDKSoak\$'|mix test --include soak/.test(run)
  );
  expect(soakCommands.length).toBe(2);
  for (const [label, run] of soakCommands) {
    expect(run, label).toContain('${BUNQUEUE_SDK_SOAK_SECONDS:?');
    expect(run, label).toContain('*[!0-9]*');
    expect(run, label).toMatch(/BUNQUEUE_SDK_SOAK_SECONDS \+ 300/);
  }
});

test('every SDK job bounds its own runtime instead of inheriting 360 minutes', () => {
  for (const [job, { 'timeout-minutes': bound }] of Object.entries(parsedWorkflow.jobs)) {
    expect(bound, job).toBeGreaterThanOrEqual(30);
    expect(bound, job).toBeLessThanOrEqual(60);
  }
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
