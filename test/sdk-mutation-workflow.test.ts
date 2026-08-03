import { expect, test } from 'bun:test';

const root = `${import.meta.dir}/..`;
const path = `${root}/.github/workflows/sdk-mutation.yml`;
// StrykerJS was removed, so the TypeScript SDK has no mutation job: its
// dependency graph was the only source of the weekly advisory failures and
// none of it ever reached the published client. The planners keep their
// generated-property coverage in `bun run test:property`.
const sdkNames = ['python', 'php', 'go', 'rust', 'elixir'] as const;

type Workflow = {
  jobs: Record<
    string,
    {
      'timeout-minutes'?: number;
      steps?: Array<{ name?: string; run?: string; uses?: string }>;
    }
  >;
  on: Record<string, unknown>;
};

test('scheduled SDK mutation campaigns cover every official client with pinned engines', async () => {
  expect(await Bun.file(path).exists()).toBe(true);
  const text = await Bun.file(path).text();
  const workflow = Bun.YAML.parse(text) as Workflow;
  const [
    typescriptPackage,
    pythonProject,
    phpMutation,
    goMutation,
    rustMutation,
    elixirProject,
    sandboxDockerfile,
  ] = await Promise.all([
    Bun.file(`${root}/sdk/typescript/package.json`).text(),
    Bun.file(`${root}/sdk/python/pyproject.toml`).text(),
    Bun.file(`${root}/sdk/php/infection.json5`).text(),
    Bun.file(`${root}/sdk/go/.gremlins.yaml`).text(),
    Bun.file(`${root}/sdk/rust/.cargo/mutants.toml`).text(),
    Bun.file(`${root}/sdk/elixir/mix.exs`).text(),
    Bun.file(`${root}/Dockerfile.test`).text(),
  ]);

  expect(workflow.on.schedule).toBeDefined();
  expect(workflow.on.workflow_dispatch).toBeDefined();
  expect(workflow.on.push).toBeUndefined();
  expect(workflow.on.pull_request).toBeUndefined();
  for (const sdk of sdkNames) {
    const job = workflow.jobs[sdk];
    expect(job, sdk).toBeDefined();
    expect(job?.['timeout-minutes'], sdk).toBeGreaterThanOrEqual(15);
    expect(job?.['timeout-minutes'], sdk).toBeLessThanOrEqual(120);
    expect(
      job?.steps?.some((step) => /mutat/i.test(step.name ?? '')),
      sdk
    ).toBe(true);
  }

  // Stryker must stay gone: it is the regression this assertion exists for.
  expect(typescriptPackage).not.toContain('stryker');
  expect(typescriptPackage).not.toContain('test:mutation');
  expect(workflow.jobs.typescript).toBeUndefined();
  expect(await Bun.file(`${root}/sdk/typescript/stryker.config.mjs`).exists()).toBe(false);
  expect(pythonProject).toContain('"mutmut==3.6.0;');
  expect(pythonProject).toContain('"bunqueue/flow_commit.py"');
  expect(text).toContain('score < 97.0');
  expect(phpMutation).toContain('"src/Flow"');
  expect(await Bun.file(`${root}/sdk/php/src/Flow/SnapshotValidator.php`).exists()).toBe(true);
  expect(goMutation).not.toContain('flow_snapshots');
  expect(rustMutation).toContain('"src/flow_commit.rs"');
  expect(elixirProject).toContain('{:muex, "== 0.8.1"');
  expect(elixirProject).toContain('lib/bunqueue/flow_snapshots.ex');
  expect(elixirProject).toContain('--fail-at 100');
  for (const marker of [
    'infection/releases/download/0.34.1',
    '--min-msi=99',
    '--min-covered-msi=99',
    'gremlins@v0.6.0',
    'cargo-mutants --version 26.0.0',
  ]) {
    expect(text, marker).toContain(marker);
  }
  for (const config of [
    'sdk/typescript/package.json',
    'sdk/python/pyproject.toml',
    'sdk/php/infection.json5',
    'sdk/php/src/Flow/SnapshotValidator.php',
    'sdk/go/.gremlins.yaml',
    'sdk/rust/.cargo/mutants.toml',
    'sdk/elixir/mix.exs',
  ]) {
    expect(sandboxDockerfile, config).toContain(config);
  }
});
