import { expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

type Job = {
  if?: string;
  needs?: string[];
  permissions?: Record<string, string>;
  steps?: Array<{ name?: string; uses?: string; run?: string; env?: Record<string, string> }>;
};
const text = await Bun.file(`${import.meta.dir}/../.github/workflows/ci.yml`).text();
const workflow = Bun.YAML.parse(text) as {
  on: { workflow_dispatch: { inputs: Record<string, { default: unknown }> } };
  jobs: Record<string, Job>;
};

test('root npm publication requires an explicit version, main, and successful product gates', () => {
  expect(workflow.on.workflow_dispatch.inputs.npm_version.default).toBe('');
  const npm = workflow.jobs.npm;
  expect(npm.if).toContain("github.event_name == 'workflow_dispatch'");
  expect(npm.if).toContain("inputs.npm_version != ''");
  expect(npm.if).toContain("github.ref == 'refs/heads/main'");
  expect(npm.if).toContain("needs.docker.result == 'success'");
  const seen = new Set<string>();
  function visit(name: string) {
    for (const dependency of workflow.jobs[name].needs ?? []) {
      if (seen.has(dependency)) continue;
      seen.add(dependency);
      visit(dependency);
    }
  }
  visit('npm');
  for (const gate of ['quality-gate', 'sdk', 'build', 'docker-test', 'docker']) {
    expect(seen.has(gate)).toBe(true);
  }
});

test('npm publishes the saved verified tarball using supported Bun flags', () => {
  const npm = workflow.jobs.npm;
  const steps = npm.steps ?? [];
  const verify = steps.findIndex((step) => step.name?.startsWith('Verify the explicitly'));
  const build = steps.findIndex(
    (step) => step.name === 'Build and verify the package consumer contract'
  );
  const publish = steps.findIndex((step) => step.name?.startsWith('Publish the verified tarball'));
  expect(verify).toBeGreaterThan(-1);
  expect(build).toBeGreaterThan(verify);
  expect(publish).toBeGreaterThan(build);
  expect(steps[verify].run).toContain('response.status !== 404');
  expect(steps[build].run).toContain('bun test test/package-consumer-smoke.test.ts');
  const dryRun = steps.find(
    (step) => step.name === 'Verify npm authentication and publication dry run'
  );
  expect(dryRun?.run).toContain('bun publish --dry-run');
  expect(dryRun?.env?.NPM_CONFIG_TOKEN).toBe('${{ secrets.NPM_TOKEN }}');
  expect(steps[build].env?.NPM_CONFIG_TOKEN).toBeUndefined();
  expect(steps[publish].run).toBe(
    'bun publish --access public "/tmp/npm-package/bunqueue-$REQUESTED_VERSION.tgz"'
  );
  expect(steps[publish].env?.NPM_CONFIG_TOKEN).toBe('${{ secrets.NPM_TOKEN }}');
  expect(npm.permissions?.['id-token']).toBeUndefined();
  expect(text).not.toContain('npm publish');
});

test('the configured credential variable authenticates the pinned Bun CLI', async () => {
  const step = workflow.jobs.npm.steps?.find(
    (item) => item.name === 'Verify npm authentication and publication dry run'
  );
  expect(step?.env?.NPM_CONFIG_TOKEN).toBe('${{ secrets.NPM_TOKEN }}');
  const directory = await mkdtemp(join(tmpdir(), 'bunqueue-npm-auth-'));
  const server = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    fetch(request) {
      return request.headers.get('authorization') === 'Bearer synthetic-test-token'
        ? Response.json({ username: 'test-publisher' })
        : new Response('Unauthorized', { status: 401 });
    },
  });
  try {
    await Bun.write(join(directory, 'package.json'), '{}');
    const child = Bun.spawn(
      [process.execPath, 'pm', 'whoami', '--registry', `http://127.0.0.1:${server.port}`],
      {
        cwd: directory,
        stdout: 'pipe',
        stderr: 'pipe',
        env: { PATH: process.env.PATH, NPM_CONFIG_TOKEN: 'synthetic-test-token' },
      }
    );
    const timeout = setTimeout(() => child.kill(), 5000);
    try {
      const [code, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      expect({ code, stdout, stderr }).toEqual({ code: 0, stdout: 'test-publisher\n', stderr: '' });
    } finally {
      clearTimeout(timeout);
      if (child.exitCode === null) child.kill();
    }
  } finally {
    server.stop(true);
    await rm(directory, { recursive: true, force: true });
  }
}, 10_000);
