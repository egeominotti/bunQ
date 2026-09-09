import { describe, expect, test } from 'bun:test';

const root = `${import.meta.dir}/..`;
const sdkNames = ['typescript', 'python', 'php', 'go', 'rust', 'elixir'] as const;
const qualityJobs = [
  'lint',
  'typecheck',
  'test',
  'test-tcp',
  'test-embedded',
  'test-websocket',
  'docs',
  'sdk',
] as const;

type Job = {
  strategy?: { matrix?: { include?: Array<{ target: string; artifact: string }> } };
  if?: string;
  needs?: string | string[];
  'runs-on'?: string;
  steps?: Array<{ name?: string; run?: string; uses?: string; with?: Record<string, unknown> }>;
  uses?: string;
};

type Workflow = {
  concurrency?: { group?: string };
  jobs: Record<string, Job>;
  on: Record<string, unknown>;
};

function dependencies(job: Job | undefined): string[] {
  if (!job?.needs) return [];
  return Array.isArray(job.needs) ? job.needs : [job.needs];
}

function jobBlock(workflow: string, name: string): string {
  const marker = `  ${name}:\n`;
  const start = workflow.indexOf(marker);
  if (start === -1) throw new Error(`missing workflow job: ${name}`);
  const remainder = workflow.slice(start + marker.length);
  const next = remainder.search(/^ {2}[\w-]+:/m);
  return workflow.slice(start, next === -1 ? undefined : start + marker.length + next);
}

function ancestors(workflow: Workflow, jobName: string, seen = new Set<string>()): Set<string> {
  for (const dependency of dependencies(workflow.jobs[jobName])) {
    if (seen.has(dependency)) continue;
    seen.add(dependency);
    ancestors(workflow, dependency, seen);
  }
  return seen;
}

function releaseGraphViolations(ci: Workflow, sdk: Workflow): string[] {
  const violations: string[] = [];
  const sdkGateNeeds = dependencies(sdk.jobs['sdk-gate']);
  for (const name of sdkNames) {
    if (!sdk.jobs[name]) violations.push(`missing SDK job ${name}`);
    if (!sdkGateNeeds.includes(name)) violations.push(`sdk-gate does not need ${name}`);
  }

  if (ci.jobs.sdk?.uses !== './.github/workflows/sdk.yml') {
    violations.push('CI does not call the reusable SDK workflow');
  }
  const qualityNeeds = dependencies(ci.jobs['quality-gate']);
  for (const name of qualityJobs) {
    if (!qualityNeeds.includes(name)) violations.push(`quality-gate does not need ${name}`);
  }
  for (const sink of ['version-gate', 'build', 'docker', 'release']) {
    const upstream = ancestors(ci, sink);
    if (!upstream.has('quality-gate')) violations.push(`${sink} can bypass quality-gate`);
    if (!upstream.has('sdk')) violations.push(`${sink} can bypass the SDK workflow`);
  }
  if (!dependencies(ci.jobs.docker).includes('build')) {
    violations.push('Docker publishing can bypass the binary build');
  }
  for (const [job, required] of [
    ['version-gate', ['quality-gate']],
    ['build', ['version-gate']],
    ['docker', ['version-gate', 'build']],
    ['release', ['version-gate', 'build', 'docker']],
  ] as const) {
    const jobNeeds = dependencies(ci.jobs[job]);
    for (const dependency of required) {
      if (!jobNeeds.includes(dependency)) violations.push(`${job} does not need ${dependency}`);
    }
  }
  return violations;
}

const [ciText, sdkText, releaseText, sandboxText, packageText] = await Promise.all([
  Bun.file(`${root}/.github/workflows/ci.yml`).text(),
  Bun.file(`${root}/.github/workflows/sdk.yml`).text(),
  Bun.file(`${root}/.github/workflows/sdk-release.yml`).text(),
  Bun.file(`${root}/scripts/test-sandbox.ts`).text(),
  Bun.file(`${root}/package.json`).text(),
]);
const ci = Bun.YAML.parse(ciText) as Workflow;
const sdk = Bun.YAML.parse(sdkText) as Workflow;
const sdkRelease = Bun.YAML.parse(releaseText) as Workflow;
const packageManifest = JSON.parse(packageText) as { scripts: Record<string, string> };

describe('release graph SDK gate', () => {
  test('the SDK workflow is reusable and has no independent push race', () => {
    expect(sdk.on.workflow_call).toBeDefined();
    expect(sdk.on.schedule).toBeDefined();
    expect(sdk.on.workflow_dispatch).toBeDefined();
    expect(sdk.on.push).toBeUndefined();
    expect(sdk.on.pull_request).toBeUndefined();
  });

  test('all six SDK results converge on an explicit fail-closed gate', () => {
    expect(dependencies(sdk.jobs['sdk-gate'])).toEqual([...sdkNames]);
    expect(sdk.jobs['sdk-gate']?.if).toBe('${{ always() }}');
    const gate = jobBlock(sdkText, 'sdk-gate');
    for (const name of sdkNames) {
      expect(gate, name).toContain(`needs.${name}.result`);
    }
  });

  test('all release-producing jobs are transitively closed by SDK and quality gates', () => {
    expect(releaseGraphViolations(ci, sdk)).toEqual([]);
    expect(dependencies(ci.jobs['version-gate'])).toEqual(['quality-gate']);
    expect(ci.jobs['quality-gate']?.if).toBe('${{ always() }}');
    expect(ciText).toContain('github.com/rhysd/actionlint/cmd/actionlint@v1.7.12');
    const gate = jobBlock(ciText, 'quality-gate');
    for (const name of qualityJobs) {
      expect(gate, name).toContain(`needs.${name}.result`);
    }
  });

  test('workflow command-file paths are quoted for ShellCheck', () => {
    expect(ciText).not.toMatch(/>>\s+\$GITHUB_(?:ENV|OUTPUT|PATH|STEP_SUMMARY)\b/);
  });

  test('local, sandbox, and CI unit gates use four isolated Bun workers', () => {
    expect(packageManifest.scripts.test).toBe('BUNQUEUE_EMBEDDED=1 bun test --parallel=4');
    expect(sandboxText).toContain("{ name: 'unit', command: ['bun', 'test', '--parallel=4'] }");
    expect(ci.jobs.test?.steps?.find((step) => step.name === 'Run unit tests')?.run).toBe(
      'bun test --parallel=4'
    );
  });

  test('Docker publication exposes both the release version and latest tags', () => {
    const metadata = ci.jobs.docker?.steps?.find(
      (step) => step.uses === 'docker/metadata-action@v5'
    );
    const tags = String(metadata?.with?.tags ?? '');

    expect(tags).toContain('type=raw,value=${{ needs.version-gate.outputs.version }}');
    expect(tags).toContain('type=raw,value=latest');
  });

  test('Docker publication authenticates and tags both registries', () => {
    const steps = ci.jobs.docker?.steps ?? [];
    const hubLogin = steps.find((step) => step.with?.registry === 'docker.io');
    expect(hubLogin?.uses).toBe('docker/login-action@v3');
    expect(hubLogin?.with?.username).toBe('${{ secrets.DOCKERHUB_USERNAME }}');
    expect(hubLogin?.with?.password).toBe('${{ secrets.DOCKERHUB_TOKEN }}');
    expect(steps.find((step) => step.name === 'Log in to Container Registry')?.with?.password).toBe(
      '${{ secrets.GITHUB_TOKEN }}'
    );
    const metadata = steps.find((step) => step.uses === 'docker/metadata-action@v5');
    expect(String(metadata?.with?.images).trim().split('\n')).toEqual([
      '${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}',
      'docker.io/egeominotti/bunqueue',
    ]);
    const publish = steps.find((step) => step.uses === 'docker/build-push-action@v6');
    expect(publish?.with?.platforms).toBe('linux/amd64,linux/arm64');
    expect(publish?.with?.tags).toBe('${{ steps.meta.outputs.tags }}');
    expect(publish?.with?.push).toBe(true);
    expect(steps.indexOf(hubLogin!)).toBeLessThan(steps.indexOf(publish!));
  });

  test('all eight executable targets have matching published archives', () => {
    const targets = [
      'linux-x64',
      'linux-arm64',
      'linux-x64-musl',
      'linux-arm64-musl',
      'darwin-x64',
      'darwin-arm64',
      'windows-x64',
      'windows-arm64',
    ];
    const matrix = ci.jobs.build?.strategy?.matrix?.include ?? [];
    expect(matrix.map((entry) => entry.target).sort()).toEqual(
      targets.map((target) => `bun-${target}`).sort()
    );
    const release = ci.jobs.release?.steps?.find(
      (step) => step.uses === 'softprops/action-gh-release@v2'
    );
    expect(release?.with?.fail_on_unmatched_files).toBe(true);
    const files = String(release?.with?.files).trim().split('\n');
    expect(files).toHaveLength(9);
    for (const target of targets) {
      const windows = target.startsWith('windows-');
      expect(matrix.find((entry) => entry.target === `bun-${target}`)?.artifact).toBe(
        `bunqueue-${target}${windows ? '.exe' : ''}`
      );
      expect(files).toContain(`release/bunqueue-${target}${windows ? '.zip' : '.tar.gz'}`);
    }
    expect(files).toContain('release/SHA256SUMS');
  });

  test('finite edge mutations are all detected', () => {
    for (const name of sdkNames) {
      const mutant = structuredClone(sdk);
      mutant.jobs['sdk-gate'].needs = dependencies(mutant.jobs['sdk-gate']).filter(
        (dependency) => dependency !== name
      );
      expect(releaseGraphViolations(ci, mutant)).toContain(`sdk-gate does not need ${name}`);
    }

    for (const name of qualityJobs) {
      const mutant = structuredClone(ci);
      mutant.jobs['quality-gate'].needs = dependencies(mutant.jobs['quality-gate']).filter(
        (dependency) => dependency !== name
      );
      expect(releaseGraphViolations(mutant, sdk)).not.toEqual([]);
    }

    for (const [job, dependency] of [
      ['version-gate', 'quality-gate'],
      ['build', 'version-gate'],
      ['docker', 'build'],
      ['release', 'docker'],
    ] as const) {
      const mutant = structuredClone(ci);
      mutant.jobs[job].needs = dependencies(mutant.jobs[job]).filter(
        (candidate) => candidate !== dependency
      );
      expect(releaseGraphViolations(mutant, sdk), `${job} without ${dependency}`).not.toEqual([]);
    }
  });

  test('the npm SDK release is manual, tested by all SDKs, pinned, and uses Bun', () => {
    expect(sdkRelease.on.workflow_dispatch).toBeDefined();
    expect(sdkRelease.on.push).toBeUndefined();
    expect(sdkRelease.jobs.sdk?.uses).toBe('./.github/workflows/sdk.yml');
    expect(dependencies(sdkRelease.jobs.publish)).toEqual(['sdk']);
    expect(releaseText).toContain('bun-version: 1.4.2');
    expect(releaseText).toContain('bun install --frozen-lockfile');
    expect(releaseText).not.toContain('|| bun install');
    expect(releaseText).toContain('test "$GITHUB_REF" = refs/heads/main');
    expect(releaseText).toContain('git rev-parse origin/main');
    expect(releaseText).toContain('bun publish --provenance --access public');
    expect(releaseText).not.toContain('npm publish');
  });
});
