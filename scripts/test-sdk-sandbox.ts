#!/usr/bin/env bun
/** Run each official language SDK in its own disposable image. */

import { createWriteStream, mkdirSync } from 'node:fs';
import { finished } from 'node:stream/promises';
import { suitePassed, writeRunReport, writeSuiteArtifacts } from './test-sandbox-report';
import {
  createSuiteTelemetry,
  monitorContainer,
  previousReport,
  type SuiteTelemetry,
} from './test-sandbox-telemetry';

const ROOT = `${import.meta.dir}/..`;
const RUN_ID = new Date().toISOString().replaceAll(':', '-');
const LOG_ROOT = `${ROOT}/artifacts/test-sandbox-sdk`;
const LOG_DIR = `${LOG_ROOT}/${RUN_ID}`;
const MEMORY = Bun.env.BUNQUEUE_SDK_TEST_MEMORY ?? '4g';
const CPUS = Bun.env.BUNQUEUE_SDK_TEST_CPUS;
const SEQUENTIAL = Bun.env.BUNQUEUE_SDK_TEST_SEQUENTIAL === '1';
const suites = [
  {
    name: 'typescript',
    command: [
      'bash',
      '-c',
      "cd sdk/typescript && bun run build && bun run check && mkdir -p /tmp/typescript-package && bun pm pack --destination /tmp/typescript-package && bun tests/integration.ts && bun tests/e2e.ts && bun run test:workers && cd ../conformance && bun runner.ts --driver 'bun drivers/typescript.ts'",
    ],
  },
  {
    name: 'python',
    command: [
      'bash',
      '-c',
      "cd sdk/python && python -m compileall -q bunqueue tests && python -m build --no-isolation --outdir /tmp/python-package && python tests/test_integration.py && python tests/run_e2e.py && cd ../conformance && bun runner.ts --driver 'python drivers/python.py'",
    ],
  },
  {
    name: 'php',
    command: [
      'bash',
      '-c',
      "cd sdk/php && composer validate --strict --no-check-publish && find src tests -name '*.php' -print0 | xargs -0 -n1 php -l && php tests/run-e2e.php && cd ../conformance && bun runner.ts --driver 'php drivers/php.php'",
    ],
  },
  {
    name: 'go',
    command: [
      'bash',
      '-c',
      "cd sdk/go && test -z \"$(gofmt -l .)\" && go vet ./... && go list ./... && go test -v ./... && go test -race -run 'Hardening|Regression|Worker' ./... && cd ../conformance && bun runner.ts --driver './drivers/go-driver'",
    ],
  },
  {
    name: 'rust',
    command: [
      'bash',
      '-c',
      "cd sdk/rust && cargo fmt --check && cargo clippy --locked --offline --all-targets -- -D warnings && cargo test --locked --offline && cargo package --locked --offline --allow-dirty --no-verify && cd ../conformance && bun runner.ts --driver 'cargo run --locked --offline --quiet --manifest-path ../rust/Cargo.toml --example conformance-driver'",
    ],
  },
  {
    name: 'elixir',
    command: [
      'bash',
      '-c',
      "cd sdk/elixir && mix format --check-formatted && mix compile --warnings-as-errors && mix test --slowest 20 && mix hex.build && cd ../conformance && bun runner.ts --driver 'cd ../elixir && mix run ../conformance/drivers/elixir.exs'",
    ],
  },
] as const;
const active = new Set<string>();

async function relay(
  stream: ReadableStream<Uint8Array>,
  terminal: NodeJS.WriteStream,
  log: NodeJS.WritableStream
): Promise<void> {
  for await (const chunk of stream) {
    terminal.write(chunk);
    log.write(chunk);
  }
}

async function logged(command: string[], path: string): Promise<number> {
  const log = createWriteStream(path, { flags: 'w' });
  const proc = Bun.spawn(command, { cwd: ROOT, stdout: 'pipe', stderr: 'pipe' });
  await Promise.all([
    relay(proc.stdout, process.stdout, log),
    relay(proc.stderr, process.stderr, log),
  ]);
  const code = await proc.exited;
  log.end();
  await finished(log);
  return code;
}

function remove(name: string): void {
  Bun.spawnSync(['docker', 'rm', '--force', name], {
    cwd: ROOT,
    stdout: 'ignore',
    stderr: 'ignore',
  });
  active.delete(name);
}

function cleanup(): void {
  for (const name of active) remove(name);
}

function dockerAvailable(): boolean {
  const result = Bun.spawnSync(['docker', 'info', '--format', '{{.ServerVersion}}'], {
    cwd: ROOT,
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  if (result.exitCode === 0) return true;
  const message = result.stderr.toString().trim() || result.stdout.toString().trim();
  console.error(`Docker daemon is unavailable${message ? `: ${message}` : ''}`);
  return false;
}

function wasOomKilled(container: string): boolean {
  const result = Bun.spawnSync(
    ['docker', 'inspect', '--format', '{{.State.OOMKilled}}', container],
    { cwd: ROOT, stdin: 'ignore', stdout: 'pipe', stderr: 'ignore' }
  );
  return result.exitCode === 0 && result.stdout.toString().trim() === 'true';
}

process.on('SIGINT', () => {
  cleanup();
  process.exit(130);
});
process.on('SIGTERM', () => {
  cleanup();
  process.exit(130);
});

if (!dockerAvailable()) process.exit(1);
mkdirSync(LOG_DIR, { recursive: true });
console.log(`Complete SDK suite logs: ${LOG_DIR}`);
const started = performance.now();
for (const suite of suites) {
  const image = `bunqueue-sdk-test:${suite.name}`;
  console.log(`Building ${image}...`);
  const command = [
    'docker',
    'build',
    '--file',
    'Dockerfile.sdk-test',
    '--target',
    `${suite.name}-sdk`,
    '--tag',
    image,
    '.',
  ];
  const startedAt = new Date().toISOString();
  const logPath = `${LOG_DIR}/${suite.name}.build.log`;
  const code = await logged(command, logPath);
  if (code !== 0) {
    const telemetry = await createSuiteTelemetry({
      suite: `${suite.name}-build`,
      command,
      container: 'docker-build',
      logPath,
      startedAt,
      finishedAt: new Date().toISOString(),
      exitCode: code,
      oomKilled: false,
      samples: [],
    });
    await writeSuiteArtifacts(LOG_DIR, telemetry, []);
    const baselinePath = await previousReport(LOG_ROOT, RUN_ID);
    await writeRunReport({
      logDir: LOG_DIR,
      mode: SEQUENTIAL ? 'sequential' : 'parallel',
      durationMs: performance.now() - started,
      suites: [telemetry],
      baselinePath,
    });
    console.error(`SDK image build failed: ${suite.name}; see ${logPath}`);
    process.exit(code);
  }
}

async function runSuite(suite: (typeof suites)[number]): Promise<SuiteTelemetry> {
  const container = `bunqueue-sdk-${suite.name}-${process.pid}`.slice(0, 63);
  active.add(container);
  console.log(`Running SDK suite ${suite.name} in ${container}...`);
  const startedAt = new Date().toISOString();
  let complete = false;
  const monitor = monitorContainer(container, () => complete);
  const logPath = `${LOG_DIR}/${suite.name}.log`;
  const command = [
    'docker',
    'run',
    '--name',
    container,
    '--init',
    '--network',
    'none',
    '--cap-drop',
    'ALL',
    '--security-opt',
    'no-new-privileges:true',
    '--pids-limit',
    '2048',
    '--memory',
    MEMORY,
    ...(CPUS ? ['--cpus', CPUS] : []),
    '--stop-timeout',
    '10',
    '--tmpfs',
    '/tmp:rw,nosuid,nodev,exec,size=4g',
    '--env',
    'CI=1',
    '--env',
    'TZ=UTC',
    '--env',
    'TMPDIR=/tmp',
    '--env',
    'WRANGLER_SEND_METRICS=false',
    '--env',
    'CLOUDFLARE_CF_FETCH_ENABLED=false',
    `bunqueue-sdk-test:${suite.name}`,
    ...suite.command,
  ];
  const exitCode = await logged(command, logPath);
  complete = true;
  const samples = await monitor;
  const telemetry = await createSuiteTelemetry({
    suite: suite.name,
    command: suite.command,
    container,
    logPath,
    startedAt,
    finishedAt: new Date().toISOString(),
    exitCode,
    oomKilled: wasOomKilled(container),
    samples,
  });
  await writeSuiteArtifacts(LOG_DIR, telemetry, samples);
  // Exit code alone would delete the container of a suite that exited 0 having reported
  // nothing, which is exactly the container someone needs to open to find out why. The
  // aggregate verdict already refuses an empty result; the retention decision has to
  // agree with it or the evidence is gone by the time anyone reads the summary.
  if (suitePassed(telemetry)) {
    remove(container);
    console.log(`SDK suite passed: ${suite.name}`);
  } else {
    active.delete(container);
    const counted =
      telemetry.tests.passed + telemetry.tests.failed + telemetry.tests.skipped;
    const why = exitCode === 0 ? 'reported no results' : `exited ${exitCode}`;
    console.error(
      `SDK suite ${suite.name} failed (${why}, ${counted} tests counted); retained container: ${container}`
    );
  }
  return telemetry;
}

const results: SuiteTelemetry[] = [];
if (SEQUENTIAL) {
  for (const suite of suites) results.push(await runSuite(suite));
} else {
  results.push(...(await Promise.all(suites.map(runSuite))));
}
const baselinePath = await previousReport(LOG_ROOT, RUN_ID);
const report = await writeRunReport({
  logDir: LOG_DIR,
  mode: SEQUENTIAL ? 'sequential' : 'parallel',
  durationMs: performance.now() - started,
  suites: results,
  baselinePath,
});
console.log(`SDK telemetry: ${LOG_DIR}/summary.md`);
if (!report.passed) process.exit(1);
console.log(`All SDK sandbox suites passed in ${(report.durationMs / 1000).toFixed(1)}s.`);
