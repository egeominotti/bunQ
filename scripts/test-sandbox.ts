#!/usr/bin/env bun
/** Build one test image, then run each required suite in a fresh container. */

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
const IMAGE = Bun.env.BUNQUEUE_TEST_IMAGE ?? 'bunqueue-test:local';
const MEMORY = Bun.env.BUNQUEUE_TEST_MEMORY ?? '4g';
const CPUS = Bun.env.BUNQUEUE_TEST_CPUS;
const SEQUENTIAL = Bun.env.BUNQUEUE_TEST_SEQUENTIAL === '1';
const RUN_ID = new Date().toISOString().replaceAll(':', '-');
const LOG_ROOT = `${ROOT}/artifacts/test-sandbox`;
const LOG_DIR = `${LOG_ROOT}/${RUN_ID}`;

const suites = [
  { name: 'unit', command: ['bun', 'test'] },
  { name: 'tcp', command: ['bun', 'scripts/tcp/run-all-tests.ts'] },
  { name: 'embedded', command: ['bun', 'scripts/embedded/run-all-tests.ts'] },
] as const;

const activeContainers = new Set<string>();
let interrupted = false;

function run(command: string[], cwd = ROOT): number {
  const proc = Bun.spawnSync(command, {
    cwd,
    stdin: 'inherit',
    stdout: 'inherit',
    stderr: 'inherit',
  });
  return proc.exitCode;
}

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

async function runLogged(command: string[], logPath: string): Promise<number> {
  const log = createWriteStream(logPath, { flags: 'w' });
  const proc = Bun.spawn(command, {
    cwd: ROOT,
    stdin: 'inherit',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  await Promise.all([
    relay(proc.stdout, process.stdout, log),
    relay(proc.stderr, process.stderr, log),
  ]);
  const exitCode = await proc.exited;
  log.end();
  await finished(log);
  return exitCode;
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

function removeContainer(container: string): void {
  Bun.spawnSync(['docker', 'rm', '--force', container], {
    cwd: ROOT,
    stdin: 'ignore',
    stdout: 'ignore',
    stderr: 'ignore',
  });
  activeContainers.delete(container);
}

function removeActiveContainers(): void {
  for (const container of activeContainers) removeContainer(container);
}

function handleSignal(signal: string): void {
  if (interrupted) return;
  interrupted = true;
  console.error(`\nReceived ${signal}; removing active test containers...`);
  removeActiveContainers();
  process.exit(130);
}

process.on('SIGINT', () => handleSignal('SIGINT'));
process.on('SIGTERM', () => handleSignal('SIGTERM'));

if (!dockerAvailable()) process.exit(1);

mkdirSync(LOG_DIR, { recursive: true });
console.log(`Complete suite logs: ${LOG_DIR}`);
console.log(`Building ${IMAGE} with the current worktree...`);
const buildCode = run(['docker', 'build', '--file', 'Dockerfile.test', '--tag', IMAGE, '.']);
if (buildCode !== 0) process.exit(buildCode);

function wasOomKilled(container: string): boolean {
  const result = Bun.spawnSync(
    ['docker', 'inspect', '--format', '{{.State.OOMKilled}}', container],
    { cwd: ROOT, stdin: 'ignore', stdout: 'pipe', stderr: 'ignore' }
  );
  return result.exitCode === 0 && result.stdout.toString().trim() === 'true';
}

async function runSuite(suite: (typeof suites)[number]): Promise<SuiteTelemetry> {
  const safeImage = IMAGE.replace(/[^a-zA-Z0-9_.-]/g, '-').toLowerCase();
  const container = `${safeImage}-${suite.name}-${process.pid}`.slice(0, 63);
  activeContainers.add(container);
  console.log(`Running ${suite.name} in ${container}...`);
  const resourceArgs = [
    '--pids-limit',
    '2048',
    '--memory',
    MEMORY,
    ...(CPUS ? ['--cpus', CPUS] : []),
  ];
  const startedAt = new Date().toISOString();
  let complete = false;
  const monitor = monitorContainer(container, () => complete);
  const code = await runLogged(
    [
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
      ...resourceArgs,
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
      IMAGE,
      ...suite.command,
    ],
    `${LOG_DIR}/${suite.name}.log`
  );
  complete = true;
  const samples = await monitor;
  const telemetry = await createSuiteTelemetry({
    suite: suite.name,
    command: suite.command,
    container,
    logPath: `${LOG_DIR}/${suite.name}.log`,
    startedAt,
    finishedAt: new Date().toISOString(),
    exitCode: code,
    oomKilled: wasOomKilled(container),
    samples,
  });
  await writeSuiteArtifacts(LOG_DIR, telemetry, samples);
  // A suite that exits 0 having produced NO parseable result is not a pass, it is a
  // gate that ran nothing and said nothing. Seen for real: a unit run stopped 11 files
  // early, printed no summary, exited 0, and was reported `PASS | 0 / 0 / 0` while five
  // model-based suites and six workflow suites had never executed. Status was decided
  // on the exit code alone, so the counts beside it were decoration. Treat an empty
  // result as a failure, loudly.
  const counted =
    telemetry.tests.passed + telemetry.tests.failed + telemetry.tests.skipped;
  // One predicate, shared with the markdown verdict, `summary.json`'s `passed` field, the
  // baseline gate and the SDK gate. They were four separate expressions, and the one on
  // `summary.json` disagreed: it reported `passed: true` for a 0/0/0 run, which is the
  // artifact the handoff process is told to read.
  const emptyResult = code === 0 && !suitePassed(telemetry);
  if (emptyResult) {
    telemetry.anomalies.push(
      `no test results parsed: the suite exited 0 with ${counted} tests counted`
    );
  }

  if (code !== 0 || emptyResult) {
    activeContainers.delete(container);
    console.error(
      emptyResult
        ? `\nSandbox suite reported NO tests: ${suite.name} exited 0 with an empty result.`
        : `\nSandbox suite failed: ${suite.name} exited with ${code}.`
    );
    console.error(`Complete log: ${LOG_DIR}/${suite.name}.log`);
    console.error(`Failed container retained: ${container}`);
    console.error(`Inspect it with: docker logs ${container}`);
  } else {
    removeContainer(container);
    console.log(`Suite passed: ${suite.name}`);
  }
  return telemetry;
}

const startedAt = performance.now();
console.log(`Running suites ${SEQUENTIAL ? 'sequentially' : 'in parallel'}...`);
const results: SuiteTelemetry[] = [];
if (SEQUENTIAL) {
  for (const suite of suites) results.push(await runSuite(suite));
} else {
  results.push(...(await Promise.all(suites.map((suite) => runSuite(suite)))));
}

const durationMs = performance.now() - startedAt;
const baselinePath = await previousReport(LOG_ROOT, RUN_ID);
const report = await writeRunReport({
  logDir: LOG_DIR,
  mode: SEQUENTIAL ? 'sequential' : 'parallel',
  durationMs,
  suites: results,
  baselinePath,
});
console.log(`Telemetry JSON: ${LOG_DIR}/summary.json`);
console.log(`Telemetry report: ${LOG_DIR}/summary.md`);

const failures = results.filter((result) => !suitePassed(result));
if (failures.length > 0) {
  console.error(
    `\nSandbox validation failed: ${failures.map((result) => result.suite).join(', ')}.`
  );
  process.exit(1);
}

console.log(`\nAll sandbox suites passed in ${(report.durationMs / 1000).toFixed(1)}s.`);
