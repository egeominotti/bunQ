#!/usr/bin/env bun
import { createWriteStream, mkdirSync } from 'node:fs';
import { finished } from 'node:stream/promises';
import { suitePassed, writeRunReport, writeSuiteArtifacts } from './test-sandbox-report';
import {
  createSuiteTelemetry,
  monitorContainer,
  previousReport,
  type SuiteTelemetry,
} from './test-sandbox-telemetry';
import {
  startSdkPostgresInfrastructure,
  type SdkPostgresInfrastructure,
} from './sdk-sandbox-postgres';
import { cleanupSdkResources, removeSdkContainer } from './sdk-sandbox-cleanup';
import { runSdkSuitesSettled } from './sdk-sandbox-orchestration';
import { sdkSandboxSuites as suites, type SdkSandboxSuite } from './sdk-sandbox-suites';

const ROOT = `${import.meta.dir}/..`;
const RUN_ID = new Date().toISOString().replaceAll(':', '-');
const LOG_ROOT = `${ROOT}/artifacts/test-sandbox-sdk`;
const LOG_DIR = `${LOG_ROOT}/${RUN_ID}`;
const MEMORY = Bun.env.BUNQUEUE_SDK_TEST_MEMORY ?? '4g';
const CPUS = Bun.env.BUNQUEUE_SDK_TEST_CPUS;
const SEQUENTIAL = Bun.env.BUNQUEUE_SDK_TEST_SEQUENTIAL === '1';
const active = new Set<string>();
let postgres: SdkPostgresInfrastructure | null = null;
const teardownErrors: Error[] = [];

async function failedSuiteTelemetry(
  suite: SdkSandboxSuite,
  error: unknown
): Promise<SuiteTelemetry> {
  const failure = error instanceof Error ? error : new Error(String(error));
  teardownErrors.push(failure);
  const logPath = `${LOG_DIR}/${suite.name}.log`;
  await Bun.write(logPath, `SDK suite orchestration failed: ${failure.message}\n`);
  const now = new Date().toISOString();
  const telemetry = await createSuiteTelemetry({
    suite: suite.name,
    command: suite.command,
    container: `bunqueue-sdk-${suite.name}-${process.pid}`.slice(0, 63),
    logPath,
    startedAt: now,
    finishedAt: now,
    exitCode: 1,
    oomKilled: false,
    samples: [],
  });
  telemetry.anomalies.push(`orchestration failure: ${failure.message}`);
  await writeSuiteArtifacts(LOG_DIR, telemetry, []);
  return telemetry;
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
  removeSdkContainer(active, name, ROOT);
}

function cleanup(): void {
  cleanupSdkResources(active, postgres, ROOT);
  postgres = null;
}

function handleSignal(signal: string, exitCode: number): void {
  try {
    cleanup();
  } catch (error) {
    console.error(
      `${signal} cleanup failed; owned Docker resources were retained for retry`,
      error
    );
    process.exit(1);
  }
  process.exit(exitCode);
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

process.on('SIGINT', () => handleSignal('SIGINT', 130));
process.on('SIGTERM', () => handleSignal('SIGTERM', 143));

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

postgres = await startSdkPostgresInfrastructure(ROOT);

async function runSuite(suite: SdkSandboxSuite): Promise<SuiteTelemetry> {
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
    postgres.network,
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
    '--env',
    `BUNQUEUE_CONFORMANCE_POSTGRES_URL=${postgres.url}`,
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
    try {
      remove(container);
      console.log(`SDK suite passed: ${suite.name}`);
    } catch (error) {
      const cleanupError = error instanceof Error ? error : new Error(String(error));
      teardownErrors.push(cleanupError);
      telemetry.exitCode = 1;
      telemetry.anomalies.push(`container cleanup failed: ${cleanupError.message}`);
      console.error(`SDK suite cleanup failed: ${suite.name}`, cleanupError);
    }
  } else {
    active.delete(container);
    const counted = telemetry.tests.passed + telemetry.tests.failed + telemetry.tests.skipped;
    const why = exitCode === 0 ? 'reported no results' : `exited ${exitCode}`;
    console.error(
      `SDK suite ${suite.name} failed (${why}, ${counted} tests counted); retained container: ${container}`
    );
  }
  return telemetry;
}

const settled = await runSdkSuitesSettled(
  suites,
  async (suite) => {
    try {
      return await runSuite(suite);
    } catch (error) {
      return failedSuiteTelemetry(suite, error);
    }
  },
  SEQUENTIAL
);
const results = settled.results;
const orchestrationErrors = [...settled.errors, ...teardownErrors];
if (orchestrationErrors.length > 0) {
  try {
    cleanup();
  } catch (error) {
    orchestrationErrors.push(error instanceof Error ? error : new Error(String(error)));
  }
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
if (!report.passed || orchestrationErrors.length > 0) {
  if (orchestrationErrors.length > 0)
    console.error(new AggregateError(orchestrationErrors, 'SDK sandbox orchestration failed'));
  if (postgres)
    console.error(
      `Retained PostgreSQL infrastructure: ${postgres.container} on network ${postgres.network}`
    );
  else console.error('PostgreSQL infrastructure cleanup completed.');
  process.exit(1);
}
postgres.stop();
postgres = null;
console.log(`All SDK sandbox suites passed in ${(report.durationMs / 1000).toFixed(1)}s.`);
