import { mkdir, mkdtemp, rm } from 'node:fs/promises';
import { arch, cpus, loadavg, platform, release, tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  batchSize,
  brokerCounts,
  consumerConnections,
  jobs,
  pollIntervalMs,
  poolSize,
  producerConnections,
  runs,
  versions,
  warmups,
  workMem,
} from './postgres-versions/config';
import { rotate, summarizeSamples } from './postgres-versions/stats';
import type {
  MatrixObservation,
  PostgresVersionMatrixReport,
  PostgresVersionSample,
} from './postgres-versions/types';
interface PostgresInstallation {
  readonly bin: string;
  readonly major: number;
  readonly version: string;
}
const [startedAt, startedAtMs] = [new Date().toISOString(), Date.now()];
const loadAverage1mStart = loadavg()[0];

function commandOutput(command: readonly string[]): string {
  const result = Bun.spawnSync([...command], { stderr: 'pipe', stdout: 'pipe' });
  if (result.exitCode !== 0) {
    throw new Error(
      `${command.join(' ')} failed: ${new TextDecoder().decode(result.stderr).trim()}`
    );
  }
  return new TextDecoder().decode(result.stdout).trim();
}

async function installations(): Promise<PostgresInstallation[]> {
  return await Promise.all(
    versions.map(async (major) => {
      const override = Bun.env[`BUNQUEUE_PG_BIN_${major}`];
      const prefix = override
        ? override.replace(/\/bin\/?$/, '')
        : commandOutput(['brew', '--prefix', `postgresql@${major}`]);
      const bin = join(prefix, 'bin');
      const postgres = join(bin, 'postgres');
      if (!(await Bun.file(postgres).exists())) throw new Error(`Missing ${postgres}`);
      const version = commandOutput([postgres, '--version']);
      if (!version.includes(` ${major}.`)) {
        throw new Error(`Expected PostgreSQL ${major} at ${postgres}, received ${version}`);
      }
      return { bin, major, version };
    })
  );
}

function reservePort(): number {
  const listener = Bun.listen({
    hostname: '127.0.0.1',
    port: 0,
    socket: { data() {} },
  });
  const port = listener.port;
  listener.stop(true);
  return port;
}

async function waitForPostgres(
  installation: PostgresInstallation,
  server: ReturnType<typeof Bun.spawn>,
  port: number
): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (server.exitCode !== null) throw new Error(`PostgreSQL exited with ${server.exitCode}`);
    const ready = Bun.spawnSync(
      [join(installation.bin, 'pg_isready'), '-h', '127.0.0.1', '-p', String(port)],
      { stderr: 'ignore', stdout: 'ignore' }
    );
    if (ready.exitCode === 0) return;
    await Bun.sleep(25);
  }
  throw new Error(`PostgreSQL ${installation.major} readiness timed out`);
}

function parseSample(stdout: string): PostgresVersionSample {
  const line = stdout
    .split('\n')
    .find((candidate) => candidate.startsWith('POSTGRES_VERSION_SAMPLE '));
  if (!line) throw new Error(`Benchmark sample did not emit its result:\n${stdout}`);
  return JSON.parse(line.slice('POSTGRES_VERSION_SAMPLE '.length)) as PostgresVersionSample;
}

async function executeSample(
  installation: PostgresInstallation,
  brokers: number
): Promise<PostgresVersionSample> {
  const root = await mkdtemp(join(tmpdir(), `bunqueue-pg${installation.major}-`));
  const data = join(root, 'data');
  const socket = join(root, 'socket');
  await mkdir(socket);
  const port = reservePort();
  let server: ReturnType<typeof Bun.spawn> | null = null;
  let serverStderr: Promise<string> | null = null;
  try {
    commandOutput([
      join(installation.bin, 'initdb'),
      '-A',
      'trust',
      '-D',
      data,
      '-E',
      'UTF8',
      '-U',
      'bunqueue',
      '--no-locale',
    ]);
    server = Bun.spawn(
      [
        join(installation.bin, 'postgres'),
        '-D',
        data,
        '-h',
        '127.0.0.1',
        '-k',
        socket,
        '-p',
        String(port),
        '-c',
        'fsync=on',
        '-c',
        'synchronous_commit=on',
        '-c',
        'full_page_writes=on',
        '-c',
        'shared_buffers=128MB',
        '-c',
        'max_connections=100',
        '-c',
        `work_mem=${workMem}`,
        '-c',
        'log_min_messages=warning',
      ],
      { stderr: 'pipe', stdout: 'ignore' }
    );
    serverStderr = new Response(server.stderr).text();
    await waitForPostgres(installation, server, port);
    commandOutput([
      join(installation.bin, 'createdb'),
      '-h',
      '127.0.0.1',
      '-p',
      String(port),
      '-U',
      'bunqueue',
      'bunqueue',
    ]);

    const sample = Bun.spawn([process.execPath, 'run', 'bench/postgres-versions/sample.ts'], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        BUNQUEUE_PG_BENCH_BATCH_SIZE: String(batchSize),
        BUNQUEUE_PG_BENCH_BROKERS: String(brokers),
        BUNQUEUE_PG_BENCH_CONSUMERS: String(consumerConnections),
        BUNQUEUE_PG_BENCH_JOBS: String(jobs),
        BUNQUEUE_PG_BENCH_MAJOR: String(installation.major),
        BUNQUEUE_PG_BENCH_POLL_INTERVAL_MS: String(pollIntervalMs),
        BUNQUEUE_PG_BENCH_PRODUCERS: String(producerConnections),
        BUNQUEUE_PG_BENCH_POOL_SIZE: String(poolSize),
        BUNQUEUE_PG_BENCH_URL: `postgresql://bunqueue@127.0.0.1:${port}/bunqueue`,
      },
      stderr: 'pipe',
      stdout: 'pipe',
    });
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(sample.stdout).text(),
      new Response(sample.stderr).text(),
      sample.exited,
    ]);
    if (exitCode !== 0) throw new Error(`Sample failed with ${exitCode}:\n${stderr}\n${stdout}`);
    return parseSample(stdout);
  } catch (error) {
    const postgresLog = serverStderr ? await Promise.race([serverStderr, Bun.sleep(100)]) : '';
    throw new Error(`${String(error)}\nPostgreSQL log:\n${postgresLog}`.trim());
  } finally {
    if (server?.exitCode === null) {
      const stopped = Bun.spawnSync(
        [join(installation.bin, 'pg_ctl'), '-D', data, '-m', 'fast', '-w', 'stop'],
        { stderr: 'ignore', stdout: 'ignore' }
      );
      if (stopped.exitCode !== 0 && server.exitCode === null) server.kill('SIGKILL');
    }
    if (server) await server.exited;
    await rm(root, { force: true, recursive: true });
  }
}

function printSummary(report: PostgresVersionMatrixReport): void {
  console.log(
    '\nPG | brokers | admission median | processing median | lifecycle median | CV | 95% CI'
  );
  for (const summary of report.summaries) {
    const lifecycle = summary.lifecycleJobsPerSecond;
    console.log(
      `${summary.postgresVersion} | ${summary.brokers} | ${summary.admissionJobsPerSecond.median} | ` +
        `${summary.processingJobsPerSecond.median} | ${lifecycle.median} | ` +
        `${lifecycle.cvPercent}% | ${lifecycle.meanCi95Low}-${lifecycle.meanCi95High}`
    );
  }
}

async function main(): Promise<void> {
  const available = await installations();
  const observations: MatrixObservation[] = [];
  console.log(
    `PostgreSQL native matrix: versions=${versions.join(',')} brokers=${brokerCounts.join(',')} ` +
      `jobs=${jobs} warmups=${warmups} runs=${runs}`
  );
  for (let round = -warmups; round < runs; round++) {
    const stage = round + warmups;
    const versionOrder = rotate(available, stage);
    const topologyOrder = stage % 2 === 0 ? brokerCounts : [...brokerCounts].reverse();
    for (const installation of versionOrder) {
      for (const brokers of topologyOrder) {
        const measured = round >= 0;
        const label = measured ? `run ${round + 1}/${runs}` : `warmup ${stage + 1}/${warmups}`;
        process.stdout.write(`PG${installation.major} brokers=${brokers} ${label} ... `);
        const sample = await executeSample(installation, brokers);
        observations.push({ measured, round, sample });
        console.log(
          `${sample.lifecycleJobsPerSecond.toLocaleString()} jobs/s ` +
            `(admit ${sample.admissionJobsPerSecond.toLocaleString()}, ` +
            `process ${sample.processingJobsPerSecond.toLocaleString()})`
        );
      }
    }
  }

  const measured = observations
    .filter((observation) => observation.measured)
    .map(({ sample }) => sample);
  const host: Record<string, string | number> = {
    architecture: arch(),
    bun: Bun.version,
    cpu: cpus()[0]?.model ?? 'unknown',
    cpuCount: cpus().length,
    loadAverage1mEnd: loadavg()[0],
    loadAverage1mStart,
    os: `${platform()} ${release()}`,
    repositoryCommit: commandOutput(['git', 'rev-parse', 'HEAD']),
    runtimeTree: commandOutput(['git', 'rev-parse', 'HEAD:src']),
    runtimeWorktreeStatus: commandOutput(['git', 'status', '--short', '--', 'src']) || 'clean',
  };
  for (const installation of available)
    host[`postgres${installation.major}`] = installation.version;
  const report: PostgresVersionMatrixReport = {
    completedAt: new Date().toISOString(),
    configuration: {
      batchSize,
      brokers: brokerCounts,
      consumerConnections,
      jobs,
      pollIntervalMs,
      producerConnections,
      poolSize,
      runs,
      versions,
      warmups,
      workMem,
    },
    durationSeconds: Math.round((Date.now() - startedAtMs) / 100) / 10,
    host,
    observations,
    startedAt,
    summaries: summarizeSamples(measured),
  };
  const outputDirectory = join(process.cwd(), 'artifacts', 'benchmarks');
  await mkdir(outputDirectory, { recursive: true });
  const output = join(outputDirectory, `postgres-versions-${startedAt.replaceAll(':', '-')}.json`);
  await Bun.write(output, `${JSON.stringify(report, null, 2)}\n`);
  printSummary(report);
  console.log(`\nRaw report: ${output}`);
}

await main();
