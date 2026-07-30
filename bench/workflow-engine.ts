/**
 * Workflow Engine benchmark coordinator.
 *
 * Every sample runs in a fresh process and temporary directory. TCP samples
 * also launch a fresh broker on dynamic ports.
 *
 * Run: bun run bench:workflow
 */
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { runSample, type Mode, type Sample, type Scenario } from './workflow-engine/sample';

const concurrency = Number(Bun.env.BENCH_CONCURRENCY ?? 32);
const startBatch = Number(Bun.env.BENCH_START_BATCH ?? 100);

function percentile(values: number[], ratio: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * ratio))] ?? 0;
}

function cv(values: number[]): number {
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length;
  return (Math.sqrt(variance) / mean) * 100;
}

function freePort(): number {
  const listener = Bun.listen({
    hostname: '127.0.0.1',
    port: 0,
    socket: { data: () => undefined },
  });
  const port = listener.port;
  listener.stop(true);
  return port;
}

async function launch(mode: Mode, scenario: Scenario, n: number): Promise<Sample> {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-workflow-bench-'));
  const tcpPort = freePort();
  let httpPort = freePort();
  while (httpPort === tcpPort) httpPort = freePort();
  try {
    const child = Bun.spawn([process.execPath, import.meta.path], {
      cwd: import.meta.dir,
      env: {
        ...process.env,
        BUNQUEUE_WORKFLOW_SAMPLE: '1',
        BENCH_MODE: mode,
        BENCH_SCENARIO: scenario,
        BENCH_N: String(n),
        BENCH_PORT: String(tcpPort),
        BENCH_HTTP_PORT: String(httpPort),
        BENCH_DATA_PATH: join(directory, 'workflow.db'),
        BENCH_BROKER_PATH: join(directory, 'broker.db'),
      },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ]);
    if (exitCode !== 0) throw new Error(stderr || stdout || `sample exited ${exitCode}`);
    if (stderr.trim() && Bun.env.BENCH_ALLOW_STDERR !== '1') {
      throw new Error(`sample emitted stderr:\n${stderr}`);
    }
    return JSON.parse(stdout.trim()) as Sample;
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

async function coordinate(): Promise<void> {
  const requestedMode = Bun.env.BENCH_MODE ?? 'all';
  const modes: Mode[] = requestedMode === 'all' ? ['embedded', 'tcp'] : [requestedMode as Mode];
  const scenarios = (Bun.env.BENCH_SCENARIOS ?? 'linear,parallel,compensation,signal').split(
    ','
  ) as Scenario[];
  const defaultRuns = Number(Bun.env.BENCH_RUNS ?? 7);
  const warmups = Number(Bun.env.BENCH_WARMUPS ?? 1);
  const counts: Record<Scenario, number> = {
    linear: Number(Bun.env.BENCH_N_LINEAR ?? 1_000),
    parallel: Number(Bun.env.BENCH_N_PARALLEL ?? 500),
    compensation: Number(Bun.env.BENCH_N_COMPENSATION ?? 200),
    signal: Number(Bun.env.BENCH_N_SIGNAL ?? 200),
  };
  const report: Record<string, unknown> = {
    generatedAt: new Date().toISOString(),
    revision: Bun.spawnSync(['git', 'rev-parse', 'HEAD']).stdout.toString().trim(),
    bun: Bun.version,
    config: {
      modes,
      scenarios,
      warmups,
      concurrency,
      startBatch,
      counts,
      rateLimitMaxRequests: Bun.env.RATE_LIMIT_MAX_REQUESTS ?? 'default (10000)',
    },
    results: {},
  };
  for (const mode of modes) {
    for (const scenario of scenarios) {
      const runs = Number(Bun.env[`BENCH_RUNS_${scenario.toUpperCase()}`] ?? defaultRuns);
      for (let i = 0; i < warmups; i++) await launch(mode, scenario, counts[scenario]);
      const samples: Sample[] = [];
      for (let i = 0; i < runs; i++) {
        const sample = await launch(mode, scenario, counts[scenario]);
        samples.push(sample);
        console.log(
          `${mode.padEnd(8)} ${scenario.padEnd(12)} ${i + 1}/${runs}: ` +
            `${sample.throughput.toLocaleString()} workflows/s, p95=${sample.latencyUs.p95}us`
        );
      }
      const throughput = samples.map((sample) => sample.throughput);
      const summary = {
        runs,
        nPerRun: counts[scenario],
        totalExecutions: runs * counts[scenario],
        throughput: {
          median: Math.round(percentile(throughput, 0.5)),
          p05: Math.round(percentile(throughput, 0.05)),
          p95: Math.round(percentile(throughput, 0.95)),
          cvPercent: Math.round(cv(throughput) * 100) / 100,
        },
        runMedianLatencyUs: {
          p50: Math.round(
            percentile(
              samples.map((sample) => sample.latencyUs.p50),
              0.5
            )
          ),
          p95: Math.round(
            percentile(
              samples.map((sample) => sample.latencyUs.p95),
              0.5
            )
          ),
          p99: Math.round(
            percentile(
              samples.map((sample) => sample.latencyUs.p99),
              0.5
            )
          ),
        },
        ...(scenario === 'signal'
          ? {
              signal: {
                parkThroughputMedian: Math.round(
                  percentile(
                    samples.map((sample) => sample.signal?.parkThroughput ?? 0),
                    0.5
                  )
                ),
                resumeThroughputMedian: Math.round(
                  percentile(
                    samples.map((sample) => sample.signal?.resumeThroughput ?? 0),
                    0.5
                  )
                ),
                parkLatencyP95RunMedianUs: Math.round(
                  percentile(
                    samples.map((sample) => sample.signal?.parkLatencyUs.p95 ?? 0),
                    0.5
                  )
                ),
              },
            }
          : {}),
        integrity: 'pass',
        samples,
      };
      (report.results as Record<string, unknown>)[`${mode}:${scenario}`] = summary;
      console.log(
        `=> ${mode}/${scenario}: median=${summary.throughput.median.toLocaleString()} workflows/s, ` +
          `CV=${summary.throughput.cvPercent}%\n`
      );
    }
  }
  const json = `${JSON.stringify(report, null, 2)}\n`;
  if (Bun.env.BENCH_OUTPUT) await Bun.write(Bun.env.BENCH_OUTPUT, json);
  console.log(json);
}

if (Bun.env.BUNQUEUE_WORKFLOW_SAMPLE === '1') {
  console.log(JSON.stringify(await runSample()));
} else {
  await coordinate();
}
