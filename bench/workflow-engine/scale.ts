/**
 * Horizontal Workflow Engine scale benchmark.
 *
 * Independent engines start behind a common barrier. Each child uses its own
 * process, SQLite files, queue name, and (for TCP) broker and dynamic ports.
 *
 * Run: bun run bench:workflow:scale
 */
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Mode, Sample } from './sample';

interface ScaleSample {
  mode: Mode;
  instances: number;
  executions: number;
  durationMs: number;
  throughput: number;
  startSkewMs: number;
  peakCpuPercent: number;
  peakRssMiB: number;
  integrity: 'pass';
}

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

function processUsage(rootPids: number[]): { cpu: number; rssMiB: number } {
  const output = Bun.spawnSync(['ps', '-eo', 'pid=,ppid=,rss=,pcpu=']).stdout.toString();
  const rows = output
    .trim()
    .split('\n')
    .map((line) => line.trim().split(/\s+/).map(Number))
    .filter((row) => row.length === 4);
  const included = new Set(rootPids);
  let changed = true;
  while (changed) {
    changed = false;
    for (const row of rows) {
      const pid = row[0] ?? 0;
      const parent = row[1] ?? 0;
      if (included.has(parent) && !included.has(pid)) {
        included.add(pid);
        changed = true;
      }
    }
  }
  let cpu = 0;
  let rssKiB = 0;
  for (const row of rows) {
    if (!included.has(row[0] ?? 0)) continue;
    rssKiB += row[2] ?? 0;
    cpu += row[3] ?? 0;
  }
  return { cpu, rssMiB: rssKiB / 1024 };
}

async function launchSet(
  mode: Mode,
  instances: number,
  n: number,
  concurrency: number
): Promise<ScaleSample> {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-workflow-scale-'));
  const startAt = Date.now() + 1_500;
  const children: ReturnType<typeof Bun.spawn>[] = [];
  try {
    for (let index = 0; index < instances; index++) {
      let httpPort = freePort();
      const tcpPort = freePort();
      while (httpPort === tcpPort) httpPort = freePort();
      children.push(
        Bun.spawn([process.execPath, join(import.meta.dir, '..', 'workflow-engine.ts')], {
          cwd: join(import.meta.dir, '../..'),
          env: {
            ...process.env,
            BUNQUEUE_WORKFLOW_SAMPLE: '1',
            BENCH_MODE: mode,
            BENCH_SCENARIO: 'linear',
            BENCH_N: String(n),
            BENCH_CONCURRENCY: String(concurrency),
            BENCH_START_BATCH: String(Bun.env.BENCH_START_BATCH ?? 100),
            BENCH_START_AT_MS: String(startAt),
            BENCH_PORT: String(tcpPort),
            BENCH_HTTP_PORT: String(httpPort),
            BENCH_DATA_PATH: join(directory, `workflow-${index}.db`),
            BENCH_BROKER_PATH: join(directory, `broker-${index}.db`),
          },
          stdout: 'pipe',
          stderr: 'pipe',
        })
      );
    }
    let peakCpuPercent = 0;
    let peakRssMiB = 0;
    const monitor = setInterval(() => {
      const usage = processUsage(children.map((child) => child.pid));
      peakCpuPercent = Math.max(peakCpuPercent, usage.cpu);
      peakRssMiB = Math.max(peakRssMiB, usage.rssMiB);
    }, 100);
    const samples = await Promise.all(
      children.map(async (child) => {
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
      })
    ).finally(() => clearInterval(monitor));
    const starts = samples.map((sample) => sample.startedAtEpochMs);
    const terminals = samples.map((sample) => sample.terminalAtEpochMs);
    const durationMs = Math.max(...terminals) - Math.min(...starts);
    const executions = instances * n;
    const eventCount = (type: string) =>
      samples.reduce((sum, sample) => sum + (sample.events[type] ?? 0), 0);
    if (
      samples.some((sample) => sample.integrity !== 'pass') ||
      eventCount('workflow:started') !== executions ||
      eventCount('workflow:completed') !== executions ||
      eventCount('step:completed') !== executions * 3
    ) {
      throw new Error('horizontal event conservation failed');
    }
    return {
      mode,
      instances,
      executions,
      durationMs: Math.round(durationMs * 100) / 100,
      throughput: Math.round((executions / durationMs) * 1_000),
      startSkewMs: Math.round((Math.max(...starts) - Math.min(...starts)) * 100) / 100,
      peakCpuPercent: Math.round(peakCpuPercent),
      peakRssMiB: Math.round(peakRssMiB),
      integrity: 'pass',
    };
  } finally {
    for (const child of children) {
      if (child.exitCode === null) child.kill();
    }
    rmSync(directory, { recursive: true, force: true });
  }
}

async function main(): Promise<void> {
  const requestedMode = Bun.env.BENCH_MODE ?? 'all';
  const modes: Mode[] = requestedMode === 'all' ? ['embedded', 'tcp'] : [requestedMode as Mode];
  const instances = (Bun.env.BENCH_INSTANCES ?? '1,4,8,12').split(',').map(Number);
  if (instances[0] !== 1) throw new Error('BENCH_INSTANCES must start with the x1 baseline');
  const runs = Number(Bun.env.BENCH_RUNS ?? 3);
  const warmups = Number(Bun.env.BENCH_WARMUPS ?? 1);
  const n = Number(Bun.env.BENCH_N ?? 5_000);
  const report: Record<string, unknown> = {
    generatedAt: new Date().toISOString(),
    revision: Bun.spawnSync(['git', 'rev-parse', 'HEAD']).stdout.toString().trim(),
    bun: Bun.version,
    config: {
      modes,
      instances,
      runs,
      warmups,
      n,
      rateLimitMaxRequests: Bun.env.RATE_LIMIT_MAX_REQUESTS ?? 'default (10000)',
    },
    results: {},
  };
  for (const mode of modes) {
    const concurrency = Number(
      mode === 'embedded'
        ? (Bun.env.BENCH_CONCURRENCY_EMBEDDED ?? 128)
        : (Bun.env.BENCH_CONCURRENCY_TCP ?? 64)
    );
    let baseline = 0;
    for (const count of instances) {
      for (let index = 0; index < warmups; index++) {
        await launchSet(mode, count, n, concurrency);
      }
      const samples: ScaleSample[] = [];
      for (let index = 0; index < runs; index++) {
        const sample = await launchSet(mode, count, n, concurrency);
        samples.push(sample);
        console.log(
          `${mode.padEnd(8)} x${String(count).padEnd(2)} ${index + 1}/${runs}: ` +
            `${sample.throughput.toLocaleString()} workflows/s`
        );
      }
      const throughput = samples.map((sample) => sample.throughput);
      const median = Math.round(percentile(throughput, 0.5));
      if (count === 1) baseline = median;
      const summary = {
        concurrency,
        runs,
        nPerInstance: n,
        throughput: {
          median,
          p05: Math.round(percentile(throughput, 0.05)),
          p95: Math.round(percentile(throughput, 0.95)),
          cvPercent: Math.round(cv(throughput) * 100) / 100,
        },
        speedup: Math.round((median / baseline) * 100) / 100,
        efficiencyPercent: Math.round((median / baseline / count) * 10_000) / 100,
        integrity: 'pass',
        samples,
      };
      (report.results as Record<string, unknown>)[`${mode}:x${count}`] = summary;
      console.log(
        `=> ${mode}/x${count}: median=${median.toLocaleString()} workflows/s, ` +
          `speedup=${summary.speedup}x, efficiency=${summary.efficiencyPercent}%\n`
      );
    }
  }
  const json = `${JSON.stringify(report, null, 2)}\n`;
  if (Bun.env.BENCH_OUTPUT) await Bun.write(Bun.env.BENCH_OUTPUT, json);
  console.log(json);
}

await main();
