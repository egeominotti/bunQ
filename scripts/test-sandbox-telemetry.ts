import { readdir } from 'node:fs/promises';
import { parseCount, parseFiles, parseSdkCounts, parseTests } from './test-sandbox-log-parser';

export interface ResourceSample {
  timestamp: string;
  cpuPercent: number;
  memoryUsedBytes: number;
  memoryLimitBytes: number;
  memoryPercent: number;
  pids: number;
  blockReadBytes: number;
  blockWriteBytes: number;
  networkRxBytes: number;
  networkTxBytes: number;
}

export interface TestMetric {
  file: string;
  name: string;
  status: 'pass' | 'fail' | 'skip';
  durationMs?: number;
}

export interface FileMetric {
  file: string;
  passed: number;
  failed: number;
  durationMs: number;
}

export interface SuiteTelemetry {
  schemaVersion: 1;
  suite: string;
  command: string[];
  container: string;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
  exitCode: number;
  oomKilled: boolean;
  tests: { passed: number; failed: number; skipped: number; cases: TestMetric[] };
  files: FileMetric[];
  resources: {
    sampleCount: number;
    cpuAveragePercent: number;
    cpuP95Percent: number;
    cpuPeakPercent: number;
    memoryStartBytes: number;
    memoryEndBytes: number;
    memoryDeltaBytes: number;
    memoryP95Bytes: number;
    memoryPeakBytes: number;
    memoryLimitBytes: number;
    memoryPeakPercent: number;
    memorySlopeBytesPerMinute: number;
    pidsPeak: number;
    blockReadBytes: number;
    blockWriteBytes: number;
    networkRxBytes: number;
    networkTxBytes: number;
  };
  anomalies: string[];
}

interface DockerStats {
  BlockIO: string;
  CPUPerc: string;
  MemPerc: string;
  MemUsage: string;
  NetIO: string;
  PIDs: string;
}

async function capture(command: string[]): Promise<{ code: number; output: string }> {
  const proc = Bun.spawn(command, { stdout: 'pipe', stderr: 'ignore' });
  const [output, code] = await Promise.all([new Response(proc.stdout).text(), proc.exited]);
  return { code, output: output.trim() };
}

function bytes(value: string): number {
  const match = value.trim().match(/^([\d.]+)\s*([kmgt]?i?b)?$/i);
  if (!match) return 0;
  const amount = Number(match[1]);
  const unit = (match[2] ?? 'b').toLowerCase();
  const powers: Record<string, number> = {
    b: 1,
    kb: 1_000,
    mb: 1_000_000,
    gb: 1_000_000_000,
    tb: 1_000_000_000_000,
    kib: 1024,
    mib: 1024 ** 2,
    gib: 1024 ** 3,
    tib: 1024 ** 4,
  };
  return amount * (powers[unit] ?? 1);
}

function pair(value: string): [number, number] {
  const [first = '0B', second = '0B'] = value.split('/');
  return [bytes(first), bytes(second)];
}

function parseSample(output: string): ResourceSample | null {
  try {
    const stats = JSON.parse(output) as DockerStats;
    const [memoryUsedBytes, memoryLimitBytes] = pair(stats.MemUsage);
    const [blockReadBytes, blockWriteBytes] = pair(stats.BlockIO);
    const [networkRxBytes, networkTxBytes] = pair(stats.NetIO);
    const sample = {
      timestamp: new Date().toISOString(),
      cpuPercent: Number.parseFloat(stats.CPUPerc) || 0,
      memoryUsedBytes,
      memoryLimitBytes,
      memoryPercent: Number.parseFloat(stats.MemPerc) || 0,
      pids: Number.parseInt(stats.PIDs, 10) || 0,
      blockReadBytes,
      blockWriteBytes,
      networkRxBytes,
      networkTxBytes,
    };
    return sample.memoryUsedBytes === 0 && sample.pids === 0 ? null : sample;
  } catch {
    return null;
  }
}

export async function monitorContainer(
  container: string,
  complete: () => boolean
): Promise<ResourceSample[]> {
  const samples: ResourceSample[] = [];
  while (!complete()) {
    const result = await capture([
      'docker',
      'stats',
      '--no-stream',
      '--format',
      '{{json .}}',
      container,
    ]);
    if (result.code === 0 && result.output) {
      const sample = parseSample(result.output);
      if (sample) samples.push(sample);
    } else {
      await Bun.sleep(100);
    }
  }
  return samples;
}

function percentile(values: number[], ratio: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.max(0, Math.ceil(sorted.length * ratio) - 1)];
}

function slope(samples: ResourceSample[]): number {
  if (samples.length < 2) return 0;
  const start = Date.parse(samples[0].timestamp);
  const xs = samples.map((sample) => (Date.parse(sample.timestamp) - start) / 60_000);
  const ys = samples.map((sample) => sample.memoryUsedBytes);
  const xMean = xs.reduce((sum, value) => sum + value, 0) / xs.length;
  const yMean = ys.reduce((sum, value) => sum + value, 0) / ys.length;
  const numerator = xs.reduce((sum, x, index) => sum + (x - xMean) * (ys[index] - yMean), 0);
  const denominator = xs.reduce((sum, x) => sum + (x - xMean) ** 2, 0);
  return denominator === 0 ? 0 : numerator / denominator;
}

export async function createSuiteTelemetry(input: {
  suite: string;
  command: readonly string[];
  container: string;
  logPath: string;
  startedAt: string;
  finishedAt: string;
  exitCode: number;
  oomKilled: boolean;
  samples: ResourceSample[];
}): Promise<SuiteTelemetry> {
  const log = await Bun.file(input.logPath).text();
  const cases = parseTests(log, input.suite);
  const files = parseFiles(log);
  const samples = input.samples.filter((sample) => sample.memoryUsedBytes > 0 || sample.pids > 0);
  const memory = samples.map((sample) => sample.memoryUsedBytes);
  const cpu = samples.map((sample) => sample.cpuPercent);
  const first = samples[0];
  const last = samples.at(-1);
  const peakMemory = Math.max(0, ...memory);
  const limit = Math.max(0, ...samples.map((sample) => sample.memoryLimitBytes));
  const memoryDelta = (last?.memoryUsedBytes ?? 0) - (first?.memoryUsedBytes ?? 0);
  const anomalies: string[] = [];
  if (input.exitCode !== 0) anomalies.push(`non-zero exit code: ${input.exitCode}`);
  if (input.oomKilled) anomalies.push('container was OOM-killed');
  if (limit > 0 && peakMemory / limit >= 0.8) anomalies.push('peak memory reached at least 80%');
  if (Math.max(0, ...samples.map((sample) => sample.pids)) >= 1638)
    anomalies.push('PID usage reached at least 80%');
  if (memoryDelta >= 128 * 1024 ** 2 && memoryDelta >= (first?.memoryUsedBytes ?? 0) * 0.5)
    anomalies.push('memory growth signal: end minus start exceeds 128 MiB and 50%');
  if (samples.length === 0) anomalies.push('no resource samples captured');

  const sdkCounts = parseSdkCounts(input.suite, log);
  const passed =
    sdkCounts?.passed ??
    (parseCount(log, 'pass') || files.reduce((sum, file) => sum + file.passed, 0));
  const failed =
    sdkCounts?.failed ??
    (parseCount(log, 'fail') || files.reduce((sum, file) => sum + file.failed, 0));
  const skipped = sdkCounts?.skipped ?? parseCount(log, 'skip');
  return {
    schemaVersion: 1,
    suite: input.suite,
    command: [...input.command],
    container: input.container,
    startedAt: input.startedAt,
    finishedAt: input.finishedAt,
    durationMs: Date.parse(input.finishedAt) - Date.parse(input.startedAt),
    exitCode: input.exitCode,
    oomKilled: input.oomKilled,
    tests: { passed, failed, skipped, cases },
    files,
    resources: {
      sampleCount: samples.length,
      cpuAveragePercent: cpu.reduce((sum, value) => sum + value, 0) / Math.max(1, cpu.length),
      cpuP95Percent: percentile(cpu, 0.95),
      cpuPeakPercent: Math.max(0, ...cpu),
      memoryStartBytes: first?.memoryUsedBytes ?? 0,
      memoryEndBytes: last?.memoryUsedBytes ?? 0,
      memoryDeltaBytes: memoryDelta,
      memoryP95Bytes: percentile(memory, 0.95),
      memoryPeakBytes: peakMemory,
      memoryLimitBytes: limit,
      memoryPeakPercent: limit > 0 ? (peakMemory / limit) * 100 : 0,
      memorySlopeBytesPerMinute: slope(samples),
      pidsPeak: Math.max(0, ...samples.map((sample) => sample.pids)),
      blockReadBytes: last?.blockReadBytes ?? 0,
      blockWriteBytes: last?.blockWriteBytes ?? 0,
      networkRxBytes: last?.networkRxBytes ?? 0,
      networkTxBytes: last?.networkTxBytes ?? 0,
    },
    anomalies,
  };
}

export async function previousReport(
  logRoot: string,
  currentRunId: string
): Promise<string | null> {
  const runs = (await readdir(logRoot, { withFileTypes: true }))
    .filter((entry) => entry.isDirectory() && entry.name !== currentRunId)
    .map((entry) => entry.name)
    .sort()
    .reverse();
  for (const run of runs) {
    const path = `${logRoot}/${run}/summary.json`;
    if (await Bun.file(path).exists()) return path;
  }
  return null;
}
