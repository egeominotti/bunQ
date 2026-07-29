import type { ResourceSample, SuiteTelemetry, TestMetric } from './test-sandbox-telemetry';

interface RunReport {
  schemaVersion: 1;
  generatedAt: string;
  mode: 'parallel' | 'sequential';
  durationMs: number;
  passed: boolean;
  baseline?: string;
  suites: SuiteTelemetry[];
}

function formatBytes(value: number): string {
  const absolute = Math.abs(value);
  const sign = value < 0 ? '-' : '';
  if (absolute >= 1024 ** 3) return `${sign}${(absolute / 1024 ** 3).toFixed(2)} GiB`;
  if (absolute >= 1024 ** 2) return `${sign}${(absolute / 1024 ** 2).toFixed(1)} MiB`;
  if (absolute >= 1024) return `${sign}${(absolute / 1024).toFixed(1)} KiB`;
  return `${value.toFixed(0)} B`;
}

function formatDuration(value: number): string {
  if (value >= 60_000) return `${(value / 60_000).toFixed(2)} min`;
  if (value >= 1000) return `${(value / 1000).toFixed(2)} s`;
  return `${value.toFixed(1)} ms`;
}

function delta(current: number, baseline: number): number {
  return baseline === 0 ? 0 : ((current - baseline) / baseline) * 100;
}

async function readBaseline(path: string | null): Promise<RunReport | null> {
  if (!path) return null;
  try {
    const parsed = (await Bun.file(path).json()) as RunReport;
    // Syntactically valid JSON is not enough. A baseline written by an older schema has
    // no `tests` on its suites, and `suitePassed` would dereference it and throw from
    // inside `writeRunReport` AFTER the whole run: the 15 minutes of work would end with
    // no `summary.json` and no `summary.md` at all. An unusable baseline is worth less
    // than the report it would destroy.
    if (!Array.isArray(parsed?.suites)) return null;
    if (!parsed.suites.every((suite) => suite?.tests && typeof suite.exitCode === 'number')) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function addBaselineSignals(current: SuiteTelemetry, baseline?: SuiteTelemetry): void {
  // A 0/0/0 run must not become the duration and peak-memory baseline the next run is
  // compared against: it observed nothing, so every later run would look like a
  // regression against nothing.
  if (!baseline || !suitePassed(baseline)) return;
  const durationDelta = delta(current.durationMs, baseline.durationMs);
  if (durationDelta >= 25 && current.durationMs - baseline.durationMs >= 30_000) {
    current.anomalies.push(`duration regression vs baseline: +${durationDelta.toFixed(1)}%`);
  }
  const memoryDelta = delta(current.resources.memoryPeakBytes, baseline.resources.memoryPeakBytes);
  if (
    memoryDelta >= 25 &&
    current.resources.memoryPeakBytes - baseline.resources.memoryPeakBytes >= 64 * 1024 ** 2
  ) {
    current.anomalies.push(`peak-memory regression vs baseline: +${memoryDelta.toFixed(1)}%`);
  }
}

function slowTests(suites: SuiteTelemetry[], limit = 20): TestMetric[] {
  return suites
    .flatMap((suite) => suite.tests.cases)
    .filter((test): test is TestMetric & { durationMs: number } => test.durationMs !== undefined)
    .sort((a, b) => b.durationMs - a.durationMs)
    .slice(0, limit);
}

function suiteTable(suites: SuiteTelemetry[]): string[] {
  const rows = [
    '| Suite | Status | Duration | Pass / Fail / Skip | Peak RAM | RAM delta | CPU avg / p95 / peak | PID peak | Anomalies |',
    '|---|---:|---:|---:|---:|---:|---:|---:|---|',
  ];
  for (const suite of suites) {
    const resources = suite.resources;
    rows.push(
      `| ${suite.suite} | ${suiteVerdict(suite)} | ${formatDuration(suite.durationMs)} | ${suite.tests.passed} / ${suite.tests.failed} / ${suite.tests.skipped} | ${formatBytes(resources.memoryPeakBytes)} (${resources.memoryPeakPercent.toFixed(1)}%) | ${formatBytes(resources.memoryDeltaBytes)} | ${resources.cpuAveragePercent.toFixed(1)}% / ${resources.cpuP95Percent.toFixed(1)}% / ${resources.cpuPeakPercent.toFixed(1)}% | ${resources.pidsPeak} | ${suite.anomalies.join('; ') || 'none'} |`
    );
  }
  return rows;
}

function resourceTable(suites: SuiteTelemetry[]): string[] {
  const rows = [
    '| Suite | Samples | Tests/s | RAM start / p95 / end / peak | RAM slope | Block read / write | Network RX / TX |',
    '|---|---:|---:|---:|---:|---:|---:|',
  ];
  for (const suite of suites) {
    const resources = suite.resources;
    const observedTests = suite.tests.passed + suite.tests.failed + suite.tests.skipped;
    const testsPerSecond = suite.durationMs > 0 ? observedTests / (suite.durationMs / 1000) : 0;
    rows.push(
      `| ${suite.suite} | ${resources.sampleCount} | ${testsPerSecond.toFixed(2)} | ${formatBytes(resources.memoryStartBytes)} / ${formatBytes(resources.memoryP95Bytes)} / ${formatBytes(resources.memoryEndBytes)} / ${formatBytes(resources.memoryPeakBytes)} | ${formatBytes(resources.memorySlopeBytesPerMinute)}/min | ${formatBytes(resources.blockReadBytes)} / ${formatBytes(resources.blockWriteBytes)} | ${formatBytes(resources.networkRxBytes)} / ${formatBytes(resources.networkTxBytes)} |`
    );
  }
  return rows;
}

function integrationTable(suites: SuiteTelemetry[]): string[] {
  const files = suites
    .flatMap((suite) => suite.files.map((file) => ({ suite: suite.suite, ...file })))
    .sort((a, b) => b.durationMs - a.durationMs);
  if (files.length === 0) return [];
  return [
    '## Integration files',
    '',
    '| Suite | File | Duration | Pass / Fail |',
    '|---|---|---:|---:|',
    ...files.map(
      (file) =>
        `| ${file.suite} | ${file.file} | ${formatDuration(file.durationMs)} | ${file.passed} / ${file.failed} |`
    ),
    '',
  ];
}

function markdown(report: RunReport): string {
  const slow = slowTests(report.suites);
  const lines = [
    '# Test sandbox telemetry',
    '',
    `- Generated: ${report.generatedAt}`,
    `- Mode: ${report.mode}`,
    `- Overall: ${report.passed ? 'PASS' : 'FAIL'}`,
    `- Wall time: ${formatDuration(report.durationMs)}`,
    `- Baseline: ${report.baseline ?? 'none (first telemetry run)'}`,
    '',
    'Resource metrics are container-level. They can be correlated with test timestamps but are not attributed as per-test heap ownership.',
    '',
    '## Suite KPIs',
    '',
    ...suiteTable(report.suites),
    '',
    '## Resource detail',
    '',
    ...resourceTable(report.suites),
    '',
    '## Slowest unit tests',
    '',
    '| File | Test | Status | Duration |',
    '|---|---|---:|---:|',
    ...slow.map(
      (test) =>
        `| ${test.file} | ${test.name.replaceAll('|', '\\|')} | ${test.status} | ${formatDuration(test.durationMs ?? 0)} |`
    ),
    '',
    ...integrationTable(report.suites),
    '## Interpretation',
    '',
    '- OOM, non-zero exits, memory pressure, PID pressure, strong end-to-start growth, and material baseline regressions are reported as anomalies.',
    '- A memory-growth signal is evidence to investigate, not proof of a leak; mixed suites intentionally populate caches and exercise different workloads.',
    '- Use the NDJSON samples to correlate changes with timestamps in the complete suite log.',
    '',
  ];
  return lines.join('\n');
}

export async function writeSuiteArtifacts(
  logDir: string,
  suite: SuiteTelemetry,
  samples: ResourceSample[]
): Promise<void> {
  const ndjson = samples.map((sample) => JSON.stringify(sample)).join('\n');
  await Promise.all([
    Bun.write(`${logDir}/${suite.suite}.metrics.ndjson`, ndjson ? `${ndjson}\n` : ''),
    Bun.write(`${logDir}/${suite.suite}.summary.json`, `${JSON.stringify(suite, null, 2)}\n`),
  ]);
}

export async function writeRunReport(input: {
  logDir: string;
  mode: RunReport['mode'];
  durationMs: number;
  suites: SuiteTelemetry[];
  baselinePath: string | null;
}): Promise<RunReport> {
  const baseline = await readBaseline(input.baselinePath);
  for (const suite of input.suites) {
    addBaselineSignals(
      suite,
      baseline?.suites.find((candidate) => candidate.suite === suite.suite)
    );
  }
  const report: RunReport = {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    mode: input.mode,
    durationMs: input.durationMs,
    passed: input.suites.every(suitePassed),
    ...(input.baselinePath && { baseline: input.baselinePath }),
    suites: input.suites,
  };
  await Promise.all([
    Bun.write(`${input.logDir}/summary.json`, `${JSON.stringify(report, null, 2)}\n`),
    Bun.write(`${input.logDir}/summary.md`, markdown(report)),
    ...input.suites.map((suite) =>
      Bun.write(
        `${input.logDir}/${suite.suite}.summary.json`,
        `${JSON.stringify(suite, null, 2)}\n`
      )
    ),
  ]);
  return report;
}
/**
 * Did this suite actually pass?
 *
 * Exit code alone is not enough. A runner that dies before reporting anything, or whose
 * output never reaches the parser, exits 0 with nothing counted, and that is precisely
 * how a run reported `unit | PASS | 0 / 0 / 0` over 434 of 445 files with every
 * model-based suite missing. An empty result is a failure to observe, not an observation
 * of success.
 *
 * Single source of truth on purpose: the markdown verdict, the JSON `passed` field, the
 * baseline gate and the SDK gate all read this. They were separate predicates, and the
 * one on `summary.json` still said `passed: true` for a 0/0/0 run, which is the artifact
 * CLAUDE.md tells a reviewer to read before handoff. A guard that disagrees with the
 * machine-readable artifact is not a guard.
 */
export function suitePassed(suite: {
  exitCode: number;
  tests: { passed: number; failed: number; skipped: number };
}): boolean {
  if (suite.exitCode !== 0) return false;
  return suite.tests.passed + suite.tests.failed + suite.tests.skipped > 0;
}

function suiteVerdict(suite: { exitCode: number; tests: { passed: number; failed: number; skipped: number } }): string {
  if (suite.exitCode !== 0) return 'FAIL';
  return suitePassed(suite) ? 'PASS' : 'FAIL (no results)';
}

