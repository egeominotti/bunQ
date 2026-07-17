import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { writeRunReport, writeSuiteArtifacts } from '../scripts/test-sandbox-report';
import { createSuiteTelemetry, type ResourceSample } from '../scripts/test-sandbox-telemetry';

const directories: string[] = [];

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((path) => rm(path, { recursive: true, force: true }))
  );
});

function sample(timestamp: string, memoryUsedBytes: number): ResourceSample {
  return {
    timestamp,
    cpuPercent: 50,
    memoryUsedBytes,
    memoryLimitBytes: 256 * 1024 ** 2,
    memoryPercent: (memoryUsedBytes / (256 * 1024 ** 2)) * 100,
    pids: 4,
    blockReadBytes: 1024,
    blockWriteBytes: 2048,
    networkRxBytes: 0,
    networkTxBytes: 0,
  };
}

describe('test sandbox telemetry', () => {
  test('parses test/file metrics and reports resource anomalies', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'bunqueue-telemetry-'));
    directories.push(directory);
    const logPath = join(directory, 'unit.log');
    await Bun.write(
      logPath,
      [
        'test/example.test.ts:',
        '(pass) example > fast [10ms]',
        '(fail) example > slow [2s]',
        ' 1 pass',
        ' 0 skip',
        ' 1 fail',
        'TEST_FILE_RESULT {"file":"test-example.ts","passed":1,"failed":1,"durationMs":2010}',
      ].join('\n')
    );
    const telemetry = await createSuiteTelemetry({
      suite: 'unit',
      command: ['bun', 'test'],
      container: 'unit-test',
      logPath,
      startedAt: '2026-01-01T00:00:00.000Z',
      finishedAt: '2026-01-01T00:02:00.000Z',
      exitCode: 1,
      oomKilled: false,
      samples: [
        sample('2026-01-01T00:00:00.000Z', 32 * 1024 ** 2),
        sample('2026-01-01T00:01:00.000Z', 128 * 1024 ** 2),
        sample('2026-01-01T00:02:00.000Z', 224 * 1024 ** 2),
        { ...sample('2026-01-01T00:02:01.000Z', 0), pids: 0 },
      ],
    });

    expect(telemetry.tests).toMatchObject({ passed: 1, failed: 1, skipped: 0 });
    expect(telemetry.tests.cases.map((item) => item.durationMs)).toEqual([10, 2000]);
    expect(telemetry.files).toEqual([
      { file: 'test-example.ts', passed: 1, failed: 1, durationMs: 2010 },
    ]);
    expect(telemetry.resources.memoryPeakPercent).toBeCloseTo(87.5);
    expect(telemetry.resources.memoryEndBytes).toBe(224 * 1024 ** 2);
    expect(telemetry.resources.memorySlopeBytesPerMinute).toBeGreaterThan(0);
    expect(telemetry.anomalies).toContain('non-zero exit code: 1');
    expect(telemetry.anomalies).toContain('peak memory reached at least 80%');
    expect(telemetry.anomalies).toContain(
      'memory growth signal: end minus start exceeds 128 MiB and 50%'
    );
  });

  test('writes NDJSON, suite JSON, aggregate JSON, and Markdown', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'bunqueue-report-'));
    directories.push(directory);
    const logPath = join(directory, 'unit.log');
    await Bun.write(logPath, ' 1 pass\n 0 skip\n 0 fail\n');
    const samples = [sample('2026-01-01T00:00:00.000Z', 32 * 1024 ** 2)];
    const telemetry = await createSuiteTelemetry({
      suite: 'unit',
      command: ['bun', 'test'],
      container: 'unit-test',
      logPath,
      startedAt: '2026-01-01T00:00:00.000Z',
      finishedAt: '2026-01-01T00:00:01.000Z',
      exitCode: 0,
      oomKilled: false,
      samples,
    });

    await writeSuiteArtifacts(directory, telemetry, samples);
    const report = await writeRunReport({
      logDir: directory,
      mode: 'parallel',
      durationMs: 1000,
      suites: [telemetry],
      baselinePath: null,
    });

    expect(report.passed).toBe(true);
    expect(await Bun.file(join(directory, 'unit.metrics.ndjson')).text()).toContain(
      '"memoryUsedBytes":33554432'
    );
    expect(await Bun.file(join(directory, 'summary.json')).json()).toMatchObject({
      schemaVersion: 1,
      passed: true,
    });
    const markdown = await Bun.file(join(directory, 'summary.md')).text();
    expect(markdown).toContain('# Test sandbox telemetry');
    expect(markdown).toContain('## Resource detail');
    expect(markdown).toContain('RAM start / p95 / end / peak');
    expect(markdown).toContain('Block read / write');
    expect(markdown).toContain('Tests/s');
  });

  test('counts ExUnit result summaries alongside SDK conformance checks', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'bunqueue-exunit-telemetry-'));
    directories.push(directory);
    const logPath = join(directory, 'elixir.log');
    await Bun.write(logPath, 'Result: 24 passed\n\n17/17 checks passed\n');
    const telemetry = await createSuiteTelemetry({
      suite: 'elixir',
      command: ['mix', 'test'],
      container: 'elixir-test',
      logPath,
      startedAt: '2026-01-01T00:00:00.000Z',
      finishedAt: '2026-01-01T00:00:01.000Z',
      exitCode: 0,
      oomKilled: false,
      samples: [sample('2026-01-01T00:00:00.000Z', 32 * 1024 ** 2)],
    });

    expect(telemetry.tests).toMatchObject({ passed: 41, failed: 0, skipped: 0 });
  });

  test('prefers ExUnit result summaries without double counting legacy output', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'bunqueue-exunit-result-'));
    directories.push(directory);
    const logPath = join(directory, 'elixir.log');
    await Bun.write(
      logPath,
      '24 tests, 2 failures, 1 excluded\nResult: 21 passed, 2 failed, 1 skipped\n'
    );
    const telemetry = await createSuiteTelemetry({
      suite: 'elixir',
      command: ['mix', 'test'],
      container: 'elixir-test',
      logPath,
      startedAt: '2026-01-01T00:00:00.000Z',
      finishedAt: '2026-01-01T00:00:01.000Z',
      exitCode: 1,
      oomKilled: false,
      samples: [sample('2026-01-01T00:00:00.000Z', 32 * 1024 ** 2)],
    });

    expect(telemetry.tests).toMatchObject({ passed: 21, failed: 2, skipped: 1 });
  });
});
