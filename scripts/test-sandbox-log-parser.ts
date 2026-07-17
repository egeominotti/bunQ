import type { FileMetric, TestMetric } from './test-sandbox-telemetry';

function parseDuration(value: string, unit: string): number {
  const amount = Number(value);
  if (unit === 's') return amount * 1000;
  if (unit === 'us' || unit === 'µs') return amount / 1000;
  return amount;
}

export function parseTests(log: string, suite: string): TestMetric[] {
  const tests: TestMetric[] = [];
  let file = 'unknown';
  for (const line of log.split('\n')) {
    const fileMatch = line.match(/^(test\/[^:]+\.ts):$/);
    if (fileMatch) file = fileMatch[1];
    const testMatch = line.match(/^\((pass|fail|skip)\) (.*?)(?: \[([\d.]+)(ms|s|us|µs)\])?$/);
    if (testMatch) {
      tests.push({
        file,
        name: testMatch[2],
        status: testMatch[1] as TestMetric['status'],
        ...(testMatch[3] && { durationMs: parseDuration(testMatch[3], testMatch[4]) }),
      });
      continue;
    }
    const goMatch = line.match(/^--- (PASS|FAIL|SKIP): (.+?)(?: \(([\d.]+)s\))?$/);
    if (goMatch) {
      tests.push({
        file: suite,
        name: goMatch[2],
        status: goMatch[1].toLowerCase() as TestMetric['status'],
        ...(goMatch[3] && { durationMs: parseDuration(goMatch[3], 's') }),
      });
      continue;
    }
    const harnessMatch = line.match(/^(PASS|FAIL|SKIP) (.+?)(?: \(([\d.]+)ms\))?$/);
    if (harnessMatch) {
      tests.push({
        file: suite,
        name: harnessMatch[2],
        status: harnessMatch[1].toLowerCase() as TestMetric['status'],
        ...(harnessMatch[3] && { durationMs: parseDuration(harnessMatch[3], 'ms') }),
      });
    }
  }
  return tests;
}

export function parseFiles(log: string): FileMetric[] {
  const files: FileMetric[] = [];
  for (const line of log.split('\n')) {
    if (!line.startsWith('TEST_FILE_RESULT ')) continue;
    try {
      files.push(JSON.parse(line.slice('TEST_FILE_RESULT '.length)) as FileMetric);
    } catch {
      // A malformed marker remains visible in the complete suite log.
    }
  }
  return files;
}

export function parseCount(log: string, label: string): number {
  const matches = [...log.matchAll(new RegExp(`^\\s*(\\d+) ${label}$`, 'gm'))];
  return Number(matches.at(-1)?.[1] ?? 0);
}

function sumMatches(log: string, pattern: RegExp, group: number): number {
  return [...log.matchAll(pattern)].reduce((total, match) => total + Number(match[group] ?? 0), 0);
}

function countMatches(log: string, pattern: RegExp): number {
  return [...log.matchAll(pattern)].length;
}

export function parseSdkCounts(
  suite: string,
  log: string
): { passed: number; failed: number; skipped: number } | null {
  let passed = 0;
  let failed = 0;
  let skipped = 0;
  if (suite === 'typescript') {
    passed = sumMatches(log, /^\s*(\d+) pass$/gm, 1);
    failed = sumMatches(log, /^\s*(\d+) fail$/gm, 1);
    skipped = sumMatches(log, /^\s*(\d+) skip$/gm, 1);
  } else if (suite === 'python' || suite === 'php') {
    // Their native harnesses use the common "passed/total passed" marker below.
  } else if (suite === 'go') {
    passed = countMatches(log, /^--- PASS: /gm);
    failed = countMatches(log, /^--- FAIL: /gm);
    skipped = countMatches(log, /^--- SKIP: /gm);
  } else if (suite === 'rust') {
    for (const match of log.matchAll(
      /^test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;/gm
    )) {
      passed += Number(match[1]);
      failed += Number(match[2]);
      skipped += Number(match[3]);
    }
  } else if (suite === 'elixir') {
    const results = [...log.matchAll(/^Result:\s*(.+)$/gm)];
    if (results.length > 0) {
      for (const result of results) {
        for (const count of result[1].matchAll(/(\d+) (passed|failed|skipped|excluded)/g)) {
          const value = Number(count[1]);
          if (count[2] === 'passed') passed += value;
          else if (count[2] === 'failed') failed += value;
          else skipped += value;
        }
      }
    } else {
      for (const match of log.matchAll(/^(\d+) tests, (\d+) failures(?:, (\d+) excluded)?$/gm)) {
        passed += Number(match[1]) - Number(match[2]) - Number(match[3] ?? 0);
        failed += Number(match[2]);
        skipped += Number(match[3] ?? 0);
      }
    }
  } else {
    return null;
  }
  for (const match of log.matchAll(/^(\d+)\/(\d+) passed(?: .*)?$/gm)) {
    passed += Number(match[1]);
    failed += Number(match[2]) - Number(match[1]);
  }
  for (const match of log.matchAll(/^(\d+)\/(\d+) checks passed$/gm)) {
    passed += Number(match[1]);
    failed += Number(match[2]) - Number(match[1]);
  }
  return { passed, failed, skipped };
}
