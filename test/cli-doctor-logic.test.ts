import { describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import {
  evaluateDoctor,
  formatDoctorText,
  type DoctorInput,
  type HealthData,
} from '../src/cli/commands/doctor';
import { VERSION } from '../src/shared/version';

function healthy(overrides: Partial<HealthData> = {}): DoctorInput {
  return {
    kind: 'ok',
    endpoint: '127.0.0.1:6790',
    health: {
      version: VERSION,
      status: 'healthy',
      uptime: 0,
      queues: { waiting: 0, active: 0, delayed: 0, completed: 0, dlq: 0 },
      connections: { tcp: 0, ws: 0, sse: 0 },
      memory: { heapUsed: 0, heapTotal: 0, rss: 0 },
      ...overrides,
    },
  };
}

describe('doctor pure evaluation', () => {
  test('healthy boundary values pass without issues', () => {
    const report = evaluateDoctor(
      healthy({
        uptime: Number.MAX_SAFE_INTEGER,
        memory: { heapUsed: 512, heapTotal: 512, rss: 512 },
      })
    );
    expect(report.issues).toBe(0);
    expect(report.exitCode).toBe(0);
    expect(report.fatal).toBe(false);
    expect(formatDoctorText(report)).toContain('All checks passed');
  });

  test('every independent issue contributes exactly once', () => {
    const report = evaluateDoctor(
      healthy({
        version: '0.0.0-other',
        status: 'degraded',
        queues: { dlq: 1 },
        memory: { rss: 513 },
      })
    );
    expect(report.issues).toBe(4);
    expect(report.exitCode).toBe(1);
    expect(formatDoctorText(report)).toContain('4 issues found');
  });

  test('network and HTTP errors are fatal deterministic reports', () => {
    const cases: DoctorInput[] = [
      { kind: 'network-error', endpoint: 'host:1', message: '😀 disconnected' },
      { kind: 'http-error', endpoint: 'host:2', status: 503 },
    ];
    for (const input of cases) {
      const first = evaluateDoctor(input);
      expect(evaluateDoctor(input)).toEqual(first);
      expect(first).toMatchObject({ reachable: false, issues: 1, fatal: true, exitCode: 1 });
      expect(formatDoctorText(first)).toContain('Cannot continue');
    }
  });

  test('generated health data obeys the issue-count formula', () => {
    fc.assert(
      fc.property(
        fc.boolean(),
        fc.boolean(),
        fc.nat({ max: 1_000_000 }),
        fc.nat({ max: 1_024 }),
        (versionMatches, statusHealthy, dlq, rss) => {
          const report = evaluateDoctor(
            healthy({
              version: versionMatches ? VERSION : `${VERSION}-other`,
              status: statusHealthy ? 'healthy' : 'degraded',
              queues: { dlq },
              memory: { rss },
            })
          );
          const expected =
            Number(!versionMatches) + Number(!statusHealthy) + Number(dlq > 0) + Number(rss > 512);
          expect(report.issues).toBe(expected);
          expect(report.exitCode).toBe(expected === 0 ? 0 : 1);
        }
      ),
      { numRuns: 5_000 }
    );
  });
});
