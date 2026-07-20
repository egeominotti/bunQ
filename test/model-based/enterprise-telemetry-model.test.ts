import { describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { selectPrometheusQueues } from '../../src/application/prometheusOperationalMetrics';
import { BackupTelemetry } from '../../src/infrastructure/backup/backupTelemetry';

type BackupAction =
  | { type: 'start' }
  | {
      type: 'success';
      durationMs: number;
      size: number;
      compressedSize: number;
      nowMs: number;
    }
  | { type: 'failure'; durationMs: number; nowMs: number }
  | { type: 'scheduler'; running: boolean };

const backupAction = fc.oneof<BackupAction>(
  fc.record({ type: fc.constant('start') }),
  fc.record({
    type: fc.constant('success'),
    durationMs: fc.integer({ min: 0, max: 600_000 }),
    size: fc.integer({ min: 0, max: 100_000_000 }),
    compressedSize: fc.integer({ min: 0, max: 100_000_000 }),
    nowMs: fc.integer({ min: 1, max: 2_000_000_000 }),
  }),
  fc.record({
    type: fc.constant('failure'),
    durationMs: fc.integer({ min: 0, max: 600_000 }),
    nowMs: fc.integer({ min: 1, max: 2_000_000_000 }),
  }),
  fc.record({ type: fc.constant('scheduler'), running: fc.boolean() })
);

describe('enterprise telemetry state models', () => {
  test('backup attempt counters conserve every generated transition', () => {
    fc.assert(
      fc.property(fc.array(backupAction, { minLength: 1, maxLength: 100 }), (actions) => {
        const telemetry = new BackupTelemetry(true, 60_000, 7);
        let active = false;
        let attempts = 0;
        let successes = 0;
        let failures = 0;
        let overlaps = 0;
        let consecutiveFailures = 0;
        let schedulerRunning = false;
        let lastDurationSeconds = 0;
        let lastSizeBytes = 0;
        let lastSuccessTimestampSeconds = 0;
        let lastFailureTimestampSeconds = 0;

        for (const action of actions) {
          if (action.type === 'start') {
            const started = telemetry.tryStart();
            if (active) {
              overlaps++;
              expect(started).toBe(false);
            } else {
              active = true;
              attempts++;
              expect(started).toBe(true);
            }
          } else if (action.type === 'success' && active) {
            telemetry.succeed(
              {
                success: true,
                size: action.size,
                compressedSize: action.compressedSize,
              },
              action.durationMs,
              action.nowMs
            );
            active = false;
            successes++;
            consecutiveFailures = 0;
            lastDurationSeconds = action.durationMs / 1000;
            lastSizeBytes = action.compressedSize;
            lastSuccessTimestampSeconds = action.nowMs / 1000;
          } else if (action.type === 'failure' && active) {
            telemetry.fail(action.durationMs, action.nowMs);
            active = false;
            failures++;
            consecutiveFailures++;
            lastDurationSeconds = action.durationMs / 1000;
            lastFailureTimestampSeconds = action.nowMs / 1000;
          } else if (action.type === 'scheduler') {
            telemetry.setSchedulerRunning(action.running);
            schedulerRunning = action.running;
          }

          const metrics = telemetry.snapshot();
          expect(metrics.attemptsTotal).toBe(attempts);
          expect(metrics.successesTotal).toBe(successes);
          expect(metrics.failuresTotal).toBe(failures);
          expect(metrics.overlapRejectionsTotal).toBe(overlaps);
          expect(metrics.consecutiveFailures).toBe(consecutiveFailures);
          expect(metrics.inProgress).toBe(active);
          expect(metrics.schedulerRunning).toBe(schedulerRunning);
          expect(metrics.lastDurationSeconds).toBe(lastDurationSeconds);
          expect(metrics.lastSizeBytes).toBe(lastSizeBytes);
          expect(metrics.lastSuccessTimestampSeconds).toBe(lastSuccessTimestampSeconds);
          expect(metrics.lastFailureTimestampSeconds).toBe(lastFailureTimestampSeconds);
          expect(metrics.attemptsTotal).toBe(
            metrics.successesTotal + metrics.failuresTotal + (metrics.inProgress ? 1 : 0)
          );
        }
      }),
      { numRuns: 500 }
    );
  });

  test('queue label selection is bounded and conserves exported plus omitted queues', () => {
    fc.assert(
      fc.property(
        fc.uniqueArray(fc.string({ minLength: 1, maxLength: 32 }), { maxLength: 250 }),
        fc.integer({ min: -20, max: 300 }),
        (queues, requestedLimit) => {
          const result = selectPrometheusQueues(queues, requestedLimit);
          const limit = Math.max(0, requestedLimit);
          expect([...result.selected]).toEqual(queues.slice(0, limit));
          expect(result.selected.size).toBeLessThanOrEqual(limit);
          expect(result.selected.size + result.omitted).toBe(queues.length);
          expect(result.omitted).toBeGreaterThanOrEqual(0);
        }
      ),
      { numRuns: 1_000 }
    );
  });
});
