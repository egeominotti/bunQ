import type { BackupResult } from './s3BackupConfig';

/** Bounded, label-free backup metrics exposed to Prometheus. */
export interface BackupMetrics {
  enabled: boolean;
  schedulerRunning: boolean;
  inProgress: boolean;
  intervalSeconds: number;
  retention: number;
  attemptsTotal: number;
  successesTotal: number;
  failuresTotal: number;
  overlapRejectionsTotal: number;
  consecutiveFailures: number;
  lastSuccessTimestampSeconds: number;
  lastFailureTimestampSeconds: number;
  lastDurationSeconds: number;
  lastSizeBytes: number;
}

export const EMPTY_BACKUP_METRICS: BackupMetrics = {
  enabled: false,
  schedulerRunning: false,
  inProgress: false,
  intervalSeconds: 0,
  retention: 0,
  attemptsTotal: 0,
  successesTotal: 0,
  failuresTotal: 0,
  overlapRejectionsTotal: 0,
  consecutiveFailures: 0,
  lastSuccessTimestampSeconds: 0,
  lastFailureTimestampSeconds: 0,
  lastDurationSeconds: 0,
  lastSizeBytes: 0,
};

/** State machine for backup attempts; all transitions preserve counter conservation. */
export class BackupTelemetry {
  private readonly metrics: BackupMetrics;

  constructor(enabled: boolean, intervalMs: number, retention: number) {
    this.metrics = {
      ...EMPTY_BACKUP_METRICS,
      enabled,
      intervalSeconds: intervalMs / 1000,
      retention,
    };
  }

  setSchedulerRunning(running: boolean): void {
    this.metrics.schedulerRunning = running;
  }

  tryStart(): boolean {
    if (this.metrics.inProgress) {
      this.metrics.overlapRejectionsTotal++;
      return false;
    }
    this.metrics.inProgress = true;
    this.metrics.attemptsTotal++;
    return true;
  }

  succeed(result: BackupResult, durationMs: number, nowMs = Date.now()): void {
    this.finish(durationMs);
    this.metrics.successesTotal++;
    this.metrics.consecutiveFailures = 0;
    this.metrics.lastSuccessTimestampSeconds = nowMs / 1000;
    this.metrics.lastSizeBytes = result.compressedSize ?? result.size ?? 0;
  }

  fail(durationMs: number, nowMs = Date.now()): void {
    this.finish(durationMs);
    this.metrics.failuresTotal++;
    this.metrics.consecutiveFailures++;
    this.metrics.lastFailureTimestampSeconds = nowMs / 1000;
  }

  snapshot(): BackupMetrics {
    return { ...this.metrics };
  }

  private finish(durationMs: number): void {
    if (!this.metrics.inProgress) {
      throw new Error('Cannot finish backup telemetry without an active attempt');
    }
    this.metrics.inProgress = false;
    this.metrics.lastDurationSeconds = Math.max(durationMs, 0) / 1000;
  }
}
