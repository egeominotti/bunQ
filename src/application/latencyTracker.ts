/**
 * Latency Tracker
 * Centralized latency tracking for all operations
 */

import { Histogram } from '../shared/histogram';

/** Operation latency histograms */
export class LatencyTracker {
  readonly push = new Histogram();
  readonly pull = new Histogram();
  readonly ack = new Histogram();

  /** Generate Prometheus output for all histograms */
  toPrometheus(): string {
    return [
      this.push.toPrometheus(
        'bunqueue_push_duration_seconds',
        'Push operation latency in seconds',
        0.001
      ),
      '',
      this.pull.toPrometheus(
        'bunqueue_pull_duration_seconds',
        'Pull operation latency in seconds',
        0.001
      ),
      '',
      this.ack.toPrometheus(
        'bunqueue_ack_duration_seconds',
        'Ack operation latency in seconds',
        0.001
      ),
    ].join('\n');
  }

  /** Get average latencies */
  getAverages(): { pushMs: number; pullMs: number; ackMs: number } {
    return {
      pushMs: this.push.getCount() > 0 ? this.push.getSum() / this.push.getCount() : 0,
      pullMs: this.pull.getCount() > 0 ? this.pull.getSum() / this.pull.getCount() : 0,
      ackMs: this.ack.getCount() > 0 ? this.ack.getSum() / this.ack.getCount() : 0,
    };
  }

  /** Get percentiles for all operations */
  getPercentiles(): {
    push: { p50: number; p95: number; p99: number };
    pull: { p50: number; p95: number; p99: number };
    ack: { p50: number; p95: number; p99: number };
  } {
    return {
      push: {
        p50: this.push.percentile(50),
        p95: this.push.percentile(95),
        p99: this.push.percentile(99),
      },
      pull: {
        p50: this.pull.percentile(50),
        p95: this.pull.percentile(95),
        p99: this.pull.percentile(99),
      },
      ack: {
        p50: this.ack.percentile(50),
        p95: this.ack.percentile(95),
        p99: this.ack.percentile(99),
      },
    };
  }
}

/** Global singleton */
export const latencyTracker = new LatencyTracker();
