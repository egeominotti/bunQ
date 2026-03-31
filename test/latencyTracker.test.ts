/**
 * Latency Tracker Tests
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { LatencyTracker } from '../src/application/latencyTracker';

describe('LatencyTracker', () => {
  let tracker: LatencyTracker;

  beforeEach(() => {
    tracker = new LatencyTracker();
  });

  test('should track push latency', () => {
    tracker.push.observe(5);
    tracker.push.observe(10);

    const avgs = tracker.getAverages();
    expect(avgs.pushMs).toBe(7.5);
  });

  test('should track pull latency', () => {
    tracker.pull.observe(2);
    tracker.pull.observe(4);

    const avgs = tracker.getAverages();
    expect(avgs.pullMs).toBe(3);
  });

  test('should track ack latency', () => {
    tracker.ack.observe(1);
    tracker.ack.observe(3);

    const avgs = tracker.getAverages();
    expect(avgs.ackMs).toBe(2);
  });

  test('should return zero averages when empty', () => {
    const avgs = tracker.getAverages();
    expect(avgs.pushMs).toBe(0);
    expect(avgs.pullMs).toBe(0);
    expect(avgs.ackMs).toBe(0);
  });

  test('should provide percentiles', () => {
    for (let i = 0; i < 100; i++) {
      tracker.push.observe(i * 0.1);
    }

    const pcts = tracker.getPercentiles();
    expect(pcts.push.p50).toBeGreaterThan(0);
    expect(pcts.push.p95).toBeGreaterThan(pcts.push.p50);
    expect(pcts.push.p99).toBeGreaterThanOrEqual(pcts.push.p95);
  });

  test('should generate Prometheus output', () => {
    tracker.push.observe(5);
    tracker.pull.observe(10);
    tracker.ack.observe(1);

    const output = tracker.toPrometheus();

    expect(output).toContain('bunqueue_push_duration_ms');
    expect(output).toContain('bunqueue_pull_duration_ms');
    expect(output).toContain('bunqueue_ack_duration_ms');
    expect(output).toContain('_bucket{le=');
    expect(output).toContain('_sum');
    expect(output).toContain('_count');
  });

  test('should generate valid histogram format per operation', () => {
    tracker.push.observe(1);

    const output = tracker.toPrometheus();
    expect(output).toContain('# HELP bunqueue_push_duration_ms');
    expect(output).toContain('# TYPE bunqueue_push_duration_ms histogram');
    expect(output).toContain('bunqueue_push_duration_ms_bucket{le="+Inf"} 1');
    expect(output).toContain('bunqueue_push_duration_ms_sum 1');
    expect(output).toContain('bunqueue_push_duration_ms_count 1');
  });
});
