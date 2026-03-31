/**
 * Histogram Tests
 * Latency tracking and Prometheus export
 */

import { describe, test, expect } from 'bun:test';
import { Histogram } from '../src/shared/histogram';

describe('Histogram', () => {
  test('should start with zero count and sum', () => {
    const h = new Histogram();
    expect(h.getCount()).toBe(0);
    expect(h.getSum()).toBe(0);
  });

  test('should track observations', () => {
    const h = new Histogram();
    h.observe(10);
    h.observe(20);
    h.observe(30);

    expect(h.getCount()).toBe(3);
    expect(h.getSum()).toBe(60);
  });

  test('should calculate percentiles', () => {
    const h = new Histogram([1, 5, 10, 50, 100]);

    // Add values that fall into specific buckets
    for (let i = 0; i < 50; i++) h.observe(3);   // <= 5 bucket
    for (let i = 0; i < 40; i++) h.observe(8);   // <= 10 bucket
    for (let i = 0; i < 10; i++) h.observe(80);  // <= 100 bucket

    // p50 should be in the 5ms bucket (50th percentile of 100 observations)
    expect(h.percentile(50)).toBe(5);
    // p95 should be in a higher bucket
    expect(h.percentile(95)).toBeGreaterThanOrEqual(10);
  });

  test('should return 0 percentile for empty histogram', () => {
    const h = new Histogram();
    expect(h.percentile(50)).toBe(0);
    expect(h.percentile(99)).toBe(0);
  });

  test('should generate valid Prometheus format', () => {
    const h = new Histogram([1, 5, 10]);
    h.observe(3);
    h.observe(7);
    h.observe(15);

    const output = h.toPrometheus('test_latency', 'Test latency');

    expect(output).toContain('# HELP test_latency Test latency');
    expect(output).toContain('# TYPE test_latency histogram');
    expect(output).toContain('test_latency_bucket{le="1"} ');
    expect(output).toContain('test_latency_bucket{le="5"} ');
    expect(output).toContain('test_latency_bucket{le="10"} ');
    expect(output).toContain('test_latency_bucket{le="+Inf"} 3');
    expect(output).toContain('test_latency_sum 25');
    expect(output).toContain('test_latency_count 3');
  });

  test('should have cumulative bucket counts', () => {
    const h = new Histogram([10, 50, 100]);
    h.observe(5);   // goes into le=10, le=50, le=100, +Inf
    h.observe(30);  // goes into le=50, le=100, +Inf
    h.observe(80);  // goes into le=100, +Inf
    h.observe(200); // goes into +Inf only

    const output = h.toPrometheus('test', 'test');

    expect(output).toContain('test_bucket{le="10"} 1');
    expect(output).toContain('test_bucket{le="50"} 2');
    expect(output).toContain('test_bucket{le="100"} 3');
    expect(output).toContain('test_bucket{le="+Inf"} 4');
  });

  test('should reset correctly', () => {
    const h = new Histogram();
    h.observe(10);
    h.observe(20);

    h.reset();

    expect(h.getCount()).toBe(0);
    expect(h.getSum()).toBe(0);
  });

  test('should handle very small values', () => {
    const h = new Histogram([0.1, 0.5, 1]);
    h.observe(0.05);

    expect(h.getCount()).toBe(1);
    expect(h.getSum()).toBeCloseTo(0.05);
  });

  test('should handle custom buckets', () => {
    const h = new Histogram([100, 200, 500, 1000]);
    h.observe(150);
    h.observe(350);
    h.observe(750);

    const output = h.toPrometheus('custom', 'Custom metric');
    expect(output).toContain('custom_bucket{le="100"} 0');
    expect(output).toContain('custom_bucket{le="200"} 1');
    expect(output).toContain('custom_bucket{le="500"} 2');
    expect(output).toContain('custom_bucket{le="1000"} 3');
  });
});
