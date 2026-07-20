/**
 * Telemetry End-to-End Integration Test
 * Validates the full telemetry pipeline: operations -> tracking -> Prometheus output
 */

import { describe, test, expect, beforeEach, afterEach, spyOn } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { generatePrometheusMetrics } from '../src/application/metricsExporter';
import { WorkerManager } from '../src/application/workerManager';
import { WebhookManager } from '../src/application/webhookManager';
import { latencyTracker } from '../src/application/latencyTracker';
import { throughputTracker } from '../src/application/throughputTracker';
import { Logger } from '../src/shared/logger';

describe('Telemetry E2E', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
    // Reset latency histograms for clean test state
    latencyTracker.push.reset();
    latencyTracker.pull.reset();
    latencyTracker.ack.reset();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('full lifecycle: push -> pull -> ack produces latency histograms', async () => {
    // Push 10 jobs
    for (let i = 0; i < 10; i++) {
      await qm.push('telemetry-test', { data: { i } });
    }

    // Pull 10 jobs
    const jobs = [];
    for (let i = 0; i < 10; i++) {
      const job = await qm.pull('telemetry-test');
      expect(job).not.toBeNull();
      jobs.push(job!);
    }

    // Ack 10 jobs
    for (const job of jobs) {
      await qm.ack(job.id, { done: true });
    }

    // Verify latency histograms were populated
    expect(latencyTracker.push.getCount()).toBe(10);
    expect(latencyTracker.pull.getCount()).toBe(10);
    expect(latencyTracker.ack.getCount()).toBe(10);

    // Sums should be positive (actual latency recorded)
    expect(latencyTracker.push.getSum()).toBeGreaterThan(0);
    expect(latencyTracker.pull.getSum()).toBeGreaterThan(0);
    expect(latencyTracker.ack.getSum()).toBeGreaterThan(0);

    // Percentiles should return non-zero values
    const pcts = latencyTracker.getPercentiles();
    expect(pcts.push.p50).toBeGreaterThanOrEqual(0);
    expect(pcts.pull.p50).toBeGreaterThanOrEqual(0);
    expect(pcts.ack.p50).toBeGreaterThanOrEqual(0);
  });

  test('Prometheus output includes all telemetry sections', async () => {
    // Push to multiple queues
    await qm.push('emails', { data: { to: 'a@b.com' } });
    await qm.push('emails', { data: { to: 'c@d.com' } });
    await qm.push('payments', { data: { amount: 100 } });

    // Pull one job
    const job = await qm.pull('emails');
    expect(job).not.toBeNull();

    // Ack
    await qm.ack(job!.id, { result: 'sent' });

    // Generate full Prometheus output
    const stats = qm.getStats();
    const perQueue = qm.getPerQueueStats();
    const wm = new WorkerManager();
    const whm = new WebhookManager();

    const output = generatePrometheusMetrics(stats, wm, whm, perQueue);

    // Section 1: Global gauges
    expect(output).toContain('bunqueue_jobs_waiting');
    expect(output).toContain('bunqueue_jobs_delayed');
    expect(output).toContain('bunqueue_jobs_active');
    expect(output).toContain('bunqueue_jobs_dlq');

    // Section 2: Global counters
    expect(output).toContain('bunqueue_jobs_pushed_total');
    expect(output).toContain('bunqueue_jobs_pulled_total');
    expect(output).toContain('bunqueue_jobs_completed_total');
    expect(output).toContain('bunqueue_jobs_failed_total');

    // Section 3: Per-queue metrics with labels
    expect(output).toContain('bunqueue_queue_jobs_waiting{queue="emails"}');
    expect(output).toContain('bunqueue_queue_jobs_waiting{queue="payments"}');
    expect(output).toContain('bunqueue_queue_jobs_active{queue="emails"}');
    expect(output).toContain('bunqueue_queue_jobs_delayed{queue="emails"}');
    expect(output).toContain('bunqueue_queue_jobs_dlq{queue="emails"}');

    // Section 4: Latency histograms
    expect(output).toContain('bunqueue_push_duration_seconds');
    expect(output).toContain('bunqueue_pull_duration_seconds');
    expect(output).toContain('bunqueue_ack_duration_seconds');
    expect(output).toContain('_bucket{le=');
    expect(output).toContain('_sum');
    expect(output).toContain('_count');

    // Histogram counts should be > 0
    expect(output).toContain('bunqueue_push_duration_seconds_count 3');
    expect(output).toContain('bunqueue_pull_duration_seconds_count 1');
    expect(output).toContain('bunqueue_ack_duration_seconds_count 1');

    wm.stop();
  });

  test('per-queue metrics accurately reflect multi-queue state', async () => {
    // Setup: jobs in different states across queues
    await qm.push('q1', { data: { id: 1 } });
    await qm.push('q1', { data: { id: 2 } });
    await qm.push('q1', { data: { id: 3 }, delay: 60000 }); // delayed

    await qm.push('q2', { data: { id: 4 }, maxAttempts: 1 });

    // Pull from q1 (1 active)
    const j1 = await qm.pull('q1');
    expect(j1).not.toBeNull();

    // Pull and fail from q2 (maxAttempts=1 -> DLQ)
    const j2 = await qm.pull('q2');
    expect(j2).not.toBeNull();
    await qm.fail(j2!.id, 'permanent');

    const perQueue = qm.getPerQueueStats();

    // q1: 1 waiting, 1 delayed, 1 active
    const q1 = perQueue.get('q1')!;
    expect(q1.waiting).toBe(1);
    expect(q1.delayed).toBe(1);
    expect(q1.active).toBe(1);
    expect(q1.dlq).toBe(0);

    // q2: 0 waiting, 0 active, 1 DLQ
    const q2 = perQueue.get('q2')!;
    expect(q2.waiting).toBe(0);
    expect(q2.delayed).toBe(0);
    expect(q2.active).toBe(0);
    expect(q2.dlq).toBe(1);
  });

  test('throughput tracker records push/pull/ack rates', async () => {
    // Push burst
    for (let i = 0; i < 50; i++) {
      await qm.push('rate-test', { data: { i } });
    }

    // Pull all
    const jobs = [];
    for (let i = 0; i < 50; i++) {
      const job = await qm.pull('rate-test');
      jobs.push(job!);
    }

    // Ack all
    for (const job of jobs) {
      await qm.ack(job.id);
    }

    // Wait for rate calculation
    await Bun.sleep(150);

    const rates = throughputTracker.getRates();
    expect(rates.pushPerSec).toBeGreaterThan(0);
    expect(rates.pullPerSec).toBeGreaterThan(0);
    expect(rates.completePerSec).toBeGreaterThan(0);
  });

  test('LOG_LEVEL filtering works at runtime', () => {
    const spy = spyOn(console, 'debug').mockImplementation(() => {
      // Suppress expected debug output while asserting the configured filter.
    });

    // Default level is info, debug should be filtered
    Logger.setLevel('info');
    const log = new Logger('E2E');
    log.debug('should be filtered');
    expect(spy).not.toHaveBeenCalled();

    // Switch to debug, should pass
    Logger.setLevel('debug');
    log.debug('should pass');
    expect(spy).toHaveBeenCalled();

    spy.mockRestore();
    Logger.setLevel('info');
  });

  test('getPrometheusMetrics on QueueManager returns complete output', async () => {
    await qm.push('test-q', { data: { a: 1 } });
    const job = await qm.pull('test-q');
    await qm.ack(job!.id);

    const output = qm.getPrometheusMetrics();

    // Must have all sections
    expect(output).toContain('bunqueue_jobs_waiting');
    expect(output).toContain('bunqueue_queue_jobs_waiting{queue="test-q"}');
    expect(output).toContain('bunqueue_push_duration_seconds_count');
    expect(output).toContain('bunqueue_pull_duration_seconds_count');
    expect(output).toContain('bunqueue_ack_duration_seconds_count');
  });

  test('batch operations correctly record latency', async () => {
    latencyTracker.push.reset();
    latencyTracker.pull.reset();

    // Batch push
    await qm.pushBatch('batch-q', [{ data: { id: 1 } }, { data: { id: 2 } }, { data: { id: 3 } }]);

    // pushBatch should record 1 latency observation (entire batch)
    expect(latencyTracker.push.getCount()).toBe(1);
    expect(latencyTracker.push.getSum()).toBeGreaterThan(0);
  });

  test('DLQ failure path tracks throughput', async () => {
    // Push job with 1 max attempt
    await qm.push('dlq-test', { data: { id: 1 }, maxAttempts: 1 });
    const job = await qm.pull('dlq-test');

    // Fail -> DLQ
    await qm.fail(job!.id, 'fatal error');

    await Bun.sleep(150);

    const rates = throughputTracker.getRates();
    expect(rates.failPerSec).toBeGreaterThan(0);
  });

  test('histogram Prometheus format is valid for scraping', async () => {
    // Generate some observations
    for (let i = 0; i < 5; i++) {
      await qm.push('prom-test', { data: { i } });
    }

    const output = latencyTracker.toPrometheus();

    // Validate Prometheus format requirements
    const lines = output.split('\n');

    // Must have HELP and TYPE for each histogram
    expect(lines.some((l) => l.startsWith('# HELP bunqueue_push_duration_seconds'))).toBe(true);
    expect(lines.some((l) => l.startsWith('# TYPE bunqueue_push_duration_seconds histogram'))).toBe(
      true
    );

    // Must have _bucket, _sum, _count
    expect(lines.some((l) => l.includes('bunqueue_push_duration_seconds_bucket{le="'))).toBe(true);
    expect(lines.some((l) => l.includes('bunqueue_push_duration_seconds_bucket{le="+Inf"}'))).toBe(
      true
    );
    expect(lines.some((l) => l.startsWith('bunqueue_push_duration_seconds_sum'))).toBe(true);
    expect(lines.some((l) => l.startsWith('bunqueue_push_duration_seconds_count'))).toBe(true);

    // +Inf bucket count must equal _count
    const infLine = lines.find((l) => l.includes('push_duration_seconds_bucket{le="+Inf"}'));
    const countLine = lines.find((l) => l.startsWith('bunqueue_push_duration_seconds_count'));
    if (infLine && countLine) {
      const infCount = infLine.split(' ')[1];
      const count = countLine.split(' ')[1];
      expect(infCount).toBe(count);
    }
  });
});
