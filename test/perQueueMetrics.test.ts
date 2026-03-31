/**
 * Per-Queue Metrics Tests
 * Validates per-queue Prometheus metric labels
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { generatePrometheusMetrics } from '../src/application/metricsExporter';
import { WorkerManager } from '../src/application/workerManager';
import { WebhookManager } from '../src/application/webhookManager';

describe('Per-Queue Metrics', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should include per-queue waiting metrics in Prometheus output', async () => {
    await qm.push('emails', { data: { to: 'a@b.com' } });
    await qm.push('emails', { data: { to: 'c@d.com' } });
    await qm.push('payments', { data: { amount: 100 } });

    const stats = qm.getStats();
    const perQueue = qm.getPerQueueStats();
    const workerManager = new WorkerManager();
    const webhookManager = new WebhookManager();

    const output = generatePrometheusMetrics(stats, workerManager, webhookManager, perQueue);

    expect(output).toContain('bunqueue_queue_jobs_waiting{queue="emails"} 2');
    expect(output).toContain('bunqueue_queue_jobs_waiting{queue="payments"} 1');

    workerManager.stop();
  });

  test('should include per-queue active metrics', async () => {
    await qm.push('emails', { data: { to: 'a@b.com' } });
    await qm.push('payments', { data: { amount: 100 } });

    await qm.pull('emails');

    const stats = qm.getStats();
    const perQueue = qm.getPerQueueStats();
    const workerManager = new WorkerManager();
    const webhookManager = new WebhookManager();

    const output = generatePrometheusMetrics(stats, workerManager, webhookManager, perQueue);

    expect(output).toContain('bunqueue_queue_jobs_active{queue="emails"} 1');
    expect(output).toContain('bunqueue_queue_jobs_waiting{queue="payments"} 1');

    workerManager.stop();
  });

  test('should include per-queue DLQ metrics', async () => {
    await qm.push('emails', { data: { to: 'a@b.com' }, maxAttempts: 1 });
    const job = await qm.pull('emails');
    await qm.fail(job!.id, 'permanent failure');

    const stats = qm.getStats();
    const perQueue = qm.getPerQueueStats();
    const workerManager = new WorkerManager();
    const webhookManager = new WebhookManager();

    const output = generatePrometheusMetrics(stats, workerManager, webhookManager, perQueue);

    expect(output).toContain('bunqueue_queue_jobs_dlq{queue="emails"} 1');

    workerManager.stop();
  });

  test('should handle no queues gracefully', () => {
    const stats = qm.getStats();
    const perQueue = qm.getPerQueueStats();
    const workerManager = new WorkerManager();
    const webhookManager = new WebhookManager();

    const output = generatePrometheusMetrics(stats, workerManager, webhookManager, perQueue);

    // Should still have global metrics
    expect(output).toContain('bunqueue_jobs_waiting');
    // Should NOT have per-queue metrics when no queues exist
    expect(output).not.toContain('bunqueue_queue_jobs_waiting{queue=');

    workerManager.stop();
  });

  test('should include delayed per-queue metrics', async () => {
    await qm.push('emails', { data: { to: 'a@b.com' }, delay: 60000 });
    await qm.push('emails', { data: { to: 'c@d.com' } });

    const stats = qm.getStats();
    const perQueue = qm.getPerQueueStats();
    const workerManager = new WorkerManager();
    const webhookManager = new WebhookManager();

    const output = generatePrometheusMetrics(stats, workerManager, webhookManager, perQueue);

    expect(output).toContain('bunqueue_queue_jobs_delayed{queue="emails"} 1');
    expect(output).toContain('bunqueue_queue_jobs_waiting{queue="emails"} 1');

    workerManager.stop();
  });

  test('should return per-queue stats via getPerQueueStats method', async () => {
    await qm.push('emails', { data: { to: 'a@b.com' } });
    await qm.push('emails', { data: { to: 'c@d.com' } });
    await qm.push('payments', { data: { amount: 100 } });

    const perQueue = qm.getPerQueueStats();

    expect(perQueue.size).toBe(2);

    const emailStats = perQueue.get('emails');
    expect(emailStats).toBeDefined();
    expect(emailStats!.waiting).toBe(2);
    expect(emailStats!.delayed).toBe(0);
    expect(emailStats!.active).toBe(0);
    expect(emailStats!.dlq).toBe(0);

    const paymentStats = perQueue.get('payments');
    expect(paymentStats).toBeDefined();
    expect(paymentStats!.waiting).toBe(1);
  });

  test('should include per-queue metrics in full getPrometheusMetrics output', async () => {
    await qm.push('emails', { data: { to: 'a@b.com' } });
    await qm.push('payments', { data: { amount: 100 } });

    const output = qm.getPrometheusMetrics();

    // Global metrics should be present
    expect(output).toContain('bunqueue_jobs_waiting 2');

    // Per-queue metrics should also be present
    expect(output).toContain('bunqueue_queue_jobs_waiting{queue="emails"} 1');
    expect(output).toContain('bunqueue_queue_jobs_waiting{queue="payments"} 1');
  });
});
