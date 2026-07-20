/**
 * Metrics Exporter Tests
 * Prometheus metrics generation
 */

import { describe, test, expect } from 'bun:test';
import { generatePrometheusMetrics, type QueueStats } from '../src/application/metricsExporter';
import { WorkerManager } from '../src/application/workerManager';
import { WebhookManager } from '../src/application/webhookManager';

describe('Metrics Exporter', () => {
  const createStats = (overrides: Partial<QueueStats> = {}): QueueStats => ({
    waiting: 0,
    prioritized: 0,
    delayed: 0,
    active: 0,
    dlq: 0,
    completed: 0,
    totalPushed: BigInt(0),
    totalPulled: BigInt(0),
    totalCompleted: BigInt(0),
    totalFailed: BigInt(0),
    uptime: 0,
    cronJobs: 0,
    cronPending: 0,
    ...overrides,
  });

  describe('generatePrometheusMetrics', () => {
    test('should generate valid Prometheus format', () => {
      const stats = createStats({ waiting: 10, active: 5 });
      const workerManager = new WorkerManager();
      const webhookManager = new WebhookManager();

      const output = generatePrometheusMetrics(stats, workerManager, webhookManager);

      // Should contain HELP and TYPE comments
      expect(output).toContain('# HELP bunqueue_jobs_waiting');
      expect(output).toContain('# TYPE bunqueue_jobs_waiting gauge');
      expect(output).toContain('bunqueue_jobs_waiting 10');

      workerManager.stop();
    });

    test('should include all queue metrics', () => {
      const stats = createStats({
        waiting: 100,
        delayed: 50,
        active: 25,
        dlq: 5,
        completed: 1000,
      });
      const workerManager = new WorkerManager();
      const webhookManager = new WebhookManager();

      const output = generatePrometheusMetrics(stats, workerManager, webhookManager);

      expect(output).toContain('bunqueue_jobs_waiting 100');
      expect(output).toContain('bunqueue_jobs_delayed 50');
      expect(output).toContain('bunqueue_jobs_active 25');
      expect(output).toContain('bunqueue_jobs_dlq 5');
      expect(output).toContain('bunqueue_jobs_completed 1000');

      workerManager.stop();
    });

    test('should include counter metrics', () => {
      const stats = createStats({
        totalPushed: BigInt(10000),
        totalPulled: BigInt(9500),
        totalCompleted: BigInt(9000),
        totalFailed: BigInt(500),
      });
      const workerManager = new WorkerManager();
      const webhookManager = new WebhookManager();

      const output = generatePrometheusMetrics(stats, workerManager, webhookManager);

      expect(output).toContain('bunqueue_jobs_pushed_total 10000');
      expect(output).toContain('bunqueue_jobs_pulled_total 9500');
      expect(output).toContain('bunqueue_jobs_completed_total 9000');
      expect(output).toContain('bunqueue_jobs_failed_total 500');

      workerManager.stop();
    });

    test('should include uptime in seconds', () => {
      const stats = createStats({ uptime: 3600000 }); // 1 hour in ms
      const workerManager = new WorkerManager();
      const webhookManager = new WebhookManager();

      const output = generatePrometheusMetrics(stats, workerManager, webhookManager);

      expect(output).toContain('bunqueue_uptime_seconds 3600');

      workerManager.stop();
    });

    test('should include cron jobs metric', () => {
      const stats = createStats({ cronJobs: 5 });
      const workerManager = new WorkerManager();
      const webhookManager = new WebhookManager();

      const output = generatePrometheusMetrics(stats, workerManager, webhookManager);

      expect(output).toContain('bunqueue_cron_jobs_registered 5');

      workerManager.stop();
    });

    test('should include worker metrics', () => {
      const stats = createStats();
      const workerManager = new WorkerManager();
      const webhookManager = new WebhookManager();

      // Register some workers
      const w1 = workerManager.register('worker-1', ['q1']);
      workerManager.register('worker-2', ['q2']);
      workerManager.incrementActive(w1.id);
      workerManager.jobCompleted(w1.id);
      workerManager.jobFailed(w1.id);

      const output = generatePrometheusMetrics(stats, workerManager, webhookManager);

      expect(output).toContain('bunqueue_workers_registered 2');
      expect(output).toContain('bunqueue_workers_active 2');
      expect(output).toContain('bunqueue_worker_active_jobs 0');
      expect(output).toContain('bunqueue_worker_concurrency_slots 2');
      expect(output).toContain('bunqueue_workers_processed_total 1');
      expect(output).toContain('bunqueue_workers_failed_total 1');

      workerManager.stop();
    });

    test('worker capacity excludes stale registrations', () => {
      const workerManager = new WorkerManager();
      const webhookManager = new WebhookManager();
      const worker = workerManager.register('stale', ['q'], 8);
      worker.lastSeen = 0;

      const output = generatePrometheusMetrics(createStats(), workerManager, webhookManager);

      expect(output).toContain('bunqueue_workers_registered 1');
      expect(output).toContain('bunqueue_workers_active 0');
      expect(output).toContain('bunqueue_worker_concurrency_slots 0');

      workerManager.stop();
    });

    test('should include webhook metrics', () => {
      const stats = createStats();
      const workerManager = new WorkerManager();
      const webhookManager = new WebhookManager();

      // Add webhooks
      webhookManager.add('https://example.com/hook1', ['job.completed']);
      const w2 = webhookManager.add('https://example.com/hook2', ['job.failed']);
      webhookManager.add('https://example.com/hook3', ['job.pushed']);
      webhookManager.setEnabled(w2.id, false);

      const output = generatePrometheusMetrics(stats, workerManager, webhookManager);

      expect(output).toContain('bunqueue_webhooks_registered 3');
      expect(output).toContain('bunqueue_webhooks_enabled 2');

      workerManager.stop();
    });

    test('should have proper metric types', () => {
      const stats = createStats();
      const workerManager = new WorkerManager();
      const webhookManager = new WebhookManager();

      const output = generatePrometheusMetrics(stats, workerManager, webhookManager);

      // Gauges
      expect(output).toContain('# TYPE bunqueue_jobs_waiting gauge');
      expect(output).toContain('# TYPE bunqueue_jobs_delayed gauge');
      expect(output).toContain('# TYPE bunqueue_jobs_active gauge');

      // Counters
      expect(output).toContain('# TYPE bunqueue_jobs_pushed_total counter');
      expect(output).toContain('# TYPE bunqueue_jobs_pulled_total counter');

      workerManager.stop();
    });

    test('should include storage and process gauges', () => {
      const workerManager = new WorkerManager();
      const webhookManager = new WebhookManager();
      const output = generatePrometheusMetrics(
        createStats(),
        workerManager,
        webhookManager,
        undefined,
        {
          storageDegraded: true,
          storageDiskFull: true,
          sqliteDatabaseSizeBytes: 4096,
          processHeapUsedBytes: 100,
          processHeapTotalBytes: 200,
          processResidentMemoryBytes: 300,
        }
      );

      expect(output).toContain('bunqueue_storage_degraded 1');
      expect(output).toContain('bunqueue_storage_disk_full 1');
      expect(output).toContain('bunqueue_sqlite_database_size_bytes 4096');
      expect(output).toContain('bunqueue_process_heap_used_bytes 100');
      expect(output).toContain('bunqueue_process_heap_total_bytes 200');
      expect(output).toContain('bunqueue_process_resident_memory_bytes 300');

      workerManager.stop();
    });
  });
});
