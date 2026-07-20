import { afterEach, describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { resolveServerConfig } from '../src/config/resolve';
import { BackupTelemetry } from '../src/infrastructure/backup/backupTelemetry';
import { S3BackupManager } from '../src/infrastructure/backup/s3Backup';

const managers: QueueManager[] = [];

afterEach(() => {
  for (const manager of managers.splice(0)) manager.shutdown();
});

describe('enterprise telemetry regressions', () => {
  test('exports build identity and standard process collectors', () => {
    const manager = new QueueManager();
    managers.push(manager);

    const output = manager.getPrometheusMetrics();
    expect(output).toContain('# TYPE bunqueue_build_info gauge');
    expect(output).toMatch(/bunqueue_build_info\{bun_version="[^"]+",version="[^"]+"\} 1/);
    expect(output).toContain('# TYPE process_cpu_seconds_total counter');
    expect(output).toContain('# TYPE process_start_time_seconds gauge');
    expect(output).toContain('# TYPE process_resident_memory_bytes gauge');
    expect(output).toContain('# TYPE process_heap_bytes gauge');
  });

  test('bounds per-queue series and reports omitted queues', async () => {
    const manager = new QueueManager({ maxPrometheusQueues: 2 });
    managers.push(manager);
    await manager.push('alpha', { id: 1 });
    await manager.push('beta', { id: 2 });
    await manager.push('gamma', { id: 3 });

    const output = manager.getPrometheusMetrics();
    const waitingSeries = output
      .split('\n')
      .filter((line) => line.startsWith('bunqueue_queue_jobs_waiting{'));
    expect(waitingSeries).toHaveLength(2);
    expect(output).toContain('bunqueue_queue_metrics_exported 2');
    expect(output).toContain('bunqueue_queue_metrics_omitted 1');
  });

  test('resolves the queue-cardinality cap from config and rejects invalid values', () => {
    expect(
      resolveServerConfig({ telemetry: { maxPrometheusQueues: 42 } }).maxPrometheusQueues
    ).toBe(42);
    expect(
      resolveServerConfig({ telemetry: { maxPrometheusQueues: -1 } }).maxPrometheusQueues
    ).toBe(100);
  });

  test('backup telemetry is initialized before the first scheduled attempt', () => {
    const manager = new S3BackupManager({
      enabled: true,
      accessKeyId: 'test',
      secretAccessKey: 'test',
      bucket: 'backups',
      databasePath: '/tmp/bunqueue-enterprise-telemetry.db',
      intervalMs: 60_000,
      retention: 7,
      prefix: 'backups/',
    });

    try {
      expect(manager.getMetrics()).toMatchObject({
        enabled: true,
        schedulerRunning: false,
        inProgress: false,
        attemptsTotal: 0,
        successesTotal: 0,
        failuresTotal: 0,
        consecutiveFailures: 0,
        lastSuccessTimestampSeconds: 0,
      });
      manager.start();
      expect(manager.getMetrics().schedulerRunning).toBe(true);
      manager.stop();
      expect(manager.getMetrics().schedulerRunning).toBe(false);
    } finally {
      manager.stop();
    }
  });

  test('backup size telemetry reports the compressed object size', () => {
    const telemetry = new BackupTelemetry(true, 60_000, 7);
    telemetry.tryStart();
    telemetry.succeed({ success: true, size: 1_000, compressedSize: 400 }, 25, 1_000);

    expect(telemetry.snapshot().lastSizeBytes).toBe(400);
  });

  test('injects bounded connection and backup state into queue metrics', () => {
    const queueManager = new QueueManager();
    managers.push(queueManager);
    const backupManager = new S3BackupManager({
      enabled: true,
      accessKeyId: 'test',
      secretAccessKey: 'test',
      bucket: 'backups',
      databasePath: '/tmp/bunqueue-enterprise-provider.db',
      intervalMs: 60_000,
      retention: 7,
      prefix: 'backups/',
    });
    queueManager.setOperationalMetricsProvider(() => ({
      backup: backupManager.getMetrics(),
      connections: { tcp: 3, websocket: 2, sse: 1 },
    }));

    try {
      const output = queueManager.getPrometheusMetrics();
      expect(output).toContain('bunqueue_backup_enabled 1');
      expect(output).toContain('bunqueue_backup_attempts_total 0');
      expect(output).toContain('bunqueue_connections{transport="tcp"} 3');
      expect(output).toContain('bunqueue_connections{transport="websocket"} 2');
      expect(output).toContain('bunqueue_connections{transport="sse"} 1');
    } finally {
      backupManager.stop();
    }
  });
});
