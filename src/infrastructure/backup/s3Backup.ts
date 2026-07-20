/**
 * S3 Backup Module
 * Automated database backup to S3-compatible storage
 *
 * Supports: AWS S3, Cloudflare R2, MinIO, DigitalOcean Spaces, etc.
 */

import { S3Client } from 'bun';
import { backupLog } from '../../shared/logger';
import {
  type S3BackupConfig,
  type BackupResult,
  type BackupItem,
  DEFAULTS,
  configFromEnv,
  validateConfig,
} from './s3BackupConfig';
import { performBackup, listBackups, restoreBackup, cleanupOldBackups } from './s3BackupOperations';
import { BackupTelemetry, type BackupMetrics } from './backupTelemetry';

// Re-export types
export type { S3BackupConfig, BackupResult } from './s3BackupConfig';
export type { BackupMetrics } from './backupTelemetry';

/**
 * S3 Backup Manager
 * Handles automated and manual backups to S3-compatible storage
 */
export class S3BackupManager {
  private readonly config: S3BackupConfig;
  private readonly client: S3Client;
  private readonly flushBeforeBackup?: () => void | Promise<void>;
  private readonly telemetry: BackupTelemetry;
  private backupInterval: ReturnType<typeof setInterval> | null = null;
  private initialBackupTimeout: ReturnType<typeof setTimeout> | null = null;
  private dashboardEmit: ((event: string, data: Record<string, unknown>) => void) | null = null;

  /** Set the dashboard event emitter callback */
  setDashboardEmit(callback: (event: string, data: Record<string, unknown>) => void): void {
    this.dashboardEmit = callback;
  }

  constructor(
    config: Partial<S3BackupConfig> & {
      databasePath: string;
      flushBeforeBackup?: () => void | Promise<void>;
    }
  ) {
    this.config = {
      enabled: config.enabled ?? false,
      accessKeyId: config.accessKeyId ?? '',
      secretAccessKey: config.secretAccessKey ?? '',
      sessionToken: config.sessionToken,
      bucket: config.bucket ?? '',
      endpoint: config.endpoint,
      virtualHostedStyle: config.virtualHostedStyle,
      region: config.region ?? DEFAULTS.region,
      intervalMs: config.intervalMs ?? DEFAULTS.intervalMs,
      retention: config.retention ?? DEFAULTS.retention,
      prefix: config.prefix ?? DEFAULTS.prefix,
      databasePath: config.databasePath,
      timeoutMs: config.timeoutMs,
    };

    this.flushBeforeBackup = config.flushBeforeBackup;
    this.telemetry = new BackupTelemetry(
      this.config.enabled,
      this.config.intervalMs,
      this.config.retention
    );

    // Initialize S3 client
    this.client = new S3Client({
      accessKeyId: this.config.accessKeyId,
      secretAccessKey: this.config.secretAccessKey,
      sessionToken: this.config.sessionToken,
      bucket: this.config.bucket,
      endpoint: this.config.endpoint,
      virtualHostedStyle: this.config.virtualHostedStyle,
      region: this.config.region,
    });
  }

  /**
   * Create configuration from environment variables
   */
  static fromEnv(databasePath: string): S3BackupConfig {
    return configFromEnv(databasePath);
  }

  /**
   * Validate configuration
   */
  validate(): { valid: boolean; errors: string[] } {
    return validateConfig(this.config);
  }

  /**
   * Start automated backup scheduler
   */
  start(): void {
    if (!this.config.enabled) {
      return;
    }

    const validation = this.validate();
    if (!validation.valid) {
      backupLog.error('S3 backup configuration invalid', { errors: validation.errors });
      return;
    }

    // Run initial backup after 1 minute
    this.initialBackupTimeout = setTimeout(() => {
      this.initialBackupTimeout = null;
      this.backup().catch((err: unknown) => {
        backupLog.error('Initial backup failed', { error: String(err) });
      });
    }, 60 * 1000);

    // Schedule periodic backups
    this.backupInterval = setInterval(() => {
      this.backup().catch((err: unknown) => {
        backupLog.error('Scheduled backup failed', { error: String(err) });
      });
    }, this.config.intervalMs);
    this.telemetry.setSchedulerRunning(true);
  }

  /**
   * Stop automated backup scheduler
   */
  stop(): void {
    if (this.initialBackupTimeout) {
      clearTimeout(this.initialBackupTimeout);
      this.initialBackupTimeout = null;
    }
    if (this.backupInterval) {
      clearInterval(this.backupInterval);
      this.backupInterval = null;
    }
    this.telemetry.setSchedulerRunning(false);
  }

  /**
   * Perform a backup
   */
  async backup(): Promise<BackupResult> {
    if (!this.telemetry.tryStart()) {
      return { success: false, error: 'Backup already in progress' };
    }

    const startedAt = Date.now();

    try {
      this.dashboardEmit?.('storage:backup-started', { bucket: this.config.bucket });
      if (this.flushBeforeBackup) {
        await this.flushBeforeBackup();
      }

      const result = await performBackup(this.config, this.client);

      if (result.success) {
        await cleanupOldBackups(this.config, this.client);
        this.dashboardEmit?.('storage:backup-completed', {
          bucket: this.config.bucket,
          key: result.key,
        });
        this.telemetry.succeed(result, Date.now() - startedAt);
      } else {
        this.dashboardEmit?.('storage:backup-failed', {
          bucket: this.config.bucket,
          error: result.error,
        });
        this.telemetry.fail(Date.now() - startedAt);
      }

      return result;
    } catch (err) {
      this.telemetry.fail(Date.now() - startedAt);
      this.dashboardEmit?.('storage:backup-failed', {
        bucket: this.config.bucket,
        error: String(err),
      });
      throw err;
    }
  }

  /**
   * List available backups
   */
  listBackups(): Promise<BackupItem[]> {
    return listBackups(this.config, this.client);
  }

  /**
   * Restore from a backup
   */
  restore(key: string): Promise<BackupResult> {
    return restoreBackup(key, this.config, this.client);
  }

  /**
   * Get backup status
   */
  getStatus(): {
    enabled: boolean;
    bucket: string;
    endpoint: string;
    intervalMs: number;
    retention: number;
    isRunning: boolean;
  } {
    return {
      enabled: this.config.enabled,
      bucket: this.config.bucket,
      endpoint: this.config.endpoint ?? 'AWS S3',
      intervalMs: this.config.intervalMs,
      retention: this.config.retention,
      isRunning: this.backupInterval !== null,
    };
  }

  /** Snapshot label-free backup telemetry for the Prometheus collector. */
  getMetrics(): BackupMetrics {
    return this.telemetry.snapshot();
  }
}
