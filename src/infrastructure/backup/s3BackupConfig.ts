/**
 * S3 Backup Configuration
 * Types and configuration factory
 */

/** S3 Backup configuration */
export interface S3BackupConfig {
  /** Enable S3 backup */
  enabled: boolean;
  /** S3 access key ID */
  accessKeyId: string;
  /** S3 secret access key */
  secretAccessKey: string;
  /** Temporary credential session token */
  sessionToken?: string;
  /** S3 bucket name */
  bucket: string;
  /** S3 endpoint (optional, for non-AWS S3-compatible services) */
  endpoint?: string;
  /** Use bucket-name-in-host S3 requests */
  virtualHostedStyle?: boolean;
  /** S3 region (optional, default: us-east-1) */
  region?: string;
  /** Backup interval in milliseconds (default: 6 hours) */
  intervalMs: number;
  /** Number of backups to retain (default: 7) */
  retention: number;
  /** Prefix for backup files (default: 'backups/') */
  prefix: string;
  /** Path to the SQLite database file */
  databasePath: string;
  /** Timeout for S3 operations in milliseconds (default: 30000) */
  timeoutMs?: number;
}

/** Backup result */
export interface BackupResult {
  success: boolean;
  key?: string;
  /** Uncompressed SQLite database size in bytes */
  size?: number;
  /** Uploaded gzip object size in bytes */
  compressedSize?: number;
  duration?: number;
  error?: string;
}

/** Backup metadata stored in S3 */
export interface BackupMetadata {
  timestamp: string;
  version: string;
  /** Original uncompressed size in bytes */
  size: number;
  /** Compressed size in bytes (if compressed) */
  compressedSize?: number;
  checksum: string;
  /** Whether the backup is gzip compressed */
  compressed?: boolean;
}

/** Backup list item */
export interface BackupItem {
  key: string;
  size: number;
  lastModified: Date;
}

/** Default configuration values */
export const DEFAULTS = {
  intervalMs: 6 * 60 * 60 * 1000, // 6 hours
  retention: 7,
  prefix: 'backups/',
  region: 'us-east-1',
} as const;

/**
 * Create configuration from environment variables
 */
export function configFromEnv(databasePath: string): S3BackupConfig {
  const virtualHostedStyle = Bun.env.S3_VIRTUAL_HOSTED_STYLE;
  return {
    enabled: Bun.env.S3_BACKUP_ENABLED === '1' || Bun.env.S3_BACKUP_ENABLED === 'true',
    accessKeyId: Bun.env.S3_ACCESS_KEY_ID ?? Bun.env.AWS_ACCESS_KEY_ID ?? '',
    secretAccessKey: Bun.env.S3_SECRET_ACCESS_KEY ?? Bun.env.AWS_SECRET_ACCESS_KEY ?? '',
    sessionToken: Bun.env.S3_SESSION_TOKEN ?? Bun.env.AWS_SESSION_TOKEN,
    bucket: Bun.env.S3_BUCKET ?? Bun.env.AWS_BUCKET ?? '',
    endpoint: Bun.env.S3_ENDPOINT ?? Bun.env.AWS_ENDPOINT,
    virtualHostedStyle:
      virtualHostedStyle === undefined
        ? undefined
        : virtualHostedStyle === '1' || virtualHostedStyle === 'true',
    region: Bun.env.S3_REGION ?? Bun.env.AWS_REGION ?? DEFAULTS.region,
    intervalMs: parseInt(Bun.env.S3_BACKUP_INTERVAL ?? '', 10) || DEFAULTS.intervalMs,
    retention: parseInt(Bun.env.S3_BACKUP_RETENTION ?? '', 10) || DEFAULTS.retention,
    prefix: Bun.env.S3_BACKUP_PREFIX ?? DEFAULTS.prefix,
    databasePath,
  };
}

/**
 * Validate configuration
 */
export function validateConfig(config: S3BackupConfig): { valid: boolean; errors: string[] } {
  const errors: string[] = [];

  if (!config.accessKeyId) {
    errors.push('S3_ACCESS_KEY_ID is required');
  }
  if (!config.secretAccessKey) {
    errors.push('S3_SECRET_ACCESS_KEY is required');
  }
  if (!config.bucket) {
    errors.push('S3_BUCKET is required');
  }
  if (!config.databasePath) {
    errors.push('Database path is required');
  }

  if (config.retention < 1) {
    errors.push('Retention must be at least 1');
  }
  if (config.intervalMs < 60000) {
    errors.push('Backup interval must be at least 60 seconds');
  }

  return { valid: errors.length === 0, errors };
}
