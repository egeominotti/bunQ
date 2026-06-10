/**
 * bunqueue Config Resolver
 * Merges config file + env vars + defaults (config file wins)
 */

import { hostname } from 'os';
import type { BunqueueConfig } from './types';
import type { CloudConfig } from '../infrastructure/cloud/types';
import type { S3BackupConfig } from '../infrastructure/backup/s3BackupConfig';
import { DEFAULTS as S3_DEFAULTS } from '../infrastructure/backup/s3BackupConfig';

/** Fully resolved server configuration */
export interface ResolvedConfig {
  tcpPort: number;
  httpPort: number;
  hostname: string;
  tcpSocketPath: string | undefined;
  httpSocketPath: string | undefined;
  tlsCertFile: string | undefined;
  tlsKeyFile: string | undefined;
  authTokens: string[];
  dataPath: string | undefined;
  corsOrigins: string[];
  requireAuthForMetrics: boolean;
  s3BackupEnabled: boolean;
  shutdownTimeoutMs: number;
  statsIntervalMs: number;
}

/* eslint-disable complexity -- pure config mapping, no real branching logic */

/** Resolve server config: config file > env vars > defaults */
export function resolveServerConfig(fileConfig: BunqueueConfig | null): ResolvedConfig {
  const fc = fileConfig;
  return {
    tcpPort: fc?.server?.tcpPort ?? parseInt(Bun.env.TCP_PORT ?? '6789', 10),
    httpPort: fc?.server?.httpPort ?? parseInt(Bun.env.HTTP_PORT ?? '6790', 10),
    hostname: fc?.server?.host ?? Bun.env.HOST ?? '0.0.0.0',
    tcpSocketPath: fc?.server?.tcpSocketPath ?? Bun.env.TCP_SOCKET_PATH,
    httpSocketPath: fc?.server?.httpSocketPath ?? Bun.env.HTTP_SOCKET_PATH,
    tlsCertFile: fc?.server?.tlsCertFile ?? Bun.env.TLS_CERT_FILE,
    tlsKeyFile: fc?.server?.tlsKeyFile ?? Bun.env.TLS_KEY_FILE,
    authTokens: fc?.auth?.tokens ?? Bun.env.AUTH_TOKENS?.split(',').filter(Boolean) ?? [],
    dataPath:
      fc?.storage?.dataPath ??
      Bun.env.BUNQUEUE_DATA_PATH ??
      Bun.env.BQ_DATA_PATH ??
      Bun.env.DATA_PATH ??
      Bun.env.SQLITE_PATH,
    corsOrigins: fc?.cors?.origins ?? Bun.env.CORS_ALLOW_ORIGIN?.split(',').filter(Boolean) ?? [],
    requireAuthForMetrics: fc?.auth?.requireAuthForMetrics ?? Bun.env.METRICS_AUTH === 'true',
    s3BackupEnabled:
      fc?.backup?.enabled ??
      (Bun.env.S3_BACKUP_ENABLED === '1' || Bun.env.S3_BACKUP_ENABLED === 'true'),
    shutdownTimeoutMs:
      fc?.timeouts?.shutdown ?? parseInt(Bun.env.SHUTDOWN_TIMEOUT_MS ?? '30000', 10),
    statsIntervalMs: fc?.timeouts?.stats ?? parseInt(Bun.env.STATS_INTERVAL_MS ?? '300000', 10),
  };
}

/**
 * Resolve server TLS options from resolved config. Returns null when TLS is
 * not configured; throws when only one of cert/key is set (fail fast at
 * startup rather than serving plaintext when the operator expected TLS).
 */
export function resolveTlsServerOptions(config: {
  tlsCertFile?: string;
  tlsKeyFile?: string;
}): { certFile: string; keyFile: string } | null {
  const { tlsCertFile, tlsKeyFile } = config;
  if (!tlsCertFile && !tlsKeyFile) return null;
  if (!tlsKeyFile) {
    throw new Error(
      'TLS misconfigured: tlsCertFile is set but tlsKeyFile (TLS_KEY_FILE) is missing'
    );
  }
  if (!tlsCertFile) {
    throw new Error(
      'TLS misconfigured: tlsKeyFile is set but tlsCertFile (TLS_CERT_FILE) is missing'
    );
  }
  return { certFile: tlsCertFile, keyFile: tlsKeyFile };
}

/** Resolve cloud config: config file > env vars. Returns null if disabled. */
export function resolveCloudConfig(
  fileConfig: BunqueueConfig | null,
  dataPath?: string
): CloudConfig | null {
  const fc = fileConfig?.cloud;
  const url = fc?.url ?? Bun.env.BUNQUEUE_CLOUD_URL;
  const apiKey = fc?.apiKey ?? Bun.env.BUNQUEUE_CLOUD_API_KEY;

  if (!url || !apiKey) return null;

  const instanceId = fc?.instanceId ?? Bun.env.BUNQUEUE_CLOUD_INSTANCE_ID;
  if (!instanceId) {
    console.error('[Cloud] BUNQUEUE_CLOUD_INSTANCE_ID is required for cloud mode.');
    return null;
  }

  return {
    url: url.replace(/\/+$/, ''),
    apiKey,
    instanceId,
    signingSecret: Bun.env.BUNQUEUE_CLOUD_SIGNING_SECRET ?? null,
    instanceName: Bun.env.BUNQUEUE_CLOUD_INSTANCE_NAME ?? hostname(),
    intervalMs: parseInt(Bun.env.BUNQUEUE_CLOUD_INTERVAL_MS ?? '15000', 10),
    includeJobData: Bun.env.BUNQUEUE_CLOUD_INCLUDE_JOB_DATA !== 'false',
    redactFields: Bun.env.BUNQUEUE_CLOUD_REDACT_FIELDS?.split(',').filter(Boolean) ?? [],
    eventFilter: Bun.env.BUNQUEUE_CLOUD_EVENTS?.split(',').filter(Boolean) ?? [],
    bufferSize: parseInt(Bun.env.BUNQUEUE_CLOUD_BUFFER_SIZE ?? '720', 10),
    circuitBreakerThreshold: parseInt(Bun.env.BUNQUEUE_CLOUD_CIRCUIT_BREAKER_THRESHOLD ?? '5', 10),
    circuitBreakerResetMs: parseInt(Bun.env.BUNQUEUE_CLOUD_CIRCUIT_BREAKER_RESET_MS ?? '60000', 10),
    useWebSocket: Bun.env.BUNQUEUE_CLOUD_USE_WEBSOCKET !== 'false',
    useHttp: Bun.env.BUNQUEUE_CLOUD_USE_HTTP !== 'false',
    dataPath: dataPath ?? null,
    remoteCommands: Bun.env.BUNQUEUE_CLOUD_REMOTE_COMMANDS !== 'false',
  };
}

/** Resolve S3 backup config: config file > env vars */
export function resolveBackupConfig(
  fileConfig: BunqueueConfig | null,
  databasePath: string
): S3BackupConfig {
  const fc = fileConfig?.backup;
  return {
    enabled:
      fc?.enabled ?? (Bun.env.S3_BACKUP_ENABLED === '1' || Bun.env.S3_BACKUP_ENABLED === 'true'),
    accessKeyId: fc?.accessKeyId ?? Bun.env.S3_ACCESS_KEY_ID ?? Bun.env.AWS_ACCESS_KEY_ID ?? '',
    secretAccessKey:
      fc?.secretAccessKey ?? Bun.env.S3_SECRET_ACCESS_KEY ?? Bun.env.AWS_SECRET_ACCESS_KEY ?? '',
    bucket: fc?.bucket ?? Bun.env.S3_BUCKET ?? Bun.env.AWS_BUCKET ?? '',
    endpoint: fc?.endpoint ?? Bun.env.S3_ENDPOINT ?? Bun.env.AWS_ENDPOINT,
    region: fc?.region ?? Bun.env.S3_REGION ?? Bun.env.AWS_REGION ?? S3_DEFAULTS.region,
    intervalMs:
      fc?.interval ?? (parseInt(Bun.env.S3_BACKUP_INTERVAL ?? '', 10) || S3_DEFAULTS.intervalMs),
    retention:
      fc?.retention ?? (parseInt(Bun.env.S3_BACKUP_RETENTION ?? '', 10) || S3_DEFAULTS.retention),
    prefix: fc?.prefix ?? Bun.env.S3_BACKUP_PREFIX ?? S3_DEFAULTS.prefix,
    databasePath,
  };
}

/* eslint-enable complexity */
