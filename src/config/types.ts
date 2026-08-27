/**
 * bunqueue Configuration Types
 * Global configuration file interface and defineConfig helper
 */

/** Global bunqueue configuration (all sections optional) */
export interface BunqueueConfig {
  server?: {
    tcpPort?: number;
    httpPort?: number;
    host?: string;
    tcpSocketPath?: string;
    httpSocketPath?: string;
    /** Path to PEM certificate file — enables native TLS on TCP + HTTP (with tlsKeyFile) */
    tlsCertFile?: string;
    /** Path to PEM private key file — enables native TLS on TCP + HTTP (with tlsCertFile) */
    tlsKeyFile?: string;
  };
  auth?: {
    tokens?: string[];
    requireAuthForMetrics?: boolean;
  };
  storage?: {
    /** Persistence backend. Inferred from url/dataPath when omitted. */
    driver?: 'memory' | 'sqlite' | 'postgres';
    dataPath?: string;
    /** PostgreSQL connection URL. Required when driver is postgres. */
    url?: string;
    /** Isolates independent bunqueue installations sharing one PostgreSQL database. */
    namespace?: string;
    /** Stable identifier for this broker process; generated automatically when omitted. */
    brokerId?: string;
    poolSize?: number;
    leaseDurationMs?: number;
    pollIntervalMs?: number;
    statementTimeoutMs?: number;
    lockTimeoutMs?: number;
    idleTransactionTimeoutMs?: number;
    maxConcurrentOperations?: number;
    maxQueuedOperations?: number;
    maxSnapshotJobs?: number;
    maxSnapshotPayloadBytes?: number;
  };
  telemetry?: {
    /** Maximum queue label values exposed to Prometheus; zero disables per-queue series. */
    maxPrometheusQueues?: number;
  };
  cors?: {
    origins?: string[];
  };
  cloud?: {
    url?: string;
    apiKey?: string;
    instanceId?: string;
  };
  backup?: {
    enabled?: boolean;
    bucket?: string;
    accessKeyId?: string;
    secretAccessKey?: string;
    sessionToken?: string;
    region?: string;
    endpoint?: string;
    virtualHostedStyle?: boolean;
    interval?: number;
    retention?: number;
    prefix?: string;
  };
  timeouts?: {
    shutdown?: number;
    stats?: number;
    worker?: number;
    lock?: number;
  };
  webhooks?: {
    maxRetries?: number;
    retryDelay?: number;
  };
  logging?: {
    level?: 'debug' | 'info' | 'warn' | 'error';
    format?: 'text' | 'json';
  };
}

/** Type-safe config helper for intellisense */
export function defineConfig(config: BunqueueConfig): BunqueueConfig {
  return config;
}
