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
    dataPath?: string;
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
    region?: string;
    endpoint?: string;
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
