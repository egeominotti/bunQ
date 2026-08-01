import type { ClientTlsOptions } from './tls';

export interface ConnectionOptions {
  host: string;
  port: number;
  token?: string;
  tls?: boolean | ClientTlsOptions;
  maxReconnectAttempts?: number;
  reconnectDelay?: number;
  maxReconnectDelay?: number;
  connectTimeout?: number;
  commandTimeout?: number;
  autoReconnect?: boolean;
  pingInterval?: number;
  maxPingFailures?: number;
  maxCommandTimeouts?: number;
  pipelining?: boolean;
  maxInFlight?: number;
}

export interface ConnectionHealth {
  healthy: boolean;
  state: 'connected' | 'connecting' | 'disconnected' | 'closed';
  lastSuccessAt: number | null;
  lastErrorAt: number | null;
  avgLatencyMs: number;
  consecutivePingFailures: number;
  consecutiveCommandTimeouts: number;
  totalCommands: number;
  totalErrors: number;
  uptimeMs: number;
}

export const DEFAULT_CONNECTION: Required<ConnectionOptions> = {
  host: 'localhost',
  port: 6789,
  token: '',
  tls: false,
  maxReconnectAttempts: Infinity,
  reconnectDelay: 100,
  maxReconnectDelay: 30000,
  connectTimeout: 5000,
  commandTimeout: 30000,
  autoReconnect: true,
  pingInterval: 30000,
  maxPingFailures: 3,
  maxCommandTimeouts: 3,
  pipelining: true,
  maxInFlight: 100,
};
