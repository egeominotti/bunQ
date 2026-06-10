/**
 * TCP Client Module
 * Re-exports all TCP client components
 */

export type {
  ConnectionOptions,
  ConnectionHealth,
  PendingCommand,
  SocketWrapper,
  ClientTlsOptions,
} from './types';
export { DEFAULT_CONNECTION } from './types';
export { HealthTracker, type HealthConfig } from './health';
export { ReconnectManager, type ReconnectConfig } from './reconnect';
export { createConnection, CommandQueue } from './connection';
export { TcpClient } from './client';
export { getSharedTcpClient, closeSharedTcpClient } from './shared';
