import type { ConnectionOptions, WorkerOptions } from '../../types';
import { resolveToken } from '../../resolveToken';
import { TcpConnectionPool } from '../../tcpPool';
import { WORKER_CONSTANTS } from '../constants';
import type { ExtendedWorkerOptions } from '../types';

export function resolveWorkerOptions(
  options: WorkerOptions,
  embedded: boolean
): ExtendedWorkerOptions {
  return {
    concurrency: options.concurrency ?? 1,
    autorun: options.autorun ?? true,
    heartbeatInterval: options.heartbeatInterval ?? 10000,
    batchSize: Math.min(options.batchSize ?? 10, 1000),
    pollTimeout: Math.min(options.pollTimeout ?? 0, WORKER_CONSTANTS.MAX_POLL_TIMEOUT),
    embedded,
    useLocks: options.useLocks ?? true,
    skipLockRenewal: options.skipLockRenewal ?? false,
    skipStalledCheck: options.skipStalledCheck ?? false,
    drainDelay: options.drainDelay ?? 50,
    lockDuration: options.lockDuration ?? 30000,
    maxStalledCount: options.maxStalledCount ?? 1,
    removeOnComplete: options.removeOnComplete,
    removeOnFail: options.removeOnFail,
    connection: options.connection,
  };
}

export function createTcpPool(options: WorkerOptions, concurrency: number): TcpConnectionPool {
  const connection: ConnectionOptions = options.connection ?? {};
  const poolSize = connection.poolSize ?? Math.min(concurrency, 8);
  const token = resolveToken(connection.token);
  return new TcpConnectionPool({
    host: connection.host ?? 'localhost',
    port: connection.port ?? 6789,
    token,
    tls: connection.tls,
    poolSize,
    pingInterval: connection.pingInterval,
    commandTimeout: connection.commandTimeout,
    maxCommandTimeouts: connection.maxCommandTimeouts,
    pipelining: connection.pipelining,
    maxInFlight: connection.maxInFlight,
  });
}
