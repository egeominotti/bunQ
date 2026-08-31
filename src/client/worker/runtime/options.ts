import type { ConnectionOptions, WorkerOptions } from '../../types';
import { assertGroupPullOptions } from '../../../domain/types/group';
import { resolveToken } from '../../resolveToken';
import { TcpConnectionPool } from '../../tcpPool';
import { WORKER_CONSTANTS } from '../constants';
import type { ExtendedWorkerOptions } from '../types';

export function resolveWorkerOptions(
  options: WorkerOptions,
  embedded: boolean
): ExtendedWorkerOptions {
  assertGroupPullOptions(options.group);
  const batch = options.batch;
  if (batch) {
    if (!Number.isSafeInteger(batch.size) || batch.size <= 0 || batch.size > 1000) {
      throw new Error('batch.size must be a positive safe integer no greater than 1000');
    }
    const minSize = batch.minSize ?? 1;
    if (!Number.isSafeInteger(minSize) || minSize <= 0 || minSize > batch.size) {
      throw new Error('batch.minSize must be between 1 and batch.size');
    }
    if (options.limiter && !options.limiter.groupKey && minSize > options.limiter.max) {
      throw new Error('batch.minSize cannot exceed limiter.max');
    }
    if (
      batch.timeout !== undefined &&
      (!Number.isSafeInteger(batch.timeout) || batch.timeout < 0)
    ) {
      throw new Error('batch.timeout must be a non-negative safe integer');
    }
  }
  return {
    concurrency: options.concurrency ?? 1,
    autorun: options.autorun ?? true,
    heartbeatInterval: options.heartbeatInterval ?? 10000,
    batchSize: Math.min(batch?.size ?? options.batchSize ?? 10, 1000),
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
    group: options.group,
    batch: batch
      ? {
          size: batch.size,
          minSize: batch.minSize ?? 1,
          timeout: batch.timeout ?? 0,
          groupAffinity: batch.groupAffinity ?? false,
        }
      : undefined,
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
