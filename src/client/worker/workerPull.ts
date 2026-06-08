/**
 * WorkerPull - Job pulling functions for embedded and TCP modes
 * Handles batch pulling with optional lock-based ownership
 */

import { getSharedManager } from '../manager';
import type { Job as InternalJob } from '../../domain/types/job';
import type { TcpConnection } from './types';
import { parseJobFromResponse } from './jobParser';

export interface PullConfig {
  readonly name: string;
  readonly workerId: string;
  readonly useLocks: boolean;
  readonly pollTimeout: number;
  /** Lock TTL in ms to request from the server on a lock-based pull. */
  readonly lockDuration?: number;
}

export async function pullEmbedded(
  config: PullConfig,
  count: number
): Promise<Array<{ job: InternalJob; token: string | null }>> {
  const manager = getSharedManager();

  // Use lock-based pull only when useLocks is enabled. Pass lockDuration so the
  // configured lock TTL is honored in embedded mode too (undefined → server default).
  if (config.useLocks) {
    if (count === 1) {
      const { job, token } = await manager.pullWithLock(
        config.name,
        config.workerId,
        0,
        config.lockDuration
      );
      return job ? [{ job, token }] : [];
    }
    const { jobs, tokens } = await manager.pullBatchWithLock(
      config.name,
      count,
      config.workerId,
      0,
      config.lockDuration
    );
    return jobs.map((job, i) => ({ job, token: tokens[i] || null }));
  }

  // No locks - use regular pull
  if (count === 1) {
    const job = await manager.pull(config.name, 0);
    return job ? [{ job, token: null }] : [];
  }
  const jobs = await manager.pullBatch(config.name, count, 0);
  return jobs.map((job) => ({ job, token: null }));
}

export async function pullTcp(
  config: PullConfig,
  tcp: TcpConnection,
  count: number,
  closing: boolean
): Promise<Array<{ job: InternalJob; token: string | null }>> {
  if (closing) return [];

  // Build pull command - only request locks if useLocks is enabled.
  // `count` belongs to the batch PULLB; a single PULL doesn't need it.
  const cmd: Record<string, unknown> = {
    cmd: count === 1 ? 'PULL' : 'PULLB',
    queue: config.name,
    timeout: config.pollTimeout,
  };
  if (count > 1) cmd.count = count;

  // Only request lock ownership when useLocks is enabled
  if (config.useLocks) {
    cmd.owner = config.workerId;
    // Propagate the configured lock TTL so the server doesn't always fall back
    // to its 30s default (WorkerOptions.lockDuration was previously ignored).
    if (config.lockDuration !== undefined) cmd.lockTtl = config.lockDuration;
  }

  const response = await tcp.send(cmd);

  if (!response.ok) return [];

  if (count === 1) {
    const job = response.job as Record<string, unknown> | null | undefined;
    // Only expect token if locks are enabled
    const token = config.useLocks ? ((response.token as string | null | undefined) ?? null) : null;
    if (job) {
      return [{ job: parseJobFromResponse(job, config.name), token }];
    }
    return [];
  }

  const jobs = response.jobs as Array<Record<string, unknown>> | undefined;
  // Only expect tokens if locks are enabled
  const tokens = config.useLocks ? ((response.tokens as string[] | undefined) ?? []) : [];
  return (
    jobs?.map((j, i) => ({
      job: parseJobFromResponse(j, config.name),
      token: tokens[i] || null,
    })) ?? []
  );
}
