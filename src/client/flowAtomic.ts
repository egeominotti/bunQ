import type { Job as DomainJob } from '../domain/types/job';
import type { AtomicFlowBatchInput, AtomicFlowBatchResult } from '../domain/types/flow';
import { getSharedManager } from './manager';
import type { TcpConnectionPool } from './tcpPool';

export interface AtomicFlowContext {
  readonly embedded: boolean;
  readonly tcp: TcpConnectionPool | null;
}

/** Execute the same one-command atomic primitive in embedded and TCP modes. */
export async function commitFlow(
  ctx: AtomicFlowContext,
  batch: AtomicFlowBatchInput
): Promise<DomainJob[]> {
  if (batch.jobs.length === 0) return [];
  if (ctx.embedded) {
    return (await getSharedManager().pushFlow(batch)).jobs;
  }
  if (!ctx.tcp) throw new Error('TCP connection not initialized');
  const response = await ctx.tcp.send({ cmd: 'PUSHF', jobs: batch.jobs });
  if (response.ok !== true) {
    throw new Error(typeof response.error === 'string' ? response.error : 'Failed to add flow');
  }
  const result = response.data as AtomicFlowBatchResult | undefined;
  if (!result || !Array.isArray(result.jobs) || result.jobs.length !== batch.jobs.length) {
    throw new Error('Invalid PUSHF response: committed job snapshots are missing');
  }
  const expected = new Set(batch.jobs.map((job) => String(job.id)));
  if (result.jobs.some((job) => !expected.delete(String(job.id))) || expected.size > 0) {
    throw new Error('Invalid PUSHF response: committed job IDs do not match the request');
  }
  return result.jobs;
}
