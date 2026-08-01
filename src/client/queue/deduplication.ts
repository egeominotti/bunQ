/**
 * Deduplication Operations
 */

import { getSharedManager } from '../manager';
import type { TcpConnectionPool } from '../tcpPool';

interface DeduplicationContext {
  name: string;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
}

/** Get the job ID associated with a deduplication key */
export async function getDeduplicationJobId(
  ctx: DeduplicationContext,
  deduplicationId: string
): Promise<string | null> {
  if (ctx.embedded) {
    return getSharedManager().getDeduplicationJobId(ctx.name, deduplicationId);
  }
  if (!ctx.tcp) return null;
  const response = await ctx.tcp.send({
    cmd: 'GetDeduplicationJobId',
    queue: ctx.name,
    deduplicationId,
  });
  return (response.data as { jobId: string | null }).jobId;
}

/** Remove a deduplication key */
export async function removeDeduplicationKey(
  ctx: DeduplicationContext,
  deduplicationId: string
): Promise<number> {
  if (ctx.embedded) {
    return getSharedManager().removeDeduplicationKey(ctx.name, deduplicationId);
  }
  if (!ctx.tcp) return 0;
  const response = await ctx.tcp.send({
    cmd: 'RemoveDeduplicationKey',
    queue: ctx.name,
    deduplicationId,
  });
  return (response.data as { count: number }).count;
}
