/* eslint-disable @typescript-eslint/no-non-null-assertion */
/**
 * Worker Query Operations
 */

import { getSharedManager } from '../manager';
import type { TcpConnectionPool } from '../tcpPool';
import type { QueueMetrics, QueueMetricType } from '../../domain/types/metrics';

interface WorkersContext {
  name: string;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
}

export interface WorkerInfo {
  id: string;
  name: string;
  addr?: string;
  queues?: string[];
  concurrency?: number;
  hostname?: string;
  pid?: number;
  status?: 'active' | 'stale';
  registeredAt?: number;
  lastSeen?: number;
  activeJobs?: number;
  processedJobs?: number;
  failedJobs?: number;
  currentJob?: string | null;
  uptime?: number;
}

/** Get workers processing this queue */
export async function getWorkers(ctx: WorkersContext): Promise<WorkerInfo[]> {
  if (ctx.embedded) {
    return getSharedManager()
      .workerManager.getForQueue(ctx.name)
      .map((worker) => ({
        id: worker.id,
        name: worker.name,
        queues: worker.queues,
        concurrency: worker.concurrency,
        hostname: worker.hostname,
        pid: worker.pid,
        status: 'active' as const,
        registeredAt: worker.registeredAt,
        lastSeen: worker.lastSeen,
        activeJobs: worker.activeJobs,
        processedJobs: worker.processedJobs,
        failedJobs: worker.failedJobs,
        currentJob: worker.currentJob,
        uptime: Date.now() - worker.registeredAt,
      }));
  }

  if (!ctx.tcp) return [];
  const response = await ctx.tcp.send({ cmd: 'ListWorkers' });
  if (!response.ok) return [];
  const workers = (response.data as { workers?: WorkerInfo[] } | undefined)?.workers ?? [];
  return workers.filter((worker) => worker.queues?.includes(ctx.name));
}

/** Get worker count */
export async function getWorkersCount(ctx: WorkersContext): Promise<number> {
  const workers = await getWorkers(ctx);
  return workers.length;
}

/** Get metrics for this queue */
export async function getMetrics(
  ctx: WorkersContext,
  type: QueueMetricType,
  start = 0,
  end = -1
): Promise<QueueMetrics> {
  if (ctx.embedded) {
    return getSharedManager().getQueueMetrics(ctx.name, type, start, end);
  }

  if (!ctx.tcp) throw new Error('TCP connection is not available');
  const response = await ctx.tcp.send({ cmd: 'Metrics', queue: ctx.name, type, start, end });
  if (!response.ok) throw new Error(String(response.error));
  const metrics = (response as { data?: QueueMetrics }).data;
  if (!metrics) throw new Error('Broker returned no queue metrics payload');
  return metrics;
}

/** Trim old events from this queue's durable lifecycle journal. */
export async function trimEvents(ctx: WorkersContext, maxLength: number): Promise<number> {
  if (ctx.embedded) return getSharedManager().trimQueueEvents(ctx.name, maxLength);
  if (!ctx.tcp) throw new Error('TCP connection is not available');
  const response = await ctx.tcp.send({ cmd: 'TrimEvents', queue: ctx.name, maxLength });
  if (!response.ok) throw new Error(String(response.error));
  return (response as { data?: { removed?: number } }).data?.removed ?? 0;
}
