/**
 * Dashboard Handlers
 * Aggregated endpoints for dashboard consumption
 * Read-only, no business logic changes - composes existing data
 */

import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import * as resp from '../../../domain/types/response';
import type { HandlerContext } from '../types';
import { throughputTracker } from '../../../application/throughputTracker';
import { latencyTracker } from '../../../application/latencyTracker';

/** Dashboard overview - single call for all dashboard data */
export function handleDashboardOverview(
  _cmd: Extract<Command, { cmd: 'DashboardOverview' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const stats = ctx.queueManager.getStats();
  const rates = throughputTracker.getRates();
  const latencies = latencyTracker.getPercentiles();
  const avgLatencies = latencyTracker.getAverages();
  const memStats = ctx.queueManager.getMemoryStats();
  const workers = ctx.queueManager.workerManager.list();
  const workerStats = ctx.queueManager.workerManager.getStats();
  const crons = ctx.queueManager.listCrons();
  const storage = ctx.queueManager.getStorageStatus();
  const mem = process.memoryUsage();

  return resp.data(
    {
      stats: {
        waiting: stats.waiting,
        active: stats.active,
        delayed: stats.delayed,
        completed: stats.completed,
        dlq: stats.dlq,
        totalPushed: Number(stats.totalPushed),
        totalPulled: Number(stats.totalPulled),
        totalCompleted: Number(stats.totalCompleted),
        totalFailed: Number(stats.totalFailed),
        uptime: stats.uptime,
      },
      throughput: rates,
      latency: {
        averages: avgLatencies,
        percentiles: latencies,
      },
      memory: {
        heapUsed: Math.round(mem.heapUsed / 1024 / 1024),
        heapTotal: Math.round(mem.heapTotal / 1024 / 1024),
        rss: Math.round(mem.rss / 1024 / 1024),
      },
      collections: memStats,
      workers: {
        total: workerStats.total,
        active: workerStats.active,
        list: workers.map((w) => ({
          id: w.id,
          name: w.name,
          queues: w.queues,
          lastSeen: w.lastSeen,
          activeJobs: w.activeJobs,
          processedJobs: w.processedJobs,
          failedJobs: w.failedJobs,
        })),
      },
      crons: crons.map((c) => ({
        name: c.name,
        queue: c.queue,
        schedule: c.schedule ?? null,
        repeatEvery: c.repeatEvery ?? null,
        nextRun: c.nextRun,
        executions: c.executions,
      })),
      storage,
      timestamp: Date.now(),
    },
    reqId
  );
}

/** Dashboard queues - all queues with per-queue stats */
export function handleDashboardQueues(
  _cmd: Extract<Command, { cmd: 'DashboardQueues' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const queueNames = ctx.queueManager.listQueues();
  const perQueueStats = ctx.queueManager.getPerQueueStats();
  const pausedStates = new Map<string, boolean>();

  for (const name of queueNames) {
    pausedStates.set(name, ctx.queueManager.isPaused(name));
  }

  const queues = queueNames.map((name) => {
    const stats = perQueueStats.get(name);
    return {
      name,
      waiting: stats?.waiting ?? 0,
      prioritized: stats?.prioritized ?? 0,
      delayed: stats?.delayed ?? 0,
      active: stats?.active ?? 0,
      dlq: stats?.dlq ?? 0,
      paused: pausedStates.get(name) ?? false,
    };
  });

  return resp.data({ queues, timestamp: Date.now() }, reqId);
}

/** Dashboard single queue detail */
export function handleDashboardQueue(
  cmd: Extract<Command, { cmd: 'DashboardQueue' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const queue = cmd.queue;
  const counts = ctx.queueManager.getQueueJobCounts(queue);
  const paused = ctx.queueManager.isPaused(queue);
  const dlqJobs = ctx.queueManager.getDlq(queue, 10);
  const priorityCounts = ctx.queueManager.getCountsPerPriority(queue);

  const result: Record<string, unknown> = {
    name: queue,
    counts,
    paused,
    priorityCounts,
    dlqPreview: dlqJobs.map((j) => ({
      id: j.id,
      data: j.data,
      attempts: j.attempts,
      createdAt: j.createdAt,
    })),
    timestamp: Date.now(),
  };

  if (cmd.includeJobs) {
    const limit = Math.min(cmd.jobsLimit ?? 10, 50);
    const waiting = ctx.queueManager.getJobs(queue, { state: 'waiting', end: limit });
    const active = ctx.queueManager.getJobs(queue, { state: 'active', end: limit });
    const delayed = ctx.queueManager.getJobs(queue, { state: 'delayed', end: limit });
    result.jobs = {
      waiting: waiting.map(jobSummary),
      active: active.map(jobSummary),
      delayed: delayed.map(jobSummary),
    };
  }

  return resp.data(result, reqId);
}

/** Minimal job summary for dashboard lists */
function jobSummary(j: {
  id: string;
  queue: string;
  data: unknown;
  priority: number;
  createdAt: number;
  runAt: number;
  attempts: number;
  progress: number;
}) {
  return {
    id: j.id,
    priority: j.priority,
    createdAt: j.createdAt,
    runAt: j.runAt,
    attempts: j.attempts,
    progress: j.progress,
  };
}
