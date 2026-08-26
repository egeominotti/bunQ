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
import type { CronJob } from '../../../domain/types/cron';
import type { Worker } from '../../../domain/types/worker';
import { pausedView } from '../../../shared/pausedView';
import { clientStorageStatus } from '../../../shared/storageHealth';

const WORKER_TIMEOUT_MS = parseInt(Bun.env.WORKER_TIMEOUT_MS ?? '30000', 10);

type DurableDashboardManager = HandlerContext['queueManager'] & {
  listWorkersDurable?: () => Promise<Worker[]>;
  listCronsDurable?: () => Promise<CronJob[]>;
};

/** Dashboard overview - single call for all dashboard data */
export function handleDashboardOverview(
  _cmd: Extract<Command, { cmd: 'DashboardOverview' }>,
  ctx: HandlerContext,
  reqId?: string
): Response | Promise<Response> {
  const manager = ctx.queueManager as DurableDashboardManager;
  if (manager.listWorkersDurable || manager.listCronsDurable) {
    return Promise.all([
      manager.listWorkersDurable
        ? manager.listWorkersDurable()
        : Promise.resolve(manager.workerManager.list()),
      manager.listCronsDurable ? manager.listCronsDurable() : Promise.resolve(manager.listCrons()),
    ]).then(([workers, crons]) => dashboardOverviewResponse(manager, workers, crons, reqId));
  }
  return dashboardOverviewResponse(
    manager,
    manager.workerManager.list(),
    manager.listCrons(),
    reqId
  );
}

function dashboardOverviewResponse(
  queueManager: HandlerContext['queueManager'],
  workers: Worker[],
  crons: CronJob[],
  reqId?: string
): Response {
  const stats = queueManager.getStats();
  const rates = throughputTracker.getRates();
  const latencies = latencyTracker.getPercentiles();
  const avgLatencies = latencyTracker.getAverages();
  const memStats = queueManager.getMemoryStats();
  const now = Date.now();
  const activeWorkers = workers.filter((worker) => now - worker.lastSeen < WORKER_TIMEOUT_MS);
  const storage = clientStorageStatus(queueManager.getStorageStatus());
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
        total: workers.length,
        active: activeWorkers.length,
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
  const rawCounts = ctx.queueManager.getQueueJobCounts(queue);
  const paused = ctx.queueManager.isPaused(queue);
  // Keep the counts consistent with the per-state lists below (#92): a paused
  // queue reports ready jobs under `paused`, not `waiting`/`prioritized`.
  const pv = pausedView(rawCounts.waiting, rawCounts.prioritized, paused);
  const counts = {
    ...rawCounts,
    waiting: pv.waiting,
    prioritized: pv.prioritized,
    paused: pv.paused,
  };
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
    // When paused, ready jobs surface under `paused` (waiting/prioritized are
    // empty) — match the counts above (#92).
    const waiting = ctx.queueManager.getJobs(queue, { state: 'waiting', end: limit });
    const active = ctx.queueManager.getJobs(queue, { state: 'active', end: limit });
    const delayed = ctx.queueManager.getJobs(queue, { state: 'delayed', end: limit });
    const pausedJobs = paused
      ? ctx.queueManager.getJobs(queue, { state: 'paused', end: limit })
      : [];
    result.jobs = {
      waiting: waiting.map(jobSummary),
      active: active.map(jobSummary),
      delayed: delayed.map(jobSummary),
      paused: pausedJobs.map(jobSummary),
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
