/**
 * HTTP dashboard endpoints
 */

import type { QueueManager } from '../../application/queueManager';
import { latencyTracker } from '../../application/latencyTracker';
import { throughputTracker } from '../../application/throughputTracker';
import type { CronJob } from '../../domain/types/cron';
import type { Worker } from '../../domain/types/worker';
import { pausedView } from '../../shared/pausedView';
import { clientStorageStatus } from '../../shared/storageHealth';
import { jsonResponse } from './httpResponse';

const WORKER_TIMEOUT_MS = parseInt(Bun.env.WORKER_TIMEOUT_MS ?? '30000', 10);

type DurableDashboardManager = QueueManager & {
  listWorkersDurable?: () => Promise<Worker[]>;
  listCronsDurable?: () => Promise<CronJob[]>;
};

/** Dashboard overview endpoint - aggregates all dashboard data in a single call */
export function dashboardOverviewEndpoint(
  queueManager: QueueManager,
  corsOrigins?: Set<string>
): Response | Promise<Response> {
  const manager = queueManager as DurableDashboardManager;
  if (manager.listWorkersDurable || manager.listCronsDurable) {
    return Promise.all([
      manager.listWorkersDurable
        ? manager.listWorkersDurable()
        : Promise.resolve(manager.workerManager.list()),
      manager.listCronsDurable ? manager.listCronsDurable() : Promise.resolve(manager.listCrons()),
    ]).then(([workers, crons]) => dashboardOverviewResponse(manager, workers, crons, corsOrigins));
  }
  return dashboardOverviewResponse(
    manager,
    manager.workerManager.list(),
    manager.listCrons(),
    corsOrigins
  );
}

function dashboardOverviewResponse(
  queueManager: QueueManager,
  workers: Worker[],
  crons: CronJob[],
  corsOrigins?: Set<string>
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

  return jsonResponse(
    {
      ok: true,
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
        list: workers
          .slice(0, 100)
          .map(
            (w: {
              id: string;
              name: string;
              queues: string[];
              lastSeen: number;
              activeJobs: number;
              processedJobs: number;
              failedJobs: number;
            }) => ({
              id: w.id,
              name: w.name,
              queues: w.queues,
              lastSeen: w.lastSeen,
              activeJobs: w.activeJobs,
              processedJobs: w.processedJobs,
              failedJobs: w.failedJobs,
            })
          ),
        truncated: workers.length > 100,
      },
      crons: {
        total: crons.length,
        list: crons
          .slice(0, 100)
          .map(
            (c: {
              name: string;
              queue: string;
              schedule: string | null;
              repeatEvery: number | null;
              nextRun: number;
              executions: number;
            }) => ({
              name: c.name,
              queue: c.queue,
              schedule: c.schedule ?? null,
              repeatEvery: c.repeatEvery ?? null,
              nextRun: c.nextRun,
              executions: c.executions,
            })
          ),
        truncated: crons.length > 100,
      },
      storage,
      timestamp: Date.now(),
    },
    200,
    corsOrigins
  );
}

/** Dashboard queues endpoint - paginated queues with per-queue stats */
export function dashboardQueuesEndpoint(
  queueManager: QueueManager,
  limit: number,
  offset: number,
  corsOrigins?: Set<string>
): Response {
  const allQueues = queueManager.listQueues();
  const total = allQueues.length;
  const queueNames = allQueues.slice(offset, offset + limit);
  const perQueueStats = queueManager.getPerQueueStats();

  const queues = queueNames.map((name: string) => {
    const stats = perQueueStats.get(name);
    return {
      name,
      waiting: stats?.waiting ?? 0,
      delayed: stats?.delayed ?? 0,
      active: stats?.active ?? 0,
      dlq: stats?.dlq ?? 0,
      paused: queueManager.isPaused(name),
    };
  });

  return jsonResponse(
    { ok: true, queues, total, limit, offset, timestamp: Date.now() },
    200,
    corsOrigins
  );
}

/** Dashboard single queue detail endpoint */
export function dashboardQueueDetailEndpoint(
  queueManager: QueueManager,
  queue: string,
  includeJobs: boolean,
  corsOrigins?: Set<string>
): Response {
  const rawCounts = queueManager.getQueueJobCounts(queue);
  const paused = queueManager.isPaused(queue);
  const pv = pausedView(rawCounts.waiting, rawCounts.prioritized, paused);
  const counts = {
    ...rawCounts,
    waiting: pv.waiting,
    prioritized: pv.prioritized,
    paused: pv.paused,
  };
  const dlqJobs = queueManager.getDlq(queue, 10);
  const priorityCounts = queueManager.getCountsPerPriority(queue);

  const result: Record<string, unknown> = {
    ok: true,
    name: queue,
    counts,
    paused,
    priorityCounts,
    dlqPreview: dlqJobs.map(
      (j: { id: string; data: unknown; attempts: number; createdAt: number }) => ({
        id: j.id,
        data: j.data,
        attempts: j.attempts,
        createdAt: j.createdAt,
      })
    ),
    timestamp: Date.now(),
  };

  if (includeJobs) {
    const waiting = queueManager.getJobs(queue, { state: 'waiting', end: 10 });
    const active = queueManager.getJobs(queue, { state: 'active', end: 10 });
    const delayed = queueManager.getJobs(queue, { state: 'delayed', end: 10 });
    const pausedJobs = paused ? queueManager.getJobs(queue, { state: 'paused', end: 10 }) : [];
    const toSummary = (j: {
      id: string;
      priority: number;
      createdAt: number;
      runAt: number;
      attempts: number;
      progress: number;
    }) => ({
      id: j.id,
      priority: j.priority,
      createdAt: j.createdAt,
      runAt: j.runAt,
      attempts: j.attempts,
      progress: j.progress,
    });
    result.jobs = {
      waiting: waiting.map(toSummary),
      active: active.map(toSummary),
      delayed: delayed.map(toSummary),
      paused: pausedJobs.map(toSummary),
    };
  }

  return jsonResponse(result, 200, corsOrigins);
}
