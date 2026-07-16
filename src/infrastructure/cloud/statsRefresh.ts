/**
 * stats:refresh command handler
 * Returns lightweight overview data for the dashboard via WS.
 * Heavy data (jobs, DLQ, locks) is fetched on-demand by other commands.
 */

import { hostname, arch, platform, cpus } from 'os';
import type { QueueManager } from '../../application/queueManager';
import { throughputTracker } from '../../application/throughputTracker';
import { latencyTracker } from '../../application/latencyTracker';
import { VERSION } from '../../shared/version';

/** Cached hostname — computed once */
const HOST = hostname();

/** Cached runtime info — computed once */
const RUNTIME = {
  bunVersion: typeof Bun !== 'undefined' ? Bun.version : 'unknown',
  os: platform(),
  arch: arch(),
  cpus: cpus().length,
};

export function buildStatsRefresh(qm: QueueManager) {
  const stats = qm.getStats();
  const memStats = qm.getMemoryStats();
  const rates = throughputTracker.getRates();
  const percentiles = latencyTracker.getPercentiles();
  const averages = latencyTracker.getAverages();
  const storage = qm.getStorageStatus();
  const mem = process.memoryUsage();
  const workerStats = qm.workerManager.getStats();

  const queueNames = qm.listQueues();
  const countsByQueue = qm.getAllQueueJobCounts();
  const queues = queueNames.flatMap((name) => {
    const counts = countsByQueue.get(name);
    if (!counts) return [];
    return {
      name,
      waiting: counts.waiting,
      prioritized: counts.prioritized,
      delayed: counts.delayed,
      active: counts.active,
      completed: counts.completed,
      failed: counts.failed,
      'waiting-children': counts['waiting-children'],
      paused: qm.isPaused(name),
      totalCompleted: counts.totalCompleted,
      totalFailed: counts.totalFailed,
    };
  });

  const crons = qm.listCrons();

  return {
    version: VERSION,
    hostname: HOST,
    pid: process.pid,
    timestamp: Date.now(),

    stats: {
      waiting: stats.waiting,
      prioritized: stats.prioritized,
      delayed: stats.delayed,
      active: stats.active,
      dlq: stats.dlq,
      completed: stats.completed,
      'waiting-children': stats['waiting-children'],
      stalled: memStats.stalledCandidates,
      paused: queues.filter((q) => q.paused).length,
      totalPushed: String(stats.totalPushed),
      totalPulled: String(stats.totalPulled),
      totalCompleted: String(stats.totalCompleted),
      totalFailed: String(stats.totalFailed),
      uptime: stats.uptime,
      cronJobs: stats.cronJobs,
      cronPending: stats.cronPending,
    },

    throughput: rates,
    latency: { averages, percentiles },

    memory: {
      heapUsed: Math.round((mem.heapUsed / 1024 / 1024) * 100) / 100,
      heapTotal: Math.round((mem.heapTotal / 1024 / 1024) * 100) / 100,
      rss: Math.round((mem.rss / 1024 / 1024) * 100) / 100,
      external: Math.round((mem.external / 1024 / 1024) * 100) / 100,
    },

    runtime: RUNTIME,
    connections: { tcp: 0, ws: 0, sse: 0 },

    queues,
    workers: workerStats,
    cronCount: crons.length,
    storage: { diskFull: storage.diskFull, error: storage.error },
  };
}
