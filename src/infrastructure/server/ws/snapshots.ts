import type { QueueManager } from '../../../application/queueManager';
import { throughputTracker } from '../../../application/throughputTracker';

export function buildStatsSnapshot(queueManager: QueueManager): Record<string, unknown> {
  const stats = queueManager.getStats();
  const rates = throughputTracker.getRates();
  const perQueue = queueManager.getPerQueueStats();
  const workerStats = queueManager.workerManager.getStats();
  const queues: Record<string, object> = {};
  for (const [name, value] of perQueue) {
    queues[name] = {
      waiting: value.waiting ?? 0,
      active: value.active ?? 0,
      delayed: value.delayed ?? 0,
      dlq: value.dlq ?? 0,
      paused: queueManager.isPaused(name),
    };
  }
  return {
    waiting: stats.waiting,
    active: stats.active,
    delayed: stats.delayed,
    completed: stats.completed,
    dlq: stats.dlq,
    totalPushed: Number(stats.totalPushed),
    totalCompleted: Number(stats.totalCompleted),
    totalFailed: Number(stats.totalFailed),
    pushPerSec: rates.pushPerSec,
    pullPerSec: rates.pullPerSec,
    uptime: stats.uptime,
    queues,
    workers: { total: workerStats.total, active: workerStats.active },
    cronJobs: queueManager.listCrons().length,
  };
}

export function buildHealthSnapshot(
  queueManager: QueueManager,
  connectionCount: number,
  connectionType: 'ws' | 'sse' = 'ws'
): Record<string, unknown> {
  const memory = process.memoryUsage();
  const storage = queueManager.getStorageStatus();
  return {
    ok: !storage.diskFull,
    uptime: Math.floor(process.uptime()),
    memory: {
      rss: Math.round(memory.rss / 1024 / 1024),
      heapUsed: Math.round(memory.heapUsed / 1024 / 1024),
    },
    connections: { [connectionType]: connectionCount },
  };
}

export function buildStorageSnapshot(queueManager: QueueManager): Record<string, unknown> {
  return {
    collections: queueManager.getMemoryStats(),
    diskFull: queueManager.getStorageStatus().diskFull,
  };
}
