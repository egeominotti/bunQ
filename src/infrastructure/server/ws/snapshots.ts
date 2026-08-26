import type { QueueManager } from '../../../application/queueManager';
import { throughputTracker } from '../../../application/throughputTracker';
import type { CronJob } from '../../../domain/types/cron';
import type { Worker } from '../../../domain/types/worker';
import { isStorageDegraded } from '../../../shared/storageHealth';

const WORKER_TIMEOUT_MS = parseInt(Bun.env.WORKER_TIMEOUT_MS ?? '30000', 10);

type DurableSnapshotManager = QueueManager & {
  listWorkersDurable?: () => Promise<Worker[]>;
  listCronsDurable?: () => Promise<CronJob[]>;
};

export function buildStatsSnapshot(
  queueManager: QueueManager
): Record<string, unknown> | Promise<Record<string, unknown>> {
  const manager = queueManager as DurableSnapshotManager;
  if (manager.listWorkersDurable || manager.listCronsDurable) {
    return Promise.all([
      manager.listWorkersDurable
        ? manager.listWorkersDurable()
        : Promise.resolve(manager.workerManager.list()),
      manager.listCronsDurable ? manager.listCronsDurable() : Promise.resolve(manager.listCrons()),
    ]).then(([workers, crons]) => statsSnapshot(manager, workers, crons.length));
  }
  return statsSnapshot(manager, manager.workerManager.list(), manager.listCrons().length);
}

function statsSnapshot(
  queueManager: QueueManager,
  workers: Worker[],
  cronJobs: number
): Record<string, unknown> {
  const stats = queueManager.getStats();
  const rates = throughputTracker.getRates();
  const perQueue = queueManager.getPerQueueStats();
  const now = Date.now();
  const activeWorkers = workers.filter((worker) => now - worker.lastSeen < WORKER_TIMEOUT_MS);
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
    workers: { total: workers.length, active: activeWorkers.length },
    cronJobs,
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
    ok: !isStorageDegraded(storage),
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
