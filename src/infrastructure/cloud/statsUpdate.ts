/**
 * stats_update — lightweight payload sent every 15s via WS
 * Used by evaluator.go for alert conditions:
 *   - Error rate → totalCompleted + totalFailed
 *   - Queue depth → waiting + active + delayed (global + per-queue)
 *   - DLQ count → dlq (global + per-queue)
 *   - Throughput → completePerSec
 */

import type { QueueManager } from '../../application/queueManager';
import { throughputTracker } from '../../application/throughputTracker';

export function buildStatsUpdate(qm: QueueManager) {
  const s = qm.getStats();
  const rates = throughputTracker.getRates();
  const queueNames = qm.listQueues();
  const countsByQueue = qm.getAllQueueJobCounts();

  const queues = queueNames.flatMap((name) => {
    const c = countsByQueue.get(name);
    if (!c) return [];
    return {
      name,
      waiting: c.waiting,
      active: c.active,
      delayed: c.delayed,
      dlq: c.failed,
      paused: qm.isPaused(name),
      totalCompleted: c.totalCompleted,
      totalFailed: c.totalFailed,
    };
  });

  return {
    type: 'stats_update',
    stats: {
      waiting: s.waiting,
      active: s.active,
      delayed: s.delayed,
      dlq: s.dlq,
      totalCompleted: Number(s.totalCompleted),
      totalFailed: Number(s.totalFailed),
      totalPushed: Number(s.totalPushed),
      stalled: qm.getMemoryStats().stalledCandidates,
      paused: queues.filter((q) => q.paused).length,
      uptime: s.uptime,
    },
    throughput: {
      pushPerSec: rates.pushPerSec,
      completePerSec: rates.completePerSec,
      failPerSec: rates.failPerSec,
    },
    queues,
  };
}
