import type { QueueManager } from '../../../application/queueManager';
import type { CloudSnapshot } from '../types';
import type { CloudSnapshotSource } from '../queueAdapter/types';

const previousQueueTotals = new Map<
  string,
  { completed: number; failed: number; pushed: number; timestamp: number }
>();
const previousQueueWaiting = new Map<string, { waiting: number; timestamp: number }>();

export function collectQueueThroughput(
  queues: Array<{ name: string; totalCompleted: number; totalFailed: number }>
): CloudSnapshot['queueThroughput'] {
  const now = Date.now();
  const result: CloudSnapshot['queueThroughput'] = {};
  for (const queue of queues) {
    const previous = previousQueueTotals.get(queue.name);
    const total = queue.totalCompleted + queue.totalFailed;
    if (previous) {
      const elapsedSec = (now - previous.timestamp) / 1000;
      if (elapsedSec > 0.5) {
        const completeDelta = queue.totalCompleted - previous.completed;
        const failDelta = queue.totalFailed - previous.failed;
        result[queue.name] = {
          pushPerSec: Math.round(((completeDelta + failDelta) / elapsedSec) * 100) / 100,
          completePerSec: Math.round((completeDelta / elapsedSec) * 100) / 100,
          failPerSec: Math.round((failDelta / elapsedSec) * 100) / 100,
          errorRate: total > 0 ? Math.round((queue.totalFailed / total) * 10000) / 10000 : 0,
        };
      }
    }
    previousQueueTotals.set(queue.name, {
      completed: queue.totalCompleted,
      failed: queue.totalFailed,
      pushed: queue.totalCompleted + queue.totalFailed,
      timestamp: now,
    });
  }
  return result;
}

export function collectWorkerUtilization(
  queueManager: QueueManager
): CloudSnapshot['workerUtilization'] {
  return queueManager.workerManager.list().map((worker) => ({
    id: worker.id,
    name: worker.name,
    utilization:
      worker.concurrency > 0 ? Math.round((worker.activeJobs / worker.concurrency) * 100) / 100 : 0,
  }));
}

export function mapSnapshotWorkerUtilization(
  workers: CloudSnapshotSource['workers']
): CloudSnapshot['workerUtilization'] {
  return workers.map((worker) => ({
    id: worker.id,
    name: worker.name,
    utilization:
      worker.concurrency > 0 ? Math.round((worker.activeJobs / worker.concurrency) * 100) / 100 : 0,
  }));
}

export function collectDurationHistogram(
  recentJobs: CloudSnapshot['recentJobs']
): CloudSnapshot['durationHistogram'] {
  const histogram = { lt100ms: 0, lt1s: 0, lt10s: 0, lt60s: 0, gt60s: 0 };
  for (const job of recentJobs) {
    if (!job.duration) continue;
    if (job.duration < 100) histogram.lt100ms++;
    else if (job.duration < 1000) histogram.lt1s++;
    else if (job.duration < 10000) histogram.lt10s++;
    else if (job.duration < 60000) histogram.lt60s++;
    else histogram.gt60s++;
  }
  return histogram;
}

export function collectQueueWaitTime(
  recentJobs: CloudSnapshot['recentJobs']
): CloudSnapshot['queueWaitTime'] {
  const byQueue = new Map<string, number[]>();
  for (const job of recentJobs) {
    if (!job.startedAt || !job.createdAt) continue;
    const wait = job.startedAt - job.createdAt;
    if (wait < 0) continue;
    let waits = byQueue.get(job.queue);
    if (!waits) {
      waits = [];
      byQueue.set(job.queue, waits);
    }
    waits.push(wait);
  }
  const result: CloudSnapshot['queueWaitTime'] = {};
  for (const [queue, waits] of byQueue) {
    const sum = waits.reduce((a, b) => a + b, 0);
    result[queue] = {
      avgMs: Math.round(sum / waits.length),
      maxMs: Math.max(...waits),
      minMs: Math.min(...waits),
    };
  }
  return result;
}

export function collectQueueRetryRate(
  recentJobs: CloudSnapshot['recentJobs']
): CloudSnapshot['queueRetryRate'] {
  const byQueue = new Map<string, { retrying: number; total: number }>();
  for (const job of recentJobs) {
    let entry = byQueue.get(job.queue);
    if (!entry) {
      entry = { retrying: 0, total: 0 };
      byQueue.set(job.queue, entry);
    }
    entry.total++;
    if (job.attempts > 0) entry.retrying++;
  }
  const result: CloudSnapshot['queueRetryRate'] = {};
  for (const [queue, data] of byQueue) {
    result[queue] = {
      retryRate: data.total > 0 ? Math.round((data.retrying / data.total) * 10000) / 10000 : 0,
      retrying: data.retrying,
      total: data.total,
    };
  }
  return result;
}

export function collectBacklogVelocity(
  queues: Array<{ name: string; waiting: number }>
): CloudSnapshot['queueBacklogVelocity'] {
  const now = Date.now();
  const result: CloudSnapshot['queueBacklogVelocity'] = {};
  for (const queue of queues) {
    const previous = previousQueueWaiting.get(queue.name);
    if (previous) {
      const elapsedMin = (now - previous.timestamp) / 60000;
      if (elapsedMin > 0.1) {
        const delta = queue.waiting - previous.waiting;
        const deltaPerMin = Math.round((delta / elapsedMin) * 100) / 100;
        result[queue.name] = {
          deltaWaiting: delta,
          deltaPerMin,
          trend: deltaPerMin > 5 ? 'growing' : deltaPerMin < -5 ? 'shrinking' : 'stable',
        };
      }
    }
    previousQueueWaiting.set(queue.name, { waiting: queue.waiting, timestamp: now });
  }
  return result;
}

export function collectPriorityDistribution(
  recentJobs: CloudSnapshot['recentJobs']
): CloudSnapshot['queuePriorityDistribution'] {
  const result: CloudSnapshot['queuePriorityDistribution'] = {};
  for (const job of recentJobs) {
    result[job.queue] ??= {};
    result[job.queue][job.priority] = (result[job.queue][job.priority] ?? 0) + 1;
  }
  return result;
}
