import type { QueueManager } from '../../../application/queueManager';
import type { CloudSnapshot } from '../types';
import type { CloudSnapshotSource } from '../queueAdapter/types';

export function collectQueueConfigs(
  queueManager: QueueManager,
  queueNames: Set<string>
): CloudSnapshot['queueConfigs'] {
  const configs: CloudSnapshot['queueConfigs'] = {};
  const perQueue = queueManager.getPerQueueStats();
  for (const name of queueNames) {
    try {
      const stall = queueManager.getStallConfig(name);
      const dlq = queueManager.getDlqConfig(name);
      const queueStats = perQueue.get(name);
      const limits = queueManager.getQueueLimits(name);
      configs[name] = {
        paused: queueManager.isPaused(name),
        rateLimit: limits.rateLimit,
        concurrencyLimit: limits.concurrencyLimit,
        concurrencyActive: queueStats?.active ?? 0,
        stallConfig: {
          enabled: stall.enabled,
          stallInterval: stall.stallInterval,
          maxStalls: stall.maxStalls,
          gracePeriod: stall.gracePeriod,
        },
        dlqConfig: {
          autoRetry: dlq.autoRetry,
          autoRetryInterval: dlq.autoRetryInterval,
          maxRetries: dlq.maxAutoRetries,
          maxAge: dlq.maxAge ?? 0,
          maxEntries: dlq.maxEntries,
        },
      };
    } catch {
      // Skip queue on error.
    }
  }
  return configs;
}

export function mapSnapshotQueueConfigs(
  queues: CloudSnapshotSource['queues']
): CloudSnapshot['queueConfigs'] {
  return Object.fromEntries(
    queues.map((queue) => [
      queue.name,
      {
        paused: queue.paused,
        rateLimit: queue.rateLimit,
        concurrencyLimit: queue.concurrencyLimit,
        concurrencyActive: queue.counts.active,
        stallConfig: {
          enabled: queue.stallConfig.enabled,
          stallInterval: queue.stallConfig.stallInterval,
          maxStalls: queue.stallConfig.maxStalls,
          gracePeriod: queue.stallConfig.gracePeriod,
        },
        dlqConfig: {
          autoRetry: queue.dlqConfig.autoRetry,
          autoRetryInterval: queue.dlqConfig.autoRetryInterval,
          maxRetries: queue.dlqConfig.maxAutoRetries,
          maxAge: queue.dlqConfig.maxAge ?? 0,
          maxEntries: queue.dlqConfig.maxEntries,
        },
      },
    ])
  );
}

export function collectWebhooks(queueManager: QueueManager): CloudSnapshot['webhooks'] {
  try {
    return queueManager.webhookManager.list().map((webhook) => ({
      id: webhook.id,
      url: webhook.url,
      events: webhook.events,
      queue: webhook.queue,
      enabled: webhook.enabled,
      successCount: webhook.successCount,
      failureCount: webhook.failureCount,
      lastTriggered: webhook.lastTriggered,
    }));
  } catch {
    return [];
  }
}

export function collectTopErrors(
  queueManager: QueueManager,
  dlqQueueNames: string[]
): CloudSnapshot['topErrors'] {
  if (dlqQueueNames.length === 0) return [];
  const errorMap = new Map<string, { count: number; queue: string; lastSeen: number }>();
  const entries = dlqQueueNames.flatMap((name) => {
    try {
      return queueManager.getDlqEntries(name);
    } catch {
      return [];
    }
  });
  for (const entry of entries) {
    const message = entry.error ?? entry.reason;
    const existing = errorMap.get(message);
    if (existing) {
      existing.count++;
      if (entry.enteredAt > existing.lastSeen) {
        existing.lastSeen = entry.enteredAt;
        existing.queue = entry.job.queue;
      }
    } else {
      errorMap.set(message, {
        count: 1,
        queue: entry.job.queue,
        lastSeen: entry.enteredAt,
      });
    }
  }
  return [...errorMap.entries()]
    .map(([message, data]) => ({ message, ...data }))
    .sort((a, b) => b.count - a.count);
}

export function mapSnapshotTopErrors(
  entries: CloudSnapshotSource['dlqEntries']
): CloudSnapshot['topErrors'] {
  const errors = new Map<string, { count: number; queue: string; lastSeen: number }>();
  for (const entry of entries) {
    const message = entry.error ?? entry.reason;
    const current = errors.get(message);
    if (!current) {
      errors.set(message, { count: 1, queue: entry.job.queue, lastSeen: entry.enteredAt });
      continue;
    }
    current.count++;
    if (entry.enteredAt > current.lastSeen) {
      current.lastSeen = entry.enteredAt;
      current.queue = entry.job.queue;
    }
  }
  return [...errors]
    .map(([message, data]) => ({ message, ...data }))
    .sort((a, b) => b.count - a.count);
}
