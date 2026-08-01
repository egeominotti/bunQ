import type { CloudCommandHandler } from '../types/command';
import { mapCloudCommandJob } from './jobMapper';

export const QUEUE_COMMANDS: Partial<Record<string, CloudCommandHandler>> = {
  'queue:pause': (queueManager, command) => {
    queueManager.pause(command.queue ?? '');
    return { queue: command.queue, paused: true };
  },
  'queue:resume': (queueManager, command) => {
    queueManager.resume(command.queue ?? '');
    return { queue: command.queue, paused: false };
  },
  'queue:drain': (queueManager, command) => {
    const count = queueManager.drain(command.queue ?? '');
    return { queue: command.queue, drained: count };
  },
  'queue:clean': (queueManager, command) => {
    const ids = queueManager.clean(
      command.queue ?? '',
      command.graceMs ?? 0,
      command.state,
      command.limit
    );
    return { queue: command.queue, cleaned: ids.length, ids };
  },
  'queue:obliterate': (queueManager, command) => {
    queueManager.obliterate(command.queue ?? '');
    return { queue: command.queue, obliterated: true };
  },
  'queue:promoteAll': async (queueManager, command) => {
    const limit = command.limit ?? 1000;
    const jobs = queueManager.getJobs(command.queue ?? '', {
      state: ['delayed'],
      start: 0,
      end: limit - 1,
    });
    let promoted = 0;
    for (const job of jobs) {
      try {
        await queueManager.promote(job.id);
        promoted++;
      } catch {
        // Skip jobs that can no longer be promoted.
      }
    }
    return { queue: command.queue, promoted };
  },
  'queue:retryCompleted': (queueManager, command) => {
    const count = queueManager.retryCompleted(command.queue ?? '');
    return { queue: command.queue, retried: count };
  },
  'queue:rateLimit': (queueManager, command) => {
    queueManager.setRateLimit(command.queue ?? '', command.max ?? 100);
    return { queue: command.queue, rateLimit: command.max ?? 100 };
  },
  'queue:clearRateLimit': (queueManager, command) => {
    queueManager.clearRateLimit(command.queue ?? '');
    return { queue: command.queue, rateLimit: null };
  },
  'queue:concurrency': (queueManager, command) => {
    queueManager.setConcurrency(command.queue ?? '', command.concurrency ?? 10);
    return { queue: command.queue, concurrency: command.concurrency ?? 10 };
  },
  'queue:clearConcurrency': (queueManager, command) => {
    queueManager.clearConcurrency(command.queue ?? '');
    return { queue: command.queue, concurrency: null };
  },
  'queue:stallConfig': (queueManager, command) => {
    queueManager.setStallConfig(command.queue ?? '', command.config ?? {});
    return { queue: command.queue, stallConfig: command.config };
  },
  'queue:dlqConfig': (queueManager, command) => {
    queueManager.setDlqConfig(command.queue ?? '', command.config ?? {});
    return { queue: command.queue, dlqConfig: command.config };
  },
  'queue:detail': (queueManager, command) => {
    const queue = command.queue ?? '';
    const counts = queueManager.getQueueJobCounts(queue);
    const paused = queueManager.isPaused(queue);
    const stallConfig = queueManager.getStallConfig(queue);
    const dlqConfig = queueManager.getDlqConfig(queue);
    const dlqEntries = queueManager.getDlqEntries(queue).slice(0, 50);
    const jobs = queueManager.getJobs(queue, {
      state: ['waiting', 'active', 'delayed', 'completed', 'failed'],
      start: 0,
      end: 49,
    });
    return {
      queue,
      paused,
      counts,
      stallConfig: {
        enabled: stallConfig.enabled,
        stallInterval: stallConfig.stallInterval,
        maxStalls: stallConfig.maxStalls,
      },
      dlqConfig: { maxRetries: dlqConfig.maxAutoRetries, maxAge: dlqConfig.maxAge ?? 0 },
      dlqEntries: dlqEntries.map((entry) => ({
        jobId: String(entry.job.id),
        reason: entry.reason,
        error: entry.error,
        enteredAt: entry.enteredAt,
        retryCount: entry.retryCount,
      })),
      jobs: jobs.map(mapCloudCommandJob),
    };
  },
  'queue:list': (queueManager) => {
    const queueNames = queueManager.listQueues();
    const countsByQueue = queueManager.getAllQueueJobCounts();
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
        paused: queueManager.isPaused(name),
        totalCompleted: counts.totalCompleted,
        totalFailed: counts.totalFailed,
      };
    });
    return { queues };
  },
  'stats:refresh': (queueManager) => queueManager.getStats(),
};
