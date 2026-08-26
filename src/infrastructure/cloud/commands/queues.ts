import { DEFAULT_DLQ_CONFIG } from '../../../domain/types/dlq';
import { DEFAULT_STALL_CONFIG } from '../../../domain/types/stall';
import type { CloudCommandHandler } from '../types/command';
import { mapCloudCommandJob } from './jobMapper';

export const QUEUE_COMMANDS: Partial<Record<string, CloudCommandHandler>> = {
  'queue:pause': async (adapter, command) => {
    await adapter.pause(command.queue ?? '', true);
    return { queue: command.queue, paused: true };
  },
  'queue:resume': async (adapter, command) => {
    await adapter.pause(command.queue ?? '', false);
    return { queue: command.queue, paused: false };
  },
  'queue:drain': async (adapter, command) => {
    const count = await adapter.drain(command.queue ?? '');
    return { queue: command.queue, drained: count };
  },
  'queue:clean': async (adapter, command) => {
    const ids = await adapter.clean(
      command.queue ?? '',
      command.graceMs ?? 0,
      command.state,
      command.limit
    );
    return { queue: command.queue, cleaned: ids.length, ids };
  },
  'queue:obliterate': async (adapter, command) => {
    await adapter.obliterate(command.queue ?? '');
    return { queue: command.queue, obliterated: true };
  },
  'queue:promoteAll': async (adapter, command) => {
    const limit = command.limit ?? 1000;
    const promoted = await adapter.promoteAll(command.queue ?? '', limit);
    return { queue: command.queue, promoted };
  },
  'queue:retryCompleted': async (adapter, command) => {
    const count = await adapter.retryCompleted(command.queue ?? '');
    return { queue: command.queue, retried: count };
  },
  'queue:rateLimit': async (adapter, command) => {
    await adapter.setRateLimit(command.queue ?? '', command.max ?? 100);
    return { queue: command.queue, rateLimit: command.max ?? 100 };
  },
  'queue:clearRateLimit': async (adapter, command) => {
    await adapter.setRateLimit(command.queue ?? '', null);
    return { queue: command.queue, rateLimit: null };
  },
  'queue:concurrency': async (adapter, command) => {
    await adapter.setConcurrency(command.queue ?? '', command.concurrency ?? 10);
    return { queue: command.queue, concurrency: command.concurrency ?? 10 };
  },
  'queue:clearConcurrency': async (adapter, command) => {
    await adapter.setConcurrency(command.queue ?? '', null);
    return { queue: command.queue, concurrency: null };
  },
  'queue:stallConfig': async (adapter, command) => {
    await adapter.setStallConfig(command.queue ?? '', command.config ?? {});
    return { queue: command.queue, stallConfig: command.config };
  },
  'queue:dlqConfig': async (adapter, command) => {
    await adapter.setDlqConfig(command.queue ?? '', command.config ?? {});
    return { queue: command.queue, dlqConfig: command.config };
  },
  'queue:detail': async (adapter, command) => {
    const queue = command.queue ?? '';
    const source = await adapter.readSnapshotSource([queue]);
    const view = source.queues.find(({ name }) => name === queue);
    const counts = view?.counts ?? {
      waiting: 0,
      prioritized: 0,
      delayed: 0,
      active: 0,
      completed: 0,
      failed: 0,
      'waiting-children': 0,
      totalCompleted: 0,
      totalFailed: 0,
    };
    const dlqEntries = source.dlqEntries.filter((entry) => entry.job.queue === queue).slice(0, 50);
    const jobs = await adapter.listJobs(queue, {
      state: ['waiting', 'active', 'delayed', 'completed', 'failed'],
      start: 0,
      end: 50,
    });
    return {
      queue,
      paused: view?.paused ?? false,
      counts,
      stallConfig: {
        enabled: view?.stallConfig.enabled ?? DEFAULT_STALL_CONFIG.enabled,
        stallInterval: view?.stallConfig.stallInterval ?? DEFAULT_STALL_CONFIG.stallInterval,
        maxStalls: view?.stallConfig.maxStalls ?? DEFAULT_STALL_CONFIG.maxStalls,
      },
      dlqConfig: {
        maxRetries: view?.dlqConfig.maxAutoRetries ?? DEFAULT_DLQ_CONFIG.maxAutoRetries,
        maxAge: view?.dlqConfig.maxAge ?? DEFAULT_DLQ_CONFIG.maxAge ?? 0,
      },
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
  'queue:list': async (adapter) => {
    const source = await adapter.readSnapshotSource();
    const queues = source.queues.map(({ name, counts, paused }) => ({
      name,
      ...counts,
      paused,
    }));
    return { queues };
  },
  'stats:refresh': async (adapter) => (await adapter.readSnapshotSource()).stats,
};
