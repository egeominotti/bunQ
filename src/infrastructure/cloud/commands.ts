/**
 * Cloud Command Definitions
 * Whitelisted commands that the dashboard can execute via WebSocket.
 */

import type { QueueManager } from '../../application/queueManager';
import type { Job } from '../../domain/types/job';
import { jobId } from '../../domain/types/job';
import type { CloudCommand } from './commandHandler';

type Handler = (qm: QueueManager, cmd: CloudCommand) => unknown;

/** Helper: derive job state from timestamps */
function deriveState(j: {
  completedAt?: number | null;
  startedAt?: number | null;
  runAt: number;
}): string {
  if (j.completedAt) return 'completed';
  if (j.startedAt) return 'active';
  if (j.runAt > Date.now()) return 'delayed';
  return 'waiting';
}

/** Helper: map job to frontend format (field names aligned with dashboard) */
// eslint-disable-next-line complexity
function mapJob(j: Job) {
  const data = j.data as Record<string, unknown> | undefined;
  const state = deriveState(j);
  const processTime = j.completedAt && j.startedAt ? j.completedAt - j.startedAt : undefined;
  return {
    id: String(j.id),
    name: (data?.name as string | undefined) ?? 'default',
    queueName: j.queue,
    _queue: j.queue,
    state,
    _status: state,
    data: data !== undefined ? JSON.stringify(data) : undefined,
    priority: j.priority,
    timestamp: j.createdAt,
    processedOn: j.startedAt ?? undefined,
    finishedOn: j.completedAt ?? undefined,
    runAt: j.runAt,
    failedReason:
      state === 'active' && j.attempts > 0 ? `Retry ${j.attempts}/${j.maxAttempts}` : undefined,
    attemptsMade: j.attempts,
    maxAttempts: j.maxAttempts,
    backoff: j.backoff,
    timeout: j.timeout ?? undefined,
    ttl: j.ttl ?? undefined,
    duration: processTime,
    waitTime: j.startedAt ? j.startedAt - j.createdAt : undefined,
    totalDuration: j.completedAt ? j.completedAt - j.createdAt : undefined,
    progress: j.progress || undefined,
    progressMessage: j.progressMessage ?? undefined,
    customId: j.customId ?? undefined,
    uniqueKey: j.uniqueKey ?? undefined,
    tags: j.tags.length > 0 ? j.tags : undefined,
    groupId: j.groupId ?? undefined,
    parentId: j.parentId ? String(j.parentId) : undefined,
    childrenIds: j.childrenIds.length > 0 ? j.childrenIds.map(String) : undefined,
    dependsOn: j.dependsOn.length > 0 ? j.dependsOn.map(String) : undefined,
    childrenCompleted: j.childrenCompleted > 0 ? j.childrenCompleted : undefined,
    lastHeartbeat: j.lastHeartbeat > 0 ? j.lastHeartbeat : undefined,
    stallCount: j.stallCount > 0 ? j.stallCount : undefined,
    stallTimeout: j.stallTimeout ?? undefined,
    removeOnComplete: j.removeOnComplete || undefined,
    removeOnFail: j.removeOnFail || undefined,
    lifo: j.lifo || undefined,
    backoffConfig: j.backoffConfig ?? undefined,
    repeat: j.repeat ?? undefined,
    stackTraceLimit: j.stackTraceLimit,
    keepLogs: j.keepLogs ?? undefined,
    sizeLimit: j.sizeLimit ?? undefined,
    failParentOnFailure: j.failParentOnFailure || undefined,
    removeDependencyOnFailure: j.removeDependencyOnFailure || undefined,
    continueParentOnFailure: j.continueParentOnFailure || undefined,
    ignoreDependencyOnFailure: j.ignoreDependencyOnFailure || undefined,
    deduplicationTtl: j.deduplicationTtl ?? undefined,
    deduplicationExtend: j.deduplicationExtend || undefined,
    deduplicationReplace: j.deduplicationReplace || undefined,
    debounceId: j.debounceId ?? undefined,
    debounceTtl: j.debounceTtl ?? undefined,
    timeline: j.timeline.length > 0 ? j.timeline : undefined,
  };
}

/** All whitelisted commands */
export const COMMANDS: Partial<Record<string, Handler>> = {
  // --- Queue control ---
  'queue:pause': (qm, cmd) => {
    qm.pause(cmd.queue ?? '');
    return { queue: cmd.queue, paused: true };
  },
  'queue:resume': (qm, cmd) => {
    qm.resume(cmd.queue ?? '');
    return { queue: cmd.queue, paused: false };
  },
  'queue:drain': (qm, cmd) => {
    const count = qm.drain(cmd.queue ?? '');
    return { queue: cmd.queue, drained: count };
  },
  'queue:clean': (qm, cmd) => {
    const ids = qm.clean(cmd.queue ?? '', cmd.graceMs ?? 0, cmd.state, cmd.limit);
    return { queue: cmd.queue, cleaned: ids.length, ids };
  },
  'queue:obliterate': (qm, cmd) => {
    qm.obliterate(cmd.queue ?? '');
    return { queue: cmd.queue, obliterated: true };
  },
  'queue:promoteAll': async (qm, cmd) => {
    const limit = cmd.limit ?? 1000;
    const jobs = qm.getJobs(cmd.queue ?? '', { state: ['delayed'], start: 0, end: limit - 1 });
    let promoted = 0;
    for (const j of jobs) {
      try {
        await qm.promote(j.id);
        promoted++;
      } catch {
        // skip
      }
    }
    return { queue: cmd.queue, promoted };
  },
  'queue:retryCompleted': (qm, cmd) => {
    const count = qm.retryCompleted(cmd.queue ?? '');
    return { queue: cmd.queue, retried: count };
  },

  // --- Queue config ---
  'queue:rateLimit': (qm, cmd) => {
    qm.setRateLimit(cmd.queue ?? '', cmd.max ?? 100);
    return { queue: cmd.queue, rateLimit: cmd.max ?? 100 };
  },
  'queue:clearRateLimit': (qm, cmd) => {
    qm.clearRateLimit(cmd.queue ?? '');
    return { queue: cmd.queue, rateLimit: null };
  },
  'queue:concurrency': (qm, cmd) => {
    qm.setConcurrency(cmd.queue ?? '', cmd.concurrency ?? 10);
    return { queue: cmd.queue, concurrency: cmd.concurrency ?? 10 };
  },
  'queue:clearConcurrency': (qm, cmd) => {
    qm.clearConcurrency(cmd.queue ?? '');
    return { queue: cmd.queue, concurrency: null };
  },
  'queue:stallConfig': (qm, cmd) => {
    qm.setStallConfig(cmd.queue ?? '', cmd.config ?? {});
    return { queue: cmd.queue, stallConfig: cmd.config };
  },
  'queue:dlqConfig': (qm, cmd) => {
    qm.setDlqConfig(cmd.queue ?? '', cmd.config ?? {});
    return { queue: cmd.queue, dlqConfig: cmd.config };
  },

  // --- Queue detail ---
  'queue:detail': (qm, cmd) => {
    const queue = cmd.queue ?? '';
    const counts = qm.getQueueJobCounts(queue);
    const paused = qm.isPaused(queue);
    const stallConfig = qm.getStallConfig(queue);
    const dlqConfig = qm.getDlqConfig(queue);
    const dlqEntries = qm.getDlqEntries(queue).slice(0, 50);
    const jobs = qm.getJobs(queue, {
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
      dlqEntries: dlqEntries.map((e) => ({
        jobId: String(e.job.id),
        reason: e.reason,
        error: e.error,
        enteredAt: e.enteredAt,
        retryCount: e.retryCount,
      })),
      jobs: jobs.map(mapJob),
    };
  },

  // --- Job operations ---
  'job:cancel': async (qm, cmd) => {
    const ok = await qm.cancel(jobId(cmd.jobId ?? ''));
    return { cancelled: ok };
  },
  'job:promote': async (qm, cmd) => {
    const ok = await qm.promote(jobId(cmd.jobId ?? ''));
    return { promoted: ok };
  },
  'job:push': async (qm, cmd) => {
    const job = await qm.push(cmd.queue ?? '', {
      data: cmd.data ?? {},
      priority: cmd.priority,
      delay: cmd.delay,
    });
    return { jobId: String(job.id), queue: cmd.queue };
  },
  'job:priority': async (qm, cmd) => {
    const ok = await qm.changePriority(jobId(cmd.jobId ?? ''), cmd.priority ?? 0);
    return { changed: ok };
  },
  'job:discard': async (qm, cmd) => {
    const ok = await qm.discard(jobId(cmd.jobId ?? ''));
    return { discarded: ok };
  },
  'job:delay': async (qm, cmd) => {
    await qm.changeDelay(jobId(cmd.jobId ?? ''), cmd.delay ?? 0);
    return { delayed: true };
  },
  'job:updateData': async (qm, cmd) => {
    const ok = await qm.updateJobData(jobId(cmd.jobId ?? ''), cmd.data);
    return { updated: ok };
  },
  'job:clearLogs': (qm, cmd) => {
    qm.clearLogs(jobId(cmd.jobId ?? ''), cmd.keepLogs);
    return { cleared: true };
  },

  // --- Job queries ---
  'job:retry': (qm, cmd) => {
    if (cmd.queue) {
      const count = qm.retryDlq(cmd.queue, cmd.jobId ? jobId(cmd.jobId) : undefined);
      return { retried: count };
    }
    return { retried: 0 };
  },
  'job:logs': (qm, cmd) => {
    const logs = qm.getLogs(jobId(cmd.jobId ?? ''));
    return { logs };
  },
  'job:result': (qm, cmd) => {
    const result = qm.getResult(jobId(cmd.jobId ?? ''));
    return { result: result ?? null };
  },
  'job:list': (qm, cmd) => {
    const limit = cmd.limit ?? 50;
    const offset = cmd.offset ?? 0;
    const states = cmd.state
      ? cmd.state.split(',')
      : ['waiting', 'active', 'delayed', 'completed', 'failed'];
    const jobs = qm.getJobs(cmd.queue ?? '', {
      state: states,
      start: offset,
      end: offset + limit - 1,
    });
    return { jobs: jobs.map(mapJob), total: qm.count(cmd.queue ?? ''), offset, limit };
  },
  'job:get': async (qm, cmd) => {
    const job = await qm.getJob(jobId(cmd.jobId ?? ''));
    if (!job) return { job: null };
    const mapped = mapJob(job);
    return {
      job: {
        ...mapped,
        logs: qm.getLogs(jobId(cmd.jobId ?? '')),
        result: qm.getResult(jobId(cmd.jobId ?? '')) ?? null,
      },
    };
  },

  // --- DLQ ---
  'dlq:retry': (qm, cmd) => {
    const count = qm.retryDlq(cmd.queue ?? '', cmd.jobId ? jobId(cmd.jobId) : undefined);
    return { retried: count };
  },
  'dlq:purge': (qm, cmd) => {
    const count = qm.purgeDlq(cmd.queue ?? '');
    return { purged: count };
  },

  // --- Cron ---
  'cron:upsert': (qm, cmd) => {
    qm.removeCron(cmd.name ?? '');
    const cron = qm.addCron({
      name: cmd.name ?? '',
      queue: cmd.queue ?? '',
      data: cmd.data ?? {},
      schedule: cmd.schedule,
    });
    return { name: cron.name, nextRun: cron.nextRun };
  },
  'cron:delete': (qm, cmd) => {
    const ok = qm.removeCron(cmd.name ?? '');
    return { deleted: ok };
  },

  // --- Webhooks ---
  'webhook:add': (qm, cmd) => {
    const wh = qm.webhookManager.add(
      cmd.url ?? '',
      cmd.events ?? [],
      cmd.queue ?? undefined,
      cmd.secret ?? undefined
    );
    return { id: wh.id, url: wh.url, events: wh.events };
  },
  'webhook:remove': (qm, cmd) => {
    const ok = qm.webhookManager.remove(cmd.webhookId ?? '');
    return { removed: ok };
  },
  'webhook:set-enabled': (qm, cmd) => {
    const ok = qm.webhookManager.setEnabled(cmd.webhookId ?? '', cmd.enabled ?? true);
    return { updated: ok };
  },

  // --- List all jobs across all queues ---
  'job:listAll': (qm, cmd) => {
    const limit = cmd.limit ?? 50;
    const offset = cmd.offset ?? 0;
    const states = cmd.state
      ? cmd.state.split(',')
      : ['waiting', 'active', 'delayed', 'completed', 'failed'];
    const queueNames = qm.listQueues();
    const allJobs: ReturnType<typeof mapJob>[] = [];
    for (const name of queueNames) {
      const jobs = qm.getJobs(name, { state: states, start: 0, end: 999 });
      for (const j of jobs) allJobs.push(mapJob(j));
    }
    // Sort by createdAt descending
    allJobs.sort((a, b) => b.timestamp - a.timestamp);
    return { jobs: allJobs.slice(offset, offset + limit), total: allJobs.length, offset, limit };
  },

  // --- Queue list ---
  'queue:list': (qm) => {
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
    return { queues };
  },

  // --- Stats & backup ---
  'stats:refresh': (qm) => {
    return qm.getStats();
  },
  's3:backup': async (qm) => {
    const handles = (qm as unknown as Record<string, unknown>).serverHandles as
      | { triggerBackup?: () => Promise<unknown> }
      | undefined;
    if (handles?.triggerBackup) {
      return await handles.triggerBackup();
    }
    return { error: 'S3 backup not configured' };
  },
};
