import { jobId } from '../../../domain/types/job';
import type { CloudCommandHandler } from '../types/command';
import { mapCloudCommandJob } from './jobMapper';

function pageInteger(value: number | undefined, fallback: number): number {
  if (value === undefined || !Number.isFinite(value)) return fallback;
  return Math.min(Number.MAX_SAFE_INTEGER, Math.max(0, Math.floor(value)));
}

export const JOB_COMMANDS: Partial<Record<string, CloudCommandHandler>> = {
  'job:cancel': async (adapter, command) => {
    const ok = await adapter.cancel(jobId(command.jobId ?? ''));
    return { cancelled: ok };
  },
  'job:promote': async (adapter, command) => {
    const ok = await adapter.promote(jobId(command.jobId ?? ''));
    return { promoted: ok };
  },
  'job:push': async (adapter, command) => {
    const job = await adapter.push(command.queue ?? '', {
      name: command.name ?? 'default',
      data: command.data ?? {},
      priority: command.priority,
      delay: command.delay,
    });
    return { jobId: String(job.id), queue: command.queue };
  },
  'job:priority': async (adapter, command) => {
    const ok = await adapter.changePriority(jobId(command.jobId ?? ''), command.priority ?? 0);
    return { changed: ok };
  },
  'job:discard': async (adapter, command) => {
    const ok = await adapter.discard(jobId(command.jobId ?? ''));
    return { discarded: ok };
  },
  'job:delay': async (adapter, command) => {
    await adapter.changeDelay(jobId(command.jobId ?? ''), command.delay ?? 0);
    return { delayed: true };
  },
  'job:updateData': async (adapter, command) => {
    const ok = await adapter.updateData(jobId(command.jobId ?? ''), command.data);
    return { updated: ok };
  },
  'job:clearLogs': async (adapter, command) => {
    await adapter.clearLogs(jobId(command.jobId ?? ''), command.keepLogs);
    return { cleared: true };
  },
  'job:retry': async (adapter, command) => {
    if (command.queue) {
      const count = await adapter.retryDlq(
        command.queue,
        command.jobId ? jobId(command.jobId) : undefined
      );
      return { retried: count };
    }
    return { retried: 0 };
  },
  'job:logs': async (adapter, command) => ({
    logs: await adapter.getLogs(jobId(command.jobId ?? '')),
  }),
  'job:result': async (adapter, command) => ({
    result: await adapter.getResult(jobId(command.jobId ?? '')),
  }),
  'job:list': async (adapter, command) => {
    const limit = pageInteger(command.limit, 50);
    const offset = pageInteger(command.offset, 0);
    const end = Math.min(Number.MAX_SAFE_INTEGER, offset + limit);
    const states = command.state
      ? command.state.split(',')
      : ['waiting', 'active', 'delayed', 'completed', 'failed'];
    const jobs = await adapter.listJobs(command.queue ?? '', {
      state: states,
      start: offset,
      end,
    });
    const source = await adapter.readSnapshotSource();
    const counts = source.queues.find(({ name }) => name === (command.queue ?? ''))?.counts;
    return {
      jobs: jobs.map(mapCloudCommandJob),
      total: (counts?.waiting ?? 0) + (counts?.prioritized ?? 0) + (counts?.delayed ?? 0),
      offset,
      limit,
    };
  },
  'job:get': async (adapter, command) => {
    const id = jobId(command.jobId ?? '');
    const job = await adapter.getJob(id);
    if (!job) return { job: null };
    return {
      job: {
        ...mapCloudCommandJob(job),
        logs: await adapter.getLogs(id),
        result: await adapter.getResult(id),
      },
    };
  },
  'job:listAll': async (adapter, command) => {
    const limit = command.limit ?? 50;
    const offset = command.offset ?? 0;
    const states = command.state
      ? command.state.split(',')
      : ['waiting', 'active', 'delayed', 'completed', 'failed'];
    const allJobs: ReturnType<typeof mapCloudCommandJob>[] = [];
    const source = await adapter.readSnapshotSource();
    for (const { job, state } of source.jobs) {
      if (states.includes(state)) allJobs.push(mapCloudCommandJob(job));
    }
    allJobs.sort((a, b) => b.timestamp - a.timestamp);
    return {
      jobs: allJobs.slice(offset, offset + limit),
      total: allJobs.length,
      offset,
      limit,
    };
  },
  'dlq:retry': async (adapter, command) => ({
    retried: await adapter.retryDlq(
      command.queue ?? '',
      command.jobId ? jobId(command.jobId) : undefined
    ),
  }),
  'dlq:purge': async (adapter, command) => ({
    purged: await adapter.purgeDlq(command.queue ?? ''),
  }),
};
