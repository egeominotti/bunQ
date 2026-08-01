import { jobId } from '../../../domain/types/job';
import type { CloudCommandHandler } from '../types/command';
import { mapCloudCommandJob } from './jobMapper';

export const JOB_COMMANDS: Partial<Record<string, CloudCommandHandler>> = {
  'job:cancel': async (queueManager, command) => {
    const ok = await queueManager.cancel(jobId(command.jobId ?? ''));
    return { cancelled: ok };
  },
  'job:promote': async (queueManager, command) => {
    const ok = await queueManager.promote(jobId(command.jobId ?? ''));
    return { promoted: ok };
  },
  'job:push': async (queueManager, command) => {
    const job = await queueManager.push(command.queue ?? '', {
      data: command.data ?? {},
      priority: command.priority,
      delay: command.delay,
    });
    return { jobId: String(job.id), queue: command.queue };
  },
  'job:priority': async (queueManager, command) => {
    const ok = await queueManager.changePriority(jobId(command.jobId ?? ''), command.priority ?? 0);
    return { changed: ok };
  },
  'job:discard': async (queueManager, command) => {
    const ok = await queueManager.discard(jobId(command.jobId ?? ''));
    return { discarded: ok };
  },
  'job:delay': async (queueManager, command) => {
    await queueManager.changeDelay(jobId(command.jobId ?? ''), command.delay ?? 0);
    return { delayed: true };
  },
  'job:updateData': async (queueManager, command) => {
    const ok = await queueManager.updateJobData(jobId(command.jobId ?? ''), command.data);
    return { updated: ok };
  },
  'job:clearLogs': (queueManager, command) => {
    queueManager.clearLogs(jobId(command.jobId ?? ''), command.keepLogs);
    return { cleared: true };
  },
  'job:retry': (queueManager, command) => {
    if (command.queue) {
      const count = queueManager.retryDlq(
        command.queue,
        command.jobId ? jobId(command.jobId) : undefined
      );
      return { retried: count };
    }
    return { retried: 0 };
  },
  'job:logs': (queueManager, command) => ({
    logs: queueManager.getLogs(jobId(command.jobId ?? '')),
  }),
  'job:result': (queueManager, command) => ({
    result: queueManager.getResult(jobId(command.jobId ?? '')) ?? null,
  }),
  'job:list': (queueManager, command) => {
    const limit = command.limit ?? 50;
    const offset = command.offset ?? 0;
    const states = command.state
      ? command.state.split(',')
      : ['waiting', 'active', 'delayed', 'completed', 'failed'];
    const jobs = queueManager.getJobs(command.queue ?? '', {
      state: states,
      start: offset,
      end: offset + limit - 1,
    });
    return {
      jobs: jobs.map(mapCloudCommandJob),
      total: queueManager.count(command.queue ?? ''),
      offset,
      limit,
    };
  },
  'job:get': async (queueManager, command) => {
    const id = jobId(command.jobId ?? '');
    const job = await queueManager.getJob(id);
    if (!job) return { job: null };
    return {
      job: {
        ...mapCloudCommandJob(job),
        logs: queueManager.getLogs(id),
        result: queueManager.getResult(id) ?? null,
      },
    };
  },
  'job:listAll': (queueManager, command) => {
    const limit = command.limit ?? 50;
    const offset = command.offset ?? 0;
    const states = command.state
      ? command.state.split(',')
      : ['waiting', 'active', 'delayed', 'completed', 'failed'];
    const allJobs: ReturnType<typeof mapCloudCommandJob>[] = [];
    for (const name of queueManager.listQueues()) {
      const jobs = queueManager.getJobs(name, { state: states, start: 0, end: 999 });
      for (const job of jobs) allJobs.push(mapCloudCommandJob(job));
    }
    allJobs.sort((a, b) => b.timestamp - a.timestamp);
    return {
      jobs: allJobs.slice(offset, offset + limit),
      total: allJobs.length,
      offset,
      limit,
    };
  },
  'dlq:retry': (queueManager, command) => ({
    retried: queueManager.retryDlq(
      command.queue ?? '',
      command.jobId ? jobId(command.jobId) : undefined
    ),
  }),
  'dlq:purge': (queueManager, command) => ({
    purged: queueManager.purgeDlq(command.queue ?? ''),
  }),
};
