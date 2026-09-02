import type { Job, JobId } from '../../../domain/types/job';
import type { GetJobsContext } from '../../types/query';
import {
  collectJobsByState,
  collectTemporalJobs,
  collectWaitingChildrenJobs,
  compareJobsByCreatedAt,
  tagState,
} from './collect';

function querySqliteByLogicalState(
  storage: NonNullable<GetJobsContext['storage']>,
  queue: string,
  sqlFilteredStates: string[],
  opts: {
    limit: number;
    offset: number;
    asc: boolean;
    now: number;
    excludedIds: ReadonlySet<JobId>;
  }
): Job[] {
  if (opts.excludedIds.size === 0) {
    return storage.queryJobsByLogicalStates(queue, sqlFilteredStates, opts);
  }

  const pageEnd = opts.offset + opts.limit;
  const jobs = storage.queryJobsByLogicalStates(queue, sqlFilteredStates, {
    limit: pageEnd + opts.excludedIds.size,
    offset: 0,
    asc: opts.asc,
    now: opts.now,
  });
  return jobs.filter((job) => !opts.excludedIds.has(job.id)).slice(opts.offset, pageEnd);
}

function mergePage(sqlJobs: Job[], extras: Job[], start: number, end: number, asc: boolean): Job[] {
  const byId = new Map<JobId, Job>();
  for (const job of sqlJobs) byId.set(job.id, job);
  for (const job of extras) byId.set(job.id, job);
  const merged = Array.from(byId.values());
  merged.sort((a, b) => compareJobsByCreatedAt(a, b, asc));
  return merged.slice(start, end);
}

function querySqlitePage(
  storage: NonNullable<GetJobsContext['storage']>,
  queue: string,
  opts: {
    start: number;
    end: number;
    asc: boolean;
    excludedIds: ReadonlySet<JobId>;
  }
): Job[] {
  if (opts.excludedIds.size === 0) {
    return storage.queryJobs(queue, {
      limit: opts.end - opts.start,
      offset: opts.start,
      asc: opts.asc,
    });
  }
  return storage
    .queryJobs(queue, { limit: opts.end + opts.excludedIds.size, offset: 0, asc: opts.asc })
    .filter((job) => !opts.excludedIds.has(job.id))
    .slice(opts.start, opts.end);
}

interface BufferedOverlay {
  readonly jobs: Job[];
  readonly currentIds: Set<JobId>;
}

function readyState(job: Job, paused: boolean, explicitStates: boolean, now: number): string {
  if (job.runAt > now) return 'delayed';
  if (paused && explicitStates) return 'paused';
  return job.priority > 0 ? 'prioritized' : 'waiting';
}

function projectBufferedJob(
  buffered: Job,
  states: string[] | null,
  paused: boolean,
  now: number,
  ctx: GetJobsContext
): { job: Job; state: string } | null {
  const location = ctx.jobIndex.get(buffered.id);
  if (!location) {
    return buffered.completedAt === null ? null : { job: buffered, state: 'completed' };
  }
  if (location.queueName !== buffered.queue) return null;

  if (location.type === 'processing') {
    const job = ctx.processingShards[location.shardIdx]?.get(buffered.id);
    return job?.queue === buffered.queue ? { job, state: 'active' } : null;
  }
  if (location.type === 'completed') {
    const job = ctx.completedJobsData.get(buffered.id) ?? buffered;
    return job.queue === buffered.queue ? { job, state: 'completed' } : null;
  }
  if (location.type !== 'queue') return null;

  const shard = ctx.shards[location.shardIdx];
  const waitingChildren =
    shard.waitingDeps.get(buffered.id) ?? shard.waitingChildren.get(buffered.id);
  if (waitingChildren?.queue === buffered.queue) {
    return { job: waitingChildren, state: 'waiting-children' };
  }
  const queued = shard.queues.get(buffered.queue)?.find(buffered.id);
  return queued ? { job: queued, state: readyState(queued, paused, states !== null, now) } : null;
}

function collectBufferedOverlay(
  storage: NonNullable<GetJobsContext['storage']>,
  queue: string,
  opts: {
    states: string[] | null;
    paused: boolean;
    now: number;
    ctx: GetJobsContext;
  }
): BufferedOverlay {
  const jobs: Job[] = [];
  const currentIds = new Set<JobId>();
  for (const buffered of storage.getBufferedJobs(queue)) {
    const projection = projectBufferedJob(buffered, opts.states, opts.paused, opts.now, opts.ctx);
    if (!projection) continue;
    currentIds.add(buffered.id);
    if (!opts.states || opts.states.includes(projection.state)) {
      jobs.push(...tagState([projection.job], projection.state));
    }
  }
  return { jobs, currentIds };
}

export function getJobs(
  queue: string,
  shardIdx: number,
  options: {
    state?: string | string[];
    start?: number;
    end?: number;
    asc?: boolean;
  },
  ctx: GetJobsContext
): Job[] {
  const { state, start = 0, end = 100, asc = true } = options;
  const states = !state
    ? null
    : Array.isArray(state)
      ? state.length === 0
        ? null
        : state
      : [state];
  const limit = end - start;
  const now = Date.now();

  if (ctx.storage) {
    const shard = ctx.shards[shardIdx];
    const isPaused = shard.getState(queue).paused;
    const buffered = collectBufferedOverlay(ctx.storage, queue, {
      states,
      paused: isPaused,
      now,
      ctx,
    });

    if (!states) {
      const dlq = tagState(shard.getDlq(queue), 'failed');
      const extras = [...dlq, ...buffered.jobs];
      const excludedIds = new Set<JobId>([...buffered.currentIds, ...dlq.map((job) => job.id)]);
      if (extras.length === 0) {
        return ctx.storage.queryJobs(queue, { limit, offset: start, asc });
      }
      const all = querySqlitePage(ctx.storage, queue, { start: 0, end, asc, excludedIds });
      return mergePage(all, extras, start, end, asc);
    }

    const sqlFilteredStates = states.filter(
      (state) =>
        state !== 'failed' &&
        state !== 'waiting-children' &&
        state !== 'paused' &&
        !(isPaused && (state === 'waiting' || state === 'prioritized'))
    );
    const parkedJobs =
      states.includes('waiting-children') || sqlFilteredStates.length > 0
        ? collectWaitingChildrenJobs(shard, queue)
        : [];
    const extras: Job[] = [...buffered.jobs];

    if (states.includes('failed')) extras.push(...tagState(shard.getDlq(queue), 'failed'));
    if (states.includes('waiting-children')) {
      extras.push(...tagState(parkedJobs, 'waiting-children'));
    }
    if (states.includes('paused') && isPaused) {
      const pausedJobs = collectTemporalJobs(
        shard,
        queue,
        { waiting: true, prioritized: true, delayed: false },
        now
      );
      extras.push(...tagState(pausedJobs, 'paused'));
    }

    const excludedIds = new Set<JobId>([
      ...buffered.currentIds,
      ...parkedJobs.map((job) => job.id),
      ...extras.map((job) => job.id),
    ]);

    if (extras.length === 0) {
      return sqlFilteredStates.length > 0
        ? querySqliteByLogicalState(ctx.storage, queue, sqlFilteredStates, {
            limit,
            offset: start,
            asc,
            now,
            excludedIds,
          })
        : [];
    }

    const sqlJobs =
      sqlFilteredStates.length > 0
        ? querySqliteByLogicalState(ctx.storage, queue, sqlFilteredStates, {
            limit: end,
            offset: 0,
            asc,
            now,
            excludedIds,
          })
        : [];

    return mergePage(sqlJobs, extras, start, end, asc);
  }

  const jobs = collectJobsByState(queue, shardIdx, states, ctx, now);
  jobs.sort((a, b) => compareJobsByCreatedAt(a, b, asc));
  return jobs.slice(start, end);
}
