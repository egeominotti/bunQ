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

    if (!states) {
      const dlq = tagState(shard.getDlq(queue), 'failed');
      if (dlq.length === 0) {
        return ctx.storage.queryJobs(queue, { limit, offset: start, asc });
      }
      const all = ctx.storage.queryJobs(queue, { limit: end, offset: 0, asc });
      return mergePage(all, dlq, start, end, asc);
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
    const parkedIds = new Set(parkedJobs.map((job) => job.id));
    const extras: Job[] = [];

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

    if (extras.length === 0) {
      return sqlFilteredStates.length > 0
        ? querySqliteByLogicalState(ctx.storage, queue, sqlFilteredStates, {
            limit,
            offset: start,
            asc,
            now,
            excludedIds: parkedIds,
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
            excludedIds: parkedIds,
          })
        : [];

    return mergePage(sqlJobs, extras, start, end, asc);
  }

  const jobs = collectJobsByState(queue, shardIdx, states, ctx, now);
  jobs.sort((a, b) => compareJobsByCreatedAt(a, b, asc));
  return jobs.slice(start, end);
}
