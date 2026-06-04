/**
 * Query Operations
 * Get job, get result, get progress
 */

import type { Job, JobId } from '../../domain/types/job';
import { JobState } from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import type { Shard } from '../../domain/queue/shard';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import { type RWLock, withReadLock } from '../../shared/lock';
import type { SetLike, MapLike } from '../../shared/lru';
import { shardIndex } from '../../shared/hash';

/** Context for query operations */
export interface QueryContext {
  storage: SqliteStorage | null;
  shards: Shard[];
  shardLocks: RWLock[];
  processingShards: Map<JobId, Job>[];
  processingLocks: RWLock[];
  jobIndex: Map<JobId, JobLocation>;
  completedJobs: SetLike<JobId>;
  completedJobsData: MapLike<JobId, Job>;
  jobResults: MapLike<JobId, unknown>;
  customIdMap: MapLike<string, JobId>;
}

/** Get job by ID */
export async function getJob(jobId: JobId, ctx: QueryContext): Promise<Job | null> {
  const location = ctx.jobIndex.get(jobId);
  if (!location) {
    // Fallback: jobIndex may not be populated after restart for completed/DLQ jobs.
    // Consult SQLite directly so getJob survives recovery.
    if (ctx.storage) {
      const job = ctx.storage.getJob(jobId);
      if (job) return job;
      const dlqEntry = ctx.storage.getDlqEntry(jobId);
      if (dlqEntry) return dlqEntry.job;
    }
    return ctx.completedJobsData.get(jobId) ?? null;
  }

  switch (location.type) {
    case 'queue': {
      return await withReadLock(ctx.shardLocks[location.shardIdx], () => {
        const shard = ctx.shards[location.shardIdx];
        return (
          shard.getQueue(location.queueName).find(jobId) ?? shard.waitingDeps.get(jobId) ?? null
        );
      });
    }
    case 'processing': {
      return await withReadLock(ctx.processingLocks[location.shardIdx], () => {
        return ctx.processingShards[location.shardIdx].get(jobId) ?? null;
      });
    }
    case 'completed':
      return ctx.storage?.getJob(jobId) ?? ctx.completedJobsData.get(jobId) ?? null;
    case 'dlq': {
      if (ctx.storage) {
        const dlqEntry = ctx.storage.getDlqEntry(jobId);
        if (dlqEntry) return dlqEntry.job;
        const job = ctx.storage.getJob(jobId);
        if (job) return job;
      }
      const dlqShardIdx = shardIndex(location.queueName);
      const dlqJobs = ctx.shards[dlqShardIdx].getDlq(location.queueName);
      return dlqJobs.find((j) => j.id === jobId) ?? null;
    }
  }
}

/** Get job result */
export function getJobResult(jobId: JobId, ctx: QueryContext): unknown {
  return ctx.jobResults.get(jobId) ?? ctx.storage?.getResult(jobId);
}

/** Get job by custom ID */
export function getJobByCustomId(customId: string, ctx: QueryContext): Job | null {
  const jobId = ctx.customIdMap.get(customId);
  if (!jobId) return null;

  const location = ctx.jobIndex.get(jobId);
  if (!location) return null;

  if (location.type === 'queue') {
    const shard = ctx.shards[location.shardIdx];
    return shard.getQueue(location.queueName).find(jobId) ?? shard.waitingDeps.get(jobId) ?? null;
  }
  if (location.type === 'processing') {
    return ctx.processingShards[location.shardIdx].get(jobId) ?? null;
  }
  if (location.type === 'completed') {
    return ctx.storage?.getJob(jobId) ?? ctx.completedJobsData.get(jobId) ?? null;
  }
  if (location.type === 'dlq') {
    if (ctx.storage) {
      const job = ctx.storage.getJob(jobId);
      if (job) return job;
    }
    const dlqShardIdx = shardIndex(location.queueName);
    const dlqJobs = ctx.shards[dlqShardIdx].getDlq(location.queueName);
    return dlqJobs.find((j) => j.id === jobId) ?? null;
  }
  return null;
}

/** Get job progress */
export function getJobProgress(
  jobId: JobId,
  ctx: QueryContext
): { progress: number; message: string | null } | null {
  const location = ctx.jobIndex.get(jobId);
  if (location?.type !== 'processing') return null;

  const job = ctx.processingShards[location.shardIdx].get(jobId);
  if (!job) return null;

  return { progress: job.progress, message: job.progressMessage };
}

/** Extended context for getJobs (needs SHARD_COUNT) */
export interface GetJobsContext extends QueryContext {
  shardCount: number;
}

/** Resolve job state from SQLite when jobIndex has no entry (post-restart recovery). */
function resolveStateFromStorage(
  jobId: JobId,
  storage: QueryContext['storage']
): JobState | 'unknown' {
  if (!storage) return 'unknown';
  if (storage.hasDlqEntry(jobId)) return JobState.Failed;
  const persisted = storage.getJobStateRaw(jobId);
  if (persisted === 'completed') return JobState.Completed;
  if (persisted === 'active') return JobState.Active;
  if (persisted !== 'waiting' && persisted !== 'delayed') return 'unknown';
  const row = storage.getJob(jobId);
  if (!row) return 'unknown';
  if (row.runAt > Date.now()) return JobState.Delayed;
  return row.priority > 0 ? JobState.Prioritized : JobState.Waiting;
}

/** Get job state by ID */
export async function getJobState(jobId: JobId, ctx: QueryContext): Promise<JobState | 'unknown'> {
  const location = ctx.jobIndex.get(jobId);

  // Check completed set first (fast path)
  if (ctx.completedJobs.has(jobId)) {
    return JobState.Completed;
  }

  if (!location) {
    return resolveStateFromStorage(jobId, ctx.storage);
  }

  switch (location.type) {
    case 'queue': {
      // Check if job is delayed, waiting, or waiting for children/deps
      const result = await withReadLock(ctx.shardLocks[location.shardIdx], () => {
        const shard = ctx.shards[location.shardIdx];
        const queueJob = shard.getQueue(location.queueName).find(jobId);
        if (queueJob) return { job: queueJob, waitingDeps: false, waitingChildren: false };
        const depsJob = shard.waitingDeps.get(jobId);
        if (depsJob) return { job: depsJob, waitingDeps: true, waitingChildren: false };
        const childrenJob = shard.waitingChildren.get(jobId);
        if (childrenJob) return { job: childrenJob, waitingDeps: false, waitingChildren: true };
        return null;
      });
      if (!result) return 'unknown';
      if (result.waitingDeps || result.waitingChildren) return 'waiting-children' as JobState;
      const now = Date.now();
      if (result.job.runAt > now) return JobState.Delayed;
      return result.job.priority > 0 ? JobState.Prioritized : JobState.Waiting;
    }
    case 'processing':
      return JobState.Active;
    case 'completed':
      return JobState.Completed;
    case 'dlq':
      return JobState.Failed;
  }
}

/** Collect completed jobs for a queue from index + storage */
function collectCompletedJobs(queue: string, ctx: GetJobsContext, maxCollect: number): Job[] {
  const jobs: Job[] = [];
  for (const [jId, location] of ctx.jobIndex) {
    if (location.type === 'completed' && location.queueName === queue) {
      const job = ctx.storage?.getJob(jId) ?? ctx.completedJobsData?.get(jId) ?? null;
      if (job) {
        jobs.push(job);
        if (jobs.length >= maxCollect) break;
      }
    }
  }
  return jobs;
}

/** Collect active jobs for a queue across all processing shards */
function collectActiveJobs(
  queue: string,
  shardIdx: number,
  ctx: GetJobsContext,
  maxCollect: number
): Job[] {
  const jobs: Job[] = [];
  // Own shard first (most likely location)
  for (const job of ctx.processingShards[shardIdx].values()) {
    if (job.queue === queue) jobs.push(job);
    if (jobs.length >= maxCollect) return jobs;
  }
  // Other shards
  for (let i = 0; i < ctx.shardCount; i++) {
    if (i === shardIdx) continue;
    for (const job of ctx.processingShards[i].values()) {
      if (job.queue === queue) jobs.push(job);
      if (jobs.length >= maxCollect) return jobs;
    }
  }
  return jobs;
}

/** Collect waiting/delayed/prioritized jobs in a single pass */
function collectTemporalJobs(
  shard: Shard,
  queue: string,
  needs: { waiting: boolean; prioritized: boolean; delayed: boolean },
  maxCollect: number
): Job[] {
  const { waiting: needWaiting, prioritized: needPrioritized, delayed: needDelayed } = needs;
  const now = Date.now();
  const jobs: Job[] = [];
  for (const j of shard.getQueue(queue).values()) {
    if (jobs.length >= maxCollect) break;
    const isDelayed = j.runAt > now;
    if (isDelayed && needDelayed) {
      jobs.push(j);
    } else if (!isDelayed) {
      // BullMQ v5: priority>0 → "prioritized", priority=0 → "waiting"
      if (j.priority > 0 ? needPrioritized : needWaiting) {
        jobs.push(j);
      }
    }
  }
  return jobs;
}

/** Collect jobs from in-memory structures by state filter */
function tagState(jobs: Job[], state: string): Job[] {
  for (const j of jobs) (j as unknown as Record<string, unknown>)._state = state;
  return jobs;
}

/** Tag temporal jobs with their actual state based on runAt/priority */
function tagTemporalState(jobs: Job[]): void {
  const now = Date.now();
  for (const j of jobs) {
    const isDelayed = j.runAt > now;
    (j as unknown as Record<string, unknown>)._state = isDelayed
      ? 'delayed'
      : j.priority > 0
        ? 'prioritized'
        : 'waiting';
  }
}

/** Collect waiting-children jobs from deps and children maps */
function collectWaitingChildrenFromShard(shard: Shard, queue: string, max: number): Job[] {
  const wcJobs: Job[] = [];
  for (const job of shard.waitingDeps.values()) {
    if (job.queue === queue) wcJobs.push(job);
    if (wcJobs.length >= max) return wcJobs;
  }
  for (const job of shard.waitingChildren.values()) {
    if (job.queue === queue) wcJobs.push(job);
    if (wcJobs.length >= max) return wcJobs;
  }
  return wcJobs;
}

interface StateNeeds {
  waiting: boolean;
  prioritized: boolean;
  delayed: boolean;
  paused: boolean;
  active: boolean;
  failed: boolean;
  completed: boolean;
  waitingChildren: boolean;
}

/**
 * Resolve which sources to collect for a state filter.
 *
 * When the queue is paused, an EXPLICIT waiting/prioritized query returns nothing:
 * those jobs are reported under 'paused' instead (BullMQ semantics, #92). An
 * unfiltered query (states === null) still lists them by their temporal state.
 */
function resolveStateNeeds(states: string[] | null, paused: boolean): StateNeeds {
  const want = (s: string): boolean => !states || states.includes(s);
  const suppressReady = !!states && paused;
  return {
    waiting: want('waiting') && !suppressReady,
    prioritized: want('prioritized') && !suppressReady,
    delayed: want('delayed'),
    paused: !!states && states.includes('paused') && paused,
    active: want('active'),
    failed: want('failed'),
    completed: want('completed'),
    waitingChildren: want('waiting-children'),
  };
}

/** When paused, the queue's ready jobs (waiting + prioritized) ARE the paused set (#92). */
function collectPausedJobs(shard: Shard, queue: string, maxPerSource: number): Job[] {
  const pausedJobs = collectTemporalJobs(
    shard,
    queue,
    { waiting: true, prioritized: true, delayed: false },
    maxPerSource
  );
  return tagState(pausedJobs, 'paused');
}

function collectJobsByState(
  queue: string,
  shardIdx: number,
  states: string[] | null,
  ctx: GetJobsContext,
  maxPerSource = Infinity
): Job[] {
  const shard = ctx.shards[shardIdx];
  const jobs: Job[] = [];
  const need = resolveStateNeeds(states, shard.getState(queue).paused);

  if (need.waiting || need.prioritized || need.delayed) {
    const temporal = collectTemporalJobs(
      shard,
      queue,
      { waiting: need.waiting, prioritized: need.prioritized, delayed: need.delayed },
      maxPerSource
    );
    tagTemporalState(temporal);
    jobs.push(...temporal);
  }
  if (need.paused) {
    jobs.push(...collectPausedJobs(shard, queue, maxPerSource));
  }
  if (need.active) {
    jobs.push(...tagState(collectActiveJobs(queue, shardIdx, ctx, maxPerSource), 'active'));
  }
  if (need.failed) {
    const dlq = shard.getDlq(queue);
    jobs.push(...tagState(maxPerSource < dlq.length ? dlq.slice(0, maxPerSource) : dlq, 'failed'));
  }
  if (need.completed) {
    jobs.push(...tagState(collectCompletedJobs(queue, ctx, maxPerSource), 'completed'));
  }
  if (need.waitingChildren) {
    jobs.push(
      ...tagState(collectWaitingChildrenFromShard(shard, queue, maxPerSource), 'waiting-children')
    );
  }
  return jobs;
}

/** Collect waiting-children jobs from in-memory shard maps */
function collectWaitingChildrenJobs(shard: Shard, queue: string): Job[] {
  const jobs: Job[] = [];
  for (const job of shard.waitingDeps.values()) {
    if (job.queue === queue) jobs.push(job);
  }
  for (const job of shard.waitingChildren.values()) {
    if (job.queue === queue) jobs.push(job);
  }
  return jobs;
}

/** Query SQLite with priority/waiting translation */
function querySqliteWithPriority(
  storage: NonNullable<GetJobsContext['storage']>,
  queue: string,
  sqlFilteredStates: string[],
  opts: { limit: number; offset: number; asc: boolean }
): Job[] {
  const hasPrioritized = sqlFilteredStates.includes('prioritized');
  const hasWaiting = sqlFilteredStates.includes('waiting');

  if (!hasPrioritized && !hasWaiting) {
    if (sqlFilteredStates.length === 1) {
      return storage.queryJobs(queue, { state: sqlFilteredStates[0], ...opts });
    }
    return storage.queryJobs(queue, { states: sqlFilteredStates, ...opts });
  }

  // Map 'prioritized' to 'waiting' for SQLite, then post-filter by priority
  const sqlStates = sqlFilteredStates
    .map((s) => (s === 'prioritized' ? 'waiting' : s))
    .filter((s, i, arr) => arr.indexOf(s) === i);

  const overFetchOpts = { ...opts, limit: opts.limit * 2 };
  let jobs =
    sqlStates.length === 1
      ? storage.queryJobs(queue, { state: sqlStates[0], ...overFetchOpts })
      : storage.queryJobs(queue, { states: sqlStates, ...overFetchOpts });

  if (hasPrioritized && !hasWaiting) {
    jobs = jobs.filter((j) => j.priority > 0);
  } else if (hasWaiting && !hasPrioritized) {
    jobs = jobs.filter((j) => j.priority <= 0);
  }
  return jobs.slice(0, opts.limit);
}

/** Merge SQL rows with in-memory extras (each gathered from index 0), sort by
 *  createdAt, and paginate [start, end) once — so offset-unaware extras don't
 *  duplicate or drop rows across pages (#92). */
function mergePage(sqlJobs: Job[], extras: Job[], start: number, end: number, asc: boolean): Job[] {
  const merged = sqlJobs.concat(extras);
  merged.sort((a, b) => (asc ? a.createdAt - b.createdAt : b.createdAt - a.createdAt));
  return merged.slice(start, end);
}

/** Get jobs from queue with filters */
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

  if (ctx.storage) {
    const shard = ctx.shards[shardIdx];
    const isPaused = shard.getState(queue).paused;
    const maxPerSource = end + 1;

    // Derived sources are NOT offset-aware (the DLQ and the paused/waiting-children
    // views come from in-memory maps). When any contributes, we must gather [0, end)
    // from EVERY source, merge, sort, then slice [start, end) exactly once — pushing
    // `offset` into the SQL query would drop rows and duplicate across pages (#92).
    if (!states) {
      const dlq = tagState(shard.getDlq(queue), 'failed');
      if (dlq.length === 0) {
        return ctx.storage.queryJobs(queue, { limit, offset: start, asc });
      }
      const all = ctx.storage.queryJobs(queue, { limit: end, offset: 0, asc });
      return mergePage(all, dlq, start, end, asc);
    }

    // States with no jobs-table row are collected from in-memory sources:
    //  - 'failed'           -> DLQ (the failed job lives in the dlq table) (#92)
    //  - 'waiting-children' -> deps/children maps
    //  - 'paused'           -> when paused, the would-be-waiting jobs ARE the paused set (#92)
    const extras: Job[] = [];
    if (states.includes('failed')) {
      extras.push(...tagState(shard.getDlq(queue), 'failed'));
    }
    if (states.includes('waiting-children')) {
      extras.push(...collectWaitingChildrenJobs(shard, queue));
    }
    if (states.includes('paused') && isPaused) {
      const pausedJobs = collectTemporalJobs(
        shard,
        queue,
        { waiting: true, prioritized: true, delayed: false },
        maxPerSource
      );
      extras.push(...tagState(pausedJobs, 'paused'));
    }

    // jobs-table states. A paused queue reports its waiting/prioritized jobs under
    // 'paused', so they must not also surface in the waiting/prioritized lists (#92).
    const sqlFilteredStates = states.filter(
      (s) =>
        s !== 'failed' &&
        s !== 'waiting-children' &&
        s !== 'paused' &&
        !(isPaused && (s === 'waiting' || s === 'prioritized'))
    );

    // Fast path: only jobs-table states, no derived sources — SQL paginates directly.
    if (extras.length === 0) {
      return sqlFilteredStates.length > 0
        ? querySqliteWithPriority(ctx.storage, queue, sqlFilteredStates, {
            limit,
            offset: start,
            asc,
          })
        : [];
    }

    const sqlJobs =
      sqlFilteredStates.length > 0
        ? querySqliteWithPriority(ctx.storage, queue, sqlFilteredStates, {
            limit: end,
            offset: 0,
            asc,
          })
        : [];

    return mergePage(sqlJobs, extras, start, end, asc);
  }

  // In-memory path (embedded mode only)
  const maxPerSource = end + 1;
  const jobs = collectJobsByState(queue, shardIdx, states, ctx, maxPerSource);
  jobs.sort((a, b) => (asc ? a.createdAt - b.createdAt : b.createdAt - a.createdAt));
  return jobs.slice(start, end);
}
