import type { Shard } from '../../../domain/queue/shard';
import type { Job } from '../../../domain/types/job';
import { compareSqliteBinaryText } from '../../../shared/serialization';
import type { GetJobsContext } from '../../types/query';

export function compareJobsByCreatedAt(a: Job, b: Job, asc: boolean): number {
  if (a.createdAt !== b.createdAt) {
    return asc ? a.createdAt - b.createdAt : b.createdAt - a.createdAt;
  }
  const byId = compareSqliteBinaryText(String(a.id), String(b.id));
  return asc ? byId : -byId;
}

function collectCompletedJobs(queue: string, ctx: GetJobsContext): Job[] {
  const jobs: Job[] = [];
  for (const [jobId, location] of ctx.jobIndex) {
    if (location.type === 'completed' && location.queueName === queue) {
      const job = ctx.storage?.getJob(jobId) ?? ctx.completedJobsData?.get(jobId) ?? null;
      if (job) jobs.push(job);
    }
  }
  return jobs;
}

function collectActiveJobs(queue: string, shardIndex: number, ctx: GetJobsContext): Job[] {
  const jobs: Job[] = [];
  for (const job of ctx.processingShards[shardIndex].values()) {
    if (job.queue === queue) jobs.push(job);
  }
  for (let index = 0; index < ctx.shardCount; index++) {
    if (index === shardIndex) continue;
    for (const job of ctx.processingShards[index].values()) {
      if (job.queue === queue) jobs.push(job);
    }
  }
  return jobs;
}

export function collectTemporalJobs(
  shard: Shard,
  queue: string,
  needs: { waiting: boolean; prioritized: boolean; delayed: boolean },
  now: number = Date.now()
): Job[] {
  const { waiting: needWaiting, prioritized: needPrioritized, delayed: needDelayed } = needs;
  const jobs: Job[] = [];
  for (const job of shard.getQueue(queue).values()) {
    const isDelayed = job.runAt > now;
    if (isDelayed && needDelayed) {
      jobs.push(job);
    } else if (!isDelayed && (job.priority > 0 ? needPrioritized : needWaiting)) {
      jobs.push(job);
    }
  }
  return jobs;
}

export function tagState(jobs: Job[], state: string): Job[] {
  for (const job of jobs) (job as unknown as Record<string, unknown>)._state = state;
  return jobs;
}

function tagTemporalState(jobs: Job[], now: number = Date.now()): void {
  for (const job of jobs) {
    const isDelayed = job.runAt > now;
    (job as unknown as Record<string, unknown>)._state = isDelayed
      ? 'delayed'
      : job.priority > 0
        ? 'prioritized'
        : 'waiting';
  }
}

export function collectWaitingChildrenJobs(shard: Shard, queue: string): Job[] {
  const jobs: Job[] = [];
  for (const job of shard.waitingDeps.values()) {
    if (job.queue === queue) jobs.push(job);
  }
  for (const job of shard.waitingChildren.values()) {
    if (job.queue === queue) jobs.push(job);
  }
  return jobs;
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

function resolveStateNeeds(states: string[] | null, paused: boolean): StateNeeds {
  const want = (state: string): boolean => !states || states.includes(state);
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

function collectPausedJobs(shard: Shard, queue: string, now: number): Job[] {
  const jobs = collectTemporalJobs(
    shard,
    queue,
    { waiting: true, prioritized: true, delayed: false },
    now
  );
  return tagState(jobs, 'paused');
}

export function collectJobsByState(
  queue: string,
  shardIndex: number,
  states: string[] | null,
  ctx: GetJobsContext,
  now: number = Date.now()
): Job[] {
  const shard = ctx.shards[shardIndex];
  const jobs: Job[] = [];
  const needs = resolveStateNeeds(states, shard.getState(queue).paused);

  if (needs.waiting || needs.prioritized || needs.delayed) {
    const temporal = collectTemporalJobs(
      shard,
      queue,
      { waiting: needs.waiting, prioritized: needs.prioritized, delayed: needs.delayed },
      now
    );
    tagTemporalState(temporal, now);
    jobs.push(...temporal);
  }
  if (needs.paused) jobs.push(...collectPausedJobs(shard, queue, now));
  if (needs.active) jobs.push(...tagState(collectActiveJobs(queue, shardIndex, ctx), 'active'));
  if (needs.failed) jobs.push(...tagState(shard.getDlq(queue), 'failed'));
  if (needs.completed) jobs.push(...tagState(collectCompletedJobs(queue, ctx), 'completed'));
  if (needs.waitingChildren) {
    jobs.push(...tagState(collectWaitingChildrenJobs(shard, queue), 'waiting-children'));
  }
  return jobs;
}
