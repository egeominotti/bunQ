/**
 * Query Command Handlers
 * GetJob, GetState, GetResult, GetJobCounts, GetJobByCustomId, GetJobs, GetChildrenValues
 */

import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import * as resp from '../../../domain/types/response';
import { jobId } from '../../../domain/types/job';
import type { Job } from '../../../domain/types/job';
import { lastFailedReason } from '../../../domain/job/terminal';
import { pausedView } from '../../../shared/pausedView';
import type { HandlerContext } from '../types';
import { sanitizeServerError } from '../errors';

interface JobView extends Job {
  state?: string;
  returnvalue?: unknown;
  failedReason?: string;
}

function jobView(ctx: HandlerContext, job: Job, state?: string): JobView {
  return {
    ...job,
    ...(state === undefined ? {} : { state }),
    returnvalue: ctx.queueManager.getResult(job.id),
    failedReason: lastFailedReason(job),
  };
}

/** Handle GetJob command */
export async function handleGetJob(
  cmd: Extract<Command, { cmd: 'GetJob' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const jid = jobId(cmd.id);
  const job = await ctx.queueManager.getJob(jid);
  if (!job) return resp.error('Job not found', reqId);
  const state = await ctx.queueManager.getJobState(jid);
  return { ok: true, job: jobView(ctx, job, state), reqId } as Response;
}

/** Handle GetState command */
export async function handleGetState(
  cmd: Extract<Command, { cmd: 'GetState' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const state = await ctx.queueManager.getJobState(jobId(cmd.id));
  return { ok: true, id: cmd.id, state, reqId } as Response;
}

/** Handle GetResult command */
export async function handleGetResult(
  cmd: Extract<Command, { cmd: 'GetResult' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const result = await ctx.queueManager.getResultAsync(jobId(cmd.id));
  return { ok: true, id: cmd.id, result, reqId };
}

/** Handle GetJobCounts command */
export function handleGetJobCounts(
  cmd: Extract<Command, { cmd: 'GetJobCounts' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  // Get queue-specific counts
  const counts = ctx.queueManager.getQueueJobCounts(cmd.queue);
  const isPaused = ctx.queueManager.isPaused(cmd.queue);
  // When paused, ready jobs (waiting + prioritized) are reported under `paused` —
  // not in their own buckets, which would double-count (#92, BullMQ semantics).
  const pv = pausedView(counts.waiting, counts.prioritized, isPaused);
  return resp.counts(
    {
      waiting: pv.waiting,
      prioritized: pv.prioritized,
      delayed: counts.delayed,
      active: counts.active,
      completed: counts.completed,
      failed: counts.failed,
      'waiting-children': counts['waiting-children'],
      paused: pv.paused,
    },
    reqId
  );
}

/** Handle GetCountsPerPriority command */
export function handleGetCountsPerPriority(
  cmd: Extract<Command, { cmd: 'GetCountsPerPriority' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const counts = ctx.queueManager.getCountsPerPriority(cmd.queue);
  return { ok: true, queue: cmd.queue, counts, reqId } as Response;
}

/** Handle GetJobByCustomId command */
export function handleGetJobByCustomId(
  cmd: Extract<Command, { cmd: 'GetJobByCustomId' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const job = ctx.queueManager.getJobByCustomId(cmd.customId);
  return job ? resp.job(jobView(ctx, job), reqId) : resp.error('Job not found', reqId);
}

/** Handle GetJobs command - list jobs with filtering and pagination */
export async function handleGetJobs(
  cmd: Extract<Command, { cmd: 'GetJobs' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const stateFilter = cmd.state;
  const jobs = ctx.queueManager.getJobs(cmd.queue, {
    state: stateFilter,
    start: cmd.offset ?? 0,
    end: (cmd.offset ?? 0) + (cmd.limit ?? 100),
    asc: cmd.asc ?? true,
  });

  // Inject state into each job for display
  // When state is a single string, use it directly; otherwise resolve per-job
  const singleState = typeof stateFilter === 'string' ? stateFilter : undefined;
  const jobsWithState = await Promise.all(
    jobs.map(async (job) => {
      const state = singleState ?? (await ctx.queueManager.getJobState(job.id));
      return jobView(ctx, job, state);
    })
  );

  return resp.jobs(jobsWithState, reqId);
}

/** Handle GetChildrenValues command - batch get children values for a parent job */
export async function handleGetChildrenValues(
  cmd: Extract<Command, { cmd: 'GetChildrenValues' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  try {
    const values = await ctx.queueManager.getChildrenValues(jobId(cmd.id));
    return resp.data({ values }, reqId);
  } catch (error) {
    return resp.error(sanitizeServerError(error), reqId);
  }
}
