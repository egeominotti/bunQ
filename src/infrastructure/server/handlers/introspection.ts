/** Queue limit and deduplication introspection handlers. */

import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import * as resp from '../../../domain/types/response';
import type { HandlerContext } from '../types';
import { jobId } from '../../../domain/types/job';

export function handleGetQueueLimits(
  cmd: Extract<Command, { cmd: 'GetQueueLimits' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  return resp.data({ limits: ctx.queueManager.getQueueLimitStatus(cmd.queue, cmd.maxJobs) }, reqId);
}

export function handleGetDeduplicationJobId(
  cmd: Extract<Command, { cmd: 'GetDeduplicationJobId' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  return resp.data(
    { jobId: ctx.queueManager.getDeduplicationJobId(cmd.queue, cmd.deduplicationId) },
    reqId
  );
}

export async function handleRemoveDeduplicationKey(
  cmd: Extract<Command, { cmd: 'RemoveDeduplicationKey' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const count = await ctx.queueManager.removeDeduplicationKey(cmd.queue, cmd.deduplicationId);
  return resp.data({ count }, reqId);
}

export async function handleRemoveJobDeduplicationKey(
  cmd: Extract<Command, { cmd: 'RemoveJobDeduplicationKey' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const removed = await ctx.queueManager.removeJobDeduplicationKey(cmd.id);
  return resp.data({ removed }, reqId);
}

export async function handleMoveToWaitingChildren(
  cmd: Extract<Command, { cmd: 'MoveToWaitingChildren' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const moved = await ctx.queueManager.moveToWaitingChildren(jobId(cmd.id), cmd.token);
  return moved ? resp.data({ moved: true }, reqId) : resp.error('Job is not active', reqId);
}
