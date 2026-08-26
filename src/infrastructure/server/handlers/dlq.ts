/**
 * DLQ Command Handlers
 * Dlq, RetryDlq, PurgeDlq
 */

import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import * as resp from '../../../domain/types/response';
import { jobId } from '../../../domain/types/job';
import type { HandlerContext } from '../types';

/** Handle Dlq command - get DLQ jobs */
export function handleDlq(
  cmd: Extract<Command, { cmd: 'Dlq' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const entries = ctx.queueManager.getDlqEntries(cmd.queue, cmd.filter);
  const selected = cmd.count === undefined ? entries : entries.slice(0, Math.max(0, cmd.count));
  return {
    ok: true,
    jobs: selected.map((entry) => entry.job),
    entries: selected,
    reqId,
  } as Response;
}

export function handleGetDlqStats(
  cmd: Extract<Command, { cmd: 'GetDlqStats' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  return resp.data({ stats: ctx.queueManager.getDlqStats(cmd.queue) }, reqId);
}

/** Handle RetryDlq command - retry DLQ jobs */
export function handleRetryDlq(
  cmd: Extract<Command, { cmd: 'RetryDlq' }>,
  ctx: HandlerContext,
  reqId?: string
): Response | Promise<Response> {
  const jid = cmd.jobId ? jobId(cmd.jobId) : undefined;
  const manager = ctx.queueManager as typeof ctx.queueManager & {
    retryDlqDurable?: (
      queue: string,
      id?: ReturnType<typeof jobId>,
      count?: number,
      filter?: typeof cmd.filter
    ) => Promise<number>;
  };
  if (manager.retryDlqDurable) {
    return manager
      .retryDlqDurable(cmd.queue, jid, cmd.count, cmd.filter)
      .then((count) => retryDlqResponse(manager, cmd.queue, jid, count, reqId));
  }
  // #111-class: honour the caller's `count` cap instead of retrying the whole DLQ.
  const count = cmd.filter
    ? ctx.queueManager.retryDlqByFilter(cmd.queue, cmd.filter)
    : ctx.queueManager.retryDlq(cmd.queue, jid, cmd.count);
  return retryDlqResponse(manager, cmd.queue, jid, count, reqId);
}

function retryDlqResponse(
  manager: HandlerContext['queueManager'],
  queue: string,
  jid: ReturnType<typeof jobId> | undefined,
  count: number,
  reqId?: string
): Response {
  if (count > 0) {
    const event = jid ? 'dlq:retried' : 'dlq:retry-all';
    const data: Record<string, unknown> = { queue, count };
    if (jid) data.jobId = String(jid);
    manager.emitDashboardEvent(event, data);
  }
  return { ok: true, count, reqId };
}

/** Handle PurgeDlq command - clear DLQ */
export function handlePurgeDlq(
  cmd: Extract<Command, { cmd: 'PurgeDlq' }>,
  ctx: HandlerContext,
  reqId?: string
): Response | Promise<Response> {
  const manager = ctx.queueManager as typeof ctx.queueManager & {
    purgeDlqDurable?: (queue: string) => Promise<number>;
  };
  const complete = (count: number): Response => {
    if (count > 0) manager.emitDashboardEvent('dlq:purged', { queue: cmd.queue, count });
    return { ok: true, count, reqId };
  };
  return manager.purgeDlqDurable
    ? manager.purgeDlqDurable(cmd.queue).then(complete)
    : complete(manager.purgeDlq(cmd.queue));
}

export function handleRemoveDlqJob(
  cmd: Extract<Command, { cmd: 'RemoveDlqJob' }>,
  ctx: HandlerContext,
  reqId?: string
): Response | Promise<Response> {
  const id = jobId(cmd.jobId);
  const manager = ctx.queueManager as typeof ctx.queueManager & {
    removeDlqJobDurable?: (queue: string, id: ReturnType<typeof jobId>) => Promise<boolean>;
  };
  return manager.removeDlqJobDurable
    ? manager.removeDlqJobDurable(cmd.queue, id).then((removed) => resp.data({ removed }, reqId))
    : resp.data({ removed: manager.removeDlqJob(cmd.queue, id) }, reqId);
}

/** Handle RetryCompleted command - retry completed jobs */
export function handleRetryCompleted(
  cmd: Extract<Command, { cmd: 'RetryCompleted' }>,
  ctx: HandlerContext,
  reqId?: string
): Response | Promise<Response> {
  const jid = cmd.id ? jobId(cmd.id) : undefined;
  const options = {
    limit: cmd.count,
    timestamp: cmd.timestamp,
  };
  const manager = ctx.queueManager as typeof ctx.queueManager & {
    retryCompletedDurable?: (
      queue: string,
      id?: ReturnType<typeof jobId>,
      value?: typeof options
    ) => Promise<number>;
  };
  return manager.retryCompletedDurable
    ? manager
        .retryCompletedDurable(cmd.queue, jid, options)
        .then((count) => ({ ok: true, count, reqId }))
    : { ok: true, count: manager.retryCompleted(cmd.queue, jid, options), reqId };
}
