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
  const jobs = ctx.queueManager.getDlq(cmd.queue, cmd.count);
  return resp.jobs(jobs, reqId);
}

/** Handle RetryDlq command - retry DLQ jobs */
export function handleRetryDlq(
  cmd: Extract<Command, { cmd: 'RetryDlq' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const jid = cmd.jobId ? jobId(cmd.jobId) : undefined;
  // #111-class: honour the caller's `count` cap instead of retrying the whole DLQ.
  const count = ctx.queueManager.retryDlq(cmd.queue, jid, cmd.count);
  if (count > 0) {
    const event = jid ? 'dlq:retried' : 'dlq:retry-all';
    const data: Record<string, unknown> = { queue: cmd.queue, count };
    if (jid) data.jobId = String(jid);
    ctx.queueManager.emitDashboardEvent(event, data);
  }
  return { ok: true, count, reqId };
}

/** Handle PurgeDlq command - clear DLQ */
export function handlePurgeDlq(
  cmd: Extract<Command, { cmd: 'PurgeDlq' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const count = ctx.queueManager.purgeDlq(cmd.queue);
  if (count > 0) ctx.queueManager.emitDashboardEvent('dlq:purged', { queue: cmd.queue, count });
  return { ok: true, count, reqId };
}

/** Handle RetryCompleted command - retry completed jobs */
export function handleRetryCompleted(
  cmd: Extract<Command, { cmd: 'RetryCompleted' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const jid = cmd.id ? jobId(cmd.id) : undefined;
  const count = ctx.queueManager.retryCompleted(cmd.queue, jid);
  return { ok: true, count, reqId };
}
