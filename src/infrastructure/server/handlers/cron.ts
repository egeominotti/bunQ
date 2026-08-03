/**
 * Cron Command Handlers
 * Cron, CronGet, CronDelete, CronList
 */

import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import * as resp from '../../../domain/types/response';
import { normalizeLegacyJobPayload } from '../../../domain/types/job';
import type { HandlerContext } from '../types';

/** Handle Cron command - add cron job */
export function handleCron(
  cmd: Extract<Command, { cmd: 'Cron' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  try {
    const payload = normalizeLegacyJobPayload({ name: cmd.jobName, data: cmd.data });
    const existing = ctx.queueManager.getCron(cmd.name);
    const cron = ctx.queueManager.addCron({
      name: cmd.name,
      jobName: payload.name,
      queue: cmd.queue,
      data: payload.data,
      schedule: cmd.schedule,
      repeatEvery: cmd.repeatEvery,
      priority: cmd.priority,
      maxLimit: cmd.maxLimit,
      timezone: cmd.timezone,
      uniqueKey: cmd.uniqueKey,
      dedup: cmd.dedup,
      skipMissedOnRestart: cmd.skipMissedOnRestart,
      immediately: cmd.immediately,
      skipIfNoWorker: cmd.skipIfNoWorker,
      preventOverlap: cmd.preventOverlap,
      jobOptions: cmd.jobOptions,
    });
    ctx.queueManager.emitDashboardEvent(existing ? 'cron:updated' : 'cron:created', {
      name: cron.name,
      queue: cron.queue,
      pattern: cron.schedule ?? undefined,
      every: cron.repeatEvery ?? undefined,
      nextRun: cron.nextRun,
    });
    return {
      ok: true,
      cron: {
        name: cron.name,
        jobName: cron.jobName,
        queue: cron.queue,
        schedule: cron.schedule,
        repeatEvery: cron.repeatEvery,
        nextRun: cron.nextRun,
        executions: cron.executions,
        maxLimit: cron.maxLimit,
        timezone: cron.timezone,
        priority: cron.priority,
      },
      reqId,
    };
  } catch (err) {
    return resp.error(err instanceof Error ? err.message : 'Failed to add cron', reqId);
  }
}

/** Handle CronGet command - get single cron job by name */
export function handleCronGet(
  cmd: Extract<Command, { cmd: 'CronGet' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const cron = ctx.queueManager.getCron(cmd.name);
  if (!cron) {
    return resp.error('Cron job not found', reqId);
  }
  return {
    ok: true,
    cron: {
      name: cron.name,
      jobName: cron.jobName,
      queue: cron.queue,
      schedule: cron.schedule,
      repeatEvery: cron.repeatEvery,
      nextRun: cron.nextRun,
      executions: cron.executions,
      maxLimit: cron.maxLimit,
      timezone: cron.timezone,
    },
    reqId,
  } as Response;
}

/** Handle CronDelete command - delete cron job */
export function handleCronDelete(
  cmd: Extract<Command, { cmd: 'CronDelete' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const removed = ctx.queueManager.removeCron(cmd.name);
  if (removed) ctx.queueManager.emitDashboardEvent('cron:deleted', { name: cmd.name });
  return removed ? resp.ok(undefined, reqId) : resp.error('Cron job not found', reqId);
}

/** Handle CronList command - list cron jobs */
export function handleCronList(
  _cmd: Extract<Command, { cmd: 'CronList' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const crons = ctx.queueManager.listCrons();
  return {
    ok: true,
    crons: crons.map((c) => ({
      name: c.name,
      jobName: c.jobName,
      queue: c.queue,
      schedule: c.schedule,
      repeatEvery: c.repeatEvery,
      nextRun: c.nextRun,
      executions: c.executions,
      maxLimit: c.maxLimit,
      timezone: c.timezone,
    })),
    reqId,
  };
}
