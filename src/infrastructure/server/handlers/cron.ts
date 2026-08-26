/**
 * Cron Command Handlers
 * Cron, CronGet, CronDelete, CronList
 */

import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import * as resp from '../../../domain/types/response';
import { normalizeLegacyJobPayload } from '../../../domain/types/job';
import type { HandlerContext } from '../types';
import type { CronJob, CronJobInput } from '../../../domain/types/cron';
import { sanitizeServerError } from '../errors';

type DurableCronManager = HandlerContext['queueManager'] & {
  addCronDurable?: (input: CronJobInput) => Promise<CronJob>;
  getCronDurable?: (name: string) => Promise<CronJob | undefined>;
  removeCronDurable?: (name: string) => Promise<boolean>;
  listCronsDurable?: () => Promise<CronJob[]>;
};

function cronData(cron: CronJob) {
  return {
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
  };
}

/** Handle Cron command - add cron job */
export function handleCron(
  cmd: Extract<Command, { cmd: 'Cron' }>,
  ctx: HandlerContext,
  reqId?: string
): Response | Promise<Response> {
  try {
    const payload = normalizeLegacyJobPayload({ name: cmd.jobName, data: cmd.data });
    const input: CronJobInput = {
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
    };
    const manager = ctx.queueManager as DurableCronManager;
    const complete = (cron: CronJob, existed: boolean): Response => {
      manager.emitDashboardEvent(existed ? 'cron:updated' : 'cron:created', {
        name: cron.name,
        queue: cron.queue,
        pattern: cron.schedule ?? undefined,
        every: cron.repeatEvery ?? undefined,
        nextRun: cron.nextRun,
      });
      return { ok: true, cron: cronData(cron), reqId };
    };
    if (manager.addCronDurable && manager.getCronDurable) {
      const addCronDurable = manager.addCronDurable.bind(manager);
      const getCronDurable = manager.getCronDurable.bind(manager);
      return (async () => {
        const existed = (await getCronDurable(cmd.name)) !== undefined;
        return complete(await addCronDurable(input), existed);
      })();
    }
    const existing = manager.getCron(cmd.name);
    return complete(manager.addCron(input), existing !== undefined);
  } catch (err) {
    return resp.error(sanitizeServerError(err), reqId);
  }
}

/** Handle CronGet command - get single cron job by name */
export function handleCronGet(
  cmd: Extract<Command, { cmd: 'CronGet' }>,
  ctx: HandlerContext,
  reqId?: string
): Response | Promise<Response> {
  const manager = ctx.queueManager as DurableCronManager;
  const complete = (cron: CronJob | undefined): Response => {
    if (!cron) {
      return resp.error('Cron job not found', reqId);
    }
    return { ok: true, cron: cronData(cron), reqId } as Response;
  };
  return manager.getCronDurable
    ? manager.getCronDurable(cmd.name).then(complete)
    : complete(manager.getCron(cmd.name));
}

/** Handle CronDelete command - delete cron job */
export function handleCronDelete(
  cmd: Extract<Command, { cmd: 'CronDelete' }>,
  ctx: HandlerContext,
  reqId?: string
): Response | Promise<Response> {
  const manager = ctx.queueManager as DurableCronManager;
  const complete = (removed: boolean): Response => {
    if (removed) manager.emitDashboardEvent('cron:deleted', { name: cmd.name });
    return removed ? resp.ok(undefined, reqId) : resp.error('Cron job not found', reqId);
  };
  return manager.removeCronDurable
    ? manager.removeCronDurable(cmd.name).then(complete)
    : complete(manager.removeCron(cmd.name));
}

/** Handle CronList command - list cron jobs */
export function handleCronList(
  _cmd: Extract<Command, { cmd: 'CronList' }>,
  ctx: HandlerContext,
  reqId?: string
): Response | Promise<Response> {
  const manager = ctx.queueManager as DurableCronManager;
  const complete = (crons: CronJob[]): Response => ({
    ok: true,
    crons: crons.map(cronData),
    reqId,
  });
  return manager.listCronsDurable
    ? manager.listCronsDurable().then(complete)
    : complete(manager.listCrons());
}
