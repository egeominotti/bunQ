/**
 * Advanced Command Handlers
 * Job management, queue control, rate limiting
 */

import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import * as resp from '../../../domain/types/response';
import { jobId } from '../../../domain/types/job';
import type { HandlerContext } from '../types';
import { validateNumericField } from '../protocol';

/**
 * Coerce a value to a finite number, or return undefined if it can't be.
 * Guards config endpoints against non-numeric input (e.g. `"abc"`) that would
 * otherwise reach numeric comparisons as NaN and silently break behaviour
 * (a string `stallInterval` disabled stall detection entirely).
 */
function toFiniteNumber(value: unknown): number | undefined {
  if (value === undefined || value === null) return undefined;
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? n : undefined;
}

/**
 * Sanitize the numeric fields of a config object: coerce numeric strings, drop
 * non-numeric garbage (so the manager's merge keeps the existing/default value
 * instead of storing NaN). Booleans and unknown keys pass through untouched.
 */
function sanitizeConfigNumbers<T extends Record<string, unknown>>(
  config: T,
  numericKeys: readonly string[]
): T {
  if (!config || typeof config !== 'object') return config;
  const numeric = new Set<string>(numericKeys);
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(config)) {
    if (!numeric.has(key)) {
      out[key] = value; // booleans / unknown keys pass through untouched
      continue;
    }
    if (value === null) {
      out[key] = null; // valid for nullable fields (e.g. dlq maxAge)
      continue;
    }
    const n = toFiniteNumber(value);
    // coerce numeric strings; omit non-numeric garbage so the manager's merge
    // keeps the existing/default value instead of storing NaN
    if (n !== undefined) out[key] = n;
  }
  return out as T;
}

// ============ Job Management ============

/** Handle Update command - update job data */
export async function handleUpdate(
  cmd: Extract<Command, { cmd: 'Update' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const success = await ctx.queueManager.updateJobData(jobId(cmd.id), cmd.data);
  if (success) ctx.queueManager.emitDashboardEvent('job:data-updated', { jobId: cmd.id });
  return success
    ? resp.ok(undefined, reqId)
    : resp.error('Job not found or cannot be updated', reqId);
}

/** Handle ChangePriority command */
export async function handleChangePriority(
  cmd: Extract<Command, { cmd: 'ChangePriority' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const success = await ctx.queueManager.changePriority(jobId(cmd.id), cmd.priority, cmd.lifo);
  if (success) {
    ctx.queueManager.emitDashboardEvent('job:priority-changed', {
      jobId: cmd.id,
      newPriority: cmd.priority,
    });
  }
  return success ? resp.ok(undefined, reqId) : resp.error('Job not found or not in queue', reqId);
}

/** Handle Promote command - move delayed to waiting */
export async function handlePromote(
  cmd: Extract<Command, { cmd: 'Promote' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const success = await ctx.queueManager.promote(jobId(cmd.id));
  if (success) ctx.queueManager.emitDashboardEvent('job:promoted', { jobId: cmd.id });
  return success ? resp.ok(undefined, reqId) : resp.error('Job not found or not delayed', reqId);
}

/** Handle UpdateParent command - update child's parent reference */
export async function handleUpdateParent(
  cmd: Extract<Command, { cmd: 'UpdateParent' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  try {
    await ctx.queueManager.updateJobParent(jobId(cmd.childId), jobId(cmd.parentId));
    return resp.ok(undefined, reqId);
  } catch {
    return resp.error('Failed to update parent', reqId);
  }
}

/** Handle MoveToDelayed command */
export async function handleMoveToDelayed(
  cmd: Extract<Command, { cmd: 'MoveToDelayed' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const id = jobId(cmd.id);
  const success = await ctx.queueManager.moveToDelayed(id, cmd.delay);
  if (success) {
    ctx.queueManager.emitDashboardEvent('job:moved-to-delayed', {
      jobId: cmd.id,
      delay: cmd.delay,
    });
    return resp.ok(undefined, reqId);
  }
  const state = await ctx.queueManager.getJobState(id);
  if (state === 'unknown') {
    return resp.error('Job not found', reqId);
  }
  return resp.error(`Job is not active (current state: ${state})`, reqId);
}

/** Handle Discard command - move to DLQ */
export async function handleDiscard(
  cmd: Extract<Command, { cmd: 'Discard' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const success = await ctx.queueManager.discard(jobId(cmd.id));
  if (success) ctx.queueManager.emitDashboardEvent('job:discarded', { jobId: cmd.id });
  return success ? resp.ok(undefined, reqId) : resp.error('Job not found', reqId);
}

/** Handle WaitJob command - wait for job completion (event-driven, no polling) */
export async function handleWaitJob(
  cmd: Extract<Command, { cmd: 'WaitJob' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const jid = jobId(cmd.id);
  // Bounded like PULL (which caps at 60s); waiting for completion is
  // legitimately longer, so the cap here is 10 min. validateNumericField also
  // rejects NaN/Infinity from hostile clients (a hand-rolled <0/>max check
  // would let NaN through and resolve instantly as a false "not completed").
  const timeoutError = validateNumericField(cmd.timeout, 'timeout', { min: 0, max: 600000 });
  if (timeoutError) return resp.error(timeoutError, reqId);
  const timeout = cmd.timeout ?? 30000;

  // First check if job exists and is already completed
  const job = await ctx.queueManager.getJob(jid);
  if (!job) return resp.error('Job not found', reqId);
  if (job.completedAt) {
    const result = ctx.queueManager.getResult(jid);
    return { ok: true, completed: true, result, reqId } as Response;
  }

  // Wait for completion event - event-driven, no polling
  const completed = await ctx.queueManager.waitForJobCompletion(jid, timeout);

  if (completed) {
    const result = ctx.queueManager.getResult(jid);
    return { ok: true, completed: true, result, reqId } as Response;
  }

  return { ok: true, completed: false, reqId } as Response;
}

// ============ Queue Control ============

/** Handle IsPaused command */
export function handleIsPaused(
  cmd: Extract<Command, { cmd: 'IsPaused' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const paused = ctx.queueManager.isPaused(cmd.queue);
  return { ok: true, paused, reqId } as Response;
}

/** Handle Obliterate command */
export function handleObliterate(
  cmd: Extract<Command, { cmd: 'Obliterate' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  ctx.queueManager.obliterate(cmd.queue);
  return resp.ok(undefined, reqId);
}

/** Handle ListQueues command */
export function handleListQueues(
  _cmd: Extract<Command, { cmd: 'ListQueues' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const queues = ctx.queueManager.listQueues();
  return { ok: true, queues, reqId } as Response;
}

/** Handle Clean command */
export function handleClean(
  cmd: Extract<Command, { cmd: 'Clean' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const ids = ctx.queueManager.clean(cmd.queue, cmd.grace, cmd.state, cmd.limit);
  if (ids.length > 0) {
    ctx.queueManager.emitDashboardEvent('queue:cleaned', {
      queue: cmd.queue,
      state: cmd.state,
      count: ids.length,
    });
  }
  return { ok: true, count: ids.length, ids, reqId };
}

/** Handle Count command */
export function handleCount(
  cmd: Extract<Command, { cmd: 'Count' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const count = ctx.queueManager.count(cmd.queue);
  return { ok: true, count, reqId };
}

// ============ Rate Limiting ============

/** Handle RateLimit command */
export function handleRateLimit(
  cmd: Extract<Command, { cmd: 'RateLimit' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const limit = toFiniteNumber(cmd.limit);
  if (limit === undefined) return resp.error('limit must be a finite number', reqId);
  ctx.queueManager.setRateLimit(cmd.queue, limit);
  ctx.queueManager.emitDashboardEvent('ratelimit:set', { queue: cmd.queue, max: limit });
  return resp.ok(undefined, reqId);
}

/** Handle RateLimitClear command */
export function handleRateLimitClear(
  cmd: Extract<Command, { cmd: 'RateLimitClear' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  ctx.queueManager.clearRateLimit(cmd.queue);
  ctx.queueManager.emitDashboardEvent('ratelimit:cleared', { queue: cmd.queue });
  return resp.ok(undefined, reqId);
}

/** Handle SetConcurrency command */
export function handleSetConcurrency(
  cmd: Extract<Command, { cmd: 'SetConcurrency' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const limit = toFiniteNumber(cmd.limit);
  if (limit === undefined) return resp.error('limit must be a finite number', reqId);
  ctx.queueManager.setConcurrency(cmd.queue, limit);
  ctx.queueManager.emitDashboardEvent('concurrency:set', {
    queue: cmd.queue,
    concurrency: limit,
  });
  return resp.ok(undefined, reqId);
}

/** Handle ClearConcurrency command */
export function handleClearConcurrency(
  cmd: Extract<Command, { cmd: 'ClearConcurrency' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  ctx.queueManager.clearConcurrency(cmd.queue);
  ctx.queueManager.emitDashboardEvent('concurrency:cleared', { queue: cmd.queue });
  return resp.ok(undefined, reqId);
}

// ============ Config Commands ============

/** Handle SetStallConfig command */
export function handleSetStallConfig(
  cmd: Extract<Command, { cmd: 'SetStallConfig' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const config = sanitizeConfigNumbers(cmd.config, ['stallInterval', 'maxStalls', 'gracePeriod']);
  ctx.queueManager.setStallConfig(cmd.queue, config);
  ctx.queueManager.emitDashboardEvent('config:stall-changed', {
    queue: cmd.queue,
    config,
  });
  return resp.ok(undefined, reqId);
}

/** Handle GetStallConfig command */
export function handleGetStallConfig(
  cmd: Extract<Command, { cmd: 'GetStallConfig' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const config = ctx.queueManager.getStallConfig(cmd.queue);
  return { ok: true, config, reqId } as Response;
}

/** Handle SetDlqConfig command */
export function handleSetDlqConfig(
  cmd: Extract<Command, { cmd: 'SetDlqConfig' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const config = sanitizeConfigNumbers(cmd.config, [
    'autoRetryInterval',
    'maxAutoRetries',
    'maxAge',
    'maxEntries',
  ]);
  ctx.queueManager.setDlqConfig(cmd.queue, config);
  ctx.queueManager.emitDashboardEvent('config:dlq-changed', {
    queue: cmd.queue,
    config,
  });
  return resp.ok(undefined, reqId);
}

/** Handle GetDlqConfig command */
export function handleGetDlqConfig(
  cmd: Extract<Command, { cmd: 'GetDlqConfig' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const config = ctx.queueManager.getDlqConfig(cmd.queue);
  return { ok: true, config, reqId } as Response;
}

/** Handle ChangeDelay command - change delay for a delayed/active job */
export async function handleChangeDelay(
  cmd: Extract<Command, { cmd: 'ChangeDelay' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const success = await ctx.queueManager.changeDelay(jobId(cmd.id), cmd.delay);
  if (success) {
    ctx.queueManager.emitDashboardEvent('job:delay-changed', {
      jobId: cmd.id,
      newDelay: cmd.delay,
    });
  }
  return success
    ? resp.ok(undefined, reqId)
    : resp.error('Job not found or cannot change delay', reqId);
}

/** Handle MoveToWait command - move a job back to waiting.
 *  Dispatch by state to match embedded branching in jobMove.ts.
 *  - active  -> moveActiveToWait
 *  - delayed -> promote
 *  - failed  -> retryDlq
 *  - waiting / prioritized -> already there, no-op success
 */
export async function handleMoveToWait(
  cmd: Extract<Command, { cmd: 'MoveToWait' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const id = jobId(cmd.id);
  const state = await ctx.queueManager.getJobState(id);

  if (state === 'active') {
    const ok = await ctx.queueManager.moveActiveToWait(id);
    return ok ? resp.ok(undefined, reqId) : resp.error('Job not found or not active', reqId);
  }
  if (state === 'delayed') {
    const ok = await ctx.queueManager.promote(id);
    return ok ? resp.ok(undefined, reqId) : resp.error('Job not found or not delayed', reqId);
  }
  if (state === 'failed') {
    const job = await ctx.queueManager.getJob(id);
    if (!job) return resp.error('Job not found', reqId);
    const count = ctx.queueManager.retryDlq(job.queue, id);
    return count > 0 ? resp.ok(undefined, reqId) : resp.error('Failed job not in DLQ', reqId);
  }
  if (state === 'waiting' || state === 'prioritized') {
    return resp.ok(undefined, reqId);
  }
  return resp.error(`Cannot move job from state '${state}' to waiting`, reqId);
}

/** Handle PromoteJobs command - promote all delayed jobs in a queue */
export async function handlePromoteJobs(
  cmd: Extract<Command, { cmd: 'PromoteJobs' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const count = await ctx.queueManager.promoteJobs(cmd.queue, cmd.count);
  return { ok: true, count, reqId };
}

// ============ Flow Dependency Commands ============

/** Handle GetFailedChildrenValues command */
export async function handleGetFailedChildrenValues(
  cmd: Extract<Command, { cmd: 'GetFailedChildrenValues' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  try {
    const values = await ctx.queueManager.getFailedChildrenValues(jobId(cmd.id));
    return { ok: true, values, reqId } as Response;
  } catch {
    return { ok: true, values: {}, reqId } as Response;
  }
}

/** Handle GetIgnoredChildrenFailures command */
export async function handleGetIgnoredChildrenFailures(
  cmd: Extract<Command, { cmd: 'GetIgnoredChildrenFailures' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  try {
    const values = await ctx.queueManager.getIgnoredChildrenFailures(jobId(cmd.id));
    return { ok: true, values, reqId } as Response;
  } catch {
    return { ok: true, values: {}, reqId } as Response;
  }
}

/** Handle RemoveChildDependency command */
export async function handleRemoveChildDependency(
  cmd: Extract<Command, { cmd: 'RemoveChildDependency' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  try {
    const removed = await ctx.queueManager.removeChildDependency(jobId(cmd.id));
    return { ok: true, removed, reqId } as Response;
  } catch (err) {
    return resp.error(err instanceof Error ? err.message : String(err), reqId);
  }
}

/** Handle RemoveUnprocessedChildren command */
export async function handleRemoveUnprocessedChildren(
  cmd: Extract<Command, { cmd: 'RemoveUnprocessedChildren' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  try {
    await ctx.queueManager.removeUnprocessedChildren(jobId(cmd.id));
    return resp.ok(undefined, reqId);
  } catch (err) {
    return resp.error(err instanceof Error ? err.message : String(err), reqId);
  }
}
