/**
 * Core Command Handlers
 * Push, Pull, Ack, Fail operations
 */

import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import * as resp from '../../../domain/types/response';
import { jobId } from '../../../domain/types/job';
import type { HandlerContext } from '../types';
import {
  validateQueueName,
  validateJobData,
  validateJobOptions,
  validateNumericField,
} from '../protocol';
import { validatePushBatchJobs } from './pushBatchValidation';

/** Handle PUSH command */
export async function handlePush(
  cmd: Extract<Command, { cmd: 'PUSH' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const queueError = validateQueueName(cmd.queue);
  if (queueError) return resp.error(queueError, reqId);

  const dataError = validateJobData(cmd.data);
  if (dataError) return resp.error(dataError, reqId);

  // Validate numeric fields
  const optionsError = validateJobOptions({
    priority: cmd.priority,
    delay: cmd.delay,
    timeout: cmd.timeout,
    maxAttempts: cmd.maxAttempts,
    backoff: cmd.backoff,
    ttl: cmd.ttl,
  });
  if (optionsError) return resp.error(optionsError, reqId);

  // Validate dependsOn references exist
  if (cmd.dependsOn && cmd.dependsOn.length > 0) {
    for (const depId of cmd.dependsOn) {
      const depJobId = jobId(depId);
      const exists =
        ctx.queueManager.getJobIndex().has(depJobId) ||
        ctx.queueManager.getCompletedJobs().has(depJobId) ||
        // A removeOnComplete parent that completed is recorded only here (its row
        // is deleted and it leaves jobIndex/completedJobs). The readiness path and
        // dependency processor already honor depCompletions; the gate must too, or
        // a late dependent is wrongly rejected with "Dependency job not found".
        ctx.queueManager.getDepCompletions().has(depJobId);
      if (!exists) {
        return resp.error(`Dependency job not found: ${depId}`, reqId);
      }
    }
  }

  try {
    const job = await ctx.queueManager.push(cmd.queue, {
      data: cmd.data,
      priority: cmd.priority,
      delay: cmd.delay,
      maxAttempts: cmd.maxAttempts,
      backoff: cmd.backoff,
      ttl: cmd.ttl,
      timeout: cmd.timeout,
      uniqueKey: cmd.uniqueKey,
      customId: cmd.jobId,
      dependsOn: cmd.dependsOn?.map((id) => jobId(id)),
      childrenIds: cmd.childrenIds?.map((id) => jobId(id)),
      parentId: cmd.parentId ? jobId(cmd.parentId) : undefined,
      tags: cmd.tags,
      groupId: cmd.groupId,
      lifo: cmd.lifo,
      removeOnComplete: cmd.removeOnComplete,
      removeOnFail: cmd.removeOnFail,
      durable: cmd.durable,
      repeat: cmd.repeat,
      failParentOnFailure: cmd.failParentOnFailure,
      removeDependencyOnFailure: cmd.removeDependencyOnFailure,
      ignoreDependencyOnFailure: cmd.ignoreDependencyOnFailure,
      continueParentOnFailure: cmd.continueParentOnFailure,
      stallTimeout: cmd.stallTimeout,
      stackTraceLimit: cmd.stackTraceLimit,
      keepLogs: cmd.keepLogs,
      sizeLimit: cmd.sizeLimit,
      dedup: cmd.dedup,
      debounceId: cmd.debounceId,
      debounceTtl: cmd.debounceTtl,
      timestamp: cmd.timestamp,
    });

    return resp.ok(job.id, reqId);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return resp.error(message, reqId);
  }
}

/** Handle PUSHB (batch push) command */
export async function handlePushBatch(
  cmd: Extract<Command, { cmd: 'PUSHB' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const queueError = validateQueueName(cmd.queue);
  if (queueError) return resp.error(queueError, reqId);

  // PUSH parity: per-job data/option bounds + dependsOn existence gate
  // (extended to earlier same-batch custom ids for intra-batch chains).
  const jobsError = validatePushBatchJobs(cmd.jobs, ctx);
  if (jobsError) return resp.error(jobsError, reqId);

  const ids = await ctx.queueManager.pushBatch(cmd.queue, cmd.jobs);
  return resp.batch(ids, reqId);
}

/** Handle PULL command */
export async function handlePull(
  cmd: Extract<Command, { cmd: 'PULL' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const queueError = validateQueueName(cmd.queue);
  if (queueError) return resp.error(queueError, reqId);

  // Validate timeout
  const timeoutError = validateNumericField(cmd.timeout, 'timeout', { min: 0, max: 60000 });
  if (timeoutError) return resp.error(timeoutError, reqId);

  // If owner is provided, use lock-based pull
  if (cmd.owner) {
    const { job, token } = await ctx.queueManager.pullWithLock(
      cmd.queue,
      cmd.owner,
      cmd.timeout,
      cmd.lockTtl,
      ctx.signal
    );
    // Register job with client for connection-based release
    if (job && ctx.clientId) {
      ctx.queueManager.registerClientJob(ctx.clientId, job.id);
    }
    return resp.pulledJob(job, token, reqId);
  }

  // Standard pull (no lock, but still track for client release unless detached)
  const job = await ctx.queueManager.pull(cmd.queue, cmd.timeout, ctx.signal);
  if (job && ctx.clientId && !cmd.detach) {
    ctx.queueManager.registerClientJob(ctx.clientId, job.id);
  }
  return resp.nullableJob(job, reqId);
}

/** Handle PULLB (batch pull) command - uses optimized single-lock batch pull */
export async function handlePullBatch(
  cmd: Extract<Command, { cmd: 'PULLB' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const queueError = validateQueueName(cmd.queue);
  if (queueError) return resp.error(queueError, reqId);

  // Validate count
  const countError = validateNumericField(cmd.count, 'count', { min: 1, max: 1000 });
  if (countError) return resp.error(countError, reqId);

  // Validate timeout (same bound as PULL)
  const timeoutError = validateNumericField(cmd.timeout, 'timeout', { min: 0, max: 60000 });
  if (timeoutError) return resp.error(timeoutError, reqId);

  // If owner is provided, use lock-based pull
  if (cmd.owner) {
    const { jobs, tokens } = await ctx.queueManager.pullBatchWithLock(
      cmd.queue,
      cmd.count,
      cmd.owner,
      cmd.timeout ?? 0,
      cmd.lockTtl,
      ctx.signal
    );
    // Register all jobs with client for connection-based release
    if (ctx.clientId) {
      for (const job of jobs) {
        ctx.queueManager.registerClientJob(ctx.clientId, job.id);
      }
    }
    return resp.pulledJobs(jobs, tokens, reqId);
  }

  // Standard pull (no locks, but still track for client release) — the
  // non-owner branch honors cmd.timeout exactly like the owner branch and PULL.
  const jobs = await ctx.queueManager.pullBatch(cmd.queue, cmd.count, cmd.timeout ?? 0, ctx.signal);
  if (ctx.clientId) {
    for (const job of jobs) {
      ctx.queueManager.registerClientJob(ctx.clientId, job.id);
    }
  }
  return resp.jobs(jobs, reqId);
}

/** Handle ACK command */
export async function handleAck(
  cmd: Extract<Command, { cmd: 'ACK' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  try {
    const jid = jobId(cmd.id);
    await ctx.queueManager.ack(jid, cmd.result, cmd.token);
    // Unregister job from client tracking
    ctx.queueManager.unregisterClientJob(ctx.clientId, jid);
    return resp.ok(undefined, reqId);
  } catch (err) {
    return resp.error(err instanceof Error ? err.message : String(err), reqId);
  }
}

/** Handle ACKB (batch ack) command - supports optional results and tokens */
export async function handleAckBatch(
  cmd: Extract<Command, { cmd: 'ACKB' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const ids = cmd.ids.map((id) => jobId(id));

  try {
    // If results provided, use ackBatchWithResults
    if (cmd.results?.length === cmd.ids.length) {
      const results = cmd.results;
      const tokens = cmd.tokens;
      const items = ids.map((id, i) => ({
        id,
        result: results[i],
        token: tokens?.[i],
      }));
      await ctx.queueManager.ackBatchWithResults(items);
    } else {
      // Use optimized batch ack without results
      await ctx.queueManager.ackBatch(ids, cmd.tokens);
    }
    // Unregister all jobs from client tracking
    for (const id of ids) {
      ctx.queueManager.unregisterClientJob(ctx.clientId, id);
    }
    return resp.ok(undefined, reqId);
  } catch (err) {
    return resp.error(err instanceof Error ? err.message : String(err), reqId);
  }
}

/** Handle FAIL command */
export async function handleFail(
  cmd: Extract<Command, { cmd: 'FAIL' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  try {
    const jid = jobId(cmd.id);
    // #74: the wire is not type-safe — accept stack only as string[], cap at
    // 100 elements before it reaches the domain (authoritative cap happens in
    // failJob at job.stackTraceLimit).
    const stack = Array.isArray(cmd.stack)
      ? cmd.stack.filter((line): line is string => typeof line === 'string').slice(0, 100)
      : undefined;
    await ctx.queueManager.fail(
      jid,
      cmd.error,
      cmd.token,
      cmd.unrecoverable,
      stack && stack.length > 0 ? stack : undefined
    );
    // Unregister job from client tracking
    ctx.queueManager.unregisterClientJob(ctx.clientId, jid);
    return resp.ok(undefined, reqId);
  } catch (err) {
    return resp.error(err instanceof Error ? err.message : String(err), reqId);
  }
}
