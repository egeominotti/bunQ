/**
 * Management Command Handlers
 * Cancel, Progress, Pause, Resume, Drain, Stats, Metrics
 */

import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import * as resp from '../../../domain/types/response';
import { jobId } from '../../../domain/types/job';
import type { HandlerContext } from '../types';
import { throughputTracker } from '../../../application/throughputTracker';
import { latencyTracker } from '../../../application/latencyTracker';
import { validateQueueName } from '../protocol/validation';

/** Handle Cancel command */
export async function handleCancel(
  cmd: Extract<Command, { cmd: 'Cancel' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const success = await ctx.queueManager.cancel(jobId(cmd.id));
  return success
    ? resp.ok(undefined, reqId)
    : resp.error('Job not found or cannot be cancelled', reqId);
}

/** Handle Progress command */
export async function handleProgress(
  cmd: Extract<Command, { cmd: 'Progress' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const id = jobId(cmd.id);
  const success = await ctx.queueManager.updateProgress(id, cmd.progress, cmd.message);
  if (success) return resp.ok(undefined, reqId);
  // Disambiguate "not found" from "not active" so callers can act on the
  // distinction (e.g. retry on transient state vs surface a missing-ID error).
  const state = await ctx.queueManager.getJobState(id);
  if (state === 'unknown') {
    return resp.error('Job not found', reqId);
  }
  return resp.error(`Job is not active (current state: ${state})`, reqId);
}

/** Handle GetProgress command */
export async function handleGetProgress(
  cmd: Extract<Command, { cmd: 'GetProgress' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const id = jobId(cmd.id);
  const progress = ctx.queueManager.getProgress(id);
  if (progress) {
    return {
      ok: true,
      progress: progress.progress,
      message: progress.message,
      reqId,
    };
  }
  const state = await ctx.queueManager.getJobState(id);
  if (state === 'unknown') {
    return resp.error('Job not found', reqId);
  }
  return resp.error(`Job is not active (current state: ${state})`, reqId);
}

/** Handle Pause command */
export function handlePause(
  cmd: Extract<Command, { cmd: 'Pause' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  ctx.queueManager.pause(cmd.queue);
  return resp.ok(undefined, reqId);
}

/** Handle Resume command */
export function handleResume(
  cmd: Extract<Command, { cmd: 'Resume' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  ctx.queueManager.resume(cmd.queue);
  return resp.ok(undefined, reqId);
}

/** Handle Drain command */
export function handleDrain(
  cmd: Extract<Command, { cmd: 'Drain' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const count = ctx.queueManager.drain(cmd.queue);
  return { ok: true, count, reqId };
}

/** Handle Stats command */
export function handleStats(ctx: HandlerContext, reqId?: string): Response {
  const s = ctx.queueManager.getStats();
  const rates = throughputTracker.getRates();
  return resp.stats(
    {
      waiting: s.waiting,
      active: s.active,
      delayed: s.delayed,
      dlq: s.dlq,
      completed: s.completed,
      failed: Number(s.totalFailed),
      uptime: s.uptime,
      pushPerSec: rates.pushPerSec,
      pullPerSec: rates.pullPerSec,
    },
    reqId
  );
}

/** Handle StorageStatus command - get real disk/storage health */
export function handleStorageStatus(
  _cmd: Extract<Command, { cmd: 'StorageStatus' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const status = ctx.queueManager.getStorageStatus();
  return resp.data({ diskFull: status.diskFull, error: status.error, since: status.since }, reqId);
}

/** Handle Metrics command */
export function handleMetrics(ctx: HandlerContext, reqId?: string): Response {
  const s = ctx.queueManager.getStats();
  const avgLatencies = latencyTracker.getAverages();
  return resp.metrics(
    {
      totalPushed: Number(s.totalPushed),
      totalPulled: Number(s.totalPulled),
      totalCompleted: Number(s.totalCompleted),
      totalFailed: Number(s.totalFailed),
      avgLatencyMs: Math.round(((avgLatencies.pushMs + avgLatencies.pullMs) / 2) * 100) / 100,
      avgProcessingMs: Math.round(avgLatencies.ackMs * 100) / 100,
      memoryUsageMb: process.memoryUsage().heapUsed / 1024 / 1024,
      sqliteSizeMb: 0,
      activeConnections: 0,
    },
    reqId
  );
}

/** Handle the queue-scoped Metrics form. */
export function handleQueueMetrics(
  cmd: Extract<Command, { cmd: 'Metrics' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  if (typeof cmd.queue !== 'string') return resp.error('Metrics queue is required', reqId);
  const queueError = validateQueueName(cmd.queue);
  if (queueError) return resp.error(queueError, reqId);
  if (cmd.type !== 'completed' && cmd.type !== 'failed') {
    return resp.error("Metrics type must be 'completed' or 'failed'", reqId);
  }
  try {
    return resp.data(
      ctx.queueManager.getQueueMetrics(cmd.queue, cmd.type, cmd.start ?? 0, cmd.end ?? -1),
      reqId
    );
  } catch (error) {
    return resp.error(error instanceof Error ? error.message : String(error), reqId);
  }
}

/** Trim one queue's durable lifecycle-event journal. */
export function handleTrimEvents(
  cmd: Extract<Command, { cmd: 'TrimEvents' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const queueError = validateQueueName(cmd.queue);
  if (queueError) return resp.error(queueError, reqId);
  try {
    return resp.data(
      { removed: ctx.queueManager.trimQueueEvents(cmd.queue, cmd.maxLength) },
      reqId
    );
  } catch (error) {
    return resp.error(error instanceof Error ? error.message : String(error), reqId);
  }
}
