/* eslint-disable @typescript-eslint/no-non-null-assertion */
/**
 * Job Move Operations (BullMQ v5 compatible)
 */

import { getSharedManager } from '../manager';
import type { TcpConnectionPool } from '../tcpPool';
import { jobId } from '../../domain/types/job';
import { buildFailCommand, failEmbeddedArgs } from './failWire';

interface JobMoveContext {
  name: string;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
  getJobState: (
    id: string
  ) => Promise<
    | 'waiting'
    | 'prioritized'
    | 'delayed'
    | 'active'
    | 'completed'
    | 'failed'
    | 'waiting-children'
    | 'unknown'
  >;
  getJobDependencies: (
    id: string
  ) => Promise<{ processed: Record<string, unknown>; unprocessed: string[] }>;
}

/** Move job to completed state */
export async function moveJobToCompleted(
  ctx: JobMoveContext,
  id: string,
  returnValue: unknown,
  _token?: string
): Promise<unknown> {
  if (ctx.embedded) {
    await getSharedManager().ack(jobId(id), returnValue);
    return null;
  }
  await ctx.tcp!.send({ cmd: 'ACK', id, result: returnValue });
  return null;
}

/** Move job to failed state */
export async function moveJobToFailed(
  ctx: JobMoveContext,
  id: string,
  error: Error,
  token?: string
): Promise<void> {
  // Mirror the worker failure path: persist the stacktrace (#74) and honour
  // UnrecoverableError so a job explicitly failed via the Queue reflection API
  // is not silently retried and does not lose its stack. Previously both were
  // dropped on this path (same silent-loss class as #111).
  if (ctx.embedded) {
    await getSharedManager().fail(jobId(id), ...failEmbeddedArgs(error, token));
  } else {
    await ctx.tcp!.send(buildFailCommand(id, error, token));
  }
}

/** Move job back to waiting state */
export async function moveJobToWait(
  ctx: JobMoveContext,
  id: string,
  _token?: string
): Promise<boolean> {
  if (ctx.embedded) {
    const manager = getSharedManager();
    const state = await ctx.getJobState(id);

    if (state === 'failed') {
      // Job is in DLQ - use retryDlq to move back to waiting
      const job = await manager.getJob(jobId(id));
      if (!job) return false;
      const count = manager.retryDlq(job.queue, jobId(id));
      return count > 0;
    }

    if (state === 'active') {
      // Job is being processed - move back to queue
      return manager.moveActiveToWait(jobId(id));
    }

    if (state === 'delayed') {
      // Delayed job - promote to waiting
      return manager.promote(jobId(id));
    }

    if (state === 'waiting' || state === 'prioritized') {
      // Already in queue (waiting or prioritized)
      return true;
    }

    return false;
  }

  const response = await ctx.tcp!.send({ cmd: 'MoveToWait', id });
  return response.ok === true;
}

/** Move job to delayed state */
export async function moveJobToDelayed(
  ctx: JobMoveContext,
  id: string,
  timestamp: number,
  _token?: string
): Promise<void> {
  if (ctx.embedded) {
    const delay = Math.max(0, timestamp - Date.now());
    const manager = getSharedManager();
    const state = await ctx.getJobState(id);

    if (state === 'failed') {
      // Job is in DLQ - retry it first, then set delay
      const job = await manager.getJob(jobId(id));
      if (!job) return;
      const count = manager.retryDlq(job.queue, jobId(id));
      if (count > 0 && delay > 0) {
        await manager.changeWaitingDelay(jobId(id), delay);
      }
    } else if (state === 'waiting' || state === 'prioritized' || state === 'delayed') {
      // Job is in queue - update its runAt directly
      await manager.changeWaitingDelay(jobId(id), delay);
    } else {
      // Active or other state - use existing changeDelay (handles processing)
      await manager.changeDelay(jobId(id), delay);
    }
  } else {
    // The public API takes an ABSOLUTE timestamp, but the server works in relative
    // delay (runAt = now + delay) and both MoveToDelayedCommand and
    // handleMoveToDelayed read the wire field `delay`. Sending `timestamp` left
    // `delay` undefined server-side → runAt = now + undefined = NaN (an active job
    // was re-queued as `waiting` with the delay dropped) or a silent no-op (a
    // waiting job). Convert to relative delay here so the wire field matches.
    const delay = Math.max(0, timestamp - Date.now());
    const response = await ctx.tcp!.send({ cmd: 'MoveToDelayed', id, delay });
    // Surface server-side failures instead of silently ignoring the response
    // (e.g. the job was completed/removed before the command arrived).
    if (response.ok === false) {
      throw new Error(
        typeof response.error === 'string' ? response.error : 'Failed to move job to delayed'
      );
    }
  }
}

/** Move job to waiting-children state */
export async function moveJobToWaitingChildren(
  ctx: JobMoveContext,
  id: string,
  _token?: string,
  _opts?: { child?: { id: string; queue: string } }
): Promise<boolean> {
  if (ctx.embedded) {
    const manager = getSharedManager();
    return manager.moveToWaitingChildren(jobId(id));
  }
  return false;
}

/** Wait until job has finished */
export async function waitJobUntilFinished(
  ctx: JobMoveContext,
  id: string,
  queueEvents: unknown,
  ttl?: number
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const timeout = ttl
      ? setTimeout(() => {
          cleanup();
          reject(new Error(`Job ${id} timed out after ${ttl}ms`));
        }, ttl)
      : null;

    const events = queueEvents as {
      on: (
        event: string,
        handler: (data: { jobId: string; returnvalue?: unknown; failedReason?: string }) => void
      ) => void;
      off: (
        event: string,
        handler: (data: { jobId: string; returnvalue?: unknown; failedReason?: string }) => void
      ) => void;
    };

    const completedHandler = (data: { jobId: string; returnvalue?: unknown }) => {
      if (data.jobId === id) {
        cleanup();
        resolve(data.returnvalue);
      }
    };

    const failedHandler = (data: { jobId: string; failedReason?: string }) => {
      if (data.jobId === id) {
        cleanup();
        reject(new Error(data.failedReason ?? 'Job failed'));
      }
    };

    const cleanup = () => {
      if (timeout) clearTimeout(timeout);
      events.off('completed', completedHandler);
      events.off('failed', failedHandler);
    };

    events.on('completed', completedHandler);
    events.on('failed', failedHandler);

    // Check if job is already finished
    void ctx.getJobState(id).then((state) => {
      if (state === 'completed') {
        cleanup();
        if (ctx.embedded) {
          const result = getSharedManager().getResult(jobId(id));
          resolve(result);
        } else {
          resolve(undefined);
        }
      } else if (state === 'failed') {
        cleanup();
        reject(new Error('Job already failed'));
      }
    });
  });
}
