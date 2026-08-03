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

function responseError(response: Record<string, unknown>, fallback: string): Error {
  return new Error(typeof response.error === 'string' ? response.error : fallback);
}

function throwTokenResponse(response: Record<string, unknown>): void {
  if (response.ok === false && /token/i.test(String(response.error))) {
    throw responseError(response, 'Invalid lock token');
  }
}

/** Move job to completed state */
export async function moveJobToCompleted(
  ctx: JobMoveContext,
  id: string,
  returnValue: unknown,
  token?: string
): Promise<unknown> {
  if (ctx.embedded) {
    await getSharedManager().ack(jobId(id), returnValue, token);
    return null;
  }
  const response = await ctx.tcp!.send({
    cmd: 'ACK',
    id,
    result: returnValue,
    ...(token === undefined ? {} : { token }),
  });
  if (response.ok !== true) throw responseError(response, 'Failed to complete job');
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
    const response = await ctx.tcp!.send(buildFailCommand(id, error, token));
    if (response.ok !== true) throw responseError(response, 'Failed to fail job');
  }
}

/** Move job back to waiting state */
export async function moveJobToWait(
  ctx: JobMoveContext,
  id: string,
  token?: string
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
      return manager.moveActiveToWait(jobId(id), token);
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

  const response = await ctx.tcp!.send({
    cmd: 'MoveToWait',
    id,
    ...(token === undefined ? {} : { token }),
  });
  throwTokenResponse(response);
  return response.ok === true;
}

/** Move job to delayed state */
export async function moveJobToDelayed(
  ctx: JobMoveContext,
  id: string,
  timestamp: number,
  token?: string
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
        await manager.changeDelay(jobId(id), delay, token);
      }
    } else if (state === 'waiting' || state === 'prioritized' || state === 'delayed') {
      // Job is in queue - update its runAt directly
      await manager.changeDelay(jobId(id), delay, token);
    } else {
      // Active or other state - use existing changeDelay (handles processing)
      await manager.changeDelay(jobId(id), delay, token);
    }
  } else {
    // The public API takes an ABSOLUTE timestamp, but the server works in relative
    // delay (runAt = now + delay) and both MoveToDelayedCommand and
    // handleMoveToDelayed read the wire field `delay`. Sending `timestamp` left
    // `delay` undefined server-side → runAt = now + undefined = NaN (an active job
    // was re-queued as `waiting` with the delay dropped) or a silent no-op (a
    // waiting job). Convert to relative delay here so the wire field matches.
    const delay = Math.max(0, timestamp - Date.now());
    const response = await ctx.tcp!.send({
      cmd: 'MoveToDelayed',
      id,
      delay,
      ...(token === undefined ? {} : { token }),
    });
    // Surface server-side failures instead of silently ignoring the response
    // (e.g. the job was completed/removed before the command arrived).
    if (response.ok === false) {
      throw responseError(response, 'Failed to move job to delayed');
    }
  }
}

/** Move job to waiting-children state */
export async function moveJobToWaitingChildren(
  ctx: JobMoveContext,
  id: string,
  token?: string,
  _opts?: { child?: { id: string; queue: string } }
): Promise<boolean> {
  if (ctx.embedded) {
    const manager = getSharedManager();
    return manager.moveToWaitingChildren(jobId(id), token);
  }
  if (!ctx.tcp) return false;
  const response = await ctx.tcp.send({
    cmd: 'MoveToWaitingChildren',
    id,
    ...(token === undefined ? {} : { token }),
  });
  throwTokenResponse(response);
  if (!response.ok) return false;
  return (response.data as { moved?: boolean } | undefined)?.moved ?? true;
}

/** Wait until job has finished */
export async function waitJobUntilFinished(
  ctx: JobMoveContext,
  id: string,
  queueEvents: unknown,
  ttl?: number
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const timeout = ttl
      ? setTimeout(() => {
          if (settled) return;
          settled = true;
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
      if (data.jobId === id && !settled) {
        settled = true;
        cleanup();
        resolve(data.returnvalue);
      }
    };

    const failedHandler = (data: { jobId: string; failedReason?: string }) => {
      if (data.jobId === id && !settled) {
        settled = true;
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

    const checkFinishedState = async (): Promise<void> => {
      const state = await ctx.getJobState(id);
      if (settled) return;
      if (state === 'completed') {
        let result: unknown;
        if (ctx.embedded) {
          result = getSharedManager().getResult(jobId(id));
        } else {
          if (!ctx.tcp) throw new Error('Queue TCP connection is unavailable');
          const response = await ctx.tcp.send({ cmd: 'GetResult', id });
          if (response.ok !== true) {
            throw new Error(
              typeof response.error === 'string' ? response.error : 'Failed to read job result'
            );
          }
          result = response.result;
        }
        if (settled) return;
        settled = true;
        cleanup();
        resolve(result);
      } else if (state === 'failed') {
        settled = true;
        cleanup();
        reject(new Error('Job already failed'));
      }
    };

    void checkFinishedState().catch((error: unknown) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(error instanceof Error ? error : new Error(String(error)));
    });
  });
}
