/**
 * Queue Control Operations
 * pause, resume, drain, obliterate, isPaused
 */

import { getSharedManager } from '../../manager';
import type { TcpConnectionPool } from '../../tcpPool';

interface ControlContext {
  name: string;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
}

/** Pause the queue */
export function pause(ctx: ControlContext): void {
  if (ctx.embedded) getSharedManager().pause(ctx.name);
  else if (ctx.tcp) void ctx.tcp.send({ cmd: 'Pause', queue: ctx.name });
}

/** Pause the queue and resolve once the server has processed it. */
export async function pauseAsync(ctx: ControlContext): Promise<void> {
  if (ctx.embedded) getSharedManager().pause(ctx.name);
  else if (ctx.tcp) await ctx.tcp.send({ cmd: 'Pause', queue: ctx.name });
}

/** Resume the queue */
export function resume(ctx: ControlContext): void {
  if (ctx.embedded) getSharedManager().resume(ctx.name);
  else if (ctx.tcp) void ctx.tcp.send({ cmd: 'Resume', queue: ctx.name });
}

/** Resume the queue and resolve once the server has processed it. */
export async function resumeAsync(ctx: ControlContext): Promise<void> {
  if (ctx.embedded) getSharedManager().resume(ctx.name);
  else if (ctx.tcp) await ctx.tcp.send({ cmd: 'Resume', queue: ctx.name });
}

/** Drain the queue (remove all waiting jobs) */
export function drain(ctx: ControlContext): void {
  if (ctx.embedded) getSharedManager().drain(ctx.name);
  else if (ctx.tcp) void ctx.tcp.send({ cmd: 'Drain', queue: ctx.name });
}

/**
 * Drain the queue and resolve with the number of removed jobs once the
 * server has processed it. Same ordering guarantee as obliterateAsync():
 * jobs added after resolution cannot be caught by a late-arriving drain.
 */
export async function drainAsync(ctx: ControlContext): Promise<number> {
  if (ctx.embedded) return getSharedManager().drain(ctx.name);
  if (!ctx.tcp) return 0;
  const response = await ctx.tcp.send({ cmd: 'Drain', queue: ctx.name });
  if (!response.ok) return 0;
  return (response.count ?? 0) as number;
}

/** Obliterate the queue (remove all jobs and data) */
export function obliterate(ctx: ControlContext): void {
  if (ctx.embedded) getSharedManager().obliterate(ctx.name);
  else if (ctx.tcp) void ctx.tcp.send({ cmd: 'Obliterate', queue: ctx.name });
}

/**
 * Obliterate the queue and resolve once the server has processed it.
 * The fire-and-forget obliterate() gives no ordering guarantee over a
 * multi-connection pool: a PUSH sent right after it can be processed first
 * and then wiped. Await this variant before enqueuing follow-up jobs.
 */
export async function obliterateAsync(ctx: ControlContext): Promise<void> {
  if (ctx.embedded) getSharedManager().obliterate(ctx.name);
  else if (ctx.tcp) await ctx.tcp.send({ cmd: 'Obliterate', queue: ctx.name });
}

/** Check if queue is paused (sync, embedded only) */
export function isPaused(ctx: ControlContext): boolean {
  if (!ctx.embedded) return false;
  return getSharedManager().isPaused(ctx.name);
}

/** Check if queue is paused (async, works with TCP) */
export async function isPausedAsync(ctx: ControlContext): Promise<boolean> {
  if (ctx.embedded) return isPaused(ctx);
  if (!ctx.tcp) return false;
  const response = await ctx.tcp.send({ cmd: 'IsPaused', queue: ctx.name });
  return response.paused === true;
}

/** Wait until queue is ready */
export async function waitUntilReady(ctx: ControlContext): Promise<void> {
  if (ctx.embedded) return;
  if (ctx.tcp) await ctx.tcp.send({ cmd: 'Ping' });
}
