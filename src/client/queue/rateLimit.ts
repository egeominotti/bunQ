/**
 * Rate Limit and Concurrency Operations
 */

import { getSharedManager } from '../manager';
import type { TcpConnectionPool } from '../tcpPool';

interface RateLimitContext {
  name: string;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
}

interface QueueLimits {
  rateLimit: { max: number; duration: number } | null;
  rateLimitTtl: number;
  concurrencyLimit: number | null;
  maxed: boolean;
}

async function getQueueLimits(ctx: RateLimitContext, maxJobs?: number): Promise<QueueLimits> {
  if (ctx.embedded) return getSharedManager().getQueueLimitStatus(ctx.name, maxJobs);
  if (!ctx.tcp) {
    return { rateLimit: null, rateLimitTtl: -2, concurrencyLimit: null, maxed: false };
  }
  const response = await ctx.tcp.send({ cmd: 'GetQueueLimits', queue: ctx.name, maxJobs });
  return (response.data as { limits: QueueLimits }).limits;
}

/** Set global concurrency limit */
export function setGlobalConcurrency(ctx: RateLimitContext, concurrency: number): void {
  if (ctx.embedded) {
    getSharedManager().setConcurrency(ctx.name, concurrency);
  } else if (ctx.tcp) {
    void ctx.tcp.send({ cmd: 'SetConcurrency', queue: ctx.name, limit: concurrency });
  }
}

/** Set global concurrency limit and resolve once the server has applied it. */
export async function setGlobalConcurrencyAsync(
  ctx: RateLimitContext,
  concurrency: number
): Promise<void> {
  if (ctx.embedded) getSharedManager().setConcurrency(ctx.name, concurrency);
  else if (ctx.tcp)
    await ctx.tcp.send({ cmd: 'SetConcurrency', queue: ctx.name, limit: concurrency });
}

/** Remove global concurrency limit */
export function removeGlobalConcurrency(ctx: RateLimitContext): void {
  if (ctx.embedded) {
    getSharedManager().clearConcurrency(ctx.name);
  } else if (ctx.tcp) {
    void ctx.tcp.send({ cmd: 'ClearConcurrency', queue: ctx.name });
  }
}

/** Remove global concurrency limit and resolve once the server has applied it. */
export async function removeGlobalConcurrencyAsync(ctx: RateLimitContext): Promise<void> {
  if (ctx.embedded) getSharedManager().clearConcurrency(ctx.name);
  else if (ctx.tcp) await ctx.tcp.send({ cmd: 'ClearConcurrency', queue: ctx.name });
}

/** Get global concurrency limit */
export async function getGlobalConcurrency(ctx: RateLimitContext): Promise<number | null> {
  return (await getQueueLimits(ctx)).concurrencyLimit;
}

/**
 * Set global rate limit: `max` jobs per `duration` ms (default 1000ms).
 * The window is honored in both embedded and TCP modes.
 */
export function setGlobalRateLimit(ctx: RateLimitContext, max: number, duration?: number): void {
  if (ctx.embedded) {
    getSharedManager().setRateLimit(ctx.name, max, duration);
  } else if (ctx.tcp) {
    void ctx.tcp.send({ cmd: 'RateLimit', queue: ctx.name, limit: max, duration });
  }
}

/** Set global rate limit and resolve once the server has applied it. */
export async function setGlobalRateLimitAsync(
  ctx: RateLimitContext,
  max: number,
  duration?: number
): Promise<void> {
  if (ctx.embedded) getSharedManager().setRateLimit(ctx.name, max, duration);
  else if (ctx.tcp) await ctx.tcp.send({ cmd: 'RateLimit', queue: ctx.name, limit: max, duration });
}

/** Remove global rate limit */
export function removeGlobalRateLimit(ctx: RateLimitContext): void {
  if (ctx.embedded) {
    getSharedManager().clearRateLimit(ctx.name);
  } else if (ctx.tcp) {
    void ctx.tcp.send({ cmd: 'RateLimitClear', queue: ctx.name });
  }
}

/** Remove global rate limit and resolve once the server has applied it. */
export async function removeGlobalRateLimitAsync(ctx: RateLimitContext): Promise<void> {
  if (ctx.embedded) getSharedManager().clearRateLimit(ctx.name);
  else if (ctx.tcp) await ctx.tcp.send({ cmd: 'RateLimitClear', queue: ctx.name });
}

/** Get global rate limit */
export function getGlobalRateLimit(
  ctx: RateLimitContext
): Promise<{ max: number; duration: number } | null> {
  return getQueueLimits(ctx).then((limits) => limits.rateLimit);
}

/**
 * Set temporary rate limit with expiration. The expiry lives broker-side
 * (RateLimit `ttl`), so it survives client exit and works identically in
 * embedded and TCP modes — no client timer involved.
 */
export async function rateLimit(ctx: RateLimitContext, expireTimeMs: number): Promise<void> {
  if (!Number.isFinite(expireTimeMs) || expireTimeMs <= 0) {
    throw new Error(
      `rateLimit: expireTimeMs must be a positive finite number, got ${expireTimeMs}`
    );
  }
  if (ctx.embedded) {
    getSharedManager().setRateLimit(ctx.name, 1, undefined, expireTimeMs);
  } else if (ctx.tcp) {
    await ctx.tcp.send({ cmd: 'RateLimit', queue: ctx.name, limit: 1, ttl: expireTimeMs });
  }
}

/** Get rate limit TTL */
export async function getRateLimitTtl(ctx: RateLimitContext, maxJobs?: number): Promise<number> {
  return (await getQueueLimits(ctx, maxJobs)).rateLimitTtl;
}

/** Check if queue is at max capacity */
export async function isMaxed(ctx: RateLimitContext): Promise<boolean> {
  return (await getQueueLimits(ctx)).maxed;
}
