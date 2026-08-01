import type { Command } from '../../../../domain/types/command';
import type { Response } from '../../../../domain/types/response';
import * as response from '../../../../domain/types/response';
import type { HandlerContext } from '../../types';
import { sanitizeConfigNumbers, toFiniteNumber } from './configNumbers';

export function handleIsPaused(
  command: Extract<Command, { cmd: 'IsPaused' }>,
  context: HandlerContext,
  requestId?: string
): Response {
  return {
    ok: true,
    paused: context.queueManager.isPaused(command.queue),
    reqId: requestId,
  } as Response;
}

export function handleObliterate(
  command: Extract<Command, { cmd: 'Obliterate' }>,
  context: HandlerContext,
  requestId?: string
): Response {
  context.queueManager.obliterate(command.queue);
  return response.ok(undefined, requestId);
}

export function handleListQueues(
  _command: Extract<Command, { cmd: 'ListQueues' }>,
  context: HandlerContext,
  requestId?: string
): Response {
  return {
    ok: true,
    queues: context.queueManager.listQueues(),
    reqId: requestId,
  } as Response;
}

export function handleClean(
  command: Extract<Command, { cmd: 'Clean' }>,
  context: HandlerContext,
  requestId?: string
): Response {
  const ids = context.queueManager.clean(
    command.queue,
    command.grace,
    command.state,
    command.limit
  );
  if (ids.length > 0) {
    context.queueManager.emitDashboardEvent('queue:cleaned', {
      queue: command.queue,
      state: command.state,
      count: ids.length,
    });
  }
  return { ok: true, count: ids.length, ids, reqId: requestId };
}

export function handleCount(
  command: Extract<Command, { cmd: 'Count' }>,
  context: HandlerContext,
  requestId?: string
): Response {
  return { ok: true, count: context.queueManager.count(command.queue), reqId: requestId };
}

export function handleRateLimit(
  command: Extract<Command, { cmd: 'RateLimit' }>,
  context: HandlerContext,
  requestId?: string
): Response {
  const limit = toFiniteNumber(command.limit);
  if (limit === undefined) return response.error('limit must be a finite number', requestId);
  const duration = toFiniteNumber(command.duration);
  const ttl = toFiniteNumber(command.ttl);
  context.queueManager.setRateLimit(command.queue, limit, duration, ttl);
  context.queueManager.emitDashboardEvent('ratelimit:set', {
    queue: command.queue,
    max: limit,
    ...(duration !== undefined && { duration }),
    ...(ttl !== undefined && { ttl }),
  });
  return response.ok(undefined, requestId);
}

export function handleRateLimitClear(
  command: Extract<Command, { cmd: 'RateLimitClear' }>,
  context: HandlerContext,
  requestId?: string
): Response {
  context.queueManager.clearRateLimit(command.queue);
  context.queueManager.emitDashboardEvent('ratelimit:cleared', { queue: command.queue });
  return response.ok(undefined, requestId);
}

export function handleSetConcurrency(
  command: Extract<Command, { cmd: 'SetConcurrency' }>,
  context: HandlerContext,
  requestId?: string
): Response {
  const limit = toFiniteNumber(command.limit);
  if (limit === undefined) return response.error('limit must be a finite number', requestId);
  context.queueManager.setConcurrency(command.queue, limit);
  context.queueManager.emitDashboardEvent('concurrency:set', {
    queue: command.queue,
    concurrency: limit,
  });
  return response.ok(undefined, requestId);
}

export function handleClearConcurrency(
  command: Extract<Command, { cmd: 'ClearConcurrency' }>,
  context: HandlerContext,
  requestId?: string
): Response {
  context.queueManager.clearConcurrency(command.queue);
  context.queueManager.emitDashboardEvent('concurrency:cleared', { queue: command.queue });
  return response.ok(undefined, requestId);
}

export function handleSetStallConfig(
  command: Extract<Command, { cmd: 'SetStallConfig' }>,
  context: HandlerContext,
  requestId?: string
): Response {
  const config = sanitizeConfigNumbers(command.config, [
    'stallInterval',
    'maxStalls',
    'gracePeriod',
  ]);
  context.queueManager.setStallConfig(command.queue, config);
  context.queueManager.emitDashboardEvent('config:stall-changed', {
    queue: command.queue,
    config,
  });
  return response.ok(undefined, requestId);
}

export function handleGetStallConfig(
  command: Extract<Command, { cmd: 'GetStallConfig' }>,
  context: HandlerContext,
  requestId?: string
): Response {
  return {
    ok: true,
    config: context.queueManager.getStallConfig(command.queue),
    reqId: requestId,
  } as Response;
}

export function handleSetDlqConfig(
  command: Extract<Command, { cmd: 'SetDlqConfig' }>,
  context: HandlerContext,
  requestId?: string
): Response {
  const config = sanitizeConfigNumbers(command.config, [
    'autoRetryInterval',
    'maxAutoRetries',
    'maxAge',
    'maxEntries',
  ]);
  context.queueManager.setDlqConfig(command.queue, config);
  context.queueManager.emitDashboardEvent('config:dlq-changed', {
    queue: command.queue,
    config,
  });
  return response.ok(undefined, requestId);
}

export function handleGetDlqConfig(
  command: Extract<Command, { cmd: 'GetDlqConfig' }>,
  context: HandlerContext,
  requestId?: string
): Response {
  return {
    ok: true,
    config: context.queueManager.getDlqConfig(command.queue),
    reqId: requestId,
  } as Response;
}
