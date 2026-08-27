import type { Command } from '../../../../domain/types/command';
import type { Response } from '../../../../domain/types/response';
import * as response from '../../../../domain/types/response';
import type { HandlerContext } from '../../types';
import { sanitizeConfigNumbers, toFiniteNumber } from './configNumbers';

export function handleIsPaused(
  command: Extract<Command, { cmd: 'IsPaused' }>,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> {
  const manager = context.queueManager as typeof context.queueManager & {
    isPausedDurable?: (queue: string) => Promise<boolean>;
  };
  if (manager.isPausedDurable) {
    return manager.isPausedDurable(command.queue).then((paused) => ({
      ok: true,
      paused,
      reqId: requestId,
    }));
  }
  return {
    ok: true,
    paused: manager.isPaused(command.queue),
    reqId: requestId,
  } as Response;
}

export function handleObliterate(
  command: Extract<Command, { cmd: 'Obliterate' }>,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> {
  const manager = context.queueManager as typeof context.queueManager & {
    obliterateDurable?: (queue: string) => Promise<void>;
  };
  if (manager.obliterateDurable) {
    return manager.obliterateDurable(command.queue).then(() => response.ok(undefined, requestId));
  }
  manager.obliterate(command.queue);
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
): Response | Promise<Response> {
  const manager = context.queueManager as typeof context.queueManager & {
    cleanDurable?: (
      queue: string,
      graceMs: number,
      state?: string,
      limit?: number
    ) => Promise<string[]>;
  };
  if (manager.cleanDurable) {
    return manager
      .cleanDurable(command.queue, command.grace, command.state, command.limit)
      .then((ids) => cleanResponse(manager, command.queue, command.state, ids, requestId));
  }
  const ids = manager.clean(command.queue, command.grace, command.state, command.limit);
  return cleanResponse(manager, command.queue, command.state, ids, requestId);
}

function cleanResponse(
  manager: HandlerContext['queueManager'],
  queue: string,
  state: string | undefined,
  ids: readonly string[],
  requestId?: string
): Response {
  if (ids.length > 0) {
    manager.emitDashboardEvent('queue:cleaned', {
      queue,
      state,
      count: ids.length,
    });
  }
  return { ok: true, count: ids.length, ids: [...ids], reqId: requestId };
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
): Response | Promise<Response> {
  const limit = toFiniteNumber(command.limit);
  if (limit === undefined) return response.error('limit must be a finite number', requestId);
  const duration = toFiniteNumber(command.duration);
  const ttl = toFiniteNumber(command.ttl);
  const manager = context.queueManager as typeof context.queueManager & {
    setRateLimitDurable?: (
      queue: string,
      limit: number | null,
      durationMs: number | null,
      ttlMs?: number | null
    ) => Promise<void>;
  };
  if (manager.setRateLimitDurable) {
    return manager.setRateLimitDurable(command.queue, limit, duration ?? 1000, ttl).then(() => {
      manager.emitDashboardEvent('ratelimit:set', {
        queue: command.queue,
        max: limit,
        duration: duration ?? 1000,
      });
      return response.ok(undefined, requestId);
    });
  }
  manager.setRateLimit(command.queue, limit, duration, ttl);
  manager.emitDashboardEvent('ratelimit:set', {
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
): Response | Promise<Response> {
  const manager = context.queueManager as typeof context.queueManager & {
    setRateLimitDurable?: (
      queue: string,
      limit: number | null,
      durationMs: number | null,
      ttlMs?: number | null
    ) => Promise<void>;
  };
  if (manager.setRateLimitDurable) {
    return manager.setRateLimitDurable(command.queue, null, null).then(() => {
      manager.emitDashboardEvent('ratelimit:cleared', { queue: command.queue });
      return response.ok(undefined, requestId);
    });
  }
  manager.clearRateLimit(command.queue);
  manager.emitDashboardEvent('ratelimit:cleared', { queue: command.queue });
  return response.ok(undefined, requestId);
}

export function handleSetConcurrency(
  command: Extract<Command, { cmd: 'SetConcurrency' }>,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> {
  const limit = toFiniteNumber(command.limit);
  if (limit === undefined) return response.error('limit must be a finite number', requestId);
  const manager = context.queueManager as typeof context.queueManager & {
    setConcurrencyDurable?: (queue: string, limit: number | null) => Promise<void>;
  };
  if (manager.setConcurrencyDurable) {
    return manager.setConcurrencyDurable(command.queue, limit).then(() => {
      manager.emitDashboardEvent('concurrency:set', {
        queue: command.queue,
        concurrency: limit,
      });
      return response.ok(undefined, requestId);
    });
  }
  manager.setConcurrency(command.queue, limit);
  manager.emitDashboardEvent('concurrency:set', {
    queue: command.queue,
    concurrency: limit,
  });
  return response.ok(undefined, requestId);
}

export function handleClearConcurrency(
  command: Extract<Command, { cmd: 'ClearConcurrency' }>,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> {
  const manager = context.queueManager as typeof context.queueManager & {
    setConcurrencyDurable?: (queue: string, limit: number | null) => Promise<void>;
  };
  if (manager.setConcurrencyDurable) {
    return manager.setConcurrencyDurable(command.queue, null).then(() => {
      manager.emitDashboardEvent('concurrency:cleared', { queue: command.queue });
      return response.ok(undefined, requestId);
    });
  }
  manager.clearConcurrency(command.queue);
  manager.emitDashboardEvent('concurrency:cleared', { queue: command.queue });
  return response.ok(undefined, requestId);
}

export function handleSetStallConfig(
  command: Extract<Command, { cmd: 'SetStallConfig' }>,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> {
  const config = sanitizeConfigNumbers(command.config, [
    'stallInterval',
    'maxStalls',
    'gracePeriod',
  ]);
  const manager = context.queueManager as typeof context.queueManager & {
    setStallConfigDurable?: (queue: string, patch: Record<string, unknown>) => Promise<void>;
  };
  const complete = (): Response => {
    manager.emitDashboardEvent('config:stall-changed', { queue: command.queue, config });
    return response.ok(undefined, requestId);
  };
  if (manager.setStallConfigDurable) {
    return manager.setStallConfigDurable(command.queue, config).then(complete);
  }
  manager.setStallConfig(command.queue, config);
  return complete();
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
): Response | Promise<Response> {
  const config = sanitizeConfigNumbers(command.config, [
    'autoRetryInterval',
    'maxAutoRetries',
    'maxAge',
    'maxEntries',
  ]);
  const manager = context.queueManager as typeof context.queueManager & {
    setDlqConfigDurable?: (queue: string, patch: Record<string, unknown>) => Promise<void>;
  };
  const complete = (): Response => {
    manager.emitDashboardEvent('config:dlq-changed', { queue: command.queue, config });
    return response.ok(undefined, requestId);
  };
  if (manager.setDlqConfigDurable) {
    return manager.setDlqConfigDurable(command.queue, config).then(complete);
  }
  manager.setDlqConfig(command.queue, config);
  return complete();
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
