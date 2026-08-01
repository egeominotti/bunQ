import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import {
  handleClean,
  handleClearConcurrency,
  handleGetDlqConfig,
  handleGetStallConfig,
  handleIsPaused,
  handleListQueues,
  handleObliterate,
  handleRateLimit,
  handleRateLimitClear,
  handleSetConcurrency,
  handleSetDlqConfig,
  handleSetStallConfig,
} from '../handlers/advanced';
import { handleCron, handleCronDelete, handleCronGet, handleCronList } from '../handlers/cron';
import {
  handleDlq,
  handleGetDlqStats,
  handlePurgeDlq,
  handleRetryCompleted,
  handleRetryDlq,
} from '../handlers/dlq';
import { handleDrain, handlePause, handleResume } from '../handlers/management';
import type { HandlerContext } from '../types';

export function routeQueueControlCommand(
  command: Command,
  context: HandlerContext,
  requestId?: string
): Response | null {
  switch (command.cmd) {
    case 'Pause':
      return handlePause(command, context, requestId);
    case 'Resume':
      return handleResume(command, context, requestId);
    case 'IsPaused':
      return handleIsPaused(command, context, requestId);
    case 'Drain':
      return handleDrain(command, context, requestId);
    case 'Obliterate':
      return handleObliterate(command, context, requestId);
    case 'ListQueues':
      return handleListQueues(command, context, requestId);
    case 'Clean':
      return handleClean(command, context, requestId);
    default:
      return null;
  }
}

export function routeDlqCommand(
  command: Command,
  context: HandlerContext,
  requestId?: string
): Response | null {
  switch (command.cmd) {
    case 'Dlq':
      return handleDlq(command, context, requestId);
    case 'GetDlqStats':
      return handleGetDlqStats(command, context, requestId);
    case 'RetryDlq':
      return handleRetryDlq(command, context, requestId);
    case 'PurgeDlq':
      return handlePurgeDlq(command, context, requestId);
    case 'RetryCompleted':
      return handleRetryCompleted(command, context, requestId);
    default:
      return null;
  }
}

export function routeRateLimitCommand(
  command: Command,
  context: HandlerContext,
  requestId?: string
): Response | null {
  switch (command.cmd) {
    case 'RateLimit':
      return handleRateLimit(command, context, requestId);
    case 'RateLimitClear':
      return handleRateLimitClear(command, context, requestId);
    case 'SetConcurrency':
      return handleSetConcurrency(command, context, requestId);
    case 'ClearConcurrency':
      return handleClearConcurrency(command, context, requestId);
    default:
      return null;
  }
}

export function routeCronCommand(
  command: Command,
  context: HandlerContext,
  requestId?: string
): Response | null {
  switch (command.cmd) {
    case 'Cron':
      return handleCron(command, context, requestId);
    case 'CronGet':
      return handleCronGet(command, context, requestId);
    case 'CronDelete':
      return handleCronDelete(command, context, requestId);
    case 'CronList':
      return handleCronList(command, context, requestId);
    default:
      return null;
  }
}

export function routeConfigCommand(
  command: Command,
  context: HandlerContext,
  requestId?: string
): Response | null {
  switch (command.cmd) {
    case 'SetStallConfig':
      return handleSetStallConfig(command, context, requestId);
    case 'GetStallConfig':
      return handleGetStallConfig(command, context, requestId);
    case 'SetDlqConfig':
      return handleSetDlqConfig(command, context, requestId);
    case 'GetDlqConfig':
      return handleGetDlqConfig(command, context, requestId);
    default:
      return null;
  }
}
