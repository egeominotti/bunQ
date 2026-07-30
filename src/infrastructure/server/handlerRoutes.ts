/**
 * Handler Routes - Command routing by category
 */

import type { Command } from '../../domain/types/command';
import type { Response } from '../../domain/types/response';
import type { HandlerContext } from './types';

// Import handlers
import {
  handlePush,
  handlePushBatch,
  handlePull,
  handlePullBatch,
  handleAck,
  handleAckBatch,
  handleFail,
} from './handlers/core';
import { handlePushFlow } from './handlers/flow';

import {
  handleGetJob,
  handleGetState,
  handleGetResult,
  handleGetJobCounts,
  handleGetCountsPerPriority,
  handleGetJobByCustomId,
  handleGetJobs,
  handleGetChildrenValues,
} from './handlers/query';

import {
  handleCancel,
  handleProgress,
  handleGetProgress,
  handlePause,
  handleResume,
  handleDrain,
  handleStats,
  handleMetrics,
  handleStorageStatus,
} from './handlers/management';

import { handleDlq, handleRetryDlq, handlePurgeDlq, handleRetryCompleted } from './handlers/dlq';

import { handleCron, handleCronGet, handleCronDelete, handleCronList } from './handlers/cron';

import {
  handleUpdate,
  handleUpdateParent,
  handleChangePriority,
  handlePromote,
  handleMoveToDelayed,
  handleDiscard,
  handleWaitJob,
  handleIsPaused,
  handleObliterate,
  handleListQueues,
  handleClean,
  handleCount,
  handleRateLimit,
  handleRateLimitClear,
  handleSetConcurrency,
  handleClearConcurrency,
  handleChangeDelay,
  handleMoveToWait,
  handlePromoteJobs,
  handleSetStallConfig,
  handleGetStallConfig,
  handleSetDlqConfig,
  handleGetDlqConfig,
  handleGetFailedChildrenValues,
  handleGetIgnoredChildrenFailures,
  handleRemoveChildDependency,
  handleRemoveUnprocessedChildren,
} from './handlers/advanced';

import {
  handleDashboardOverview,
  handleDashboardQueues,
  handleDashboardQueue,
} from './handlers/dashboard';

import {
  handleAddLog,
  handleGetLogs,
  handleHeartbeat,
  handleJobHeartbeat,
  handleJobHeartbeatBatch,
  handlePing,
  handleHello,
  handleRegisterWorker,
  handleUnregisterWorker,
  handleListWorkers,
  handleAddWebhook,
  handleRemoveWebhook,
  handleListWebhooks,
  handlePrometheus,
  handleClearLogs,
  handleExtendLock,
  handleExtendLocks,
  handleSetWebhookEnabled,
  handleCompactMemory,
} from './handlers/monitoring';

/** Route core commands (PUSH, PULL, ACK, FAIL) */
export async function routeCoreCommand(
  cmd: Command,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response | null> {
  // eslint-disable-next-line @typescript-eslint/switch-exhaustiveness-check
  switch (cmd.cmd) {
    case 'PUSH':
      return handlePush(cmd, ctx, reqId);
    case 'PUSHB':
      return handlePushBatch(cmd, ctx, reqId);
    case 'PUSHF':
      return handlePushFlow(cmd, ctx, reqId);
    case 'PULL':
      return handlePull(cmd, ctx, reqId);
    case 'PULLB':
      return handlePullBatch(cmd, ctx, reqId);
    case 'ACK':
      return handleAck(cmd, ctx, reqId);
    case 'ACKB':
      return handleAckBatch(cmd, ctx, reqId);
    case 'FAIL':
      return handleFail(cmd, ctx, reqId);
    default:
      return null;
  }
}

/** Route query commands */
export async function routeQueryCommand(
  cmd: Command,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response | null> {
  // eslint-disable-next-line @typescript-eslint/switch-exhaustiveness-check
  switch (cmd.cmd) {
    case 'GetJob':
      return handleGetJob(cmd, ctx, reqId);
    case 'GetState':
      return handleGetState(cmd, ctx, reqId);
    case 'GetResult':
      return handleGetResult(cmd, ctx, reqId);
    case 'GetJobCounts':
      return handleGetJobCounts(cmd, ctx, reqId);
    case 'GetCountsPerPriority':
      return handleGetCountsPerPriority(cmd, ctx, reqId);
    case 'GetJobByCustomId':
      return handleGetJobByCustomId(cmd, ctx, reqId);
    case 'GetJobs':
      return handleGetJobs(cmd, ctx, reqId);
    case 'Count':
      return handleCount(cmd, ctx, reqId);
    case 'GetProgress':
      return handleGetProgress(cmd, ctx, reqId);
    case 'GetChildrenValues':
      return handleGetChildrenValues(cmd, ctx, reqId);
    default:
      return null;
  }
}

/** Route management commands */
export async function routeManagementCommand(
  cmd: Command,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response | null> {
  // eslint-disable-next-line @typescript-eslint/switch-exhaustiveness-check
  switch (cmd.cmd) {
    case 'Cancel':
      return handleCancel(cmd, ctx, reqId);
    case 'Progress':
      return handleProgress(cmd, ctx, reqId);
    case 'Update':
      return handleUpdate(cmd, ctx, reqId);
    case 'UpdateParent':
      return handleUpdateParent(cmd, ctx, reqId);
    case 'ChangePriority':
      return handleChangePriority(cmd, ctx, reqId);
    case 'Promote':
      return handlePromote(cmd, ctx, reqId);
    case 'MoveToDelayed':
      return handleMoveToDelayed(cmd, ctx, reqId);
    case 'Discard':
      return handleDiscard(cmd, ctx, reqId);
    case 'WaitJob':
      return handleWaitJob(cmd, ctx, reqId);
    case 'ChangeDelay':
      return handleChangeDelay(cmd, ctx, reqId);
    case 'MoveToWait':
      return handleMoveToWait(cmd, ctx, reqId);
    case 'PromoteJobs':
      return handlePromoteJobs(cmd, ctx, reqId);
    case 'ExtendLock':
      return handleExtendLock(cmd, ctx, reqId);
    case 'ExtendLocks':
      return handleExtendLocks(cmd, ctx, reqId);
    case 'GetFailedChildrenValues':
      return handleGetFailedChildrenValues(cmd, ctx, reqId);
    case 'GetIgnoredChildrenFailures':
      return handleGetIgnoredChildrenFailures(cmd, ctx, reqId);
    case 'RemoveChildDependency':
      return handleRemoveChildDependency(cmd, ctx, reqId);
    case 'RemoveUnprocessedChildren':
      return handleRemoveUnprocessedChildren(cmd, ctx, reqId);
    default:
      return null;
  }
}

/** Route queue control commands */
export function routeQueueControlCommand(
  cmd: Command,
  ctx: HandlerContext,
  reqId?: string
): Response | null {
  // eslint-disable-next-line @typescript-eslint/switch-exhaustiveness-check
  switch (cmd.cmd) {
    case 'Pause':
      return handlePause(cmd, ctx, reqId);
    case 'Resume':
      return handleResume(cmd, ctx, reqId);
    case 'IsPaused':
      return handleIsPaused(cmd, ctx, reqId);
    case 'Drain':
      return handleDrain(cmd, ctx, reqId);
    case 'Obliterate':
      return handleObliterate(cmd, ctx, reqId);
    case 'ListQueues':
      return handleListQueues(cmd, ctx, reqId);
    case 'Clean':
      return handleClean(cmd, ctx, reqId);
    default:
      return null;
  }
}

/** Route DLQ commands */
export function routeDlqCommand(
  cmd: Command,
  ctx: HandlerContext,
  reqId?: string
): Response | null {
  // eslint-disable-next-line @typescript-eslint/switch-exhaustiveness-check
  switch (cmd.cmd) {
    case 'Dlq':
      return handleDlq(cmd, ctx, reqId);
    case 'RetryDlq':
      return handleRetryDlq(cmd, ctx, reqId);
    case 'PurgeDlq':
      return handlePurgeDlq(cmd, ctx, reqId);
    case 'RetryCompleted':
      return handleRetryCompleted(cmd, ctx, reqId);
    default:
      return null;
  }
}

/** Route rate limit commands */
export function routeRateLimitCommand(
  cmd: Command,
  ctx: HandlerContext,
  reqId?: string
): Response | null {
  // eslint-disable-next-line @typescript-eslint/switch-exhaustiveness-check
  switch (cmd.cmd) {
    case 'RateLimit':
      return handleRateLimit(cmd, ctx, reqId);
    case 'RateLimitClear':
      return handleRateLimitClear(cmd, ctx, reqId);
    case 'SetConcurrency':
      return handleSetConcurrency(cmd, ctx, reqId);
    case 'ClearConcurrency':
      return handleClearConcurrency(cmd, ctx, reqId);
    default:
      return null;
  }
}

/** Route cron commands */
export function routeCronCommand(
  cmd: Command,
  ctx: HandlerContext,
  reqId?: string
): Response | null {
  // eslint-disable-next-line @typescript-eslint/switch-exhaustiveness-check
  switch (cmd.cmd) {
    case 'Cron':
      return handleCron(cmd, ctx, reqId);
    case 'CronGet':
      return handleCronGet(cmd, ctx, reqId);
    case 'CronDelete':
      return handleCronDelete(cmd, ctx, reqId);
    case 'CronList':
      return handleCronList(cmd, ctx, reqId);
    default:
      return null;
  }
}

/** Route config commands (stall/DLQ config) */
export function routeConfigCommand(
  cmd: Command,
  ctx: HandlerContext,
  reqId?: string
): Response | null {
  // eslint-disable-next-line @typescript-eslint/switch-exhaustiveness-check
  switch (cmd.cmd) {
    case 'SetStallConfig':
      return handleSetStallConfig(cmd, ctx, reqId);
    case 'GetStallConfig':
      return handleGetStallConfig(cmd, ctx, reqId);
    case 'SetDlqConfig':
      return handleSetDlqConfig(cmd, ctx, reqId);
    case 'GetDlqConfig':
      return handleGetDlqConfig(cmd, ctx, reqId);
    default:
      return null;
  }
}

/** Route monitoring commands */
export function routeMonitoringCommand(
  cmd: Command,
  ctx: HandlerContext,
  reqId?: string
): Response | null {
  // eslint-disable-next-line @typescript-eslint/switch-exhaustiveness-check
  switch (cmd.cmd) {
    case 'Stats':
      return handleStats(ctx, reqId);
    case 'Metrics':
      return handleMetrics(ctx, reqId);
    case 'Prometheus':
      return handlePrometheus(cmd, ctx, reqId);
    case 'AddLog':
      return handleAddLog(cmd, ctx, reqId);
    case 'GetLogs':
      return handleGetLogs(cmd, ctx, reqId);
    case 'Heartbeat':
      return handleHeartbeat(cmd, ctx, reqId);
    case 'JobHeartbeat':
      return handleJobHeartbeat(cmd, ctx, reqId);
    case 'JobHeartbeatB':
      return handleJobHeartbeatBatch(cmd, ctx, reqId);
    case 'Ping':
      return handlePing(cmd, ctx, reqId);
    case 'Hello':
      return handleHello(cmd, ctx, reqId);
    case 'RegisterWorker':
      return handleRegisterWorker(cmd, ctx, reqId);
    case 'UnregisterWorker':
      return handleUnregisterWorker(cmd, ctx, reqId);
    case 'ListWorkers':
      return handleListWorkers(cmd, ctx, reqId);
    case 'AddWebhook':
      return handleAddWebhook(cmd, ctx, reqId);
    case 'RemoveWebhook':
      return handleRemoveWebhook(cmd, ctx, reqId);
    case 'ListWebhooks':
      return handleListWebhooks(cmd, ctx, reqId);
    case 'StorageStatus':
      return handleStorageStatus(cmd, ctx, reqId);
    case 'ClearLogs':
      return handleClearLogs(cmd, ctx, reqId);
    case 'SetWebhookEnabled':
      return handleSetWebhookEnabled(cmd, ctx, reqId);
    case 'CompactMemory':
      return handleCompactMemory(cmd, ctx, reqId);
    default:
      return null;
  }
}

/** Route dashboard commands */
export function routeDashboardCommand(
  cmd: Command,
  ctx: HandlerContext,
  reqId?: string
): Response | null {
  // eslint-disable-next-line @typescript-eslint/switch-exhaustiveness-check
  switch (cmd.cmd) {
    case 'DashboardOverview':
      return handleDashboardOverview(cmd, ctx, reqId);
    case 'DashboardQueues':
      return handleDashboardQueues(cmd, ctx, reqId);
    case 'DashboardQueue':
      return handleDashboardQueue(cmd, ctx, reqId);
    default:
      return null;
  }
}
