import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import {
  handleDashboardOverview,
  handleDashboardQueue,
  handleDashboardQueues,
} from '../handlers/dashboard';
import {
  handleMetrics,
  handleQueueMetrics,
  handleStats,
  handleStorageStatus,
  handleTrimEvents,
} from '../handlers/management';
import {
  handleAddLog,
  handleAddWebhook,
  handleClearLogs,
  handleCompactMemory,
  handleGetLogs,
  handleHeartbeat,
  handleHello,
  handleJobHeartbeat,
  handleJobHeartbeatBatch,
  handleListWebhooks,
  handleListWorkers,
  handlePing,
  handlePrometheus,
  handleRegisterWorker,
  handleRemoveWebhook,
  handleSetWebhookEnabled,
  handleUnregisterWorker,
} from '../handlers/monitoring';
import type { HandlerContext } from '../types';

export function routeMonitoringCommand(
  command: Command,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> | null {
  switch (command.cmd) {
    case 'Stats':
      return handleStats(context, requestId);
    case 'Metrics':
      return command.queue !== undefined || command.type !== undefined
        ? handleQueueMetrics(command, context, requestId)
        : handleMetrics(context, requestId);
    case 'TrimEvents':
      return handleTrimEvents(command, context, requestId);
    case 'Prometheus':
      return handlePrometheus(command, context, requestId);
    case 'AddLog':
      return handleAddLog(command, context, requestId);
    case 'GetLogs':
      return handleGetLogs(command, context, requestId);
    case 'Heartbeat':
      return handleHeartbeat(command, context, requestId);
    case 'JobHeartbeat':
      return handleJobHeartbeat(command, context, requestId);
    case 'JobHeartbeatB':
      return handleJobHeartbeatBatch(command, context, requestId);
    case 'Ping':
      return handlePing(command, context, requestId);
    case 'Hello':
      return handleHello(command, context, requestId);
    case 'RegisterWorker':
      return handleRegisterWorker(command, context, requestId);
    case 'UnregisterWorker':
      return handleUnregisterWorker(command, context, requestId);
    case 'ListWorkers':
      return handleListWorkers(command, context, requestId);
    case 'AddWebhook':
      return handleAddWebhook(command, context, requestId);
    case 'RemoveWebhook':
      return handleRemoveWebhook(command, context, requestId);
    case 'ListWebhooks':
      return handleListWebhooks(command, context, requestId);
    case 'StorageStatus':
      return handleStorageStatus(command, context, requestId);
    case 'ClearLogs':
      return handleClearLogs(command, context, requestId);
    case 'SetWebhookEnabled':
      return handleSetWebhookEnabled(command, context, requestId);
    case 'CompactMemory':
      return handleCompactMemory(command, context, requestId);
    default:
      return null;
  }
}

export function routeDashboardCommand(
  command: Command,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> | null {
  switch (command.cmd) {
    case 'DashboardOverview':
      return handleDashboardOverview(command, context, requestId);
    case 'DashboardQueues':
      return handleDashboardQueues(command, context, requestId);
    case 'DashboardQueue':
      return handleDashboardQueue(command, context, requestId);
    default:
      return null;
  }
}
