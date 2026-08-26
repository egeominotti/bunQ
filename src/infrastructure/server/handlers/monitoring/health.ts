import type {
  AddLogCommand,
  GetLogsCommand,
  HeartbeatCommand,
  HelloCommand,
  JobHeartbeatBatchCommand,
  JobHeartbeatCommand,
  PingCommand,
} from '../../../../domain/types/command';
import { jobId, type JobId } from '../../../../domain/types/job';
import {
  PROTOCOL_CAPABILITIES,
  PROTOCOL_VERSION,
  type ProtocolCapability,
} from '../../../../domain/types/protocol';
import type { Response } from '../../../../domain/types/response';
import * as response from '../../../../domain/types/response';
import { VERSION } from '../../../../shared/version';
import type { HandlerContext } from '../../types';

export { PROTOCOL_VERSION };
export const SUPPORTED_CAPABILITIES: ProtocolCapability[] = [...PROTOCOL_CAPABILITIES];

export function handleAddLog(
  command: AddLogCommand,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> {
  const id = jobId(command.id);
  const level = command.level ?? 'info';
  const manager = context.queueManager as typeof context.queueManager & {
    addLogDurable?: (id: JobId, message: string, value: typeof level) => Promise<boolean>;
  };
  const complete = (success: boolean): Response =>
    success
      ? response.data({ added: true }, requestId)
      : response.error('Job not found', requestId);
  return manager.addLogDurable
    ? manager.addLogDurable(id, command.message, level).then(complete)
    : complete(manager.addLog(id, command.message, level));
}

export function handleGetLogs(
  command: GetLogsCommand,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> {
  const id = jobId(command.id);
  const manager = context.queueManager as typeof context.queueManager & {
    getLogsDurable?: (id: JobId) => Promise<ReturnType<typeof context.queueManager.getLogs>>;
  };
  const complete = (all: ReturnType<typeof context.queueManager.getLogs>): Response => {
    const total = all.length;
    const logs =
      command.start === undefined && command.end === undefined
        ? all
        : all.slice(command.start ?? 0, (command.end ?? total - 1) + 1);
    return response.data({ logs, count: total }, requestId);
  };
  return manager.getLogsDurable
    ? manager.getLogsDurable(id).then(complete)
    : complete(manager.getLogs(id));
}

export function handleHeartbeat(
  command: HeartbeatCommand,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> {
  const stats =
    command.activeJobs !== undefined ||
    command.processed !== undefined ||
    command.failed !== undefined
      ? {
          activeJobs: command.activeJobs,
          processed: command.processed,
          failed: command.failed,
        }
      : undefined;
  const manager = context.queueManager as typeof context.queueManager & {
    heartbeatWorkerDurable?: (
      id: string,
      value?: typeof stats,
      clientId?: string
    ) => Promise<boolean>;
  };
  const complete = (success: boolean): Response => {
    if (success) {
      manager.emitDashboardEvent('worker:heartbeat', { workerId: command.id });
      return response.data({ ok: true }, requestId);
    }
    return response.error('Worker not found', requestId);
  };
  return manager.heartbeatWorkerDurable
    ? manager.heartbeatWorkerDurable(command.id, stats, context.clientId).then(complete)
    : complete(manager.workerManager.heartbeat(command.id, stats));
}

export function handleJobHeartbeat(
  command: JobHeartbeatCommand,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> {
  const id = jobId(command.id);
  const manager = context.queueManager as typeof context.queueManager & {
    heartbeatDurable?: (id: JobId, token?: string, duration?: number) => Promise<boolean>;
  };
  if (manager.heartbeatDurable) {
    return manager
      .heartbeatDurable(id, command.token, command.duration)
      .then((success) =>
        success
          ? response.data({ ok: true }, requestId)
          : response.error('Job not found or not active (or invalid token)', requestId)
      );
  }
  if (command.duration && command.token) {
    const success = context.queueManager.renewJobLock(id, command.token, command.duration);
    return success
      ? response.data({ ok: true }, requestId)
      : response.error('Job not found or not active (or invalid token)', requestId);
  }
  const success = context.queueManager.jobHeartbeat(id, command.token);
  return success
    ? response.data({ ok: true }, requestId)
    : response.error('Job not found or not active (or invalid token)', requestId);
}

export function handleJobHeartbeatBatch(
  command: JobHeartbeatBatchCommand,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> {
  const ids = command.ids.map((id) => jobId(id));
  const manager = context.queueManager as typeof context.queueManager & {
    heartbeatBatchDurable?: (ids: JobId[], tokens?: string[]) => Promise<number>;
  };
  if (manager.heartbeatBatchDurable) {
    return manager
      .heartbeatBatchDurable(ids, command.tokens)
      .then((count) => response.data({ ok: true, count }, requestId));
  }
  return response.data(
    { ok: true, count: manager.jobHeartbeatBatch(ids, command.tokens) },
    requestId
  );
}

export function handlePing(
  _command: PingCommand,
  _context: HandlerContext,
  requestId?: string
): Response {
  return response.data({ pong: true, time: Date.now() }, requestId);
}

export function handleHello(
  _command: HelloCommand,
  _context: HandlerContext,
  requestId?: string
): Response {
  return response.hello(PROTOCOL_VERSION, SUPPORTED_CAPABILITIES, 'bunqueue', VERSION, requestId);
}
