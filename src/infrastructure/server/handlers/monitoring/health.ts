import type {
  AddLogCommand,
  GetLogsCommand,
  HeartbeatCommand,
  HelloCommand,
  JobHeartbeatBatchCommand,
  JobHeartbeatCommand,
  PingCommand,
} from '../../../../domain/types/command';
import { jobId } from '../../../../domain/types/job';
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
): Response {
  const success = context.queueManager.addLog(
    jobId(command.id),
    command.message,
    command.level ?? 'info'
  );
  return success
    ? response.data({ added: true }, requestId)
    : response.error('Job not found', requestId);
}

export function handleGetLogs(
  command: GetLogsCommand,
  context: HandlerContext,
  requestId?: string
): Response {
  const all = context.queueManager.getLogs(jobId(command.id));
  const total = all.length;
  const logs =
    command.start === undefined && command.end === undefined
      ? all
      : all.slice(command.start ?? 0, (command.end ?? total - 1) + 1);
  return response.data({ logs, count: total }, requestId);
}

export function handleHeartbeat(
  command: HeartbeatCommand,
  context: HandlerContext,
  requestId?: string
): Response {
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
  const success = context.queueManager.workerManager.heartbeat(command.id, stats);
  if (success) {
    context.queueManager.emitDashboardEvent('worker:heartbeat', { workerId: command.id });
    return response.data({ ok: true }, requestId);
  }
  return response.error('Worker not found', requestId);
}

export function handleJobHeartbeat(
  command: JobHeartbeatCommand,
  context: HandlerContext,
  requestId?: string
): Response {
  const id = jobId(command.id);
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
): Response {
  const ids = command.ids.map((id) => jobId(id));
  const count = context.queueManager.jobHeartbeatBatch(ids, command.tokens);
  return response.data({ ok: true, count }, requestId);
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
