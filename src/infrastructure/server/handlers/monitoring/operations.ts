import type {
  ClearLogsCommand,
  CompactMemoryCommand,
  ExtendLockCommand,
  ExtendLocksCommand,
  PrometheusCommand,
} from '../../../../domain/types/command';
import { jobId } from '../../../../domain/types/job';
import type { Response } from '../../../../domain/types/response';
import * as response from '../../../../domain/types/response';
import type { HandlerContext } from '../../types';

export function handlePrometheus(
  _command: PrometheusCommand,
  context: HandlerContext,
  requestId?: string
): Response {
  return response.data({ metrics: context.queueManager.getPrometheusMetrics() }, requestId);
}

export function handleClearLogs(
  command: ClearLogsCommand,
  context: HandlerContext,
  requestId?: string
): Response {
  context.queueManager.clearLogs(jobId(command.id), command.keepLogs);
  return response.ok(undefined, requestId);
}

export async function handleExtendLock(
  command: ExtendLockCommand,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  const success = await context.queueManager.extendLock(
    jobId(command.id),
    command.token ?? null,
    command.duration
  );
  return success
    ? response.ok(undefined, requestId)
    : response.error('Lock not found or invalid token', requestId);
}

export async function handleExtendLocks(
  command: ExtendLocksCommand,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  let count = 0;
  for (let index = 0; index < command.ids.length; index++) {
    const success = await context.queueManager.extendLock(
      jobId(command.ids[index]),
      command.tokens[index] ?? null,
      command.durations[index]
    );
    if (success) count++;
  }
  return { ok: true, count, reqId: requestId };
}

export function handleCompactMemory(
  _command: CompactMemoryCommand,
  context: HandlerContext,
  requestId?: string
): Response {
  context.queueManager.compactMemory();
  return response.ok(undefined, requestId);
}
