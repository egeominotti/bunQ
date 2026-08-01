import type { Command } from '../../../../domain/types/command';
import { jobId } from '../../../../domain/types/job';
import type { Response } from '../../../../domain/types/response';
import * as response from '../../../../domain/types/response';
import type { HandlerContext } from '../../types';

export async function handleGetFailedChildrenValues(
  command: Extract<Command, { cmd: 'GetFailedChildrenValues' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  try {
    const values = await context.queueManager.getFailedChildrenValues(jobId(command.id));
    return { ok: true, values, reqId: requestId } as Response;
  } catch {
    return { ok: true, values: {}, reqId: requestId } as Response;
  }
}

export async function handleGetIgnoredChildrenFailures(
  command: Extract<Command, { cmd: 'GetIgnoredChildrenFailures' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  try {
    const values = await context.queueManager.getIgnoredChildrenFailures(jobId(command.id));
    return { ok: true, values, reqId: requestId } as Response;
  } catch {
    return { ok: true, values: {}, reqId: requestId } as Response;
  }
}

export async function handleRemoveChildDependency(
  command: Extract<Command, { cmd: 'RemoveChildDependency' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  try {
    const removed = await context.queueManager.removeChildDependency(jobId(command.id));
    return { ok: true, removed, reqId: requestId } as Response;
  } catch (error) {
    return response.error(error instanceof Error ? error.message : String(error), requestId);
  }
}

export async function handleRemoveUnprocessedChildren(
  command: Extract<Command, { cmd: 'RemoveUnprocessedChildren' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  try {
    await context.queueManager.removeUnprocessedChildren(jobId(command.id));
    return response.ok(undefined, requestId);
  } catch (error) {
    return response.error(error instanceof Error ? error.message : String(error), requestId);
  }
}
