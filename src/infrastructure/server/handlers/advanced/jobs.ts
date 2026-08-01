import type { Command } from '../../../../domain/types/command';
import { jobId } from '../../../../domain/types/job';
import type { Response } from '../../../../domain/types/response';
import * as response from '../../../../domain/types/response';
import { validateNumericField } from '../../protocol';
import type { HandlerContext } from '../../types';

export async function handleUpdate(
  command: Extract<Command, { cmd: 'Update' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  const success = await context.queueManager.updateJobData(jobId(command.id), command.data);
  if (success) context.queueManager.emitDashboardEvent('job:data-updated', { jobId: command.id });
  return success
    ? response.ok(undefined, requestId)
    : response.error('Job not found or cannot be updated', requestId);
}

export async function handleChangePriority(
  command: Extract<Command, { cmd: 'ChangePriority' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  const success = await context.queueManager.changePriority(
    jobId(command.id),
    command.priority,
    command.lifo
  );
  if (success) {
    context.queueManager.emitDashboardEvent('job:priority-changed', {
      jobId: command.id,
      newPriority: command.priority,
    });
  }
  return success
    ? response.ok(undefined, requestId)
    : response.error('Job not found or not in queue', requestId);
}

export async function handlePromote(
  command: Extract<Command, { cmd: 'Promote' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  const success = await context.queueManager.promote(jobId(command.id));
  if (success) context.queueManager.emitDashboardEvent('job:promoted', { jobId: command.id });
  return success
    ? response.ok(undefined, requestId)
    : response.error('Job not found or not delayed', requestId);
}

export async function handleUpdateParent(
  command: Extract<Command, { cmd: 'UpdateParent' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  try {
    await context.queueManager.updateJobParent(jobId(command.childId), jobId(command.parentId));
    return response.ok(undefined, requestId);
  } catch (error) {
    return response.error(
      error instanceof Error ? error.message : 'Failed to update parent',
      requestId
    );
  }
}

export async function handleMoveToDelayed(
  command: Extract<Command, { cmd: 'MoveToDelayed' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  const id = jobId(command.id);
  const success = await context.queueManager.moveToDelayed(id, command.delay);
  if (success) {
    context.queueManager.emitDashboardEvent('job:moved-to-delayed', {
      jobId: command.id,
      delay: command.delay,
    });
    return response.ok(undefined, requestId);
  }
  const state = await context.queueManager.getJobState(id);
  if (state === 'unknown') return response.error('Job not found', requestId);
  return response.error(`Job is not active (current state: ${state})`, requestId);
}

export async function handleDiscard(
  command: Extract<Command, { cmd: 'Discard' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  const success = await context.queueManager.discard(jobId(command.id));
  if (success) context.queueManager.emitDashboardEvent('job:discarded', { jobId: command.id });
  return success ? response.ok(undefined, requestId) : response.error('Job not found', requestId);
}

export async function handleWaitJob(
  command: Extract<Command, { cmd: 'WaitJob' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  const id = jobId(command.id);
  const timeoutError = validateNumericField(command.timeout, 'timeout', {
    min: 0,
    max: 600000,
  });
  if (timeoutError) return response.error(timeoutError, requestId);
  const timeout = command.timeout ?? 30000;
  const job = await context.queueManager.getJob(id);
  if (!job) return response.error('Job not found', requestId);
  if (job.completedAt) {
    return {
      ok: true,
      completed: true,
      result: context.queueManager.getResult(id),
      reqId: requestId,
    } as Response;
  }

  const completed = await context.queueManager.waitForJobCompletion(id, timeout);
  if (completed) {
    return {
      ok: true,
      completed: true,
      result: context.queueManager.getResult(id),
      reqId: requestId,
    } as Response;
  }
  return { ok: true, completed: false, reqId: requestId } as Response;
}

export async function handleChangeDelay(
  command: Extract<Command, { cmd: 'ChangeDelay' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  const success = await context.queueManager.changeDelay(jobId(command.id), command.delay);
  if (success) {
    context.queueManager.emitDashboardEvent('job:delay-changed', {
      jobId: command.id,
      newDelay: command.delay,
    });
  }
  return success
    ? response.ok(undefined, requestId)
    : response.error('Job not found or cannot change delay', requestId);
}

export async function handleMoveToWait(
  command: Extract<Command, { cmd: 'MoveToWait' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  const id = jobId(command.id);
  const state = await context.queueManager.getJobState(id);
  if (state === 'active') {
    const success = await context.queueManager.moveActiveToWait(id);
    return success
      ? response.ok(undefined, requestId)
      : response.error('Job not found or not active', requestId);
  }
  if (state === 'delayed') {
    const success = await context.queueManager.promote(id);
    return success
      ? response.ok(undefined, requestId)
      : response.error('Job not found or not delayed', requestId);
  }
  if (state === 'failed') {
    const job = await context.queueManager.getJob(id);
    if (!job) return response.error('Job not found', requestId);
    const count = context.queueManager.retryDlq(job.queue, id);
    return count > 0
      ? response.ok(undefined, requestId)
      : response.error('Failed job not in DLQ', requestId);
  }
  if (state === 'waiting' || state === 'prioritized') return response.ok(undefined, requestId);
  return response.error(`Cannot move job from state '${state}' to waiting`, requestId);
}

export async function handlePromoteJobs(
  command: Extract<Command, { cmd: 'PromoteJobs' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  const count = await context.queueManager.promoteJobs(command.queue, command.count);
  return { ok: true, count, reqId: requestId };
}
