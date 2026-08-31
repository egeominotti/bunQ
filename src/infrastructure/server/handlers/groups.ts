import type { Command } from '../../../domain/types/command';
import { validatePositiveSafeInteger } from '../../../domain/types/group';
import type { Response } from '../../../domain/types/response';
import * as resp from '../../../domain/types/response';
import { validateGroupId, validateQueueName } from '../protocol';
import type { HandlerContext } from '../types';

type GroupCommand = Extract<Command, { groupId: string }>;

function validate(command: GroupCommand): string | null {
  const queueError = validateQueueName(command.queue);
  if (queueError) return queueError;
  return validateGroupId(command.groupId, true);
}

export async function handleGroupCommand(
  command: GroupCommand,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  const error = validate(command);
  if (error) return resp.error(error, requestId);
  const manager = context.queueManager;

  switch (command.cmd) {
    case 'GetGroupJobsCount':
      return resp.data(
        { count: await manager.getGroupJobsCount(command.queue, command.groupId) },
        requestId
      );
    case 'GetGroupActiveCount':
      return resp.data(
        { count: await manager.getGroupActiveCount(command.queue, command.groupId) },
        requestId
      );
    case 'SetGroupRateLimit': {
      const maxError = validatePositiveSafeInteger(command.max, 'max');
      const durationError = validatePositiveSafeInteger(command.duration, 'duration');
      const numericError = maxError ?? durationError;
      if (numericError) return resp.error(numericError, requestId);
      await manager.setGroupRateLimit(
        command.queue,
        command.groupId,
        command.max,
        command.duration
      );
      return resp.ok(undefined, requestId);
    }
    case 'GetGroupRateLimit':
      return resp.data(
        { limit: await manager.getGroupRateLimit(command.queue, command.groupId) },
        requestId
      );
    case 'RemoveGroupRateLimit':
      return resp.data(
        { removed: await manager.removeGroupRateLimit(command.queue, command.groupId) },
        requestId
      );
    case 'GetGroupRateLimitTtl':
      return resp.data(
        {
          ttl: await manager.getGroupRateLimitTtl(command.queue, command.groupId, command.maxJobs),
        },
        requestId
      );
    case 'SetGroupConcurrency': {
      const concurrencyError = validatePositiveSafeInteger(command.concurrency, 'concurrency');
      if (concurrencyError) return resp.error(concurrencyError, requestId);
      await manager.setGroupConcurrency(command.queue, command.groupId, command.concurrency);
      return resp.ok(undefined, requestId);
    }
    case 'GetGroupConcurrency':
      return resp.data(
        { concurrency: await manager.getGroupConcurrency(command.queue, command.groupId) },
        requestId
      );
    case 'RemoveGroupConcurrency':
      return resp.data(
        { removed: await manager.removeGroupConcurrency(command.queue, command.groupId) },
        requestId
      );
  }
}

export async function handleGroupsJobsCount(
  command: Extract<Command, { cmd: 'GetGroupsJobsCount' }>,
  context: HandlerContext,
  requestId?: string
): Promise<Response> {
  const queueError = validateQueueName(command.queue);
  if (queueError) return resp.error(queueError, requestId);
  return resp.data(
    { count: await context.queueManager.getGroupsJobsCount(command.queue) },
    requestId
  );
}
