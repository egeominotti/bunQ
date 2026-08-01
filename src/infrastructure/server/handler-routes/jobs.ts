import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import {
  handleChangeDelay,
  handleChangePriority,
  handleCount,
  handleDiscard,
  handleGetFailedChildrenValues,
  handleGetIgnoredChildrenFailures,
  handleMoveToDelayed,
  handleMoveToWait,
  handlePromote,
  handlePromoteJobs,
  handleRemoveChildDependency,
  handleRemoveUnprocessedChildren,
  handleUpdate,
  handleUpdateParent,
  handleWaitJob,
} from '../handlers/advanced';
import {
  handleAck,
  handleAckBatch,
  handleFail,
  handlePull,
  handlePullBatch,
  handlePush,
  handlePushBatch,
} from '../handlers/core';
import { handlePushFlow } from '../handlers/flow';
import {
  handleGetDeduplicationJobId,
  handleGetQueueLimits,
  handleMoveToWaitingChildren,
  handleRemoveDeduplicationKey,
  handleRemoveJobDeduplicationKey,
} from '../handlers/introspection';
import { handleCancel, handleGetProgress, handleProgress } from '../handlers/management';
import { handleExtendLock, handleExtendLocks } from '../handlers/monitoring';
import {
  handleGetChildrenValues,
  handleGetCountsPerPriority,
  handleGetJob,
  handleGetJobByCustomId,
  handleGetJobCounts,
  handleGetJobs,
  handleGetResult,
  handleGetState,
} from '../handlers/query';
import type { HandlerContext } from '../types';

// biome-ignore lint/suspicious/useAwait: stable router API always returns a Promise.
export async function routeCoreCommand(
  command: Command,
  context: HandlerContext,
  requestId?: string
): Promise<Response | null> {
  switch (command.cmd) {
    case 'PUSH':
      return handlePush(command, context, requestId);
    case 'PUSHB':
      return handlePushBatch(command, context, requestId);
    case 'PUSHF':
      return handlePushFlow(command, context, requestId);
    case 'PULL':
      return handlePull(command, context, requestId);
    case 'PULLB':
      return handlePullBatch(command, context, requestId);
    case 'ACK':
      return handleAck(command, context, requestId);
    case 'ACKB':
      return handleAckBatch(command, context, requestId);
    case 'FAIL':
      return handleFail(command, context, requestId);
    default:
      return null;
  }
}

// biome-ignore lint/suspicious/useAwait: stable router API always returns a Promise.
export async function routeQueryCommand(
  command: Command,
  context: HandlerContext,
  requestId?: string
): Promise<Response | null> {
  switch (command.cmd) {
    case 'GetJob':
      return handleGetJob(command, context, requestId);
    case 'GetState':
      return handleGetState(command, context, requestId);
    case 'GetResult':
      return handleGetResult(command, context, requestId);
    case 'GetJobCounts':
      return handleGetJobCounts(command, context, requestId);
    case 'GetCountsPerPriority':
      return handleGetCountsPerPriority(command, context, requestId);
    case 'GetJobByCustomId':
      return handleGetJobByCustomId(command, context, requestId);
    case 'GetJobs':
      return handleGetJobs(command, context, requestId);
    case 'Count':
      return handleCount(command, context, requestId);
    case 'GetProgress':
      return handleGetProgress(command, context, requestId);
    case 'GetChildrenValues':
      return handleGetChildrenValues(command, context, requestId);
    case 'GetQueueLimits':
      return handleGetQueueLimits(command, context, requestId);
    case 'GetDeduplicationJobId':
      return handleGetDeduplicationJobId(command, context, requestId);
    default:
      return null;
  }
}

// biome-ignore lint/suspicious/useAwait: stable router API always returns a Promise.
export async function routeManagementCommand(
  command: Command,
  context: HandlerContext,
  requestId?: string
): Promise<Response | null> {
  switch (command.cmd) {
    case 'Cancel':
      return handleCancel(command, context, requestId);
    case 'Progress':
      return handleProgress(command, context, requestId);
    case 'Update':
      return handleUpdate(command, context, requestId);
    case 'UpdateParent':
      return handleUpdateParent(command, context, requestId);
    case 'ChangePriority':
      return handleChangePriority(command, context, requestId);
    case 'Promote':
      return handlePromote(command, context, requestId);
    case 'MoveToDelayed':
      return handleMoveToDelayed(command, context, requestId);
    case 'Discard':
      return handleDiscard(command, context, requestId);
    case 'WaitJob':
      return handleWaitJob(command, context, requestId);
    case 'ChangeDelay':
      return handleChangeDelay(command, context, requestId);
    case 'MoveToWait':
      return handleMoveToWait(command, context, requestId);
    case 'PromoteJobs':
      return handlePromoteJobs(command, context, requestId);
    case 'ExtendLock':
      return handleExtendLock(command, context, requestId);
    case 'ExtendLocks':
      return handleExtendLocks(command, context, requestId);
    case 'GetFailedChildrenValues':
      return handleGetFailedChildrenValues(command, context, requestId);
    case 'GetIgnoredChildrenFailures':
      return handleGetIgnoredChildrenFailures(command, context, requestId);
    case 'RemoveChildDependency':
      return handleRemoveChildDependency(command, context, requestId);
    case 'RemoveUnprocessedChildren':
      return handleRemoveUnprocessedChildren(command, context, requestId);
    case 'RemoveDeduplicationKey':
      return handleRemoveDeduplicationKey(command, context, requestId);
    case 'RemoveJobDeduplicationKey':
      return handleRemoveJobDeduplicationKey(command, context, requestId);
    case 'MoveToWaitingChildren':
      return handleMoveToWaitingChildren(command, context, requestId);
    default:
      return null;
  }
}
