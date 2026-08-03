import type { Job as InternalJob } from '../../../domain/types/job';
import { UnrecoverableError } from '../../errors';
import { getSharedManager } from '../../manager';
import type { AckBatcher } from '../ackBatcher';
import type { TcpConnection } from '../types';
import { outcomeWasApplied } from '../ackOutcome';

export function computeStackLines(error: Error): {
  stackLines: string[];
  wireStack: string[] | undefined;
} {
  const stackLines = error.stack
    ? error.stack
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean)
    : [];
  const wireStack = stackLines.length > 0 ? stackLines.slice(0, 50) : undefined;
  return { stackLines, wireStack };
}

interface MoveToFailedHandlerOptions {
  embedded: boolean;
  tcp: TcpConnection | null;
  internalJob: InternalJob;
  token: string | null | undefined;
  removeOnFail: boolean | undefined;
  onCalled: (error: Error) => void;
  onIgnored: () => void;
}

export function createMoveToFailedHandler(
  options: MoveToFailedHandlerOptions
): (id: string, error: Error, _lockToken?: string) => Promise<void> {
  const { embedded, tcp, internalJob, token, removeOnFail, onCalled, onIgnored } = options;
  return async (_id: string, error: Error, _lockToken?: string) => {
    const { wireStack } = computeStackLines(error);
    const unrecoverable = error instanceof UnrecoverableError;
    let applied = true;
    if (embedded) {
      const manager = getSharedManager();
      const outcome = await manager.fail(
        internalJob.id,
        error.message,
        token ?? undefined,
        unrecoverable,
        wireStack,
        removeOnFail
      );
      applied = outcome?.applied !== false;
    } else if (tcp) {
      const response = await tcp.send({
        cmd: 'FAIL',
        id: internalJob.id,
        error: error.message,
        ...(wireStack ? { stack: wireStack } : {}),
        ...(token ? { token } : {}),
        ...(unrecoverable ? { unrecoverable: true } : {}),
        ...(removeOnFail ? { removeOnFail: true } : {}),
      });
      if (response.ok !== true) {
        throw new Error(typeof response.error === 'string' ? response.error : 'FAIL failed');
      }
      applied = outcomeWasApplied(response.data);
    }
    if (!applied) {
      onIgnored();
      return;
    }
    onCalled(error);
  };
}

interface MoveToCompletedHandlerOptions {
  embedded: boolean;
  ackBatcher: AckBatcher;
  internalJob: InternalJob;
  token: string | null | undefined;
  removeOnComplete: boolean | undefined;
  onCalled: (value: unknown) => void;
  onIgnored: () => void;
}

export function createMoveToCompletedHandler(
  options: MoveToCompletedHandlerOptions
): (id: string, returnValue: unknown, _lockToken?: string) => Promise<unknown> {
  const { embedded, ackBatcher, internalJob, token, removeOnComplete, onCalled, onIgnored } =
    options;
  return async (_id: string, returnValue: unknown, _lockToken?: string) => {
    let applied = true;
    if (embedded) {
      const manager = getSharedManager();
      const outcome = await manager.ack(internalJob.id, returnValue, token ?? undefined, {
        removeOnComplete,
      });
      applied = outcome?.applied !== false;
    } else {
      applied = await ackBatcher.queue(
        String(internalJob.id),
        returnValue,
        token ?? undefined,
        removeOnComplete
      );
    }
    if (!applied) {
      onIgnored();
      return null;
    }
    onCalled(returnValue);
    return null;
  };
}
