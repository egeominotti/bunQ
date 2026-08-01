import type { Job as InternalJob } from '../../../domain/types/job';
import { UnrecoverableError } from '../../errors';
import { getSharedManager } from '../../manager';
import type { AckBatcher } from '../ackBatcher';
import type { TcpConnection } from '../types';

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

export function createMoveToFailedHandler(
  embedded: boolean,
  tcp: TcpConnection | null,
  internalJob: InternalJob,
  token: string | null | undefined,
  onCalled: (error: Error) => void
): (id: string, error: Error, _lockToken?: string) => Promise<void> {
  return async (_id: string, error: Error, _lockToken?: string) => {
    const { wireStack } = computeStackLines(error);
    const unrecoverable = error instanceof UnrecoverableError;
    if (embedded) {
      const manager = getSharedManager();
      await manager.fail(
        internalJob.id,
        error.message,
        token ?? undefined,
        unrecoverable,
        wireStack
      );
    } else if (tcp) {
      await tcp.send({
        cmd: 'FAIL',
        id: internalJob.id,
        error: error.message,
        ...(wireStack ? { stack: wireStack } : {}),
        ...(token ? { token } : {}),
        ...(unrecoverable ? { unrecoverable: true } : {}),
      });
    }
    onCalled(error);
  };
}

export function createMoveToCompletedHandler(
  embedded: boolean,
  ackBatcher: AckBatcher,
  internalJob: InternalJob,
  token: string | null | undefined,
  onCalled: (value: unknown) => void
): (id: string, returnValue: unknown, _lockToken?: string) => Promise<unknown> {
  return async (_id: string, returnValue: unknown, _lockToken?: string) => {
    if (embedded) {
      const manager = getSharedManager();
      await manager.ack(internalJob.id, returnValue, token ?? undefined);
    } else {
      await ackBatcher.queue(String(internalJob.id), returnValue, token ?? undefined);
    }
    onCalled(returnValue);
    return null;
  };
}
