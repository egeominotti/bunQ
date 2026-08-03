import type { Socket } from 'bun';
import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import * as resp from '../../../domain/types/response';
import { validateQueueName } from '../protocol';
import type { TcpConnectionData } from '../types/tcpServer';
import type { TcpConnectionRegistry } from './connections';

type EventCommand = Extract<Command, { cmd: 'SubscribeEvents' | 'UnsubscribeEvents' }>;

export function handleEventSubscription(
  command: EventCommand,
  socket: Socket<TcpConnectionData>,
  registry: TcpConnectionRegistry
): Response {
  const { ctx } = socket.data;
  if (ctx.authTokens.size > 0 && !ctx.authenticated) {
    return resp.error('Not authenticated', command.reqId);
  }

  if (command.cmd === 'UnsubscribeEvents') {
    registry.unsubscribeEvents(socket);
    return resp.ok(undefined, command.reqId);
  }

  const validationError = validateQueueName(command.queue);
  if (validationError) return resp.error(validationError, command.reqId);
  registry.subscribeEvents(socket, command.queue);
  return resp.ok(undefined, command.reqId);
}
