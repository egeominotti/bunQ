import type { Command } from '../../../domain/types/command';
import { error, type Response } from '../../../domain/types/response';
import type { ConnectionState } from '../types/protocol';

export function parseCommand(data: string): Command | null {
  try {
    const parsed = JSON.parse(data) as Record<string, unknown>;
    if (!parsed['cmd']) return null;
    return parsed as unknown as Command;
  } catch {
    return null;
  }
}

export function serializeResponse(response: Response): string {
  return JSON.stringify(response);
}

export function parseCommands(data: string): Command[] {
  const lines = data.split('\n').filter((line) => line.trim().length > 0);
  const commands: Command[] = [];
  for (const line of lines) {
    const command = parseCommand(line);
    if (command) commands.push(command);
  }
  return commands;
}

export function createConnectionState(clientId: string): ConnectionState {
  return { authenticated: false, clientId };
}

export function errorResponse(message: string, requestId?: string): string {
  return serializeResponse(error(message, requestId));
}
