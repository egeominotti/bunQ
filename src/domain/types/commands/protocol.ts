import type { BaseCommand } from './base';

export interface AuthCommand extends BaseCommand {
  readonly cmd: 'Auth';
  readonly token: string;
}
export interface HelloCommand extends BaseCommand {
  readonly cmd: 'Hello';
  readonly protocolVersion: number;
  readonly capabilities?: 'pipelining'[];
}
