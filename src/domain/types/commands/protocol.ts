import type { BaseCommand } from './base';
import type { ProtocolCapability } from '../protocol';

export interface AuthCommand extends BaseCommand {
  readonly cmd: 'Auth';
  readonly token: string;
}
export interface HelloCommand extends BaseCommand {
  readonly cmd: 'Hello';
  readonly protocolVersion: number;
  readonly capabilities?: ProtocolCapability[];
}

/** Subscribe this TCP connection to one queue's lifecycle event stream. */
export interface SubscribeEventsCommand extends BaseCommand {
  readonly cmd: 'SubscribeEvents';
  readonly queue: string;
}

/** Remove the lifecycle event subscription owned by this TCP connection. */
export interface UnsubscribeEventsCommand extends BaseCommand {
  readonly cmd: 'UnsubscribeEvents';
}
