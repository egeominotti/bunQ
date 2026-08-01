import type { FrameParser } from '../protocol';
import type { ConnectionState } from './protocol';
import type { HandlerContext } from '../types';
import type { Semaphore } from '../../../shared/semaphore';
import type { SocketWriteQueue } from '../socketWriteQueue';
import type { TlsServerOptions } from '../tls';

export interface TcpServerConfig {
  port?: number;
  hostname?: string;
  authTokens?: string[];
  idleTimeoutMs?: number;
  maxWriteQueueBytes?: number;
  tls?: TlsServerOptions;
}

export interface TcpConnectionData {
  state: ConnectionState;
  abortController: AbortController;
  frameParser: FrameParser;
  ctx: HandlerContext;
  semaphore: Semaphore;
  writeQueue: SocketWriteQueue;
  stallTimer: ReturnType<typeof setTimeout> | null;
}
