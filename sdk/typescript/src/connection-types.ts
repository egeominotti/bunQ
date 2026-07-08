/** Connection option and message types. */

import type { Observability } from './observability.js';

export type TlsOption = boolean | { caFile?: string; rejectUnauthorized?: boolean } | undefined;

export interface ConnectionOptions extends Observability {
  host?: string;
  port?: number;
  token?: string;
  tls?: TlsOption;
  connectTimeoutMs?: number;
  commandTimeoutMs?: number;
  /**
   * Max in-flight commands before {@link Connection.call} applies backpressure
   * (awaits a free slot). 0 / undefined = unbounded. Bounds memory under load.
   */
  maxInFlight?: number;
}

export type Command = Record<string, unknown> & { cmd: string };
export type Response = Record<string, unknown> & { ok: boolean };

/**
 * The connection surface consumers (Queue, FlowProducer, Job) depend on.
 * Both {@link Connection} and a round-robin ConnectionPool satisfy it, so a
 * pool can be dropped in transparently for producer-side throughput.
 */
export interface ConnectionLike {
  call<R = Response>(command: Command, timeoutMs?: number): Promise<R>;
  ping(): Promise<boolean>;
  connect(): Promise<void>;
  close(): void;
  readonly isConnected: boolean;
  readonly generation: number;
  on(event: string, listener: (...args: unknown[]) => void): this;
}

export interface Pending {
  resolve: (response: Response) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}
