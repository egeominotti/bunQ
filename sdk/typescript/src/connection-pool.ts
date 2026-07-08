/**
 * A round-robin pool of TCP connections for producer-side throughput.
 *
 * Each `call()` is dispatched to the next connection, so independent commands
 * (PUSH, queries, control) fan out across N sockets instead of head-of-lining
 * on one. Use it for {@link Queue} / {@link FlowProducer}; NOT for a Worker,
 * whose pull→ack flow is bound to a single connection's lock ownership and
 * reconnect/registration lifecycle.
 *
 * Satisfies {@link ConnectionLike}, so it is a drop-in for a single Connection.
 * Lifecycle events from every member connection are re-emitted here.
 */

import { EventEmitter } from 'node:events';
import { Connection } from './connection.js';
import type { Command, ConnectionLike, ConnectionOptions, Response } from './connection-types.js';
import { LIFECYCLE_EVENTS } from './observability.js';

export class ConnectionPool extends EventEmitter implements ConnectionLike {
  private readonly connections: Connection[];
  private cursor = 0;

  constructor(size: number, options: ConnectionOptions = {}) {
    super();
    const n = Math.max(1, Math.floor(size));
    this.connections = Array.from({ length: n }, () => {
      const conn = new Connection(options);
      for (const event of LIFECYCLE_EVENTS) {
        conn.on(event, (payload) => this.emit(event, payload));
      }
      return conn;
    });
  }

  /** How many connections back this pool. */
  get size(): number {
    return this.connections.length;
  }

  private next(): Connection {
    const conn = this.connections[this.cursor];
    this.cursor = (this.cursor + 1) % this.connections.length;
    return conn;
  }

  call<R = Response>(command: Command, timeoutMs?: number): Promise<R> {
    return this.next().call<R>(command, timeoutMs);
  }

  ping(): Promise<boolean> {
    return this.connections[0].ping();
  }

  async connect(): Promise<void> {
    await Promise.all(this.connections.map((conn) => conn.connect()));
  }

  close(): void {
    for (const conn of this.connections) conn.close();
  }

  /** True while at least one member connection is up. */
  get isConnected(): boolean {
    return this.connections.some((conn) => conn.isConnected);
  }

  /** Representative generation (first connection); pools are producer-side. */
  get generation(): number {
    return this.connections[0].generation;
  }
}
