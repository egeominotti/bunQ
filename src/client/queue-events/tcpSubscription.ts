import type { JobEvent } from '../../domain/types/queue';
import type { ConnectionOptions } from '../types';
import { resolveToken } from '../resolveToken';
import { TcpClient } from '../tcpClient';

interface TcpEventSubscriptionOptions {
  connection?: ConnectionOptions;
  onError: (error: Error) => void;
  onEvent: (event: JobEvent) => void;
  queue: string;
}

/** One dedicated TCP connection with one broker-side queue subscription. */
export class TcpEventSubscription {
  private readonly client: TcpClient;
  private readonly queue: string;
  private readonly onError: (error: Error) => void;
  private closed = false;
  private ready = false;
  private generation = 0;
  private subscriptionPromise: Promise<void> | null = null;

  constructor(options: TcpEventSubscriptionOptions) {
    this.queue = options.queue;
    this.onError = options.onError;
    const connection = options.connection ?? {};
    this.client = new TcpClient({
      host: connection.host ?? 'localhost',
      port: connection.port ?? 6789,
      token: resolveToken(connection.token) ?? '',
      tls: connection.tls ?? false,
      pingInterval: connection.pingInterval ?? 30_000,
      commandTimeout: connection.commandTimeout ?? 30_000,
      maxCommandTimeouts: connection.maxCommandTimeouts ?? 3,
      pipelining: connection.pipelining ?? true,
      maxInFlight: connection.maxInFlight ?? 100,
    });

    this.client.on('queueEvent', (event) => {
      if (!this.closed && event.queue === this.queue) options.onEvent(event);
    });
    this.client.on('error', (error) => {
      if (!this.closed) this.onError(error);
    });
    this.client.on('disconnected', () => {
      if (this.closed) return;
      this.generation++;
      this.ready = false;
      this.subscriptionPromise = null;
    });
    this.client.on('connected', () => {
      if (!this.closed) void this.ensureSubscribed().catch(() => undefined);
    });

    void this.ensureSubscribed().catch(() => undefined);
  }

  waitUntilReady(): Promise<void> {
    if (this.closed) return Promise.reject(new Error('Event subscription is closed'));
    if (this.ready && this.client.isConnected()) {
      return this.client.send({ cmd: 'Ping' }).then((reply) => {
        if (reply.ok !== true) throw new Error('Queue event connection is not ready');
      });
    }
    return this.ensureSubscribed();
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.generation++;
    this.ready = false;
    this.subscriptionPromise = null;
    this.client.close();
  }

  private ensureSubscribed(): Promise<void> {
    if (this.closed) return Promise.reject(new Error('Event subscription is closed'));
    if (this.ready) return Promise.resolve();
    if (this.subscriptionPromise) return this.subscriptionPromise;

    const generation = this.generation;
    const operation = (this.client.isConnected() ? Promise.resolve() : this.client.connect())
      .then(() => this.client.send({ cmd: 'SubscribeEvents', queue: this.queue }))
      .then((reply) => {
        if (reply.ok !== true) {
          throw new Error(
            typeof reply.error === 'string' ? reply.error : 'Queue event subscription failed'
          );
        }
        if (this.closed || generation !== this.generation) {
          throw new Error('Event subscription changed while connecting');
        }
        this.ready = true;
      });
    this.subscriptionPromise = operation;
    void operation.then(
      () => {
        if (this.subscriptionPromise === operation) this.subscriptionPromise = null;
      },
      (error: unknown) => {
        if (this.subscriptionPromise === operation) this.subscriptionPromise = null;
        if (!this.closed) this.onError(error instanceof Error ? error : new Error(String(error)));
      }
    );
    return operation;
  }
}
