import { EventEmitter } from 'events';
import { CommandQueue } from '../connection';
import { HealthTracker } from '../health';
import { ReconnectManager } from '../reconnect';
import type { ConnectionOptions, SocketWrapper } from '../types';
import { DEFAULT_CONNECTION } from '../types';
import type { JobEvent } from '../../../domain/types/queue';

/** Shared TCP client state and event contract. */
export class TcpClientState extends EventEmitter {
  on(
    event: 'connected' | 'disconnected' | 'maxReconnectAttemptsReached',
    listener: () => void
  ): this;
  on(event: 'reconnecting', listener: (data: { attempt: number; delay: number }) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  on(event: 'warning', listener: (data: { type: string; reqId?: string }) => void): this;
  on(event: 'queueEvent', listener: (event: JobEvent) => void): this;
  on(
    event: 'health',
    listener: (data: { type: string; latency?: number; reason?: string }) => void
  ): this;
  // oxlint-disable-next-line typescript/no-explicit-any -- EventEmitter's fallback overload requires any[]
  on(event: string, listener: (...args: any[]) => void): this {
    return super.on(event, listener);
  }

  once(
    event: 'connected' | 'disconnected' | 'maxReconnectAttemptsReached',
    listener: () => void
  ): this;
  once(event: 'reconnecting', listener: (data: { attempt: number; delay: number }) => void): this;
  once(event: 'error', listener: (error: Error) => void): this;
  once(event: 'warning', listener: (data: { type: string; reqId?: string }) => void): this;
  once(event: 'queueEvent', listener: (event: JobEvent) => void): this;
  once(
    event: 'health',
    listener: (data: { type: string; latency?: number; reason?: string }) => void
  ): this;
  // oxlint-disable-next-line typescript/no-explicit-any -- EventEmitter's fallback overload requires any[]
  once(event: string, listener: (...args: any[]) => void): this {
    return super.once(event, listener);
  }

  protected socket: SocketWrapper | null = null;
  protected connected = false;
  protected connecting = false;
  protected readonly options: Required<ConnectionOptions>;
  protected readonly health: HealthTracker;
  protected readonly reconnect: ReconnectManager;
  protected readonly commands: CommandQueue;
  protected reqIdCounter = 0;

  constructor(options: Partial<ConnectionOptions> = {}) {
    super();
    this.options = { ...DEFAULT_CONNECTION, ...options };
    this.commands = new CommandQueue();
    this.health = new HealthTracker({
      pingInterval: this.options.pingInterval,
      maxPingFailures: this.options.maxPingFailures,
      maxCommandTimeouts: this.options.maxCommandTimeouts,
    });
    this.reconnect = new ReconnectManager({
      maxReconnectAttempts: this.options.maxReconnectAttempts,
      reconnectDelay: this.options.reconnectDelay,
      maxReconnectDelay: this.options.maxReconnectDelay,
      autoReconnect: this.options.autoReconnect,
    });

    this.reconnect.on('reconnecting', (data) => this.emit('reconnecting', data));
    this.reconnect.on('maxReconnectAttemptsReached', () => {
      this.emit('maxReconnectAttemptsReached');
      this.commands.rejectAll(new Error('Max reconnection attempts reached'));
    });
  }
}
