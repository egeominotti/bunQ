/**
 * TCP Client
 * Production-ready TCP client with auto-reconnection (msgpack binary protocol)
 * Supports pipelining via reqId-based response matching for high throughput
 */

import { EventEmitter } from 'events';
import type { ConnectionOptions, ConnectionHealth, SocketWrapper } from './types';
import { DEFAULT_CONNECTION } from './types';
import { HealthTracker } from './health';
import { ReconnectManager } from './reconnect';
import { createConnection, CommandQueue } from './connection';
import { pack, unpack } from 'msgpackr';
import { FrameParser } from '../../infrastructure/server/protocol';

/**
 * TCP Client - manages connection to bunqueue server
 * Supports pipelining for high-throughput operations
 */
export class TcpClient extends EventEmitter {
  private socket: SocketWrapper | null = null;
  private connected = false;
  private connecting = false;
  private readonly options: Required<ConnectionOptions>;
  private readonly health: HealthTracker;
  private readonly reconnect: ReconnectManager;
  private readonly commands: CommandQueue;
  /** Request ID counter for pipelining support */
  private reqIdCounter = 0;

  constructor(options: Partial<ConnectionOptions> = {}) {
    super();
    this.options = { ...DEFAULT_CONNECTION, ...options };
    this.commands = new CommandQueue();
    this.health = new HealthTracker({
      pingInterval: this.options.pingInterval,
      maxPingFailures: this.options.maxPingFailures,
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

  /** Connect to server */
  async connect(): Promise<void> {
    if (this.connected) return;
    if (this.connecting) {
      return this.waitForConnection();
    }

    this.connecting = true;
    this.reconnect.setClosed(false);

    try {
      await this.doConnect();
      this.reconnect.reset();
      this.emit('connected');
      this.health.startPing(async () => {
        await this.ping();
      });
      this.processQueue();
    } catch (err) {
      this.connecting = false;
      if (this.reconnect.canReconnect()) {
        this.reconnect.scheduleReconnect(() => this.connect());
      }
      throw err;
    }
  }

  private waitForConnection(): Promise<void> {
    return new Promise((resolve, reject) => {
      const onConnect = () => {
        this.off('error', onError);
        resolve();
      };
      const onError = (err: Error) => {
        this.off('connected', onConnect);
        reject(err);
      };
      this.once('connected', onConnect);
      this.once('error', onError);
    });
  }

  private async doConnect(): Promise<void> {
    const { socket } = await createConnection(
      {
        host: this.options.host,
        port: this.options.port,
      },
      this.options.connectTimeout,
      {
        onData: (frame) => {
          this.handleData(frame);
        },
        onClose: () => {
          this.handleClose();
        },
        onError: (error) => this.emit('error', error),
      }
    );

    this.socket = socket;
    this.connected = true;
    this.connecting = false;
    this.health.recordConnected();

    if (this.options.token) {
      await this.authenticate();
    }
  }

  private async authenticate(): Promise<void> {
    const response = await this.sendDirect({ cmd: 'Auth', token: this.options.token });
    if (!response.ok) {
      throw new Error('Authentication failed');
    }
  }

  /**
   * Handle incoming data frame
   * Uses reqId-based matching for pipelining support
   */
  private handleData(frame: Uint8Array): void {
    try {
      const response = unpack(frame) as Record<string, unknown>;
      const reqId = response.reqId as string | undefined;

      // Try pipelining mode first (reqId-based matching)
      if (reqId) {
        const pending = this.commands.removeByReqId(reqId);
        if (pending) {
          clearTimeout(pending.timeout);
          pending.resolve(response);
          // Process more queued commands if we have room
          this.processQueue();
          return;
        }
      }

      // Fall back to legacy mode (current command)
      const current = this.commands.getCurrentCommand();
      if (current) {
        clearTimeout(current.timeout);
        current.resolve(response);
        this.commands.setCurrentCommand(null);
        this.processQueue();
        return;
      }

      // Unknown response - log warning
      this.emit('warning', { type: 'unknown_response', reqId });
    } catch {
      // Invalid response - reject current command if any
      const current = this.commands.getCurrentCommand();
      if (current) {
        clearTimeout(current.timeout);
        current.reject(new Error('Invalid response from server'));
        this.commands.setCurrentCommand(null);
        this.processQueue();
      }
    }
  }

  private handleClose(): void {
    const wasConnected = this.connected;
    this.connected = false;
    this.connecting = false;
    this.socket = null;
    this.health.stopPing();

    // Reject all in-flight commands (pipelining + legacy)
    this.commands.rejectAll(new Error('Connection lost'));

    if (wasConnected) {
      this.emit('disconnected');
      if (this.reconnect.canReconnect()) {
        this.reconnect.scheduleReconnect(() => this.connect());
      }
    }
  }

  /** Send ping to check connection health */
  async ping(): Promise<boolean> {
    if (!this.connected) return false;
    try {
      const start = Date.now();
      const response = await this.send({ cmd: 'Ping' });
      const data = response.data as Record<string, unknown> | undefined;
      const success = data?.pong === true;

      if (success) {
        this.health.recordPingSuccess(Date.now() - start);
        this.emit('health', { type: 'ping_success', latency: Date.now() - start });
      } else {
        this.handlePingFailure();
      }
      return success;
    } catch {
      this.handlePingFailure();
      return false;
    }
  }

  private handlePingFailure(): void {
    if (this.health.recordPingFailure()) {
      this.emit('health', { type: 'unhealthy', reason: 'max_ping_failures' });
      this.forceReconnect();
    } else {
      this.emit('health', { type: 'ping_failed' });
    }
  }

  private forceReconnect(): void {
    if (this.reconnect.isClosed()) return;
    if (this.socket) {
      this.socket.end();
      this.socket = null;
    }
    this.connected = false;
    this.health.stopPing();
    if (this.reconnect.canReconnect()) this.reconnect.scheduleReconnect(() => this.connect());
  }

  /** Get connection health metrics */
  getHealth(): ConnectionHealth {
    return this.health.getHealth(this.getState());
  }

  /** Generate unique request ID for pipelining */
  private generateReqId(): string {
    // Use modulo to prevent overflow, wrap at 2^31
    this.reqIdCounter = (this.reqIdCounter + 1) & 0x7fffffff;
    return String(this.reqIdCounter);
  }

  /**
   * Send command directly (bypass queue)
   * Used for authentication and critical commands
   */
  private sendDirect(command: Record<string, unknown>): Promise<Record<string, unknown>> {
    if (!this.socket) return Promise.reject(new Error('Not connected'));

    const startTime = Date.now();
    this.health.recordCommandSent();

    const reqId = this.generateReqId();
    const commandWithReqId = { ...command, reqId };

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        // Try to remove from in-flight
        const removed = this.commands.removeByReqId(reqId);
        if (removed) {
          this.health.recordError();
          reject(new Error('Command timeout'));
        }
      }, this.options.commandTimeout);

      const pending = {
        id: 0,
        reqId,
        command: commandWithReqId,
        resolve: (result: Record<string, unknown>) => {
          this.health.recordSuccess(Date.now() - startTime);
          resolve(result);
        },
        reject: (err: Error) => {
          this.health.recordError();
          reject(err);
        },
        timeout,
      };

      // Add to in-flight tracking
      this.commands.addInFlight(pending);

      // Send immediately
      this.socket?.write(FrameParser.frame(pack(commandWithReqId)));
    });
  }

  /**
   * Process queued commands
   * Sends multiple commands up to maxInFlight (pipelining)
   */
  private processQueue(): void {
    if (!this.connected || !this.socket) return;

    // Send commands while under the maxInFlight limit
    while (this.commands.hasPending() && this.commands.canSendMore(this.options.maxInFlight)) {
      const next = this.commands.dequeue();
      if (!next) break;

      // Clear old timeout and create new one
      clearTimeout(next.timeout);
      const newTimeout = setTimeout(() => {
        const removed = this.commands.removeByReqId(next.reqId);
        if (removed) {
          this.health.recordError();
          next.reject(new Error('Command timeout'));
        }
      }, this.options.commandTimeout);

      next.timeout = newTimeout;

      // Add to in-flight tracking and send
      this.commands.addInFlight(next);
      this.socket.write(FrameParser.frame(pack(next.command)));
    }
  }

  /**
   * Send command and wait for response
   * Uses pipelining for high throughput
   */
  async send(command: Record<string, unknown>): Promise<Record<string, unknown>> {
    const startTime = Date.now();
    this.health.recordCommandSent();

    const reqId = this.generateReqId();
    const commandWithReqId = { ...command, reqId };

    return new Promise((resolve, reject) => {
      const id = this.commands.nextId();

      const timeout = setTimeout(() => {
        // Try to remove from queue first
        if (this.commands.remove(id)) {
          this.health.recordError();
          reject(new Error('Command timeout'));
          return;
        }
        // Try to remove from in-flight
        const removed = this.commands.removeByReqId(reqId);
        if (removed) {
          this.health.recordError();
          reject(new Error('Command timeout'));
        }
      }, this.options.commandTimeout);

      const pending = {
        id,
        reqId,
        command: commandWithReqId,
        resolve: (result: Record<string, unknown>) => {
          this.health.recordSuccess(Date.now() - startTime);
          resolve(result);
        },
        reject: (err: Error) => {
          this.health.recordError();
          reject(err);
        },
        timeout,
      };

      // Add to queue
      this.commands.enqueue(pending);

      // Connect if needed, then process queue
      if (!this.connected && !this.connecting) {
        this.connect().catch(() => {});
      } else if (this.connected) {
        this.processQueue();
      }
    });
  }

  /** Close connection */
  close(): void {
    this.reconnect.setClosed(true);
    this.health.stopPing();
    this.reconnect.cancelReconnect();
    this.commands.rejectAll(new Error('Client closed'));
    if (this.socket) {
      this.socket.end();
      this.socket = null;
      this.connected = false;
    }
  }

  /** Check if connected */
  isConnected(): boolean {
    return this.connected;
  }

  /** Get connection state */
  getState(): 'connected' | 'connecting' | 'disconnected' | 'closed' {
    if (this.reconnect.isClosed()) return 'closed';
    if (this.connected) return 'connected';
    if (this.connecting) return 'connecting';
    return 'disconnected';
  }

  /** Get number of in-flight commands (for monitoring) */
  getInFlightCount(): number {
    return this.commands.getInFlightCount();
  }
}
