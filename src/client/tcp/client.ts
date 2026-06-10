/**
 * TCP Client
 * Production-ready TCP client with auto-reconnection (msgpack binary protocol)
 * Supports pipelining via reqId-based response matching for high throughput
 */

import { EventEmitter } from 'events';
import type { ConnectionOptions, ConnectionHealth, SocketWrapper, PendingCommand } from './types';
import { DEFAULT_CONNECTION } from './types';
import { HealthTracker } from './health';
import { ReconnectManager } from './reconnect';
import { createConnection, CommandQueue } from './connection';
import { pack, unpack } from 'msgpackr';
import { FrameParser } from '../../infrastructure/server/protocol';

/**
 * Sentinel error class for the synthetic rejection issued by close()/rejectAll().
 * Using a dedicated subclass (instead of matching `error.message`) ensures the
 * process-level filter never collides with user code that happens to throw an
 * Error whose message is "Client closed" (Redis, HTTP, DB pool clients all do).
 */
export class ClientClosedError extends Error {
  constructor(message = 'Client closed') {
    super(message);
    this.name = 'ClientClosedError';
  }
}

/**
 * One-time installer of a process-level filter that swallows the synthetic
 * ClientClosedError issued by close()/rejectAll(). Catches the rare chained-
 * Promise leak that `cmd.promise?.catch(() => {})` in rejectAll cannot reach
 * (e.g. await-then-throw patterns where the rejection propagates through the
 * awaiter's own Promise chain).
 *
 * Implementation note: the listener is intentionally a no-op for non-matching
 * rejections. process.on('unhandledRejection', fn) calls all registered
 * listeners independently — so existing handlers (e.g. main.ts shutdown
 * handler) still fire for real rejections. We must NOT re-throw here because
 * a throw inside an unhandledRejection listener becomes an uncaughtException,
 * which would interact badly with main.ts's existing uncaughtException
 * handler and cause double-shutdown attempts.
 */
let clientClosedFilterInstalled = false;
function installClientClosedFilter(): void {
  if (clientClosedFilterInstalled) return;
  clientClosedFilterInstalled = true;
  process.on('unhandledRejection', (reason: unknown) => {
    if (reason instanceof ClientClosedError) {
      return; // swallow — other listeners (if any) fire independently
    }
    // Non-matching rejection: no-op here. Other registered listeners (or the
    // runtime default per --unhandled-rejections) handle it independently.
  });
}

/**
 * TCP Client - manages connection to bunqueue server
 * Supports pipelining for high-throughput operations
 */
export class TcpClient extends EventEmitter {
  // ============ Typed Event Overloads ============

  on(
    event: 'connected' | 'disconnected' | 'maxReconnectAttemptsReached',
    listener: () => void
  ): this;
  on(event: 'reconnecting', listener: (data: { attempt: number; delay: number }) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  on(event: 'warning', listener: (data: { type: string; reqId?: string }) => void): this;
  on(
    event: 'health',
    listener: (data: { type: string; latency?: number; reason?: string }) => void
  ): this;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
  once(
    event: 'health',
    listener: (data: { type: string; latency?: number; reason?: string }) => void
  ): this;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  once(event: string, listener: (...args: any[]) => void): this {
    return super.once(event, listener);
  }

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
        tls: this.options.tls,
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

    if (this.options.token) {
      try {
        await this.authenticate();
      } catch (err) {
        this.socket.end();
        this.socket = null;
        throw err;
      }
    }

    this.connected = true;
    this.connecting = false;
    this.health.recordConnected();
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
      // Malformed/unparseable frame: the framed byte stream is now
      // unrecoverable (we cannot trust subsequent frame boundaries), and in
      // pipelining mode the legacy `currentCommand` slot is null — so checking
      // it alone would leave every in-flight command hanging until its
      // per-command timeout. Reject ALL pending + in-flight commands promptly
      // and tear down the socket so reconnection (if enabled) can re-establish
      // a clean stream.
      const err = new Error('Invalid response from server');
      this.emit('warning', { type: 'malformed_frame' });
      this.commands.rejectAll(err);
      this.forceReconnect();
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

  /**
   * A command timed out. On a half-open socket (peer gone, no FIN/RST) writes
   * keep succeeding but no response ever returns — every command times out
   * while the socket still looks "connected". The health-check ping is one way
   * to notice, but it can be disabled or slower than real traffic, leaving a
   * worker's PULL loop to time out forever without ever reconnecting (#94).
   * Treat a sustained run of timeouts as a dead link and force a reconnect.
   */
  private handleCommandTimeout(): void {
    if (this.health.recordCommandTimeout()) {
      this.emit('health', { type: 'unhealthy', reason: 'max_command_timeouts' });
      this.forceReconnect();
    }
  }

  private forceReconnect(): void {
    if (this.reconnect.isClosed()) return;
    if (this.socket) {
      // end() can throw on an already-errored/half-dead socket. Swallow it:
      // failing to close the corpse must NOT abort the reconnect path below,
      // or the connection would stay wedged forever (the #94 failure mode).
      try {
        this.socket.end();
      } catch {
        /* socket already torn down */
      }
      this.socket = null;
    }
    this.connected = false;
    this.health.stopPing();
    // Settle every in-flight/queued command NOW. Otherwise their per-command
    // timeouts keep ticking and fire AFTER the fresh socket is up — each stale
    // timeout bumps the new connection's dead-link counter and can re-trigger
    // forceReconnect in a loop (a reconnect storm that never stabilises). It
    // also unblocks awaiting callers (e.g. a Worker's PULL) immediately instead
    // of making them wait out the full commandTimeout on a corpse.
    this.commands.rejectAll(new Error('Connection lost'));
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

    // Definite-assignment assertion: assigned synchronously by Promise executor below.
    let pendingRef!: PendingCommand;
    const promise = new Promise<Record<string, unknown>>((resolve, reject) => {
      const timeout = setTimeout(() => {
        // Try to remove from in-flight
        const removed = this.commands.removeByReqId(reqId);
        if (removed) {
          this.health.recordError();
          reject(new Error('Command timeout'));
        }
      }, this.options.commandTimeout);

      pendingRef = {
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
    });

    // Promise constructor executor is synchronous, so pendingRef is assigned.
    pendingRef.promise = promise;
    this.commands.addInFlight(pendingRef);

    // Send immediately (this.socket null-checked at entry of sendDirect)
    this.socket.write(FrameParser.frame(pack(commandWithReqId)));

    return promise;
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
          // In-flight command got no response: count it toward dead-link detection.
          this.handleCommandTimeout();
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

    const id = this.commands.nextId();
    // Definite-assignment assertion: assigned synchronously by Promise executor below.
    let pendingRef!: PendingCommand;
    const promise = new Promise<Record<string, unknown>>((resolve, reject) => {
      const timeout = setTimeout(() => {
        // Try to remove from queue first. A still-queued command never reached
        // the socket (e.g. waiting on connect), so it is NOT evidence of a dead
        // link — reject it but don't count it toward dead-link detection.
        if (this.commands.remove(id)) {
          this.health.recordError();
          reject(new Error('Command timeout'));
          return;
        }
        // Try to remove from in-flight: this one WAS sent and got no response.
        const removed = this.commands.removeByReqId(reqId);
        if (removed) {
          this.health.recordError();
          reject(new Error('Command timeout'));
          this.handleCommandTimeout();
        }
      }, this.options.commandTimeout);

      pendingRef = {
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
    });

    // Promise constructor executor is synchronous, so pendingRef is assigned.
    pendingRef.promise = promise;
    this.commands.enqueue(pendingRef);

    // Connect if needed, then process queue
    if (!this.connected && !this.connecting) {
      // Connection errors during send are handled by command timeout/rejection
      this.connect().catch(() => {});
    } else if (this.connected) {
      this.processQueue();
    }

    return promise;
  }

  /** Close connection */
  close(): void {
    this.reconnect.setClosed(true);
    this.health.stopPing();
    this.reconnect.cancelReconnect();
    // Install a process-level filter for the synthetic ClientClosedError we
    // are about to issue. Catches the rare case where a caller's .catch() is
    // attached on a chained Promise that our cmd.promise?.catch() cannot
    // reach (await-then-throw patterns).
    installClientClosedFilter();
    this.commands.rejectAll(new ClientClosedError());
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
