/**
 * TCP connection to a bunqueue server — cross-runtime (Node.js, Bun, Deno).
 *
 * Requests carry a `reqId` string; the server echoes it back, enabling
 * pipelining (many in-flight commands per socket). Uses only `node:`
 * builtins (net/tls), which Node, Bun and Deno all support.
 */

import type { Socket } from 'node:net';
import { pack, unpack } from 'msgpackr';
import type {
  Command,
  ConnectionOptions,
  Pending,
  Response,
  TlsOption,
} from './connection-types.js';
import { AuthError, CommandError, CommandTimeoutError, ConnectionClosedError } from './errors.js';
import { compact, FrameParser, frame, PROTOCOL_VERSION } from './frame.js';
import { openSocket } from './socket-factory.js';

export type { Command, ConnectionOptions, Response, TlsOption } from './connection-types.js';

/** A single pipelined TCP connection to a bunqueue server. */
export class Connection {
  readonly host: string;
  readonly port: number;
  readonly token: string | undefined;
  readonly tls: TlsOption;
  readonly connectTimeoutMs: number;
  readonly commandTimeoutMs: number;

  private socket: Socket | null = null;
  private connected = false;
  private closed = false;
  private connecting: Promise<void> | null = null;
  private pending = new Map<string, Pending>();
  private reqCounter = 0;
  private parser = new FrameParser();
  private connectGeneration = 0;
  private failedAttempts = 0;
  private nextAttemptAt = 0;
  // Half-open recovery (#94): after this many consecutive command timeouts the
  // socket is presumed dead and torn down so the next call reconnects.
  private readonly maxCommandTimeouts = 3;
  private consecutiveTimeouts = 0;

  constructor(options: ConnectionOptions = {}) {
    this.host = options.host ?? 'localhost';
    this.port = options.port ?? 6789;
    this.token = options.token;
    this.tls = options.tls;
    this.connectTimeoutMs = options.connectTimeoutMs ?? 5000;
    this.commandTimeoutMs = options.commandTimeoutMs ?? 10_000;
  }

  get isConnected(): boolean {
    return this.connected;
  }

  /**
   * Monotonic counter, incremented on every successful (re)connect. Consumers
   * that hold per-connection server state (e.g. a Worker registration) compare
   * it between operations and re-establish that state after a reconnect.
   */
  get generation(): number {
    return this.connectGeneration;
  }

  /** Open the socket (and authenticate) if not already connected. */
  async connect(): Promise<void> {
    if (this.connected) return;
    if (this.closed) throw new ConnectionClosedError('connection closed by client');
    // Fast-fail while inside the reconnect backoff window: without this, a
    // producer calling add() against a downed server pays the full connect
    // timeout on EVERY call (reconnect storm).
    if (Date.now() < this.nextAttemptAt) {
      throw new ConnectionClosedError(
        `server unreachable, retry in ${this.nextAttemptAt - Date.now()}ms`
      );
    }
    if (this.connecting) return this.connecting;
    this.connecting = this.doConnect()
      .then(() => {
        this.failedAttempts = 0;
        this.nextAttemptAt = 0;
      })
      .catch((err) => {
        this.failedAttempts += 1;
        const backoff = Math.min(500 * 2 ** (this.failedAttempts - 1), 5000);
        this.nextAttemptAt = Date.now() + backoff;
        throw err;
      })
      .finally(() => {
        this.connecting = null;
      });
    return this.connecting;
  }

  private async doConnect(): Promise<void> {
    const socket = await openSocket(this.host, this.port, this.tls, this.connectTimeoutMs);
    socket.setNoDelay(true);
    // TCP keepalive (~15s idle) surfaces a half-open link (cloud LB/NAT idle
    // drop with no FIN/RST) in seconds instead of ~tcp_retries2 minutes.
    socket.setKeepAlive(true, 15_000);
    this.parser.clear();
    this.socket = socket;

    socket.on('data', (chunk: Buffer) => this.handleData(chunk));
    socket.on('error', () => this.teardown());
    socket.on('close', () => this.teardown());

    this.connected = true;
    this.connectGeneration += 1;
    this.consecutiveTimeouts = 0;

    // INVARIANT (H3): connected is flipped true before Auth, which is safe
    // ONLY because call() writes the Auth frame synchronously — there is no
    // `await` between this line and the Auth socket.write, so no concurrent
    // call() can interleave a frame ahead of Auth on the wire. Do NOT insert
    // an await here or before the Auth call, or a command could race ahead of
    // Auth (the Python SDK guards this with a lock; JS relies on this ordering).
    if (this.token) {
      try {
        await this.call({ cmd: 'Auth', token: this.token });
      } catch (err) {
        this.teardown();
        if (err instanceof CommandError) throw new AuthError(err.message);
        throw err;
      }
    }
  }

  /**
   * Send a command and await its response. Rejects with CommandError when
   * the server answers ok=false. Reconnects lazily if the link was lost.
   */
  async call(command: Command, timeoutMs?: number): Promise<Response> {
    if (!this.connected) await this.connect();
    const socket = this.socket;
    if (!socket) throw new ConnectionClosedError('not connected');

    this.reqCounter = (this.reqCounter + 1) & 0x7fffffff;
    const reqId = String(this.reqCounter);
    const payload = pack({ ...compact(command), reqId });
    const gen = this.connectGeneration; // snapshot: a timeout must not tear down a newer conn

    return new Promise<Response>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(reqId);
        this.noteTimeout(gen);
        reject(new CommandTimeoutError(`no response for ${command.cmd} within timeout`));
      }, timeoutMs ?? this.commandTimeoutMs);

      this.pending.set(reqId, {
        resolve: (response) => {
          clearTimeout(timer);
          this.consecutiveTimeouts = 0; // any reply means the link is alive
          if (!response.ok) {
            reject(new CommandError(String(response.error ?? 'unknown server error')));
          } else {
            resolve(response);
          }
        },
        reject: (err) => {
          clearTimeout(timer);
          reject(err);
        },
        timer,
      });

      socket.write(frame(payload), (err) => {
        if (err) {
          const entry = this.pending.get(reqId);
          this.pending.delete(reqId);
          entry?.reject(new ConnectionClosedError(`send failed: ${err.message}`));
          this.teardown();
        }
      });
    });
  }

  /** Ping the server; returns true when it answers pong. */
  async ping(): Promise<boolean> {
    try {
      const response = await this.call({ cmd: 'Ping' });
      const data = response.data as Record<string, unknown> | undefined;
      return data?.pong === true;
    } catch {
      return false;
    }
  }

  /** Protocol negotiation; returns server name/version/protocolVersion. */
  hello(): Promise<Response> {
    return this.call({
      cmd: 'Hello',
      protocolVersion: PROTOCOL_VERSION,
      capabilities: ['pipelining'],
    });
  }

  /** Close permanently; in-flight commands reject. */
  close(): void {
    this.closed = true;
    this.teardown();
  }

  private handleData(chunk: Buffer): void {
    let frames: Buffer[];
    try {
      frames = this.parser.addData(chunk);
    } catch {
      this.teardown();
      return;
    }
    for (const framePayload of frames) {
      let message: unknown;
      try {
        message = unpack(framePayload);
      } catch {
        continue; // skip unparseable frame; a desynced stream dies via socket error
      }
      if (typeof message !== 'object' || message === null) continue;
      const response = message as Response;
      const reqId = response.reqId;
      if (reqId === undefined || reqId === null) continue; // server-push unsupported
      const entry = this.pending.get(String(reqId));
      if (entry) {
        this.pending.delete(String(reqId));
        entry.resolve(response);
      }
    }
  }

  /**
   * A dead/half-open link makes every command time out while the socket still
   * looks connected. After maxCommandTimeouts consecutive timeouts, tear down
   * so the next call() reconnects instead of wedging (mirrors #94).
   */
  private noteTimeout(gen: number): void {
    // A timeout from an already-replaced connection must not tear down (or
    // miscount against) the current one.
    if (gen !== this.connectGeneration) return;
    this.consecutiveTimeouts += 1;
    if (this.consecutiveTimeouts >= this.maxCommandTimeouts) this.teardown();
  }

  private teardown(): void {
    this.connected = false;
    const socket = this.socket;
    this.socket = null;
    if (socket) {
      socket.removeAllListeners();
      socket.destroy();
    }
    const pending = this.pending;
    this.pending = new Map();
    for (const entry of pending.values()) {
      clearTimeout(entry.timer);
      entry.reject(new ConnectionClosedError('connection lost'));
    }
  }
}
