/**
 * TCP connection to a bunqueue server — cross-runtime (Node.js, Bun, Deno).
 *
 * Requests carry a `reqId` string; the server echoes it back, enabling
 * pipelining (many in-flight commands per socket). Uses only `node:`
 * builtins (net/tls), which Node, Bun and Deno all support.
 */

import { readFileSync } from 'node:fs';
import { connect as netConnect, type Socket } from 'node:net';
import { type ConnectionOptions as TlsConnectOptions, connect as tlsConnect } from 'node:tls';
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

  /** Open the socket (and authenticate) if not already connected. */
  async connect(): Promise<void> {
    if (this.connected) return;
    if (this.closed) throw new ConnectionClosedError('connection closed by client');
    if (this.connecting) return this.connecting;
    this.connecting = this.doConnect().finally(() => {
      this.connecting = null;
    });
    return this.connecting;
  }

  private async doConnect(): Promise<void> {
    const socket = await this.openSocket();
    socket.setNoDelay(true);
    this.parser.clear();
    this.socket = socket;

    socket.on('data', (chunk: Buffer) => this.handleData(chunk));
    socket.on('error', () => this.teardown());
    socket.on('close', () => this.teardown());

    this.connected = true;

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

  private openSocket(): Promise<Socket> {
    return new Promise((resolve, reject) => {
      let settled = false;
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        socket.destroy();
        reject(new ConnectionClosedError(`connect timeout to ${this.host}:${this.port}`));
      }, this.connectTimeoutMs);

      const onError = (err: Error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        reject(new ConnectionClosedError(`connect failed: ${err.message}`));
      };
      const onReady = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        socket.off('error', onError);
        resolve(socket);
      };

      let socket: Socket;
      if (this.tls) {
        const tlsOpts: TlsConnectOptions = { host: this.host, port: this.port };
        if (typeof this.tls === 'object') {
          if (this.tls.caFile) tlsOpts.ca = readFileSync(this.tls.caFile);
          if (this.tls.rejectUnauthorized === false) tlsOpts.rejectUnauthorized = false;
        }
        socket = tlsConnect(tlsOpts, onReady);
      } else {
        socket = netConnect({ host: this.host, port: this.port }, onReady);
      }
      socket.on('error', onError);
    });
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

    return new Promise<Response>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(reqId);
        reject(new CommandTimeoutError(`no response for ${command.cmd} within timeout`));
      }, timeoutMs ?? this.commandTimeoutMs);

      this.pending.set(reqId, {
        resolve: (response) => {
          clearTimeout(timer);
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
