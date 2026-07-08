/**
 * TCP Connection Handler
 * Manages low-level socket connection and data handling (msgpack binary protocol)
 */

import { readFileSync } from 'node:fs';
import type { Socket } from 'bun';
import type { SocketWrapper, PendingCommand, ClientTlsOptions } from './types';
import { FrameParser, FrameSizeError } from '../../infrastructure/server/protocol';

/** Connection events */
export interface ConnectionEvents {
  onData: (frame: Uint8Array) => void;
  onClose: () => void;
  onError: (error: Error) => void;
}

/** Connection result */
export interface ConnectionResult {
  socket: SocketWrapper;
  cleanup: () => void;
}

/** Connection target */
export interface ConnectionTarget {
  /** TCP host */
  host?: string;
  /** TCP port */
  port?: number;
  /** Enable TLS: true (system CAs) or per-connection TLS options */
  tls?: boolean | ClientTlsOptions;
}

/**
 * Map client TLS options to the `tls` value accepted by Bun.connect.
 * Returns undefined when TLS is disabled (plaintext, the default).
 *
 * The CA is read into bytes (not passed as a `Bun.file` handle): Bun computes
 * the peer's `authorizationError` from this `ca`, and we enforce it in the
 * `handshake` handler (see `tlsRequiresVerification` / #109) — Bun.connect
 * itself never rejects an unauthorized peer on the client side.
 */
export function buildClientTls(
  tls: boolean | ClientTlsOptions | undefined
): true | Record<string, unknown> | undefined {
  if (!tls) return undefined;
  if (tls === true) return true;
  return {
    ...(tls.rejectUnauthorized !== undefined && { rejectUnauthorized: tls.rejectUnauthorized }),
    ...(tls.caFile !== undefined && { ca: readFileSync(tls.caFile) }),
  };
}

/**
 * Whether the caller asked us to authenticate the server certificate.
 * Verification is the default for any TLS connection; only an explicit
 * `rejectUnauthorized: false` opts out (encryption-only). #109
 */
export function tlsRequiresVerification(tls: boolean | ClientTlsOptions | undefined): boolean {
  if (!tls) return false;
  if (tls === true) return true;
  return tls.rejectUnauthorized !== false;
}

/**
 * Establish TCP connection to server
 */
export async function createConnection(
  target: ConnectionTarget,
  connectTimeout: number,
  events: ConnectionEvents
): Promise<ConnectionResult> {
  return new Promise((resolve, reject) => {
    const socketData: SocketWrapper = {
      write: () => {},
      end: () => {},
      frameParser: new FrameParser(),
    };

    let connectionResolved = false;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;

    // TLS server-certificate verification. Bun.connect never rejects an
    // unauthorized peer on the client side, but it DOES compute the peer's
    // authorizationError and hand it to the `handshake` callback — so we gate
    // the connection on it ourselves. #109
    //
    // Crucially, once a `handshake` handler is registered, Bun fires `open`
    // BEFORE the TLS handshake completes (without it, `open` fires only after).
    // So for EVERY TLS connection we must resolve on `handshake`, not `open` —
    // otherwise the pool writes its first command onto a socket whose handshake
    // is still in flight and the bytes are lost. Plaintext has no handshake
    // event, so it still resolves in `open`.
    const tlsValue = buildClientTls(target.tls);
    const isTls = tlsValue !== undefined;
    const verifyTls = tlsRequiresVerification(target.tls);
    let opened = false;
    let handshakeOk = !isTls; // plaintext: nothing to wait for; TLS: wait for handshake
    const maybeResolveOpen = () => {
      if (opened && handshakeOk && !connectionResolved) {
        connectionResolved = true;
        // Clear the connect timeout only now that we are actually resolving.
        // For TLS, `open` fires BEFORE `handshake`, so clearing the timeout in
        // `open` would leave the handshake phase unbounded — a stalled/malicious
        // peer that completes TCP but never drives the TLS handshake (no error,
        // no close) would hang this promise forever. Keeping the timeout armed
        // until resolve preserves connectTimeout as a bound over the handshake.
        cleanup();
        resolve({ socket: socketData, cleanup });
      }
    };

    const cleanup = () => {
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
    };

    const targetDesc = `${target.host}:${target.port}`;

    // Socket handlers
    const socketHandlers = {
      data(_sock: Socket<unknown>, data: Buffer) {
        let frames: Uint8Array[];
        try {
          // `data` (Bun Buffer) is copied into addData's own buffer synchronously
          // and never retained, so the `new Uint8Array(data)` wrapper was a
          // redundant full copy per read on the client response path.
          frames = socketData.frameParser.addData(data);
        } catch (err) {
          if (err instanceof FrameSizeError) {
            events.onError(
              new Error(
                `Frame too large: ${err.requestedSize} bytes exceeds maximum ${err.maxSize}`
              )
            );
            return;
          }
          throw err;
        }
        for (const frame of frames) {
          events.onData(frame);
        }
      },
      open(sock: Socket<unknown>) {
        // NB: do NOT clear the connect timeout here. For TLS, `open` precedes
        // `handshake`; the timeout must stay armed until `maybeResolveOpen`
        // actually resolves (or a stalled handshake could hang forever). #109
        // Enable TCP keepalive so the OS probes idle connections and surfaces a
        // dead peer (suspended host, NAT/LB drop) via an error/close event,
        // instead of a half-open socket lingering until tcp_retries2 (~15 min).
        // Best-effort: not all platforms honor the delay, and older Bun builds
        // may lack the method — never let it abort connection setup. See #94.
        try {
          (
            sock as unknown as { setKeepAlive?: (enable: boolean, delayMs?: number) => void }
          ).setKeepAlive?.(true, 15000);
        } catch {
          /* keepalive unsupported on this platform/runtime */
        }
        socketData.write = (d: Uint8Array | string) => sock.write(d);
        socketData.end = () => sock.end();
        opened = true;
        maybeResolveOpen();
      },
      handshake(sock: Socket<unknown>, success: boolean, authorizationError: Error | null) {
        // Fires only for TLS. When verification is required, a failed handshake
        // or any authorizationError (wrong/absent CA, self-signed, host
        // mismatch) must abort — otherwise TLS is encryption-only and an active
        // MITM can impersonate the broker and harvest the auth token. #109
        if (verifyTls && (!success || authorizationError)) {
          if (!connectionResolved) {
            connectionResolved = true;
            cleanup();
            const why = authorizationError?.message ?? 'handshake failed';
            reject(new Error(`TLS verification failed for ${targetDesc}: ${why}`));
          }
          try {
            sock.end();
          } catch {
            /* already closing */
          }
          return;
        }
        handshakeOk = true;
        maybeResolveOpen();
      },
      close() {
        if (!connectionResolved) {
          connectionResolved = true;
          cleanup();
          reject(new Error('Connection closed'));
        }
        events.onClose();
      },
      error(_sock: Socket<unknown>, error: Error) {
        if (!connectionResolved) {
          connectionResolved = true;
          cleanup();
          reject(new Error(`Connection error: ${error.message}`));
        }
        events.onError(error);
      },
      connectError(_sock: Socket<unknown>, error: Error) {
        if (!connectionResolved) {
          connectionResolved = true;
          cleanup();
          reject(new Error(`Failed to connect to ${targetDesc}: ${error.message}`));
        }
      },
    };

    // Connect via TCP (optionally wrapped in TLS — protocol is unchanged).
    // `tlsValue`/`isTls` were computed above to drive handshake-gated resolve.
    void (Bun.connect as (opts: unknown) => Promise<unknown>)({
      hostname: target.host ?? 'localhost',
      port: target.port ?? 6789,
      ...(tlsValue !== undefined && { tls: tlsValue }),
      socket: socketHandlers,
    }).catch((error: unknown) => {
      // Bun.connect rejects (instead of firing connectError) for some failure
      // modes, e.g. a TLS handshake refused synchronously. Route it through the
      // same rejection path so callers never hang until the connect timeout.
      if (!connectionResolved) {
        connectionResolved = true;
        cleanup();
        const message = error instanceof Error ? error.message : String(error);
        reject(new Error(`Failed to connect to ${targetDesc}: ${message}`));
      }
    });

    timeoutId = setTimeout(() => {
      if (!connectionResolved) {
        connectionResolved = true;
        reject(new Error(`Connection timeout to ${targetDesc}`));
      }
    }, connectTimeout);
  });
}

/**
 * Command queue for managing pending commands
 * Supports both legacy single-command mode and pipelining mode with reqId tracking
 */
export class CommandQueue {
  private readonly pendingCommands: Map<number, PendingCommand> = new Map();
  private pendingQueue: number[] = [];
  private currentCommand: PendingCommand | null = null;
  private commandIdCounter = 0;

  /** In-flight commands tracked by reqId for pipelining */
  private readonly inFlightByReqId: Map<string, PendingCommand> = new Map();

  /** Get current command being processed (legacy mode) */
  getCurrentCommand(): PendingCommand | null {
    return this.currentCommand;
  }

  /** Set current command (legacy mode) */
  setCurrentCommand(cmd: PendingCommand | null): void {
    this.currentCommand = cmd;
  }

  /** Check if has pending commands in queue */
  hasPending(): boolean {
    return this.pendingCommands.size > 0;
  }

  /** Get number of in-flight commands */
  getInFlightCount(): number {
    return this.inFlightByReqId.size;
  }

  /** Check if can send more commands (pipelining backpressure) */
  canSendMore(maxInFlight: number): boolean {
    return this.inFlightByReqId.size < maxInFlight;
  }

  /** Add command to in-flight tracking by reqId */
  addInFlight(command: PendingCommand): void {
    this.inFlightByReqId.set(command.reqId, command);
  }

  /** Get in-flight command by reqId */
  getByReqId(reqId: string): PendingCommand | undefined {
    return this.inFlightByReqId.get(reqId);
  }

  /** Remove in-flight command by reqId */
  removeByReqId(reqId: string): PendingCommand | undefined {
    const cmd = this.inFlightByReqId.get(reqId);
    if (cmd) {
      this.inFlightByReqId.delete(reqId);
    }
    return cmd;
  }

  /** Add command to queue */
  enqueue(command: PendingCommand): void {
    this.pendingCommands.set(command.id, command);
    this.pendingQueue.push(command.id);
  }

  /** Get next command ID */
  nextId(): number {
    return ++this.commandIdCounter;
  }

  /** Dequeue next command */
  dequeue(): PendingCommand | null {
    const nextId = this.pendingQueue.shift();
    if (nextId === undefined) return null;

    const next = this.pendingCommands.get(nextId);
    if (!next) return null;

    this.pendingCommands.delete(nextId);
    return next;
  }

  /** Remove command by ID */
  remove(id: number): boolean {
    if (!this.pendingCommands.has(id)) return false;

    this.pendingCommands.delete(id);
    const queueIdx = this.pendingQueue.indexOf(id);
    if (queueIdx !== -1) {
      this.pendingQueue.splice(queueIdx, 1);
    }
    return true;
  }

  /** Reject all pending and in-flight commands */
  rejectAll(error: Error): void {
    // Reject queued commands. Attach a silent .catch BEFORE rejecting so that
    // callers without a .catch in place (fire-and-forget heartbeats, polling
    // loops mid-await on intentional close) don't surface as unhandled
    // rejections and force non-zero process exit.
    for (const cmd of this.pendingCommands.values()) {
      clearTimeout(cmd.timeout);
      cmd.promise?.catch(() => {});
      cmd.reject(error);
    }
    this.pendingCommands.clear();
    this.pendingQueue = [];

    // Reject in-flight commands (pipelining)
    for (const cmd of this.inFlightByReqId.values()) {
      clearTimeout(cmd.timeout);
      cmd.promise?.catch(() => {});
      cmd.reject(error);
    }
    this.inFlightByReqId.clear();

    // Reject current command (legacy)
    if (this.currentCommand) {
      clearTimeout(this.currentCommand.timeout);
      this.currentCommand.reject(error);
      this.currentCommand = null;
    }
  }

  /** Clear current command with optional rejection (legacy mode) */
  clearCurrent(error?: Error): void {
    if (this.currentCommand) {
      clearTimeout(this.currentCommand.timeout);
      if (error) {
        this.currentCommand.reject(error);
      }
      this.currentCommand = null;
    }
  }
}
