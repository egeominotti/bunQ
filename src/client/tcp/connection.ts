/**
 * TCP Connection Handler
 * Manages low-level socket connection and data handling (msgpack binary protocol)
 */

import type { Socket } from 'bun';
import type { SocketWrapper, PendingCommand } from './types';
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
          frames = socketData.frameParser.addData(new Uint8Array(data));
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
        cleanup();
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
        connectionResolved = true;
        resolve({ socket: socketData, cleanup });
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

    // Connect via TCP
    void Bun.connect({
      hostname: target.host ?? 'localhost',
      port: target.port ?? 6789,
      socket: socketHandlers,
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
