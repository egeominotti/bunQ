/**
 * TCP Server
 * Handles TCP connections with msgpack binary protocol
 * Supports pipelining with parallel command processing
 */

import type { Socket, TCPSocketListener } from 'bun';
import type { QueueManager } from '../../application/queueManager';
import type { Response } from '../../domain/types/response';
import type { Command } from '../../domain/types/command';
import { handleCommand, type HandlerContext } from './handler';
import {
  FrameParser,
  FrameSizeError,
  createConnectionState,
  type ConnectionState,
} from './protocol';
import { uuid } from '../../shared/hash';
import { tcpLog } from '../../shared/logger';
import { getRateLimiter } from './rateLimiter';
import { pack, unpack } from 'msgpackr';
import { Semaphore, withSemaphore } from '../../shared/semaphore';
import { SocketWriteQueue } from './socketWriteQueue';

/** Max concurrent commands per connection for pipelining */
const MAX_CONCURRENT_PER_CONNECTION = 50;

/**
 * Idle/read stall timeout (ms) for the slowloris mitigation. A connection that
 * has STARTED a frame (partial bytes buffered) but makes no further progress
 * toward completing it within this window is closed. Healthy connections that
 * are simply idle (no partial frame buffered) are never affected, so long-lived
 * idle-but-healthy clients are not disturbed. 0 disables the timeout.
 *
 * Implemented with a manual per-connection timer (Bun.listen has no
 * `idleTimeout` socket option as of Bun 1.3.x, and `socket.timeout()` resets on
 * every byte — so a slowloris that trickles one byte per window would defeat
 * it). The manual timer is armed only while a partial frame is in progress.
 */
const TCP_IDLE_TIMEOUT_MS = Math.max(0, parseInt(Bun.env.TCP_IDLE_TIMEOUT_MS ?? '60000', 10) || 0);

/**
 * Maximum bytes that may be buffered in a connection's outbound write queue
 * before the connection is dropped. A client that stops reading while the
 * server keeps producing responses would otherwise grow the pending queue
 * without bound (write-side memory DoS). Generous default (64MB). 0 disables.
 */
const MAX_WRITE_QUEUE_BYTES = Math.max(
  0,
  parseInt(Bun.env.TCP_MAX_WRITE_QUEUE_BYTES ?? String(64 * 1024 * 1024), 10) || 0
);

/**
 * Release client jobs with retry logic and exponential backoff.
 * Ensures jobs are not left in an inconsistent state if release fails.
 */
async function releaseClientJobsWithRetry(
  queueManager: QueueManager,
  clientId: string,
  maxRetries = 3
): Promise<number> {
  let lastError: Error | undefined;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await queueManager.releaseClientJobs(clientId);
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      // Exponential backoff: 100ms, 200ms, 400ms
      await Bun.sleep(100 * Math.pow(2, attempt));
    }
  }

  tcpLog.error('Failed to release client jobs after retries', {
    clientId,
    error: lastError?.message,
  });
  throw lastError ?? new Error('Failed to release client jobs after retries');
}

/** TCP Server configuration */
export interface TcpServerConfig {
  /** TCP port */
  port?: number;
  /** Hostname to bind */
  hostname?: string;
  /** Auth tokens for authentication */
  authTokens?: string[];
  /**
   * Slowloris stall timeout (ms): close a connection that started a frame but
   * made no progress completing it within this window. 0 disables. Defaults to
   * TCP_IDLE_TIMEOUT_MS env (60s). Mainly for tests.
   */
  idleTimeoutMs?: number;
  /**
   * Max outbound write-queue bytes before dropping a connection (write-side
   * memory bound). 0 disables. Defaults to TCP_MAX_WRITE_QUEUE_BYTES env (64MB).
   * Mainly for tests.
   */
  maxWriteQueueBytes?: number;
}

/** Per-connection data */
interface ConnectionData {
  state: ConnectionState;
  frameParser: FrameParser;
  ctx: HandlerContext;
  /** Semaphore for limiting concurrent command processing (pipelining) */
  semaphore: Semaphore;
  /** Backpressure-aware write queue: buffers unwritten tails, flushes on drain */
  writeQueue: SocketWriteQueue;
  /** Active partial-frame stall timer (slowloris mitigation); null when idle. */
  stallTimer: ReturnType<typeof setTimeout> | null;
}

/** Serialize response to framed msgpack */
function serializeResponse(response: Response): Uint8Array {
  return FrameParser.frame(pack(response));
}

/** Error response as framed msgpack */
function errorResponse(message: string, reqId?: string): Uint8Array {
  return FrameParser.frame(pack({ ok: false, error: message, reqId }));
}

/**
 * Create and start TCP server
 */
export function createTcpServer(queueManager: QueueManager, config: TcpServerConfig) {
  const authTokens = new Set(config.authTokens ?? []);
  const connections = new Map<string, Socket<ConnectionData>>();
  const idleTimeoutMs = config.idleTimeoutMs ?? TCP_IDLE_TIMEOUT_MS;
  const maxWriteQueueBytes = config.maxWriteQueueBytes ?? MAX_WRITE_QUEUE_BYTES;

  /** Cancel any pending stall timer for a connection. */
  function clearStallTimer(socket: Socket<ConnectionData>): void {
    if (socket.data.stallTimer !== null) {
      clearTimeout(socket.data.stallTimer);
      socket.data.stallTimer = null;
    }
  }

  /**
   * (Re)arm the slowloris stall timer based on parser state. The timer runs
   * ONLY while a partial frame is buffered (a frame was started but not yet
   * completed). It is reset on every data event that still leaves a partial
   * frame, and cleared once the buffer drains to empty (frame completed). If it
   * fires, the peer started a frame and then stalled -> terminate.
   */
  function updateStallTimer(socket: Socket<ConnectionData>): void {
    if (idleTimeoutMs <= 0) return;
    clearStallTimer(socket);
    if (!socket.data.frameParser.hasPartialFrame) return;
    socket.data.stallTimer = setTimeout(() => {
      socket.data.stallTimer = null;
      tcpLog.warn('Closing stalled connection (incomplete frame)', {
        clientId: socket.data.state.clientId,
        bufferedBytes: socket.data.frameParser.bufferedBytes,
        idleTimeoutMs,
      });
      socket.data.writeQueue.clear();
      socket.terminate();
    }, idleTimeoutMs);
  }

  const socketHandlers = {
    open(socket: Socket<ConnectionData>) {
      const clientId = uuid();
      const state = createConnectionState(clientId);
      const ctx: HandlerContext = {
        queueManager,
        authTokens,
        authenticated: authTokens.size === 0, // Auto-auth if no tokens
        clientId, // For job ownership tracking
      };

      socket.data = {
        state,
        frameParser: new FrameParser(),
        ctx,
        semaphore: new Semaphore(MAX_CONCURRENT_PER_CONNECTION),
        writeQueue: new SocketWriteQueue(maxWriteQueueBytes),
        stallTimer: null,
      };

      connections.set(clientId, socket);
      queueManager.emitDashboardEvent('client:connected', { clientId, transport: 'tcp' });
    },

    async data(socket: Socket<ConnectionData>, data: Buffer) {
      const { frameParser, ctx, state, semaphore, writeQueue } = socket.data;
      const rateLimiter = getRateLimiter();

      // Check rate limit
      if (!rateLimiter.isAllowed(state.clientId)) {
        ctx.queueManager.emitDashboardEvent('ratelimit:hit', { clientId: state.clientId });
        writeQueue.write(socket, errorResponse('Rate limit exceeded'));
        return;
      }

      let frames: Uint8Array[];
      try {
        frames = frameParser.addData(new Uint8Array(data));
      } catch (err) {
        if (err instanceof FrameSizeError) {
          clearStallTimer(socket);
          writeQueue.write(
            socket,
            errorResponse(
              `Frame too large: ${err.requestedSize} bytes exceeds maximum ${err.maxSize}`
            )
          );
          socket.end();
          return;
        }
        throw err;
      }

      // Slowloris mitigation: (re)arm the stall timer based on whether a partial
      // frame is now buffered. Progress (a completed frame draining the buffer)
      // disarms it; a started-but-unfinished frame arms it. This bounds memory
      // held by a peer that declares a large (legal) frame then never finishes.
      updateStallTimer(socket);

      // Drop a connection whose outbound queue grew unbounded (client stopped
      // reading while we keep producing responses) — bounds write-side memory.
      const dropForWriteOverflow = (): boolean => {
        if (writeQueue.isOverBudget) {
          tcpLog.warn('Closing connection: write queue exceeded budget', {
            clientId: state.clientId,
            queuedBytes: writeQueue.bytesQueued,
          });
          clearStallTimer(socket);
          writeQueue.clear();
          socket.terminate();
          return true;
        }
        return false;
      };

      // Process frames in parallel for pipelining support
      // Each command is processed with semaphore-controlled concurrency
      const processFrame = async (frame: Uint8Array): Promise<void> => {
        let cmd: Command;
        try {
          cmd = unpack(frame) as Command;
        } catch {
          writeQueue.write(socket, errorResponse('Invalid command format'));
          return;
        }

        if (!cmd?.cmd) {
          writeQueue.write(socket, errorResponse('Invalid command'));
          return;
        }

        // Process with concurrency limit
        await withSemaphore(semaphore, async () => {
          try {
            const response = await handleCommand(cmd, ctx);
            writeQueue.write(socket, serializeResponse(response));
            dropForWriteOverflow();
          } catch (err) {
            const raw = err instanceof Error ? err.message : 'Unknown error';
            const message =
              raw.includes('SQLITE') || raw.includes('database') ? 'Internal server error' : raw;
            writeQueue.write(socket, errorResponse(message, cmd.reqId));
            dropForWriteOverflow();
          }
        });
      };

      // Process all frames in parallel (client uses reqId to match responses)
      await Promise.all(frames.map(processFrame));
    },

    close(socket: Socket<ConnectionData>) {
      const clientId = socket.data.state.clientId;
      // Cancel the slowloris stall timer and drop any buffered-but-unwritten
      // bytes; the socket is gone.
      clearStallTimer(socket);
      socket.data.writeQueue.clear();
      connections.delete(clientId);
      getRateLimiter().removeClient(clientId);
      queueManager.unregisterWorkersByClientId(clientId);
      queueManager.emitDashboardEvent('client:disconnected', { clientId, transport: 'tcp' });

      // Release all jobs owned by this client back to queue with retry logic
      releaseClientJobsWithRetry(queueManager, clientId)
        .then(() => {
          // Jobs released successfully
        })
        .catch((err: unknown) => {
          // After all retries failed, fall back to a force-release that
          // unconditionally clears client tracking (prevents Map leak) and
          // resets heartbeats so the stall detector recovers any orphaned
          // 'active' jobs on its next tick.
          const touched = queueManager.forceReleaseClientJobs(clientId);
          tcpLog.error('Client jobs release failed; fell back to force-release', {
            clientId,
            error: String(err),
            forcedJobs: touched,
            note: 'Stall detector will recover orphaned active jobs on next tick',
          });
        });
    },

    error(_socket: Socket<ConnectionData>, error: Error) {
      tcpLog.error('Connection error', { error: error.message });
    },

    drain(socket: Socket<ConnectionData>) {
      // Socket is ready for more writes after backpressure: flush any unwritten
      // tail bytes buffered by short writes, preserving frame order.
      socket.data.writeQueue.flush(socket);
    },
  };

  // Create TCP server
  const server: TCPSocketListener<ConnectionData> = Bun.listen<ConnectionData>({
    hostname: config.hostname ?? '0.0.0.0',
    port: config.port ?? 6789,
    socket: socketHandlers,
  });
  return {
    server,
    connections,

    /** Get connection count */
    getConnectionCount(): number {
      return connections.size;
    },

    /** Broadcast to all connections */
    broadcast(message: unknown): void {
      const frame = FrameParser.frame(pack(message));
      for (const socket of connections.values()) {
        // Each connection writes through its own backpressure-aware queue so a
        // slow reader buffers (in order) instead of dropping frame tail bytes.
        socket.data.writeQueue.write(socket, frame);
        // Drop any reader that has fallen too far behind (write-side bound).
        if (socket.data.writeQueue.isOverBudget) {
          clearStallTimer(socket);
          socket.data.writeQueue.clear();
          socket.terminate();
        }
      }
    },

    /** Stop the server */
    stop(): void {
      server.stop();
      for (const socket of connections.values()) {
        clearStallTimer(socket);
        socket.end();
      }
      connections.clear();
    },
  };
}

export type TcpServer = ReturnType<typeof createTcpServer>;
