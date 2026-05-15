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

/** Max concurrent commands per connection for pipelining */
const MAX_CONCURRENT_PER_CONNECTION = 50;

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
}

/** Per-connection data */
interface ConnectionData {
  state: ConnectionState;
  frameParser: FrameParser;
  ctx: HandlerContext;
  /** Semaphore for limiting concurrent command processing (pipelining) */
  semaphore: Semaphore;
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
      };

      connections.set(clientId, socket);
      queueManager.emitDashboardEvent('client:connected', { clientId, transport: 'tcp' });
    },

    async data(socket: Socket<ConnectionData>, data: Buffer) {
      const { frameParser, ctx, state, semaphore } = socket.data;
      const rateLimiter = getRateLimiter();

      // Check rate limit
      if (!rateLimiter.isAllowed(state.clientId)) {
        ctx.queueManager.emitDashboardEvent('ratelimit:hit', { clientId: state.clientId });
        socket.write(errorResponse('Rate limit exceeded'));
        return;
      }

      let frames: Uint8Array[];
      try {
        frames = frameParser.addData(new Uint8Array(data));
      } catch (err) {
        if (err instanceof FrameSizeError) {
          socket.write(
            errorResponse(
              `Frame too large: ${err.requestedSize} bytes exceeds maximum ${err.maxSize}`
            )
          );
          socket.end();
          return;
        }
        throw err;
      }

      // Process frames in parallel for pipelining support
      // Each command is processed with semaphore-controlled concurrency
      const processFrame = async (frame: Uint8Array): Promise<void> => {
        let cmd: Command;
        try {
          cmd = unpack(frame) as Command;
        } catch {
          socket.write(errorResponse('Invalid command format'));
          return;
        }

        if (!cmd?.cmd) {
          socket.write(errorResponse('Invalid command'));
          return;
        }

        // Process with concurrency limit
        await withSemaphore(semaphore, async () => {
          try {
            const response = await handleCommand(cmd, ctx);
            socket.write(serializeResponse(response));
          } catch (err) {
            const raw = err instanceof Error ? err.message : 'Unknown error';
            const message =
              raw.includes('SQLITE') || raw.includes('database') ? 'Internal server error' : raw;
            socket.write(errorResponse(message, cmd.reqId));
          }
        });
      };

      // Process all frames in parallel (client uses reqId to match responses)
      await Promise.all(frames.map(processFrame));
    },

    close(socket: Socket<ConnectionData>) {
      const clientId = socket.data.state.clientId;
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

    drain(_socket: Socket<ConnectionData>) {
      // Called when socket is ready for more writes after backpressure
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
        socket.write(frame);
      }
    },

    /** Stop the server */
    stop(): void {
      server.stop();
      for (const socket of connections.values()) {
        socket.end();
      }
      connections.clear();
    },
  };
}

export type TcpServer = ReturnType<typeof createTcpServer>;
