import type { Socket } from 'bun';
import type { QueueManager } from '../../../application/queueManager';
import { uuid } from '../../../shared/hash';
import { tcpLog } from '../../../shared/logger';
import { encodeMessagePack } from '../../../shared/msgpack';
import { Semaphore } from '../../../shared/semaphore';
import { FrameParser, createConnectionState } from '../protocol';
import { getRateLimiter } from '../rateLimiter';
import { SocketWriteQueue } from '../socketWriteQueue';
import type { TcpConnectionData } from '../types/tcpServer';
import { releaseClientJobsWithRetry } from './clientRelease';
import { MAX_CONCURRENT_PER_CONNECTION } from './constants';

/** Owns per-client state, slowloris timers, backpressure, and disconnect cleanup. */
export class TcpConnectionRegistry {
  readonly connections = new Map<string, Socket<TcpConnectionData>>();

  constructor(
    private readonly queueManager: QueueManager,
    private readonly authTokens: Set<string>,
    private readonly idleTimeoutMs: number,
    private readonly maxWriteQueueBytes: number
  ) {}

  init(socket: Socket<TcpConnectionData>): void {
    if (socket.data) return;
    const clientId = uuid();
    const abortController = new AbortController();
    socket.data = {
      state: createConnectionState(clientId),
      abortController,
      frameParser: new FrameParser(),
      ctx: {
        queueManager: this.queueManager,
        authTokens: this.authTokens,
        authenticated: this.authTokens.size === 0,
        clientId,
        signal: abortController.signal,
      },
      semaphore: new Semaphore(MAX_CONCURRENT_PER_CONNECTION),
      writeQueue: new SocketWriteQueue(this.maxWriteQueueBytes),
      stallTimer: null,
    };
    this.connections.set(clientId, socket);
    this.queueManager.emitDashboardEvent('client:connected', { clientId, transport: 'tcp' });
  }

  clearStallTimer(socket: Socket<TcpConnectionData>): void {
    if (socket.data.stallTimer !== null) {
      clearTimeout(socket.data.stallTimer);
      socket.data.stallTimer = null;
    }
  }

  updateStallTimer(socket: Socket<TcpConnectionData>): void {
    if (this.idleTimeoutMs <= 0) return;
    this.clearStallTimer(socket);
    if (!socket.data.frameParser.hasPartialFrame) return;
    socket.data.stallTimer = setTimeout(() => {
      socket.data.stallTimer = null;
      tcpLog.warn('Closing stalled connection (incomplete frame)', {
        clientId: socket.data.state.clientId,
        bufferedBytes: socket.data.frameParser.bufferedBytes,
        idleTimeoutMs: this.idleTimeoutMs,
      });
      socket.data.writeQueue.clear();
      socket.terminate();
    }, this.idleTimeoutMs);
  }

  dropForWriteOverflow(socket: Socket<TcpConnectionData>): boolean {
    const { writeQueue, state } = socket.data;
    if (!writeQueue.isOverBudget) return false;
    tcpLog.warn('Closing connection: write queue exceeded budget', {
      clientId: state.clientId,
      queuedBytes: writeQueue.bytesQueued,
    });
    this.clearStallTimer(socket);
    writeQueue.clear();
    socket.terminate();
    return true;
  }

  close(socket: Socket<TcpConnectionData>): void {
    if (!socket.data) return;
    socket.data.abortController.abort();
    const clientId = socket.data.state.clientId;
    this.clearStallTimer(socket);
    socket.data.writeQueue.clear();
    this.connections.delete(clientId);
    getRateLimiter().removeClient(clientId);
    this.queueManager.unregisterWorkersByClientId(clientId);
    this.queueManager.emitDashboardEvent('client:disconnected', {
      clientId,
      transport: 'tcp',
    });

    releaseClientJobsWithRetry(this.queueManager, clientId).catch((error: unknown) => {
      const touched = this.queueManager.forceReleaseClientJobs(clientId);
      tcpLog.error('Client jobs release failed; fell back to force-release', {
        clientId,
        error: String(error),
        forcedJobs: touched,
        note: 'Stall detector will recover orphaned active jobs on next tick',
      });
    });
  }

  broadcast(message: unknown): void {
    const frame = FrameParser.frame(encodeMessagePack(message));
    for (const socket of this.connections.values()) {
      socket.data.writeQueue.write(socket, frame);
      if (socket.data.writeQueue.isOverBudget) {
        this.clearStallTimer(socket);
        socket.data.writeQueue.clear();
        socket.terminate();
      }
    }
  }

  closeAll(): void {
    for (const socket of this.connections.values()) {
      this.clearStallTimer(socket);
      socket.end();
    }
    this.connections.clear();
  }
}
