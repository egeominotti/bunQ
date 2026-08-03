import type { Socket } from 'bun';
import type { QueueManager } from '../../../application/queueManager';
import type { JobEvent } from '../../../domain/types/queue';
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
  private eventUnsubscribe: (() => void) | null = null;

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
      eventQueue: null,
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
    socket.data.eventQueue = null;
    this.connections.delete(clientId);
    this.releaseEventBridgeIfIdle();
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

  subscribeEvents(socket: Socket<TcpConnectionData>, queue: string): void {
    socket.data.eventQueue = queue;
    if (!this.eventUnsubscribe) {
      this.eventUnsubscribe = this.queueManager.subscribe((event) => this.broadcastEvent(event));
    }
  }

  unsubscribeEvents(socket: Socket<TcpConnectionData>): void {
    socket.data.eventQueue = null;
    this.releaseEventBridgeIfIdle();
  }

  private broadcastEvent(event: JobEvent): void {
    let frame: Uint8Array | null = null;
    for (const socket of this.connections.values()) {
      if (socket.data.eventQueue !== event.queue) continue;
      frame ??= FrameParser.frame(encodeMessagePack({ type: 'event', event }));
      socket.data.writeQueue.write(socket, frame);
      this.dropForWriteOverflow(socket);
    }
  }

  private releaseEventBridgeIfIdle(): void {
    if (!this.eventUnsubscribe) return;
    for (const socket of this.connections.values()) {
      if (socket.data.eventQueue !== null) return;
    }
    this.eventUnsubscribe();
    this.eventUnsubscribe = null;
  }

  closeAll(): void {
    this.eventUnsubscribe?.();
    this.eventUnsubscribe = null;
    for (const socket of this.connections.values()) {
      this.clearStallTimer(socket);
      socket.data.eventQueue = null;
      socket.data.writeQueue.clear();
      socket.terminate();
    }
    this.connections.clear();
  }
}
