/**
 * SSE Handler - Enterprise-grade Server-Sent Events
 *
 * Features:
 * - Typed SSE events (event: field) for selective client subscription
 * - Event IDs for client-side deduplication
 * - Last-Event-ID resume on reconnection (ring buffer)
 * - Heartbeat keepalive (30s) to detect dead connections
 * - Periodic broadcasts: stats (5s), health (10s), storage (30s)
 * - Dashboard event forwarding (worker, queue control, DLQ, cron, etc.)
 * - Connection limits to prevent resource exhaustion
 * - Automatic dead client cleanup
 */

import type { QueueManager } from '../../application/queueManager';
import type { JobEvent } from '../../domain/types/queue';
import { throughputTracker } from '../../application/throughputTracker';
import { uuid } from '../../shared/hash';
import { QueueCountsScheduler } from './queueCountsScheduler';

const textEncoder = new TextEncoder();

/** Tuning constants */
const MAX_CLIENTS = 1000;
const HEARTBEAT_MS = 30_000;
const RETRY_MS = 3_000;
const EVENT_BUFFER_SIZE = 1000;

/** SSE client tracking */
export interface SseClient {
  id: string;
  controller: ReadableStreamDefaultController;
  queueFilter: string | null;
}

/** Buffered event for Last-Event-ID replay */
interface BufferedEvent {
  id: number;
  event: string;
  data: string;
  queue: string;
}

/** Old eventType → SSE event name (mirrors wsHandler) */
const EVENT_MAP: Record<string, string> = {
  pushed: 'job:pushed',
  pulled: 'job:active',
  completed: 'job:completed',
  failed: 'job:failed',
  progress: 'job:progress',
  stalled: 'job:stalled',
  removed: 'job:removed',
  delayed: 'job:delayed',
  duplicated: 'job:duplicated',
  retried: 'job:retried',
  'waiting-children': 'job:waiting-children',
  drained: 'queue:drained',
};

/**
 * SSE Handler - manages Server-Sent Events clients with full event parity
 */
export class SseHandler {
  private readonly clients = new Map<string, SseClient>();
  private eventId = 0;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private statsInterval: ReturnType<typeof setInterval> | null = null;
  private healthInterval: ReturnType<typeof setInterval> | null = null;
  private storageInterval: ReturnType<typeof setInterval> | null = null;
  private queueManager: QueueManager | null = null;
  private queueCountsScheduler: QueueCountsScheduler | null = null;
  private readonly eventBuffer: BufferedEvent[] = [];

  /** Get client count */
  get size(): number {
    return this.clients.size;
  }

  // ── Lifecycle ──────────────────────────────────────────────

  /** Start heartbeat + periodic broadcasts (stats, health, storage) */
  startBroadcasts(qm: QueueManager): void {
    this.queueManager = qm;
    this.queueCountsScheduler ??= new QueueCountsScheduler(qm, (queue, counts) => {
      this.sendTypedEvent('queue:counts', {
        queue,
        waiting: counts.waiting,
        prioritized: counts.prioritized,
        active: counts.active,
        completed: counts.completed,
        failed: counts.failed,
        delayed: counts.delayed,
      });
    });

    this.heartbeatTimer ??= setInterval(() => {
      this.sendHeartbeat();
    }, HEARTBEAT_MS);

    this.statsInterval ??= setInterval(() => {
      if (this.clients.size > 0) this.broadcastStats(qm);
    }, 5000);

    this.healthInterval ??= setInterval(() => {
      if (this.clients.size > 0) this.broadcastHealth(qm);
    }, 10000);

    this.storageInterval ??= setInterval(() => {
      if (this.clients.size > 0) this.broadcastStorage(qm);
    }, 30000);
  }

  /** @deprecated Use startBroadcasts() instead */
  startHeartbeat(): void {
    this.heartbeatTimer ??= setInterval(() => {
      this.sendHeartbeat();
    }, HEARTBEAT_MS);
  }

  /** Stop all timers */
  private stopTimers(): void {
    this.queueCountsScheduler?.stop();
    this.queueCountsScheduler = null;
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.statsInterval) {
      clearInterval(this.statsInterval);
      this.statsInterval = null;
    }
    if (this.healthInterval) {
      clearInterval(this.healthInterval);
      this.healthInterval = null;
    }
    if (this.storageInterval) {
      clearInterval(this.storageInterval);
      this.storageInterval = null;
    }
  }

  // ── Heartbeat ──────────────────────────────────────────────

  /** Send heartbeat comment to all clients, prune dead ones */
  private sendHeartbeat(): void {
    if (this.clients.size === 0) return;
    const heartbeat = textEncoder.encode(`:heartbeat\n\n`);
    const disconnected: string[] = [];

    for (const [clientId, client] of this.clients) {
      try {
        client.controller.enqueue(heartbeat);
      } catch {
        disconnected.push(clientId);
      }
    }

    for (const id of disconnected) this.clients.delete(id);
  }

  // ── Job event broadcasting ─────────────────────────────────

  /** Broadcast job event to matching clients (with typed SSE event field) */
  broadcast(event: JobEvent): void {
    // No clients => nothing to send, and nothing to buffer that anyone could
    // replay. Mirrors wsHandler.broadcast. Without this, every job event still
    // paid JSON.stringify + encode + ring buffer + an O(queue size) per-event
    // getQueueJobCounts, turning a bulk push into O(N²) even with no dashboard
    // attached (the common high-throughput case).
    if (this.clients.size === 0) return;

    const id = ++this.eventId;
    const eventName = EVENT_MAP[event.eventType] ?? `job:${event.eventType}`;

    const eventData: Record<string, unknown> = {
      queue: event.queue,
      jobId: event.jobId,
      timestamp: event.timestamp,
    };
    if (event.error) eventData.error = event.error;
    if (event.progress !== undefined) eventData.progress = event.progress;
    if (event.prev) eventData.prev = event.prev;
    if (event.delay !== undefined) eventData.delay = event.delay;

    const data = JSON.stringify(eventData);
    this.bufferEvent(id, eventName, data, event.queue);

    const msg = textEncoder.encode(`id: ${id}\nevent: ${eventName}\ndata: ${data}\n\n`);
    const disconnected: string[] = [];

    for (const [clientId, client] of this.clients) {
      if (!client.queueFilter || client.queueFilter === event.queue) {
        try {
          client.controller.enqueue(msg);
        } catch {
          disconnected.push(clientId);
        }
      }
    }

    for (const clientId of disconnected) this.clients.delete(clientId);

    // Counts are eventually exact, but coalesced so a PUSHB does not perform
    // one full queue scan per inserted job.
    this.queueCountsScheduler?.schedule(event.queue);
  }

  // ── Dashboard / system event broadcasting ──────────────────

  /** Emit a typed event (dashboard events: worker, queue control, DLQ, etc.) */
  emitEvent(event: string, data: Record<string, unknown>): void {
    this.sendTypedEvent(event, data);
  }

  /** Send typed SSE event to all clients (system events bypass queue filter) */
  private sendTypedEvent(eventName: string, data: Record<string, unknown>): void {
    if (this.clients.size === 0) return;
    const id = ++this.eventId;
    const jsonData = JSON.stringify(data);

    const msg = textEncoder.encode(`id: ${id}\nevent: ${eventName}\ndata: ${jsonData}\n\n`);
    const disconnected: string[] = [];

    for (const [clientId, client] of this.clients) {
      try {
        client.controller.enqueue(msg);
      } catch {
        disconnected.push(clientId);
      }
    }

    for (const clientId of disconnected) this.clients.delete(clientId);
  }

  // ── Periodic broadcasts ────────────────────────────────────

  private broadcastStats(qm: QueueManager): void {
    const stats = qm.getStats();
    const rates = throughputTracker.getRates();
    const perQueue = qm.getPerQueueStats();
    const workerStats = qm.workerManager.getStats();
    const crons = qm.listCrons();

    const queues: Record<string, object> = {};
    for (const [name, s] of perQueue) {
      queues[name] = {
        waiting: s.waiting ?? 0,
        active: s.active ?? 0,
        delayed: s.delayed ?? 0,
        dlq: s.dlq ?? 0,
        paused: qm.isPaused(name),
      };
    }

    this.sendTypedEvent('stats:snapshot', {
      waiting: stats.waiting,
      active: stats.active,
      delayed: stats.delayed,
      completed: stats.completed,
      dlq: stats.dlq,
      totalPushed: Number(stats.totalPushed),
      totalCompleted: Number(stats.totalCompleted),
      totalFailed: Number(stats.totalFailed),
      pushPerSec: rates.pushPerSec,
      pullPerSec: rates.pullPerSec,
      uptime: stats.uptime,
      queues,
      workers: { total: workerStats.total, active: workerStats.active },
      cronJobs: crons.length,
    });
  }

  private broadcastHealth(qm: QueueManager): void {
    const mem = process.memoryUsage();
    const storage = qm.getStorageStatus();

    this.sendTypedEvent('health:status', {
      ok: !storage.diskFull,
      uptime: Math.floor(process.uptime()),
      memory: {
        rss: Math.round(mem.rss / 1024 / 1024),
        heapUsed: Math.round(mem.heapUsed / 1024 / 1024),
      },
      connections: { sse: this.clients.size },
    });
  }

  private broadcastStorage(qm: QueueManager): void {
    const memStats = qm.getMemoryStats();
    const storage = qm.getStorageStatus();

    this.sendTypedEvent('storage:status', {
      collections: memStats,
      diskFull: storage.diskFull,
    });
  }

  // ── Event buffer (ring buffer for Last-Event-ID replay) ────

  private bufferEvent(id: number, event: string, data: string, queue: string): void {
    this.eventBuffer.push({ id, event, data, queue });
    if (this.eventBuffer.length > EVENT_BUFFER_SIZE) {
      this.eventBuffer.shift();
    }
  }

  /** Replay missed events for a reconnecting client */
  private replayEvents(client: SseClient, lastEventId: number): void {
    for (const buffered of this.eventBuffer) {
      if (buffered.id <= lastEventId) continue;
      if (client.queueFilter && buffered.queue && client.queueFilter !== buffered.queue) continue;
      try {
        const msg = `id: ${buffered.id}\nevent: ${buffered.event}\ndata: ${buffered.data}\n\n`;
        client.controller.enqueue(textEncoder.encode(msg));
      } catch {
        break;
      }
    }
  }

  // ── Client connection ──────────────────────────────────────

  /** Create SSE response for a new client */
  createResponse(queueFilter: string | null, corsOrigin: string, lastEventId?: string): Response {
    if (this.clients.size >= MAX_CLIENTS) {
      return new Response('Too many SSE connections', { status: 503 });
    }

    const clientId = uuid();
    const resumeId = lastEventId ? parseInt(lastEventId, 10) : 0;

    const stream = new ReadableStream({
      start: (controller) => {
        const client: SseClient = { id: clientId, controller, queueFilter };
        this.clients.set(clientId, client);

        // Retry interval + connected confirmation in a single chunk
        controller.enqueue(
          textEncoder.encode(
            `retry: ${RETRY_MS}\ndata: {"connected":true,"clientId":"${clientId}"}\n\n`
          )
        );

        // Replay missed events on reconnection
        if (resumeId > 0) {
          this.replayEvents(client, resumeId);
        }
      },
      cancel: () => {
        this.clients.delete(clientId);
        this.queueManager?.unregisterWorkersByClientId(clientId);
        this.queueManager?.releaseClientJobs(clientId).catch(() => {});
      },
    });

    return new Response(stream, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
        'Access-Control-Allow-Origin': corsOrigin,
      },
    });
  }

  /** Close all connections and stop all timers */
  closeAll(): void {
    this.stopTimers();
    for (const [, client] of this.clients) {
      try {
        client.controller.close();
      } catch {
        /* ignore */
      }
    }
    this.clients.clear();
  }

  /** Get clients map (for backward compatibility) */
  getClients(): Map<string, SseClient> {
    return this.clients;
  }
}
