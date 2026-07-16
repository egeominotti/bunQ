/**
 * WebSocket Handler - Pub/sub with dashboard events
 *
 * Format: { event: "job:completed", ts: 1710000000000, data: { queue, jobId, ... } }
 *
 * Subscribe:   { cmd: "Subscribe", events: ["job:*", "stats:snapshot", "queue:*"] }
 * Unsubscribe: { cmd: "Unsubscribe", events: ["job:*"] }
 * Wildcard:    "*" = all, "job:*" = all job events, "queue:*" = all queue events
 *
 * Periodic:    health:status (10s), stats:snapshot (5s)
 * On change:   queue:counts (emitted on every job state change)
 * Legacy:      clients that never Subscribe get all events in old format
 */

import type { ServerWebSocket } from 'bun';
import type { QueueManager } from '../../application/queueManager';
import type { JobEvent } from '../../domain/types/queue';
import { handleCommand, type HandlerContext } from './handler';
import { parseCommand, serializeResponse, errorResponse } from './protocol';
import { throughputTracker } from '../../application/throughputTracker';
import { QueueCountsScheduler } from './queueCountsScheduler';

const textDecoder = new TextDecoder();

/** Old eventType → new event name */
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

/** All valid subscription patterns (events + wildcards) */
const VALID_PATTERNS = new Set([
  '*',
  // Job lifecycle (22)
  'job:pushed',
  'job:active',
  'job:completed',
  'job:failed',
  'job:removed',
  'job:promoted',
  'job:progress',
  'job:delayed',
  'job:stalled',
  'job:retried',
  'job:discarded',
  'job:priority-changed',
  'job:data-updated',
  'job:delay-changed',
  'job:timeout',
  'job:lock-expired',
  'job:deduplicated',
  'job:waiting-children',
  'job:dependencies-resolved',
  'job:moved-to-delayed',
  'job:expired',
  'job:*',
  // Queue (9)
  'queue:created',
  'queue:removed',
  'queue:paused',
  'queue:resumed',
  'queue:drained',
  'queue:cleaned',
  'queue:obliterated',
  'queue:counts',
  'queue:idle',
  'queue:threshold',
  'queue:*',
  // DLQ (6)
  'dlq:added',
  'dlq:retried',
  'dlq:retry-all',
  'dlq:purged',
  'dlq:auto-retried',
  'dlq:expired',
  'dlq:*',
  // Flow (2)
  'flow:completed',
  'flow:failed',
  'flow:*',
  // Cron (6)
  'cron:created',
  'cron:updated',
  'cron:deleted',
  'cron:fired',
  'cron:missed',
  'cron:skipped',
  'cron:*',
  // Worker (7)
  'worker:connected',
  'worker:disconnected',
  'worker:heartbeat',
  'worker:idle',
  'worker:removed-stale',
  'worker:overloaded',
  'worker:error',
  'worker:*',
  // Rate limit & concurrency (7)
  'ratelimit:set',
  'ratelimit:cleared',
  'ratelimit:hit',
  'ratelimit:rejected',
  'concurrency:set',
  'concurrency:cleared',
  'concurrency:rejected',
  'ratelimit:*',
  'concurrency:*',
  // Webhook (6)
  'webhook:added',
  'webhook:removed',
  'webhook:fired',
  'webhook:failed',
  'webhook:enabled',
  'webhook:disabled',
  'webhook:*',
  // Batch (2)
  'batch:pushed',
  'batch:pulled',
  'batch:*',
  // Client (2)
  'client:connected',
  'client:disconnected',
  'client:*',
  // Auth (1)
  'auth:failed',
  'auth:*',
  // Cleanup (2)
  'cleanup:orphans-removed',
  'cleanup:stale-deps-removed',
  'cleanup:*',
  // System (10)
  'health:status',
  'health:*',
  'stats:snapshot',
  'stats:*',
  'storage:status',
  'storage:backup-started',
  'storage:backup-completed',
  'storage:backup-failed',
  'storage:size-warning',
  'storage:*',
  'server:started',
  'server:shutdown',
  'server:recovered',
  'server:memory-warning',
  'server:*',
  // Memory (1)
  'memory:compacted',
  'memory:*',
  // Config (2)
  'config:stall-changed',
  'config:dlq-changed',
  'config:*',
]);

/** WebSocket client data */
export interface WsData {
  id: string;
  authenticated: boolean;
  queueFilter: string | null;
  /** Subscribed patterns. Null = legacy mode */
  subscriptions: Set<string> | null;
}

/** Check if event matches any subscription pattern */
function matches(event: string, subs: Set<string>): boolean {
  if (subs.has('*') || subs.has(event)) return true;
  const prefix = event.split(':')[0];
  return subs.has(`${prefix}:*`);
}

/** Max concurrent WebSocket connections */
const MAX_WS_CLIENTS = 1000;
/** Backpressure threshold — skip sends when buffered data exceeds 1MB */
const BACKPRESSURE_BYTES = 1024 * 1024;

export class WsHandler {
  private readonly clients = new Map<string, ServerWebSocket<WsData>>();
  private statsInterval: ReturnType<typeof setInterval> | null = null;
  private healthInterval: ReturnType<typeof setInterval> | null = null;
  private storageInterval: ReturnType<typeof setInterval> | null = null;
  private queueCountsScheduler: QueueCountsScheduler | null = null;
  droppedMessages = 0;

  get size(): number {
    return this.clients.size;
  }

  /** Check if a new connection can be accepted */
  canAccept(): boolean {
    return this.clients.size < MAX_WS_CLIENTS;
  }

  /** Send with backpressure detection — returns false if client is dead */
  private safeSend(ws: ServerWebSocket<WsData>, data: string): boolean {
    try {
      const buffered = typeof ws.getBufferedAmount === 'function' ? ws.getBufferedAmount() : 0;
      if (buffered > BACKPRESSURE_BYTES) {
        this.droppedMessages++;
        return true; // alive but slow — skip this message, don't disconnect
      }
      ws.send(data);
      return true;
    } catch {
      return false; // dead connection
    }
  }

  /** Start periodic broadcasts */
  startBroadcasts(qm: QueueManager): void {
    this.queueCountsScheduler ??= new QueueCountsScheduler(qm, (queue, counts) => {
      this.emit('queue:counts', {
        queue,
        waiting: counts.waiting,
        prioritized: counts.prioritized,
        active: counts.active,
        completed: counts.completed,
        failed: counts.failed,
        delayed: counts.delayed,
      });
    });

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

  /** Stop periodic broadcasts */
  stopBroadcasts(): void {
    this.queueCountsScheduler?.stop();
    this.queueCountsScheduler = null;
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

  // ── Periodic broadcasts ──────────────────────────────────

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

    this.emit('stats:snapshot', {
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

    this.emit('health:status', {
      ok: !storage.diskFull,
      uptime: Math.floor(process.uptime()),
      memory: {
        rss: Math.round(mem.rss / 1024 / 1024),
        heapUsed: Math.round(mem.heapUsed / 1024 / 1024),
      },
      connections: { ws: this.clients.size },
    });
  }

  private broadcastStorage(qm: QueueManager): void {
    const memStats = qm.getMemoryStats();
    const storage = qm.getStorageStatus();

    this.emit('storage:status', {
      collections: memStats,
      diskFull: storage.diskFull,
    });
  }

  // ── Event broadcasting ───────────────────────────────────

  /** Emit a dashboard event to subscribed clients */
  emitEvent(event: string, data: Record<string, unknown>): void {
    this.emit(event, data);
  }

  /** Core emit: sends { event, ts, data } to matching clients */
  private emit(event: string, data: Record<string, unknown>): void {
    if (this.clients.size === 0) return;

    let msg: string | null = null;
    const dead: string[] = [];

    for (const [id, ws] of this.clients) {
      if (ws.data.subscriptions && matches(event, ws.data.subscriptions)) {
        msg ??= JSON.stringify({ event, ts: Date.now(), data });
        if (!this.safeSend(ws, msg)) dead.push(id);
      }
    }

    for (const id of dead) this.clients.delete(id);
  }

  /** Broadcast job event (from eventsManager) + emit queue:counts */
  broadcast(event: JobEvent): void {
    if (this.clients.size === 0) return;

    const dashEvent = EVENT_MAP[event.eventType] ?? `job:${event.eventType}`;

    // Build new-format data
    const eventData: Record<string, unknown> = {
      queue: event.queue,
      jobId: event.jobId,
    };
    if (event.error) eventData.error = event.error;
    if (event.progress !== undefined) eventData.progress = event.progress;
    if (event.prev) eventData.prev = event.prev;
    if (event.delay !== undefined) eventData.delay = event.delay;

    let newMsg: string | null = null;
    let legacyMsg: string | null = null;
    const dead: string[] = [];

    for (const [id, ws] of this.clients) {
      if (ws.data.queueFilter && ws.data.queueFilter !== event.queue) continue;

      if (ws.data.subscriptions === null) {
        legacyMsg ??= JSON.stringify(event);
        if (!this.safeSend(ws, legacyMsg)) dead.push(id);
      } else if (matches(dashEvent, ws.data.subscriptions)) {
        newMsg ??= JSON.stringify({ event: dashEvent, ts: event.timestamp, data: eventData });
        if (!this.safeSend(ws, newMsg)) dead.push(id);
      }
    }

    for (const id of dead) this.clients.delete(id);

    // Counts are eventually exact, but coalesced so a PUSHB does not perform
    // one full queue scan per inserted job.
    this.queueCountsScheduler?.schedule(event.queue);
  }

  // ── Connection lifecycle ─────────────────────────────────

  onOpen(ws: ServerWebSocket<WsData>): void {
    this.clients.set(ws.data.id, ws);
  }

  onClose(ws: ServerWebSocket<WsData>): void {
    this.clients.delete(ws.data.id);
  }

  // ── Message handling ─────────────────────────────────────

  async onMessage(
    ws: ServerWebSocket<WsData>,
    message: string | Buffer,
    ctx: HandlerContext
  ): Promise<void> {
    const text = typeof message === 'string' ? message : textDecoder.decode(message);

    // Check for WS-only commands before typed parsing
    try {
      const raw = JSON.parse(text) as Record<string, unknown>;
      if (raw['cmd'] === 'Subscribe') {
        this.handleSubscribe(ws, raw);
        return;
      }
      if (raw['cmd'] === 'Unsubscribe') {
        this.handleUnsubscribe(ws, raw);
        return;
      }
    } catch {
      ws.send(errorResponse('Invalid JSON'));
      return;
    }

    const cmd = parseCommand(text);
    if (!cmd) {
      ws.send(errorResponse('Invalid command'));
      return;
    }

    try {
      const response = await handleCommand(cmd, ctx);
      if (cmd.cmd === 'Auth' && response.ok) ws.data.authenticated = true;
      ws.send(serializeResponse(response));
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      ws.send(errorResponse(msg, cmd.reqId));
    }
  }

  private handleSubscribe(
    ws: ServerWebSocket<WsData>,
    cmd: { events?: string[]; reqId?: string }
  ): void {
    const events = cmd.events;
    if (!Array.isArray(events) || events.length === 0) {
      ws.send(errorResponse('events must be a non-empty array', cmd.reqId));
      return;
    }
    const invalid = events.filter((e) => !VALID_PATTERNS.has(e));
    if (invalid.length > 0) {
      ws.send(errorResponse(`Invalid: ${invalid.join(', ')}`, cmd.reqId));
      return;
    }
    ws.data.subscriptions ??= new Set();
    for (const e of events) ws.data.subscriptions.add(e);
    ws.send(JSON.stringify({ ok: true, subscribed: [...ws.data.subscriptions], reqId: cmd.reqId }));
  }

  private handleUnsubscribe(
    ws: ServerWebSocket<WsData>,
    cmd: { events?: string[]; reqId?: string }
  ): void {
    if (!ws.data.subscriptions) {
      ws.send(JSON.stringify({ ok: true, subscribed: [], reqId: cmd.reqId }));
      return;
    }
    const events = cmd.events;
    if (!Array.isArray(events) || events.length === 0) {
      ws.data.subscriptions.clear();
    } else {
      for (const e of events) ws.data.subscriptions.delete(e);
    }
    ws.send(JSON.stringify({ ok: true, subscribed: [...ws.data.subscriptions], reqId: cmd.reqId }));
  }

  getClients(): Map<string, ServerWebSocket<WsData>> {
    return this.clients;
  }
}
