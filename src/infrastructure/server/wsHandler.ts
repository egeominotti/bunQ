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
import { sanitizeServerError } from './errors';
import { parseCommand, serializeResponse, errorResponse } from './protocol';
import { QueueCountsScheduler } from './queueCountsScheduler';
import type { WsData } from './types/ws';
import {
  matchesWsSubscription,
  MAX_WS_CLIENTS,
  textDecoder,
  WS_BACKPRESSURE_BYTES,
  WS_EVENT_MAP,
} from './ws/constants';
import { buildHealthSnapshot, buildStatsSnapshot, buildStorageSnapshot } from './ws/snapshots';
import { handleWsSubscribe, handleWsUnsubscribe } from './ws/subscriptions';

export type { WsData } from './types/ws';

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
      if (buffered > WS_BACKPRESSURE_BYTES) {
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
      if (this.clients.size > 0) void this.broadcastStats(qm);
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

  private async broadcastStats(qm: QueueManager): Promise<void> {
    try {
      this.emit('stats:snapshot', await buildStatsSnapshot(qm));
    } catch {
      // A failed durable read must not be replaced by a stale local snapshot.
    }
  }

  private broadcastHealth(qm: QueueManager): void {
    this.emit('health:status', buildHealthSnapshot(qm, this.clients.size));
  }

  private broadcastStorage(qm: QueueManager): void {
    this.emit('storage:status', buildStorageSnapshot(qm));
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
      if (ws.data.subscriptions && matchesWsSubscription(event, ws.data.subscriptions)) {
        msg ??= JSON.stringify({ event, ts: Date.now(), data });
        if (!this.safeSend(ws, msg)) dead.push(id);
      }
    }

    for (const id of dead) this.clients.delete(id);
  }

  /** Broadcast job event (from eventsManager) + emit queue:counts */
  broadcast(event: JobEvent): void {
    if (this.clients.size === 0) return;

    const dashEvent = WS_EVENT_MAP[event.eventType] ?? `job:${event.eventType}`;

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
      } else if (matchesWsSubscription(dashEvent, ws.data.subscriptions)) {
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
        handleWsSubscribe(ws, raw);
        return;
      }
      if (raw['cmd'] === 'Unsubscribe') {
        handleWsUnsubscribe(ws, raw);
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
      ws.send(errorResponse(sanitizeServerError(err), cmd.reqId));
    }
  }

  getClients(): Map<string, ServerWebSocket<WsData>> {
    return this.clients;
  }
}
