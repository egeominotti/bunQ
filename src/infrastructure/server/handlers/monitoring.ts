/**
 * Monitoring Handlers
 * Handles logs, workers, webhooks, and prometheus metrics
 */

import { jobId } from '../../../domain/types/job';
import type {
  AddLogCommand,
  GetLogsCommand,
  HeartbeatCommand,
  JobHeartbeatCommand,
  JobHeartbeatBatchCommand,
  PingCommand,
  HelloCommand,
  RegisterWorkerCommand,
  UnregisterWorkerCommand,
  ListWorkersCommand,
  AddWebhookCommand,
  RemoveWebhookCommand,
  ListWebhooksCommand,
  PrometheusCommand,
  ClearLogsCommand,
  ExtendLockCommand,
  ExtendLocksCommand,
  SetWebhookEnabledCommand,
  CompactMemoryCommand,
} from '../../../domain/types/command';
import { VERSION } from '../../../shared/version';
import type { Response } from '../../../domain/types/response';
import * as resp from '../../../domain/types/response';
import type { HandlerContext } from '../types';
import { validateWebhookUrl } from '../protocol';

// ============ Job Logs ============

export function handleAddLog(cmd: AddLogCommand, ctx: HandlerContext, reqId?: string): Response {
  const jid = jobId(cmd.id);
  const level = cmd.level ?? 'info';
  const success = ctx.queueManager.addLog(jid, cmd.message, level);

  if (success) {
    return resp.data({ added: true }, reqId);
  }
  return resp.error('Job not found', reqId);
}

export function handleGetLogs(cmd: GetLogsCommand, ctx: HandlerContext, reqId?: string): Response {
  const jid = jobId(cmd.id);
  const all = ctx.queueManager.getLogs(jid);
  // Honor optional pagination (start/end inclusive) the client already sends.
  const total = all.length;
  const logs =
    cmd.start === undefined && cmd.end === undefined
      ? all
      : all.slice(cmd.start ?? 0, (cmd.end ?? total - 1) + 1);
  return resp.data({ logs, count: total }, reqId);
}

// ============ Worker Heartbeat ============

export function handleHeartbeat(
  cmd: HeartbeatCommand,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const stats =
    cmd.activeJobs !== undefined || cmd.processed !== undefined || cmd.failed !== undefined
      ? { activeJobs: cmd.activeJobs, processed: cmd.processed, failed: cmd.failed }
      : undefined;
  const success = ctx.queueManager.workerManager.heartbeat(cmd.id, stats);
  if (success) {
    ctx.queueManager.emitDashboardEvent('worker:heartbeat', { workerId: cmd.id });
    return resp.data({ ok: true }, reqId);
  }
  return resp.error('Worker not found', reqId);
}

// ============ Job Heartbeat (for stall detection & lock renewal) ============

export function handleJobHeartbeat(
  cmd: JobHeartbeatCommand,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const jid = jobId(cmd.id);

  // If duration is specified, extend lock with that duration
  if (cmd.duration && cmd.token) {
    const success = ctx.queueManager.renewJobLock(jid, cmd.token, cmd.duration);
    if (success) return resp.data({ ok: true }, reqId);
    return resp.error('Job not found or not active (or invalid token)', reqId);
  }

  // Standard heartbeat
  const success = ctx.queueManager.jobHeartbeat(jid, cmd.token);
  if (success) {
    return resp.data({ ok: true }, reqId);
  }
  return resp.error('Job not found or not active (or invalid token)', reqId);
}

export function handleJobHeartbeatBatch(
  cmd: JobHeartbeatBatchCommand,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const ids = cmd.ids.map((id) => jobId(id));
  // Pass tokens to renew locks if provided
  const count = ctx.queueManager.jobHeartbeatBatch(ids, cmd.tokens);
  return resp.data({ ok: true, count }, reqId);
}

// ============ Ping (health check) ============

export function handlePing(_cmd: PingCommand, _ctx: HandlerContext, reqId?: string): Response {
  return resp.data({ pong: true, time: Date.now() }, reqId);
}

// ============ Hello (protocol negotiation) ============

/** Current protocol version */
export const PROTOCOL_VERSION = 2;

/** Supported capabilities */
export const SUPPORTED_CAPABILITIES: 'pipelining'[] = ['pipelining'];

export function handleHello(_cmd: HelloCommand, _ctx: HandlerContext, reqId?: string): Response {
  return resp.hello(PROTOCOL_VERSION, SUPPORTED_CAPABILITIES, 'bunqueue', VERSION, reqId);
}

// ============ Worker Management ============

export function handleRegisterWorker(
  cmd: RegisterWorkerCommand,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const worker = ctx.queueManager.registerWorker(cmd.name, cmd.queues, cmd.concurrency, {
    workerId: cmd.workerId,
    hostname: cmd.hostname,
    pid: cmd.pid,
    startedAt: cmd.startedAt,
    clientId: ctx.clientId,
  });
  return resp.data(
    {
      workerId: worker.id,
      name: worker.name,
      queues: worker.queues,
      concurrency: worker.concurrency,
      hostname: worker.hostname,
      pid: worker.pid,
      status: 'active' as const,
      registeredAt: worker.registeredAt,
      lastSeen: worker.lastSeen,
      activeJobs: worker.activeJobs,
      processedJobs: worker.processedJobs,
      failedJobs: worker.failedJobs,
      currentJob: worker.currentJob,
    },
    reqId
  );
}

export function handleUnregisterWorker(
  cmd: UnregisterWorkerCommand,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const success = ctx.queueManager.unregisterWorker(cmd.workerId);
  if (success) {
    return resp.data({ removed: true }, reqId);
  }
  return resp.error('Worker not found', reqId);
}

/** Worker timeout for status computation */
const WORKER_TIMEOUT_MS = parseInt(Bun.env.WORKER_TIMEOUT_MS ?? '30000', 10);

/** Compute worker status from lastSeen timestamp */
function computeWorkerStatus(lastSeen: number, now: number): 'active' | 'stale' {
  return now - lastSeen < WORKER_TIMEOUT_MS ? 'active' : 'stale';
}

export function handleListWorkers(
  _cmd: ListWorkersCommand,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const workers = ctx.queueManager.workerManager.list();
  const now = Date.now();
  return resp.data(
    {
      workers: workers.map((w) => ({
        id: w.id,
        name: w.name,
        queues: w.queues,
        concurrency: w.concurrency,
        hostname: w.hostname,
        pid: w.pid,
        status: computeWorkerStatus(w.lastSeen, now),
        registeredAt: w.registeredAt,
        lastSeen: w.lastSeen,
        activeJobs: w.activeJobs,
        processedJobs: w.processedJobs,
        failedJobs: w.failedJobs,
        currentJob: w.currentJob,
        uptime: now - w.registeredAt,
      })),
      stats: ctx.queueManager.workerManager.getStats(),
    },
    reqId
  );
}

// ============ Webhooks ============

export function handleAddWebhook(
  cmd: AddWebhookCommand,
  ctx: HandlerContext,
  reqId?: string
): Response {
  // Validate webhook URL to prevent SSRF
  const urlError = validateWebhookUrl(cmd.url);
  if (urlError) return resp.error(urlError, reqId);

  const webhook = ctx.queueManager.webhookManager.add(cmd.url, cmd.events, cmd.queue, cmd.secret);
  ctx.queueManager.emitDashboardEvent('webhook:added', {
    id: webhook.id,
    url: webhook.url,
    events: webhook.events,
  });
  return resp.data(
    {
      webhookId: webhook.id,
      url: webhook.url,
      events: webhook.events,
      queue: webhook.queue,
      createdAt: webhook.createdAt,
    },
    reqId
  );
}

export function handleRemoveWebhook(
  cmd: RemoveWebhookCommand,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const success = ctx.queueManager.webhookManager.remove(cmd.webhookId);
  if (success) {
    ctx.queueManager.emitDashboardEvent('webhook:removed', { id: cmd.webhookId });
    return resp.data({ removed: true }, reqId);
  }
  return resp.error('Webhook not found', reqId);
}

export function handleListWebhooks(
  _cmd: ListWebhooksCommand,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const webhooks = ctx.queueManager.webhookManager.list();
  return resp.data(
    {
      webhooks: webhooks.map((w) => ({
        id: w.id,
        url: w.url,
        events: w.events,
        queue: w.queue,
        createdAt: w.createdAt,
        lastTriggered: w.lastTriggered,
        successCount: w.successCount,
        failureCount: w.failureCount,
        enabled: w.enabled,
      })),
      stats: ctx.queueManager.webhookManager.getStats(),
    },
    reqId
  );
}

// ============ Prometheus Metrics ============

export function handlePrometheus(
  _cmd: PrometheusCommand,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const metrics = ctx.queueManager.getPrometheusMetrics();
  return resp.data({ metrics }, reqId);
}

// ============ Clear Logs ============

export function handleClearLogs(
  cmd: ClearLogsCommand,
  ctx: HandlerContext,
  reqId?: string
): Response {
  ctx.queueManager.clearLogs(jobId(cmd.id), cmd.keepLogs);
  return resp.ok(undefined, reqId);
}

// ============ Extend Lock ============

export async function handleExtendLock(
  cmd: ExtendLockCommand,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  const success = await ctx.queueManager.extendLock(jobId(cmd.id), cmd.token ?? null, cmd.duration);
  return success ? resp.ok(undefined, reqId) : resp.error('Lock not found or invalid token', reqId);
}

export async function handleExtendLocks(
  cmd: ExtendLocksCommand,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  let count = 0;
  for (let i = 0; i < cmd.ids.length; i++) {
    const success = await ctx.queueManager.extendLock(
      jobId(cmd.ids[i]),
      cmd.tokens[i] ?? null,
      cmd.durations[i]
    );
    if (success) count++;
  }
  return { ok: true, count, reqId };
}

// ============ Set Webhook Enabled ============

export function handleSetWebhookEnabled(
  cmd: SetWebhookEnabledCommand,
  ctx: HandlerContext,
  reqId?: string
): Response {
  const success = ctx.queueManager.webhookManager.setEnabled(cmd.id, cmd.enabled);
  return success ? resp.ok(undefined, reqId) : resp.error('Webhook not found', reqId);
}

// ============ Compact Memory ============

export function handleCompactMemory(
  _cmd: CompactMemoryCommand,
  ctx: HandlerContext,
  reqId?: string
): Response {
  ctx.queueManager.compactMemory();
  return resp.ok(undefined, reqId);
}
