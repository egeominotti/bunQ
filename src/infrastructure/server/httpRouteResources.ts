/**
 * HTTP Routes - Crons, Webhooks, Workers, Monitoring
 * All /crons/*, /webhooks/*, /workers/*, /ping endpoints
 */

import { handleCommand } from './handler';
import type { HandlerContext } from './types';
import { jsonResponse, parseJsonBody } from './httpEndpoints';
import type { CronJobOptions } from '../../domain/types/cron';

type Body = Record<string, unknown>;

// Pre-compiled regex patterns for URL matching
const RE_CRON_BY_NAME = /^\/crons\/([^/]+)$/;
const RE_WEBHOOK_BY_ID = /^\/webhooks\/([^/]+)$/;
const RE_WEBHOOK_ENABLED = /^\/webhooks\/([^/]+)\/enabled$/;
const RE_WORKER_BY_ID = /^\/workers\/([^/]+)$/;
const RE_WORKER_HEARTBEAT = /^\/workers\/([^/]+)\/heartbeat$/;

/** Route cron, webhook, worker, and monitoring requests. Returns Response or null. */
export async function routeResourceRoutes(
  req: Request,
  path: string,
  method: string,
  ctx: HandlerContext,
  cors: Set<string>
): Promise<Response | null> {
  // ── Cron ──────────────────────────────────────────────────

  // GET /crons - list all
  if (path === '/crons' && method === 'GET') {
    const r = await handleCommand({ cmd: 'CronList' }, ctx);
    return jsonResponse(r, 200, cors);
  }

  // POST /crons - add cron
  if (path === '/crons' && method === 'POST') {
    let body: Body;
    try {
      body = (await req.json()) as Body;
    } catch {
      return jsonResponse({ ok: false, error: 'Invalid JSON body' }, 400, cors);
    }
    const r = await handleCommand(
      {
        cmd: 'Cron',
        name: body['name'] as string,
        queue: body['queue'] as string,
        data: body['data'],
        schedule: body['schedule'] as string | undefined,
        repeatEvery: body['repeatEvery'] as number | undefined,
        priority: body['priority'] as number | undefined,
        maxLimit: body['maxLimit'] as number | undefined,
        timezone: body['timezone'] as string | undefined,
        uniqueKey: body['uniqueKey'] as string | undefined,
        dedup: body['dedup'] as { ttl?: number; extend?: boolean; replace?: boolean } | undefined,
        skipMissedOnRestart: body['skipMissedOnRestart'] as boolean | undefined,
        immediately: body['immediately'] as boolean | undefined,
        skipIfNoWorker: body['skipIfNoWorker'] as boolean | undefined,
        preventOverlap: body['preventOverlap'] as boolean | undefined,
        jobOptions: body['jobOptions'] as CronJobOptions | undefined,
      },
      ctx
    );
    return jsonResponse(r, r.ok ? 200 : 400, cors);
  }

  // GET /crons/:name
  const cronGetMatch = path.match(RE_CRON_BY_NAME);
  if (cronGetMatch && method === 'GET') {
    const name = decodeURIComponent(cronGetMatch[1]);
    const r = await handleCommand({ cmd: 'CronGet', name }, ctx);
    return jsonResponse(r, r.ok ? 200 : 404, cors);
  }

  // DELETE /crons/:name
  if (cronGetMatch && method === 'DELETE') {
    const name = decodeURIComponent(cronGetMatch[1]);
    const r = await handleCommand({ cmd: 'CronDelete', name }, ctx);
    return jsonResponse(r, r.ok ? 200 : 404, cors);
  }

  // ── Webhooks ──────────────────────────────────────────────

  // GET /webhooks
  if (path === '/webhooks' && method === 'GET') {
    const r = await handleCommand({ cmd: 'ListWebhooks' }, ctx);
    return jsonResponse(r, 200, cors);
  }

  // POST /webhooks
  if (path === '/webhooks' && method === 'POST') {
    let body: Body;
    try {
      body = (await req.json()) as Body;
    } catch {
      return jsonResponse({ ok: false, error: 'Invalid JSON body' }, 400, cors);
    }
    const r = await handleCommand(
      {
        cmd: 'AddWebhook',
        url: body['url'] as string,
        events: body['events'] as string[],
        queue: body['queue'] as string | undefined,
        secret: body['secret'] as string | undefined,
      },
      ctx
    );
    return jsonResponse(r, r.ok ? 200 : 400, cors);
  }

  // DELETE /webhooks/:id
  const webhookMatch = path.match(RE_WEBHOOK_BY_ID);
  if (webhookMatch && method === 'DELETE') {
    const r = await handleCommand(
      {
        cmd: 'RemoveWebhook',
        webhookId: webhookMatch[1],
      },
      ctx
    );
    return jsonResponse(r, r.ok ? 200 : 404, cors);
  }

  // PUT /webhooks/:id/enabled
  const webhookEnabledMatch = path.match(RE_WEBHOOK_ENABLED);
  if (webhookEnabledMatch && method === 'PUT') {
    const body = await parseJsonBody(req, cors);
    if (body instanceof Response) return body;
    const r = await handleCommand(
      {
        cmd: 'SetWebhookEnabled',
        id: webhookEnabledMatch[1],
        enabled: body['enabled'] as boolean,
      },
      ctx
    );
    return jsonResponse(r, r.ok ? 200 : 400, cors);
  }

  // ── Workers ───────────────────────────────────────────────

  // GET /workers
  if (path === '/workers' && method === 'GET') {
    const r = await handleCommand({ cmd: 'ListWorkers' }, ctx);
    return jsonResponse(r, 200, cors);
  }

  // POST /workers
  if (path === '/workers' && method === 'POST') {
    let body: Body;
    try {
      body = (await req.json()) as Body;
    } catch {
      return jsonResponse({ ok: false, error: 'Invalid JSON body' }, 400, cors);
    }
    const r = await handleCommand(
      {
        cmd: 'RegisterWorker',
        name: body['name'] as string,
        queues: body['queues'] as string[],
        concurrency: body['concurrency'] as number | undefined,
        workerId: body['workerId'] as string | undefined,
        hostname: body['hostname'] as string | undefined,
        pid: body['pid'] as number | undefined,
        startedAt: body['startedAt'] as number | undefined,
      },
      ctx
    );
    return jsonResponse(r, r.ok ? 200 : 400, cors);
  }

  // DELETE /workers/:id
  const workerMatch = path.match(RE_WORKER_BY_ID);
  if (workerMatch && method === 'DELETE') {
    const r = await handleCommand(
      {
        cmd: 'UnregisterWorker',
        workerId: workerMatch[1],
      },
      ctx
    );
    return jsonResponse(r, r.ok ? 200 : 404, cors);
  }

  // POST /workers/:id/heartbeat
  const workerHeartbeatMatch = path.match(RE_WORKER_HEARTBEAT);
  if (workerHeartbeatMatch && method === 'POST') {
    let body: Body = {};
    try {
      body = (await req.json()) as Body;
    } catch {
      // Body is optional for heartbeat
    }
    const r = await handleCommand(
      {
        cmd: 'Heartbeat',
        id: workerHeartbeatMatch[1],
        activeJobs: body['activeJobs'] as number | undefined,
        processed: body['processed'] as number | undefined,
        failed: body['failed'] as number | undefined,
      },
      ctx
    );
    return jsonResponse(r, 200, cors);
  }

  // ── Monitoring ────────────────────────────────────────────

  // GET /ping
  if (path === '/ping' && method === 'GET') {
    const r = await handleCommand({ cmd: 'Ping' }, ctx);
    return jsonResponse(r, 200, cors);
  }

  // GET /storage
  if (path === '/storage' && method === 'GET') {
    const r = await handleCommand({ cmd: 'StorageStatus' }, ctx);
    return jsonResponse(r, 200, cors);
  }

  return null;
}
