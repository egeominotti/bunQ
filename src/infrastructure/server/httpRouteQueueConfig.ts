/**
 * HTTP Routes - Queue config, DLQ, rate-limiting, concurrency
 * All /queues/:queue/{dlq,rate-limit,concurrency,stall-config,dlq-config} endpoints
 */

import { handleCommand } from './handler';
import type { HandlerContext } from './types';
import { jsonResponse, parseJsonBody } from './httpEndpoints';

// Pre-compiled regex patterns for URL matching
const RE_QUEUE_DLQ_STATS = /^\/queues\/([^/]+)\/dlq\/stats$/;
const RE_QUEUE_DLQ = /^\/queues\/([^/]+)\/dlq$/;
const RE_QUEUE_DLQ_RETRY = /^\/queues\/([^/]+)\/dlq\/retry$/;
const RE_QUEUE_DLQ_PURGE = /^\/queues\/([^/]+)\/dlq\/purge$/;
const RE_QUEUE_RATE_LIMIT = /^\/queues\/([^/]+)\/rate-limit$/;
const RE_QUEUE_CONCURRENCY = /^\/queues\/([^/]+)\/concurrency$/;
const RE_QUEUE_STALL_CONFIG = /^\/queues\/([^/]+)\/stall-config$/;
const RE_QUEUE_DLQ_CONFIG = /^\/queues\/([^/]+)\/dlq-config$/;

/** Route queue config/admin requests. Returns Response or null if no match. */
export async function routeQueueConfigRoutes(
  req: Request,
  path: string,
  method: string,
  ctx: HandlerContext,
  cors: Set<string>
): Promise<Response | null> {
  // DLQ Stats: GET /queues/:queue/dlq/stats (must be checked before /dlq)
  const dlqStatsMatch = path.match(RE_QUEUE_DLQ_STATS);
  if (dlqStatsMatch && method === 'GET') {
    const queue = decodeURIComponent(dlqStatsMatch[1]);
    const stats = ctx.queueManager.getDlqStats(queue);
    return jsonResponse({ ok: true, stats }, 200, cors);
  }

  // DLQ: GET /queues/:queue/dlq - returns full DlqEntry objects with metadata
  const dlqMatch = path.match(RE_QUEUE_DLQ);
  if (dlqMatch && method === 'GET') {
    const queue = decodeURIComponent(dlqMatch[1]);
    const all = ctx.queueManager.getDlqEntries(queue);
    // Optional pagination so a dashboard can page large DLQs. Non-numeric params
    // are ignored (treated as absent) rather than producing an empty/garbage slice.
    const params = new URL(req.url).searchParams;
    const toInt = (v: string | null): number | undefined => {
      if (v === null) return undefined;
      const n = Number(v);
      return Number.isFinite(n) ? Math.trunc(n) : undefined;
    };
    const limit = toInt(params.get('limit'));
    const offset = toInt(params.get('offset'));
    const start = Math.max(0, offset ?? 0);
    const entries =
      limit === undefined && offset === undefined
        ? all
        : all.slice(start, start + (limit !== undefined ? Math.max(0, limit) : all.length));
    return jsonResponse({ ok: true, entries, total: all.length }, 200, cors);
  }

  // POST /queues/:queue/dlq/retry
  const dlqRetryMatch = path.match(RE_QUEUE_DLQ_RETRY);
  if (dlqRetryMatch && method === 'POST') {
    const queue = decodeURIComponent(dlqRetryMatch[1]);
    const body = await parseJsonBody(req, cors);
    if (body instanceof Response) return body;
    const r = await handleCommand(
      {
        cmd: 'RetryDlq',
        queue,
        jobId: body['jobId'] as string | undefined,
      },
      ctx
    );
    return jsonResponse(r, 200, cors);
  }

  // POST /queues/:queue/dlq/purge
  const dlqPurgeMatch = path.match(RE_QUEUE_DLQ_PURGE);
  if (dlqPurgeMatch && method === 'POST') {
    const queue = decodeURIComponent(dlqPurgeMatch[1]);
    const r = await handleCommand({ cmd: 'PurgeDlq', queue }, ctx);
    return jsonResponse(r, 200, cors);
  }

  // Rate limiting: PUT/DELETE /queues/:queue/rate-limit
  const rateLimitMatch = path.match(RE_QUEUE_RATE_LIMIT);
  if (rateLimitMatch && method === 'PUT') {
    const queue = decodeURIComponent(rateLimitMatch[1]);
    const body = await parseJsonBody(req, cors);
    if (body instanceof Response) return body;
    const r = await handleCommand(
      {
        cmd: 'RateLimit',
        queue,
        limit: body['limit'] as number,
      },
      ctx
    );
    return jsonResponse(r, 200, cors);
  }
  if (rateLimitMatch && method === 'DELETE') {
    const queue = decodeURIComponent(rateLimitMatch[1]);
    const r = await handleCommand({ cmd: 'RateLimitClear', queue }, ctx);
    return jsonResponse(r, 200, cors);
  }

  // Concurrency: PUT/DELETE /queues/:queue/concurrency
  const concurrencyMatch = path.match(RE_QUEUE_CONCURRENCY);
  if (concurrencyMatch && method === 'PUT') {
    const queue = decodeURIComponent(concurrencyMatch[1]);
    const body = await parseJsonBody(req, cors);
    if (body instanceof Response) return body;
    const r = await handleCommand(
      {
        cmd: 'SetConcurrency',
        queue,
        // Accept the natural `concurrency` field for this endpoint as well as `limit`.
        limit: (body['concurrency'] ?? body['limit']) as number,
      },
      ctx
    );
    return jsonResponse(r, 200, cors);
  }
  if (concurrencyMatch && method === 'DELETE') {
    const queue = decodeURIComponent(concurrencyMatch[1]);
    const r = await handleCommand({ cmd: 'ClearConcurrency', queue }, ctx);
    return jsonResponse(r, 200, cors);
  }

  // Config: GET/PUT /queues/:queue/stall-config
  const stallConfigMatch = path.match(RE_QUEUE_STALL_CONFIG);
  if (stallConfigMatch && method === 'GET') {
    const queue = decodeURIComponent(stallConfigMatch[1]);
    const r = await handleCommand({ cmd: 'GetStallConfig', queue }, ctx);
    return jsonResponse(r, 200, cors);
  }
  if (stallConfigMatch && method === 'PUT') {
    const queue = decodeURIComponent(stallConfigMatch[1]);
    const body = await parseJsonBody(req, cors);
    if (body instanceof Response) return body;
    const r = await handleCommand(
      {
        cmd: 'SetStallConfig',
        queue,
        config: body['config'] ?? body,
      } as Parameters<typeof handleCommand>[0],
      ctx
    );
    return jsonResponse(r, 200, cors);
  }

  // Config: GET/PUT /queues/:queue/dlq-config
  const dlqConfigMatch = path.match(RE_QUEUE_DLQ_CONFIG);
  if (dlqConfigMatch && method === 'GET') {
    const queue = decodeURIComponent(dlqConfigMatch[1]);
    const r = await handleCommand({ cmd: 'GetDlqConfig', queue }, ctx);
    return jsonResponse(r, 200, cors);
  }
  if (dlqConfigMatch && method === 'PUT') {
    const queue = decodeURIComponent(dlqConfigMatch[1]);
    const body = await parseJsonBody(req, cors);
    if (body instanceof Response) return body;
    const r = await handleCommand(
      {
        cmd: 'SetDlqConfig',
        queue,
        config: body['config'] ?? body,
      } as Parameters<typeof handleCommand>[0],
      ctx
    );
    return jsonResponse(r, 200, cors);
  }

  return null;
}
