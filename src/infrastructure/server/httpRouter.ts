/**
 * Authenticated HTTP route dispatcher.
 */

import type { HandlerContext } from './types';
import { validateQueueName } from './protocol';
import {
  dashboardOverviewEndpoint,
  dashboardQueueDetailEndpoint,
  dashboardQueuesEndpoint,
  jsonResponse,
  metricsEndpoint,
  statsEndpoint,
} from './httpEndpoints';
import { routeJobRoutes } from './httpRouteJobs';
import { routeQueueConfigRoutes } from './httpRouteQueueConfig';
import { routeQueueRoutes } from './httpRouteQueues';
import { routeResourceRoutes } from './httpRouteResources';

const RE_DASHBOARD_QUEUE_DETAIL = /^\/dashboard\/queues\/([^/]+)$/;

/** Route an authenticated HTTP request to its handler. */
export async function routeHttpRequest(
  req: Request,
  path: string,
  ctx: HandlerContext,
  corsOrigins: Set<string>
): Promise<Response> {
  const method = req.method;

  if (path === '/stats' && method === 'GET') {
    return statsEndpoint(ctx.queueManager, corsOrigins);
  }
  if (path === '/metrics' && method === 'GET') {
    return metricsEndpoint(ctx.queueManager, corsOrigins);
  }
  if (path === '/dashboard' && method === 'GET') {
    return dashboardOverviewEndpoint(ctx.queueManager, corsOrigins);
  }
  if (path === '/dashboard/queues' && method === 'GET') {
    const url = new URL(req.url);
    const limit = Math.min(
      Math.max(parseInt(url.searchParams.get('limit') ?? '100', 10) || 100, 1),
      500
    );
    const offset = Math.max(parseInt(url.searchParams.get('offset') ?? '0', 10) || 0, 0);
    return dashboardQueuesEndpoint(ctx.queueManager, limit, offset, corsOrigins);
  }

  const queueMatch = path.match(RE_DASHBOARD_QUEUE_DETAIL);
  if (queueMatch && method === 'GET') {
    const queue = decodeURIComponent(queueMatch[1]);
    const queueError = validateQueueName(queue);
    if (queueError) return jsonResponse({ ok: false, error: queueError }, 400, corsOrigins);
    const includeJobs = new URL(req.url).searchParams.get('includeJobs') === 'true';
    return dashboardQueueDetailEndpoint(ctx.queueManager, queue, includeJobs, corsOrigins);
  }

  const routers = [
    routeJobRoutes,
    routeQueueRoutes,
    routeQueueConfigRoutes,
    routeResourceRoutes,
  ] as const;
  for (const router of routers) {
    const result = await router(req, path, method, ctx, corsOrigins);
    if (result) return result;
  }

  return jsonResponse({ ok: false, error: 'Not found' }, 404, corsOrigins);
}
