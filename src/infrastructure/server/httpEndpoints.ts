/**
 * HTTP Endpoints - Health, diagnostics, and stats endpoints
 */

import type { QueueManager } from '../../application/queueManager';
import { VERSION } from '../../shared/version';
import { throughputTracker } from '../../application/throughputTracker';
import { jsonResponse } from './httpResponse';

export {
  dashboardOverviewEndpoint,
  dashboardQueueDetailEndpoint,
  dashboardQueuesEndpoint,
} from './httpDashboardEndpoints';
export { jsonResponse } from './httpResponse';

/** Parse JSON body from request. Returns parsed object or 400 Response on invalid JSON.
 *  Empty/missing body returns {} for backward compatibility with optional-body routes. */
export async function parseJsonBody(
  req: Request,
  cors: Set<string>
): Promise<Record<string, unknown> | Response> {
  try {
    const text = await req.text();
    if (!text || text.trim() === '') return {};
    return JSON.parse(text) as Record<string, unknown>;
  } catch {
    return jsonResponse({ ok: false, error: 'Invalid JSON body' }, 400, cors);
  }
}

/** CORS preflight response */
export function corsResponse(corsOrigins: Set<string>): Response {
  return new Response(null, {
    status: 204,
    headers: {
      'Access-Control-Allow-Origin': corsOrigins.has('*')
        ? '*'
        : Array.from(corsOrigins).join(', '),
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      'Access-Control-Max-Age': '86400',
    },
  });
}

/** Health check endpoint */
export function healthEndpoint(
  queueManager: QueueManager,
  wsCount: number,
  sseCount: number,
  tcpCount = 0
): Response {
  const stats = queueManager.getStats();
  const uptime = process.uptime();
  const memoryUsage = process.memoryUsage();
  const storageStatus = queueManager.getStorageStatus();
  const isHealthy = !storageStatus.diskFull;

  return jsonResponse(
    {
      ok: isHealthy,
      status: isHealthy ? 'healthy' : 'degraded',
      uptime: Math.floor(uptime),
      version: VERSION,
      queues: {
        waiting: stats.waiting,
        active: stats.active,
        delayed: stats.delayed,
        completed: stats.completed,
        dlq: stats.dlq,
      },
      connections: {
        tcp: tcpCount,
        ws: wsCount,
        sse: sseCount,
      },
      memory: {
        heapUsed: Math.round(memoryUsage.heapUsed / 1024 / 1024),
        heapTotal: Math.round(memoryUsage.heapTotal / 1024 / 1024),
        rss: Math.round(memoryUsage.rss / 1024 / 1024),
      },
      ...(storageStatus.diskFull && {
        storage: {
          diskFull: true,
          error: storageStatus.error,
          since: storageStatus.since,
        },
      }),
    },
    isHealthy ? 200 : 503
  );
}

/** Readiness check - persistence failures must stop new traffic. */
export function readinessEndpoint(queueManager: QueueManager, corsOrigins?: Set<string>): Response {
  const storage = queueManager.getStorageStatus();
  const ready = !storage.diskFull;
  return jsonResponse(
    {
      ok: ready,
      ready,
      ...(ready ? {} : { storage: { diskFull: true, error: storage.error, since: storage.since } }),
    },
    ready ? 200 : 503,
    corsOrigins
  );
}

/** GC endpoint - force garbage collection */
export function gcEndpoint(queueManager: QueueManager): Response {
  const before = process.memoryUsage();
  if (typeof Bun !== 'undefined' && Bun.gc) {
    Bun.gc(true);
  }
  queueManager.compactMemory();
  const after = process.memoryUsage();

  return jsonResponse({
    ok: true,
    before: {
      heapUsed: Math.round(before.heapUsed / 1024 / 1024),
      heapTotal: Math.round(before.heapTotal / 1024 / 1024),
      rss: Math.round(before.rss / 1024 / 1024),
    },
    after: {
      heapUsed: Math.round(after.heapUsed / 1024 / 1024),
      heapTotal: Math.round(after.heapTotal / 1024 / 1024),
      rss: Math.round(after.rss / 1024 / 1024),
    },
  });
}

/** Heap stats endpoint - for debugging memory leaks */
export async function heapStatsEndpoint(queueManager: QueueManager): Promise<Response> {
  if (typeof Bun !== 'undefined' && Bun.gc) {
    Bun.gc(true);
  }

  const { heapStats } = await import('bun:jsc');
  const stats = heapStats();
  const mem = process.memoryUsage();
  const memStats = queueManager.getMemoryStats();

  const typeCounts = stats.objectTypeCounts as Record<string, number> | undefined;
  const topTypes = typeCounts
    ? Object.entries(typeCounts)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 20)
        .map(([type, count]) => ({ type, count }))
    : [];

  return jsonResponse({
    ok: true,
    memory: {
      heapUsed: Math.round(mem.heapUsed / 1024 / 1024),
      heapTotal: Math.round(mem.heapTotal / 1024 / 1024),
      rss: Math.round(mem.rss / 1024 / 1024),
    },
    heap: {
      objectCount: stats.objectCount,
      protectedCount: stats.protectedObjectCount,
      globalCount: stats.globalObjectCount,
    },
    collections: memStats,
    topObjectTypes: topTypes,
  });
}

/** Stats endpoint */
export function statsEndpoint(queueManager: QueueManager, corsOrigins?: Set<string>): Response {
  const stats = queueManager.getStats();
  const memStats = queueManager.getMemoryStats();
  const mem = process.memoryUsage();
  const rates = throughputTracker.getRates();

  return jsonResponse(
    {
      ok: true,
      stats: {
        ...stats,
        totalPushed: Number(stats.totalPushed),
        totalPulled: Number(stats.totalPulled),
        totalCompleted: Number(stats.totalCompleted),
        totalFailed: Number(stats.totalFailed),
        pushPerSec: rates.pushPerSec,
        pullPerSec: rates.pullPerSec,
        completePerSec: rates.completePerSec,
        failPerSec: rates.failPerSec,
      },
      memory: {
        heapUsed: Math.round(mem.heapUsed / 1024 / 1024),
        heapTotal: Math.round(mem.heapTotal / 1024 / 1024),
        rss: Math.round(mem.rss / 1024 / 1024),
        external: Math.round(mem.external / 1024 / 1024),
        arrayBuffers: Math.round(mem.arrayBuffers / 1024 / 1024),
      },
      collections: memStats,
    },
    200,
    corsOrigins
  );
}

/** Metrics endpoint */
export function metricsEndpoint(queueManager: QueueManager, corsOrigins?: Set<string>): Response {
  const stats = queueManager.getStats();

  return jsonResponse(
    {
      ok: true,
      metrics: {
        totalPushed: Number(stats.totalPushed),
        totalPulled: Number(stats.totalPulled),
        totalCompleted: Number(stats.totalCompleted),
        totalFailed: Number(stats.totalFailed),
      },
    },
    200,
    corsOrigins
  );
}
