/**
 * HTTP Endpoints - Health, diagnostics, and stats endpoints
 */

import type { QueueManager } from '../../application/queueManager';
import { VERSION } from '../../shared/version';
import { throughputTracker } from '../../application/throughputTracker';
import { latencyTracker } from '../../application/latencyTracker';
import { pausedView } from '../../shared/pausedView';

/** JSON response helper */
export function jsonResponse(data: unknown, status = 200, corsOrigins?: Set<string>): Response {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  };

  if (corsOrigins) {
    headers['Access-Control-Allow-Origin'] = corsOrigins.has('*')
      ? '*'
      : Array.from(corsOrigins).join(', ');
  }

  return new Response(JSON.stringify(data), { status, headers });
}

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
  sseCount: number
): Response {
  const stats = queueManager.getStats();
  const uptime = process.uptime();
  const memoryUsage = process.memoryUsage();
  const storageStatus = queueManager.getStorageStatus();
  const isHealthy = !storageStatus.diskFull;

  return jsonResponse({
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
      tcp: 0,
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
  });
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

/** Dashboard overview endpoint - aggregates all dashboard data in a single call */
export function dashboardOverviewEndpoint(
  queueManager: QueueManager,
  corsOrigins?: Set<string>
): Response {
  const stats = queueManager.getStats();
  const rates = throughputTracker.getRates();
  const latencies = latencyTracker.getPercentiles();
  const avgLatencies = latencyTracker.getAverages();
  const memStats = queueManager.getMemoryStats();
  const workers = queueManager.workerManager.list();
  const workerStats = queueManager.workerManager.getStats();
  const crons = queueManager.listCrons();
  const storage = queueManager.getStorageStatus();
  const mem = process.memoryUsage();

  return jsonResponse(
    {
      ok: true,
      stats: {
        waiting: stats.waiting,
        active: stats.active,
        delayed: stats.delayed,
        completed: stats.completed,
        dlq: stats.dlq,
        totalPushed: Number(stats.totalPushed),
        totalPulled: Number(stats.totalPulled),
        totalCompleted: Number(stats.totalCompleted),
        totalFailed: Number(stats.totalFailed),
        uptime: stats.uptime,
      },
      throughput: rates,
      latency: {
        averages: avgLatencies,
        percentiles: latencies,
      },
      memory: {
        heapUsed: Math.round(mem.heapUsed / 1024 / 1024),
        heapTotal: Math.round(mem.heapTotal / 1024 / 1024),
        rss: Math.round(mem.rss / 1024 / 1024),
      },
      collections: memStats,
      workers: {
        total: workerStats.total,
        active: workerStats.active,
        list: workers
          .slice(0, 100)
          .map(
            (w: {
              id: string;
              name: string;
              queues: string[];
              lastSeen: number;
              activeJobs: number;
              processedJobs: number;
              failedJobs: number;
            }) => ({
              id: w.id,
              name: w.name,
              queues: w.queues,
              lastSeen: w.lastSeen,
              activeJobs: w.activeJobs,
              processedJobs: w.processedJobs,
              failedJobs: w.failedJobs,
            })
          ),
        truncated: workers.length > 100,
      },
      crons: {
        total: crons.length,
        list: crons
          .slice(0, 100)
          .map(
            (c: {
              name: string;
              queue: string;
              schedule: string | null;
              repeatEvery: number | null;
              nextRun: number;
              executions: number;
            }) => ({
              name: c.name,
              queue: c.queue,
              schedule: c.schedule ?? null,
              repeatEvery: c.repeatEvery ?? null,
              nextRun: c.nextRun,
              executions: c.executions,
            })
          ),
        truncated: crons.length > 100,
      },
      storage,
      timestamp: Date.now(),
    },
    200,
    corsOrigins
  );
}

/** Dashboard queues endpoint - paginated queues with per-queue stats */
export function dashboardQueuesEndpoint(
  queueManager: QueueManager,
  limit: number,
  offset: number,
  corsOrigins?: Set<string>
): Response {
  const allQueues = queueManager.listQueues();
  const total = allQueues.length;
  const queueNames = allQueues.slice(offset, offset + limit);
  const perQueueStats = queueManager.getPerQueueStats();

  const queues = queueNames.map((name: string) => {
    const stats = perQueueStats.get(name);
    return {
      name,
      waiting: stats?.waiting ?? 0,
      delayed: stats?.delayed ?? 0,
      active: stats?.active ?? 0,
      dlq: stats?.dlq ?? 0,
      paused: queueManager.isPaused(name),
    };
  });

  return jsonResponse(
    { ok: true, queues, total, limit, offset, timestamp: Date.now() },
    200,
    corsOrigins
  );
}

/** Dashboard single queue detail endpoint */
export function dashboardQueueDetailEndpoint(
  queueManager: QueueManager,
  queue: string,
  includeJobs: boolean,
  corsOrigins?: Set<string>
): Response {
  const rawCounts = queueManager.getQueueJobCounts(queue);
  const paused = queueManager.isPaused(queue);
  // Keep counts consistent with the per-state lists below (#92): a paused queue
  // reports ready jobs under `paused`, not `waiting`/`prioritized`.
  const pv = pausedView(rawCounts.waiting, rawCounts.prioritized, paused);
  const counts = {
    ...rawCounts,
    waiting: pv.waiting,
    prioritized: pv.prioritized,
    paused: pv.paused,
  };
  const dlqJobs = queueManager.getDlq(queue, 10);
  const priorityCounts = queueManager.getCountsPerPriority(queue);

  const result: Record<string, unknown> = {
    ok: true,
    name: queue,
    counts,
    paused,
    priorityCounts,
    dlqPreview: dlqJobs.map(
      (j: { id: string; data: unknown; attempts: number; createdAt: number }) => ({
        id: j.id,
        data: j.data,
        attempts: j.attempts,
        createdAt: j.createdAt,
      })
    ),
    timestamp: Date.now(),
  };

  if (includeJobs) {
    // When paused, ready jobs surface under `paused` (waiting is empty) — match
    // the counts above (#92).
    const waiting = queueManager.getJobs(queue, { state: 'waiting', end: 10 });
    const active = queueManager.getJobs(queue, { state: 'active', end: 10 });
    const delayed = queueManager.getJobs(queue, { state: 'delayed', end: 10 });
    const pausedJobs = paused ? queueManager.getJobs(queue, { state: 'paused', end: 10 }) : [];
    const toSummary = (j: {
      id: string;
      priority: number;
      createdAt: number;
      runAt: number;
      attempts: number;
      progress: number;
    }) => ({
      id: j.id,
      priority: j.priority,
      createdAt: j.createdAt,
      runAt: j.runAt,
      attempts: j.attempts,
      progress: j.progress,
    });
    result.jobs = {
      waiting: waiting.map(toSummary),
      active: active.map(toSummary),
      delayed: delayed.map(toSummary),
      paused: pausedJobs.map(toSummary),
    };
  }

  return jsonResponse(result, 200, corsOrigins);
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
