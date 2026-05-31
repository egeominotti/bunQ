/**
 * Snapshot Collector
 * Two-tier collection: light (every 15s) + heavy (every 90s)
 *
 * Light: stats, throughput, latency, memory, connections — O(SHARD_COUNT)
 * Heavy: recentJobs, dlqEntries, topErrors, workerDetails, queueConfigs, webhooks — O(queues × shards)
 */

import { hostname, arch, platform, cpus } from 'os';
import type { QueueManager } from '../../application/queueManager';
import { throughputTracker } from '../../application/throughputTracker';
import { latencyTracker } from '../../application/latencyTracker';
import { getTaskErrorStats } from '../../application/backgroundTasks';
import { VERSION } from '../../shared/version';
import type { CloudSnapshot } from './types';
import { cloudLog } from './logger';
import {
  collectLiveJobs,
  collectDlqEntries,
  collectTopErrors,
  collectQueueConfigs,
  collectWebhooks,
  collectQueueThroughput,
  collectWorkerUtilization,
  collectDurationHistogram,
  collectQueueWaitTime,
  collectQueueRetryRate,
  collectBacklogVelocity,
  collectStallDetails,
  collectPriorityDistribution,
} from './snapshotHelpers';

/** Cached hostname — computed once */
const HOST = hostname();

/** Cached runtime info — computed once */
const RUNTIME = {
  bunVersion: typeof Bun !== 'undefined' ? Bun.version : 'unknown',
  os: platform(),
  arch: arch(),
  cpus: cpus().length,
};

/** Optional server handles for connection stats + backup + storage */
export interface ServerHandles {
  getConnectionCount: () => number;
  getWsClientCount: () => number;
  getSseClientCount: () => number;
  getBackupStatus?: () => {
    enabled: boolean;
    bucket: string;
    endpoint: string;
    intervalMs: number;
    retention: number;
    isRunning: boolean;
  } | null;
  getSqliteStats?: () => { dbSizeBytes: number; writeBufferPending: number } | null;
  getMcpOperations?: () => {
    operations: Array<{
      tool: string;
      queue: string | null;
      timestamp: number;
      durationMs: number;
      success: boolean;
      error: string | null;
    }>;
    summary: {
      totalInvocations: number;
      successCount: number;
      failureCount: number;
      avgDurationMs: number;
      topTools: Array<{ tool: string; count: number }>;
    };
  };
}

/** Parameters for snapshot collection */
export interface CollectSnapshotParams {
  queueManager: QueueManager;
  instanceId: string;
  instanceName: string;
  startedAt: number;
  sequenceId: number;
  serverHandles?: ServerHandles;
  includeHeavy: boolean;
  /** Fields to redact from job data (mirrors CloudConfig.redactFields). Default: none */
  redactFields?: readonly string[];
  /** Include job data in recentJobs/dlqEntries (mirrors CloudConfig.includeJobData). Default: true */
  includeJobData?: boolean;
}

/** Enrich failed jobs in recentJobs with duration from DLQ attempt history */
function enrichFailedJobDurations(
  recentJobs: CloudSnapshot['recentJobs'],
  dlqEntries: CloudSnapshot['dlqEntries']
): void {
  if (dlqEntries.length === 0) return;
  const dlqMap = new Map<string, { duration: number; failedAt: number }>();
  for (const e of dlqEntries) {
    if (e.attemptHistory.length > 0) {
      const last = e.attemptHistory[e.attemptHistory.length - 1];
      dlqMap.set(e.jobId, { duration: last.duration, failedAt: last.failedAt });
    }
  }
  for (const j of recentJobs) {
    if (j.state === 'failed' && !j.duration) {
      const dlq = dlqMap.get(j.id);
      if (dlq) {
        (j as { duration?: number }).duration = dlq.duration;
        (j as { completedAt?: number }).completedAt = dlq.failedAt;
        (j as { totalDuration?: number }).totalDuration = dlq.failedAt - j.createdAt;
      }
    }
  }
}

/** Resolve redaction options with defaults (includeJobData defaults on). */
function resolveRedaction(params: CollectSnapshotParams): {
  redactFields: readonly string[];
  includeJobData: boolean;
} {
  return {
    redactFields: params.redactFields ?? [],
    includeJobData: params.includeJobData ?? true,
  };
}

/** Map cron definitions to snapshot shape */
function collectCrons(queueManager: QueueManager): CloudSnapshot['crons'] {
  return queueManager.listCrons().map((c) => ({
    name: c.name,
    queue: c.queue,
    schedule: c.schedule ?? null,
    repeatEvery: c.repeatEvery ?? null,
    nextRun: c.nextRun,
    executions: c.executions,
    maxLimit: c.maxLimit,
    lastRun: c.executions > 0 && c.repeatEvery ? c.nextRun - c.repeatEvery : null,
    priority: c.priority,
    timezone: c.timezone ?? null,
    data: c.data,
    uniqueKey: c.uniqueKey ?? null,
    dedup: c.dedup ?? null,
  }));
}

/** Collect a snapshot. Light data always fresh, heavy data cached between refreshes. */
export async function collectSnapshot(params: CollectSnapshotParams): Promise<CloudSnapshot> {
  const t0 = performance.now();
  const { queueManager, instanceId, instanceName, startedAt, sequenceId, serverHandles } = params;
  const { redactFields, includeJobData } = resolveRedaction(params);

  // ─── MCP operations (drain buffer into snapshot) ───
  const mcpData = serverHandles?.getMcpOperations?.();

  // ─── Light data (O(SHARD_COUNT), every snapshot) ───
  const stats = queueManager.getStats();
  const memStats = queueManager.getMemoryStats();
  const workerStats = queueManager.workerManager.getStats();
  const rates = throughputTracker.getRates();
  const percentiles = latencyTracker.getPercentiles();
  const averages = latencyTracker.getAverages();
  const storage = queueManager.getStorageStatus();
  const mem = process.memoryUsage();

  const queueNames = queueManager.listQueues();
  const queues = queueNames.map((name) => {
    const counts = queueManager.getQueueJobCounts(name);
    return {
      name,
      waiting: counts.waiting,
      prioritized: counts.prioritized,
      delayed: counts.delayed,
      active: counts.active,
      completed: counts.completed,
      failed: counts.failed,
      'waiting-children': counts['waiting-children'],
      paused: queueManager.isPaused(name),
      totalCompleted: counts.totalCompleted,
      totalFailed: counts.totalFailed,
    };
  });

  const crons = collectCrons(queueManager);

  // ─── Full data (every snapshot) ───
  const allQueueNames = queues.map((q) => q.name);
  const dlqQueues = queues.filter((q) => q.failed > 0);

  // Live jobs: waiting/active/delayed/failed — bounded by processing capacity
  const recentJobs = collectLiveJobs(queueManager, allQueueNames, {
    redactFields,
    includeJobData,
  });

  const dlqEntries = collectDlqEntries(
    queueManager,
    dlqQueues.map((q) => q.name),
    { redactFields, includeJobData }
  );

  enrichFailedJobDurations(recentJobs, dlqEntries);

  const topErrors = collectTopErrors(
    queueManager,
    dlqQueues.map((q) => q.name)
  );
  const now = Date.now();
  const WORKER_TIMEOUT_MS = 30_000;
  const workerDetails = queueManager.workerManager.list().map((w) => {
    const totalJobs = w.processedJobs + w.failedJobs;
    return {
      id: w.id,
      name: w.name,
      queues: w.queues,
      concurrency: w.concurrency,
      hostname: w.hostname,
      pid: w.pid,
      registeredAt: w.registeredAt,
      lastSeen: w.lastSeen,
      activeJobs: w.activeJobs,
      processedJobs: w.processedJobs,
      failedJobs: w.failedJobs,
      currentJob: w.currentJob,
      // Computed fields
      uptime: now - w.registeredAt,
      status:
        now - w.lastSeen > WORKER_TIMEOUT_MS
          ? ('stalled' as const)
          : w.activeJobs > 0
            ? ('active' as const)
            : ('idle' as const),
      errorRate: totalJobs > 0 ? +(w.failedJobs / totalJobs).toFixed(4) : 0,
      utilization: w.concurrency > 0 ? +(w.activeJobs / w.concurrency).toFixed(4) : 0,
    };
  });
  const queueConfigs = collectQueueConfigs(queueManager, new Set(queueNames));
  const webhooks = collectWebhooks(queueManager);
  const s3Backup = serverHandles?.getBackupStatus?.() ?? null;
  const telemetry = queueManager.getCloudTelemetry(allQueueNames);

  // Job results, logs, and locks
  const jobResultsMap = queueManager.getAllJobResults();
  const jobResults: Record<string, unknown> = {};
  for (const [id, val] of jobResultsMap) jobResults[id] = val;

  const jobLogsMap = queueManager.getAllJobLogs();
  const jobLogEntries: Record<
    string,
    Array<{ timestamp: number; level: 'info' | 'warn' | 'error'; message: string }>
  > = {};
  for (const [id, logs] of jobLogsMap) jobLogEntries[id] = logs;

  const locksMap = queueManager.getAllJobLocks();
  const activeLocks = [...locksMap.values()].map((l) => ({
    jobId: String(l.jobId),
    owner: l.owner,
    token: l.token,
    createdAt: l.createdAt,
    expiresAt: l.expiresAt,
    lastRenewalAt: l.lastRenewalAt,
    renewalCount: l.renewalCount,
    ttl: l.ttl,
  }));

  const result: CloudSnapshot = {
    instanceId,
    instanceName,
    version: VERSION,
    hostname: HOST,
    pid: process.pid,
    startedAt,
    timestamp: Date.now(),
    sequenceId,

    stats: {
      waiting: stats.waiting,
      prioritized: stats.prioritized,
      delayed: stats.delayed,
      active: stats.active,
      dlq: stats.dlq,
      completed: stats.completed,
      'waiting-children': stats['waiting-children'],
      stalled: memStats.stalledCandidates,
      paused: queues.filter((q) => q.paused).length,
      totalPushed: String(stats.totalPushed),
      totalPulled: String(stats.totalPulled),
      totalCompleted: String(stats.totalCompleted),
      totalFailed: String(stats.totalFailed),
      uptime: stats.uptime,
      cronJobs: stats.cronJobs,
      cronPending: stats.cronPending,
    },

    throughput: rates,
    latency: { averages, percentiles },

    memory: {
      heapUsed: Math.round((mem.heapUsed / 1024 / 1024) * 100) / 100,
      heapTotal: Math.round((mem.heapTotal / 1024 / 1024) * 100) / 100,
      rss: Math.round((mem.rss / 1024 / 1024) * 100) / 100,
      external: Math.round((mem.external / 1024 / 1024) * 100) / 100,
    },

    collections: {
      jobIndex: memStats.jobIndex,
      completedJobs: memStats.completedJobs,
      jobResults: memStats.jobResults,
      jobLogs: memStats.jobLogs,
      customIdMap: memStats.customIdMap,
      jobLocks: memStats.jobLocks,
      processingTotal: memStats.processingTotal,
      queuedTotal: memStats.queuedTotal,
      temporalIndexTotal: memStats.temporalIndexTotal,
      delayedHeapTotal: memStats.delayedHeapTotal,
    },

    queues,
    workers: workerStats,
    crons,
    storage: { diskFull: storage.diskFull, error: storage.error },
    taskErrors: getTaskErrorStats(),

    connections: {
      tcp: serverHandles?.getConnectionCount() ?? 0,
      ws: serverHandles?.getWsClientCount() ?? 0,
      sse: serverHandles?.getSseClientCount() ?? 0,
    },

    // Analytics
    queueThroughput: collectQueueThroughput(queues),
    durationHistogram: collectDurationHistogram(recentJobs),
    workerUtilization: collectWorkerUtilization(queueManager),
    sqliteStats: serverHandles?.getSqliteStats?.() ?? null,
    runtime: RUNTIME,
    queueWaitTime: collectQueueWaitTime(recentJobs),
    queueRetryRate: collectQueueRetryRate(recentJobs),
    queueBacklogVelocity: collectBacklogVelocity(queues),
    stallDetails: await collectStallDetails(queueManager),
    queuePriorityDistribution: collectPriorityDistribution(recentJobs),

    // MCP telemetry
    ...(mcpData && mcpData.operations.length > 0
      ? { mcpOperations: mcpData.operations, mcpSummary: mcpData.summary }
      : {}),

    recentJobs,
    dlqEntries,
    topErrors,
    workerDetails,
    queueConfigs,
    webhooks,
    s3Backup,
    jobResults,
    jobLogEntries,
    activeLocks,
    queueExtended: telemetry.perQueue,
    eventSubscribers: telemetry.eventSubscribers,
    pendingDepChecks: telemetry.pendingDepChecks,
  };
  cloudLog.info('Snapshot collect', { ms: Math.round((performance.now() - t0) * 100) / 100 });
  return result;
}
