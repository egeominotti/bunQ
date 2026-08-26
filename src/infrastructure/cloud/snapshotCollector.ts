/**
 * Snapshot Collector
 * Two-tier collection: light (every 15s) + heavy (every 90s)
 *
 * Light: stats, throughput, latency, memory, connections — O(SHARD_COUNT)
 * Heavy: recentJobs, dlqEntries, topErrors, workerDetails, queueConfigs, webhooks — O(queues × shards)
 */

import { throughputTracker } from '../../application/throughputTracker';
import { latencyTracker } from '../../application/latencyTracker';
import { getTaskErrorStats } from '../../application/backgroundTasks';
import { VERSION } from '../../shared/version';
import { clientStorageStatus } from '../../shared/storageHealth';
import type { CloudSnapshot } from './types';
import { cloudLog } from './logger';
import {
  SNAPSHOT_HOST,
  SNAPSHOT_RUNTIME,
  enrichFailedJobDurations,
  mapSnapshotCrons,
  mapSnapshotJobArtifacts,
  mapSnapshotWorkers,
  resolveRedaction,
} from './snapshot/collector';
import {
  collectWebhooks,
  collectQueueThroughput,
  collectDurationHistogram,
  collectQueueWaitTime,
  collectQueueRetryRate,
  collectBacklogVelocity,
  collectStallDetails,
  collectPriorityDistribution,
  mapSnapshotDlqEntries,
  mapSnapshotJobs,
  mapSnapshotQueueConfigs,
  mapSnapshotTopErrors,
  mapSnapshotWorkerUtilization,
} from './snapshotHelpers';
import type { CollectSnapshotParams } from './types/collector';
import { resolveCloudQueueAdapter } from './queueAdapter/registry';

export type { CollectSnapshotParams, ServerHandles } from './types/collector';

/** Collect a snapshot. Light data always fresh, heavy data cached between refreshes. */
export async function collectSnapshot(params: CollectSnapshotParams): Promise<CloudSnapshot> {
  const t0 = performance.now();
  const { queueManager, instanceId, instanceName, startedAt, sequenceId, serverHandles } = params;
  const { redactFields, includeJobData } = resolveRedaction(params);
  const source = await resolveCloudQueueAdapter(queueManager).readSnapshotSource();

  // ─── MCP operations (drain buffer into snapshot) ───
  const mcpData = serverHandles?.getMcpOperations?.();

  // ─── Light data (O(SHARD_COUNT), every snapshot) ───
  const stats = source.stats;
  const memStats = queueManager.getMemoryStats();
  const workerStats = source.workerStats;
  const rates = throughputTracker.getRates();
  const percentiles = latencyTracker.getPercentiles();
  const averages = latencyTracker.getAverages();
  const storage = clientStorageStatus(queueManager.getStorageStatus());
  const mem = process.memoryUsage();

  const queues = source.queues.map(({ name, counts, paused }) => ({ name, ...counts, paused }));
  const crons = mapSnapshotCrons(source.crons);

  // ─── Full data (every snapshot) ───
  const allQueueNames = queues.map((q) => q.name);
  const recentJobs = mapSnapshotJobs(source.jobs, {
    redactFields,
    includeJobData,
  });
  const dlqEntries = mapSnapshotDlqEntries(source.dlqEntries, {
    redactFields,
    includeJobData,
  });

  enrichFailedJobDurations(recentJobs, dlqEntries);

  const topErrors = mapSnapshotTopErrors(source.dlqEntries);
  const workerDetails = mapSnapshotWorkers(source.workers, Date.now());
  const queueConfigs = mapSnapshotQueueConfigs(source.queues);
  const webhooks = collectWebhooks(queueManager);
  const s3Backup = serverHandles?.getBackupStatus?.() ?? null;
  const telemetry = queueManager.getCloudTelemetry(allQueueNames);

  const { jobResults, jobLogEntries, activeLocks } = mapSnapshotJobArtifacts(source);

  const result: CloudSnapshot = {
    instanceId,
    instanceName,
    version: VERSION,
    hostname: SNAPSHOT_HOST,
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
    workerUtilization: mapSnapshotWorkerUtilization(source.workers),
    sqliteStats: serverHandles?.getSqliteStats?.() ?? null,
    runtime: SNAPSHOT_RUNTIME,
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
