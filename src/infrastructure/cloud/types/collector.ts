import type { QueueManager } from '../../../application/queueManager';

/** Optional server handles for connection stats, backup, storage, and MCP telemetry. */
export interface ServerHandles {
  getConnectionCount: () => number;
  getWsClientCount: () => number;
  getSseClientCount: () => number;
  triggerBackup?: () => Promise<unknown>;
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

/** Parameters for snapshot collection. */
export interface CollectSnapshotParams {
  queueManager: QueueManager;
  instanceId: string;
  instanceName: string;
  startedAt: number;
  sequenceId: number;
  serverHandles?: ServerHandles;
  includeHeavy: boolean;
  /** Fields to redact from job data (mirrors CloudConfig.redactFields). Default: none. */
  redactFields?: readonly string[];
  /** Include job data in snapshots (mirrors CloudConfig.includeJobData). Default: true. */
  includeJobData?: boolean;
}
