export interface BenchJob {
  id: string;
  priority: number;
  createdAt: number;
  data: unknown;
  groupId?: string | null;
}

export interface QueueManagerLike {
  push(queue: string, input: Record<string, unknown>): Promise<BenchJob>;
  pushBatch(queue: string, inputs: Array<Record<string, unknown>>): Promise<string[]>;
  pull(
    queue: string,
    timeoutMs?: number,
    signal?: AbortSignal,
    groupOptions?: { concurrency?: number; limit?: { max: number; duration: number } }
  ): Promise<BenchJob | null>;
  pullBatch(
    queue: string,
    count: number,
    timeoutMs?: number,
    signal?: AbortSignal,
    groupOptions?: { concurrency?: number; limit?: { max: number; duration: number } }
  ): Promise<BenchJob[]>;
  getJobs(
    queue: string,
    options?: { state?: string | string[]; start?: number; end?: number; asc?: boolean }
  ): BenchJob[];
  getStats(): Record<string, number | bigint>;
  getQueuesSummary(): Array<{
    name: string;
    counts: {
      waiting: number;
      prioritized?: number;
      active: number;
      completed: number;
      failed: number;
      delayed: number;
    };
  }>;
  getQueueJobCounts(queue: string): {
    waiting: number;
    prioritized: number;
    delayed: number;
  };
  getMemoryStats(): { jobIndex: number; queuedTotal: number; delayedHeapTotal: number };
  shutdown(): void;
}

export interface StorageLike {
  close(): void;
  countActiveJobs(): number;
  countPendingJobs(): number;
}

export interface TemporalManagerLike {
  addToIndex(createdAt: number, jobId: string, queue: string): void;
  getOldJobs(
    queue: string,
    thresholdMs: number,
    limit: number
  ): Array<{ jobId: string; createdAt: number }>;
  removeFromIndex(jobId: string): void;
  addDelayed(jobId: string, runAt: number): void;
  removeDelayed(jobId: string): boolean;
  readonly indexSize: number;
  getSizes(): {
    delayedJobIds: number;
    delayedHeap: number;
    delayedRunAt: number;
    temporalIndex: number;
  };
}

export interface WaiterManagerLike {
  notifyBatch(count: number): void;
  waitForJob(timeoutMs: number): Promise<void>;
  readonly length: number;
}

export interface RuntimeModules {
  QueueManager: new (config?: Record<string, unknown>) => QueueManagerLike;
  SqliteStorage: new (config: { path: string }) => StorageLike;
  pack: (value: unknown) => Uint8Array;
  TemporalManager: new () => TemporalManagerLike;
  WaiterManager: new () => WaiterManagerLike;
}

export interface Distribution {
  count: number;
  min: number;
  median: number;
  p95: number;
  max: number;
  mad: number;
}

export interface BenchmarkResult {
  id: string;
  category: string;
  operation: string;
  unit: 'ms';
  direction: 'lower-is-better';
  workload: Record<string, number | string | boolean>;
  samples: number[];
  distribution: Distribution;
  correctness: Record<string, unknown>;
  comparable: boolean;
  note?: string;
}

export interface Profile {
  recoveryJobs: number;
  recoverySamples: number;
  memoryQueryJobs: number;
  sqlNormalJobs: number;
  sqlPriorityJobs: number;
  querySamples: number;
  holBlockedJobs: number;
  holSamples: number;
  ungroupedJobs: number;
  mixedJobs: number;
  mixedGroups: number;
  queuePathSamples: number;
  statsQueues: number;
  statsJobs: number;
  statsSamples: number;
  temporalUnrelatedJobs: number;
  temporalQuerySamples: number;
  temporalRemovalSamples: number;
  waiterCount: number;
  waiterSamples: number;
  delayedChurnJobs: number;
  delayedSamples: number;
}

export const FULL_PROFILE: Profile = {
  recoveryJobs: 10_001,
  recoverySamples: 3,
  memoryQueryJobs: 50_000,
  sqlNormalJobs: 20_000,
  sqlPriorityJobs: 5_000,
  querySamples: 21,
  holBlockedJobs: 5_000,
  holSamples: 7,
  ungroupedJobs: 20_000,
  mixedJobs: 20_000,
  mixedGroups: 100,
  queuePathSamples: 7,
  statsQueues: 200,
  statsJobs: 50_000,
  statsSamples: 15,
  temporalUnrelatedJobs: 500_000,
  temporalQuerySamples: 31,
  temporalRemovalSamples: 15,
  waiterCount: 10_000,
  waiterSamples: 5,
  delayedChurnJobs: 100_000,
  delayedSamples: 5,
};

export const SMOKE_PROFILE: Profile = {
  recoveryJobs: 10_001,
  recoverySamples: 1,
  memoryQueryJobs: 5_000,
  sqlNormalJobs: 20,
  sqlPriorityJobs: 5,
  querySamples: 5,
  holBlockedJobs: 50,
  holSamples: 3,
  ungroupedJobs: 500,
  mixedJobs: 500,
  mixedGroups: 10,
  queuePathSamples: 2,
  statsQueues: 20,
  statsJobs: 2_000,
  statsSamples: 5,
  temporalUnrelatedJobs: 10_000,
  temporalQuerySamples: 5,
  temporalRemovalSamples: 3,
  waiterCount: 1_000,
  waiterSamples: 3,
  delayedChurnJobs: 5_000,
  delayedSamples: 3,
};
