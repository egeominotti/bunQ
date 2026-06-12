/**
 * Queue Manager
 * Core orchestrator for all queue operations
 */

import type { Job, JobId, JobInput, JobLock, LockToken } from '../domain/types/job';
import { DEFAULT_LOCK_TTL } from '../domain/types/job';
import type { JobLocation, JobEvent } from '../domain/types/queue';
import { EventType } from '../domain/types/queue';
import type { CronJob, CronJobInput } from '../domain/types/cron';
import type { JobLogEntry, CreateWorkerOptions } from '../domain/types/worker';
import type { StallConfig } from '../domain/types/stall';
import type { DlqConfig, DlqEntry, DlqFilter, DlqStats } from '../domain/types/dlq';
import { FailureReason } from '../domain/types/dlq';
import { Shard } from '../domain/queue/shard';
import { SqliteStorage } from '../infrastructure/persistence/sqlite';
import { CronScheduler } from '../infrastructure/scheduler/cronScheduler';
import { WebhookManager } from './webhookManager';
import { WorkerManager } from './workerManager';
import { EventsManager } from './eventsManager';
import { createMonitoringState, type MonitoringState } from './monitoringChecks';
import { RWLock, withWriteLock } from '../shared/lock';
import { shardIndex, SHARD_COUNT } from '../shared/hash';
import { pushJob, pushJobBatch } from './operations/push';
import { pullJob, pullJobBatch } from './operations/pull';
import { ackJob, ackJobBatch, ackJobBatchWithResults, failJob } from './operations/ack';
import * as queueControl from './operations/queueControl';
import * as jobMgmt from './operations/jobManagement';
import * as jobTransitions from './operations/jobStateTransitions';
import * as queryOps from './operations/queryOperations';
import * as dlqOps from './dlqManager';
import * as logsOps from './jobLogsManager';
import { generatePrometheusMetrics } from './metricsExporter';
import { LRUMap, BoundedSet, BoundedMap, type SetLike } from '../shared/lru';

import type { QueueManagerConfig } from './types';
import { DEFAULT_CONFIG } from './types';
import * as lockMgr from './lockManager';
import * as bgTasks from './backgroundTasks';
import * as statsMgr from './statsManager';
import { ContextFactory, type ContextDependencies, type ContextCallbacks } from './contextFactory';
import { processPendingDependencies } from './dependencyProcessor';
import { handleTaskError, handleTaskSuccess } from './taskErrorTracking';

export type { QueueManagerConfig };

/**
 * QueueManager - Central coordinator
 */
export class QueueManager {
  private readonly config: typeof DEFAULT_CONFIG & { dataPath?: string };
  private readonly storage: SqliteStorage | null;

  // Sharded data structures
  private readonly shards: Shard[] = [];
  private readonly shardLocks: RWLock[] = [];
  private readonly processingShards: Map<JobId, Job>[] = [];
  private readonly processingLocks: RWLock[] = [];

  // Global indexes (bounded with LRU eviction)
  private readonly jobIndex = new Map<JobId, JobLocation>();
  private readonly completedJobs!: BoundedSet<JobId>;
  private readonly completedJobsData!: BoundedMap<JobId, Job>;
  private readonly jobResults!: LRUMap<JobId, unknown>;
  private readonly customIdMap!: LRUMap<string, JobId>;
  private readonly jobLogs!: LRUMap<JobId, JobLogEntry[]>;

  // Deferred dependency resolution queue
  private readonly pendingDepChecks = new Set<JobId>();

  // Two-phase stall detection
  private readonly stalledCandidates = new Set<JobId>();

  // Event-driven dependency flush state
  private depFlushScheduled = false;
  private depFlushRunning = false;

  // Lock-based job ownership tracking
  private readonly jobLocks = new Map<JobId, JobLock>();
  private readonly clientJobs = new Map<string, Set<JobId>>();

  // Repeat chain: maps completed job ID -> successor repeat job ID
  private readonly repeatChain = new Map<JobId, JobId>();

  // BullMQ v5 flow failure tracking
  /** Stores failed children values for continueParentOnFailure — parentId → { childKey: error } */
  private readonly failedChildrenValues = new Map<JobId, Record<string, string>>();
  /** Stores ignored children failures for ignoreDependencyOnFailure — parentId → { childKey: error } */
  private readonly ignoredChildrenFailures = new Map<JobId, Record<string, string>>();

  // Cron scheduler
  private readonly cronScheduler: CronScheduler;

  // Managers
  readonly webhookManager: WebhookManager;
  readonly workerManager: WorkerManager;
  private readonly eventsManager: EventsManager;

  // Dashboard event callback (set by HTTP server for WS broadcast)
  private dashboardEmit: ((event: string, data: Record<string, unknown>) => void) | null = null;
  private recoveryStats: { queues: number; jobs: number } | null = null;

  // Job logs config
  private readonly maxLogsPerJob = 100;

  // Metrics
  private readonly metrics = {
    totalPushed: { value: 0n },
    totalPulled: { value: 0n },
    totalCompleted: { value: 0n },
    totalFailed: { value: 0n },
  };
  private readonly perQueueMetrics = new Map<
    string,
    { totalCompleted: bigint; totalFailed: bigint }
  >();
  private readonly startTime = Date.now();

  // Background task handles
  private readonly backgroundTaskHandles!: bgTasks.BackgroundTaskHandles | null;

  // Queue names cache
  private readonly queueNamesCache = new Set<string>();
  // Monitoring state
  private readonly monitoringState: MonitoringState = createMonitoringState();

  // Context factory
  private readonly contextFactory: ContextFactory;

  constructor(config: QueueManagerConfig = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.storage = config.dataPath ? new SqliteStorage({ path: config.dataPath }) : null;

    // Initialize bounded collections
    this.completedJobsData = new BoundedMap<JobId, Job>(this.config.maxCompletedJobs);
    this.completedJobs = new BoundedSet<JobId>(this.config.maxCompletedJobs, (jobId) => {
      this.jobIndex.delete(jobId);
      this.completedJobsData.delete(jobId);
    });
    this.jobResults = new LRUMap<JobId, unknown>(this.config.maxJobResults);
    this.customIdMap = new LRUMap<string, JobId>(this.config.maxCustomIds);
    this.jobLogs = new LRUMap<JobId, JobLogEntry[]>(this.config.maxJobLogs);

    // Initialize shards
    for (let i = 0; i < SHARD_COUNT; i++) {
      this.shards.push(new Shard());
      this.shardLocks.push(new RWLock());
      this.processingShards.push(new Map());
      this.processingLocks.push(new RWLock());
    }

    // Initialize cron scheduler
    this.cronScheduler = new CronScheduler();
    this.cronScheduler.setPushCallback(async (queue, input) => {
      await this.push(queue, input);
    });
    // Set up persistence callback for cron state
    if (this.storage) {
      const storage = this.storage;
      this.cronScheduler.setPersistCallback((name, executions, nextRun) => {
        storage.updateCron(name, executions, nextRun);
      });
    }

    // Initialize managers
    this.webhookManager = new WebhookManager({ validateUrls: config.validateWebhookUrls });
    this.workerManager = new WorkerManager();
    // Wire worker check for skipIfNoWorker crons
    this.cronScheduler.setWorkerCheckCallback((queue) => {
      return this.workerManager.getForQueue(queue).length > 0;
    });
    this.eventsManager = new EventsManager(this.webhookManager);

    // Initialize context factory
    this.contextFactory = new ContextFactory(
      this.getContextDependencies(),
      this.getContextCallbacks()
    );

    // Load and start
    bgTasks.recover(this.contextFactory.getBackgroundContext());
    this.recoveryStats = { queues: this.queueNamesCache.size, jobs: this.jobIndex.size };
    if (this.storage) {
      this.cronScheduler.load(this.storage.loadCronJobs());
    }
    this.backgroundTaskHandles = bgTasks.startBackgroundTasks(
      this.contextFactory.getBackgroundContext(),
      this.cronScheduler
    );
  }

  // ============ Context Dependencies ============

  private getContextDependencies(): ContextDependencies {
    return {
      config: this.config,
      storage: this.storage,
      shards: this.shards,
      shardLocks: this.shardLocks,
      processingShards: this.processingShards,
      processingLocks: this.processingLocks,
      jobIndex: this.jobIndex,
      completedJobs: this.completedJobs,
      completedJobsData: this.completedJobsData,
      jobResults: this.jobResults,
      customIdMap: this.customIdMap,
      jobLogs: this.jobLogs,
      jobLocks: this.jobLocks,
      clientJobs: this.clientJobs,
      stalledCandidates: this.stalledCandidates,
      pendingDepChecks: this.pendingDepChecks,
      queueNamesCache: this.queueNamesCache,
      repeatChain: this.repeatChain,
      eventsManager: this.eventsManager,
      webhookManager: this.webhookManager,
      workerManager: this.workerManager,
      monitoringState: this.monitoringState,
      metrics: this.metrics,
      startTime: this.startTime,
      maxLogsPerJob: this.maxLogsPerJob,
      perQueueMetrics: this.perQueueMetrics,
    };
  }

  private getContextCallbacks(): ContextCallbacks {
    return {
      fail: this.fail.bind(this),
      registerQueueName: this.registerQueueName.bind(this),
      unregisterQueueName: this.unregisterQueueName.bind(this),
      onJobCompleted: this.onJobCompleted.bind(this),
      onJobsCompleted: this.onJobsCompleted.bind(this),
      hasPendingDeps: this.hasPendingDeps.bind(this),
      onRepeat: this.handleRepeat.bind(this),
      emitDashboardEvent: this.emitDashboardEvent.bind(this),
      onChildTerminalFailure: this.failParentOnChildFailure.bind(this),
      onChildDependencyOption: this.onChildDependencyOption.bind(this),
    };
  }

  private handleRepeat(job: Job): void {
    if (!job.repeat) return;
    const delay = job.repeat.every ?? 0;
    const oldId = job.id;
    void this.push(job.queue, {
      data: job.data,
      priority: job.priority,
      delay,
      maxAttempts: job.maxAttempts,
      backoff: job.backoff,
      ttl: job.ttl ?? undefined,
      timeout: job.timeout ?? undefined,
      tags: job.tags,
      groupId: job.groupId ?? undefined,
      lifo: job.lifo,
      removeOnComplete: job.removeOnComplete,
      removeOnFail: job.removeOnFail,
      repeat: {
        every: job.repeat.every,
        limit: job.repeat.limit,
        pattern: job.repeat.pattern,
        count: job.repeat.count + 1,
      },
    }).then((newJob) => {
      // Store mapping so updateJobData can find the successor
      this.repeatChain.set(oldId, newJob.id);
      // Limit chain size to prevent memory leak (only keep recent mappings)
      if (this.repeatChain.size > 10000) {
        const first = this.repeatChain.keys().next().value;
        if (first !== undefined) this.repeatChain.delete(first);
      }
    });
  }

  // ============ Core Operations ============

  async push(queue: string, input: JobInput): Promise<Job> {
    this.registerQueueName(queue);
    return pushJob(queue, input, this.contextFactory.getPushContext());
  }

  async pushBatch(queue: string, inputs: JobInput[]): Promise<JobId[]> {
    this.registerQueueName(queue);
    return pushJobBatch(queue, inputs, this.contextFactory.getPushContext());
  }

  async pull(queue: string, timeoutMs: number = 0): Promise<Job | null> {
    return pullJob(queue, timeoutMs, this.contextFactory.getPullContext());
  }

  async pullWithLock(
    queue: string,
    owner: string,
    timeoutMs: number = 0,
    lockTtl: number = DEFAULT_LOCK_TTL
  ): Promise<{ job: Job | null; token: string | null }> {
    const job = await pullJob(queue, timeoutMs, this.contextFactory.getPullContext());
    if (!job) return { job: null, token: null };
    const token = lockMgr.createLock(job.id, owner, this.contextFactory.getLockContext(), lockTtl);
    return { job, token };
  }

  async pullBatch(queue: string, count: number, timeoutMs: number = 0): Promise<Job[]> {
    return pullJobBatch(queue, count, timeoutMs, this.contextFactory.getPullContext());
  }

  async pullBatchWithLock(
    queue: string,
    count: number,
    owner: string,
    timeoutMs: number = 0,
    lockTtl: number = DEFAULT_LOCK_TTL
  ): Promise<{ jobs: Job[]; tokens: string[] }> {
    const jobs = await pullJobBatch(queue, count, timeoutMs, this.contextFactory.getPullContext());
    const tokens: string[] = [];
    for (const job of jobs) {
      const token = lockMgr.createLock(
        job.id,
        owner,
        this.contextFactory.getLockContext(),
        lockTtl
      );
      tokens.push(token ?? '');
    }
    return { jobs, tokens };
  }

  async ack(jobId: JobId, result?: unknown, token?: string): Promise<void> {
    const lockCtx = this.contextFactory.getLockContext();
    if (token && !lockMgr.verifyLock(jobId, token, lockCtx)) {
      this.throwIfOwnershipConflict(jobId, lockCtx);
      // No ownership conflict. If job is still in processing (dedup case
      // from Issue #33: lock removed but job still there), proceed with ACK.
      // Otherwise (lock expiration: job requeued), return gracefully.
      const loc = this.jobIndex.get(jobId);
      if (loc?.type !== 'processing') {
        // Job may have been stall-retried to queue while we processed it.
        // Complete it from queue to prevent duplicate execution (Issue #33).
        if (loc?.type === 'queue') {
          await this.completeStallRetriedJob(jobId, result);
          lockMgr.releaseLock(jobId, lockCtx, token);
        }
        return;
      }
    }
    try {
      await ackJob(jobId, result, this.contextFactory.getAckContext());
    } catch (err) {
      // Job removed from processing by stall detection and re-queued.
      // Try to complete from queue to prevent duplicate execution (Issue #33).
      // With token: always attempt (worker had ownership via lock).
      // Without token: only if job was stall-retried (attempts > 0), to avoid
      // completing freshly-pushed jobs that were never pulled.
      if (err instanceof Error && err.message.includes('not found')) {
        const shouldRecover = token ?? this.isStallRetried(jobId);
        if (shouldRecover && (await this.completeStallRetriedJob(jobId, result))) {
          if (token) lockMgr.releaseLock(jobId, lockCtx, token);
          return;
        }
      }
      throw err;
    }
    lockMgr.releaseLock(jobId, lockCtx, token);
  }

  async ackBatch(jobIds: JobId[], tokens?: string[]): Promise<void> {
    const lockCtx = this.contextFactory.getLockContext();
    const validJobIds: JobId[] = [];
    const validTokens: string[] | undefined = tokens ? [] : undefined;
    if (tokens?.length === jobIds.length) {
      for (let i = 0; i < jobIds.length; i++) {
        const t = tokens[i];
        if (t && !lockMgr.verifyLock(jobIds[i], t, lockCtx)) {
          this.throwIfOwnershipConflict(jobIds[i], lockCtx);
          // Recover stall-retried job (#75): lock expired and job was
          // re-queued by lock expiration or stall detection. Complete it
          // from the queue to prevent duplicate execution.
          const loc = this.jobIndex.get(jobIds[i]);
          if (loc?.type === 'queue') {
            await this.completeStallRetriedJob(jobIds[i], undefined);
            lockMgr.releaseLock(jobIds[i], lockCtx, t);
          }
          continue;
        }
        validJobIds.push(jobIds[i]);
        if (validTokens) validTokens.push(t);
      }
    } else {
      validJobIds.push(...jobIds);
    }
    if (validJobIds.length > 0) {
      await ackJobBatch(validJobIds, this.contextFactory.getAckContext());
    }
    if (validTokens) {
      for (let i = 0; i < validJobIds.length; i++) {
        lockMgr.releaseLock(validJobIds[i], lockCtx, validTokens[i]);
      }
    } else if (tokens) {
      for (let i = 0; i < jobIds.length; i++) {
        lockMgr.releaseLock(jobIds[i], lockCtx, tokens[i]);
      }
    }
  }

  async ackBatchWithResults(
    items: Array<{ id: JobId; result: unknown; token?: string }>
  ): Promise<void> {
    const lockCtx = this.contextFactory.getLockContext();
    const validItems: typeof items = [];
    for (const item of items) {
      if (item.token && !lockMgr.verifyLock(item.id, item.token, lockCtx)) {
        this.throwIfOwnershipConflict(item.id, lockCtx);
        // Recover stall-retried job (#75): lock expired and job was
        // re-queued by lock expiration or stall detection. Complete it
        // from the queue to prevent duplicate execution.
        const loc = this.jobIndex.get(item.id);
        if (loc?.type === 'queue') {
          await this.completeStallRetriedJob(item.id, item.result);
          lockMgr.releaseLock(item.id, lockCtx, item.token);
        }
        continue;
      }
      validItems.push(item);
    }
    if (validItems.length > 0) {
      await ackJobBatchWithResults(validItems, this.contextFactory.getAckContext());
    }
    for (const item of validItems) {
      lockMgr.releaseLock(item.id, lockCtx, item.token);
    }
  }

  async fail(
    jobId: JobId,
    error?: string,
    token?: string,
    unrecoverable = false,
    stack?: string[]
  ): Promise<void> {
    const lockCtx = this.contextFactory.getLockContext();
    if (token && !lockMgr.verifyLock(jobId, token, lockCtx)) {
      this.throwIfOwnershipConflict(jobId, lockCtx);
      const loc = this.jobIndex.get(jobId);
      if (loc?.type !== 'processing') return;
    }
    try {
      await failJob(jobId, error, this.contextFactory.getAckContext(), unrecoverable, stack);
    } catch (err) {
      // Job removed from processing by stall detection. The stall retry
      // already handles requeuing, so the fail is redundant — return silently.
      // Only applies when caller has a lock token (worker with ownership).
      if (token && err instanceof Error && err.message.includes('not found')) {
        const loc = this.jobIndex.get(jobId);
        if (loc?.type === 'queue') return;
      }
      throw err;
    }
    lockMgr.releaseLock(jobId, lockCtx, token);
  }

  /**
   * Check if a failed lock verification is a genuine ownership conflict.
   * If the job is still in processing with a different lock, throw.
   * If the job was already requeued by the background lock expiration task, return silently.
   */
  private throwIfOwnershipConflict(jobId: JobId, lockCtx: { jobLocks: Map<JobId, JobLock> }): void {
    const loc = this.jobIndex.get(jobId);
    if (loc?.type === 'processing' && lockCtx.jobLocks.has(jobId)) {
      throw new Error(`Invalid or expired lock token for job ${jobId}`);
    }
  }

  /** Check if a queued job was stall-retried (has been processed before). */
  private isStallRetried(jobId: JobId): boolean {
    const loc = this.jobIndex.get(jobId);
    if (loc?.type !== 'queue') return false;
    const shard = this.shards[loc.shardIdx];
    const pq = shard.getQueue(loc.queueName);
    const job = pq.find(jobId);
    return job !== null && job.attempts > 0;
  }

  /**
   * Complete a job that stall detection moved back to the queue while still processing.
   * Removes from queue and marks completed to prevent duplicate execution (Issue #33).
   */
  private async completeStallRetriedJob(jId: JobId, result: unknown): Promise<boolean> {
    const loc = this.jobIndex.get(jId);
    if (loc?.type !== 'queue') return false;

    const idx = loc.shardIdx;
    const queueName = loc.queueName;
    const shard = this.shards[idx];

    let job: Job | null = null;
    await withWriteLock(this.shardLocks[idx], () => {
      const pq = shard.getQueue(queueName);
      job = pq.remove(jId);
      if (job) {
        shard.decrementQueued(jId);
        shard.releaseJobResources(queueName, job.uniqueKey, job.groupId);
      }
    });

    if (!job) {
      // Job was already pulled from queue by another worker — can't prevent.
      return false;
    }

    const ctx = this.contextFactory.getAckContext();
    if (!(job as Job).removeOnComplete) {
      ctx.completedJobs.add(jId);
      ctx.completedJobsData.set(jId, job);
      if (result !== undefined) {
        ctx.jobResults.set(jId, result);
        ctx.storage?.storeResult(jId, result);
      }
      ctx.jobIndex.set(jId, { type: 'completed', queueName: (job as Job).queue });
      ctx.storage?.markCompleted(jId, Date.now(), (job as Job).timeline);
    } else {
      ctx.jobIndex.delete(jId);
      ctx.storage?.deleteJob(jId);
    }

    ctx.totalCompleted.value++;
    ctx.broadcast({
      eventType: 'completed' as EventType,
      queue: queueName,
      jobId: jId,
      timestamp: Date.now(),
      data: result,
    });

    ctx.onJobCompleted(jId);
    return true;
  }

  jobHeartbeat(jobId: JobId, token?: string): boolean {
    const loc = this.jobIndex.get(jobId);
    if (loc?.type !== 'processing') return false;

    if (token) {
      return lockMgr.renewJobLock(jobId, token, this.contextFactory.getLockContext());
    }

    const processing = this.processingShards[loc.shardIdx];
    const job = processing.get(jobId);
    if (job) {
      job.lastHeartbeat = Date.now();
      return true;
    }
    return false;
  }

  jobHeartbeatBatch(jobIds: JobId[], tokens?: string[]): number {
    let count = 0;
    for (let i = 0; i < jobIds.length; i++) {
      if (this.jobHeartbeat(jobIds[i], tokens?.[i])) count++;
    }
    return count;
  }

  // ============ Lock Management ============

  /** Remove a lock unconditionally (used by worker dedup to clear stale locks) */
  removeLock(jobId: JobId): void {
    this.contextFactory.getLockContext().jobLocks.delete(jobId);
  }

  createLock(jobId: JobId, owner: string, ttl: number = DEFAULT_LOCK_TTL): LockToken | null {
    return lockMgr.createLock(jobId, owner, this.contextFactory.getLockContext(), ttl);
  }

  verifyLock(jobId: JobId, token: string): boolean {
    return lockMgr.verifyLock(jobId, token, this.contextFactory.getLockContext());
  }

  renewJobLock(jobId: JobId, token: string, newTtl?: number): boolean {
    return lockMgr.renewJobLock(jobId, token, this.contextFactory.getLockContext(), newTtl);
  }

  renewJobLockBatch(items: Array<{ id: JobId; token: string; ttl?: number }>): string[] {
    return lockMgr.renewJobLockBatch(items, this.contextFactory.getLockContext());
  }

  releaseLock(jobId: JobId, token?: string): boolean {
    return lockMgr.releaseLock(jobId, this.contextFactory.getLockContext(), token);
  }

  getLockInfo(jobId: JobId): JobLock | null {
    return lockMgr.getLockInfo(jobId, this.contextFactory.getLockContext());
  }

  // ============ Client-Job Tracking ============

  registerClientJob(clientId: string, jobId: JobId): void {
    lockMgr.registerClientJob(clientId, jobId, this.contextFactory.getLockContext());
  }

  unregisterClientJob(clientId: string | undefined, jobId: JobId): void {
    lockMgr.unregisterClientJob(clientId, jobId, this.contextFactory.getLockContext());
  }

  releaseClientJobs(clientId: string): Promise<number> {
    return lockMgr.releaseClientJobs(clientId, this.contextFactory.getLockContext());
  }

  /**
   * Force-release client tracking without acquiring queue locks. Last-resort
   * fallback when releaseClientJobs has exhausted its retry budget — clears
   * the clientJobs map entry to prevent leaks and resets job heartbeats so
   * the stall detector recovers orphaned active jobs on its next tick.
   */
  forceReleaseClientJobs(clientId: string): number {
    return lockMgr.forceReleaseClientJobs(clientId, this.contextFactory.getLockContext());
  }

  // ============ Query Operations ============

  async getJob(jobId: JobId): Promise<Job | null> {
    return queryOps.getJob(jobId, this.contextFactory.getQueryContext());
  }

  async getJobState(jobId: JobId): Promise<string> {
    return queryOps.getJobState(jobId, this.contextFactory.getQueryContext());
  }

  getResult(jobId: JobId): unknown {
    return queryOps.getJobResult(jobId, this.contextFactory.getQueryContext());
  }

  /**
   * Get return values from all children of a parent job.
   * Returns a Record where keys are job keys (queueName:jobId) and values are return values.
   * BullMQ v5 compatible.
   */
  async getChildrenValues(parentJobId: JobId): Promise<Record<string, unknown>> {
    const job = await this.getJob(parentJobId);
    if (!job?.childrenIds || job.childrenIds.length === 0) {
      return {};
    }

    const ctx = this.contextFactory.getQueryContext();
    const results: Record<string, unknown> = {};

    for (const childId of job.childrenIds) {
      const result = queryOps.getJobResult(childId, ctx);
      if (result !== undefined) {
        // Get child job to find its queue name
        const childJob = await this.getJob(childId);
        const key = childJob ? `${childJob.queue}:${childId}` : childId;
        results[key] = result;
      }
    }

    return results;
  }

  /**
   * Update a job's parent reference.
   * Used by FlowProducer when creating flows where children need to reference parent.
   */
  async updateJobParent(childJobId: JobId, parentJobId: JobId): Promise<void> {
    const childJob = await this.getJob(childJobId);
    if (!childJob) return;

    // Update domain parentId field (cast to bypass readonly for flow linkage)
    (childJob as { parentId: JobId | null }).parentId = parentJobId;

    // Update the job's data to include parent reference
    const jobData = childJob.data as Record<string, unknown>;
    jobData.__parentId = parentJobId;
    jobData.__parentQueue = childJob.queue;

    // Also add this child to parent's childrenIds
    const parentJob = await this.getJob(parentJobId);
    if (parentJob) {
      if (!parentJob.childrenIds) {
        parentJob.childrenIds = [];
      }
      if (!parentJob.childrenIds.includes(childJobId)) {
        parentJob.childrenIds.push(childJobId);
      }
    }

    // Persist changes to SQLite if storage is available
    if (this.storage) {
      this.storage.updateJobData(childJobId, childJob.data);
      if (parentJob) {
        this.storage.updateJobChildrenIds(parentJobId, parentJob.childrenIds);
      }
    }

    // Handle race condition: child may have already terminally failed
    // before parent linkage was established (parentId was 'pending').
    // If so, propagate failParentOnFailure now with the real parent ID.
    const childLoc = this.jobIndex.get(childJobId);
    if (childLoc?.type === 'dlq' && childJob.failParentOnFailure) {
      this.moveParentToFailed(parentJobId, childJob, 'Child job failed').catch(() => {
        // Best-effort: parent may already be in DLQ or removed
      });
    }
    if (
      childLoc?.type === 'dlq' &&
      (childJob.removeDependencyOnFailure ||
        childJob.ignoreDependencyOnFailure ||
        childJob.continueParentOnFailure)
    ) {
      this.onChildDependencyOption(childJob, 'Child job failed');
    }
  }

  getJobByCustomId(customId: string): Job | null {
    return queryOps.getJobByCustomId(customId, this.contextFactory.getQueryContext());
  }

  getProgress(jobId: JobId) {
    return queryOps.getJobProgress(jobId, this.contextFactory.getQueryContext());
  }

  count(queue: string): number {
    return queueControl.getQueueCount(queue, this.contextFactory.getQueueControlContext());
  }

  // ============ Queue Control ============

  pause(queue: string): void {
    queueControl.pauseQueue(queue, this.contextFactory.getQueueControlContext());
    this.dashboardEmit?.('queue:paused', { queue });
    this.eventsManager.broadcast({
      eventType: EventType.Paused,
      queue,
      jobId: '' as JobId,
      timestamp: Date.now(),
    });
  }

  resume(queue: string): void {
    queueControl.resumeQueue(queue, this.contextFactory.getQueueControlContext());
    this.dashboardEmit?.('queue:resumed', { queue });
    this.eventsManager.broadcast({
      eventType: EventType.Resumed,
      queue,
      jobId: '' as JobId,
      timestamp: Date.now(),
    });
  }

  isPaused(queue: string): boolean {
    return queueControl.isQueuePaused(queue, this.contextFactory.getQueueControlContext());
  }

  drain(queue: string): number {
    const count = queueControl.drainQueue(queue, this.contextFactory.getQueueControlContext());
    if (count > 0) this.dashboardEmit?.('queue:drained', { queue, count });
    return count;
  }

  obliterate(queue: string): void {
    queueControl.obliterateQueue(queue, this.contextFactory.getQueueControlContext());
    dlqOps.purgeDlqJobs(queue, this.contextFactory.getDlqContext());

    // obliterateQueue() clears the waiting/delayed shard only. Active jobs in
    // processingShards, plus completed/result/log/lock state in global indexes,
    // plus SQLite rows, all survive unless we purge them here.
    const toDrop = new Set<JobId>();
    for (const [jid, loc] of this.jobIndex) {
      if (loc.type === 'processing') {
        const job = this.processingShards[loc.shardIdx]?.get(jid);
        if (job?.queue === queue) toDrop.add(jid);
      } else if (loc.queueName === queue) {
        toDrop.add(jid);
      }
    }

    for (const jid of toDrop) {
      const loc = this.jobIndex.get(jid);
      if (loc?.type === 'processing') {
        this.processingShards[loc.shardIdx]?.delete(jid);
      }
      this.jobIndex.delete(jid);
      this.completedJobs.delete(jid);
      this.completedJobsData.delete(jid);
      this.jobResults.delete(jid);
      this.jobLogs.delete(jid);
      this.jobLocks.delete(jid);
      this.failedChildrenValues.delete(jid);
      this.ignoredChildrenFailures.delete(jid);
      this.pendingDepChecks.delete(jid);
      this.stalledCandidates.delete(jid);
      this.repeatChain.delete(jid);
      this.storage?.deleteJob(jid);
    }

    // repeatChain maps oldId → newId; drop rows whose value is now a ghost.
    const chainKeysToDelete: JobId[] = [];
    for (const [oldId, newId] of this.repeatChain) {
      if (toDrop.has(newId)) chainKeysToDelete.push(oldId);
    }
    for (const oldId of chainKeysToDelete) this.repeatChain.delete(oldId);

    // Drop customId → JobId mappings that point at a dropped job
    const customIdsToDelete: string[] = [];
    for (const [cid, jid] of this.customIdMap.entries()) {
      if (toDrop.has(jid)) customIdsToDelete.push(cid);
    }
    for (const cid of customIdsToDelete) this.customIdMap.delete(cid);

    this.unregisterQueueName(queue);
    this.dashboardEmit?.('queue:obliterated', { queue });
    this.dashboardEmit?.('queue:removed', { queue });
  }

  listQueues(): string[] {
    return Array.from(this.queueNamesCache);
  }

  private registerQueueName(queue: string): void {
    const isNew = !this.queueNamesCache.has(queue);
    this.queueNamesCache.add(queue);
    if (isNew) this.dashboardEmit?.('queue:created', { queue });
  }

  private unregisterQueueName(queue: string): void {
    this.queueNamesCache.delete(queue);
  }

  clean(queue: string, graceMs: number, state?: string, limit?: number): JobId[] {
    return queueControl.cleanQueue(
      queue,
      graceMs,
      this.contextFactory.getQueueControlContext(),
      state,
      limit
    );
  }

  getCountsPerPriority(queue: string): Record<number, number> {
    const idx = shardIndex(queue);
    const counts = this.shards[idx].getCountsPerPriority(queue);
    return Object.fromEntries(counts);
  }

  getJobs(
    queue: string,
    options: {
      state?: string | string[];
      start?: number;
      end?: number;
      asc?: boolean;
    } = {}
  ): Job[] {
    const idx = shardIndex(queue);
    return queryOps.getJobs(queue, idx, options, {
      ...this.contextFactory.getQueryContext(),
      shardCount: SHARD_COUNT,
    });
  }

  // ============ DLQ Operations ============

  getDlq(queue: string, count?: number): Job[] {
    return dlqOps.getDlqJobs(queue, this.contextFactory.getDlqContext(), count);
  }

  getDlqEntries(queue: string, filter?: DlqFilter): DlqEntry[] {
    return dlqOps.getDlqEntries(queue, this.contextFactory.getDlqContext(), filter);
  }

  getDlqCount(queue: string): number {
    return this.shards[shardIndex(queue)].getDlqCount(queue);
  }

  getDlqStats(queue: string): DlqStats {
    return dlqOps.getDlqStats(queue, this.contextFactory.getDlqContext());
  }

  retryDlq(queue: string, jobId?: JobId): number {
    return dlqOps.retryDlqJobs(queue, this.contextFactory.getDlqContext(), jobId);
  }

  purgeDlq(queue: string): number {
    return dlqOps.purgeDlqJobs(queue, this.contextFactory.getDlqContext());
  }

  retryCompleted(queue: string, jobId?: JobId): number {
    return dlqOps.retryCompletedJobs(queue, this.contextFactory.getRetryCompletedContext(), jobId);
  }

  // ============ Rate Limiting ============

  setRateLimit(queue: string, limit: number): void {
    this.shards[shardIndex(queue)].setRateLimit(queue, limit);
  }

  clearRateLimit(queue: string): void {
    this.shards[shardIndex(queue)].clearRateLimit(queue);
  }

  setConcurrency(queue: string, limit: number): void {
    this.shards[shardIndex(queue)].setConcurrency(queue, limit);
  }

  clearConcurrency(queue: string): void {
    this.shards[shardIndex(queue)].clearConcurrency(queue);
  }

  /** Get rate limit and concurrency limit for a queue */
  getQueueLimits(queue: string): { rateLimit: number | null; concurrencyLimit: number | null } {
    const state = this.shards[shardIndex(queue)].getState(queue);
    return { rateLimit: state.rateLimit, concurrencyLimit: state.concurrencyLimit };
  }

  /** Get all job results (for cloud telemetry) */
  getAllJobResults(): Map<JobId, unknown> {
    const map = new Map<JobId, unknown>();
    for (const [k, v] of this.jobResults.entries()) map.set(k, v);
    return map;
  }

  /** Get all job logs (for cloud telemetry) */
  getAllJobLogs(): Map<JobId, JobLogEntry[]> {
    const map = new Map<JobId, JobLogEntry[]>();
    for (const [k, v] of this.jobLogs.entries()) map.set(k, v);
    return map;
  }

  /** Get all active job locks (for cloud telemetry) */
  getAllJobLocks(): Map<JobId, JobLock> {
    return this.jobLocks;
  }

  // ============ Stall & DLQ Config ============

  setStallConfig(queue: string, config: Record<string, unknown>): void {
    this.shards[shardIndex(queue)].setStallConfig(queue, config);
  }

  getStallConfig(queue: string): StallConfig {
    return this.shards[shardIndex(queue)].getStallConfig(queue);
  }

  setDlqConfig(queue: string, config: Record<string, unknown>): void {
    this.shards[shardIndex(queue)].setDlqConfig(queue, config);
  }

  getDlqConfig(queue: string): DlqConfig {
    return this.shards[shardIndex(queue)].getDlqConfig(queue);
  }

  /** Get extended telemetry data for cloud snapshot */
  getCloudTelemetry(queueNames: string[]) {
    const perQueue: Record<
      string,
      { uniqueKeys: number; activeGroups: number; waitingDeps: number; waitingChildren: number }
    > = {};

    for (const name of queueNames) {
      const idx = shardIndex(name);
      const shard = this.shards[idx];
      const uniqueMap = shard.uniqueKeys.get(name);
      const groupSet = shard.activeGroups.get(name);

      let waitingDeps = 0;
      for (const j of shard.waitingDeps.values()) {
        if (j.queue === name) waitingDeps++;
      }
      let waitingChildren = 0;
      for (const j of shard.waitingChildren.values()) {
        if (j.queue === name) waitingChildren++;
      }

      perQueue[name] = {
        uniqueKeys: uniqueMap?.size ?? 0,
        activeGroups: groupSet?.size ?? 0,
        waitingDeps,
        waitingChildren,
      };
    }

    return {
      perQueue,
      eventSubscribers: this.eventsManager.subscriberCount,
      pendingDepChecks: this.pendingDepChecks.size,
    };
  }

  // ============ Job Management ============

  async cancel(jobId: JobId): Promise<boolean> {
    return jobMgmt.cancelJob(jobId, this.contextFactory.getJobMgmtContext());
  }

  async updateProgress(jobId: JobId, progress: number, message?: string): Promise<boolean> {
    return jobMgmt.updateJobProgress(
      jobId,
      progress,
      this.contextFactory.getJobMgmtContext(),
      message
    );
  }

  async updateJobData(jobId: JobId, data: unknown): Promise<boolean> {
    return jobMgmt.updateJobData(jobId, data, this.contextFactory.getJobMgmtContext());
  }

  async changePriority(jobId: JobId, priority: number, lifo?: boolean): Promise<boolean> {
    return jobMgmt.changeJobPriority(
      jobId,
      priority,
      this.contextFactory.getJobMgmtContext(),
      lifo
    );
  }

  async promote(jobId: JobId): Promise<boolean> {
    return jobMgmt.promoteJob(jobId, this.contextFactory.getJobMgmtContext());
  }

  async moveToDelayed(jobId: JobId, delay: number): Promise<boolean> {
    return jobMgmt.moveJobToDelayed(jobId, delay, this.contextFactory.getJobMgmtContext());
  }

  async changeDelay(jobId: JobId, delay: number): Promise<boolean> {
    const ctx = this.contextFactory.getJobMgmtContext();
    const loc = ctx.jobIndex.get(jobId);
    // Jobs already in queue (waiting/delayed): mutate runAt in place
    if (loc?.type === 'queue') {
      return jobTransitions.changeWaitingDelay(jobId, delay, ctx);
    }
    // Active/processing jobs: move back to queue with new delay
    return jobMgmt.moveJobToDelayed(jobId, delay, ctx);
  }

  async moveActiveToWait(jobId: JobId): Promise<boolean> {
    return jobTransitions.moveActiveToWait(jobId, this.contextFactory.getJobMgmtContext());
  }

  async changeWaitingDelay(jobId: JobId, delay: number): Promise<boolean> {
    return jobTransitions.changeWaitingDelay(jobId, delay, this.contextFactory.getJobMgmtContext());
  }

  async moveToWaitingChildren(jobId: JobId): Promise<boolean> {
    return jobTransitions.moveToWaitingChildren(jobId, this.contextFactory.getJobMgmtContext());
  }

  async extendLock(
    jobId: JobId | string,
    token: string | null,
    duration: number
  ): Promise<boolean> {
    const jid = typeof jobId === 'string' ? (jobId as JobId) : jobId;
    const ctx = this.contextFactory.getLockContext();

    if (token) {
      // Use provided token for verification
      return lockMgr.renewJobLock(jid, token, ctx, duration);
    }

    // Fall back to looking up the token
    const lockInfo = lockMgr.getLockInfo(jid, ctx);
    if (lockInfo) {
      return lockMgr.renewJobLock(jid, lockInfo.token, ctx, duration);
    }
    return false;
  }

  async discard(jobId: JobId): Promise<boolean> {
    return jobMgmt.discardJob(jobId, this.contextFactory.getJobMgmtContext());
  }

  // ============ Job Logs ============

  addLog(jobId: JobId, message: string, level: 'info' | 'warn' | 'error' = 'info'): boolean {
    return logsOps.addJobLog(jobId, message, this.contextFactory.getLogsContext(), level);
  }

  getLogs(jobId: JobId): JobLogEntry[] {
    return logsOps.getJobLogs(jobId, this.contextFactory.getLogsContext());
  }

  clearLogs(jobId: JobId, keepLogs?: number): void {
    logsOps.clearJobLogs(jobId, this.contextFactory.getLogsContext(), keepLogs);
  }

  // ============ Metrics ============

  getPerQueueStats() {
    return statsMgr.getPerQueueStats(this.contextFactory.getStatsContext(), this.queueNamesCache);
  }

  getPrometheusMetrics(): string {
    return generatePrometheusMetrics(
      this.getStats(),
      this.workerManager,
      this.webhookManager,
      this.getPerQueueStats()
    );
  }

  // ============ Cron Operations ============

  addCron(input: CronJobInput): CronJob {
    // On upsert with preventOverlap, remove any orphaned queued job (#73).
    // Between disconnect and reconnect, a cron tick may push a job that no
    // worker consumes.  When the client re-upserts, that stale job would be
    // picked up immediately by the new worker.  Remove it so the cron
    // scheduler creates a fresh job at the correct next-run time.
    if (input.preventOverlap) {
      const uniqueKey = input.uniqueKey ?? `cron:${input.name}`;
      this.removeOrphanedCronJob(input.queue, uniqueKey);
    }

    const cron = this.cronScheduler.add(input);
    this.storage?.saveCron(cron);
    return cron;
  }

  /** Remove an orphaned cron job from the queue by its uniqueKey */
  private removeOrphanedCronJob(queue: string, uniqueKey: string): void {
    const idx = shardIndex(queue);
    const shard = this.shards[idx];
    const entry = shard.getUniqueKeyEntry(queue, uniqueKey);
    if (!entry) return;

    const jobId = entry.jobId;
    const location = this.jobIndex.get(jobId);
    // Only remove waiting/queued jobs, never processing jobs
    if (location?.type !== 'queue') return;

    const job = shard.getQueue(queue).remove(jobId);
    if (job) {
      shard.decrementQueued(jobId);
      shard.releaseUniqueKey(queue, uniqueKey);
      this.jobIndex.delete(jobId);
      this.storage?.deleteJob(jobId);
    }
  }

  removeCron(name: string): boolean {
    const removed = this.cronScheduler.remove(name);
    if (removed) this.storage?.deleteCron(name);
    return removed;
  }

  getCron(name: string): CronJob | undefined {
    return this.cronScheduler.get(name);
  }

  listCrons(): CronJob[] {
    return this.cronScheduler.list();
  }

  // ============ Events ============

  /** Register dashboard event emitter (for WS pub/sub) */
  setDashboardEmit(fn: (event: string, data: Record<string, unknown>) => void): void {
    this.dashboardEmit = fn;
    this.cronScheduler.setDashboardEmit(fn);
    this.webhookManager.setDashboardEmit(fn);
    this.workerManager.setDashboardEmit(fn);
    if (this.recoveryStats) {
      fn('server:recovered', this.recoveryStats);
      this.recoveryStats = null;
    }
  }

  /** Emit a dashboard event (callable from handlers) */
  emitDashboardEvent(event: string, data: Record<string, unknown>): void {
    this.dashboardEmit?.(event, data);
  }

  subscribe(callback: (event: JobEvent) => void): () => void {
    return this.eventsManager.subscribe(callback);
  }

  waitForJobCompletion(jobId: JobId, timeoutMs: number): Promise<boolean> {
    return this.eventsManager.waitForJobCompletion(jobId, timeoutMs);
  }

  /** Register worker with dashboard event */
  registerWorker(name: string, queues: string[], concurrency?: number, opts?: CreateWorkerOptions) {
    const worker = this.workerManager.register(name, queues, concurrency, opts);
    this.dashboardEmit?.('worker:connected', {
      workerId: worker.id,
      name: worker.name,
      queues: worker.queues,
      hostname: worker.hostname,
      pid: worker.pid,
    });
    return worker;
  }

  /** Unregister worker with dashboard event */
  unregisterWorker(workerId: string): boolean {
    const result = this.workerManager.unregister(workerId);
    if (result) {
      this.dashboardEmit?.('worker:disconnected', { workerId });
    }
    return result;
  }

  /** Unregister all workers associated with a TCP client ID */
  unregisterWorkersByClientId(clientId: string): number {
    return this.workerManager.unregisterByClientId(clientId);
  }

  // ============ Internal State Access ============

  getJobIndex(): Map<JobId, JobLocation> {
    return this.jobIndex;
  }

  getCompletedJobs(): SetLike<JobId> {
    return this.completedJobs;
  }

  getShards(): Shard[] {
    return this.shards;
  }

  private onJobCompleted(completedId: JobId): void {
    // Release flow-failure tracking once a parent job reaches terminal completion
    // (AUDIT H8). The parent consumes these values via getFailedChildrenValues()/
    // getIgnoredChildrenFailures() while it is processing (before it acks), so by the
    // time it completes they are no longer needed. Keyed by parentId; obliterate()
    // and shutdown() clear them too. Without this, every parent completion that
    // involved a failed child with continueParentOnFailure / ignoreDependencyOnFailure
    // leaked an entry permanently.
    this.failedChildrenValues.delete(completedId);
    this.ignoredChildrenFailures.delete(completedId);

    this.pendingDepChecks.add(completedId);
    this.scheduleDependencyFlush();
    void this.checkFlowCompleted(completedId);
  }

  /** Check if completing this job completes an entire flow */
  private async checkFlowCompleted(completedId: JobId): Promise<void> {
    const job = await this.getJob(completedId);
    if (!job?.parentId) return;

    const parent = await this.getJob(job.parentId);
    if (!parent?.childrenIds || parent.childrenIds.length === 0) return;

    const allDone = parent.childrenIds.every((childId) => this.completedJobs.has(childId));
    if (allDone) {
      this.dashboardEmit?.('flow:completed', {
        parentJobId: String(parent.id),
        queue: parent.queue,
        childrenCount: parent.childrenIds.length,
      });
    }
  }

  /**
   * BullMQ v5: failParentOnFailure — when a child terminally fails
   * and has failParentOnFailure: true, also fail the parent job.
   */
  private failParentOnChildFailure(childJob: Job, error: string | undefined): void {
    const parentId = childJob.parentId;
    if (!parentId) return;

    this.moveParentToFailed(parentId, childJob, error).catch(() => {
      // Best-effort: parent may already be in DLQ or removed
    });
  }

  /** Move a parent job to DLQ/failed state because a child failed */
  private async moveParentToFailed(
    parentId: JobId,
    childJob: Job,
    error: string | undefined
  ): Promise<void> {
    const parentJob = await this.getJob(parentId);
    if (!parentJob) return;

    const parentLoc = this.jobIndex.get(parentId);
    if (!parentLoc) return;

    // Only fail parent if it's in a queue (waitingDeps/waitingChildren) state
    if (parentLoc.type !== 'queue') return;

    const idx = shardIndex(parentJob.queue);
    await withWriteLock(this.shardLocks[idx], () => {
      // Re-check inside lock to prevent duplicate DLQ entries (TOCTOU guard)
      if (this.jobIndex.get(parentId)?.type !== 'queue') return;

      const shard = this.shards[idx];

      // Remove from waitingDeps if present
      if (shard.waitingDeps.has(parentId)) {
        shard.waitingDeps.delete(parentId);
        shard.unregisterDependencies(parentId, parentJob.dependsOn);
      }

      // Remove from waitingChildren if present
      if (shard.waitingChildren.has(parentId)) {
        shard.waitingChildren.delete(parentId);
      }

      // Remove from queue if present
      const queue = shard.getQueue(parentJob.queue);
      if (queue.find(parentId)) {
        queue.remove(parentId);
        shard.decrementQueued(parentId);
      }

      // Add parent to DLQ with child_failed reason
      const failError = `Child job ${childJob.id} failed: ${error ?? 'unknown error'}`;
      const entry = shard.addToDlq(parentJob, FailureReason.Unknown, failError);
      this.jobIndex.set(parentId, { type: 'dlq', queueName: parentJob.queue });
      this.storage?.saveDlqEntry(entry);
      this.storage?.deleteJob(parentId);
    });

    // Parent reached a terminal (DLQ) state — release its flow-failure tracking.
    this.failedChildrenValues.delete(parentId);
    this.ignoredChildrenFailures.delete(parentId);

    // Broadcast failed event for parent
    this.eventsManager.broadcast({
      eventType: 'failed' as EventType,
      queue: parentJob.queue,
      jobId: parentId,
      timestamp: Date.now(),
      error: `Child job ${childJob.id} failed: ${error ?? 'unknown error'}`,
      data: parentJob.data,
    });

    this.dashboardEmit?.('flow:failed', {
      parentJobId: String(parentId),
      failedChildId: String(childJob.id),
      queue: parentJob.queue,
      error: error ?? 'Child job failed',
    });
  }

  /**
   * Handle child dependency options: removeDependencyOnFailure, ignoreDependencyOnFailure, continueParentOnFailure
   */
  private onChildDependencyOption(childJob: Job, error: string | undefined): void {
    if (!childJob.parentId) return;

    if (childJob.continueParentOnFailure) {
      this.continueParentOnChildFailure(childJob, error).catch(() => {});
    } else {
      // removeDependencyOnFailure or ignoreDependencyOnFailure
      this.removeChildFromParentDeps(childJob, error, childJob.ignoreDependencyOnFailure).catch(
        () => {}
      );
    }
  }

  /**
   * continueParentOnFailure: move parent to queue when a child fails.
   * Stores the failure info for getFailedChildrenValues().
   */
  private async continueParentOnChildFailure(
    childJob: Job,
    error: string | undefined
  ): Promise<void> {
    const parentId = childJob.parentId;
    if (!parentId) return;
    const parentJob = await this.getJob(parentId);
    if (!parentJob) return;

    const parentLoc = this.jobIndex.get(parentId);
    if (parentLoc?.type !== 'queue') return;

    // Store failed child value
    const childKey = `${childJob.queue}:${childJob.id}`;
    const existing = this.failedChildrenValues.get(parentId) ?? {};
    existing[childKey] = error ?? 'unknown error';
    this.failedChildrenValues.set(parentId, existing);

    const idx = shardIndex(parentJob.queue);
    await this.promoteParentAfterChildFailure(parentId, parentJob, idx);
  }

  /**
   * Move a flow parent from its waiting state into the run queue after a child failure
   * (continueParentOnFailure / last-dependency resolution). Deferred to the next tick
   * so the failed child's worker batch drains first — the parent is then picked up on
   * a subsequent poll rather than racing to completion in the same synchronous cascade.
   */
  private async promoteParentAfterChildFailure(
    parentId: JobId,
    parentJob: Job,
    idx: number
  ): Promise<void> {
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, 0);
      timer.unref?.();
    });

    let promoted = false;
    await withWriteLock(this.shardLocks[idx], () => {
      // TOCTOU guard
      if (this.jobIndex.get(parentId)?.type !== 'queue') return;

      const shard = this.shards[idx];

      // Remove from waitingDeps
      if (shard.waitingDeps.has(parentId)) {
        shard.waitingDeps.delete(parentId);
        shard.unregisterDependencies(parentId, parentJob.dependsOn);
      }
      // Remove from waitingChildren
      if (shard.waitingChildren.has(parentId)) {
        shard.waitingChildren.delete(parentId);
      }
      // Add to queue
      const queue = shard.getQueue(parentJob.queue);
      if (!queue.find(parentId)) {
        const now = Date.now();
        parentJob.runAt = now;
        queue.push(parentJob);
        shard.incrementQueued(parentId, false, parentJob.createdAt, parentJob.queue, now);
        this.jobIndex.set(parentId, { type: 'queue', shardIdx: idx, queueName: parentJob.queue });
        shard.notify();
        promoted = true;
      }
    });

    if (promoted) {
      this.eventsManager.broadcast({
        eventType: 'waiting' as EventType,
        queue: parentJob.queue,
        jobId: parentId,
        timestamp: Date.now(),
        prev: 'waiting-children',
      });
    }
  }

  /**
   * removeDependencyOnFailure / ignoreDependencyOnFailure:
   * Remove child from parent's pending deps. If last dep, promote parent.
   * If ignoreDependencyOnFailure, also store failure reason.
   */
  private async removeChildFromParentDeps(
    childJob: Job,
    error: string | undefined,
    storeIgnored: boolean
  ): Promise<void> {
    const parentId = childJob.parentId;
    if (!parentId) return;
    const parentJob = await this.getJob(parentId);
    if (!parentJob) return;

    const parentLoc = this.jobIndex.get(parentId);
    if (parentLoc?.type !== 'queue') return;

    if (storeIgnored) {
      const childKey = `${childJob.queue}:${childJob.id}`;
      const existing = this.ignoredChildrenFailures.get(parentId) ?? {};
      existing[childKey] = error ?? 'unknown error';
      this.ignoredChildrenFailures.set(parentId, existing);
    }

    const idx = shardIndex(parentJob.queue);
    // Remove the failed child from the parent's pending deps synchronously so
    // dependency tracking stays consistent; defer only the promotion decision.
    let readyToPromote = false;
    await withWriteLock(this.shardLocks[idx], () => {
      if (this.jobIndex.get(parentId)?.type !== 'queue') return;

      const shard = this.shards[idx];
      const parentInDeps = shard.waitingDeps.get(parentId);
      if (!parentInDeps) return;

      // Remove child from parent's dependsOn
      const depIndex = parentJob.dependsOn.indexOf(childJob.id);
      if (depIndex !== -1) {
        parentJob.dependsOn.splice(depIndex, 1);
        shard.unregisterDependencies(parentId, [childJob.id]);
      }

      // If no more pending deps, the parent is ready to be promoted
      readyToPromote =
        parentJob.dependsOn.length === 0 ||
        parentJob.dependsOn.every((dep) => this.completedJobs.has(dep));
    });

    if (readyToPromote) {
      await this.promoteParentAfterChildFailure(parentId, parentJob, idx);
    }
  }

  /**
   * Get failed children values for a parent job (populated by continueParentOnFailure).
   */
  async getFailedChildrenValues(parentJobId: JobId): Promise<Record<string, string>> {
    return this.failedChildrenValues.get(parentJobId) ?? {};
  }

  /**
   * Get ignored children failures for a parent job (populated by ignoreDependencyOnFailure).
   */
  async getIgnoredChildrenFailures(parentJobId: JobId): Promise<Record<string, string>> {
    return this.ignoredChildrenFailures.get(parentJobId) ?? {};
  }

  /**
   * Remove a child job's dependency from its parent.
   * If this was the last pending child, promotes parent to queue.
   * Throws if the job has no parent.
   */
  async removeChildDependency(childJobId: JobId): Promise<boolean> {
    const childJob = await this.getJob(childJobId);
    if (!childJob) throw new Error(`Job not found: ${childJobId}`);
    if (!childJob.parentId) throw new Error(`Job ${childJobId} has no parent`);

    const parentId = childJob.parentId;
    const parentJob = await this.getJob(parentId);
    if (!parentJob) return false;

    const parentLoc = this.jobIndex.get(parentId);
    if (parentLoc?.type !== 'queue') return false;

    const idx = shardIndex(parentJob.queue);
    let promoted = false;
    await withWriteLock(this.shardLocks[idx], () => {
      if (this.jobIndex.get(parentId)?.type !== 'queue') return;

      const shard = this.shards[idx];
      const parentInDeps = shard.waitingDeps.get(parentId);
      if (!parentInDeps) return;

      const depIndex = parentJob.dependsOn.indexOf(childJobId);
      if (depIndex !== -1) {
        parentJob.dependsOn.splice(depIndex, 1);
        shard.unregisterDependencies(parentId, [childJobId]);
      }

      const allDone =
        parentJob.dependsOn.length === 0 ||
        parentJob.dependsOn.every((dep) => this.completedJobs.has(dep));

      if (allDone) {
        shard.waitingDeps.delete(parentId);
        const now = Date.now();
        parentJob.runAt = now;
        shard.getQueue(parentJob.queue).push(parentJob);
        shard.incrementQueued(parentId, false, parentJob.createdAt, parentJob.queue, now);
        this.jobIndex.set(parentId, { type: 'queue', shardIdx: idx, queueName: parentJob.queue });
        shard.notify();
        promoted = true;
      }
    });

    if (promoted) {
      this.eventsManager.broadcast({
        eventType: 'waiting' as EventType,
        queue: parentJob.queue,
        jobId: parentId,
        timestamp: Date.now(),
        prev: 'waiting-children',
      });
    }

    return true;
  }

  /**
   * Remove all unprocessed (waiting/delayed) children of a parent job.
   * Active, completed, and failed children are not affected.
   */
  async removeUnprocessedChildren(parentJobId: JobId): Promise<void> {
    const parent = await this.getJob(parentJobId);
    if (!parent?.childrenIds || parent.childrenIds.length === 0) return;

    for (const childId of parent.childrenIds) {
      const loc = this.jobIndex.get(childId);
      // Only cancel children in 'queue' state (waiting/delayed), not active/completed/failed
      if (loc?.type === 'queue') {
        try {
          await this.cancel(childId);
        } catch {
          // Best-effort
        }
      }
    }
  }

  private onJobsCompleted(completedIds: JobId[]): void {
    for (const id of completedIds) this.pendingDepChecks.add(id);
    this.scheduleDependencyFlush();
  }

  /**
   * Schedule dependency flush on next microtask.
   * Coalesces multiple onJobCompleted calls in the same tick.
   */
  private scheduleDependencyFlush(): void {
    if (this.depFlushScheduled) return;
    this.depFlushScheduled = true;
    queueMicrotask(() => {
      this.depFlushScheduled = false;
      if (this.depFlushRunning) return;
      void this.runDependencyFlush();
    });
  }

  /**
   * Run dependency flush with reentrancy loop.
   * The while-loop handles new completions arriving during async lock waits.
   */
  private async runDependencyFlush(): Promise<void> {
    this.depFlushRunning = true;
    try {
      while (this.pendingDepChecks.size > 0) {
        await processPendingDependencies(this.contextFactory.getBackgroundContext());
        handleTaskSuccess('dependency');
      }
    } catch (err) {
      handleTaskError('dependency', err);
    } finally {
      this.depFlushRunning = false;
      if (this.pendingDepChecks.size > 0) {
        this.scheduleDependencyFlush();
      }
    }
  }

  private hasPendingDeps(): boolean {
    for (const shard of this.shards) {
      if (shard.waitingDeps.size > 0) return true;
    }
    return false;
  }

  // ============ Stats ============

  getStats() {
    return statsMgr.getStats(this.contextFactory.getStatsContext(), this.cronScheduler);
  }

  /** Get summary of all queues: name, paused, counts */
  getQueuesSummary(): Array<{
    name: string;
    paused: boolean;
    counts: { waiting: number; active: number; completed: number; failed: number; delayed: number };
  }> {
    const ctx = this.contextFactory.getStatsContext();
    const queues = this.listQueues();
    const result: Array<{
      name: string;
      paused: boolean;
      counts: {
        waiting: number;
        active: number;
        completed: number;
        failed: number;
        delayed: number;
      };
    }> = [];
    for (const name of queues) {
      const c = statsMgr.getQueueJobCounts(name, ctx);
      result.push({
        name,
        paused: this.isPaused(name),
        counts: {
          waiting: c.waiting,
          active: c.active,
          completed: c.completed,
          failed: c.failed,
          delayed: c.delayed,
        },
      });
    }
    return result;
  }

  /** Get job counts for a specific queue */
  getQueueJobCounts(queueName: string) {
    return statsMgr.getQueueJobCounts(queueName, this.contextFactory.getStatsContext());
  }

  getMemoryStats() {
    return statsMgr.getMemoryStats(this.contextFactory.getStatsContext());
  }

  /** Get storage health status (disk full detection) */
  getStorageStatus(): { diskFull: boolean; error: string | null; since: number | null } {
    if (!this.storage) return { diskFull: false, error: null, since: null };
    return this.storage.getDiskFullStatus();
  }

  compactMemory(): void {
    statsMgr.compactMemory(this.contextFactory.getStatsContext());
    this.dashboardEmit?.('memory:compacted', {});
  }

  // ============ Lifecycle ============

  shutdown(): void {
    this.cronScheduler.stop();
    this.workerManager.stop();
    this.eventsManager.clear();
    if (this.backgroundTaskHandles) {
      bgTasks.stopBackgroundTasks(this.backgroundTaskHandles);
    }
    this.storage?.close();

    // Clear in-memory collections
    this.jobIndex.clear();
    this.completedJobs.clear();
    this.completedJobsData.clear();
    this.jobResults.clear();
    this.jobLogs.clear();
    this.customIdMap.clear();
    this.pendingDepChecks.clear();
    this.queueNamesCache.clear();
    this.jobLocks.clear();
    this.stalledCandidates.clear();
    this.clientJobs.clear();
    this.repeatChain.clear();
    this.failedChildrenValues.clear();
    this.ignoredChildrenFailures.clear();
    for (const shard of this.processingShards) shard.clear();
    for (const shard of this.shards) {
      shard.waitingDeps.clear();
      shard.dependencyIndex.clear();
      shard.waitingChildren.clear();
      shard.uniqueKeys.clear();
      shard.activeGroups.clear();
    }
  }
}
