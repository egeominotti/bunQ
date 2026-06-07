/**
 * Queue
 * BullMQ-style queue for job management
 */

import { TcpConnectionPool, getSharedPool, releaseSharedPool } from '../tcpPool';
import type {
  Job,
  JobOptions,
  QueueOptions,
  StallConfig,
  DlqConfig,
  DlqEntry,
  DlqStats,
  DlqFilter,
  ConnectionOptions,
  ChangePriorityOpts,
  GetDependenciesOpts,
  JobDependencies,
  JobDependenciesCount,
  JobStateType,
} from '../types';
import { FORCE_EMBEDDED } from './helpers';
import { AddBatcher } from './addBatcher';
import { resolveToken } from '../resolveToken';
import { getSharedManager } from '../manager';

// Import operation modules
import * as addOps from './operations/add';
import * as queryOps from './operations/query';
import * as countsOps from './operations/counts';
import * as controlOps from './operations/control';
import * as managementOps from './operations/management';
import * as stallOps from './stall';
import * as dlqOps from './dlq';
import * as rateLimitOps from './rateLimit';
import * as schedulerOps from './scheduler';
import type { RepeatOpts, JobTemplate, SchedulerInfo } from './scheduler';
import * as deduplicationOps from './deduplication';
import * as jobMoveOps from './jobMove';
import * as workersOps from './workers';
import * as bullmqCompatOps from './bullmqCompat';

/**
 * Queue class for adding and managing jobs
 */
export class Queue<T = unknown> {
  readonly name: string;
  /**
   * Server-side queue key. Equals `name` unless `prefixKey` is set, in which
   * case it is `prefixKey + name`. All TCP/embedded operations route through
   * this key so that two queues with the same logical name but different
   * `prefixKey` values are fully isolated on the broker.
   */
  private readonly queueKey: string;
  private readonly prefixKey: string;
  private readonly opts: QueueOptions;
  private readonly embedded: boolean;
  private readonly tcpPool: TcpConnectionPool | null;
  private readonly useSharedPool: boolean;
  private readonly addBatcher: AddBatcher<unknown> | null;

  constructor(name: string, opts: QueueOptions = {}) {
    this.name = name;
    this.prefixKey = opts.prefixKey ?? '';
    this.queueKey = this.prefixKey + name;
    this.opts = opts;
    this.embedded = opts.embedded ?? FORCE_EMBEDDED;

    if (this.embedded) {
      // Initialize shared manager with programmatic dataPath (if provided)
      getSharedManager(opts.dataPath);
      this.tcpPool = null;
      this.useSharedPool = false;
      this.addBatcher = null;
    } else {
      const connOpts: ConnectionOptions = opts.connection ?? {};
      const poolSize = connOpts.poolSize ?? 4;
      const token = resolveToken(connOpts.token);

      if (poolSize === 4 && !token) {
        this.tcpPool = getSharedPool({
          host: connOpts.host,
          port: connOpts.port,
          poolSize,
          pingInterval: connOpts.pingInterval,
          commandTimeout: connOpts.commandTimeout,
          maxCommandTimeouts: connOpts.maxCommandTimeouts,
          pipelining: connOpts.pipelining,
          maxInFlight: connOpts.maxInFlight,
        });
        this.useSharedPool = true;
      } else {
        this.tcpPool = new TcpConnectionPool({
          host: connOpts.host ?? 'localhost',
          port: connOpts.port ?? 6789,
          token,
          poolSize,
          pingInterval: connOpts.pingInterval,
          commandTimeout: connOpts.commandTimeout,
          maxCommandTimeouts: connOpts.maxCommandTimeouts,
          pipelining: connOpts.pipelining,
          maxInFlight: connOpts.maxInFlight,
        });
        this.useSharedPool = false;
      }

      // Initialize auto-batcher for TCP mode
      const autoBatch = opts.autoBatch;
      if (autoBatch?.enabled === false) {
        this.addBatcher = null;
      } else {
        this.addBatcher = new AddBatcher(
          {
            maxSize: autoBatch?.maxSize ?? 50,
            maxDelayMs: autoBatch?.maxDelayMs ?? 5,
          },
          (jobs) => addOps.addBulk(this.addCtx, jobs)
        );
      }
    }
  }

  // Context builders for modules.
  // `name` here is the **server-side** queue key (already includes prefixKey),
  // so every operation that forwards `ctx.name` to the broker is automatically
  // namespaced. The user-facing logical name lives on `this.name`.
  private get ctx() {
    return {
      name: this.queueKey,
      embedded: this.embedded,
      tcp: this.tcpPool,
      prefixKey: this.prefixKey,
    };
  }

  private get addCtx() {
    return {
      ...this.ctx,
      opts: this.opts,
      getJobState: (id: string) => this.getJobState(id),
      removeAsync: (id: string) => this.removeAsync(id),
      retryJob: (id: string) => this.retryJob(id),
      getChildrenValues: (id: string) => this.getChildrenValues(id),
      updateJobData: (id: string, data: unknown) => this.updateJobData(id, data),
      promoteJob: (id: string) => this.promoteJob(id),
      changeJobDelay: (id: string, delay: number) => this.changeJobDelay(id, delay),
      changeJobPriority: (id: string, opts: ChangePriorityOpts) => this.changeJobPriority(id, opts),
      extendJobLock: (id: string, token: string, dur: number) => this.extendJobLock(id, token, dur),
      clearJobLogs: (id: string, keep?: number) => this.clearJobLogs(id, keep),
      getJobDependencies: (id: string, o?: GetDependenciesOpts) => this.getJobDependencies(id, o),
      getJobDependenciesCount: (id: string, o?: GetDependenciesOpts) =>
        this.getJobDependenciesCount(id, o),
      moveJobToCompleted: (id: string, r: unknown, t?: string) => this.moveJobToCompleted(id, r, t),
      moveJobToFailed: (id: string, e: Error, t?: string) => this.moveJobToFailed(id, e, t),
      moveJobToWait: (id: string, t?: string) => this.moveJobToWait(id, t),
      moveJobToDelayed: (id: string, ts: number, t?: string) => this.moveJobToDelayed(id, ts, t),
      moveJobToWaitingChildren: (
        id: string,
        t?: string,
        o?: { child?: { id: string; queue: string } }
      ) => this.moveJobToWaitingChildren(id, t, o),
      waitJobUntilFinished: (id: string, qe: unknown, ttl?: number) =>
        this.waitJobUntilFinished(id, qe, ttl),
    };
  }

  private get queryCtx() {
    return {
      ...this.ctx,
      getJobState: (id: string): Promise<JobStateType> => this.getJobState(id),
      removeAsync: (id: string): Promise<void> => this.removeAsync(id),
      retryJob: (id: string): Promise<void> => this.retryJob(id),
      getChildrenValues: (id: string): Promise<Record<string, unknown>> =>
        this.getChildrenValues(id),
      // Extended context for toPublicJob (full opts support)
      updateJobData: (id: string, data: unknown) => this.updateJobData(id, data),
      promoteJob: (id: string) => this.promoteJob(id),
      changeJobDelay: (id: string, delay: number) => this.changeJobDelay(id, delay),
      changeJobPriority: (id: string, opts: { priority: number; lifo?: boolean }) =>
        this.changeJobPriority(id, opts),
      extendJobLock: (id: string, token: string, dur: number) => this.extendJobLock(id, token, dur),
      clearJobLogs: (id: string, keep?: number) => this.clearJobLogs(id, keep),
      getJobDependencies: (id: string, o?: GetDependenciesOpts) => this.getJobDependencies(id, o),
      getJobDependenciesCount: (id: string, o?: GetDependenciesOpts) =>
        this.getJobDependenciesCount(id, o),
      // Move / wait callbacks
      moveJobToCompleted: (id: string, r: unknown, t?: string) => this.moveJobToCompleted(id, r, t),
      moveJobToFailed: (id: string, e: Error, t?: string) => this.moveJobToFailed(id, e, t),
      moveJobToWait: (id: string, t?: string) => this.moveJobToWait(id, t),
      moveJobToDelayed: (id: string, ts: number, t?: string) => this.moveJobToDelayed(id, ts, t),
      moveJobToWaitingChildren: (
        id: string,
        t?: string,
        o?: { child?: { id: string; queue: string } }
      ) => this.moveJobToWaitingChildren(id, t, o),
      waitJobUntilFinished: (id: string, qe: unknown, ttl?: number) =>
        this.waitJobUntilFinished(id, qe, ttl),
    };
  }

  private get moveCtx() {
    return {
      ...this.ctx,
      getJobState: (id: string) => this.getJobState(id),
      getJobDependencies: (id: string) => this.getJobDependencies(id),
    };
  }

  // ============ Add Operations ============
  add(name: string, data: T, opts?: JobOptions): Promise<Job<T>> {
    // Bypass batcher for durable jobs or when batcher is not active
    if (this.addBatcher && !opts?.durable) {
      return this.addBatcher.enqueue(name, data, opts) as Promise<Job<T>>;
    }
    return addOps.add(this.addCtx, name, data, opts);
  }
  addBulk(jobs: Array<{ name: string; data: T; opts?: JobOptions }>): Promise<Job<T>[]> {
    return addOps.addBulk(this.addCtx, jobs);
  }

  // ============ Query Operations ============
  getJob(id: string): Promise<Job<T> | null> {
    return queryOps.getJob(this.queryCtx, id);
  }
  getJobState(id: string): Promise<JobStateType> {
    return queryOps.getJobState(this.queryCtx, id);
  }
  getChildrenValues(id: string): Promise<Record<string, unknown>> {
    return queryOps.getChildrenValues(this.queryCtx, id);
  }
  getJobs(opts?: {
    state?: string | string[];
    start?: number;
    end?: number;
    asc?: boolean;
  }): Job<T>[] {
    return queryOps.getJobs(this.queryCtx, opts);
  }
  getJobsAsync(opts?: { state?: string | string[]; start?: number; end?: number; asc?: boolean }) {
    return queryOps.getJobsAsync(this.queryCtx, opts);
  }
  getWaiting(start?: number, end?: number) {
    return queryOps.getWaiting<T>(this.queryCtx, start, end);
  }
  getWaitingAsync(start?: number, end?: number) {
    return queryOps.getWaitingAsync<T>(this.queryCtx, start, end);
  }
  getDelayed(start?: number, end?: number) {
    return queryOps.getDelayed<T>(this.queryCtx, start, end);
  }
  getDelayedAsync(start?: number, end?: number) {
    return queryOps.getDelayedAsync<T>(this.queryCtx, start, end);
  }
  getActive(start?: number, end?: number) {
    return queryOps.getActive<T>(this.queryCtx, start, end);
  }
  getActiveAsync(start?: number, end?: number) {
    return queryOps.getActiveAsync<T>(this.queryCtx, start, end);
  }
  getCompleted(start?: number, end?: number) {
    return queryOps.getCompleted<T>(this.queryCtx, start, end);
  }
  getCompletedAsync(start?: number, end?: number) {
    return queryOps.getCompletedAsync<T>(this.queryCtx, start, end);
  }
  getFailed(start?: number, end?: number) {
    return queryOps.getFailed<T>(this.queryCtx, start, end);
  }
  getFailedAsync(start?: number, end?: number) {
    return queryOps.getFailedAsync<T>(this.queryCtx, start, end);
  }

  // ============ Count Operations ============
  getJobCounts() {
    return countsOps.getJobCounts(this.ctx);
  }
  getJobCountsAsync() {
    return countsOps.getJobCountsAsync(this.ctx);
  }
  getWaitingCount() {
    return countsOps.getWaitingCount(this.ctx);
  }
  getActiveCount() {
    return countsOps.getActiveCount(this.ctx);
  }
  getCompletedCount() {
    return countsOps.getCompletedCount(this.ctx);
  }
  getFailedCount() {
    return countsOps.getFailedCount(this.ctx);
  }
  getDelayedCount() {
    return countsOps.getDelayedCount(this.ctx);
  }
  count() {
    return countsOps.count(this.ctx);
  }
  countAsync() {
    return countsOps.countAsync(this.ctx);
  }
  getCountsPerPriority() {
    return countsOps.getCountsPerPriority(this.ctx);
  }
  getCountsPerPriorityAsync() {
    return countsOps.getCountsPerPriorityAsync(this.ctx);
  }

  // ============ Control Operations ============
  pause() {
    controlOps.pause(this.ctx);
  }
  resume() {
    controlOps.resume(this.ctx);
  }
  drain() {
    controlOps.drain(this.ctx);
  }
  obliterate() {
    controlOps.obliterate(this.ctx);
  }
  isPaused() {
    return controlOps.isPaused(this.ctx);
  }
  isPausedAsync() {
    return controlOps.isPausedAsync(this.ctx);
  }
  waitUntilReady() {
    return controlOps.waitUntilReady(this.ctx);
  }

  // ============ Management Operations ============
  remove(id: string) {
    managementOps.remove(this.ctx, id);
  }
  removeAsync(id: string) {
    return managementOps.removeAsync(this.ctx, id);
  }
  retryJob(id: string) {
    return managementOps.retryJob(this.ctx, id);
  }
  retryJobs(opts?: { state?: 'failed' | 'completed'; count?: number; timestamp?: number }) {
    return managementOps.retryJobs(this.ctx, opts);
  }
  clean(grace: number, limit: number, type?: string) {
    return managementOps.clean(
      this.ctx,
      grace,
      limit,
      type as 'completed' | 'wait' | 'active' | 'paused' | 'delayed' | 'failed'
    );
  }
  cleanAsync(grace: number, limit: number, type?: string) {
    return managementOps.cleanAsync(
      this.ctx,
      grace,
      limit,
      type as 'completed' | 'wait' | 'active' | 'paused' | 'delayed' | 'failed'
    );
  }
  promoteJobs(opts?: { count?: number }) {
    return managementOps.promoteJobs(this.ctx, opts);
  }
  promoteJob(id: string) {
    return managementOps.promoteJob(this.ctx, id);
  }
  updateJobProgress(id: string, progress: number | object) {
    return managementOps.updateJobProgress(this.ctx, id, progress);
  }
  getJobLogs(id: string, start?: number, end?: number, _asc?: boolean) {
    return managementOps.getJobLogs(this.ctx, id, start, end);
  }
  addJobLog(id: string, logRow: string, _keepLogs?: number) {
    return managementOps.addJobLog(this.ctx, id, logRow);
  }
  clearJobLogs(id: string, keepLogs?: number) {
    return managementOps.clearJobLogs(this.ctx, id, keepLogs);
  }
  updateJobData(id: string, data: unknown) {
    return managementOps.updateJobData(this.ctx, id, data);
  }
  changeJobDelay(id: string, delay: number) {
    return managementOps.changeJobDelay(this.ctx, id, delay);
  }
  changeJobPriority(id: string, opts: ChangePriorityOpts) {
    return managementOps.changeJobPriority(this.ctx, id, opts);
  }
  extendJobLock(id: string, token: string, duration: number) {
    return managementOps.extendJobLock(this.ctx, id, token, duration);
  }

  // ============ Stall Detection ============
  setStallConfig(config: Partial<StallConfig>) {
    stallOps.setStallConfig(this.ctx, config);
  }
  getStallConfig(): StallConfig {
    return stallOps.getStallConfig(this.ctx);
  }
  getStallConfigAsync(): Promise<StallConfig> {
    return stallOps.getStallConfigAsync(this.ctx);
  }

  // ============ DLQ Operations ============
  setDlqConfig(config: Partial<DlqConfig>) {
    dlqOps.setDlqConfig(this.ctx, config);
  }
  getDlqConfig(): DlqConfig {
    return dlqOps.getDlqConfig(this.ctx);
  }
  getDlqConfigAsync(): Promise<DlqConfig> {
    return dlqOps.getDlqConfigAsync(this.ctx);
  }
  getDlq(filter?: DlqFilter): DlqEntry<T>[] {
    return dlqOps.getDlq<T>(this.ctx, filter);
  }
  getDlqStats(): DlqStats {
    return dlqOps.getDlqStats(this.ctx);
  }
  retryDlq(id?: string) {
    return dlqOps.retryDlq(this.ctx, id);
  }
  retryDlqByFilter(filter: DlqFilter) {
    return dlqOps.retryDlqByFilter(this.ctx, filter);
  }
  purgeDlq() {
    return dlqOps.purgeDlq(this.ctx);
  }
  retryCompleted(id?: string) {
    return dlqOps.retryCompleted(this.ctx, id);
  }
  retryCompletedAsync(id?: string) {
    return dlqOps.retryCompletedAsync(this.ctx, id);
  }

  // ============ Rate Limit Operations ============
  setGlobalConcurrency(concurrency: number) {
    rateLimitOps.setGlobalConcurrency(this.ctx, concurrency);
  }
  removeGlobalConcurrency() {
    rateLimitOps.removeGlobalConcurrency(this.ctx);
  }
  getGlobalConcurrency() {
    return rateLimitOps.getGlobalConcurrency(this.ctx);
  }
  setGlobalRateLimit(max: number, duration?: number) {
    rateLimitOps.setGlobalRateLimit(this.ctx, max, duration);
  }
  removeGlobalRateLimit() {
    rateLimitOps.removeGlobalRateLimit(this.ctx);
  }
  getGlobalRateLimit() {
    return rateLimitOps.getGlobalRateLimit(this.ctx);
  }
  rateLimit(expireTimeMs: number) {
    return rateLimitOps.rateLimit(this.ctx, expireTimeMs);
  }
  getRateLimitTtl(maxJobs?: number) {
    return rateLimitOps.getRateLimitTtl(this.ctx, maxJobs);
  }
  isMaxed() {
    return rateLimitOps.isMaxed(this.ctx);
  }

  // ============ Scheduler Operations ============
  upsertJobScheduler(
    schedulerId: string,
    repeatOpts: RepeatOpts,
    jobTemplate?: JobTemplate<T>
  ): Promise<SchedulerInfo | null> {
    return schedulerOps.upsertJobScheduler(
      { ...this.ctx, defaultJobOptions: this.opts.defaultJobOptions },
      schedulerId,
      repeatOpts,
      jobTemplate
    );
  }
  removeJobScheduler(schedulerId: string) {
    return schedulerOps.removeJobScheduler(this.ctx, schedulerId);
  }
  getJobScheduler(schedulerId: string) {
    return schedulerOps.getJobScheduler(this.ctx, schedulerId);
  }
  getJobSchedulers(start?: number, end?: number, asc?: boolean) {
    return schedulerOps.getJobSchedulers(this.ctx, start, end, asc);
  }
  getJobSchedulersCount() {
    return schedulerOps.getJobSchedulersCount(this.ctx);
  }

  // ============ Deduplication Operations ============
  getDeduplicationJobId(deduplicationId: string) {
    return deduplicationOps.getDeduplicationJobId(this.ctx, deduplicationId);
  }
  removeDeduplicationKey(deduplicationId: string) {
    return deduplicationOps.removeDeduplicationKey(this.ctx, deduplicationId);
  }

  // ============ Job Move Operations ============
  moveJobToCompleted(id: string, returnValue: unknown, token?: string) {
    return jobMoveOps.moveJobToCompleted(this.moveCtx, id, returnValue, token);
  }
  moveJobToFailed(id: string, error: Error, token?: string) {
    return jobMoveOps.moveJobToFailed(this.moveCtx, id, error, token);
  }
  moveJobToWait(id: string, token?: string) {
    return jobMoveOps.moveJobToWait(this.moveCtx, id, token);
  }
  moveJobToDelayed(id: string, timestamp: number, token?: string) {
    return jobMoveOps.moveJobToDelayed(this.moveCtx, id, timestamp, token);
  }
  moveJobToWaitingChildren(
    id: string,
    token?: string,
    opts?: { child?: { id: string; queue: string } }
  ) {
    return jobMoveOps.moveJobToWaitingChildren(this.moveCtx, id, token, opts);
  }
  waitJobUntilFinished(id: string, queueEvents: unknown, ttl?: number) {
    return jobMoveOps.waitJobUntilFinished(this.moveCtx, id, queueEvents, ttl);
  }

  // ============ Dependency Operations ============
  getJobDependencies(id: string, opts?: GetDependenciesOpts): Promise<JobDependencies> {
    return bullmqCompatOps.getJobDependencies(this.addCtx as never, id, opts);
  }
  getJobDependenciesCount(id: string, opts?: GetDependenciesOpts): Promise<JobDependenciesCount> {
    return bullmqCompatOps.getJobDependenciesCount(this.addCtx as never, id, opts);
  }
  getDependencies(
    parentId: string,
    type?: 'processed' | 'unprocessed',
    start?: number,
    end?: number
  ) {
    return bullmqCompatOps.getDependencies(this.addCtx as never, parentId, type, start, end);
  }

  // ============ BullMQ Compatibility ============
  getPrioritized(start?: number, end?: number) {
    return bullmqCompatOps.getPrioritized<T>(
      {
        ...this.addCtx,
        getWaitingAsync: (s?: number, e?: number) => this.getWaitingAsync(s, e),
        getPrioritizedAsync: (s?: number, e?: number) =>
          this.getJobsAsync({ state: 'prioritized', start: s, end: e }),
      } as never,
      start,
      end
    );
  }
  getPrioritizedCount() {
    return bullmqCompatOps.getPrioritizedCount<T>({
      ...this.addCtx,
      getWaitingAsync: (s?: number, e?: number) => this.getWaitingAsync(s, e),
      getPrioritizedAsync: (s?: number, e?: number) =>
        this.getJobsAsync({ state: 'prioritized', start: s, end: e }),
    } as never);
  }
  getWaitingChildren(start?: number, end?: number) {
    return bullmqCompatOps.getWaitingChildren<T>(this.addCtx as never, start, end);
  }
  getWaitingChildrenCount() {
    return bullmqCompatOps.getWaitingChildrenCount<T>(this.addCtx as never);
  }
  trimEvents(maxLength: number) {
    return workersOps.trimEvents(this.ctx, maxLength);
  }

  // ============ Worker/Metrics Operations ============
  getWorkers() {
    return workersOps.getWorkers(this.ctx);
  }
  getWorkersCount() {
    return workersOps.getWorkersCount(this.ctx);
  }
  getMetrics(type: 'completed' | 'failed', start?: number, end?: number) {
    return workersOps.getMetrics(this.ctx, type, start, end);
  }

  // ============ Connection ============
  async disconnect(): Promise<void> {
    if (this.addBatcher) {
      await this.addBatcher.flush();
      await this.addBatcher.waitForInFlight();
      this.addBatcher.stop();
    }
    this.close();
  }

  close(): void {
    this.addBatcher?.stop();
    if (this.tcpPool) {
      if (this.useSharedPool) releaseSharedPool(this.tcpPool);
      else this.tcpPool.close();
    }
  }
}
