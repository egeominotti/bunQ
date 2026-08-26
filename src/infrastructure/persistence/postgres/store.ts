import type { JobId } from '../../../domain/types/job';
import { claimPostgresJobs } from './claims';
import {
  getPostgresQueueState,
  setPostgresConcurrency,
  setPostgresDlqConfig,
  setPostgresPaused,
  setPostgresRateLimit,
  setPostgresStallConfig,
} from './control';
import { drainPostgresQueue, obliteratePostgresQueue } from './queueDestruction';
import { promotePostgresJobs } from './promotion';
import {
  changePostgresDelay,
  changePostgresPriority,
  clearPostgresJobUniqueKey,
  movePostgresJobToWaitingChildren,
  promotePostgresJob,
  updatePostgresJobData,
  updatePostgresProgress,
} from './mutations';
import { removePostgresJob, retryPostgresTerminalJob } from './destructiveMutations';
import { completePostgresJob } from './completionOutcome';
import { failPostgresJob } from './outcomes';
import { renewPostgresLease } from './leaseRenewal';
import {
  getPostgresCounts,
  getPostgresJob,
  getPostgresJobs,
  findMissingPostgresDependencies,
  latestPostgresEventId,
  listPostgresDlq,
  listPostgresJobs,
  listPostgresQueues,
  loadAllPostgresJobs,
  type ListPostgresJobsOptions,
} from './queries';
import {
  getPostgresChildrenValues,
  getPostgresCompletionResult,
  loadPostgresCompletionResults,
} from './completionQueries';
import { recoverExpiredPostgresLeases } from './recovery';
import { getPostgresFlowFailureValues } from './flowFailures';
import { PostgresAdmissionStore } from './admissionStore';
import { cleanPostgresQueue, purgePostgresDlq } from './maintenance';
import { maintainPostgresDlq } from './dlqLifecycle';
import { addPostgresJobLog, clearPostgresJobLogs, getPostgresJobLogs } from './logs';
import {
  getPostgresCron,
  listPostgresCrons,
  processPostgresCrons,
  removePostgresCron,
  upsertPostgresCron,
} from './crons';
import type { CronJobInput } from '../../../domain/types/cron';
import type { Worker } from '../../../domain/types/worker';
import {
  heartbeatPostgresWorker,
  listPostgresWorkers,
  removePostgresClientWorkers,
  removePostgresWorker,
  savePostgresWorker,
} from './workers';
import { getPostgresQueueMetrics, trimPostgresQueueEvents } from './telemetry';
import { releasePostgresClientLease } from './leaseRelease';
import type { QueueMetricType } from '../../../domain/types/metrics';
import { removePostgresChildDependency, updatePostgresJobParent } from './relationships';
import { cancelPostgresJob, discardPostgresJob, removePostgresDlqJob } from './terminal';
import type { ClaimedPostgresJob, PostgresCompletionInput, PostgresFailureInput } from './types';
import { databaseNow, runPostgresPostCommitMaintenance } from './context';
import { completePostgresJobs } from './batchCompletion';
import { prunePostgresCompletionTombstones } from './completionLifecycle';
import { loadPostgresLifetimeMetrics } from './lifetimeMetrics';
import { loadPostgresCloudReadModel } from './cloudReadModel';
import { removePostgresUnprocessedChildren } from './removeUnprocessedChildren';
import {
  loadPostgresJobProjections,
  loadPostgresManagerSnapshot,
  loadPostgresQueueReadModel,
} from './readModels';
/** Database-authoritative PostgreSQL queue store safe for concurrent brokers. */
export class PostgresQueueStore extends PostgresAdmissionStore {
  async now(): Promise<number> {
    await this.initialize();
    return await databaseNow(this.context.sql);
  }
  async claim(
    queue: string,
    count: number,
    owner = this.config.brokerId,
    leaseDurationMs = this.config.leaseDurationMs
  ): Promise<ClaimedPostgresJob[]> {
    await this.initialize();
    return await claimPostgresJobs(this.context, queue, count, owner, leaseDurationMs);
  }

  async complete(id: JobId, token: string, result?: unknown, remove = false) {
    await this.initialize();
    const transition = await completePostgresJob(this.context, id, token, result, remove);
    if (transition.applied && (remove || transition.job?.removeOnComplete)) {
      await runPostgresPostCommitMaintenance(this.context, 'completion-retention', () =>
        prunePostgresCompletionTombstones(this.context)
      );
    }
    return transition;
  }

  async completeMany(items: readonly PostgresCompletionInput[]) {
    await this.initialize();
    const transitions = await completePostgresJobs(this.context, items);
    if (transitions.some((transition) => transition.applied && transition.removed)) {
      await runPostgresPostCommitMaintenance(this.context, 'completion-retention', () =>
        prunePostgresCompletionTombstones(this.context)
      );
    }
    return transitions;
  }

  async fail(input: PostgresFailureInput) {
    await this.initialize();
    return await failPostgresJob(this.context, input);
  }

  async renew(id: JobId, token: string, durationMs: number): Promise<number | null> {
    await this.initialize();
    return await renewPostgresLease(this.context, id, token, durationMs);
  }

  async recoverExpired(limit?: number): Promise<number> {
    await this.initialize();
    return await recoverExpiredPostgresLeases(this.context, limit);
  }

  async maintainDlq(limit?: number) {
    await this.initialize();
    return await maintainPostgresDlq(this.context, limit);
  }

  addCron = (input: CronJobInput) => upsertPostgresCron(this.context, input);
  getCron = (name: string) => getPostgresCron(this.context, name);
  listCrons = () => listPostgresCrons(this.context);
  removeCron = (name: string) => removePostgresCron(this.context, name);
  processCrons = (limit?: number) => processPostgresCrons(this.context, limit);
  saveWorker = (worker: Worker) => savePostgresWorker(this.context, worker);
  heartbeatWorker = (
    id: string,
    stats?: { activeJobs?: number; processed?: number; failed?: number },
    clientId?: string
  ) => heartbeatPostgresWorker(this.context, id, stats, clientId);
  removeWorker = (id: string, clientId?: string) =>
    removePostgresWorker(this.context, id, clientId);
  removeClientWorkers = (clientId: string) => removePostgresClientWorkers(this.context, clientId);
  listWorkers = () => listPostgresWorkers(this.context);
  getQueueMetrics = (queue: string, type: QueueMetricType, start?: number, end?: number) =>
    getPostgresQueueMetrics(this.context, queue, type, start, end);
  trimQueueEvents = (queue: string, maxLength: number) =>
    trimPostgresQueueEvents(this.context, queue, maxLength);
  releaseClientLease = (id: JobId, token: string) =>
    releasePostgresClientLease(this.context, id, token);

  getJob = (id: JobId) => getPostgresJob(this.context, id);
  getJobs = (ids: readonly JobId[]) => getPostgresJobs(this.context, ids);
  findMissingDependencies = (ids: readonly JobId[]) =>
    findMissingPostgresDependencies(this.context, ids);
  getResult = (id: JobId) => getPostgresCompletionResult(this.context, id);
  getCounts = (queue?: string) => getPostgresCounts(this.context, queue);
  getQueues = () => listPostgresQueues(this.context);
  getDlq = (queue: string, limit?: number) => listPostgresDlq(this.context, queue, limit);
  getChildrenValues = (id: JobId) => getPostgresChildrenValues(this.context, id);
  getFailedChildrenValues = (id: JobId) =>
    getPostgresFlowFailureValues(this.context, id, 'continue');
  getIgnoredChildrenFailures = (id: JobId) =>
    getPostgresFlowFailureValues(this.context, id, 'ignore');
  addLog = (id: JobId, message: string, level: 'info' | 'warn' | 'error') =>
    addPostgresJobLog(this.context, id, message, level);
  getLogs = (id: JobId) => getPostgresJobLogs(this.context, id);
  clearLogs = (id: JobId, keepLogs?: number) => clearPostgresJobLogs(this.context, id, keepLogs);
  getQueueState = (queue: string) => getPostgresQueueState(this.context, queue);
  list = (queue: string, options?: ListPostgresJobsOptions) =>
    listPostgresJobs(this.context, queue, options);
  loadAll = () => loadAllPostgresJobs(this.context);
  loadResults = (queue?: string) => loadPostgresCompletionResults(this.context, queue);
  latestEventId = () => latestPostgresEventId(this.context);
  loadLifetimeMetrics = () => loadPostgresLifetimeMetrics(this.context);
  loadCloudReadModel = () => loadPostgresCloudReadModel(this.context);
  loadManagerSnapshot = () => loadPostgresManagerSnapshot(this.context);
  loadQueueReadModel = (queue: string) => loadPostgresQueueReadModel(this.context, queue);
  loadJobProjections = (requests: Parameters<typeof loadPostgresJobProjections>[1]) =>
    loadPostgresJobProjections(this.context, requests);
  updateData = (id: JobId, data: unknown) => updatePostgresJobData(this.context, id, data);
  updateProgress = (id: JobId, value: number, message?: string) =>
    updatePostgresProgress(this.context, id, value, message);
  changePriority = (id: JobId, priority: number, lifo?: boolean) =>
    changePostgresPriority(this.context, id, priority, lifo);
  changeDelay = (id: JobId, runAt: number, token?: string) =>
    changePostgresDelay(this.context, id, runAt, token);
  moveToWaitingChildren = (id: JobId, token?: string) =>
    movePostgresJobToWaitingChildren(this.context, id, token);
  clearUniqueKey = (id: JobId) => clearPostgresJobUniqueKey(this.context, id);
  removeDependency = (id: JobId) => removePostgresChildDependency(this.context, id);
  removeUnprocessedChildren = (parentId: JobId) =>
    removePostgresUnprocessedChildren(this.context, parentId);
  updateParent = (childId: JobId, parentId: JobId) =>
    updatePostgresJobParent(this.context, childId, parentId);
  promote = (id: JobId) => promotePostgresJob(this.context, id);
  cancel = (id: JobId) => cancelPostgresJob(this.context, id);
  discard = (id: JobId, token?: string) => discardPostgresJob(this.context, id, token);
  remove = (id: JobId, token?: string) => removePostgresJob(this.context, id, token);
  removeDlq = (queue: string, id: JobId) => removePostgresDlqJob(this.context, queue, id);
  purgeDlq = (queue: string) => purgePostgresDlq(this.context, queue);
  clean = (queue: string, graceMs: number, state?: string, limit?: number) =>
    cleanPostgresQueue(this.context, queue, graceMs, state, limit);
  retry = (id: JobId, timestamp?: number) => retryPostgresTerminalJob(this.context, id, timestamp);
  pause = (queue: string, paused: boolean) => setPostgresPaused(this.context, queue, paused);
  setRateLimit = (
    queue: string,
    limit: number | null,
    durationMs: number | null,
    ttlMs?: number | null
  ) => setPostgresRateLimit(this.context, queue, limit, durationMs, ttlMs);
  setConcurrency = (queue: string, limit: number | null) =>
    setPostgresConcurrency(this.context, queue, limit);
  setStallConfig = (queue: string, config: Parameters<typeof setPostgresStallConfig>[2]) =>
    setPostgresStallConfig(this.context, queue, config);
  setDlqConfig = (queue: string, config: Parameters<typeof setPostgresDlqConfig>[2]) =>
    setPostgresDlqConfig(this.context, queue, config);
  drain = (queue: string) => drainPostgresQueue(this.context, queue);
  obliterate = (queue: string) => obliteratePostgresQueue(this.context, queue);
  promoteMany = (queue: string, count?: number) => promotePostgresJobs(this.context, queue, count);
}
