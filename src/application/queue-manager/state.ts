import type { Job, JobId, JobLock } from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import type { JobLogEntry } from '../../domain/types/worker';
import { Shard } from '../../domain/queue/shard';
import { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import { CronScheduler } from '../../infrastructure/scheduler/cronScheduler';
import { RWLock } from '../../shared/lock';
import { SHARD_COUNT } from '../../shared/hash';
import { BoundedMap, BoundedSet, LRUMap } from '../../shared/lru';
import { WebhookManager } from '../webhookManager';
import { WorkerManager } from '../workerManager';
import { EventsManager } from '../eventsManager';
import { createMonitoringState, type MonitoringState } from '../monitoringChecks';
import type { OperationalMetrics } from '../metricsExporter';
import { DEFAULT_CONFIG, type QueueManagerConfig } from '../types';
import * as bgTasks from '../backgroundTasks';
import { ContextFactory } from '../contextFactory';
import { DependencyResultTracker } from '../dependencyResultTracker';
import { recoverFlowFailures } from '../flowFailureRecovery';
import { DependencyCompletionTracker } from '../dependencyCompletions';
import { createContextCallbacks, createContextDependencies, managerRuntime } from './context';

export type { QueueManagerConfig };

export abstract class QueueManagerState {
  protected readonly config: typeof DEFAULT_CONFIG & { dataPath?: string };
  protected readonly storage: SqliteStorage | null;
  protected readonly shards: Shard[] = [];
  protected readonly shardLocks: RWLock[] = [];
  protected readonly customIdLock = new RWLock();
  protected readonly processingShards: Map<JobId, Job>[] = [];
  protected readonly processingLocks: RWLock[] = [];
  protected readonly jobIndex = new Map<JobId, JobLocation>();
  protected readonly completedJobs: BoundedSet<JobId>;
  protected readonly completedJobsData: BoundedMap<JobId, Job>;
  protected readonly depCompletions: DependencyCompletionTracker;
  protected readonly timedOutJobs: BoundedSet<JobId>;
  protected readonly jobResults: LRUMap<JobId, unknown>;
  protected readonly dependencyResults = new DependencyResultTracker();
  protected readonly customIdMap: LRUMap<string, JobId>;
  protected readonly jobLogs: LRUMap<JobId, JobLogEntry[]>;
  protected readonly pendingDepChecks = new Set<JobId>();
  protected readonly stalledCandidates = new Set<JobId>();
  protected depFlushScheduled = false;
  protected depFlushRunning = false;
  protected readonly jobLocks = new Map<JobId, JobLock>();
  protected readonly clientJobs = new Map<string, Set<JobId>>();
  protected readonly repeatChain = new Map<JobId, JobId>();
  protected readonly failedChildrenValues = new Map<JobId, Record<string, string>>();
  protected readonly ignoredChildrenFailures = new Map<JobId, Record<string, string>>();
  protected readonly cronScheduler: CronScheduler;
  readonly webhookManager: WebhookManager;
  readonly workerManager: WorkerManager;
  protected readonly eventsManager: EventsManager;
  protected dashboardEmit: ((event: string, data: Record<string, unknown>) => void) | null = null;
  protected recoveryStats: { queues: number; jobs: number } | null = null;
  protected readonly maxLogsPerJob = 100;
  protected readonly metrics = {
    totalPushed: { value: 0n },
    totalPulled: { value: 0n },
    totalCompleted: { value: 0n },
    totalFailed: { value: 0n },
  };
  protected operationalMetricsProvider: (() => OperationalMetrics) | null = null;
  protected readonly perQueueMetrics: LRUMap<
    string,
    { totalCompleted: bigint; totalFailed: bigint }
  >;
  protected readonly startTime = Date.now();
  protected readonly backgroundTaskHandles: bgTasks.BackgroundTaskHandles | null;
  protected readonly queueNamesCache = new Set<string>();
  protected readonly monitoringState: MonitoringState = createMonitoringState();
  protected readonly contextFactory: ContextFactory;

  constructor(config: QueueManagerConfig = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.storage = config.dataPath ? new SqliteStorage({ path: config.dataPath }) : null;
    this.completedJobsData = new BoundedMap(this.config.maxCompletedJobs);
    this.completedJobs = new BoundedSet(this.config.maxCompletedJobs, (id) => {
      this.jobIndex.delete(id);
      this.completedJobsData.delete(id);
    });
    this.depCompletions = new DependencyCompletionTracker(this.config.maxCompletedJobs, (id) => {
      this.storage?.deleteDependencyCompletion(id);
    });
    this.timedOutJobs = new BoundedSet(this.config.maxCompletedJobs);
    this.perQueueMetrics = new LRUMap(this.config.maxCustomIds);
    this.jobResults = new LRUMap(this.config.maxJobResults);
    this.customIdMap = new LRUMap(this.config.maxCustomIds);
    this.jobLogs = new LRUMap(this.config.maxJobLogs);

    const runtime = managerRuntime(this);
    for (let index = 0; index < SHARD_COUNT; index++) {
      this.shards.push(new Shard({ onDlqEvicted: (entry) => runtime.onDlqEvicted(entry) }));
      this.shardLocks.push(new RWLock());
      this.processingShards.push(new Map());
      this.processingLocks.push(new RWLock());
    }

    this.cronScheduler = new CronScheduler();
    this.cronScheduler.setPushCallback(async (queue, input) => {
      await runtime.push(queue, input);
    });
    if (this.storage) {
      const storage = this.storage;
      this.cronScheduler.setPersistCallback((name, executions, nextRun) => {
        storage.updateCron(name, executions, nextRun);
      });
    }

    this.webhookManager = new WebhookManager({ validateUrls: config.validateWebhookUrls });
    this.workerManager = new WorkerManager();
    this.cronScheduler.setWorkerCheckCallback(
      (queue) => this.workerManager.getForQueue(queue).length > 0
    );
    this.eventsManager = new EventsManager(this.webhookManager);
    this.contextFactory = new ContextFactory(
      createContextDependencies(managerRuntime(this)),
      createContextCallbacks(runtime)
    );

    bgTasks.recover(this.contextFactory.getBackgroundContext());
    if (this.storage) {
      recoverFlowFailures({
        storage: this.storage,
        shards: this.shards,
        jobIndex: this.jobIndex,
        completedJobs: this.completedJobs,
        depCompletions: this.depCompletions,
        maxDependencyCompletions: this.config.maxCompletedJobs,
        dependencyResults: this.dependencyResults,
        failedChildrenValues: this.failedChildrenValues,
        ignoredChildrenFailures: this.ignoredChildrenFailures,
      });
    }
    this.recoveryStats = { queues: this.queueNamesCache.size, jobs: this.jobIndex.size };
    if (this.storage) this.cronScheduler.load(this.storage.loadCronJobs());
    this.backgroundTaskHandles = bgTasks.startBackgroundTasks(
      this.contextFactory.getBackgroundContext(),
      this.cronScheduler
    );
  }
}
