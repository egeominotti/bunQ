import type { JobId } from '../../domain/types/job';
import {
  PostgresQueueStore,
  type PostgresStoredJob,
  type PostgresStoreEvent,
} from '../../infrastructure/persistence/postgres';
import { QueueManager } from '../queueManager';
import { stopBackgroundTasks } from '../backgroundTasks';
import type { PostgresQueueManagerConfig } from './config';
import { PostgresDeferredWriteQueue, throwDeferredWriteErrors } from './deferredWrites';
import {
  applyPostgresEventMetrics,
  hydratePostgresLifetimeMetrics,
  postgresEventType,
} from './lifetimeMetrics';
import { PostgresQueueSnapshot } from './snapshot';
import { PostgresStartupEventBuffer } from './startupEventBuffer';
import { PostgresOperationGate } from './operationGate';

export abstract class PostgresQueueManagerState extends QueueManager {
  protected readonly postgresStore: PostgresQueueStore;
  protected readonly postgresSnapshot: PostgresQueueSnapshot;
  protected readonly activeTokens = new Map<JobId, string>();
  protected readonly postgresReady: Promise<void>;
  private readonly refreshes = new Map<string, Promise<void>>();
  private readonly dirtyQueues = new Set<string>();
  private readonly queueEventVersions = new Map<string, number>();
  private readonly jobRefreshWatches = new Map<JobId, { version: number; watchers: number }>();
  private snapshotEventBuffer: PostgresStartupEventBuffer | null = new PostgresStartupEventBuffer();
  private readonly unsubscribeEvent: () => void;
  private readonly unsubscribeInvalidation: () => void;
  private readonly writes = new PostgresDeferredWriteQueue();
  protected readonly operations = new PostgresOperationGate();
  private shutdownPromise: Promise<void> | null = null;
  private shutdownComplete = false;
  private baseStopped = false;
  private stopQueueRefreshes = false;

  constructor(config: PostgresQueueManagerConfig) {
    const { postgres, ...baseConfig } = config;
    super(baseConfig);
    if (this.backgroundTaskHandles) stopBackgroundTasks(this.backgroundTaskHandles);
    this.postgresSnapshot = new PostgresQueueSnapshot(
      this.config.maxCompletedJobs,
      this.config.maxJobResults
    );
    this.postgresStore = new PostgresQueueStore({
      ...postgres,
      maxQueueEvents: this.config.maxQueueEvents,
      maxMetricDataPoints: this.config.maxMetricDataPoints,
      maxCompletedJobs: this.config.maxCompletedJobs,
      maxJobResults: this.config.maxJobResults,
    });
    this.unsubscribeEvent = this.postgresStore.onEvent((event) => this.onStoreEvent(event));
    this.unsubscribeInvalidation = this.postgresStore.onInvalidation((queue) => {
      this.scheduleQueueRefresh(queue);
    });
    this.postgresReady = this.initializePostgres();
  }
  async waitUntilReady(): Promise<void> {
    await this.postgresReady;
  }

  async flushPostgresWrites(): Promise<void> {
    await this.postgresReady;
    await this.writes.flush();
  }

  async shutdownPostgres(): Promise<void> {
    if (this.shutdownComplete) return;
    if (!this.shutdownPromise) {
      const attempt = this.performPostgresShutdown()
        .then(() => {
          this.shutdownComplete = true;
        })
        .finally(() => {
          if (this.shutdownPromise === attempt && !this.shutdownComplete) {
            this.shutdownPromise = null;
          }
        });
      this.shutdownPromise = attempt;
    }
    await this.shutdownPromise;
  }

  override shutdown(): void {
    void this.shutdownPostgres().catch(() => undefined);
  }

  private async performPostgresShutdown(): Promise<void> {
    const operationDrain = this.operations.closeAndDrain();
    this.stopBaseRuntime();
    await operationDrain;
    this.unsubscribeEvent();
    this.unsubscribeInvalidation();
    this.stopQueueRefreshes = true;
    this.dirtyQueues.clear();
    const errors = await this.writes.closeAndDrain();
    try {
      await this.postgresReady;
    } catch (error) {
      errors.push(error);
    }
    await Promise.allSettled(this.refreshes.values());
    try {
      await this.postgresStore.close();
    } catch (error) {
      errors.push(error);
    }
    throwDeferredWriteErrors(errors, 'PostgreSQL shutdown failed');
  }

  protected tokenFor(id: JobId, token?: string): string | null {
    return token ?? this.activeTokens.get(id) ?? this.postgresSnapshot.get(id)?.token ?? null;
  }

  protected forgetToken(id: JobId): void {
    this.activeTokens.delete(id);
  }

  protected runPostgresOperation<T>(operation: () => Promise<T>): Promise<T> {
    return this.operations.run(operation);
  }

  protected async refreshJob(id: JobId): Promise<void> {
    await this.fetchPostgresJob(id);
  }

  protected async fetchPostgresJob(id: JobId): Promise<PostgresStoredJob | null> {
    const version = this.startJobRefresh(id);
    try {
      const row = await this.postgresStore.getJob(id);
      this.applyJobRefresh(id, row, version);
      return row;
    } finally {
      this.finishJobRefresh(id);
    }
  }

  protected async refreshJobs(ids: readonly JobId[]): Promise<void> {
    const uniqueIds = [...new Set(ids)];
    const versions = new Map<JobId, number>();
    for (const id of uniqueIds) versions.set(id, this.startJobRefresh(id));
    try {
      const rows = await this.postgresStore.getJobs(uniqueIds);
      const found = new Set(rows.map((row) => row.job.id));
      for (const row of rows) {
        const version = versions.get(row.job.id);
        if (version !== undefined) this.applyJobRefresh(row.job.id, row, version);
      }
      for (const id of uniqueIds) {
        const version = versions.get(id);
        if (!found.has(id) && version !== undefined) this.applyJobRefresh(id, null, version);
      }
    } finally {
      for (const id of uniqueIds) this.finishJobRefresh(id);
    }
  }

  protected async refreshQueue(queue: string): Promise<void> {
    while (true) {
      const eventVersion = this.queueEventVersions.get(queue) ?? 0;
      const [rows, state, queues, completions] = await Promise.all([
        this.postgresStore.list(queue, { limit: 2_147_483_647, asc: true }),
        this.postgresStore.getQueueState(queue),
        this.postgresStore.getQueues(),
        this.postgresStore.loadResults(queue),
      ]);
      if (eventVersion !== (this.queueEventVersions.get(queue) ?? 0)) continue;
      if (rows.length === 0 && !queues.includes(queue)) {
        for (const id of this.postgresSnapshot.removeQueue(queue)) this.activeTokens.delete(id);
        return;
      }
      this.postgresSnapshot.replaceQueue(queue, rows, state, completions);
      return;
    }
  }

  protected enqueueWrite(operation: () => Promise<void>): void {
    this.writes.enqueue(operation);
  }

  private async initializePostgres(): Promise<void> {
    await this.postgresStore.initialize();
    while (this.snapshotEventBuffer) {
      this.snapshotEventBuffer.reset();
      const [rows, results, crons, queues, lifetimeMetrics] = await Promise.all([
        this.postgresStore.loadAll(),
        this.postgresStore.loadResults(),
        this.postgresStore.listCrons(),
        this.postgresStore.getQueues(),
        this.postgresStore.loadLifetimeMetrics(),
      ]);
      const states = await Promise.all(
        queues.map((queue) => this.postgresStore.getQueueState(queue))
      );
      const events = this.snapshotEventBuffer.take();
      if (!events) continue;
      this.snapshotEventBuffer = null;
      this.postgresSnapshot.hydrate(rows);
      this.postgresSnapshot.hydrateResults(results);
      hydratePostgresLifetimeMetrics(this.metrics, this.perQueueMetrics, lifetimeMetrics);
      this.cronScheduler.load(crons);
      for (const state of states) this.postgresSnapshot.setQueueState(state);
      for (const event of events) {
        this.postgresSnapshot.apply(event);
        applyPostgresEventMetrics(
          this.metrics,
          this.perQueueMetrics,
          event,
          event.id > lifetimeMetrics.baselineEventId
        );
      }
    }
  }

  private startJobRefresh(id: JobId): number {
    const watch = this.jobRefreshWatches.get(id) ?? { version: 0, watchers: 0 };
    watch.watchers++;
    this.jobRefreshWatches.set(id, watch);
    return watch.version;
  }

  private applyJobRefresh(id: JobId, row: PostgresStoredJob | null, version: number): void {
    const watch = this.jobRefreshWatches.get(id);
    if (!watch || watch.version !== version) return;
    if (row) this.postgresSnapshot.put(row);
    else this.postgresSnapshot.remove(id);
    watch.version++;
  }

  private finishJobRefresh(id: JobId): void {
    const watch = this.jobRefreshWatches.get(id);
    if (!watch || --watch.watchers === 0) this.jobRefreshWatches.delete(id);
  }

  private scheduleQueueRefresh(queue: string): void {
    if (this.stopQueueRefreshes) return;
    this.dirtyQueues.add(queue);
    if (this.refreshes.has(queue)) return;
    const refresh = this.postgresReady
      .then(() => this.refreshDirtyQueue(queue))
      .catch(() => {
        this.dirtyQueues.delete(queue);
      })
      .finally(() => {
        this.refreshes.delete(queue);
        if (this.dirtyQueues.has(queue)) this.scheduleQueueRefresh(queue);
      });
    this.refreshes.set(queue, refresh);
  }

  private async refreshDirtyQueue(queue: string): Promise<void> {
    let retryDelay = this.postgresStore.config.pollIntervalMs;
    while (!this.stopQueueRefreshes && this.dirtyQueues.delete(queue)) {
      try {
        await this.refreshQueue(queue);
        this.postgresStore.reportQueueRefresh(queue, null);
        retryDelay = this.postgresStore.config.pollIntervalMs;
      } catch (error) {
        this.postgresStore.reportQueueRefresh(queue, error);
        if (this.stopQueueRefreshes) return;
        this.dirtyQueues.add(queue);
        await Bun.sleep(retryDelay);
        retryDelay = Math.min(retryDelay * 2, 1_000);
      }
    }
  }

  private onStoreEvent(event: PostgresStoreEvent): void {
    this.queueEventVersions.set(event.queue, (this.queueEventVersions.get(event.queue) ?? 0) + 1);
    const refreshWatch = this.jobRefreshWatches.get(event.jobId);
    if (refreshWatch) refreshWatch.version++;
    this.postgresSnapshot.apply(event);
    const startupBuffer = this.snapshotEventBuffer;
    startupBuffer?.capture(event);
    if (event.state !== 'active') this.activeTokens.delete(event.jobId);
    const mapped = postgresEventType(event.type);
    if (!mapped) return;
    if (!startupBuffer) {
      applyPostgresEventMetrics(this.metrics, this.perQueueMetrics, event, true);
    }
    this.eventsManager.broadcast({
      eventType: mapped,
      queue: event.queue,
      jobId: event.jobId,
      timestamp: event.occurredAt,
      ...(Object.hasOwn(event, 'result') && { data: event.result }),
      ...(event.error !== undefined && { error: event.error }),
      ...(mapped === 'failed' && { terminal: event.state === 'failed' }),
    });
  }

  private stopBaseRuntime(): void {
    if (this.baseStopped) return;
    this.baseStopped = true;
    super.shutdown();
  }
}
