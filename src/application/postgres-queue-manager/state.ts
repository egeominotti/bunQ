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
import { applyPostgresJobProjection, PostgresProjectionRefreshes } from './projectionRefreshes';

export abstract class PostgresQueueManagerState extends QueueManager {
  protected readonly postgresStore: PostgresQueueStore;
  protected readonly postgresSnapshot: PostgresQueueSnapshot;
  protected readonly activeTokens = new Map<JobId, string>();
  protected readonly postgresReady: Promise<void>;
  private readonly refreshes = new Map<string, Promise<void>>();
  private readonly dirtyQueues = new Set<string>();
  private readonly queueEventVersions = new Map<string, number>();
  private snapshotEventBuffer: PostgresStartupEventBuffer | null = new PostgresStartupEventBuffer();
  private readonly unsubscribeEvent: () => void;
  private readonly unsubscribeInvalidation: () => void;
  private readonly writes = new PostgresDeferredWriteQueue();
  protected readonly operations = new PostgresOperationGate();
  private readonly projectionRefreshes: PostgresProjectionRefreshes;
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
    this.projectionRefreshes = new PostgresProjectionRefreshes(
      (requests) => this.postgresStore.loadJobProjections(requests),
      (id, projection) =>
        applyPostgresJobProjection(this.postgresSnapshot, this.activeTokens, id, projection),
      (queue, id, error) => this.postgresStore.reportProjectionRefresh(queue, id, error),
      this.postgresStore.config.pollIntervalMs
    );
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
    this.stopQueueRefreshes = true;
    this.dirtyQueues.clear();
    this.projectionRefreshes.close();
    const operationDrain = this.operations.closeAndDrain();
    await operationDrain;
    this.stopBaseRuntime();
    this.unsubscribeEvent();
    this.unsubscribeInvalidation();
    const errors = await this.writes.closeAndDrain();
    try {
      await this.postgresReady;
    } catch (error) {
      errors.push(error);
    }
    await Promise.allSettled([...this.refreshes.values(), this.projectionRefreshes.drain()]);
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

  protected async refreshJob(id: JobId, queue?: string): Promise<void> {
    const targetQueue = queue ?? this.postgresSnapshot.get(id)?.job.queue ?? '';
    try {
      await this.projectionRefreshes.refreshNow(id, targetQueue);
    } catch {
      this.projectionRefreshes.request(id, targetQueue);
    }
  }

  protected async fetchPostgresJob(id: JobId): Promise<PostgresStoredJob | null> {
    const queue = this.postgresSnapshot.get(id)?.job.queue ?? '';
    return (await this.projectionRefreshes.refreshNow(id, queue)).row;
  }

  protected async refreshJobs(ids: readonly JobId[], queue?: string): Promise<void> {
    const requests = [...new Set(ids)].map((id) => ({
      id,
      queue: queue ?? this.postgresSnapshot.get(id)?.job.queue ?? '',
    }));
    try {
      await this.projectionRefreshes.refreshManyNow(requests);
    } catch {
      for (const request of requests) this.projectionRefreshes.request(request.id, request.queue);
    }
  }

  protected async refreshQueue(queue: string): Promise<boolean> {
    const eventVersion = this.queueEventVersions.get(queue) ?? 0;
    const { rows, state, exists, results } = await this.postgresStore.loadQueueReadModel(queue);
    if (eventVersion !== (this.queueEventVersions.get(queue) ?? 0)) {
      this.dirtyQueues.add(queue);
      return false;
    }
    if (!exists) {
      for (const id of this.postgresSnapshot.removeQueue(queue)) this.activeTokens.delete(id);
      return true;
    }
    this.postgresSnapshot.replaceQueue(queue, rows, state, results);
    return true;
  }

  protected async refreshQueueAfterCommit(queue: string): Promise<void> {
    try {
      if (!(await this.refreshQueue(queue))) this.scheduleQueueRefresh(queue);
      else this.postgresStore.reportQueueRefresh(queue, null);
    } catch (error) {
      this.postgresStore.reportQueueRefresh(queue, error);
      this.scheduleQueueRefresh(queue);
    }
  }

  protected enqueueWrite(operation: () => Promise<void>): void {
    this.writes.enqueue(operation);
  }

  private async initializePostgres(): Promise<void> {
    await this.postgresStore.initialize();
    for (let attempt = 0; attempt < 2; attempt++) {
      const buffer = this.snapshotEventBuffer;
      if (!buffer) return;
      buffer.reset();
      const { rows, results, crons, states, lifetimeMetrics } =
        await this.postgresStore.loadManagerSnapshot();
      const events = buffer.take();
      if (!events && attempt === 0) continue;
      this.snapshotEventBuffer = null;
      this.postgresSnapshot.hydrate(rows);
      this.postgresSnapshot.hydrateResults(results);
      hydratePostgresLifetimeMetrics(this.metrics, this.perQueueMetrics, lifetimeMetrics);
      this.cronScheduler.load([...crons]);
      for (const state of states) this.postgresSnapshot.setQueueState(state);
      for (const event of events ?? []) {
        this.postgresSnapshot.apply(event);
        applyPostgresEventMetrics(
          this.metrics,
          this.perQueueMetrics,
          event,
          event.id > lifetimeMetrics.baselineEventId
        );
      }
      this.projectionRefreshes.start();
      return;
    }
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
        const applied = await this.refreshQueue(queue);
        if (!applied && !this.stopQueueRefreshes) {
          await Bun.sleep(this.postgresStore.config.pollIntervalMs);
        }
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
    const startupBuffer = this.snapshotEventBuffer;
    startupBuffer?.capture(event);
    if (startupBuffer) this.postgresSnapshot.apply(event);
    if (startupBuffer?.overflowed) this.scheduleQueueRefresh(event.queue);
    else this.projectionRefreshes.request(event.jobId, event.queue);
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

  protected applyPostgresClaim(claim: Parameters<PostgresQueueSnapshot['claim']>[0]): void {
    this.projectionRefreshes.supersede(claim.job.id);
    this.activeTokens.set(claim.job.id, claim.token);
    this.postgresSnapshot.claim(claim);
  }

  private stopBaseRuntime(): void {
    if (this.baseStopped) return;
    this.baseStopped = true;
    super.shutdown();
  }
}
