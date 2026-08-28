import { recoverExpiredPostgresLeases } from './recovery';
import { releasePostgresBrokerLeases } from './leaseRelease';
import { PostgresEventStream } from './events';
import { prunePostgresEventCommits } from './eventJournal';
import { maintainPostgresDlq, repairPostgresDlq } from './dlqMaintenance';
import { listPostgresCrons, processPostgresCrons, reconcilePostgresCronsOnStartup } from './crons';
import { purgeStalePostgresWorkers, removePostgresBrokerWorkers } from './workers';
import { initializePostgresSchema } from './schemaInitialization';
import type { PostgresContext } from './context';
import type {
  PostgresDeliveredStoreEvent,
  PostgresStorageConfig,
  PostgresStorageHealth,
} from './types';
import { PostgresPostCommitMaintenance } from './postCommitMaintenance';
import { sweepPostgresEventRetention } from './telemetry';
import { prunePostgresCompletionTombstones } from './completionLifecycle';
import { PostgresMaintenanceFlights } from './maintenanceFlights';
import { resolvePostgresRuntimeConfig } from './runtimeConfig';
import { PostgresEventCommitGc } from './eventCommitGc';
import { createPostgresConnection } from './connection';
import { PostgresStorageHealthTracker } from './storageHealth';
import {
  heartbeatPostgresBroker,
  PostgresBrokerSessionFencedError,
  purgeStalePostgresBrokers,
  registerPostgresBroker,
  unregisterPostgresBroker,
} from './brokerSessions';

/** Connection, schema, event stream, health, and maintenance lifecycle. */
export class PostgresQueueStoreRuntime {
  readonly context: PostgresContext;
  readonly events: PostgresEventStream;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private recoveryTimer: ReturnType<typeof setInterval> | null = null;
  private dlqTimer: ReturnType<typeof setInterval> | null = null;
  private cronTimer: ReturnType<typeof setInterval> | null = null;
  private readyPromise: Promise<void> | null = null;
  private closing = false;
  private closed = false;
  private closeAttempt: Promise<void> | null = null;
  private leasesReleased = false;
  private workersRemoved = false;
  private brokerUnregistered = false;
  private eventsClosed = false;
  private eventStatusUnsubscribed = false;
  private sqlClosed = false;
  private brokerRegistrationStarted = false;
  private readonly healthTracker: PostgresStorageHealthTracker;
  private readonly unsubscribeEventStatus: () => void;
  private readonly postCommitMaintenance: PostgresPostCommitMaintenance;
  private readonly maintenanceFlights: PostgresMaintenanceFlights;
  private readonly eventCommitGc: PostgresEventCommitGc;

  constructor(config: PostgresStorageConfig) {
    const resolved = resolvePostgresRuntimeConfig(config);
    const sql = createPostgresConnection(resolved);
    this.healthTracker = new PostgresStorageHealthTracker(resolved);
    this.postCommitMaintenance = new PostgresPostCommitMaintenance(
      (subsystem, error) => this.reportMaintenance(subsystem, error),
      Math.max(25, resolved.pollIntervalMs)
    );
    this.maintenanceFlights = new PostgresMaintenanceFlights((subsystem, error) =>
      this.reportMaintenance(subsystem, error)
    );
    this.context = {
      sql,
      config: resolved,
      postCommitMaintenance: (subsystem, operation) =>
        this.postCommitMaintenance.run(subsystem, operation),
    };
    this.eventCommitGc = new PostgresEventCommitGc(
      (batchSize, maxBatches) => prunePostgresEventCommits(this.context, batchSize, maxBatches),
      (subsystem, error) => this.reportMaintenance(subsystem, error)
    );
    this.events = new PostgresEventStream(this.context);
    this.unsubscribeEventStatus = this.events.subscribeDrainStatus((error) => {
      if (error === null) this.healthTracker.clear('event-stream');
      else this.healthTracker.record('event-stream', error);
    });
  }

  get config() {
    return this.context.config;
  }

  initialize(): Promise<void> {
    this.readyPromise ??= this.initializeOnce();
    return this.readyPromise;
  }

  close(): Promise<void> {
    if (this.closed) return Promise.resolve();
    if (this.closeAttempt) return this.closeAttempt;
    this.closing = true;
    this.stopMaintenance();
    const attempt = this.closeOnce().finally(() => {
      this.closeAttempt = null;
    });
    this.closeAttempt = attempt;
    return attempt;
  }

  private stopMaintenance(): void {
    this.postCommitMaintenance.close();
    this.maintenanceFlights.close();
    this.eventCommitGc.close();
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.recoveryTimer) clearInterval(this.recoveryTimer);
    if (this.dlqTimer) clearInterval(this.dlqTimer);
    if (this.cronTimer) clearInterval(this.cronTimer);
    this.heartbeatTimer = null;
    this.recoveryTimer = null;
    this.dlqTimer = null;
    this.cronTimer = null;
  }

  private async closeOnce(): Promise<void> {
    const errors: unknown[] = [];
    let initialized = true;
    try {
      await this.initialize();
    } catch (error) {
      initialized = false;
      errors.push(error);
    }
    if (initialized || this.brokerRegistrationStarted) {
      await this.maintenanceFlights.drain();
      await this.eventCommitGc.drain();
      await this.postCommitMaintenance.drain();
      this.leasesReleased = await this.tryCloseStep(
        this.leasesReleased,
        () => releasePostgresBrokerLeases(this.context),
        errors
      );
      this.workersRemoved = await this.tryCloseStep(
        this.workersRemoved,
        () => removePostgresBrokerWorkers(this.context),
        errors
      );
      this.brokerUnregistered = await this.tryCloseStep(
        this.brokerUnregistered,
        async () => {
          await unregisterPostgresBroker(this.context);
        },
        errors
      );
    }
    this.eventsClosed = await this.tryCloseStep(
      this.eventsClosed,
      () => this.events.close(),
      errors
    );
    if (this.eventsClosed && !this.eventStatusUnsubscribed) {
      this.unsubscribeEventStatus();
      this.eventStatusUnsubscribed = true;
    }
    const databaseClosed =
      !this.brokerRegistrationStarted ||
      (this.leasesReleased && this.workersRemoved && this.brokerUnregistered);
    if (databaseClosed) {
      this.sqlClosed = await this.tryCloseStep(
        this.sqlClosed,
        () => this.context.sql.close({ timeout: 5 }),
        errors
      );
    }
    if (this.eventsClosed && this.sqlClosed && (!initialized || databaseClosed)) {
      this.closed = true;
    }
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) throw new AggregateError(errors, 'PostgreSQL shutdown failed');
  }

  private async tryCloseStep(
    completed: boolean,
    operation: () => Promise<unknown>,
    errors: unknown[]
  ): Promise<boolean> {
    if (completed) return true;
    try {
      await operation();
      return true;
    } catch (error) {
      errors.push(error);
      return false;
    }
  }

  health(): PostgresStorageHealth {
    return this.healthTracker.snapshot();
  }

  reportQueueRefresh(queue: string, error: unknown): void {
    const key = `queue-refresh:${queue}`;
    if (error === null) {
      this.healthTracker.clear(key);
      return;
    }
    this.healthTracker.record(key, error, `Queue refresh ${queue}: `);
  }

  reportProjectionRefresh(queue: string, id: string, error: unknown): void {
    const key = `projection-refresh:${id}`;
    if (error === null) {
      this.healthTracker.clear(key);
      return;
    }
    this.healthTracker.record(key, error, `Projection refresh ${queue}/${id}: `);
  }

  private reportMaintenance(subsystem: string, error: unknown): void {
    const key = `maintenance:${subsystem}`;
    if (error === null) this.healthTracker.clear(key);
    else this.healthTracker.record(key, error);
  }

  onEvent(listener: (event: PostgresDeliveredStoreEvent) => void): () => void {
    return this.events.subscribe(listener);
  }

  onInvalidation(listener: (queue: string) => void): () => void {
    return this.events.subscribeInvalidation(listener);
  }

  waitForWork(queue: string, timeoutMs: number, signal?: AbortSignal): Promise<void> {
    return this.events.wait(queue, timeoutMs, signal);
  }

  private async initializeOnce(): Promise<void> {
    try {
      await this.context.sql.connect();
      await initializePostgresSchema(this.context);
      await listPostgresCrons(this.context);
      await registerPostgresBroker(this.context);
      this.brokerRegistrationStarted = true;
      await reconcilePostgresCronsOnStartup(this.context);
      await recoverExpiredPostgresLeases(this.context);
      await repairPostgresDlq(this.context);
      await prunePostgresCompletionTombstones(this.context);
      await sweepPostgresEventRetention(this.context);
      await prunePostgresEventCommits(this.context);
      await this.events.start();
      if (!this.closing) this.startMaintenance();
      this.healthTracker.clear('lifecycle');
    } catch (error) {
      this.healthTracker.record('lifecycle', error);
      throw error;
    }
  }

  private startMaintenance(): void {
    this.eventCommitGc.start();
    const heartbeatMs = Math.max(1000, Math.floor(this.config.leaseDurationMs / 3));
    this.heartbeatTimer = setInterval(
      () => void this.runMaintenance(() => this.heartbeatBroker(), 'heartbeat'),
      heartbeatMs
    );
    this.recoveryTimer = setInterval(
      () => void this.runMaintenance(() => recoverExpiredPostgresLeases(this.context), 'recovery'),
      Math.max(500, Math.floor(this.config.leaseDurationMs / 2))
    );
    this.dlqTimer = setInterval(() => {
      void this.runMaintenance(async () => {
        await maintainPostgresDlq(this.context);
        await purgeStalePostgresWorkers(this.context);
        await purgeStalePostgresBrokers(this.context);
      }, 'dlq');
      void this.runMaintenance(() => sweepPostgresEventRetention(this.context), 'event-retention');
      void this.postCommitMaintenance.run('completion-retention', () =>
        prunePostgresCompletionTombstones(this.context)
      );
    }, 60_000);
    this.cronTimer = setInterval(
      () => void this.runMaintenance(() => processPostgresCrons(this.context), 'cron'),
      this.config.pollIntervalMs
    );
  }

  private async runMaintenance(
    operation: () => Promise<unknown>,
    subsystem = 'maintenance'
  ): Promise<void> {
    await this.maintenanceFlights.run(subsystem, operation);
  }

  private async heartbeatBroker(): Promise<void> {
    try {
      await heartbeatPostgresBroker(this.context);
    } catch (error) {
      if (error instanceof PostgresBrokerSessionFencedError) this.stopMaintenance();
      throw error;
    }
  }
}
