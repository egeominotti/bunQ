import { hostname } from 'node:os';
import { SQL } from 'bun';
import {
  heartbeatPostgresBroker,
  recoverExpiredPostgresLeases,
  unregisterPostgresBroker,
} from './recovery';
import { releasePostgresBrokerLeases } from './leaseRelease';
import { PostgresEventStream } from './events';
import { prunePostgresEventCommits } from './eventJournal';
import { maintainPostgresDlq, repairPostgresDlq } from './dlqMaintenance';
import { processPostgresCrons, reconcilePostgresCronsOnStartup } from './crons';
import { purgeStalePostgresWorkers, removePostgresBrokerWorkers } from './workers';
import { initializePostgresSchema } from './schemaInitialization';
import type { PostgresContext } from './context';
import type { PostgresStorageConfig, PostgresStorageHealth, PostgresStoreEvent } from './types';
import { PostgresPostCommitMaintenance } from './postCommitMaintenance';
import { sweepPostgresEventRetention } from './telemetry';
import { prunePostgresCompletionTombstones } from './completionLifecycle';

function resolveConfig(config: PostgresStorageConfig) {
  const url = new URL(config.url);
  if (url.protocol !== 'postgres:' && url.protocol !== 'postgresql:') {
    throw new Error('PostgreSQL storage requires a postgres:// or postgresql:// URL');
  }
  if (config.maxQueueEvents !== undefined && config.maxQueueEvents < 1) {
    throw new Error('PostgreSQL storage requires maxQueueEvents to be at least 1');
  }
  return {
    url: config.url,
    namespace: config.namespace?.trim() || 'default',
    brokerId:
      config.brokerId?.trim() || `${hostname()}:${process.pid}:${Bun.randomUUIDv7().slice(-12)}`,
    poolSize: Math.max(2, config.poolSize ?? 10),
    leaseDurationMs: Math.max(1000, config.leaseDurationMs ?? 30_000),
    pollIntervalMs: Math.max(25, config.pollIntervalMs ?? 250),
    maxQueueEvents: Math.max(1, Math.floor(config.maxQueueEvents ?? 10_000)),
    maxMetricDataPoints: Math.max(0, Math.floor(config.maxMetricDataPoints ?? 20_160)),
    maxCompletedJobs: Math.max(1, Math.floor(config.maxCompletedJobs ?? 50_000)),
    maxJobResults: Math.max(0, Math.floor(config.maxJobResults ?? 10_000)),
  };
}

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
  private readonly healthErrors = new Map<string, { message: string; since: number }>();
  private readonly unsubscribeEventStatus: () => void;
  private readonly postCommitMaintenance: PostgresPostCommitMaintenance;

  constructor(config: PostgresStorageConfig) {
    const resolved = resolveConfig(config);
    const sql = new SQL(resolved.url, {
      max: resolved.poolSize,
      connectionTimeout: 10,
      idleTimeout: 30,
      maxLifetime: 3600,
    });
    this.postCommitMaintenance = new PostgresPostCommitMaintenance(
      (subsystem, error) => this.reportMaintenance(subsystem, error),
      Math.max(25, resolved.pollIntervalMs)
    );
    this.context = {
      sql,
      config: resolved,
      postCommitMaintenance: (subsystem, operation) =>
        this.postCommitMaintenance.run(subsystem, operation),
    };
    this.events = new PostgresEventStream(this.context);
    this.unsubscribeEventStatus = this.events.subscribeDrainStatus((error) => {
      if (error === null) this.clearHealthError('event-stream');
      else this.recordHealthError('event-stream', error);
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
    if (initialized) {
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
        () => unregisterPostgresBroker(this.context),
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
    const databaseClosed = this.leasesReleased && this.workersRemoved && this.brokerUnregistered;
    if (!initialized || databaseClosed) {
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
    const errors = [...this.healthErrors.entries()].sort(([left], [right]) =>
      left.localeCompare(right)
    );
    const messages = errors.map(([, { message }]) => message);
    const sinceValues = errors.map(([, { since }]) => since);
    return {
      ok: messages.length === 0,
      error: messages.length > 0 ? messages.join('; ') : null,
      since: sinceValues.length > 0 ? Math.min(...sinceValues) : null,
      backend: 'postgres',
      brokerId: this.config.brokerId,
      namespace: this.config.namespace,
    };
  }

  reportQueueRefresh(queue: string, error: unknown): void {
    const key = `queue-refresh:${queue}`;
    if (error === null) {
      this.clearHealthError(key);
      return;
    }
    this.recordHealthError(key, error, `Queue refresh ${queue}: `);
  }

  private reportMaintenance(subsystem: string, error: unknown): void {
    const key = `maintenance:${subsystem}`;
    if (error === null) this.clearHealthError(key);
    else this.recordHealthError(key, error);
  }

  onEvent(listener: (event: PostgresStoreEvent) => void): () => void {
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
      await reconcilePostgresCronsOnStartup(this.context);
      await heartbeatPostgresBroker(this.context);
      await recoverExpiredPostgresLeases(this.context);
      await repairPostgresDlq(this.context);
      await prunePostgresCompletionTombstones(this.context);
      await sweepPostgresEventRetention(this.context);
      await prunePostgresEventCommits(this.context);
      await this.events.start();
      if (!this.closing) this.startMaintenance();
      this.clearHealthError('lifecycle');
    } catch (error) {
      this.recordHealthError('lifecycle', error);
      throw error;
    }
  }

  private startMaintenance(): void {
    const heartbeatMs = Math.max(1000, Math.floor(this.config.leaseDurationMs / 3));
    this.heartbeatTimer = setInterval(
      () => void this.runMaintenance(() => heartbeatPostgresBroker(this.context), 'heartbeat'),
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
        await prunePostgresEventCommits(this.context);
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
    try {
      await operation();
      this.reportMaintenance(subsystem, null);
    } catch (error) {
      this.reportMaintenance(subsystem, error);
    }
  }

  private recordHealthError(key: string, error: unknown, prefix = ''): void {
    const message = error instanceof Error ? error.message : String(error);
    const since = this.healthErrors.get(key)?.since ?? Date.now();
    this.healthErrors.set(key, { message: `${prefix}${message}`, since });
  }

  private clearHealthError(key: string): void {
    this.healthErrors.delete(key);
  }
}
