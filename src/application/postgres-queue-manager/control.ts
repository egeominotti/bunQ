import { DEFAULT_DLQ_CONFIG, type DlqConfig } from '../../domain/types/dlq';
import { DEFAULT_STALL_CONFIG, type StallConfig } from '../../domain/types/stall';
import type { PostgresQueueState } from '../../infrastructure/persistence/postgres';
import { normalizePostgresRateLimit } from '../../infrastructure/persistence/postgres/rateLimit';
import { PostgresQueueManagerMaintenance } from './maintenance';

const PENDING_STATES = ['waiting', 'prioritized', 'delayed', 'waiting-children'] as const;

function normalizeDlq(current: DlqConfig, patch: Record<string, unknown>): DlqConfig {
  const next = { ...current };
  if (typeof patch.autoRetry === 'boolean') next.autoRetry = patch.autoRetry;
  if (Number.isFinite(patch.autoRetryInterval)) {
    next.autoRetryInterval = Math.max(0, Math.floor(patch.autoRetryInterval as number));
  }
  if (Number.isFinite(patch.maxAutoRetries)) {
    next.maxAutoRetries = Math.max(0, Math.floor(patch.maxAutoRetries as number));
  }
  if (patch.maxAge === null) next.maxAge = null;
  else if (Number.isFinite(patch.maxAge)) {
    next.maxAge = Math.max(0, Math.floor(patch.maxAge as number));
  }
  if (Number.isFinite(patch.maxEntries)) {
    next.maxEntries = Math.max(1, Math.floor(patch.maxEntries as number));
  }
  return next;
}

/** Durable queue controls and policies shared by every PostgreSQL broker. */
export class PostgresQueueManagerControl extends PostgresQueueManagerMaintenance {
  override async setGroupRateLimit(
    queue: string,
    groupId: string,
    max: number,
    duration: number
  ): Promise<void> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      await this.postgresStore.setGroupRateLimit(queue, groupId, max, duration);
    });
  }

  override async removeGroupRateLimit(queue: string, groupId: string): Promise<number> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.removeGroupRateLimit(queue, groupId);
    });
  }

  override async setGroupConcurrency(
    queue: string,
    groupId: string,
    concurrency: number
  ): Promise<void> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      await this.postgresStore.setGroupConcurrency(queue, groupId, concurrency);
    });
  }

  override async removeGroupConcurrency(queue: string, groupId: string): Promise<number> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.removeGroupConcurrency(queue, groupId);
    });
  }

  override pause(queue: string): void {
    this.operations.runSync(() => {
      this.setLocalQueueState(queue, { paused: true });
      this.enqueueWrite(() => this.postgresStore.pause(queue, true));
    });
  }

  override resume(queue: string): void {
    this.operations.runSync(() => {
      this.setLocalQueueState(queue, { paused: false });
      this.enqueueWrite(() => this.postgresStore.pause(queue, false));
    });
  }

  async pauseDurable(queue: string, paused: boolean): Promise<void> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      await this.postgresStore.pause(queue, paused);
      await this.refreshQueueAfterCommit(queue);
    });
  }

  override drain(queue: string): number {
    return this.operations.runSync(() => {
      const count = this.postgresSnapshot.removeStates(queue, PENDING_STATES);
      this.enqueueWrite(async () => {
        await this.postgresStore.drain(queue);
        await this.refreshQueueAfterCommit(queue);
      });
      return count;
    });
  }

  async drainDurable(queue: string): Promise<number> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const count = await this.postgresStore.drain(queue);
      await this.refreshQueueAfterCommit(queue);
      return count;
    });
  }

  override obliterate(queue: string): void {
    this.operations.runSync(() => {
      this.postgresSnapshot.removeStates(queue, [
        ...PENDING_STATES,
        'active',
        'completed',
        'failed',
      ]);
      this.enqueueWrite(async () => {
        await this.postgresStore.obliterate(queue);
        await this.refreshQueueAfterCommit(queue);
      });
    });
  }

  async obliterateDurable(queue: string): Promise<void> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      await this.postgresStore.obliterate(queue);
      await this.refreshQueueAfterCommit(queue);
    });
  }

  override setRateLimit(queue: string, limit: number, durationMs?: number, ttlMs?: number): void {
    this.operations.runSync(() => {
      const normalized = normalizePostgresRateLimit(durationMs, ttlMs);
      this.setLocalQueueState(queue, {
        rateLimit: limit,
        rateDurationMs: normalized.durationMs,
        rateExpiresAt: normalized.ttlMs === null ? null : Date.now() + normalized.ttlMs,
      });
      this.enqueueWrite(() =>
        this.postgresStore.setRateLimit(queue, limit, normalized.durationMs, normalized.ttlMs)
      );
    });
  }

  override clearRateLimit(queue: string): void {
    this.operations.runSync(() => {
      this.setLocalQueueState(queue, {
        rateLimit: null,
        rateDurationMs: null,
        rateExpiresAt: null,
      });
      this.enqueueWrite(() => this.postgresStore.setRateLimit(queue, null, null));
    });
  }

  override setConcurrency(queue: string, limit: number): void {
    this.operations.runSync(() => {
      this.setLocalQueueState(queue, { concurrencyLimit: limit });
      this.enqueueWrite(() => this.postgresStore.setConcurrency(queue, limit));
    });
  }

  override clearConcurrency(queue: string): void {
    this.operations.runSync(() => {
      this.setLocalQueueState(queue, { concurrencyLimit: null });
      this.enqueueWrite(() => this.postgresStore.setConcurrency(queue, null));
    });
  }

  async setRateLimitDurable(
    queue: string,
    limit: number | null,
    durationMs: number | null,
    ttlMs?: number | null
  ): Promise<void> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const normalized = normalizePostgresRateLimit(durationMs, ttlMs);
      await this.postgresStore.setRateLimit(queue, limit, normalized.durationMs, normalized.ttlMs);
      await this.refreshQueueAfterCommit(queue);
    });
  }

  async setConcurrencyDurable(queue: string, limit: number | null): Promise<void> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      await this.postgresStore.setConcurrency(queue, limit);
      await this.refreshQueueAfterCommit(queue);
    });
  }

  override setStallConfig(queue: string, patch: Record<string, unknown>): void {
    this.operations.runSync(() => {
      const config = { ...this.getStallConfig(queue), ...patch } as StallConfig;
      this.setLocalQueueState(queue, { stallConfig: config });
      this.enqueueWrite(() => this.postgresStore.setStallConfig(queue, config));
    });
  }

  override getStallConfig(queue: string): StallConfig {
    return this.postgresSnapshot.queueState(queue)?.stallConfig ?? DEFAULT_STALL_CONFIG;
  }

  async setStallConfigDurable(queue: string, patch: Record<string, unknown>): Promise<void> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const state = await this.postgresStore.getQueueState(queue);
      const config = { ...state.stallConfig, ...patch } as StallConfig;
      await this.postgresStore.setStallConfig(queue, config);
      await this.refreshQueueAfterCommit(queue);
    });
  }

  override setDlqConfig(queue: string, patch: Record<string, unknown>): void {
    this.operations.runSync(() => {
      const config = normalizeDlq(this.getDlqConfig(queue), patch);
      this.setLocalQueueState(queue, { dlqConfig: config });
      this.enqueueWrite(() => this.postgresStore.setDlqConfig(queue, config));
    });
  }

  override getDlqConfig(queue: string): DlqConfig {
    return this.postgresSnapshot.queueState(queue)?.dlqConfig ?? DEFAULT_DLQ_CONFIG;
  }

  async setDlqConfigDurable(queue: string, patch: Record<string, unknown>): Promise<void> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const state = await this.postgresStore.getQueueState(queue);
      await this.postgresStore.setDlqConfig(queue, normalizeDlq(state.dlqConfig, patch));
      await this.refreshQueueAfterCommit(queue);
    });
  }

  private setLocalQueueState(
    queue: string,
    patch: Partial<Omit<PostgresQueueState, 'queue'>>
  ): void {
    const current = this.postgresSnapshot.queueState(queue) ?? {
      queue,
      paused: false,
      rateLimit: null,
      rateDurationMs: null,
      rateWindowStartedAt: null,
      rateExpiresAt: null,
      rateCount: 0,
      concurrencyLimit: null,
      stallConfig: DEFAULT_STALL_CONFIG,
      dlqConfig: DEFAULT_DLQ_CONFIG,
    };
    this.postgresSnapshot.setQueueState({ ...current, ...patch });
  }
}
