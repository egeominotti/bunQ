import type { DlqEntry, DlqFilter } from '../../domain/types/dlq';
import type { JobId } from '../../domain/types/job';
import { PostgresQueueManagerRelationships } from './relationships';

function matchesFilter(entry: DlqEntry, filter: DlqFilter, now: number): boolean {
  if (filter.reason && entry.reason !== filter.reason) return false;
  if (filter.olderThan !== undefined && entry.enteredAt >= filter.olderThan) return false;
  if (filter.newerThan !== undefined && entry.enteredAt <= filter.newerThan) return false;
  if (
    filter.expired !== undefined &&
    (entry.expiresAt !== null && entry.expiresAt <= now) !== filter.expired
  ) {
    return false;
  }
  return true;
}

function boundedLimit(value: number | undefined): number {
  if (value === undefined) return Number.MAX_SAFE_INTEGER;
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : 0;
}

/** Durable maintenance commands that cannot use the synchronous SQLite façade. */
export class PostgresQueueManagerMaintenance extends PostgresQueueManagerRelationships {
  async retryDlqDurable(
    queue: string,
    id?: JobId,
    count?: number,
    filter?: DlqFilter
  ): Promise<number> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const limit = boundedLimit(count ?? filter?.limit);
      if (limit === 0) return 0;
      const now = await this.postgresStore.now();
      let entries = await this.postgresStore.getDlq(queue, 2_147_483_647);
      if (id) entries = entries.filter((entry) => entry.job.id === id);
      if (filter) entries = entries.filter((entry) => matchesFilter(entry, filter, now));
      const offset = Math.max(0, filter?.offset ?? 0);
      entries = entries.slice(offset, offset + limit);
      let retried = 0;
      for (const entry of entries) {
        if (await this.postgresStore.retry(entry.job.id)) retried++;
      }
      if (retried > 0) await this.refreshQueue(queue);
      return retried;
    });
  }

  async purgeDlqDurable(queue: string): Promise<number> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const ids = await this.postgresStore.purgeDlq(queue);
      if (ids.length > 0) await this.refreshQueue(queue);
      return ids.length;
    });
  }

  async removeDlqJobDurable(queue: string, id: JobId): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const removed = await this.postgresStore.removeDlq(queue, id);
      if (removed) this.postgresSnapshot.remove(id);
      return removed;
    });
  }

  async retryCompletedDurable(
    queue: string,
    id?: JobId,
    options: { limit?: number; timestamp?: number } = {}
  ): Promise<number> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const limit = boundedLimit(options.limit);
      if (limit === 0) return 0;
      let rows = await this.postgresStore.list(queue, {
        states: ['completed'],
        limit: 2_147_483_647,
        asc: true,
      });
      if (id) rows = rows.filter((row) => row.job.id === id);
      const timestamp = options.timestamp;
      if (timestamp !== undefined) {
        rows = rows.filter((row) => (row.job.completedAt ?? Infinity) <= timestamp);
      }
      let retried = 0;
      for (const row of rows.slice(0, limit)) {
        if (await this.postgresStore.retry(row.job.id)) retried++;
      }
      if (retried > 0) await this.refreshQueue(queue);
      return retried;
    });
  }

  async cleanDurable(
    queue: string,
    graceMs: number,
    state?: string,
    limit?: number
  ): Promise<JobId[]> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const ids = await this.postgresStore.clean(queue, graceMs, state, limit);
      if (ids.length > 0) await this.refreshQueue(queue);
      return ids;
    });
  }
}
