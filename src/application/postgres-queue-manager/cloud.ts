import type { JobId } from '../../domain/types/job';
import type { PostgresJobState } from '../../infrastructure/persistence/postgres';
import type { PostgresCloudReadModel } from '../../infrastructure/persistence/postgres/cloudReadModel';
import { PostgresQueueManagerServices } from './services';

const POSTGRES_STATES = new Set<PostgresJobState>([
  'waiting',
  'prioritized',
  'delayed',
  'waiting-children',
  'active',
  'completed',
  'failed',
]);

function pageIndex(value: number | undefined, fallback: number): number {
  if (value === undefined || !Number.isFinite(value)) return fallback;
  return Math.min(Number.MAX_SAFE_INTEGER, Math.max(0, Math.floor(value)));
}

function durableStates(value?: string | string[]): PostgresJobState[] | undefined {
  if (value === undefined) return undefined;
  const requested = Array.isArray(value) ? value : [value];
  const normalized = requested.flatMap((state) =>
    state === 'paused' ? ['waiting', 'prioritized'] : [state]
  );
  return [...new Set(normalized)].filter((state): state is PostgresJobState =>
    POSTGRES_STATES.has(state as PostgresJobState)
  );
}

/** PostgreSQL-specific Cloud read port with no eventually-consistent fallback. */
export class PostgresQueueManagerCloud extends PostgresQueueManagerServices {
  getCloudProcessStats() {
    return super.getStats();
  }

  async readCloudSnapshotDurable(): Promise<PostgresCloudReadModel> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.loadCloudReadModel();
    });
  }

  async listCloudJobsDurable(
    queue: string,
    options: { state?: string | string[]; start?: number; end?: number; asc?: boolean } = {}
  ) {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const start = pageIndex(options.start, 0);
      const end = Math.max(start, pageIndex(options.end, 100));
      const rows = await this.postgresStore.list(queue, {
        states: durableStates(options.state),
        offset: start,
        limit: end - start,
        asc: options.asc ?? true,
      });
      return rows.map((row) => row.job);
    });
  }

  async getCloudResultDurable(id: JobId): Promise<unknown> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const result = await this.postgresStore.getResult(id);
      return result.found ? result.result : null;
    });
  }
}
