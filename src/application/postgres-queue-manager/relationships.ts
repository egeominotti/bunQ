import type { JobId } from '../../domain/types/job';
import { PostgresQueueManagerLease } from './lease';

/** PostgreSQL-backed parent/child relationship mutations. */
export class PostgresQueueManagerRelationships extends PostgresQueueManagerLease {
  override async getFailedChildrenValues(parentId: JobId): Promise<Record<string, string>> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.getFailedChildrenValues(parentId);
    });
  }

  override async getIgnoredChildrenFailures(parentId: JobId): Promise<Record<string, string>> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.getIgnoredChildrenFailures(parentId);
    });
  }

  override async updateJobParent(childId: JobId, parentId: JobId): Promise<void> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      await this.postgresStore.updateParent(childId, parentId);
      await Promise.all([this.refreshJob(childId), this.refreshJob(parentId)]);
    });
  }

  override async removeChildDependency(childId: JobId): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const child = await this.postgresStore.getJob(childId);
      const parentId = child?.job.parentId ?? null;
      const removed = await this.postgresStore.removeDependency(childId);
      if (removed) {
        await this.refreshJob(childId);
        if (parentId) await this.refreshJob(parentId);
      }
      return removed;
    });
  }

  override async removeUnprocessedChildren(parentId: JobId): Promise<void> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const removed = await this.postgresStore.removeUnprocessedChildren(parentId);
      if (removed.length > 0) await this.refreshJobs([parentId, ...removed]);
    });
  }
}
