import { PostgresQueueManagerState } from './state';

/** Database-authoritative group getters shared by every PostgreSQL broker. */
export class PostgresQueueManagerGroupQueries extends PostgresQueueManagerState {
  override async getGroupJobsCount(queue: string, groupId: string): Promise<number> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.getGroupJobsCount(queue, groupId);
    });
  }

  override async getGroupsJobsCount(queue: string): Promise<number> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.getGroupJobsCount(queue);
    });
  }

  override async getGroupActiveCount(queue: string, groupId: string): Promise<number> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.getGroupActiveCount(queue, groupId);
    });
  }

  override async getGroupRateLimit(queue: string, groupId: string) {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.getGroupRateLimit(queue, groupId);
    });
  }

  override async getGroupRateLimitTtl(
    queue: string,
    groupId: string,
    maxJobs?: number
  ): Promise<number> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.getGroupRateLimitTtl(queue, groupId, maxJobs);
    });
  }

  override async getGroupConcurrency(queue: string, groupId: string): Promise<number | null> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.getGroupConcurrency(queue, groupId);
    });
  }

  override async isGroupPaused(queue: string, groupId: string): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.getGroupPaused(queue, groupId);
    });
  }
}
