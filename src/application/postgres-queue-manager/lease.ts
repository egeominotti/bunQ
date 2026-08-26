import {
  DEFAULT_LOCK_TTL,
  jobId,
  lockToken,
  type JobId,
  type JobLock,
  type LockToken,
} from '../../domain/types/job';
import { PostgresQueueManagerDelivery } from './delivery';

export class PostgresQueueManagerLease extends PostgresQueueManagerDelivery {
  override async extendLock(
    id: JobId | string,
    token: string | null,
    duration: number
  ): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const target = jobId(String(id));
      const leaseToken = token ?? this.tokenFor(target);
      if (!leaseToken) return false;
      const expiresAt = await this.postgresStore.renew(target, leaseToken, duration);
      if (expiresAt !== null) {
        await this.refreshJob(target);
        return true;
      }
      this.forgetToken(target);
      await this.refreshJob(target);
      return false;
    });
  }

  async heartbeatDurable(id: JobId, token?: string, duration?: number): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      const leaseToken = this.tokenFor(id, token);
      if (!leaseToken) return false;
      return await this.extendLock(
        id,
        leaseToken,
        duration ?? this.postgresStore.config.leaseDurationMs
      );
    });
  }

  async heartbeatBatchDurable(ids: JobId[], tokens?: string[]): Promise<number> {
    return await this.runPostgresOperation(async () => {
      const renewed = await Promise.all(
        ids.map((id, index) => this.heartbeatDurable(id, tokens?.[index]))
      );
      return renewed.filter(Boolean).length;
    });
  }

  override jobHeartbeat(id: JobId, token?: string): boolean {
    return this.operations.runSync(() => {
      const leaseToken = this.tokenFor(id, token);
      const row = this.postgresSnapshot.get(id);
      if (!leaseToken || row?.state !== 'active') return false;
      this.enqueueWrite(async () => {
        const renewed = await this.postgresStore.renew(
          id,
          leaseToken,
          this.postgresStore.config.leaseDurationMs
        );
        if (renewed !== null) await this.refreshJob(id);
      });
      return true;
    });
  }

  override jobHeartbeatBatch(ids: JobId[], tokens?: string[]): number {
    return this.operations.runSync(() =>
      ids.reduce((count, id, index) => count + (this.jobHeartbeat(id, tokens?.[index]) ? 1 : 0), 0)
    );
  }

  override verifyLock(id: JobId, token: string): boolean {
    const row = this.postgresSnapshot.get(id);
    return row?.state === 'active' && this.tokenFor(id) === token;
  }

  override renewJobLock(id: JobId, token: string, ttl = DEFAULT_LOCK_TTL): boolean {
    return this.operations.runSync(() => {
      if (!this.verifyLock(id, token)) return false;
      this.enqueueWrite(async () => {
        const renewed = await this.postgresStore.renew(id, token, ttl);
        if (renewed !== null) await this.refreshJob(id);
      });
      return true;
    });
  }

  override getLockInfo(id: JobId): JobLock | null {
    const row = this.postgresSnapshot.get(id);
    const token = this.tokenFor(id);
    if (row?.state !== 'active' || !token || row.leaseUntil === null) return null;
    return {
      jobId: id,
      token: lockToken(token),
      owner: row.leaseOwner ?? this.postgresStore.config.brokerId,
      createdAt: row.job.startedAt ?? Date.now(),
      expiresAt: row.leaseUntil,
      lastRenewalAt: row.job.lastHeartbeat,
      renewalCount: row.leaseRenewals,
      ttl: Math.max(1, row.leaseUntil - row.job.lastHeartbeat),
    };
  }

  override createLock(id: JobId, _owner: string, _ttl = DEFAULT_LOCK_TTL): LockToken | null {
    const token = this.tokenFor(id);
    return token ? lockToken(token) : null;
  }
}
