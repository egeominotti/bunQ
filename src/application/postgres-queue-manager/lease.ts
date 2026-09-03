import {
  DEFAULT_LOCK_TTL,
  jobId,
  lockToken,
  type JobId,
  type JobLock,
  type LockToken,
} from '../../domain/types/job';
import type { PostgresStoredJob } from '../../infrastructure/persistence/postgres';
import { PostgresQueueManagerDelivery } from './delivery';
import { applyPostgresJobProjection } from './projectionRefreshes';
import type { PostgresDirectProjectionTicket } from './projectionRefreshes';

interface LeaseRenewalRequest {
  readonly id: JobId;
  readonly token: string;
  readonly durationMs: number;
}

function leaseKey(id: JobId, token: string): string {
  return `${String(id)}\0${token}`;
}

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
      return (
        (await this.renewLeaseBatch([{ id: target, token: leaseToken, durationMs: duration }])) ===
        1
      );
    });
  }

  async heartbeatDurable(id: JobId, token?: string, duration?: number): Promise<boolean> {
    const leaseToken = this.tokenFor(id, token);
    if (!leaseToken) return false;
    return await this.extendLock(
      id,
      leaseToken,
      duration ?? this.postgresStore.config.leaseDurationMs
    );
  }

  async heartbeatBatchDurable(ids: JobId[], tokens?: string[]): Promise<number> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const requests = ids.flatMap((id, index) => {
        const token = this.tokenFor(id, tokens?.[index]);
        return token ? [{ id, token, durationMs: this.postgresStore.config.leaseDurationMs }] : [];
      });
      return await this.renewLeaseBatch(requests);
    });
  }

  override jobHeartbeat(id: JobId, token?: string): boolean {
    return this.operations.runSync(() => {
      const leaseToken = this.tokenFor(id, token);
      const row = this.postgresSnapshot.get(id);
      if (!leaseToken || row?.state !== 'active') return false;
      this.enqueueWrite(() =>
        this.renewLeaseBatch([
          { id, token: leaseToken, durationMs: this.postgresStore.config.leaseDurationMs },
        ]).then(() => undefined)
      );
      return true;
    });
  }

  override jobHeartbeatBatch(ids: JobId[], tokens?: string[]): number {
    return this.operations.runSync(() => {
      const requests = ids.flatMap((id, index) => {
        const token = this.tokenFor(id, tokens?.[index]);
        return token && this.postgresSnapshot.get(id)?.state === 'active'
          ? [{ id, token, durationMs: this.postgresStore.config.leaseDurationMs }]
          : [];
      });
      if (requests.length > 0) {
        this.enqueueWrite(() => this.renewLeaseBatch(requests).then(() => undefined));
      }
      return requests.length;
    });
  }

  override verifyLock(id: JobId, token: string): boolean {
    const row = this.postgresSnapshot.get(id);
    return row?.state === 'active' && this.tokenFor(id) === token;
  }

  override renewJobLock(id: JobId, token: string, ttl = DEFAULT_LOCK_TTL): boolean {
    return this.operations.runSync(() => {
      if (!this.verifyLock(id, token)) return false;
      this.enqueueWrite(() =>
        this.renewLeaseBatch([{ id, token, durationMs: ttl }]).then(() => undefined)
      );
      return true;
    });
  }

  private async renewLeaseBatch(requests: readonly LeaseRenewalRequest[]): Promise<number> {
    if (requests.length === 0) return 0;
    const tickets = new Map<JobId, PostgresDirectProjectionTicket>();
    for (const { id } of requests) {
      if (tickets.has(id)) continue;
      tickets.set(
        id,
        this.projectionRefreshes.beginDirect(id, this.postgresSnapshot.get(id)?.job.queue ?? '')
      );
    }
    let rows: Awaited<ReturnType<typeof this.postgresStore.renewMany>>;
    try {
      rows = await this.postgresStore.renewMany(requests);
    } catch (error) {
      for (const ticket of tickets.values()) this.projectionRefreshes.cancelDirect(ticket);
      await this.refreshJobs([...tickets.keys()]);
      throw error;
    }
    const renewed = new Set(rows.map(({ row, token }) => leaseKey(row.job.id, token)));
    const repairIds = new Set(tickets.keys());
    for (const { row } of rows) {
      const ticket = tickets.get(row.job.id);
      if (ticket && this.applyLeaseRenewal(row, ticket)) repairIds.delete(row.job.id);
    }
    for (const request of requests) {
      if (renewed.has(leaseKey(request.id, request.token))) continue;
      repairIds.add(request.id);
      if (this.activeTokens.get(request.id) === request.token) this.forgetToken(request.id);
    }
    if (repairIds.size > 0) await this.refreshJobs([...repairIds]);
    return requests.reduce(
      (count, request) => count + (renewed.has(leaseKey(request.id, request.token)) ? 1 : 0),
      0
    );
  }

  private applyLeaseRenewal(
    row: PostgresStoredJob,
    ticket: PostgresDirectProjectionTicket
  ): boolean {
    const current = this.postgresSnapshot.get(row.job.id);
    if (current?.state !== 'active' || current.token !== row.token) return false;
    if (!this.projectionRefreshes.consumeDirect(ticket)) return false;
    applyPostgresJobProjection(this.postgresSnapshot, this.activeTokens, row.job.id, {
      row,
      completion: null,
    });
    return true;
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
