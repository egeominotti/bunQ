import type { JobId } from '../../domain/types/job';
import { PostgresQueueManagerCloud } from './cloud';

interface ClientReleaseSession {
  readonly leases: Map<JobId, string>;
  released: number;
  inFlight: Promise<number> | null;
}

export class PostgresQueueManagerOperations extends PostgresQueueManagerCloud {
  private readonly clientReleaseSessions = new Map<string, ClientReleaseSession>();

  override async cancel(id: JobId): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const removed = await this.postgresStore.cancel(id);
      if (removed) this.postgresSnapshot.remove(id);
      return removed;
    });
  }

  override async updateProgress(id: JobId, progress: number, message?: string): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const updated = await this.postgresStore.updateProgress(id, progress, message);
      if (updated) await this.refreshJob(id);
      return updated;
    });
  }

  override async updateJobData(id: JobId, data: unknown): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const updated = await this.postgresStore.updateData(id, data);
      if (updated) await this.refreshJob(id);
      return updated;
    });
  }

  override async changePriority(id: JobId, priority: number, lifo?: boolean): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const updated = await this.postgresStore.changePriority(id, priority, lifo);
      if (updated) await this.refreshJob(id);
      return updated;
    });
  }

  override async promote(id: JobId): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const promoted = await this.postgresStore.promote(id);
      if (promoted) await this.refreshJob(id);
      return promoted;
    });
  }

  override async promoteJobs(queue: string, count?: number): Promise<number> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const promoted = await this.postgresStore.promoteMany(queue, count);
      if (promoted.length > 0) await this.refreshQueueAfterCommit(queue);
      return promoted.length;
    });
  }

  override async moveToDelayed(id: JobId, delay: number, token?: string): Promise<boolean> {
    return await this.runPostgresOperation(() => this.changeDelay(id, delay, token));
  }

  override async changeDelay(id: JobId, delay: number, token?: string): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const now = await this.postgresStore.now();
      const changed = await this.postgresStore.changeDelay(
        id,
        now + Math.max(0, delay),
        this.tokenFor(id, token) ?? undefined
      );
      if (changed) {
        this.forgetToken(id);
        await this.refreshJob(id);
      }
      return changed;
    });
  }

  override async moveActiveToWait(id: JobId, token?: string): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const now = await this.postgresStore.now();
      const moved = await this.postgresStore.changeDelay(
        id,
        now,
        this.tokenFor(id, token) ?? undefined
      );
      if (moved) {
        this.forgetToken(id);
        await this.refreshJob(id);
      }
      return moved;
    });
  }

  override async changeWaitingDelay(id: JobId, delay: number): Promise<boolean> {
    return await this.runPostgresOperation(() => this.changeDelay(id, delay));
  }

  override async moveToWaitingChildren(id: JobId, token?: string): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const moved = await this.postgresStore.moveToWaitingChildren(
        id,
        this.tokenFor(id, token) ?? undefined
      );
      if (moved) {
        this.forgetToken(id);
        await this.refreshJob(id);
      }
      return moved;
    });
  }

  override async discard(id: JobId, token?: string): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const removed = await this.postgresStore.discard(id, this.tokenFor(id, token) ?? undefined);
      if (removed) {
        this.forgetToken(id);
        await this.refreshJob(id);
      }
      return removed;
    });
  }

  override getDeduplicationJobId(queue: string, key: string): string | null {
    return this.postgresSnapshot.getByUniqueKey(queue, key)?.id ?? null;
  }

  override async removeDeduplicationKey(queue: string, key: string): Promise<number> {
    return await this.runPostgresOperation(async () => {
      const job = this.postgresSnapshot.getByUniqueKey(queue, key);
      if (!job) return 0;
      const removed = await this.postgresStore.clearUniqueKey(job.id);
      if (removed) await this.refreshJob(job.id);
      return removed ? 1 : 0;
    });
  }

  override async removeJobDeduplicationKey(id: string): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const removed = await this.postgresStore.clearUniqueKey(id as JobId);
      if (removed) await this.refreshJob(id as JobId);
      return removed;
    });
  }

  override async releaseClientJobs(clientId: string): Promise<number> {
    return await this.runPostgresOperation(() => this.runClientRelease(clientId));
  }

  override forceReleaseClientJobs(clientId: string): number {
    const admitted = this.operations.tryRunSync(() => {
      const session = this.getClientReleaseSession(clientId);
      const tracked = session.leases.size;
      if (tracked > 0)
        this.enqueueWrite(() => this.runClientRelease(clientId).then(() => undefined));
      return tracked;
    });
    if (admitted.accepted) return admitted.value;
    return this.clientReleaseSessions.get(clientId)?.leases.size ?? 0;
  }

  private getClientReleaseSession(clientId: string): ClientReleaseSession {
    const current = this.clientReleaseSessions.get(clientId);
    if (current) return current;
    const leases = new Map<JobId, string>();
    for (const id of this.clientJobs.get(clientId) ?? []) {
      const token = this.tokenFor(id);
      if (token) leases.set(id, token);
      else this.clientJobs.get(clientId)?.delete(id);
    }
    if (this.clientJobs.get(clientId)?.size === 0) this.clientJobs.delete(clientId);
    const session: ClientReleaseSession = { leases, released: 0, inFlight: null };
    this.clientReleaseSessions.set(clientId, session);
    return session;
  }

  private async runClientRelease(clientId: string): Promise<number> {
    const session = this.getClientReleaseSession(clientId);
    if (session.inFlight) return await session.inFlight;
    const attempt = this.releaseClientSession(clientId, session).finally(() => {
      if (session.inFlight === attempt) session.inFlight = null;
    });
    session.inFlight = attempt;
    return await attempt;
  }

  private async releaseClientSession(
    clientId: string,
    session: ClientReleaseSession
  ): Promise<number> {
    for (const [id, token] of [...session.leases]) {
      const released = await this.postgresStore.releaseClientLease(id, token);
      session.leases.delete(id);
      this.clientJobs.get(clientId)?.delete(id);
      if (this.clientJobs.get(clientId)?.size === 0) this.clientJobs.delete(clientId);
      if (this.activeTokens.get(id) === token) this.activeTokens.delete(id);
      if (released) session.released++;
      await this.refreshJob(id);
    }
    if (session.leases.size === 0) this.clientReleaseSessions.delete(clientId);
    return session.released;
  }
}
