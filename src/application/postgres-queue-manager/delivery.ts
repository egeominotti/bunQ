import type { AtomicFlowBatchInput, AtomicFlowBatchResult } from '../../domain/types/flow';
import {
  createJob,
  DEFAULT_LOCK_TTL,
  generateJobId,
  jobId,
  type Job,
  type JobId,
  type JobInput,
} from '../../domain/types/job';
import { validateRepeatJobInput } from '../repeatJobs';
import { PostgresQueueManagerTerminalDelivery } from './terminalDelivery';

export class PostgresQueueManagerDelivery extends PostgresQueueManagerTerminalDelivery {
  override async push(queue: string, input: JobInput): Promise<Job> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      validateRepeatJobInput(input);
      const id = input.customId ? jobId(input.customId) : generateJobId();
      const admitted = await this.postgresStore.insert(
        createJob(id, queue, input, await this.postgresStore.now())
      );
      await this.refreshJob(admitted.job.id);
      return admitted.job;
    });
  }

  override async pushBatch(queue: string, inputs: JobInput[]): Promise<JobId[]> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      for (const input of inputs) validateRepeatJobInput(input);
      const now = await this.postgresStore.now();
      const jobs = inputs.map((input) =>
        createJob(input.customId ? jobId(input.customId) : generateJobId(), queue, input, now)
      );
      const stored = await this.postgresStore.insertMany(jobs);
      await this.refreshJobs(stored.map((job) => job.id));
      return stored.map((job) => job.id);
    });
  }

  override async pushFlow(batch: AtomicFlowBatchInput): Promise<AtomicFlowBatchResult> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const jobs = await this.postgresStore.insertFlow(batch);
      await Promise.all(
        new Set(jobs.map((job) => job.queue)).values().map((queue) => this.refreshQueue(queue))
      );
      return { jobs };
    });
  }

  override async pull(queue: string, timeoutMs = 0, signal?: AbortSignal): Promise<Job | null> {
    const claims = await this.claimUntil(
      queue,
      1,
      this.postgresStore.config.brokerId,
      timeoutMs,
      undefined,
      signal
    );
    return claims[0]?.job ?? null;
  }

  override async pullWithLock(
    queue: string,
    owner: string,
    timeoutMs = 0,
    lockTtl = DEFAULT_LOCK_TTL,
    signal?: AbortSignal
  ): Promise<{ job: Job | null; token: string | null }> {
    const claims = await this.claimUntil(queue, 1, owner, timeoutMs, lockTtl, signal);
    const claim = claims[0];
    return claim ? { job: claim.job, token: claim.token } : { job: null, token: null };
  }

  override async pullBatch(
    queue: string,
    count: number,
    timeoutMs = 0,
    signal?: AbortSignal
  ): Promise<Job[]> {
    const claims = await this.claimUntil(
      queue,
      count,
      this.postgresStore.config.brokerId,
      timeoutMs,
      undefined,
      signal
    );
    return claims.map((claim) => claim.job);
  }

  // oxlint-disable-next-line max-params -- public API includes cancellation and lease policy
  override async pullBatchWithLock(
    queue: string,
    count: number,
    owner: string,
    timeoutMs = 0,
    lockTtl = DEFAULT_LOCK_TTL,
    signal?: AbortSignal
  ): Promise<{ jobs: Job[]; tokens: string[] }> {
    const claims = await this.claimUntil(queue, count, owner, timeoutMs, lockTtl, signal);
    return {
      jobs: claims.map((claim) => claim.job),
      tokens: claims.map((claim) => claim.token),
    };
  }

  // oxlint-disable-next-line max-params -- internal bridge mirrors the public pull contract
  private async claimUntil(
    queue: string,
    count: number,
    owner: string,
    timeoutMs: number,
    leaseDurationMs?: number,
    signal?: AbortSignal
  ) {
    await this.postgresReady;
    await this.flushPostgresWrites();
    const deadline = Date.now() + Math.max(0, timeoutMs);
    do {
      const claims = await this.runPostgresOperation(async () => {
        const admitted = await this.postgresStore.claim(queue, count, owner, leaseDurationMs);
        for (const claim of admitted) {
          this.activeTokens.set(claim.job.id, claim.token);
          this.postgresSnapshot.claim(claim);
        }
        return admitted;
      });
      if (claims.length > 0 || timeoutMs <= 0 || signal?.aborted) return claims;
      const remaining = deadline - Date.now();
      if (remaining <= 0) return [];
      await this.postgresStore.waitForWork(
        queue,
        Math.min(remaining, this.postgresStore.config.pollIntervalMs),
        signal
      );
    } while (!signal?.aborted);
    return [];
  }
}
