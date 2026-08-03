import type { Job, JobInput } from '../../domain/types/job';
import type { Shard } from '../../domain/queue/shard';
import type { DurableAdmissionMetadata } from '../../infrastructure/persistence/sqlite';
import { commitCustomIdAdmission, type CustomIdResult } from './customId';
import type { PushContext } from './pushContext';
import {
  insertPreparedJobToShard,
  prepareJobInsertion,
  type PreparedJobInsertion,
} from './pushInsert';

export type AcceptedCustomId = Exclude<CustomIdResult, { skip: true }>;

export interface PushTarget {
  queue: string;
  shard: Shard;
  shardIdx: number;
}

interface PublishPreparedJobOptions {
  job: Job;
  input: JobInput;
  target: PushTarget;
  customId: AcceptedCustomId;
  prepared: PreparedJobInsertion;
  ctx: PushContext;
  registerDedupKey?: boolean;
}

export function admissionMetadata(
  customId: AcceptedCustomId,
  prepared: PreparedJobInsertion
): DurableAdmissionMetadata | undefined {
  const retireGenerationId = customId.retirement ? customId.id : undefined;
  if (!retireGenerationId && prepared.completionPins.length === 0) return undefined;
  return {
    retireGenerationId,
    completionPins: prepared.completionPins.length > 0 ? prepared.completionPins : undefined,
  };
}

/** Publish only non-throwing RAM ownership after the storage admission commits. */
export function publishPreparedJob(options: PublishPreparedJobOptions): void {
  const { job, input, target, customId, prepared, ctx, registerDedupKey = true } = options;
  commitCustomIdAdmission(input, customId, ctx);
  if (registerDedupKey && job.uniqueKey) {
    target.shard.registerUniqueKeyWithTtl(
      target.queue,
      job.uniqueKey,
      job.id,
      job.deduplicationTtl ?? undefined
    );
  }
  insertPreparedJobToShard(job, target, ctx, prepared);
}

/** Persist first whenever admission can fail, then expose the job in RAM. */
export function acceptStandardJob(
  job: Job,
  input: JobInput,
  target: PushTarget,
  customId: AcceptedCustomId,
  ctx: PushContext
): { storageHandled: boolean } {
  const prepared = prepareJobInsertion(job, ctx);
  const admission = admissionMetadata(customId, prepared);
  const mustPersistBeforePublish = Boolean(ctx.storage && (input.durable || admission));
  if (mustPersistBeforePublish) {
    ctx.storage?.insertJob(job, input.durable, admission);
  }
  publishPreparedJob({ job, input, target, customId, prepared, ctx });
  return { storageHandled: mustPersistBeforePublish };
}

export function throwPushErrors(errors: unknown[], message: string): void {
  const present = errors.filter((error) => error !== undefined);
  if (present.length === 1) throw present[0];
  if (present.length > 1) throw new AggregateError(present, message);
}
