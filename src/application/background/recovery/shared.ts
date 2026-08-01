import { createDlqEntry, FailureReason } from '../../../domain/types/dlq';
import type { Job } from '../../../domain/types/job';
import { queueLog } from '../../../shared/logger';
import type { BackgroundContext } from '../../types';

export const RECOVERY_BATCH_SIZE = 10000;

export type RecoveryStorage = NonNullable<BackgroundContext['storage']>;
export type QueueStateRecord = ReturnType<RecoveryStorage['loadQueueState']>[number];

export function quarantineCorruptDependsOn(ctx: BackgroundContext, job: Job): void {
  if (!ctx.storage) return;
  const entry = createDlqEntry(job, FailureReason.Unknown, 'corrupt-dependency-metadata');
  ctx.storage.saveDlqEntry(entry);
  ctx.storage.deleteJob(job.id);
  ctx.registerQueueName(job.queue);
  queueLog.error('Recovered job with corrupt depends_on metadata -> routed to DLQ', {
    jobId: String(job.id),
    queue: job.queue,
  });
}
