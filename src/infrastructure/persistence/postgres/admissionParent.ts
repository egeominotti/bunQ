import type { TransactionSQL } from 'bun';
import type { Job } from '../../../domain/types/job';
import type { PostgresContext } from './context';
import { lockPostgresFlowParent } from './flowFailures';

export function linkedPostgresParentQueue(job: Job): string | null {
  if (
    !job.parentId ||
    typeof job.data !== 'object' ||
    job.data === null ||
    Array.isArray(job.data)
  ) {
    return null;
  }
  const data = job.data as Record<string, unknown>;
  if (String(data.__parentId) !== String(job.parentId)) return null;
  return typeof data.__parentQueue === 'string' && data.__parentQueue.length > 0
    ? data.__parentQueue
    : null;
}

export async function lockPostgresAdmissionParent(
  tx: TransactionSQL,
  ctx: PostgresContext,
  job: Job,
  expectedQueue: string
): Promise<void> {
  const parentId = job.parentId!;
  if (parentId === job.id) throw new Error('A job cannot be its own parent');
  await lockPostgresFlowParent(tx, ctx, parentId);
  const rows = await tx<Array<{ queue: string; state: string }>>`
    SELECT queue, state FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(parentId)}
    FOR UPDATE
  `;
  const parent = rows[0];
  if (!parent) throw new Error(`Parent job not found: ${String(parentId)}`);
  if (parent.queue !== expectedQueue) {
    throw new Error(
      `Parent job ${String(parentId)} belongs to queue ${parent.queue}, not ${expectedQueue}`
    );
  }
  if (['active', 'completed', 'failed'].includes(parent.state)) {
    throw new Error(`Parent job ${String(parentId)} is not linkable`);
  }
}
