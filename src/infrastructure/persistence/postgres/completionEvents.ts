import type { TransactionSQL } from 'bun';
import type { Job } from '../../../domain/types/job';
import { recordPostgresJobEvents } from './batchEvents';
import type { PostgresContext } from './context';
import { recordPostgresMetricAdditions } from './metricWrites';

interface CompletionEventInput {
  readonly job: Job;
  readonly result: unknown;
  readonly removed: boolean;
}

async function updateMetrics(
  tx: TransactionSQL,
  ctx: PostgresContext,
  inputs: readonly CompletionEventInput[],
  now: number
): Promise<void> {
  const counts = new Map<string, number>();
  for (const input of inputs) counts.set(input.job.queue, (counts.get(input.job.queue) ?? 0) + 1);
  await recordPostgresMetricAdditions(
    tx,
    ctx,
    'completed',
    [...counts].map(([queue, count]) => ({ queue, count })),
    now
  );
}

/** Persist one completion event per job while amortizing queue-level bookkeeping. */
export async function recordPostgresCompletionEvents(
  tx: TransactionSQL,
  ctx: PostgresContext,
  inputs: readonly CompletionEventInput[],
  now: number
): Promise<void> {
  if (inputs.length === 0) return;
  await recordPostgresJobEvents(
    tx,
    ctx,
    inputs.map((input) => ({
      job: input.job,
      type: 'completed',
      state: 'completed',
      result: input.result,
      removed: input.removed,
    })),
    now
  );
  await updateMetrics(tx, ctx, inputs, now);
}
