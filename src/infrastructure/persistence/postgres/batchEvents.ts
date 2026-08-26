import type { TransactionSQL } from 'bun';
import type { Job } from '../../../domain/types/job';
import { eventPayload, postgresByteaBase64 } from './codec';
import type { PostgresContext } from './context';
import {
  recordPostgresEventPruneWatermarks,
  summarizePostgresPrunedEvents,
  type PostgresEventPruneWatermarkInput,
} from './eventPruneWatermarks';
import { prunePostgresQueueEvents, tryLockPostgresEventRetention } from './eventRetention';
import { POSTGRES_EVENT_CHANNEL } from './schema';
import type { PostgresJobState } from './types';

export interface PostgresBatchJobEventInput {
  readonly job: Job;
  readonly type: string;
  readonly state?: PostgresJobState;
  readonly result?: unknown;
  readonly error?: string;
  readonly removed?: boolean;
}

interface EventRange {
  readonly startId: number;
  readonly endId: number;
}

interface TrimResult {
  readonly watermarks: Map<string, number>;
  readonly skippedQueues: Set<string>;
}

function payloadForEvent(input: PostgresBatchJobEventInput): Uint8Array {
  return eventPayload({
    job: input.job,
    ...(input.state && { state: input.state }),
    ...(Object.hasOwn(input, 'result') && { result: input.result }),
    ...(input.error !== undefined && { error: input.error }),
    ...(input.removed !== undefined && { removed: input.removed }),
  });
}

async function insertEvents(
  tx: TransactionSQL,
  ctx: PostgresContext,
  inputs: readonly PostgresBatchJobEventInput[],
  now: number
): Promise<EventRange> {
  const inserted = await tx<Array<{ id: number | string | bigint }>>`
    INSERT INTO bunqueue_events
      (namespace, queue, event_type, job_id, occurred_at, payload)
    SELECT
      ${ctx.config.namespace}, batch.queue, batch.event_type, batch.job_id, ${now},
      decode(batch.payload_base64, 'base64')
    FROM unnest(
      ${tx.array(
        inputs.map((input) => input.job.queue),
        'TEXT'
      )},
      ${tx.array(
        inputs.map((input) => input.type),
        'TEXT'
      )},
      ${tx.array(
        inputs.map((input) => String(input.job.id)),
        'TEXT'
      )},
      ${tx.array(
        inputs.map((input) => postgresByteaBase64(payloadForEvent(input))),
        'TEXT'
      )}
    ) WITH ORDINALITY AS batch(queue, event_type, job_id, payload_base64, position)
    ORDER BY batch.position
    RETURNING id
  `;
  const ids = inserted.map((row) => Number(row.id));
  return { startId: Math.min(...ids), endId: Math.max(...ids) };
}

async function trimEvents(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queues: readonly string[],
  sourceEventId: number
): Promise<TrimResult> {
  const watermarks: PostgresEventPruneWatermarkInput[] = [];
  const skippedQueues = new Set<string>();
  for (const queue of [...queues].sort()) {
    if (!(await tryLockPostgresEventRetention(tx, ctx, queue))) {
      skippedQueues.add(queue);
      continue;
    }
    const pruned = await prunePostgresQueueEvents(tx, ctx, queue, ctx.config.maxQueueEvents);
    const prune = summarizePostgresPrunedEvents(pruned);
    if (prune.prunedThrough > 0) watermarks.push({ queue, sourceEventId, ...prune });
  }
  await recordPostgresEventPruneWatermarks(tx, ctx, watermarks);
  return {
    watermarks: new Map(watermarks.map(({ queue, prunedThrough }) => [queue, prunedThrough])),
    skippedQueues,
  };
}

/** Persist job events while amortizing retention and wakeups per affected queue. */
export async function recordPostgresJobEvents(
  tx: TransactionSQL,
  ctx: PostgresContext,
  inputs: readonly PostgresBatchJobEventInput[],
  now: number
): Promise<void> {
  if (inputs.length === 0) return;
  const queues = [...new Set(inputs.map((input) => input.job.queue))];
  const eventRange = await insertEvents(tx, ctx, inputs, now);
  const { watermarks, skippedQueues } = await trimEvents(tx, ctx, queues, eventRange.endId);
  for (const queue of queues) {
    const prunedThrough = watermarks.get(queue);
    await tx.notify(
      POSTGRES_EVENT_CHANNEL,
      JSON.stringify({
        namespace: ctx.config.namespace,
        queue,
        brokerId: ctx.config.brokerId,
        eventStartId: eventRange.startId,
        eventEndId: eventRange.endId,
        ...(prunedThrough !== undefined && { prunedThrough }),
        ...(skippedQueues.has(queue) && { retentionRequested: true }),
        ...(watermarks.size > 0 && { scanPruneWatermarks: true }),
      })
    );
  }
}
