import type { SQL, TransactionSQL } from 'bun';
import type { DlqEntry, DlqRetryState } from '../../../domain/types/dlq';
import type { Job, JobId } from '../../../domain/types/job';
import { eventPayload } from './codec';
import {
  recordPostgresEventPruneWatermarks,
  summarizePostgresPrunedEvents,
} from './eventPruneWatermarks';
import { prunePostgresQueueEvents, tryLockPostgresEventRetention } from './eventRetention';
import { POSTGRES_EVENT_CHANNEL } from './schema';
import { recordPostgresMetricAdditions } from './metricWrites';
import type { PostgresJobState, ResolvedPostgresStorageConfig } from './types';

export type PostgresReadSql = SQL | TransactionSQL;

export interface PostgresContext {
  readonly sql: SQL;
  readonly config: ResolvedPostgresStorageConfig;
  readonly postCommitMaintenance?: (
    subsystem: string,
    operation: () => Promise<unknown>
  ) => Promise<void>;
}

/** Run work that follows an already committed transition without changing its outcome. */
export async function runPostgresPostCommitMaintenance(
  ctx: PostgresContext,
  subsystem: string,
  operation: () => Promise<unknown>
): Promise<void> {
  if (ctx.postCommitMaintenance) {
    await ctx.postCommitMaintenance(subsystem, operation);
    return;
  }
  await operation();
}

export function databaseNowSql(): string {
  return 'floor(extract(epoch from clock_timestamp()) * 1000)::bigint';
}

export async function databaseNow(tx: SQL | TransactionSQL): Promise<number> {
  const [row] = await tx<{ now: number | string | bigint }[]>`
    SELECT floor(extract(epoch from clock_timestamp()) * 1000)::bigint AS now
  `;
  return Number(row.now);
}

export interface RecordPostgresEventInput {
  readonly queue: string;
  readonly type: string;
  readonly jobId: JobId;
  readonly occurredAt: number;
  readonly job?: Job;
  readonly state?: PostgresJobState;
  readonly result?: unknown;
  readonly error?: string;
  readonly removed?: boolean;
  readonly dlqEntry?: DlqEntry | null;
  readonly dlqRetryState?: DlqRetryState | null;
}

export async function recordPostgresEvent(
  tx: TransactionSQL,
  ctx: PostgresContext,
  input: RecordPostgresEventInput
): Promise<number> {
  const payload = eventPayload({
    ...(input.job && { job: input.job }),
    ...(input.state && { state: input.state }),
    ...(Object.hasOwn(input, 'result') && { result: input.result }),
    ...(input.error !== undefined && { error: input.error }),
    ...(input.removed !== undefined && { removed: input.removed }),
    ...(Object.hasOwn(input, 'dlqEntry') && { dlqEntry: input.dlqEntry }),
    ...(Object.hasOwn(input, 'dlqRetryState') && { dlqRetryState: input.dlqRetryState }),
  });
  const [row] = await tx<{ id: number | string | bigint }[]>`
    INSERT INTO bunqueue_events
      (namespace, queue, event_type, job_id, occurred_at, payload)
    VALUES
      (${ctx.config.namespace}, ${input.queue}, ${input.type}, ${String(input.jobId)},
       ${input.occurredAt}, ${payload})
    RETURNING id
  `;
  const eventId = Number(row.id);
  const eventLimit = ctx.config.maxQueueEvents;
  const retentionLockAcquired = await tryLockPostgresEventRetention(tx, ctx, input.queue);
  const pruned = retentionLockAcquired
    ? await prunePostgresQueueEvents(tx, ctx, input.queue, eventLimit)
    : [];
  const prune = summarizePostgresPrunedEvents(pruned);
  const { prunedThrough } = prune;
  if (prunedThrough > 0) {
    await recordPostgresEventPruneWatermarks(tx, ctx, [
      { queue: input.queue, sourceEventId: eventId, ...prune },
    ]);
  }
  const metricType =
    input.type === 'completed'
      ? 'completed'
      : input.type === 'failed' && input.state === 'failed'
        ? 'failed'
        : null;
  if (metricType) {
    await recordPostgresMetricAdditions(
      tx,
      ctx,
      metricType,
      [{ queue: input.queue, count: 1 }],
      input.occurredAt
    );
  }
  await tx.notify(
    POSTGRES_EVENT_CHANNEL,
    JSON.stringify({
      namespace: ctx.config.namespace,
      queue: input.queue,
      brokerId: ctx.config.brokerId,
      eventId,
      eventStartId: eventId,
      eventEndId: eventId,
      invalidateQueue: input.type.startsWith('queue-'),
      ...(prunedThrough > 0 && { prunedThrough }),
      ...(!retentionLockAcquired && { retentionRequested: true }),
    })
  );
  return eventId;
}
