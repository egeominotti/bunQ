import { SQL, type TransactionSQL } from 'bun';
import { PostgresQueueManager } from '../../src/application/postgresQueueManager';
import type { Job, JobId } from '../../src/domain/types/job';
import { PostgresQueueStore } from '../../src/infrastructure/persistence/postgres';
import {
  encodePostgresValue,
  eventPayload,
} from '../../src/infrastructure/persistence/postgres/codec';
import type { PostgresContext } from '../../src/infrastructure/persistence/postgres/context';
import {
  maxPostgresEventId,
  recordPostgresEventPruneWatermarks,
  summarizePostgresPrunedEvents,
} from '../../src/infrastructure/persistence/postgres/eventPruneWatermarks';
import { POSTGRES_EVENT_CHANNEL } from '../../src/infrastructure/persistence/postgres/schema';

export interface Deferred<T> {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
  readonly reject: (error: unknown) => void;
}

export interface PausablePostgresEventStream {
  cursor: number;
  subscription: { unlisten(): Promise<void> } | null;
  pollTimer: ReturnType<typeof setInterval> | null;
  drain(): Promise<void>;
}

export function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((accept, decline) => {
    resolve = accept;
    reject = decline;
  });
  return { promise, resolve, reject };
}

export function postgresManagerStore(manager: PostgresQueueManager): PostgresQueueStore {
  return (manager as unknown as { postgresStore: PostgresQueueStore }).postgresStore;
}

export function postgresManagerSnapshotHas(manager: PostgresQueueManager, id: JobId): boolean {
  const state = manager as unknown as { postgresSnapshot: { get(id: JobId): unknown } };
  return state.postgresSnapshot.get(id) !== undefined;
}

export function postgresEventStream(manager: PostgresQueueManager): PausablePostgresEventStream {
  return postgresManagerStore(manager).events as unknown as PausablePostgresEventStream;
}

export function postgresRaceContext(manager: PostgresQueueManager, sql: SQL): PostgresContext {
  return { sql, config: { ...postgresManagerStore(manager).context.config } };
}

export async function insertPostgresWaitingJob(
  tx: TransactionSQL,
  namespace: string,
  job: Job
): Promise<void> {
  await tx`
    INSERT INTO bunqueue_jobs (
      namespace, id, queue, payload, state, priority, lifo, run_at, created_at,
      attempts, max_attempts, ttl, timeout, unique_key, unique_expires_at,
      custom_id, group_id, parent_id
    ) VALUES (
      ${namespace}, ${String(job.id)}, ${job.queue}, ${encodePostgresValue(job)},
      'waiting', ${job.priority}, ${job.lifo}, ${job.runAt}, ${job.createdAt},
      ${job.attempts}, ${job.maxAttempts}, ${job.ttl}, ${job.timeout}, ${job.uniqueKey},
      NULL, ${job.customId}, ${job.groupId}, ${job.parentId}
    )
  `;
}

export async function insertPostgresPushedEvent(
  tx: TransactionSQL,
  namespace: string,
  job: Job
): Promise<number> {
  const [row] = await tx<{ id: number | string | bigint }[]>`
    INSERT INTO bunqueue_events
      (namespace, queue, event_type, job_id, occurred_at, payload)
    VALUES (
      ${namespace}, ${job.queue}, 'pushed', ${String(job.id)}, ${Date.now()},
      ${eventPayload({ job, state: 'waiting' })}
    )
    RETURNING id
  `;
  return Number(row.id);
}

export async function notifyPostgresEvent(
  tx: TransactionSQL,
  context: PostgresContext,
  queue: string,
  eventId: number,
  prunedThrough = 0
): Promise<void> {
  await tx.notify(
    POSTGRES_EVENT_CHANNEL,
    JSON.stringify({
      namespace: context.config.namespace,
      queue,
      eventId,
      ...(prunedThrough > 0 && { prunedThrough, scanPruneWatermarks: true }),
    })
  );
}

export async function trimAndNotifyPostgresEvent(
  tx: TransactionSQL,
  context: PostgresContext,
  queue: string,
  sourceEventId: number
): Promise<number> {
  const pruned = await tx<
    Array<{
      id: number | string | bigint;
      commit_seq: number | string | bigint | null;
    }>
  >`
    DELETE FROM bunqueue_events AS removed
    WHERE removed.namespace = ${context.config.namespace} AND removed.queue = ${queue}
      AND removed.id <= (
        SELECT id
        FROM bunqueue_events
        WHERE namespace = ${context.config.namespace} AND queue = ${queue}
        ORDER BY id DESC
        OFFSET 1
        LIMIT 1
      )
    RETURNING removed.id, (
      SELECT journal.commit_seq
      FROM bunqueue_event_commits AS journal
      WHERE journal.namespace = removed.namespace
        AND journal.transaction_id = removed.transaction_id
    ) AS commit_seq
  `;
  const prunedThrough = maxPostgresEventId(pruned);
  if (prunedThrough > 0) {
    await recordPostgresEventPruneWatermarks(tx, context, [
      { queue, sourceEventId, ...summarizePostgresPrunedEvents(pruned) },
    ]);
  }
  await notifyPostgresEvent(tx, context, queue, sourceEventId, prunedThrough);
  return prunedThrough;
}

export async function pausePostgresEventStream(
  manager: PostgresQueueManager
): Promise<PausablePostgresEventStream> {
  const stream = postgresEventStream(manager);
  if (stream.pollTimer) clearInterval(stream.pollTimer);
  stream.pollTimer = null;
  await stream.subscription?.unlisten();
  stream.subscription = null;
  await stream.drain();
  return stream;
}

export async function eventually(
  condition: () => boolean | Promise<boolean>,
  timeoutMs = 2_000
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  do {
    if (await condition()) return true;
    await Bun.sleep(20);
  } while (Date.now() < deadline);
  return false;
}

export function postgresWaitingIds(manager: PostgresQueueManager, queue: string): string[] {
  return manager
    .getJobs(queue, { state: 'waiting' })
    .map((job) => String(job.id))
    .sort();
}

export async function cleanupPostgresNamespace(url: string, namespace: string): Promise<void> {
  const sql = new SQL(url, { max: 2 });
  try {
    await sql.begin(async (tx) => {
      for (const table of [
        'bunqueue_metric_buckets',
        'bunqueue_metric_totals',
        'bunqueue_workers',
        'bunqueue_crons',
        'bunqueue_job_logs',
        'bunqueue_repeat_links',
        'bunqueue_flow_failures',
        'bunqueue_dependencies',
        'bunqueue_completions',
        'bunqueue_jobs',
        'bunqueue_queue_state',
        'bunqueue_event_prune_watermarks',
        'bunqueue_events',
        'bunqueue_event_commits',
        'bunqueue_brokers',
      ]) {
        await tx.unsafe(`DELETE FROM ${table} WHERE namespace = $1`, [namespace]);
      }
    });
  } finally {
    await sql.close({ timeout: 5 });
  }
}
