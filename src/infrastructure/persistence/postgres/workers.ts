import type { SQL, TransactionSQL } from 'bun';
import type { Worker } from '../../../domain/types/worker';
import { decodePostgresValue, encodePostgresValue } from './codec';
import type { PostgresContext } from './context';
import { databaseNow } from './context';

interface WorkerRow {
  id: string;
  payload: Uint8Array;
}

export async function savePostgresWorker(ctx: PostgresContext, worker: Worker): Promise<void> {
  await ctx.sql.begin(async (tx) => {
    worker.lastSeen = await databaseNow(tx);
    await tx`
      INSERT INTO bunqueue_workers
        (namespace, id, broker_id, client_id, queues, payload, last_seen)
      VALUES
        (${ctx.config.namespace}, ${worker.id}, ${ctx.config.brokerId}, ${worker.clientId},
         ${tx.array(worker.queues, 'TEXT')}, ${encodePostgresValue(worker)}, ${worker.lastSeen})
      ON CONFLICT (namespace, id) DO UPDATE SET
        broker_id = excluded.broker_id,
        client_id = excluded.client_id,
        queues = excluded.queues,
        payload = excluded.payload,
        last_seen = excluded.last_seen
    `;
  });
}

export async function removePostgresWorker(
  ctx: PostgresContext,
  id: string,
  clientId?: string
): Promise<boolean> {
  const expectedClientId = clientId ?? null;
  const rows = await ctx.sql<{ id: string }[]>`
    DELETE FROM bunqueue_workers
    WHERE namespace = ${ctx.config.namespace} AND id = ${id}
      AND broker_id = ${ctx.config.brokerId}
      AND (${expectedClientId}::text IS NULL OR client_id IS NOT DISTINCT FROM ${expectedClientId})
    RETURNING id
  `;
  return rows.length > 0;
}

export async function heartbeatPostgresWorker(
  ctx: PostgresContext,
  id: string,
  stats?: { activeJobs?: number; processed?: number; failed?: number },
  clientId?: string
): Promise<boolean> {
  return await ctx.sql.begin(async (tx) => {
    const expectedClientId = clientId ?? null;
    const rows = await tx<WorkerRow[]>`
      SELECT id, payload FROM bunqueue_workers
      WHERE namespace = ${ctx.config.namespace} AND id = ${id}
        AND broker_id = ${ctx.config.brokerId}
        AND (${expectedClientId}::text IS NULL OR client_id IS NOT DISTINCT FROM ${expectedClientId})
      FOR UPDATE
    `;
    const row = rows[0];
    if (!row) return false;
    const worker = decodePostgresValue<Worker | null>(
      row.payload,
      null,
      `postgresWorker:${row.id}`
    );
    if (!worker) throw new Error(`Corrupt PostgreSQL worker payload for ${row.id}`);
    worker.lastSeen = await databaseNow(tx);
    if (stats?.activeJobs !== undefined) worker.activeJobs = stats.activeJobs;
    if (stats?.processed !== undefined) worker.processedJobs = stats.processed;
    if (stats?.failed !== undefined) worker.failedJobs = stats.failed;
    if (clientId !== undefined) worker.clientId = clientId;
    await tx`
      UPDATE bunqueue_workers
      SET broker_id = ${ctx.config.brokerId}, client_id = ${worker.clientId},
          payload = ${encodePostgresValue(worker)}, last_seen = ${worker.lastSeen}
      WHERE namespace = ${ctx.config.namespace} AND id = ${id}
        AND broker_id = ${ctx.config.brokerId}
        AND (${expectedClientId}::text IS NULL OR client_id IS NOT DISTINCT FROM ${expectedClientId})
    `;
    return true;
  });
}

export async function removePostgresClientWorkers(
  ctx: PostgresContext,
  clientId: string
): Promise<number> {
  const rows = await ctx.sql<{ id: string }[]>`
    DELETE FROM bunqueue_workers
    WHERE namespace = ${ctx.config.namespace}
      AND broker_id = ${ctx.config.brokerId} AND client_id = ${clientId}
    RETURNING id
  `;
  return rows.length;
}

export async function removePostgresBrokerWorkers(ctx: PostgresContext): Promise<number> {
  const rows = await ctx.sql<{ id: string }[]>`
    DELETE FROM bunqueue_workers
    WHERE namespace = ${ctx.config.namespace} AND broker_id = ${ctx.config.brokerId}
    RETURNING id
  `;
  return rows.length;
}

export async function listPostgresWorkers(ctx: PostgresContext): Promise<Worker[]> {
  const rows = await ctx.sql<WorkerRow[]>`
    SELECT id, payload FROM bunqueue_workers
    WHERE namespace = ${ctx.config.namespace}
    ORDER BY id
  `;
  return rows.flatMap((row) => {
    const worker = decodePostgresValue<Worker | null>(
      row.payload,
      null,
      `postgresWorker:${row.id}`
    );
    return worker ? [worker] : [];
  });
}

export async function hasActivePostgresWorker(
  sql: SQL | TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  now: number,
  timeoutMs = 30_000
): Promise<boolean> {
  const rows = await sql<{ found: boolean }[]>`
    SELECT EXISTS (
      SELECT 1 FROM bunqueue_workers
      WHERE namespace = ${ctx.config.namespace}
        AND ${queue} = ANY(queues)
        AND last_seen > ${now - timeoutMs}
    ) AS found
  `;
  return rows[0]?.found ?? false;
}

export async function purgeStalePostgresWorkers(
  ctx: PostgresContext,
  requestedNow?: number,
  staleMs = 90_000
): Promise<number> {
  const now = requestedNow ?? (await databaseNow(ctx.sql));
  const rows = await ctx.sql<{ id: string }[]>`
    DELETE FROM bunqueue_workers
    WHERE namespace = ${ctx.config.namespace} AND last_seen <= ${now - staleMs}
    RETURNING id
  `;
  return rows.length;
}
