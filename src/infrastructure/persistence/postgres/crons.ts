import type { TransactionSQL } from 'bun';
import { createCronJob, type CronJob, type CronJobInput } from '../../../domain/types/cron';
import { createJob, generateJobId, type JobInput } from '../../../domain/types/job';
import {
  expandCronShortcut,
  getNextCronRun,
  getNextIntervalRun,
  validateCronExpression,
} from '../../scheduler/cronParser';
import { postgresAdvisoryLockName } from './advisoryLocks';
import { admitPostgresJob } from './admission';
import { decodePostgresValue, encodePostgresValue } from './codec';
import { databaseNow, type PostgresContext, type PostgresReadSql } from './context';
import { hasActivePostgresWorker } from './workers';
import { postgresBrokerStaleMs } from './brokerSessions';

export interface PostgresCronRow {
  name: string;
  payload: Uint8Array;
  next_run: number | string | bigint;
  executions: number;
  max_limit: number | null;
}

export function decodePostgresCron(row: PostgresCronRow): CronJob {
  const cron = decodePostgresValue<CronJob | null>(row.payload, null, `postgresCron:${row.name}`);
  if (!cron) throw new Error(`Corrupt PostgreSQL cron payload for ${row.name}`);
  cron.nextRun = Number(row.next_run);
  cron.executions = row.executions;
  return cron;
}

function validateInput(input: CronJobInput): void {
  if (!input.schedule && !input.repeatEvery) {
    throw new Error('Cron job must have either schedule or repeatEvery');
  }
  if (input.schedule) {
    const error = validateCronExpression(expandCronShortcut(input.schedule), input.timezone);
    if (error) throw new Error(`Invalid cron expression: ${error}`);
  }
}

function nextRun(input: CronJobInput, now: number): number {
  if (input.schedule) {
    return getNextCronRun(expandCronShortcut(input.schedule), now, input.timezone);
  }
  return getNextIntervalRun(input.repeatEvery as number, now);
}

export async function upsertPostgresCron(
  ctx: PostgresContext,
  input: CronJobInput
): Promise<CronJob> {
  validateInput(input);
  return await ctx.sql.begin(async (tx) => {
    const existing = await tx<PostgresCronRow[]>`
      SELECT name, payload, next_run, executions, max_limit
      FROM bunqueue_crons
      WHERE namespace = ${ctx.config.namespace} AND name = ${input.name}
      FOR UPDATE
    `;
    const now = await databaseNow(tx);
    const cron = createCronJob(
      input,
      input.immediately && !existing[0] ? now : nextRun(input, now)
    );
    cron.executions = existing[0]?.executions ?? 0;
    await tx`
      INSERT INTO bunqueue_crons
        (namespace, name, payload, next_run, executions, max_limit, updated_at)
      VALUES
        (${ctx.config.namespace}, ${cron.name}, ${encodePostgresValue(cron)}, ${cron.nextRun},
         ${cron.executions}, ${cron.maxLimit}, ${now})
      ON CONFLICT (namespace, name) DO UPDATE SET
        payload = excluded.payload,
        next_run = excluded.next_run,
        executions = excluded.executions,
        max_limit = excluded.max_limit,
        updated_at = excluded.updated_at
    `;
    return cron;
  });
}

export async function getPostgresCron(
  ctx: PostgresContext,
  name: string
): Promise<CronJob | undefined> {
  const rows = await ctx.sql<PostgresCronRow[]>`
    SELECT name, payload, next_run, executions, max_limit
    FROM bunqueue_crons
    WHERE namespace = ${ctx.config.namespace} AND name = ${name}
  `;
  return rows[0] ? decodePostgresCron(rows[0]) : undefined;
}

export async function listPostgresCrons(
  ctx: PostgresContext,
  sql: PostgresReadSql = ctx.sql
): Promise<CronJob[]> {
  const rows = await sql<PostgresCronRow[]>`
    SELECT name, payload, next_run, executions, max_limit
    FROM bunqueue_crons
    WHERE namespace = ${ctx.config.namespace}
    ORDER BY name
  `;
  return rows.map(decodePostgresCron);
}

export async function removePostgresCron(ctx: PostgresContext, name: string): Promise<boolean> {
  const rows = await ctx.sql<{ name: string }[]>`
    DELETE FROM bunqueue_crons
    WHERE namespace = ${ctx.config.namespace} AND name = ${name}
    RETURNING name
  `;
  return rows.length > 0;
}

function spawnedInput(cron: CronJob): JobInput {
  const uniqueKey = cron.uniqueKey ?? (cron.preventOverlap ? `cron:${cron.name}` : undefined);
  const options = cron.jobOptions;
  return {
    name: cron.jobName,
    data: cron.data,
    priority: cron.priority,
    uniqueKey,
    dedup: cron.dedup ?? undefined,
    maxAttempts: options?.maxAttempts,
    backoff: options?.backoff,
    timeout: options?.timeout,
    delay: options?.delay,
    stallTimeout: options?.stallTimeout,
    removeOnComplete: options?.removeOnComplete,
    removeOnFail: options?.removeOnFail,
  };
}

function followingRun(cron: CronJob, now: number): number {
  if (cron.schedule) {
    return getNextCronRun(expandCronShortcut(cron.schedule), now, cron.timezone ?? undefined);
  }
  const interval = cron.repeatEvery as number;
  const scheduled = getNextIntervalRun(interval, cron.nextRun);
  return scheduled <= now ? getNextIntervalRun(interval, now) : scheduled;
}

/** Recalculate missed rows only on the oldest live broker for this namespace. */
export async function reconcilePostgresCronsOnStartup(ctx: PostgresContext): Promise<number> {
  const lockName = postgresAdvisoryLockName('cron-startup', ctx.config.namespace);
  return await ctx.sql.begin(async (tx) => {
    await tx`
      SELECT pg_advisory_xact_lock(hashtextextended(${lockName}, 0))
    `;
    const now = await databaseNow(tx);
    const [leader] = await tx<{ elected: boolean }[]>`
      SELECT
        broker_id = ${ctx.config.brokerId}
          AND session_id IS NOT DISTINCT FROM ${ctx.config.brokerSessionId} AS elected
      FROM bunqueue_brokers
      WHERE namespace = ${ctx.config.namespace}
        AND heartbeat_at > ${now - postgresBrokerStaleMs(ctx)}
      ORDER BY started_at, broker_id, session_id NULLS FIRST
      LIMIT 1
    `;
    if (!leader?.elected) return 0;
    const rows = await tx<PostgresCronRow[]>`
      SELECT name, payload, next_run, executions, max_limit
      FROM bunqueue_crons
      WHERE namespace = ${ctx.config.namespace} AND next_run < ${now}
      ORDER BY next_run, name
      FOR UPDATE
    `;
    let reconciled = 0;
    for (const row of rows) {
      const cron = decodePostgresCron(row);
      if (!cron.skipMissedOnRestart && !cron.skipIfNoWorker) continue;
      cron.nextRun = cron.schedule
        ? getNextCronRun(expandCronShortcut(cron.schedule), now, cron.timezone ?? undefined)
        : getNextIntervalRun(cron.repeatEvery as number, now);
      await tx`
        UPDATE bunqueue_crons
        SET payload = ${encodePostgresValue(cron)}, next_run = ${cron.nextRun}, updated_at = ${now}
        WHERE namespace = ${ctx.config.namespace} AND name = ${cron.name}
      `;
      reconciled++;
    }
    return reconciled;
  });
}

async function fireCron(
  tx: TransactionSQL,
  ctx: PostgresContext,
  row: PostgresCronRow,
  now: number
): Promise<boolean> {
  const cron = decodePostgresCron(row);
  const admitted = await admitPostgresJob(
    tx,
    ctx,
    createJob(generateJobId(), cron.queue, spawnedInput(cron), now),
    { linkParent: false }
  );
  if (!admitted.inserted && cron.preventOverlap) {
    await skipCron(tx, ctx, cron, now);
    return false;
  }
  cron.executions++;
  cron.nextRun = followingRun(cron, now);
  await tx`
    UPDATE bunqueue_crons
    SET payload = ${encodePostgresValue(cron)}, next_run = ${cron.nextRun},
        executions = ${cron.executions}, updated_at = ${now}
    WHERE namespace = ${ctx.config.namespace} AND name = ${cron.name}
  `;
  return true;
}

async function skipCron(
  tx: TransactionSQL,
  ctx: PostgresContext,
  cron: CronJob,
  now: number
): Promise<void> {
  cron.nextRun = followingRun(cron, now);
  await tx`
    UPDATE bunqueue_crons
    SET payload = ${encodePostgresValue(cron)}, next_run = ${cron.nextRun}, updated_at = ${now}
    WHERE namespace = ${ctx.config.namespace} AND name = ${cron.name}
  `;
}

/** Advance and enqueue each due cron once under PostgreSQL row locks. */
export async function processPostgresCrons(ctx: PostgresContext, limit = 100): Promise<number> {
  return await ctx.sql.begin(async (tx) => {
    const now = await databaseNow(tx);
    const rows = await tx<PostgresCronRow[]>`
      SELECT name, payload, next_run, executions, max_limit
      FROM bunqueue_crons
      WHERE namespace = ${ctx.config.namespace}
        AND next_run <= ${now}
        AND (max_limit IS NULL OR executions < max_limit)
      ORDER BY next_run, name
      LIMIT ${Math.max(1, limit)}
      FOR UPDATE SKIP LOCKED
    `;
    let fired = 0;
    for (const row of rows) {
      const cron = decodePostgresCron(row);
      if (cron.skipIfNoWorker && !(await hasActivePostgresWorker(tx, ctx, cron.queue, now))) {
        await skipCron(tx, ctx, cron, now);
        continue;
      }
      if (await fireCron(tx, ctx, row, now)) fired++;
    }
    return fired;
  });
}
