import type { TransactionSQL } from 'bun';
import { databaseNow, type PostgresContext } from './context';

interface BrokerRow {
  readonly broker_id: string;
  readonly session_id: string | null;
  readonly heartbeat_at: number | string | bigint;
}

export class PostgresBrokerIdentityInUseError extends Error {
  constructor(brokerId: string) {
    super(`PostgreSQL brokerId "${brokerId}" is already active`);
    this.name = 'PostgresBrokerIdentityInUseError';
  }
}

export class PostgresBrokerSessionFencedError extends Error {
  constructor(brokerId: string) {
    super(`PostgreSQL broker session for "${brokerId}" has been fenced`);
    this.name = 'PostgresBrokerSessionFencedError';
  }
}

export function postgresBrokerStaleMs(ctx: PostgresContext): number {
  const heartbeatMs = Math.max(1000, Math.floor(ctx.config.leaseDurationMs / 3));
  return Math.max(ctx.config.leaseDurationMs, heartbeatMs * 3);
}

export async function registerPostgresBroker(ctx: PostgresContext): Promise<void> {
  const now = await databaseNow(ctx.sql);
  const staleBefore = now - postgresBrokerStaleMs(ctx);
  const [existing] = await ctx.sql<BrokerRow[]>`
    SELECT broker_id, session_id, heartbeat_at FROM bunqueue_brokers
    WHERE namespace = ${ctx.config.namespace} AND broker_id = ${ctx.config.brokerId}
  `;
  if (
    existing &&
    existing.session_id !== ctx.config.brokerSessionId &&
    Number(existing.heartbeat_at) > staleBefore
  ) {
    throw new PostgresBrokerIdentityInUseError(ctx.config.brokerId);
  }

  const rows = await ctx.sql.begin(async (tx) => {
    const transactionNow = await databaseNow(tx);
    return await tx<{ broker_id: string }[]>`
      INSERT INTO bunqueue_brokers AS broker
        (namespace, broker_id, session_id, started_at, heartbeat_at)
      VALUES
        (${ctx.config.namespace}, ${ctx.config.brokerId}, ${ctx.config.brokerSessionId},
         ${transactionNow}, ${transactionNow})
      ON CONFLICT (namespace, broker_id) DO UPDATE SET
        session_id = excluded.session_id,
        started_at = excluded.started_at,
        heartbeat_at = excluded.heartbeat_at
      WHERE broker.session_id IS NOT DISTINCT FROM excluded.session_id
         OR broker.heartbeat_at <= ${transactionNow - postgresBrokerStaleMs(ctx)}
      RETURNING broker_id
    `;
  });
  if (rows.length === 0) throw new PostgresBrokerIdentityInUseError(ctx.config.brokerId);
}

export async function lockPostgresBrokerSession(
  tx: TransactionSQL,
  ctx: PostgresContext
): Promise<void> {
  const rows = await tx<{ broker_id: string }[]>`
    SELECT broker_id FROM bunqueue_brokers
    WHERE namespace = ${ctx.config.namespace}
      AND broker_id = ${ctx.config.brokerId}
      AND session_id = ${ctx.config.brokerSessionId}
    FOR SHARE
  `;
  if (rows.length === 0) throw new PostgresBrokerSessionFencedError(ctx.config.brokerId);
}

export async function heartbeatPostgresBroker(ctx: PostgresContext): Promise<void> {
  const now = await databaseNow(ctx.sql);
  const rows = await ctx.sql<{ broker_id: string }[]>`
    UPDATE bunqueue_brokers SET heartbeat_at = ${now}
    WHERE namespace = ${ctx.config.namespace}
      AND broker_id = ${ctx.config.brokerId}
      AND session_id = ${ctx.config.brokerSessionId}
    RETURNING broker_id
  `;
  if (rows.length === 0) throw new PostgresBrokerSessionFencedError(ctx.config.brokerId);
}

export async function unregisterPostgresBroker(ctx: PostgresContext): Promise<void> {
  await ctx.sql`
    DELETE FROM bunqueue_brokers
    WHERE namespace = ${ctx.config.namespace}
      AND broker_id = ${ctx.config.brokerId}
      AND session_id = ${ctx.config.brokerSessionId}
  `;
}

export async function purgeStalePostgresBrokers(ctx: PostgresContext): Promise<number> {
  const now = await databaseNow(ctx.sql);
  const rows = await ctx.sql<{ broker_id: string }[]>`
    DELETE FROM bunqueue_brokers
    WHERE namespace = ${ctx.config.namespace}
      AND heartbeat_at <= ${now - postgresBrokerStaleMs(ctx)}
      AND NOT (
        broker_id = ${ctx.config.brokerId}
        AND session_id IS NOT DISTINCT FROM ${ctx.config.brokerSessionId}
      )
    RETURNING broker_id
  `;
  return rows.length;
}
