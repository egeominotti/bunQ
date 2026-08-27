import { expect, test } from 'bun:test';
import { SQL } from 'bun';
import type { PostgresContext } from '../src/infrastructure/persistence/postgres/context';
import { initializePostgresSchema } from '../src/infrastructure/persistence/postgres/schemaInitialization';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const LIVE_DEDUP_PREDICATE =
  "unique_keyisnotnullandstate=anyarray['waiting'::text,'prioritized'::text," +
  "'delayed'::text,'waiting-children'::text,'active'::text]";

function context(sql: SQL, url: string): PostgresContext {
  return {
    sql,
    config: {
      url,
      namespace: 'schema-dedup-guard',
      brokerId: 'schema-dedup-guard',
      poolSize: 2,
      leaseDurationMs: 30_000,
      pollIntervalMs: 25,
      maxQueueEvents: 100,
      maxMetricDataPoints: 100,
      maxCompletedJobs: 100,
      maxJobResults: 100,
    },
  };
}

async function withIsolatedDatabase(run: (sql: SQL, ctx: PostgresContext) => Promise<void>) {
  const admin = new SQL(postgresUrl!, { max: 1 });
  const name = `bq_dedup_${Date.now()}_${crypto.randomUUID().replaceAll('-', '').slice(0, 12)}`;
  const url = new URL(postgresUrl!);
  url.pathname = `/${name}`;
  let created = false;
  try {
    await admin.unsafe(`CREATE DATABASE "${name}"`).simple();
    created = true;
    const sql = new SQL(url.toString(), { max: 2 });
    try {
      await run(sql, context(sql, url.toString()));
    } finally {
      await sql.close({ timeout: 5 });
    }
  } finally {
    if (created) await admin.unsafe(`DROP DATABASE "${name}" WITH (FORCE)`).simple();
    await admin.close({ timeout: 5 });
  }
}

async function liveDedupIndexState(sql: SQL) {
  const [state] = await sql<
    Array<{
      columns: string[];
      key_columns: number;
      predicate: string;
      total_columns: number;
      unique: boolean;
    }>
  >`
    SELECT
      indexes.indisunique AS unique,
      indexes.indnkeyatts AS key_columns,
      indexes.indnatts AS total_columns,
      ARRAY(
        SELECT attribute.attname::text
        FROM unnest(indexes.indkey::smallint[]) WITH ORDINALITY AS key(attnum, position)
        JOIN pg_attribute AS attribute
          ON attribute.attrelid = indexes.indrelid AND attribute.attnum = key.attnum
        ORDER BY key.position
      ) AS columns,
      regexp_replace(
        lower(COALESCE(pg_get_expr(indexes.indpred, indexes.indrelid), '')),
        '[()[:space:]]', '', 'g'
      ) AS predicate
    FROM pg_index AS indexes
    WHERE indexes.indexrelid = to_regclass('bunqueue_jobs_live_unique_key_idx')
  `;
  return state;
}

async function replaceWithNonUniqueIndex(sql: SQL): Promise<void> {
  await sql
    .unsafe(
      `DROP INDEX bunqueue_jobs_live_unique_key_idx;
       CREATE INDEX bunqueue_jobs_live_unique_key_idx
         ON bunqueue_jobs(namespace, queue, unique_key)
         WHERE unique_key IS NOT NULL
           AND state IN ('waiting', 'prioritized', 'delayed', 'waiting-children', 'active');`
    )
    .simple();
}

test.skipIf(!postgresUrl)('repairs semantic drift in the live deduplication index', async () => {
  await withIsolatedDatabase(async (sql, ctx) => {
    await initializePostgresSchema(ctx);
    await replaceWithNonUniqueIndex(sql);

    await initializePostgresSchema(ctx);

    expect(await liveDedupIndexState(sql)).toEqual({
      columns: ['namespace', 'queue', 'unique_key'],
      key_columns: 3,
      predicate: LIVE_DEDUP_PREDICATE,
      total_columns: 3,
      unique: true,
    });
  });
});

test.skipIf(!postgresUrl)(
  'repairs a same-name unique index with weaker key and predicate semantics',
  async () => {
    await withIsolatedDatabase(async (sql, ctx) => {
      await initializePostgresSchema(ctx);
      await sql
        .unsafe(
          `DROP INDEX bunqueue_jobs_live_unique_key_idx;
           CREATE UNIQUE INDEX bunqueue_jobs_live_unique_key_idx
             ON bunqueue_jobs(namespace, queue, unique_key, state)
             WHERE unique_key IS NOT NULL;`
        )
        .simple();

      await initializePostgresSchema(ctx);

      expect(await liveDedupIndexState(sql)).toMatchObject({
        columns: ['namespace', 'queue', 'unique_key'],
        key_columns: 3,
        predicate: LIVE_DEDUP_PREDICATE,
        total_columns: 3,
        unique: true,
      });
    });
  }
);

test.skipIf(!postgresUrl)(
  'fails closed and preserves duplicate live keys across repair retries',
  async () => {
    await withIsolatedDatabase(async (sql, ctx) => {
      await initializePostgresSchema(ctx);
      await replaceWithNonUniqueIndex(sql);
      await sql`
        INSERT INTO bunqueue_jobs
          (namespace, id, queue, payload, state, run_at, created_at, unique_key)
        VALUES
          ('schema-dedup-guard', 'first', 'queue', ${new Uint8Array()}, 'waiting', 0, 0, 'key'),
          ('schema-dedup-guard', 'second', 'queue', ${new Uint8Array()}, 'waiting', 0, 0, 'key')
      `;

      await expect(initializePostgresSchema(ctx)).rejects.toThrow();
      await expect(initializePostgresSchema(ctx)).rejects.toThrow();
      const [rows] = await sql<{ count: number | string | bigint }[]>`
        SELECT COUNT(*) AS count
        FROM bunqueue_jobs
        WHERE namespace = 'schema-dedup-guard'
      `;
      expect(Number(rows.count)).toBe(2);
      expect(await liveDedupIndexState(sql)).toMatchObject({ unique: false });
    });
  }
);
