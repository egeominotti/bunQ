import { describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import type { PostgresContext } from '../src/infrastructure/persistence/postgres/context';
import { POSTGRES_SCHEMA_VERSION } from '../src/infrastructure/persistence/postgres/schema';
import { initializePostgresSchema } from '../src/infrastructure/persistence/postgres/schemaInitialization';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;

function context(sql: SQL, url: string): PostgresContext {
  return {
    sql,
    config: {
      url,
      namespace: 'schema-guard',
      brokerId: 'schema-guard',
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

async function withIsolatedDatabase(run: (sql: SQL, url: string) => Promise<void>) {
  const admin = new SQL(postgresUrl!, { max: 1 });
  const name = `bq_schema_${Date.now()}_${crypto.randomUUID().replaceAll('-', '').slice(0, 12)}`;
  const url = new URL(postgresUrl!);
  url.pathname = `/${name}`;
  let created = false;
  try {
    await admin.unsafe(`CREATE DATABASE "${name}"`).simple();
    created = true;
    const sql = new SQL(url.toString(), { max: 2 });
    try {
      await run(sql, url.toString());
    } finally {
      await sql.close({ timeout: 5 });
    }
  } finally {
    if (created) await admin.unsafe(`DROP DATABASE "${name}" WITH (FORCE)`).simple();
    await admin.close({ timeout: 5 });
  }
}

describe('PostgreSQL schema guard', () => {
  test.skipIf(!postgresUrl)('rejects a database created by a newer engine version', async () => {
    await withIsolatedDatabase(async (sql, url) => {
      const ctx = context(sql, url);
      await initializePostgresSchema(ctx);
      const newer = POSTGRES_SCHEMA_VERSION + 1;
      await sql`
        INSERT INTO bunqueue_schema_migrations (version, applied_at)
        VALUES (${newer}, ${Date.now()})
      `;

      await expect(initializePostgresSchema(ctx)).rejects.toThrow(
        `PostgreSQL schema version ${newer} is newer than supported version ${POSTGRES_SCHEMA_VERSION}`
      );
    });
  });

  test.skipIf(!postgresUrl)('repairs drift in correctness-critical journal objects', async () => {
    await withIsolatedDatabase(async (sql, url) => {
      const ctx = context(sql, url);
      await initializePostgresSchema(ctx);
      await sql.unsafe('DROP TRIGGER bunqueue_events_register_commit ON bunqueue_events').simple();

      await initializePostgresSchema(ctx);
      const triggers = await sql<{ tgname: string }[]>`
        SELECT tgname FROM pg_trigger
        WHERE NOT tgisinternal AND tgenabled <> 'D'
          AND tgname IN (
            'bunqueue_events_register_commit',
            'bunqueue_watermarks_insert_register_commit',
            'bunqueue_watermarks_update_register_commit',
            'bunqueue_assign_event_commit'
          )
        ORDER BY tgname
      `;
      expect(triggers.map(({ tgname }) => tgname)).toEqual([
        'bunqueue_assign_event_commit',
        'bunqueue_events_register_commit',
        'bunqueue_watermarks_insert_register_commit',
        'bunqueue_watermarks_update_register_commit',
      ]);

      const [inserted] = await sql<{ id: number | string | bigint }[]>`
        INSERT INTO bunqueue_events
          (namespace, queue, event_type, job_id, occurred_at, payload)
        VALUES ('schema-guard', 'queue', 'pushed', 'job', ${Date.now()}, NULL)
        RETURNING id
      `;
      await sql.unsafe('ALTER SEQUENCE bunqueue_event_commit_seq CACHE 8').simple();
      await initializePostgresSchema(ctx);
      const [sequence] = await sql<{ seqcache: number | string | bigint }[]>`
        SELECT seqcache FROM pg_sequence
        WHERE seqrelid = to_regclass('bunqueue_event_commit_seq')
      `;
      expect(Number(sequence.seqcache)).toBe(1);

      const [committed] = await sql<{ commit_seq: number | string | bigint | null }[]>`
        SELECT journal.commit_seq
        FROM bunqueue_events AS event
        JOIN bunqueue_event_commits AS journal
          ON journal.namespace = event.namespace
         AND journal.transaction_id = event.transaction_id
        WHERE event.id = ${inserted.id}
      `;
      expect(Number(committed.commit_seq)).toBeGreaterThan(0);
    });
  });

  test.skipIf(!postgresUrl)('keeps the current schema on the catalog-only fast path', async () => {
    await withIsolatedDatabase(async (sql, url) => {
      const ctx = context(sql, url);
      await initializePostgresSchema(ctx);
      const objectIds = async () => {
        const [ids] = await sql<
          Array<{
            event_index: number | string | bigint;
            group_ready_index: number | string | bigint;
            group_rotation_index: number | string | bigint;
            replay_index: number | string | bigint;
            watermark_index: number | string | bigint;
          }>
        >`
          SELECT
            to_regclass('bunqueue_events_transaction_idx')::oid AS event_index,
            to_regclass('bunqueue_jobs_group_ready_idx')::oid AS group_ready_index,
            to_regclass('bunqueue_group_state_rotation_idx')::oid AS group_rotation_index,
            to_regclass('bunqueue_event_commits_replay_idx')::oid AS replay_index,
            to_regclass('bunqueue_event_prune_watermarks_commit_idx')::oid
              AS watermark_index
        `;
        return Object.values(ids).map(Number);
      };
      const before = await objectIds();

      await initializePostgresSchema(ctx);

      expect(await objectIds()).toEqual(before);
    });
  });

  test.skipIf(!postgresUrl)('repairs complete group-state table drift', async () => {
    await withIsolatedDatabase(async (sql, url) => {
      const ctx = context(sql, url);
      await initializePostgresSchema(ctx);
      await sql
        .unsafe(
          `ALTER TABLE bunqueue_group_state
             DROP CONSTRAINT bunqueue_group_state_pkey;
           ALTER TABLE bunqueue_group_state
             DROP COLUMN rate_duration_ms;
           ALTER TABLE bunqueue_group_state
             ALTER COLUMN rate_window_started_at TYPE TEXT
             USING rate_window_started_at::text;
           ALTER TABLE bunqueue_jobs
             ALTER COLUMN group_order TYPE TEXT USING group_order::text;
           ALTER SEQUENCE bunqueue_group_order_seq CACHE 8;
           DROP INDEX bunqueue_jobs_group_ready_idx;
           CREATE INDEX bunqueue_jobs_group_ready_idx
             ON bunqueue_jobs(namespace, queue, id);
           DROP INDEX bunqueue_group_state_rotation_idx;
           CREATE UNIQUE INDEX bunqueue_group_state_rotation_idx
             ON bunqueue_group_state(namespace, queue, last_served DESC, group_id);`
        )
        .simple();

      await initializePostgresSchema(ctx);
      const [state] = await sql<
        Array<{
          duration_type: string | null;
          group_order_type: string | null;
          group_ready_definition: string | null;
          primary_columns: string[] | null;
          rotation_definition: string | null;
          sequence_cache: number | string | bigint | null;
          window_type: string | null;
        }>
      >`
        SELECT
          (
            SELECT format_type(attribute.atttypid, attribute.atttypmod)
            FROM pg_attribute AS attribute
            WHERE attribute.attrelid = to_regclass('bunqueue_group_state')
              AND attribute.attname = 'rate_duration_ms'
              AND NOT attribute.attisdropped
          ) AS duration_type,
          (
            SELECT format_type(attribute.atttypid, attribute.atttypmod)
            FROM pg_attribute AS attribute
            WHERE attribute.attrelid = to_regclass('bunqueue_jobs')
              AND attribute.attname = 'group_order'
              AND NOT attribute.attisdropped
          ) AS group_order_type,
          pg_get_indexdef(to_regclass('bunqueue_jobs_group_ready_idx'))
            AS group_ready_definition,
          (
            SELECT ARRAY_AGG(attribute.attname::text ORDER BY key.position)
            FROM pg_constraint AS constraint_state
            CROSS JOIN LATERAL
              unnest(constraint_state.conkey) WITH ORDINALITY AS key(attnum, position)
            JOIN pg_attribute AS attribute
              ON attribute.attrelid = constraint_state.conrelid
             AND attribute.attnum = key.attnum
            WHERE constraint_state.conrelid = to_regclass('bunqueue_group_state')
              AND constraint_state.contype = 'p'
          ) AS primary_columns,
          pg_get_indexdef(to_regclass('bunqueue_group_state_rotation_idx'))
            AS rotation_definition,
          (
            SELECT seqcache FROM pg_sequence
            WHERE seqrelid = to_regclass('bunqueue_group_order_seq')
          ) AS sequence_cache,
          (
            SELECT format_type(attribute.atttypid, attribute.atttypmod)
            FROM pg_attribute AS attribute
            WHERE attribute.attrelid = to_regclass('bunqueue_group_state')
              AND attribute.attname = 'rate_window_started_at'
              AND NOT attribute.attisdropped
          ) AS window_type
      `;
      expect(state).toEqual({
        duration_type: 'bigint',
        group_order_type: 'bigint',
        group_ready_definition: state.group_ready_definition,
        primary_columns: ['namespace', 'queue', 'group_id'],
        rotation_definition: state.rotation_definition,
        sequence_cache: state.sequence_cache,
        window_type: 'bigint',
      });
      expect(state.group_ready_definition).toContain(
        '(namespace, queue, group_id, run_at, group_order, id)'
      );
      expect(state.rotation_definition).toContain('(namespace, queue, last_served, group_id)');
      expect(state.rotation_definition).not.toContain('UNIQUE');
      expect(state.rotation_definition).not.toContain('DESC');
      expect(Number(state.sequence_cache)).toBe(1);
    });
  });

  test.skipIf(!postgresUrl)(
    'repairs semantic drift hidden behind correctness-critical object names',
    async () => {
      await withIsolatedDatabase(async (sql, url) => {
        const ctx = context(sql, url);
        await initializePostgresSchema(ctx);
        await sql
          .unsafe(
            `DROP INDEX bunqueue_events_transaction_idx;
             DROP TRIGGER bunqueue_events_register_commit ON bunqueue_events;
             CREATE TRIGGER bunqueue_events_register_commit
             AFTER INSERT ON bunqueue_events
             REFERENCING NEW TABLE AS new_event_rows
             FOR EACH STATEMENT EXECUTE FUNCTION bunqueue_register_watermark_rows();
             ALTER TABLE bunqueue_events ALTER COLUMN transaction_id DROP DEFAULT;
             ALTER TABLE bunqueue_events ALTER COLUMN transaction_id TYPE TEXT
               USING transaction_id::text;
             CREATE INDEX bunqueue_events_transaction_idx
               ON bunqueue_events(namespace, id, transaction_id);
             CREATE OR REPLACE FUNCTION bunqueue_assign_event_commit()
             RETURNS TRIGGER LANGUAGE plpgsql AS $function$
             BEGIN
               RETURN NULL;
             END;
             $function$;`
          )
          .simple();

        await initializePostgresSchema(ctx);
        const [state] = await sql<
          Array<{
            index_definition: string;
            default_definition: string | null;
            function_body: string;
            function_name: string;
            type_name: string;
          }>
        >`
          SELECT
            pg_get_indexdef(to_regclass('bunqueue_events_transaction_idx')) AS index_definition,
            pg_get_expr(defaults.adbin, defaults.adrelid) AS default_definition,
            functions.prosrc AS function_body,
            trigger_function.proname AS function_name,
            format_type(attribute.atttypid, attribute.atttypmod) AS type_name
          FROM pg_attribute AS attribute
          LEFT JOIN pg_attrdef AS defaults
            ON defaults.adrelid = attribute.attrelid AND defaults.adnum = attribute.attnum
          CROSS JOIN pg_proc AS functions
          CROSS JOIN pg_trigger AS trigger
          CROSS JOIN pg_proc AS trigger_function
          WHERE attribute.attrelid = to_regclass('bunqueue_events')
            AND attribute.attname = 'transaction_id'
            AND functions.oid = to_regprocedure('bunqueue_assign_event_commit()')
            AND trigger.tgrelid = to_regclass('bunqueue_events')
            AND trigger.tgname = 'bunqueue_events_register_commit'
            AND trigger_function.oid = trigger.tgfoid
        `;
        expect(state.index_definition).toContain('(namespace, transaction_id, id)');
        expect(state.default_definition).toContain('pg_current_xact_id');
        expect(state.function_body).toContain("nextval('bunqueue_event_commit_seq')");
        expect(state.function_body).toContain('pg_advisory_xact_lock');
        expect(state.function_name).toBe('bunqueue_register_event_rows');
        expect(state.type_name).toBe('bigint');
      });
    }
  );

  test.skipIf(!postgresUrl)('migrates version 13 with bounded completion indexes', async () => {
    await withIsolatedDatabase(async (sql, url) => {
      const ctx = context(sql, url);
      await initializePostgresSchema(ctx);
      await sql
        .unsafe(
          `DELETE FROM bunqueue_schema_migrations WHERE version > 13;
           INSERT INTO bunqueue_schema_migrations (version, applied_at) VALUES (13, 0)
             ON CONFLICT (version) DO NOTHING;
           DROP INDEX bunqueue_completions_queue_idx;
           DROP INDEX bunqueue_completions_recent_idx;`
        )
        .simple();

      await initializePostgresSchema(ctx);
      const [state] = await sql<
        { current: boolean; queue_index: boolean; recent_index: boolean }[]
      >`
        SELECT
          EXISTS (
            SELECT 1 FROM bunqueue_schema_migrations
            WHERE version = ${POSTGRES_SCHEMA_VERSION}
          ) AS current,
          to_regclass('bunqueue_completions_queue_idx') IS NOT NULL AS queue_index,
          to_regclass('bunqueue_completions_recent_idx') IS NOT NULL AS recent_index
      `;
      expect(state).toEqual({ current: true, queue_index: true, recent_index: true });
    });
  });

  test.skipIf(!postgresUrl)('migrates a version-12 journal in place', async () => {
    await withIsolatedDatabase(async (sql, url) => {
      const ctx = context(sql, url);
      await initializePostgresSchema(ctx);
      await sql
        .unsafe(
          `DELETE FROM bunqueue_schema_migrations WHERE version > 12;
           INSERT INTO bunqueue_schema_migrations (version, applied_at) VALUES (12, 0)
             ON CONFLICT (version) DO NOTHING;
           DROP TRIGGER bunqueue_events_register_commit ON bunqueue_events;
           DROP TRIGGER bunqueue_watermarks_insert_register_commit
             ON bunqueue_event_prune_watermarks;
           DROP TRIGGER bunqueue_watermarks_update_register_commit
             ON bunqueue_event_prune_watermarks;
           DROP TABLE bunqueue_event_commits;
           DROP SEQUENCE bunqueue_event_commit_seq;
           ALTER TABLE bunqueue_events
             DROP COLUMN transaction_id;
           ALTER TABLE bunqueue_event_prune_watermarks
             DROP COLUMN transaction_id, DROP COLUMN commit_seq,
             DROP COLUMN pruned_commit_seq, DROP COLUMN prunes_current_transaction;
           INSERT INTO bunqueue_events
             (namespace, queue, event_type, job_id, occurred_at, payload)
           VALUES ('schema-guard', 'legacy', 'pushed', 'legacy-job', 1, NULL);`
        )
        .simple();

      await initializePostgresSchema(ctx);
      const [state] = await sql<{ current: boolean; journal: boolean; migrated: boolean }[]>`
        SELECT
          EXISTS (
            SELECT 1 FROM bunqueue_schema_migrations
            WHERE version = ${POSTGRES_SCHEMA_VERSION}
          ) AS current,
          EXISTS (
            SELECT 1 FROM pg_sequence
            WHERE seqrelid = to_regclass('bunqueue_event_commit_seq')
              AND seqcache = 1
          ) AND to_regclass('bunqueue_event_commits') IS NOT NULL AS journal,
          EXISTS (
            SELECT 1 FROM bunqueue_events AS event
            JOIN bunqueue_event_commits AS journal
              ON journal.namespace = event.namespace
             AND journal.transaction_id = event.transaction_id
            WHERE event.job_id = 'legacy-job' AND journal.commit_seq IS NOT NULL
          ) AS migrated
      `;
      expect(state).toEqual({ current: true, journal: true, migrated: true });
    });
  });
});
