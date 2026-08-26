import type { TransactionSQL } from 'bun';
import { databaseNow, type PostgresContext } from './context';
import {
  POSTGRES_ASSIGN_EVENT_COMMIT_BODY,
  POSTGRES_REGISTER_EVENT_ROWS_BODY,
  POSTGRES_REGISTER_WATERMARK_ROWS_BODY,
} from './eventJournalSchema';
import { POSTGRES_SCHEMA, POSTGRES_SCHEMA_VERSION } from './schema';

interface FunctionState {
  name: string;
  body: string | null;
  canonical: boolean;
}

const EXPECTED_FUNCTION_BODIES = new Map([
  ['bunqueue_register_event_rows', POSTGRES_REGISTER_EVENT_ROWS_BODY],
  ['bunqueue_register_watermark_rows', POSTGRES_REGISTER_WATERMARK_ROWS_BODY],
  ['bunqueue_assign_event_commit', POSTGRES_ASSIGN_EVENT_COMMIT_BODY],
]);

function normalizeFunctionBody(value: string): string {
  return value.replaceAll(/\s+/g, ' ').trim();
}

async function hasCurrentSchema(tx: TransactionSQL): Promise<boolean> {
  const [table] = await tx<{ exists: boolean }[]>`
    SELECT to_regclass('bunqueue_schema_migrations') IS NOT NULL AS exists
  `;
  if (!table.exists) return false;

  const [migration] = await tx<Array<{ current: boolean; latest: number | string | null }>>`
    SELECT
      EXISTS (
        SELECT 1 FROM bunqueue_schema_migrations
        WHERE version = ${POSTGRES_SCHEMA_VERSION}
      ) AS current,
      MAX(version) AS latest
    FROM bunqueue_schema_migrations
  `;
  const latest = Number(migration.latest ?? 0);
  if (latest > POSTGRES_SCHEMA_VERSION) {
    throw new Error(
      `PostgreSQL schema version ${latest} is newer than supported version ${POSTGRES_SCHEMA_VERSION}`
    );
  }
  if (!migration.current) return false;

  const [objects] = await tx<{ valid: boolean }[]>`
    WITH expected_columns(table_name, column_name, type_name, not_null, default_kind) AS (
      VALUES
        ('bunqueue_events', 'transaction_id', 'bigint', TRUE, 'transaction'),
        ('bunqueue_event_commits', 'namespace', 'text', TRUE, 'none'),
        ('bunqueue_event_commits', 'transaction_id', 'bigint', TRUE, 'none'),
        ('bunqueue_event_commits', 'commit_seq', 'bigint', FALSE, 'none'),
        ('bunqueue_event_prune_watermarks', 'transaction_id', 'bigint', TRUE, 'transaction'),
        ('bunqueue_event_prune_watermarks', 'commit_seq', 'bigint', FALSE, 'none'),
        ('bunqueue_event_prune_watermarks', 'pruned_commit_seq', 'bigint', TRUE, 'zero'),
        ('bunqueue_event_prune_watermarks', 'prunes_current_transaction', 'boolean', TRUE, 'false')
    ), expected_indexes(index_name, table_name, columns, descending, predicate) AS (
      VALUES
        ('bunqueue_events_transaction_idx', 'bunqueue_events',
         ARRAY['namespace', 'transaction_id', 'id'], ARRAY[FALSE, FALSE, FALSE], ''),
        ('bunqueue_event_commits_replay_idx', 'bunqueue_event_commits',
         ARRAY['namespace', 'commit_seq', 'transaction_id'], ARRAY[FALSE, FALSE, FALSE],
         'commit_seqisnotnull'),
        ('bunqueue_event_prune_watermarks_commit_idx', 'bunqueue_event_prune_watermarks',
         ARRAY['namespace', 'queue', 'commit_seq'], ARRAY[FALSE, FALSE, TRUE],
         'commit_seqisnotnull'),
        ('bunqueue_event_prune_watermarks_pending_commit_idx',
         'bunqueue_event_prune_watermarks', ARRAY['namespace', 'transaction_id'],
         ARRAY[FALSE, FALSE], 'commit_seqisnull'),
        ('bunqueue_event_prune_watermarks_transaction_idx',
         'bunqueue_event_prune_watermarks', ARRAY['namespace', 'transaction_id'],
         ARRAY[FALSE, FALSE], '')
    ), expected_triggers(
      trigger_name, table_name, function_name, trigger_type, new_table,
      is_constraint, is_deferrable
    ) AS (
      VALUES
        ('bunqueue_events_register_commit', 'bunqueue_events',
         'bunqueue_register_event_rows', 4, 'new_event_rows', FALSE, FALSE),
        ('bunqueue_watermarks_insert_register_commit', 'bunqueue_event_prune_watermarks',
         'bunqueue_register_watermark_rows', 4, 'new_watermark_rows', FALSE, FALSE),
        ('bunqueue_watermarks_update_register_commit', 'bunqueue_event_prune_watermarks',
         'bunqueue_register_watermark_rows', 16, 'new_watermark_rows', FALSE, FALSE),
        ('bunqueue_assign_event_commit', 'bunqueue_event_commits',
         'bunqueue_assign_event_commit', 5, NULL, TRUE, TRUE)
    )
    SELECT
      EXISTS (
        SELECT 1 FROM pg_sequence
        WHERE seqrelid = to_regclass('bunqueue_event_commit_seq')
          AND seqtypid = to_regtype('bigint')
          AND seqincrement = 1 AND seqmin = 1
          AND seqmax = 9223372036854775807
          AND NOT seqcycle
          AND seqcache = 1
      )
      AND to_regclass('bunqueue_event_commits') IS NOT NULL
      AND (
        SELECT COALESCE(bool_and(COALESCE(
          attribute.atttypid = to_regtype(expected.type_name)
          AND attribute.attnotnull = expected.not_null
          AND CASE expected.default_kind
            WHEN 'transaction' THEN regexp_replace(
              lower(COALESCE(pg_get_expr(defaults.adbin, defaults.adrelid), '')),
              '[()[:space:]]', '', 'g'
            ) = 'pg_current_xact_id::text::bigint'
            WHEN 'zero' THEN regexp_replace(
              lower(COALESCE(pg_get_expr(defaults.adbin, defaults.adrelid), '')),
              '[()[:space:]]', '', 'g'
            ) IN ('0', '0::bigint')
            WHEN 'false' THEN regexp_replace(
              lower(COALESCE(pg_get_expr(defaults.adbin, defaults.adrelid), '')),
              '[()[:space:]]', '', 'g'
            ) IN ('false', 'false::boolean')
            ELSE defaults.oid IS NULL
          END,
          FALSE
        )), FALSE)
        FROM expected_columns AS expected
        LEFT JOIN pg_attribute AS attribute
          ON attribute.attrelid = to_regclass(expected.table_name)
         AND attribute.attname = expected.column_name AND NOT attribute.attisdropped
        LEFT JOIN pg_attrdef AS defaults
          ON defaults.adrelid = attribute.attrelid AND defaults.adnum = attribute.attnum
      )
      AND (
        SELECT COALESCE(bool_and(COALESCE(
          indexes.indrelid = to_regclass(expected.table_name)
          AND indexes.indisvalid AND indexes.indisready
          AND NOT indexes.indisunique AND NOT indexes.indisexclusion
          AND access_method.amname = 'btree'
          AND ARRAY(
            SELECT attribute.attname::text
            FROM unnest(indexes.indkey::smallint[]) WITH ORDINALITY AS key(attnum, position)
            JOIN pg_attribute AS attribute
              ON attribute.attrelid = indexes.indrelid AND attribute.attnum = key.attnum
            ORDER BY key.position
          ) = expected.columns
          AND ARRAY(
            SELECT (option & 1) = 1
            FROM unnest(indexes.indoption::smallint[]) WITH ORDINALITY AS ordering(option, position)
            ORDER BY ordering.position
          ) = expected.descending
          AND regexp_replace(
            lower(COALESCE(pg_get_expr(indexes.indpred, indexes.indrelid), '')),
            '[()[:space:]]', '', 'g'
          ) = expected.predicate,
          FALSE
        )), FALSE)
        FROM expected_indexes AS expected
        LEFT JOIN pg_class AS index_class ON index_class.oid = to_regclass(expected.index_name)
        LEFT JOIN pg_index AS indexes ON indexes.indexrelid = index_class.oid
        LEFT JOIN pg_am AS access_method ON access_method.oid = index_class.relam
      )
      AND (
        SELECT COALESCE(bool_and(EXISTS (
          SELECT 1 FROM pg_trigger AS trigger
          WHERE trigger.tgrelid = to_regclass(expected.table_name)
            AND trigger.tgname = expected.trigger_name
            AND trigger.tgfoid = to_regprocedure(expected.function_name || '()')
            AND NOT trigger.tgisinternal AND trigger.tgenabled = 'O'
            AND trigger.tgtype = expected.trigger_type
            AND trigger.tgnewtable IS NOT DISTINCT FROM expected.new_table
            AND trigger.tgoldtable IS NULL
            AND trigger.tgqual IS NULL AND trigger.tgnargs = 0
            AND (trigger.tgconstraint <> 0) = expected.is_constraint
            AND trigger.tgdeferrable = expected.is_deferrable
            AND trigger.tginitdeferred = expected.is_deferrable
        )), FALSE)
        FROM expected_triggers AS expected
      ) AS valid
  `;
  if (!objects.valid) return false;

  const functions = await tx<FunctionState[]>`
    SELECT expected.name, functions.prosrc AS body,
           functions.prorettype = to_regtype('trigger')
             AND language.lanname = 'plpgsql'
             AND functions.prokind = 'f'
             AND functions.provolatile = 'v'
             AND functions.proparallel = 'u'
             AND NOT functions.prosecdef
             AND NOT functions.proisstrict
             AND functions.proconfig IS NULL
             AND NOT functions.proleakproof AS canonical
    FROM unnest(${tx.array([...EXPECTED_FUNCTION_BODIES.keys()], 'TEXT')}) AS expected(name)
    LEFT JOIN pg_proc AS functions
      ON functions.oid = to_regprocedure(expected.name || '()')
    LEFT JOIN pg_language AS language ON language.oid = functions.prolang
  `;
  return (
    functions.length === EXPECTED_FUNCTION_BODIES.size &&
    functions.every(({ name, body, canonical }) => {
      const expected = EXPECTED_FUNCTION_BODIES.get(name);
      return canonical && body !== null && expected !== undefined
        ? normalizeFunctionBody(body) === normalizeFunctionBody(expected)
        : false;
    })
  );
}

/** Initialize or migrate the shared schema without locking live tables when current. */
export async function initializePostgresSchema(ctx: PostgresContext): Promise<void> {
  await ctx.sql.begin(async (tx) => {
    await tx`SELECT pg_advisory_xact_lock(hashtext('bunqueue:schema'))`;
    if (await hasCurrentSchema(tx)) return;

    await tx.unsafe(POSTGRES_SCHEMA).simple();
    const appliedAt = await databaseNow(tx);
    await tx`
      INSERT INTO bunqueue_schema_migrations (version, applied_at)
      VALUES (${POSTGRES_SCHEMA_VERSION}, ${appliedAt})
      ON CONFLICT (version) DO NOTHING
    `;
  });
}
