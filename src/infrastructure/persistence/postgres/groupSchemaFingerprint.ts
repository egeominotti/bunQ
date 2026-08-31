import type { TransactionSQL } from 'bun';

/** Verify every catalog property required by durable group scheduling. */
export async function hasCurrentPostgresGroupSchema(tx: TransactionSQL): Promise<boolean> {
  const [schema] = await tx<{ valid: boolean }[]>`
    WITH expected_columns(table_name, column_name, type_name, not_null, default_kind) AS (
      VALUES
        ('bunqueue_jobs', 'group_order', 'bigint', FALSE, 'none'),
        ('bunqueue_group_state', 'namespace', 'text', TRUE, 'none'),
        ('bunqueue_group_state', 'queue', 'text', TRUE, 'none'),
        ('bunqueue_group_state', 'group_id', 'text', TRUE, 'none'),
        ('bunqueue_group_state', 'rate_limit', 'bigint', FALSE, 'none'),
        ('bunqueue_group_state', 'rate_duration_ms', 'bigint', FALSE, 'none'),
        ('bunqueue_group_state', 'rate_window_started_at', 'bigint', FALSE, 'none'),
        ('bunqueue_group_state', 'rate_count', 'bigint', TRUE, 'zero'),
        ('bunqueue_group_state', 'rate_effective_max', 'bigint', FALSE, 'none'),
        ('bunqueue_group_state', 'rate_effective_duration_ms', 'bigint', FALSE, 'none'),
        ('bunqueue_group_state', 'concurrency_limit', 'bigint', FALSE, 'none'),
        ('bunqueue_group_state', 'last_served', 'bigint', FALSE, 'none')
    ), expected_indexes(
      index_name, table_name, columns, descending, predicate
    ) AS (
      VALUES
        ('bunqueue_jobs_group_ready_idx', 'bunqueue_jobs',
         ARRAY['namespace', 'queue', 'group_id', 'run_at', 'group_order', 'id'],
         ARRAY[FALSE, FALSE, FALSE, FALSE, FALSE, FALSE],
         'group_idisnotnullandstate=anyarray[''waiting''::text,''prioritized''::text,' ||
         '''delayed''::text]'),
        ('bunqueue_group_state_rotation_idx', 'bunqueue_group_state',
         ARRAY['namespace', 'queue', 'last_served', 'group_id'],
         ARRAY[FALSE, FALSE, FALSE, FALSE], '')
    )
    SELECT
      to_regclass('bunqueue_group_state') IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM pg_sequence
        WHERE seqrelid = to_regclass('bunqueue_group_order_seq')
          AND seqtypid = to_regtype('bigint')
          AND seqincrement = 1 AND seqmin = 1
          AND seqmax = 9223372036854775807
          AND NOT seqcycle AND seqcache = 1
      )
      AND (
        SELECT COALESCE(bool_and(COALESCE(
          attribute.atttypid = to_regtype(expected.type_name)
          AND attribute.attnotnull = expected.not_null
          AND CASE expected.default_kind
            WHEN 'zero' THEN regexp_replace(
              lower(COALESCE(pg_get_expr(defaults.adbin, defaults.adrelid), '')),
              '[()[:space:]]', '', 'g'
            ) IN ('0', '0::bigint')
            ELSE defaults.oid IS NULL
          END,
          FALSE
        )), FALSE)
        FROM expected_columns AS expected
        LEFT JOIN pg_attribute AS attribute
          ON attribute.attrelid = to_regclass(expected.table_name)
         AND attribute.attname = expected.column_name
         AND NOT attribute.attisdropped
        LEFT JOIN pg_attrdef AS defaults
          ON defaults.adrelid = attribute.attrelid
         AND defaults.adnum = attribute.attnum
      )
      AND (
        SELECT COALESCE(bool_and(COALESCE(
          indexes.indrelid = to_regclass(expected.table_name)
          AND indexes.indisvalid AND indexes.indisready
          AND NOT indexes.indisunique AND NOT indexes.indisexclusion
          AND indexes.indnkeyatts = cardinality(expected.columns)
          AND indexes.indnatts = indexes.indnkeyatts
          AND indexes.indexprs IS NULL
          AND access_method.amname = 'btree'
          AND ARRAY(
            SELECT attribute.attname::text
            FROM unnest(indexes.indkey::smallint[]) WITH ORDINALITY AS key(attnum, position)
            JOIN pg_attribute AS attribute
              ON attribute.attrelid = indexes.indrelid
             AND attribute.attnum = key.attnum
            ORDER BY key.position
          ) = expected.columns
          AND ARRAY(
            SELECT (option & 1) = 1
            FROM unnest(indexes.indoption::smallint[])
              WITH ORDINALITY AS ordering(option, position)
            ORDER BY ordering.position
          ) = expected.descending
          AND regexp_replace(
            lower(COALESCE(pg_get_expr(indexes.indpred, indexes.indrelid), '')),
            '[()[:space:]]', '', 'g'
          ) = expected.predicate,
          FALSE
        )), FALSE)
        FROM expected_indexes AS expected
        LEFT JOIN pg_class AS index_class
          ON index_class.oid = to_regclass(expected.index_name)
        LEFT JOIN pg_index AS indexes ON indexes.indexrelid = index_class.oid
        LEFT JOIN pg_am AS access_method ON access_method.oid = index_class.relam
      )
      AND EXISTS (
        SELECT 1
        FROM pg_constraint AS constraint_state
        JOIN pg_index AS index_state ON index_state.indexrelid = constraint_state.conindid
        JOIN pg_class AS index_class ON index_class.oid = index_state.indexrelid
        JOIN pg_am AS access_method ON access_method.oid = index_class.relam
        WHERE constraint_state.conrelid = to_regclass('bunqueue_group_state')
          AND constraint_state.contype = 'p'
          AND constraint_state.convalidated
          AND NOT constraint_state.condeferrable
          AND NOT constraint_state.condeferred
          AND index_state.indisprimary AND index_state.indisunique
          AND index_state.indisvalid AND index_state.indisready
          AND index_state.indimmediate
          AND index_state.indnkeyatts = 3 AND index_state.indnatts = 3
          AND index_state.indexprs IS NULL AND index_state.indpred IS NULL
          AND access_method.amname = 'btree'
          AND ARRAY(
            SELECT attribute.attname::text
            FROM unnest(constraint_state.conkey)
              WITH ORDINALITY AS key(attnum, position)
            JOIN pg_attribute AS attribute
              ON attribute.attrelid = constraint_state.conrelid
             AND attribute.attnum = key.attnum
            ORDER BY key.position
          ) = ARRAY['namespace', 'queue', 'group_id']
      ) AS valid
  `;
  return schema.valid;
}
