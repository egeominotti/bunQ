export const POSTGRES_TRACK_EVENT_INSERTIONS_BODY = `BEGIN
  INSERT INTO bunqueue_event_retention_deltas
    (namespace, queue, transaction_id, delta_count, last_event_id)
  SELECT
    namespace,
    queue,
    pg_current_xact_id()::text::bigint,
    COUNT(*)::BIGINT,
    MAX(id)
  FROM new_event_rows
  GROUP BY namespace, queue
  ORDER BY namespace, queue
  ON CONFLICT (namespace, queue, transaction_id) DO UPDATE SET
    delta_count = bunqueue_event_retention_deltas.delta_count
      + excluded.delta_count,
    last_event_id = GREATEST(
      bunqueue_event_retention_deltas.last_event_id,
      excluded.last_event_id
    );
  RETURN NULL;
END;`;

export const POSTGRES_TRACK_EVENT_DELETIONS_BODY = `BEGIN
  INSERT INTO bunqueue_event_retention_deltas
    (namespace, queue, transaction_id, delta_count, last_event_id)
  SELECT
    namespace,
    queue,
    pg_current_xact_id()::text::bigint,
    -COUNT(*)::BIGINT,
    MAX(id)
  FROM old_event_rows
  GROUP BY namespace, queue
  ORDER BY namespace, queue
  ON CONFLICT (namespace, queue, transaction_id) DO UPDATE SET
    delta_count = bunqueue_event_retention_deltas.delta_count
      + excluded.delta_count,
    last_event_id = GREATEST(
      bunqueue_event_retention_deltas.last_event_id,
      excluded.last_event_id
    );
  RETURN NULL;
END;`;

/** Exact retained-event counters maintained for every PostgreSQL event mutation. */
export const POSTGRES_EVENT_RETENTION_SCHEMA = `
CREATE TABLE IF NOT EXISTS bunqueue_event_retention_state (
  namespace TEXT NOT NULL,
  queue TEXT NOT NULL,
  retained_count BIGINT NOT NULL DEFAULT 0,
  last_event_id BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (namespace, queue)
);

CREATE TABLE IF NOT EXISTS bunqueue_event_retention_deltas (
  namespace TEXT NOT NULL,
  queue TEXT NOT NULL,
  transaction_id BIGINT NOT NULL,
  delta_count BIGINT NOT NULL,
  last_event_id BIGINT NOT NULL,
  PRIMARY KEY (namespace, queue, transaction_id)
);

LOCK TABLE bunqueue_events IN ACCESS EXCLUSIVE MODE;
DROP TRIGGER IF EXISTS bunqueue_events_track_retention_insert ON bunqueue_events;
DROP TRIGGER IF EXISTS bunqueue_events_track_retention_delete ON bunqueue_events;

TRUNCATE bunqueue_event_retention_state, bunqueue_event_retention_deltas;

ALTER TABLE bunqueue_event_retention_state
  ADD COLUMN IF NOT EXISTS namespace TEXT,
  ADD COLUMN IF NOT EXISTS queue TEXT,
  ADD COLUMN IF NOT EXISTS retained_count BIGINT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_event_id BIGINT DEFAULT 0;
ALTER TABLE bunqueue_event_retention_state
  ALTER COLUMN namespace TYPE TEXT USING namespace::text,
  ALTER COLUMN namespace DROP DEFAULT,
  ALTER COLUMN namespace SET NOT NULL,
  ALTER COLUMN queue TYPE TEXT USING queue::text,
  ALTER COLUMN queue DROP DEFAULT,
  ALTER COLUMN queue SET NOT NULL,
  ALTER COLUMN retained_count TYPE BIGINT USING retained_count::bigint,
  ALTER COLUMN retained_count SET DEFAULT 0,
  ALTER COLUMN retained_count SET NOT NULL,
  ALTER COLUMN last_event_id TYPE BIGINT USING last_event_id::bigint,
  ALTER COLUMN last_event_id SET DEFAULT 0,
  ALTER COLUMN last_event_id SET NOT NULL;

ALTER TABLE bunqueue_event_retention_deltas
  ADD COLUMN IF NOT EXISTS namespace TEXT,
  ADD COLUMN IF NOT EXISTS queue TEXT,
  ADD COLUMN IF NOT EXISTS transaction_id BIGINT,
  ADD COLUMN IF NOT EXISTS delta_count BIGINT,
  ADD COLUMN IF NOT EXISTS last_event_id BIGINT;
ALTER TABLE bunqueue_event_retention_deltas
  ALTER COLUMN namespace TYPE TEXT USING namespace::text,
  ALTER COLUMN namespace DROP DEFAULT,
  ALTER COLUMN namespace SET NOT NULL,
  ALTER COLUMN queue TYPE TEXT USING queue::text,
  ALTER COLUMN queue DROP DEFAULT,
  ALTER COLUMN queue SET NOT NULL,
  ALTER COLUMN transaction_id TYPE BIGINT USING transaction_id::bigint,
  ALTER COLUMN transaction_id DROP DEFAULT,
  ALTER COLUMN transaction_id SET NOT NULL,
  ALTER COLUMN delta_count TYPE BIGINT USING delta_count::bigint,
  ALTER COLUMN delta_count DROP DEFAULT,
  ALTER COLUMN delta_count SET NOT NULL,
  ALTER COLUMN last_event_id TYPE BIGINT USING last_event_id::bigint,
  ALTER COLUMN last_event_id DROP DEFAULT,
  ALTER COLUMN last_event_id SET NOT NULL;

DO $event_retention_primary_keys$
DECLARE
  columns_sql TEXT;
  constraint_name TEXT;
  current_primary BOOLEAN;
  relation_kind "char";
  target RECORD;
BEGIN
  FOR target IN
    SELECT * FROM (VALUES
      ('bunqueue_event_retention_state', 'bunqueue_event_retention_state_pkey',
       ARRAY['namespace', 'queue']::TEXT[]),
      ('bunqueue_event_retention_deltas', 'bunqueue_event_retention_deltas_pkey',
       ARRAY['namespace', 'queue', 'transaction_id']::TEXT[])
    ) AS expected(table_name, key_name, key_columns)
  LOOP
    SELECT EXISTS (
      SELECT 1
      FROM pg_constraint AS constraint_state
      JOIN pg_index AS index_state ON index_state.indexrelid = constraint_state.conindid
      JOIN pg_class AS index_class ON index_class.oid = index_state.indexrelid
      JOIN pg_am AS access_method ON access_method.oid = index_class.relam
      WHERE constraint_state.conrelid = to_regclass(target.table_name)
        AND constraint_state.conname = target.key_name
        AND constraint_state.contype = 'p'
        AND constraint_state.convalidated
        AND NOT constraint_state.condeferrable
        AND NOT constraint_state.condeferred
        AND index_state.indisprimary AND index_state.indisunique
        AND index_state.indisvalid AND index_state.indisready
        AND index_state.indimmediate
        AND index_state.indnkeyatts = cardinality(target.key_columns)
        AND index_state.indnatts = index_state.indnkeyatts
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
        ) = target.key_columns
    ) INTO current_primary;
    IF current_primary THEN CONTINUE; END IF;

    FOR constraint_name IN
      SELECT conname FROM pg_constraint
      WHERE conrelid = to_regclass(target.table_name)
        AND (contype = 'p' OR conname = target.key_name)
    LOOP
      EXECUTE format(
        'ALTER TABLE %I DROP CONSTRAINT %I',
        target.table_name,
        constraint_name
      );
    END LOOP;

    IF to_regclass(target.key_name) IS NOT NULL THEN
      SELECT relkind INTO relation_kind
      FROM pg_class WHERE oid = to_regclass(target.key_name);
      IF relation_kind NOT IN ('i', 'I') THEN
        RAISE EXCEPTION USING
          ERRCODE = '55000',
          MESSAGE = format(
            'Cannot repair primary key %s: the name belongs to a non-index relation',
            target.key_name
          ),
          HINT = 'Rename or remove the conflicting relation, then restart bunqueue.';
      END IF;
      EXECUTE format('DROP INDEX %I', target.key_name);
    END IF;

    SELECT string_agg(format('%I', column_name), ', ' ORDER BY position)
    INTO columns_sql
    FROM unnest(target.key_columns) WITH ORDINALITY AS key(column_name, position);
    EXECUTE format(
      'ALTER TABLE %I ADD CONSTRAINT %I PRIMARY KEY (%s)',
      target.table_name,
      target.key_name,
      columns_sql
    );
  END LOOP;
END;
$event_retention_primary_keys$;

INSERT INTO bunqueue_event_retention_state
  (namespace, queue, retained_count, last_event_id)
SELECT namespace, queue, COUNT(*)::BIGINT, MAX(id)
FROM bunqueue_events
GROUP BY namespace, queue
ON CONFLICT (namespace, queue) DO UPDATE SET
  retained_count = excluded.retained_count,
  last_event_id = excluded.last_event_id;

CREATE OR REPLACE FUNCTION bunqueue_track_event_insertions()
RETURNS TRIGGER LANGUAGE plpgsql AS $function$
${POSTGRES_TRACK_EVENT_INSERTIONS_BODY}
$function$;

CREATE OR REPLACE FUNCTION bunqueue_track_event_deletions()
RETURNS TRIGGER LANGUAGE plpgsql AS $function$
${POSTGRES_TRACK_EVENT_DELETIONS_BODY}
$function$;

CREATE TRIGGER bunqueue_events_track_retention_insert
AFTER INSERT ON bunqueue_events
REFERENCING NEW TABLE AS new_event_rows
FOR EACH STATEMENT EXECUTE FUNCTION bunqueue_track_event_insertions();

CREATE TRIGGER bunqueue_events_track_retention_delete
AFTER DELETE ON bunqueue_events
REFERENCING OLD TABLE AS old_event_rows
FOR EACH STATEMENT EXECUTE FUNCTION bunqueue_track_event_deletions();
`;
