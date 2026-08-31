/** Correctness-critical group schema and idempotent v19 repair operations. */
export const POSTGRES_GROUP_SCHEMA = `
CREATE SEQUENCE IF NOT EXISTS bunqueue_group_order_seq AS BIGINT CACHE 1;
ALTER SEQUENCE bunqueue_group_order_seq
  AS BIGINT INCREMENT BY 1 NO MINVALUE NO MAXVALUE NO CYCLE CACHE 1;

ALTER TABLE bunqueue_jobs ADD COLUMN IF NOT EXISTS group_order BIGINT;
ALTER TABLE bunqueue_jobs
  ALTER COLUMN group_order TYPE BIGINT USING group_order::bigint,
  ALTER COLUMN group_order DROP DEFAULT,
  ALTER COLUMN group_order DROP NOT NULL;

DO $group_order_backfill$
DECLARE
  target RECORD;
BEGIN
  PERFORM setval(
    'bunqueue_group_order_seq',
    GREATEST(
      (SELECT last_value FROM bunqueue_group_order_seq),
      COALESCE((SELECT MAX(group_order) FROM bunqueue_jobs), 0),
      1
    ),
    TRUE
  );
  FOR target IN
    SELECT namespace, id
    FROM bunqueue_jobs
    WHERE group_id IS NOT NULL AND group_order IS NULL
    ORDER BY created_at, id, namespace
  LOOP
    UPDATE bunqueue_jobs
    SET group_order = nextval('bunqueue_group_order_seq')
    WHERE namespace = target.namespace AND id = target.id;
  END LOOP;
END;
$group_order_backfill$;

CREATE TABLE IF NOT EXISTS bunqueue_group_state (
  namespace TEXT NOT NULL,
  queue TEXT NOT NULL,
  group_id TEXT NOT NULL,
  rate_limit BIGINT,
  rate_duration_ms BIGINT,
  rate_window_started_at BIGINT,
  rate_count BIGINT NOT NULL DEFAULT 0,
  rate_effective_max BIGINT,
  rate_effective_duration_ms BIGINT,
  concurrency_limit BIGINT,
  last_served BIGINT,
  PRIMARY KEY (namespace, queue, group_id)
);
ALTER TABLE bunqueue_group_state
  ADD COLUMN IF NOT EXISTS namespace TEXT,
  ADD COLUMN IF NOT EXISTS queue TEXT,
  ADD COLUMN IF NOT EXISTS group_id TEXT,
  ADD COLUMN IF NOT EXISTS rate_limit BIGINT,
  ADD COLUMN IF NOT EXISTS rate_duration_ms BIGINT,
  ADD COLUMN IF NOT EXISTS rate_window_started_at BIGINT,
  ADD COLUMN IF NOT EXISTS rate_count BIGINT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS rate_effective_max BIGINT,
  ADD COLUMN IF NOT EXISTS rate_effective_duration_ms BIGINT,
  ADD COLUMN IF NOT EXISTS concurrency_limit BIGINT,
  ADD COLUMN IF NOT EXISTS last_served BIGINT;
ALTER TABLE bunqueue_group_state
  ALTER COLUMN namespace TYPE TEXT USING namespace::text,
  ALTER COLUMN queue TYPE TEXT USING queue::text,
  ALTER COLUMN group_id TYPE TEXT USING group_id::text,
  ALTER COLUMN rate_limit TYPE BIGINT USING rate_limit::bigint,
  ALTER COLUMN rate_duration_ms TYPE BIGINT USING rate_duration_ms::bigint,
  ALTER COLUMN rate_window_started_at TYPE BIGINT USING rate_window_started_at::bigint,
  ALTER COLUMN rate_count TYPE BIGINT USING rate_count::bigint,
  ALTER COLUMN rate_effective_max TYPE BIGINT USING rate_effective_max::bigint,
  ALTER COLUMN rate_effective_duration_ms TYPE BIGINT USING rate_effective_duration_ms::bigint,
  ALTER COLUMN concurrency_limit TYPE BIGINT USING concurrency_limit::bigint,
  ALTER COLUMN last_served TYPE BIGINT USING last_served::bigint;
UPDATE bunqueue_group_state SET rate_count = 0 WHERE rate_count IS NULL;
ALTER TABLE bunqueue_group_state
  ALTER COLUMN namespace DROP DEFAULT,
  ALTER COLUMN namespace SET NOT NULL,
  ALTER COLUMN queue DROP DEFAULT,
  ALTER COLUMN queue SET NOT NULL,
  ALTER COLUMN group_id DROP DEFAULT,
  ALTER COLUMN group_id SET NOT NULL,
  ALTER COLUMN rate_limit DROP DEFAULT,
  ALTER COLUMN rate_limit DROP NOT NULL,
  ALTER COLUMN rate_duration_ms DROP DEFAULT,
  ALTER COLUMN rate_duration_ms DROP NOT NULL,
  ALTER COLUMN rate_window_started_at DROP DEFAULT,
  ALTER COLUMN rate_window_started_at DROP NOT NULL,
  ALTER COLUMN rate_count SET DEFAULT 0,
  ALTER COLUMN rate_count SET NOT NULL,
  ALTER COLUMN rate_effective_max DROP DEFAULT,
  ALTER COLUMN rate_effective_max DROP NOT NULL,
  ALTER COLUMN rate_effective_duration_ms DROP DEFAULT,
  ALTER COLUMN rate_effective_duration_ms DROP NOT NULL,
  ALTER COLUMN concurrency_limit DROP DEFAULT,
  ALTER COLUMN concurrency_limit DROP NOT NULL,
  ALTER COLUMN last_served DROP DEFAULT,
  ALTER COLUMN last_served DROP NOT NULL;

DO $group_state_primary_key$
DECLARE
  constraint_name TEXT;
BEGIN
  IF NOT EXISTS (
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
      AND access_method.amname = 'btree'
      AND ARRAY(
        SELECT attribute.attname::text
        FROM unnest(constraint_state.conkey) WITH ORDINALITY AS key(attnum, position)
        JOIN pg_attribute AS attribute
          ON attribute.attrelid = constraint_state.conrelid
         AND attribute.attnum = key.attnum
        ORDER BY key.position
      ) = ARRAY['namespace', 'queue', 'group_id']
  ) THEN
    FOR constraint_name IN
      SELECT conname FROM pg_constraint
      WHERE conrelid = to_regclass('bunqueue_group_state')
        AND (contype = 'p' OR conname = 'bunqueue_group_state_pkey')
    LOOP
      EXECUTE format(
        'ALTER TABLE bunqueue_group_state DROP CONSTRAINT %I',
        constraint_name
      );
    END LOOP;
    IF to_regclass('bunqueue_group_state_pkey') IS NOT NULL THEN
      DROP INDEX bunqueue_group_state_pkey;
    END IF;
    ALTER TABLE bunqueue_group_state
      ADD CONSTRAINT bunqueue_group_state_pkey PRIMARY KEY (namespace, queue, group_id);
  END IF;
END;
$group_state_primary_key$;

DROP INDEX IF EXISTS bunqueue_jobs_group_ready_idx;
CREATE INDEX bunqueue_jobs_group_ready_idx
  ON bunqueue_jobs(namespace, queue, group_id, run_at ASC, group_order ASC, id ASC)
  WHERE group_id IS NOT NULL AND state IN ('waiting', 'prioritized', 'delayed');
DROP INDEX IF EXISTS bunqueue_group_state_rotation_idx;
CREATE INDEX bunqueue_group_state_rotation_idx
  ON bunqueue_group_state(namespace, queue, last_served, group_id);
`;
