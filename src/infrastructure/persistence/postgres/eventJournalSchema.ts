export const POSTGRES_REGISTER_EVENT_ROWS_BODY = `BEGIN
  INSERT INTO bunqueue_event_commits (namespace, transaction_id)
  SELECT DISTINCT namespace, transaction_id FROM new_event_rows
  ORDER BY namespace, transaction_id
  ON CONFLICT DO NOTHING;
  RETURN NULL;
END;`;

export const POSTGRES_REGISTER_WATERMARK_ROWS_BODY = `BEGIN
  INSERT INTO bunqueue_event_commits (namespace, transaction_id)
  SELECT DISTINCT namespace, transaction_id FROM new_watermark_rows
  ORDER BY namespace, transaction_id
  ON CONFLICT DO NOTHING;
  RETURN NULL;
END;`;

export const POSTGRES_ASSIGN_EVENT_COMMIT_BODY = `DECLARE
  assigned_commit_seq BIGINT;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'bunqueue:event-commit:' || length(NEW.namespace)::text || ':' || NEW.namespace,
    0
  ));
  assigned_commit_seq := nextval('bunqueue_event_commit_seq');

  UPDATE bunqueue_event_commits
  SET commit_seq = assigned_commit_seq
  WHERE namespace = NEW.namespace
    AND transaction_id = NEW.transaction_id
    AND commit_seq IS NULL;
  UPDATE bunqueue_event_prune_watermarks
  SET commit_seq = assigned_commit_seq,
      pruned_commit_seq = CASE
        WHEN prunes_current_transaction THEN GREATEST(
          COALESCE(pruned_commit_seq, 0), assigned_commit_seq
        )
        ELSE pruned_commit_seq
      END,
      prunes_current_transaction = FALSE
  WHERE namespace = NEW.namespace
    AND transaction_id = NEW.transaction_id
    AND commit_seq IS NULL;
  RETURN NULL;
END;`;

/** Commit-time journal sequencer installed by the PostgreSQL schema migration. */
export const POSTGRES_EVENT_JOURNAL_SCHEMA = `
CREATE SEQUENCE IF NOT EXISTS bunqueue_event_commit_seq AS BIGINT CACHE 1;
ALTER SEQUENCE bunqueue_event_commit_seq
  AS BIGINT INCREMENT BY 1 NO MINVALUE NO MAXVALUE NO CYCLE CACHE 1;

CREATE TABLE IF NOT EXISTS bunqueue_event_commits (
  namespace TEXT NOT NULL,
  transaction_id BIGINT NOT NULL,
  commit_seq BIGINT,
  PRIMARY KEY (namespace, transaction_id)
);

ALTER TABLE bunqueue_event_commits
  ADD COLUMN IF NOT EXISTS namespace TEXT;
ALTER TABLE bunqueue_event_commits
  ADD COLUMN IF NOT EXISTS transaction_id BIGINT;
ALTER TABLE bunqueue_event_commits
  ADD COLUMN IF NOT EXISTS commit_seq BIGINT;
ALTER TABLE bunqueue_event_commits
  ALTER COLUMN namespace TYPE TEXT USING namespace::text,
  ALTER COLUMN transaction_id TYPE BIGINT USING transaction_id::bigint,
  ALTER COLUMN commit_seq TYPE BIGINT USING commit_seq::bigint;

ALTER TABLE bunqueue_events
  ADD COLUMN IF NOT EXISTS transaction_id BIGINT;
ALTER TABLE bunqueue_events
  ALTER COLUMN transaction_id DROP DEFAULT;
ALTER TABLE bunqueue_events
  ALTER COLUMN transaction_id TYPE BIGINT USING transaction_id::bigint;
ALTER TABLE bunqueue_events
  ALTER COLUMN transaction_id SET DEFAULT (pg_current_xact_id()::text::bigint);

ALTER TABLE bunqueue_event_prune_watermarks
  ADD COLUMN IF NOT EXISTS transaction_id BIGINT;
ALTER TABLE bunqueue_event_prune_watermarks
  ADD COLUMN IF NOT EXISTS commit_seq BIGINT;
ALTER TABLE bunqueue_event_prune_watermarks
  ADD COLUMN IF NOT EXISTS pruned_commit_seq BIGINT;
ALTER TABLE bunqueue_event_prune_watermarks
  ADD COLUMN IF NOT EXISTS prunes_current_transaction BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE bunqueue_event_prune_watermarks
  ALTER COLUMN transaction_id DROP DEFAULT,
  ALTER COLUMN pruned_commit_seq DROP DEFAULT,
  ALTER COLUMN prunes_current_transaction DROP DEFAULT;
ALTER TABLE bunqueue_event_prune_watermarks
  ALTER COLUMN transaction_id TYPE BIGINT USING transaction_id::bigint,
  ALTER COLUMN commit_seq TYPE BIGINT USING commit_seq::bigint,
  ALTER COLUMN pruned_commit_seq TYPE BIGINT USING pruned_commit_seq::bigint,
  ALTER COLUMN prunes_current_transaction TYPE BOOLEAN
    USING prunes_current_transaction::boolean;
ALTER TABLE bunqueue_event_prune_watermarks
  ALTER COLUMN transaction_id SET DEFAULT (pg_current_xact_id()::text::bigint);
ALTER TABLE bunqueue_event_prune_watermarks
  ALTER COLUMN pruned_commit_seq SET DEFAULT 0,
  ALTER COLUMN prunes_current_transaction SET DEFAULT FALSE;

UPDATE bunqueue_events
SET transaction_id = -id
WHERE transaction_id IS NULL;
UPDATE bunqueue_event_prune_watermarks
SET transaction_id = COALESCE(transaction_id, -source_event_id),
    commit_seq = COALESCE(commit_seq, source_event_id),
    pruned_commit_seq = COALESCE(pruned_commit_seq, commit_seq, source_event_id)
WHERE transaction_id IS NULL OR commit_seq IS NULL OR pruned_commit_seq IS NULL;

ALTER TABLE bunqueue_events
  ALTER COLUMN transaction_id SET NOT NULL;
ALTER TABLE bunqueue_event_commits
  ALTER COLUMN namespace SET NOT NULL,
  ALTER COLUMN transaction_id SET NOT NULL;
ALTER TABLE bunqueue_event_prune_watermarks
  ALTER COLUMN transaction_id SET NOT NULL,
  ALTER COLUMN pruned_commit_seq SET NOT NULL,
  ALTER COLUMN prunes_current_transaction SET NOT NULL;

DO $migration$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = to_regclass('bunqueue_events')
      AND attname = 'commit_seq' AND NOT attisdropped
  ) THEN
    EXECUTE $sql$
      INSERT INTO bunqueue_event_commits (namespace, transaction_id, commit_seq)
      SELECT namespace, transaction_id, MAX(commit_seq)
      FROM bunqueue_events
      WHERE commit_seq IS NOT NULL
      GROUP BY namespace, transaction_id
      ON CONFLICT (namespace, transaction_id) DO UPDATE SET
        commit_seq = GREATEST(
          COALESCE(bunqueue_event_commits.commit_seq, 0),
          excluded.commit_seq
        )
    $sql$;
  ELSE
    INSERT INTO bunqueue_event_commits (namespace, transaction_id, commit_seq)
    SELECT namespace, transaction_id, MAX(id)
    FROM bunqueue_events
    GROUP BY namespace, transaction_id
    ON CONFLICT (namespace, transaction_id) DO NOTHING;
  END IF;
END;
$migration$;

INSERT INTO bunqueue_event_commits (namespace, transaction_id, commit_seq)
SELECT namespace, transaction_id, MAX(commit_seq)
FROM bunqueue_event_prune_watermarks
WHERE commit_seq IS NOT NULL
GROUP BY namespace, transaction_id
ON CONFLICT (namespace, transaction_id) DO UPDATE SET
  commit_seq = GREATEST(
    COALESCE(bunqueue_event_commits.commit_seq, 0),
    excluded.commit_seq
  );

DO $block$
DECLARE
  journal_floor BIGINT := 0;
  legacy_head_floor BIGINT := 0;
  sequence_floor BIGINT := 0;
BEGIN
  SELECT COALESCE(MAX(commit_seq), 0) INTO journal_floor
  FROM bunqueue_event_commits;
  SELECT last_value INTO sequence_floor FROM bunqueue_event_commit_seq;
  IF to_regclass('bunqueue_event_commit_heads') IS NOT NULL THEN
    EXECUTE 'SELECT COALESCE(MAX(commit_seq), 0) FROM bunqueue_event_commit_heads'
      INTO legacy_head_floor;
  END IF;
  PERFORM setval(
    'bunqueue_event_commit_seq',
    GREATEST(journal_floor, legacy_head_floor, sequence_floor, 1),
    TRUE
  );
END;
$block$;

DROP TABLE IF EXISTS bunqueue_event_commit_heads;

DROP TRIGGER IF EXISTS bunqueue_events_register_commit ON bunqueue_events;
DROP TRIGGER IF EXISTS bunqueue_watermarks_insert_register_commit
  ON bunqueue_event_prune_watermarks;
DROP TRIGGER IF EXISTS bunqueue_watermarks_update_register_commit
  ON bunqueue_event_prune_watermarks;
DROP TABLE IF EXISTS bunqueue_event_commit_assignments CASCADE;
DROP INDEX IF EXISTS bunqueue_events_commit_idx;
DROP INDEX IF EXISTS bunqueue_events_pending_commit_idx;
ALTER TABLE bunqueue_events DROP COLUMN IF EXISTS commit_seq;

DROP INDEX IF EXISTS bunqueue_events_transaction_idx;
DROP INDEX IF EXISTS bunqueue_event_commits_replay_idx;
DROP INDEX IF EXISTS bunqueue_event_prune_watermarks_commit_idx;
DROP INDEX IF EXISTS bunqueue_event_prune_watermarks_pending_commit_idx;
DROP INDEX IF EXISTS bunqueue_event_prune_watermarks_transaction_idx;

CREATE INDEX bunqueue_events_transaction_idx
  ON bunqueue_events(namespace, transaction_id, id);
CREATE INDEX bunqueue_event_commits_replay_idx
  ON bunqueue_event_commits(namespace, commit_seq, transaction_id)
  WHERE commit_seq IS NOT NULL;
CREATE INDEX bunqueue_event_prune_watermarks_commit_idx
  ON bunqueue_event_prune_watermarks(namespace, queue, commit_seq DESC)
  WHERE commit_seq IS NOT NULL;
CREATE INDEX bunqueue_event_prune_watermarks_pending_commit_idx
  ON bunqueue_event_prune_watermarks(namespace, transaction_id)
  WHERE commit_seq IS NULL;
CREATE INDEX bunqueue_event_prune_watermarks_transaction_idx
  ON bunqueue_event_prune_watermarks(namespace, transaction_id);

CREATE OR REPLACE FUNCTION bunqueue_register_event_rows()
RETURNS TRIGGER LANGUAGE plpgsql AS $function$
${POSTGRES_REGISTER_EVENT_ROWS_BODY}
$function$;

CREATE OR REPLACE FUNCTION bunqueue_register_watermark_rows()
RETURNS TRIGGER LANGUAGE plpgsql AS $function$
${POSTGRES_REGISTER_WATERMARK_ROWS_BODY}
$function$;

CREATE OR REPLACE FUNCTION bunqueue_assign_event_commit()
RETURNS TRIGGER LANGUAGE plpgsql AS $function$
${POSTGRES_ASSIGN_EVENT_COMMIT_BODY}
$function$;

DROP TRIGGER IF EXISTS bunqueue_events_register_commit ON bunqueue_events;
CREATE TRIGGER bunqueue_events_register_commit
AFTER INSERT ON bunqueue_events
REFERENCING NEW TABLE AS new_event_rows
FOR EACH STATEMENT EXECUTE FUNCTION bunqueue_register_event_rows();

DROP TRIGGER IF EXISTS bunqueue_watermarks_insert_register_commit
  ON bunqueue_event_prune_watermarks;
CREATE TRIGGER bunqueue_watermarks_insert_register_commit
AFTER INSERT ON bunqueue_event_prune_watermarks
REFERENCING NEW TABLE AS new_watermark_rows
FOR EACH STATEMENT EXECUTE FUNCTION bunqueue_register_watermark_rows();

DROP TRIGGER IF EXISTS bunqueue_watermarks_update_register_commit
  ON bunqueue_event_prune_watermarks;
CREATE TRIGGER bunqueue_watermarks_update_register_commit
AFTER UPDATE ON bunqueue_event_prune_watermarks
REFERENCING NEW TABLE AS new_watermark_rows
FOR EACH STATEMENT EXECUTE FUNCTION bunqueue_register_watermark_rows();

DROP TRIGGER IF EXISTS bunqueue_assign_event_commit
  ON bunqueue_event_commits;
CREATE CONSTRAINT TRIGGER bunqueue_assign_event_commit
AFTER INSERT ON bunqueue_event_commits
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION bunqueue_assign_event_commit();
`;
