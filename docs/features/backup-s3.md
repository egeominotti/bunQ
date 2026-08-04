# S3 Backup

> **Category:** Infrastructure · **Source:** `src/infrastructure/backup/`

## Purpose

S3 backup creates off-host, transactionally consistent copies of bunqueue's
SQLite database. Current-format backups are gzip-compressed and paired with a
metadata object containing the uncompressed size and SHA-256 digest. The same
implementation is used by the scheduled server backup and by the local
`bunqueue backup` CLI.

Backup requires a persistent SQLite path. An in-memory server has no database
file to snapshot, so enabling S3 backup without `BUNQUEUE_DATA_PATH`,
`BQ_DATA_PATH`, `DATA_PATH`, `SQLITE_PATH`, or `storage.dataPath` is a startup
configuration error. `bootServer` exits with code 1 before binding TCP/HTTP;
it does not keep running with backup silently disabled.

## Module Map

- `s3Backup.ts` — scheduler, per-manager overlap guard, S3 client construction,
  dashboard events and orchestration.
- `s3BackupConfig.ts` — public types, defaults, environment parsing and
  validation.
- `s3BackupOperations.ts` — backup, listing, restore and retention operations.
- `sqliteBackupFiles.ts` — WAL-safe SQLite snapshot creation, integrity checks,
  live-file installation and sidecar quarantine.
- `s3BackupIo.ts` — gzip, SHA-256, transient retry and bounded operations.

The server constructs the manager in `bootstrap.ts`. Before every server-side
snapshot it calls `QueueManager.flushPersistence()`, which synchronously asks
the SQLite `WriteBuffer` to flush. If storage retry/backoff leaves any write
pending, the flush throws and the backup aborts: bunqueue never reports a
snapshot as complete while accepted state exists only in memory. CLI commands
operate locally and therefore require the server to be stopped before restore.

When the Cloud agent is enabled, bootstrap also injects a `triggerBackup`
server handle. The whitelisted `s3:backup` remote command awaits the same
manager's `backup()` method and returns its real result. The handle is omitted
when backup is disabled, in which case the Cloud command fails explicitly with
`S3 backup not configured`; a rejected backup is likewise returned as a failed
command result.

## Configuration

`S3BackupConfig` contains:

```typescript
interface S3BackupConfig {
  enabled: boolean;
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  bucket: string;
  endpoint?: string;
  virtualHostedStyle?: boolean;
  region?: string;
  intervalMs: number;
  retention: number;
  prefix: string;
  databasePath: string;
  timeoutMs?: number;
}
```

| Environment | Field | Default / rule |
| --- | --- | --- |
| `S3_BACKUP_ENABLED` | `enabled` | `false`; `1` or `true` enables |
| `S3_ACCESS_KEY_ID` / `AWS_ACCESS_KEY_ID` | `accessKeyId` | required |
| `S3_SECRET_ACCESS_KEY` / `AWS_SECRET_ACCESS_KEY` | `secretAccessKey` | required |
| `S3_SESSION_TOKEN` / `AWS_SESSION_TOKEN` | `sessionToken` | optional temporary-credential token |
| `S3_BUCKET` / `AWS_BUCKET` | `bucket` | required |
| `S3_ENDPOINT` / `AWS_ENDPOINT` | `endpoint` | unset for AWS |
| `S3_VIRTUAL_HOSTED_STYLE` | `virtualHostedStyle` | provider/client default; `1` or `true` forces bucket-in-host requests |
| `S3_REGION` / `AWS_REGION` | `region` | `us-east-1` |
| `S3_BACKUP_INTERVAL` | `intervalMs` | `21600000`; minimum 60000 ms |
| `S3_BACKUP_RETENTION` | `retention` | `7`; minimum 1 |
| `S3_BACKUP_PREFIX` | `prefix` | `backups/` |
| data-path aliases above | `databasePath` | required |
| constructor only | `timeoutMs` | 30000 ms per S3 attempt |

`validateConfig` rejects missing credentials, bucket or database path,
retention below one, and intervals shorter than one minute. File configuration
wins over environment configuration in `resolveBackupConfig`.

## Backup State Machine

`S3BackupManager.start()` validates configuration, schedules the first backup
one minute after startup, then schedules the configured interval. Repeated
`start()` calls are idempotent while that scheduler is active: the manager owns
exactly one initial timeout and one periodic interval. `stop()` invalidates the
scheduler generation before clearing both handles, so a callback that was
already queued cannot run a backup or detach a newer handle. A later `start()`
creates a fresh generation and is supported. Stopping the scheduler does not
cancel a `backup()` already in progress. Only one `backup()` may run on a
manager instance; an overlap returns a failed `BackupResult`.

`BackupTelemetry` owns the observable attempt state. It exposes label-free,
zero-initialized Prometheus values for enablement, scheduler/activity state,
configured interval/retention, attempt/success/failure/overlap counters,
consecutive failures, and the last outcome timestamps, duration and compressed
size. A successful `BackupResult` retains both `size` (uncompressed SQLite
bytes) and `compressedSize` (uploaded gzip bytes); telemetry uses the latter.
`bootstrap.ts` injects a snapshot provider into QueueManager; instances remain
isolated and no process-global backup state is used.

Every transition preserves:

```text
attempts_total = successes_total + failures_total + (in_progress ? 1 : 0)
```

An overlap increments only `overlap_rejections_total`. Success resets
`consecutive_failures`; Prometheus exports Unix event timestamps so freshness
is computed with `time() - last_success_timestamp_seconds`.

One successful backup performs these transitions:

1. Flush pending server `WriteBuffer` entries through the callback supplied by
   `bootstrap.ts`; reject the cycle if the pending count is non-zero afterward.
2. Open a separate SQLite connection and run `VACUUM INTO` to a unique sibling
   file. This is a SQLite-level snapshot: committed frames in the WAL are read
   even if a long-lived reader prevents a `wal_checkpoint(TRUNCATE)`.
3. Run `PRAGMA integrity_check` on that standalone snapshot.
4. Read and gzip the snapshot; compute SHA-256 over the uncompressed bytes.
5. Generate a collision-resistant key:
   `${prefix}bunqueue-${timestamp}-${crypto.randomUUID()}.db`.
6. Upload `<key>.meta.json` first, then upload `<key>` as the publication/commit
   point. A `.db` object produced by the current writer is therefore never
   visible without its metadata already present.
7. List all `.db` objects and delete payload/metadata pairs older than the
   newest `retention` objects.
8. Remove the local snapshot and its possible SQLite sidecars in `finally`.

The metadata shape is:

```typescript
interface BackupMetadata {
  timestamp: string;
  version: string;
  size: number;            // uncompressed bytes
  compressedSize: number;  // uploaded bytes
  checksum: string;        // SHA-256 of uncompressed bytes
  compressed: true;
}
```

An immediate payload-upload failure triggers best-effort deletion of both the
payload key and metadata. An orphan can remain only when that cleanup itself
fails, or after a locally timed-out PUT whose remote completion cannot safely
be inferred or cancelled. Listing and retention deliberately use `.db` objects
as the set of committed backups; a metadata-only orphan is not advertised as a
backup.

## Restore State Machine

`bunqueue backup restore <key> --force` requires explicit overwrite consent.
The server must be stopped: atomically replacing a file does not invalidate
already-open SQLite handles.

Restore then:

1. Downloads the payload and checks for `<key>.meta.json`, with retry and a
   timeout on every S3 operation attempt.
2. For current format, requires valid metadata, requires gzip, decompresses,
   checks compressed and uncompressed sizes, and verifies SHA-256.
3. Without metadata, accepts only a legacy **uncompressed** SQLite payload.
   A gzip payload without metadata is rejected because it cannot be
   authenticated.
4. Checks the `SQLite format 3` header, writes a unique temporary candidate,
   and runs `PRAGMA integrity_check`.
5. Quarantines live `-wal`, `-shm`, and `-journal` sidecars before the swap.
   A failure before the swap rolls those moves back.
6. Atomically renames the validated candidate over the main database and then
   removes quarantined sidecars. Stale WAL frames can therefore never replay
   over the restored database.

Until the candidate rename succeeds, the live main file is untouched. Invalid
metadata, checksum mismatch, corrupt gzip, bad SQLite header, failed integrity
check, or a pre-swap filesystem error returns a failed `BackupResult`.

## S3 Reliability Semantics

Every list, existence check, download, upload and delete uses a per-attempt
timeout (30 seconds by default). Network errors, timeouts and retryable 5xx
conditions receive up to three retries with 500/1000/2000 ms backoff. Timeout
handles are always cleared.

`listBackups` follows continuation tokens, rejects missing/repeated continuation
tokens, sorts newest first, and throws a contextual error instead of converting
an S3 failure into an indistinguishable empty bucket. Retention cleanup logs and
continues after an individual pair deletion failure.

The overlap flag is process-local. Operators must not run scheduled and manual
backups from different processes against the same file at the same time. Unique
snapshot paths and object keys prevent accidental key/file collisions, but they
are not a distributed lock.

## Invariants and Tests

The deterministic regressions cover:

- a WAL reader that blocks checkpointing while committed rows still appear in
  the snapshot;
- stale live WAL/SHM sidecars that must not alter restored content;
- checksum, metadata, legacy-format and listing failure behavior.

`test/model-based/backup-model.test.ts` generates command histories over
inserts, long-lived readers, backup, restore, retention and corruption. Its
model asserts:

- source rows equal the logical model;
- each listed current-format backup has metadata;
- restoring a snapshot produces exactly the rows captured at publication;
- stale sidecars cannot resurrect newer rows;
- retention preserves exactly the newest configured backups;
- a corrupt restore never changes the current live database.

The backup model is part of `bun run test:model`, alongside the broker lifecycle
model. `enterprise-telemetry-model.test.ts` additionally generates backup
start/success/failure/overlap/scheduler transitions and checks counter
conservation after every action.

## Related Documentation

- [Persistence](./persistence.md)
- [Configuration](./configuration.md)
- [CLI](./cli.md)
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md)
- [Architecture](../architecture.md)
- [Data model](../data-model.md)
