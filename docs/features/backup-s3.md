# S3 Backup

> **Category:** Infrastructure · **Source:** `src/infrastructure/backup/s3Backup.ts`, `src/infrastructure/backup/s3BackupOperations.ts`, `src/infrastructure/backup/s3BackupConfig.ts`

## Purpose

Periodically snapshots the live SQLite database to an S3-compatible object store (AWS S3, Cloudflare R2, MinIO, DigitalOcean Spaces, etc.) and prunes old snapshots according to a retention policy. Each snapshot is gzip-compressed, checksummed (SHA-256), and accompanied by a JSON metadata sidecar so restores can verify integrity before swapping the live database into place. It exists to give a single-instance bunqueue server durable off-host disaster recovery without any external runtime dependency beyond Bun's built-in `S3Client`.

## Responsibilities & Scope

Owns:

- The automated backup scheduler (initial backup + periodic interval) — `s3Backup.ts:89` / `s3Backup.ts:109`.
- A single in-flight guard so concurrent backups cannot overlap — `s3Backup.ts:134`.
- WAL checkpoint before reading the DB file, gzip compression, checksum computation, and upload of both the `.db` and `.meta.json` objects — `s3BackupOperations.ts:144`.
- Listing snapshots (paginated), retention-based cleanup, and validate-before-replace restore — `s3BackupOperations.ts:231` / `:375` / `:276`.
- Config materialization from env (`fromEnv`) and validation (`validate`).
- Emitting dashboard lifecycle events (`storage:backup-*`).

Does NOT own:

- Flushing the in-memory `WriteBuffer` to SQLite. The manager only invokes an optional `flushBeforeBackup()` callback if one is supplied by the caller; see [Persistence](./persistence.md). The server bootstrap does **not** wire this callback (see Edge Cases).
- Config precedence/merging of file-config vs env — that lives in `src/config/resolve.ts` (`resolveBackupConfig`).
- The CLI surface (`bunqueue backup …`) — that lives in `src/cli/commands/backup.ts`.
- Server lifecycle (start/stop wiring) — done in `src/infrastructure/server/bootstrap.ts`.

## Dependencies

Internal:

- `s3BackupConfig.ts` — types, `DEFAULTS`, `configFromEnv`, `validateConfig`.
- `s3BackupOperations.ts` — the four free functions (`performBackup`, `listBackups`, `restoreBackup`, `cleanupOldBackups`).
- `../../shared/logger` (`backupLog`).
- `../../shared/version` (`VERSION`, stamped into metadata).
- Consumed by `src/config/resolve.ts`, `src/infrastructure/server/bootstrap.ts` (scheduler wiring + dashboard emit), `src/cli/commands/backup.ts` (CLI), and `src/infrastructure/cloud/snapshotCollector.ts` (reads `getStatus()` for telemetry).

External / runtime (Bun built-ins, zero npm deps):

- `S3Client` from `bun` — all object I/O (`file().write`, `file().arrayBuffer`, `file().exists`, `list`, `delete`).
- `bun:sqlite` `Database` — WAL checkpoint and `PRAGMA integrity_check`.
- `Bun.file`, `Bun.write`, `Bun.CryptoHasher('sha256')`.
- Web Streams `CompressionStream`/`DecompressionStream` (`'gzip'`) for non-blocking compress/decompress.
- `node:fs/promises` (`rename`, `unlink`) for the atomic restore swap.

## Public Interface

Exported from `src/infrastructure/backup/index.ts`: `S3BackupManager`, type `S3BackupConfig`, type `BackupResult`.

Class `S3BackupManager` (`s3Backup.ts:27`):

```typescript
constructor(config: Partial<S3BackupConfig> & {
  databasePath: string;
  flushBeforeBackup?: () => Promise<void>;
})

static fromEnv(databasePath: string): S3BackupConfig
setDashboardEmit(callback: (event: string, data: Record<string, unknown>) => void): void
validate(): { valid: boolean; errors: string[] }
start(): void
stop(): void
backup(): Promise<BackupResult>
listBackups(): Promise<BackupItem[]>
restore(key: string): Promise<BackupResult>
getStatus(): {
  enabled: boolean; bucket: string; endpoint: string;
  intervalMs: number; retention: number; isRunning: boolean;
}
```

Operations module free functions (`s3BackupOperations.ts`):

```typescript
performBackup(config: S3BackupConfig, client: S3Client): Promise<BackupResult>
listBackups(config: S3BackupConfig, client: S3Client): Promise<BackupItem[]>
restoreBackup(key: string, config: S3BackupConfig, client: S3Client): Promise<BackupResult>
cleanupOldBackups(config: S3BackupConfig, client: S3Client): Promise<void>
```

CLI commands (handled locally, **not** over TCP — `src/cli/commands/backup.ts`):

- `bunqueue backup now` (alias `create`) → `manager.backup()`
- `bunqueue backup list` → `manager.listBackups()`
- `bunqueue backup restore <key> [-f|--force]` → `manager.restore(key)` (refuses without `--force`, warns to stop the server first)
- `bunqueue backup status` → `manager.getStatus()`

No TCP commands and no HTTP endpoints are handled by this module.

Events emitted via the dashboard-emit callback (consumed by the WS/dashboard layer in `src/infrastructure/server/wsHandler.ts`):

- `storage:backup-started` → `{ bucket }`
- `storage:backup-completed` → `{ bucket, key }`
- `storage:backup-failed` → `{ bucket, error }`

## Data Models

See [data-model](../data-model.md) for the global schema. The shapes specific to this module (`s3BackupConfig.ts`):

`S3BackupConfig` — `enabled`, `accessKeyId`, `secretAccessKey`, `bucket`, `endpoint?`, `region?`, `intervalMs`, `retention`, `prefix`, `databasePath`, `timeoutMs?`.

`BackupResult` — `{ success: boolean; key?: string; size?: number; duration?: number; error?: string }`. `size` is the **original (uncompressed)** byte count; `duration` is in ms.

`BackupMetadata` — uploaded as `<key>.meta.json`: `{ timestamp, version, size, compressedSize?, checksum, compressed? }`. `size` = original bytes, `compressedSize` = gzip bytes, `checksum` = SHA-256 hex of the **uncompressed** data, `compressed` = `true` for the current format.

`BackupItem` — `{ key, size, lastModified: Date }`, returned by `listBackups`.

Object key layout: `${prefix}bunqueue-${timestamp}.db` plus a sibling `${...}.db.meta.json`, where `timestamp` is `new Date().toISOString()` with `:` and `.` replaced by `-` (`s3BackupOperations.ts:170`). Default `prefix` is `backups/`.

## Business Logic / Control Flow

**Scheduler (`start()` — `s3Backup.ts:89`):** No-op if `enabled` is false. Runs `validate()`; if invalid, logs and returns (does **not** throw). Schedules an initial backup `60 * 1000` ms (1 minute) after start (`s3Backup.ts:101`) and a recurring `setInterval` every `intervalMs` (`s3Backup.ts:109`). Both call `backup()` and swallow rejections into `backupLog.error`. `stop()` clears both timers.

**`backup()` orchestration (`s3Backup.ts:133`):**

1. If `isBackupInProgress`, returns `{ success: false, error: 'Backup already in progress' }` immediately (no throw) — `s3Backup.ts:134`.
2. Sets the in-flight flag, emits `storage:backup-started`.
3. Awaits `flushBeforeBackup()` if provided.
4. Calls `performBackup`. On success, runs `cleanupOldBackups` and emits `storage:backup-completed`; on failure emits `storage:backup-failed` with the result error.
5. A thrown exception (e.g. from `flushBeforeBackup`) emits `storage:backup-failed` and re-throws.
6. `finally` always clears `isBackupInProgress` (`s3Backup.ts:169`).

**`performBackup` (`s3BackupOperations.ts:144`):**

1. Verify the DB file exists; throw if not.
2. Open the DB and run `PRAGMA wal_checkpoint(TRUNCATE)` to fold the WAL into the main file; wrapped in try/catch that **ignores** errors (DB may be locked or non-WAL) — `s3BackupOperations.ts:160`.
3. Build the timestamped key.
4. Read the whole DB file into an `ArrayBuffer`, gzip it via `gzipAsync`, and compute SHA-256 over the **uncompressed** bytes.
5. Upload the compressed blob (`application/gzip`) under `withTimeout(withRetry(...))`, then upload the metadata JSON (`application/json`) the same way — `s3BackupOperations.ts:189` / `:206`.
6. Return `{ success: true, key, size: originalSize, duration }`. Any error is caught, logged, and returned as `{ success: false, error }` (never throws to the caller).

**`listBackups` (`s3BackupOperations.ts:231`):** Paginates `client.list({ prefix, maxKeys: 100, continuationToken? })` until `isTruncated` is false, keeps only keys ending in `.db` (excluding `.meta.json`), maps to `BackupItem`, sorts newest-first by `lastModified`. On error, logs and returns `[]`.

**`cleanupOldBackups` (`s3BackupOperations.ts:375`):** Lists backups, clamps retention to `Math.max(retention, 1)`. If `backups.length <= retention`, returns. Otherwise deletes everything after the newest `retention` (`backups.slice(retention)`), deleting both the `.db` object and its `.meta.json` if present. Per-object delete failures are caught and logged as warnings (cleanup continues).

**`restoreBackup` (`s3BackupOperations.ts:276`) — validate-before-replace:**

1. Verify the S3 object exists; throw if not.
2. Download with `withTimeout` (no retry on download).
3. Read the `.meta.json` if present.
4. Decide `isCompressed` from `metadata.compressed`, else by sniffing gzip magic bytes `0x1f 0x8b` (`s3BackupOperations.ts:307`) — handles legacy uncompressed backups.
5. Decompress if needed.
6. If metadata has a checksum, recompute SHA-256 over the decompressed data and throw on mismatch (`s3BackupOperations.ts:323`).
7. Validate the first 16 bytes start with `SQLite format 3` (`s3BackupOperations.ts:330`).
8. Write the payload to a temp file `${databasePath}.restore-<ts>-<rand>.tmp`, run `verifyDatabaseIntegrity` (`PRAGMA integrity_check`) on the temp file, then `rename()` it over the live DB only on full success (`s3BackupOperations.ts:338`–`:349`). On any failure the temp file is unlinked (best effort) and the live DB is left untouched.

**Retry / timeout helpers:** `withRetry` (`s3BackupOperations.ts:50`) does up to 3 retries (4 attempts) with exponential backoff 500/1000/2000 ms, **only** for transient errors classified by `isTransientError` (`:26`) — connection reset/refused, timeouts, socket hang up, network, HTTP 500/502/503/504, etc. `withTimeout` (`:14`) races against `DEFAULT_S3_TIMEOUT_MS = 30_000` (overridable via `config.timeoutMs`).

## Concurrency & Locking

- No shard/job locks are taken; this module operates entirely outside the [Concurrency & Locking](./concurrency-and-locking.md) hierarchy.
- The only mutual-exclusion is the per-instance `isBackupInProgress` boolean (`s3Backup.ts:134`). It guards against the periodic interval firing while a previous backup is still running, and against an overlapping manual `backup()` on the **same** manager instance. It is not cross-process: a separately constructed manager (e.g. the CLI's `bunqueue backup now`) shares no state with the running server.
- The WAL checkpoint and DB-file read happen while the server may still be writing. There is no read lock; the checkpoint plus SQLite's WAL semantics keep the snapshot internally consistent, but writes landing after the checkpoint may or may not be captured.
- Restore temp filenames include `Date.now()` + a random suffix to avoid collisions between concurrent restores; the final `rename` is the atomic swap point.

## Edge Cases & Failure Modes

- **WriteBuffer not flushed by default.** `backup()` only flushes if a `flushBeforeBackup` callback was passed to the constructor. The server bootstrap (`src/infrastructure/server/bootstrap.ts:133`) constructs the manager **without** this callback, relying solely on `PRAGMA wal_checkpoint(TRUNCATE)`. In-memory writes still buffered in the 10ms `WriteBuffer` (see [Persistence](./persistence.md)) may not be on disk at checkpoint time, so the snapshot can miss the most recent ~10ms of non-durable writes. Invariant to be aware of when reasoning about RPO.
- **Validation failure is silent at startup.** `start()` logs invalid config and returns without throwing; the server keeps running with backups disabled. `validateConfig` requires `accessKeyId`, `secretAccessKey`, `bucket`, `databasePath`, `retention >= 1`, and `intervalMs >= 60000` (`s3BackupConfig.ts:90`).
- **`performBackup` / `cleanupOldBackups` never throw to the scheduler** — failures surface as `{ success: false }` or logged warnings. A failed `performBackup` skips cleanup (cleanup only runs on success), so a transient outage will not prune backups.
- **WAL checkpoint errors are ignored** — a locked or non-WAL DB still proceeds to read the file as-is.
- **Restore is overwrite-but-safe.** The live DB is only replaced after integrity + checksum + header validation on a temp file pass; any failure leaves the original intact. `verifyDatabaseIntegrity` deletes the file it inspects on failure, so it must only ever target the temp candidate, never the live DB (enforced by always pointing it at `tempPath`).
- **Checksum/compression backward compatibility.** Restore detects gzip via metadata or magic bytes, and skips checksum verification when no metadata sidecar exists — older or sidecar-less backups still restore.
- **Pagination.** `listBackups` follows `nextContinuationToken` so retention/cleanup are correct beyond 100 objects.
- **Cleanup partial failure** — a delete error for one key is logged and skipped; remaining old backups are still attempted.
- **Download has no retry** (only `withTimeout`), unlike upload which has both retry and timeout.
- **CLI restore guard** — `bunqueue backup restore` refuses without `--force` and warns to stop the server first (`src/cli/commands/backup.ts:128`).

## Configuration

Defaults live in `s3BackupConfig.ts:62` (`DEFAULTS`). Env parsing is `configFromEnv` (`:72`); the server uses `resolveBackupConfig` (`src/config/resolve.ts:123`), which lets a file-config `backup` block override each env var.

| Env var | Config field | Default | Notes |
| --- | --- | --- | --- |
| `S3_BACKUP_ENABLED` | `enabled` | `false` | `'1'` or `'true'` enables |
| `S3_ACCESS_KEY_ID` (fallback `AWS_ACCESS_KEY_ID`) | `accessKeyId` | `''` | required |
| `S3_SECRET_ACCESS_KEY` (fallback `AWS_SECRET_ACCESS_KEY`) | `secretAccessKey` | `''` | required |
| `S3_BUCKET` (fallback `AWS_BUCKET`) | `bucket` | `''` | required |
| `S3_ENDPOINT` (fallback `AWS_ENDPOINT`) | `endpoint` | unset | non-AWS S3-compatible services |
| `S3_REGION` (fallback `AWS_REGION`) | `region` | `us-east-1` | |
| `S3_BACKUP_INTERVAL` | `intervalMs` | `21600000` (6h) | must be ≥ 60000 ms |
| `S3_BACKUP_RETENTION` | `retention` | `7` | must be ≥ 1; clamped to ≥1 in cleanup |
| `S3_BACKUP_PREFIX` | `prefix` | `backups/` | object key prefix |
| — | `timeoutMs` | `30000` | per-op timeout; constructor does not read it from env |
| `BUNQUEUE_DATA_PATH` (and `BQ_DATA_PATH`/`DATA_PATH`/`SQLITE_PATH`) | `databasePath` | — | the SQLite file to snapshot/restore |

Note: `S3_BACKUP_INTERVAL`/`S3_BACKUP_RETENTION` use `parseInt(...) || DEFAULT`, so `0` or non-numeric falls back to the default. `timeoutMs` has no env binding in `configFromEnv`/`resolveBackupConfig` and defaults to `DEFAULT_S3_TIMEOUT_MS` inside the operations.

## Related Docs

- [Persistence (SQLite, WriteBuffer, ReadThrough)](./persistence.md)
- [Configuration & Entrypoint](./configuration.md)
- [Background Tasks](./background-tasks.md)
- [CLI](./cli.md)
- [bunqueue Cloud Dashboard Integration](./cloud-integration.md)
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md)
- [architecture](../architecture.md)
- [data-model](../data-model.md)
