# Persistence (SQLite, WriteBuffer, ReadThrough)

> **Category:** Infrastructure · **Source:** `src/infrastructure/persistence/sqlite.ts`, `src/infrastructure/persistence/sqliteBatch.ts`, `src/infrastructure/persistence/schema.ts`, `src/infrastructure/persistence/statements.ts`, `src/infrastructure/persistence/sqliteSerializer.ts`

## Purpose

The persistence layer is the durable store that backs the in-memory sharded queues. It wraps Bun's native `bun:sqlite` (`Database`) in WAL mode, batches inserts through a double-buffered `WriteBuffer` for high throughput, serializes payloads with MessagePack (`msgpackr`) for ~2–3x smaller/faster blobs than JSON, and exposes batched recovery queries used to rebuild in-memory state on restart. It exists so that jobs, results, DLQ entries, cron definitions, and per-queue control-state survive a process restart without forcing every write onto the hot path.

## Responsibilities & Scope

Owns:
- The SQLite `Database` handle, PRAGMA tuning, schema creation, and incremental migrations (`sqlite.ts:71-121`, `sqlite.ts:255-278`).
- Buffered and durable (immediate) job inserts, single and bulk (`insertJob`, `insertJobImmediate`, `insertJobsBatch`).
- State-mutating writes: `markActive`, `markCompleted`, `markFailed`, `updateForRetry`, `updateJobData`, `updateJobChildrenIds`, `deleteJob`.
- Results, DLQ rows, cron rows, and queue control-state rows (CRUD + bulk load).
- Paginated recovery reads (`loadPendingJobs`, `loadActiveJobs`, `loadCompletedJobs`, `loadDlq`, plus the id-set loaders).
- Disk-full detection, write-retry/backoff, and critical-loss accounting.
- MessagePack (de)serialization and DB-row ↔ `Job` conversion.

Does NOT own (delegated elsewhere):
- The recovery orchestration / re-enqueue logic — `recover()` in [Background Tasks](./background-tasks.md) (`backgroundTasks.ts:227`) consumes these read APIs and decides what to enqueue, retry, or quarantine.
- Data-path resolution from env/file-config — [Configuration & Entrypoint](./configuration.md) (`config/resolve.ts:44-49`).
- In-memory queue/shard state, dedup maps, and counters — [Core Queue Engine](./core-queue-engine.md).
- DLQ policy (auto-retry, expiry) — [Dead Letter Queue](./dead-letter-queue.md). This layer only persists/loads the rows.
- S3 file backup of the `.db` — [S3 Backup](./backup-s3.md).
- A separate read-through cache object: there is no standalone cache class here. Reads hit prepared statements directly; the "ReadThrough" concept is realized as the in-memory collections owned by the [Core Queue Engine](./core-queue-engine.md), which are hydrated from these load APIs at boot and then served in memory.

## Dependencies

Internal:
- `domain/types/job` (`Job`, `JobId`, `JobTimelineEntry`), `domain/types/cron` (`CronJob`), `domain/types/dlq` (`DlqEntry`, `FailureReason`, `createDlqEntry`).
- `shared/logger` (`storageLog`).
- Sibling modules in this folder: `schema.ts`, `statements.ts`, `sqliteSerializer.ts`, `sqliteBatch.ts`.

External/runtime:
- `bun:sqlite` `Database` (native SQLite).
- `msgpackr` `pack`/`unpack` — the only third-party runtime dependency touched here.
- `Bun.file()` for `getSize()`.

## Public Interface

`SqliteStorage` (class) — the single facade, constructed with a `SqliteConfig`:

```ts
interface SqliteConfig {
  path: string;
  walMode?: boolean;
  synchronous?: 'OFF' | 'NORMAL' | 'FULL';
  cacheSize?: number;
  writeBufferSize?: number;     // default: 100
  writeBufferFlushMs?: number;  // constructor default: 10 (JSDoc says 50 — stale)
  onCriticalLoss?: SqliteCriticalLossCallback;
}
type SqliteCriticalLossCallback = (jobs: Job[], lastError: Error, attempts: number) => void;
```

> Note: `QueueManager` instantiates storage with only `{ path }` (`queueManager.ts:148`), so the effective defaults are `writeBufferSize=100` and `writeBufferFlushMs=10` (`sqlite.ts:93-94`). The `writeBufferFlushMs` JSDoc at `sqlite.ts:42-43` claims a default of 50 but the actual fallback is 10.

Key methods (real signatures):
- Inserts: `insertJob(job: Job, durable?: boolean): void`, `insertJobImmediate(job: Job): void`, `insertJobsBatch(jobs: Job[], durable?: boolean): void`.
- State: `markActive(jobId, startedAt, timeline?)`, `markCompleted(jobId, completedAt, timeline?)`, `markFailed(job, error)`, `updateForRetry(job)`, `updateJobData(jobId, data)`, `updateRunAt(jobId, runAt)` (persists moveToDelayed/changeDelay, re-deriving `state` from `run_at`), `updateJobChildrenIds(jobId, childrenIds)`, `deleteJob(jobId)`.
- Results: `storeResult(jobId, result)`, `getResult(jobId)`, `hasResult(jobId)`.
- DLQ: `saveDlqEntry(entry)`, `deleteDlqEntry(jobId)`, `clearDlqQueue(queue)`, `loadDlq(): Map<string, DlqEntry[]>`, `getDlqEntry(jobId)`, `hasDlqEntry(jobId)`, `loadDlqJobIds(): Set<JobId>`.
- Queries: `getJob(id)`, `getJobStateRaw(jobId)`, `queryJobs(queue, {state|states, limit, offset, asc})`.
- Recovery loads: `loadPendingJobs(limit=10000, offset=0)`, `loadActiveJobs(limit=10000, offset=0)`, `loadCompletedJobs(limit=10000, offset=0)`, `loadCompletedJobIds(): Set<JobId>`, `countPendingJobs()`, `countActiveJobs()`.
- Cron: `saveCron(cron)`, `loadCronJobs(): CronJob[]`, `deleteCron(name)`, `updateCron(name, executions, nextRun)`.
- Queue control-state (#100): `saveQueueState(name, paused, rateLimit, concurrencyLimit)`, `loadQueueState()`, `deleteQueueState(name)`.
- Health/ops: `flushWriteBuffer(): number`, `get diskFull`, `getDiskFullStatus()`, `getCriticalLosses()`, `clearCriticalLosses()`, `getSize()`, `close()`.

`sqliteBatch.ts` exports:
- `class BatchInsertManager` — `insertJobsBatch(jobs: Job[]): BatchInsertResult` (never throws).
- `class WriteBuffer` — `add`, `addBatch`, `flush(): number`, `hasPending(id)`, `removePending(id)`, `pendingCount`, `stop()`, `stopGracefully(timeoutMs=5000): Promise<number>`, `getRetryState()`.
- Types: `BatchInsertResult { transient: Job[]; conflicts: Job[]; error?: Error }`, `WriteBufferErrorCallback`, `CriticalErrorCallback`.

`sqliteSerializer.ts` exports:
- `pack(data): Uint8Array`, `unpack<T>(buffer, fallback, context): T`.
- `rowToJob(row: DbJob): Job`, `reconstructDlqEntry(entry: DlqEntry): DlqEntry`.
- `CORRUPT_DEPENDS_ON: symbol`, `isCorruptDependsOn(job): boolean`.

`schema.ts` exports: `PRAGMA_SETTINGS`, `SCHEMA`, `MIGRATION_TABLE`, `SCHEMA_VERSION = 13`, `MIGRATIONS`.
`statements.ts` exports: `SQL_STATEMENTS`, `prepareStatements(db)`, `StatementName`, and DB-row types `DbJob`, `DbCron`, `DbQueueState`.

No TCP commands, HTTP endpoints, CLI commands, or events are emitted directly by this module — it is a library consumed by the application layer.

## Data Models

DB-row types are defined in `statements.ts` and converted to/from domain types in `sqliteSerializer.ts`. See [data-model](../data-model.md) for full domain shapes. Tables (`schema.ts:17-129`):

- `jobs` — `id TEXT PK`, `queue`, `data BLOB` (msgpack), `priority`, `created_at`, `run_at`, `started_at`, `completed_at`, `attempts`, `max_attempts`, `backoff`, `ttl`, `timeout`, `unique_key`, `custom_id`, `depends_on BLOB`, `parent_id`, `children_ids BLOB`, `tags BLOB`, `state` (default `'waiting'`), `lifo`, `group_id`, `progress`, `progress_msg`, `remove_on_complete`, `remove_on_fail`, `stall_timeout`, `last_heartbeat`, `timeline BLOB`, `stacktrace BLOB`.
- `job_results` — `job_id TEXT PK`, `result BLOB`, `completed_at`.
- `dlq` — `id INTEGER PK AUTOINCREMENT`, `job_id`, `queue`, `entry BLOB` (full `DlqEntry`), `entered_at`. Note `dlq` is append-only by `id`; a single `job_id` can have multiple rows.
- `cron_jobs` — keyed by `name`, includes `dedup BLOB`, `skip_missed_on_restart`, `skip_if_no_worker`, `prevent_overlap` (default 1), `job_options BLOB`.
- `queue_state` (#100) — `name TEXT PK`, `paused`, `rate_limit`, `concurrency_limit`.
- `migrations` — `version PK`, `applied_at`.

All blob columns store MessagePack. `rowToJob` (`sqliteSerializer.ts:71-151`) rehydrates a full `Job`, defaulting BullMQ-compat fields that are intentionally not persisted (`backoffConfig`, `stackTraceLimit`, dedup/debounce flags, etc.).

## Business Logic / Control Flow

Buffered write (default path):
1. `insertJob(job)` → `writeBuffer.add(job)` pushes onto the active buffer (`sqlite.ts:287-293`, `sqliteBatch.ts:230-235`).
2. The buffer auto-flushes every `writeBufferFlushMs` (10ms) via `setInterval`, or immediately when `activeBuffer.length >= bufferSize` (`sqliteBatch.ts:217-245`).
3. `flush()` does an atomic double-buffer swap (active → flush buffer, new active = []) under a `flushing` guard, then calls `BatchInsertManager.insertJobsBatch` (`sqliteBatch.ts:257-337`).
4. `BatchInsertManager` runs one transaction of multi-row `INSERT`s chunked at `MAX_ROWS_PER_INSERT = floor(999/24) = 41` rows (SQLite ~999 bound variables, `COLS_PER_ROW=24`); prepared statements are cached per chunk size for sizes 1–100 (`sqliteBatch.ts:55-128`). The multi-row `INSERT` is an upsert (`ON CONFLICT(id) DO UPDATE`), so a colliding `id` overwrites the existing row in place instead of throwing `UNIQUE` and failing the whole flush.

Durable write (`durable: true`): bypasses the buffer entirely. `insertJobImmediate` runs the single prepared `insertJob` statement under `safeWrite`; `insertJobsBatch(jobs, true)` runs the whole batch inside one explicit `db.transaction` so a mid-batch failure rolls back all rows (`sqlite.ts:296-300`, `sqlite.ts:589-603`). This is the lower-throughput, no-loss mode.

State transitions and the flush-before-update invariant: `markActive`/`markCompleted`/`markFailed` first call `flushIfBuffered(jobId)` (`sqlite.ts:341-352`). If the row's INSERT is still buffered, the `UPDATE` would match 0 rows and the later buffered INSERT would overwrite the state change with the original `waiting`/`delayed` state. `flushIfBuffered` checks `writeBuffer.hasPending(id)` and flushes first.

Delete: `deleteJob` calls `writeBuffer.removePending(id)` so a still-buffered INSERT cannot resurrect a deleted row, then deletes the `jobs` row and `job_results` row in one transaction (atomic cascade, issue #84). DLQ rows are deliberately not cascaded — `moveFailedJobToDlq` writes the DLQ entry then `deleteJob`, keeping the DLQ row (`sqlite.ts:452-469`).

Recovery (consumer side, `backgroundTasks.ts:227-434`): `recover()` reads `loadCompletedJobIds()` + `loadDlqJobIds()`, then paginates `loadActiveJobs` (Phase 1: stalled → retry-with-backoff or DLQ; cron `preventOverlap` rows and DLQ-duplicate rows are dropped), `loadPendingJobs` (Phase 2: enqueue ready, park unmet deps in `waitingDeps`, restore `customId`/`uniqueKey` dedup), `loadDlq` (restore DLQ), `loadQueueState` (re-apply pause/rate/concurrency — in-memory only, no write-back loop), and `loadCompletedJobs` capped at `maxCompletedJobs` (Phase 3, issue #84).

Migrations (`sqlite.ts:255-278`): on construct, ensures `migrations` table, reads `MAX(version)`; if below `SCHEMA_VERSION` (13) it runs `SCHEMA` (idempotent `CREATE ... IF NOT EXISTS` / indexes) then applies each incremental `MIGRATIONS[v]` for `v > current && v > 1`, swallowing errors where a column/index already exists, and records the new version. Migration 13 adds the `stacktrace` blob (server-side failure stack, issue #74).

Close (`sqlite.ts:818-840`): stop the buffer timer, final `flush()`, `PRAGMA wal_checkpoint(TRUNCATE)` to avoid stale WAL locks on restart, then `db.close()`. Flush and WAL checkpoint are wrapped for logging; `writeBuffer.stop()` and `db.close()` are not wrapped and will abort on error.

## Concurrency & Locking

This module is not lock-coordinated with the shard locks documented in [Concurrency & Locking](./concurrency-and-locking.md) — it relies on the single-threaded JS event loop plus SQLite's own locking:
- `PRAGMA busy_timeout = 5000` lets SQLite wait out a busy lock rather than failing immediately (`schema.ts:13`).
- `WriteBuffer.flushing` is a re-entrancy guard: a `flush()` while another is in progress returns 0 immediately (`sqliteBatch.ts:259`). The auto-flush timer also skips while a backoff retry is pending (`sqliteBatch.ts:219`).
- The double-buffer swap (`activeBuffer` ↔ `flushBuffer`) is the atomicity primitive: new `add()`s land in a fresh active buffer while the snapshot is written, so no job is written twice or missed across a flush boundary.
- All mutations are wrapped in `safeWrite()` which toggles the disk-full flag and re-throws.

## Edge Cases & Failure Modes

- **Per-row constraint isolation (#92 class).** If the atomic batch fails on a constraint (a duplicate `jobs.id` is absorbed in place by the `ON CONFLICT(id) DO UPDATE` upsert, but e.g. a `NOT NULL` violation still throws), `BatchInsertManager` falls back to one-INSERT-per-row, classifying each failure: `SQLITE_CONSTRAINT*` / "constraint failed" → `conflicts` (permanent, dropped and reported, **never** re-buffered — re-buffering would poison every future flush); everything else → `transient` (re-buffered + retried). Valid sibling rows still persist (`sqliteBatch.ts:21-89`, `sqliteBatch.ts:289-327`).
- **Exponential backoff + critical loss.** Transient failures re-buffer the failed jobs (prepended) and retry with exponential backoff: base 100ms, doubled before each retry (so the first retry fires after 200ms), capped at 30000ms, max 10 retries (`sqliteBatch.ts:195-202`, `scheduleBackoffRetry` `sqliteBatch.ts:340-357`). On exhaustion, jobs are handed to `onCriticalError` → `handleCriticalLoss` (`sqlite.ts:131-186`): every lost job is logged with a truncated 500-char data preview, retained in a bounded list (`MAX_RETAINED_LOSSES = 100`, FIFO), and **persisted to the DLQ table** via `saveDlqEntry` (direct prepared statement, not through the failing buffer, so it cannot recurse), then forwarded to the user `onCriticalLoss` callback (wrapped in try/catch).
- **Disk full.** `isSqliteFullError` matches `SQLITE_FULL` / "database or disk is full"; `setDiskFull` logs once and exposes `diskFull` / `getDiskFullStatus()`; the flag auto-clears on the next successful `safeWrite` (`sqlite.ts:48-53`, `sqlite.ts:208-234`).
- **PRAGMA failure is non-fatal.** Applying `PRAGMA_SETTINGS` is wrapped in try/catch because a transient `SQLITE_IOERR_FSTAT` (e.g. `mmap_size` calling `fstat` during a restart/cleanup race) must not escape the constructor and tear down the process (`sqlite.ts:78-84`).
- **Corrupt `depends_on` blob.** A failed msgpack decode of `depends_on` is NOT collapsed to `[]` (which recovery would treat as "ready, no deps" → out-of-order execution). `decodeDependsOn` returns `corrupt: true` and `rowToJob` stamps the job with the non-enumerable `CORRUPT_DEPENDS_ON` symbol; recovery (`isCorruptDependsOn`) routes it to the DLQ instead (`sqliteSerializer.ts:42-68`, `sqliteSerializer.ts:138-148`).
- **Lossy decode fallback.** `unpack` logs and returns the provided fallback on decode error rather than throwing, so a single corrupt blob does not abort a whole load (`sqliteSerializer.ts:19-27`).
- **Id type round-trip.** `brandId` preserves non-string id types without conversion (msgpackr can round-trip bigint), preserving id equality on the critical-loss → DLQ → restart path (`sqliteSerializer.ts:162-164`).
- **Completed-without-result.** A job acked with no result has `state='completed'` but no `job_results` row; `loadCompletedJobIds` unions both sources so dependency recovery still unblocks dependents (`sqlite.ts:568-579`).
- **Shutdown loss reporting.** `WriteBuffer.stop()` / `stopGracefully(timeoutMs=5000)` flush remaining jobs and report anything still buffered via `reportLostJobs` → `onCriticalError`, so nothing is silently dropped on shutdown (`sqliteBatch.ts:390-486`).
- **Memory bounds.** Recovery loads are paginated (`limit/offset`, default batch 10000) to avoid memory spikes; completed-job recovery is capped at `maxCompletedJobs`. Retained critical-loss records are capped at 100.

## Configuration

- **Data path** (resolution order, `config/resolve.ts:44-49`): `fileConfig.storage.dataPath` → `BUNQUEUE_DATA_PATH` → `BQ_DATA_PATH` → `DATA_PATH` → `SQLITE_PATH`. If unset, `QueueManager` runs with `storage = null` (pure in-memory, no persistence). Programmatic: `new Queue('q', { embedded: true, dataPath: './data/q.db' })`.
- **PRAGMAs** (`schema.ts:6-14`, fixed at startup): `journal_mode=WAL`, `synchronous=NORMAL`, `cache_size=-64000` (~64 MB), `temp_store=MEMORY`, `mmap_size=268435456` (256 MB), `page_size=4096`, `busy_timeout=5000`.
- **WriteBuffer**: `writeBufferSize` (default 100 rows → force flush), `writeBufferFlushMs` (effective default 10 ms). Throughput/durability trade-off: buffered ≈ up to 10 ms of loss on crash; `durable: true` job option bypasses the buffer for zero-loss writes at lower throughput.
- **`onCriticalLoss`** callback for surfacing dropped jobs to ops tooling.
- Indexes maintained by the schema: `idx_jobs_queue_state`, partial `idx_jobs_run_at WHERE state IN ('waiting','delayed')`, `idx_jobs_unique`, `idx_jobs_custom_id`, `idx_jobs_parent`, `idx_jobs_state_started`, `idx_jobs_group_id`, `idx_jobs_pending_priority`, `idx_jobs_completed_order`, and `dlq` indexes on `queue`/`job_id`/`entered_at`.

## Related Docs

- [architecture](../architecture.md) — where persistence sits in the request/recovery flow.
- [data-model](../data-model.md) — full `Job`, `CronJob`, `DlqEntry` definitions.
- [Core Queue Engine](./core-queue-engine.md) — the in-memory state hydrated from these loads.
- [Background Tasks](./background-tasks.md) — `recover()` and cleanup that drive these read/write APIs.
- [Dead Letter Queue](./dead-letter-queue.md) — DLQ policy over the rows persisted here.
- [Job Lifecycle](./job-lifecycle.md) — push/pull/ack/fail that trigger the state writes.
- [Scheduler & Cron](./scheduler-and-cron.md) — consumer of the `cron_jobs` table.
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md) — owner of the control-state persisted in `queue_state` (#100).
- [S3 Backup](./backup-s3.md) — file-level backup of the SQLite database.
- [Configuration & Entrypoint](./configuration.md) — data-path resolution and env vars.
