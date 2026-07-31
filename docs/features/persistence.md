# Persistence (SQLite, WriteBuffer, ReadThrough)

> **Category:** Infrastructure · **Source:** `src/infrastructure/persistence/sqlite.ts`, `src/infrastructure/persistence/sqliteBatch.ts`, `src/infrastructure/persistence/schema.ts`, `src/infrastructure/persistence/dependencyCompletionStore.ts`, `src/infrastructure/persistence/dependencyCompletionSchema.ts`, `src/infrastructure/persistence/statements.ts`, `src/infrastructure/persistence/sqliteSerializer.ts`

## Purpose

The persistence layer is the durable store that backs the in-memory sharded queues. It wraps Bun's native `bun:sqlite` (`Database`) in WAL mode, batches inserts through a double-buffered `WriteBuffer` for high throughput, serializes payloads with MessagePack (`msgpackr`) for ~2–3x smaller/faster blobs than JSON, and exposes batched recovery queries used to rebuild in-memory state on restart. It exists so that jobs, results, DLQ entries, cron definitions, and per-queue control-state survive a process restart without forcing every write onto the hot path.

## Responsibilities & Scope

Owns:
- The SQLite `Database` handle, PRAGMA tuning, schema creation, and incremental migrations (`sqlite.ts:71-121`, `sqlite.ts:255-278`).
- Buffered and durable (immediate) job inserts, single and bulk (`insertJob`, `insertJobImmediate`, `insertJobsBatch`).
- State-mutating writes: `markActive`, `markCompleted`, `markFailed`, `updateForRetry`, `updateJobData`, `updateJobChildrenIds`, `deleteJob`.
- Results, DLQ rows, cron rows, queue control-state rows, the durable
  flow-failure outbox, and `removeOnComplete` dependency evidence with bounded
  recent retention plus live-edge pin ownership.
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
- Sibling modules in this folder: `schema.ts`, `statements.ts`,
  `sqliteSerializer.ts`, `sqliteBatch.ts`, `dependencyCompletionSchema.ts`, and
  `dependencyCompletionStore.ts`.

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
- Flow transactions: `commitFailedJob(jobId, dlqEntry, flowFailure)`,
  `updateFlowLink(child, parent, state)`, `removeFlowLink(child, parent, state)`,
  `updateFlowParentResolution(parent)`, `saveFlowFailure(record)`,
  `loadFlowFailures()`, `deleteFlowFailure(parentId, childId?)`.
- Removed-completion evidence:
  `commitRemovedCompletion(job, retentionLimit, completedAt?)`,
  `loadDependencyCompletions(retentionLimit)`,
  `deleteDependencyCompletion(jobId)`, and
  `deleteDependencyCompletionsForQueue(queue)`.
- Results: `storeResult(jobId, result)`, `getResult(jobId)`, `hasResult(jobId)`.
- DLQ: `saveDlqEntry(entry)`, `deleteDlqEntry(jobId)`, `clearDlqQueue(queue)`, `loadDlq(): Map<string, DlqEntry[]>`, `getDlqEntry(jobId)`, `hasDlqEntry(jobId)`, `loadDlqJobIds(): Set<JobId>`.
- Queries: `getJob(id)`, `getJobStateRaw(jobId)`, `queryJobs(queue, {state|states, limit, offset, asc})`.
- Recovery loads: `loadPendingJobs(limit=10000, offset=0)`, `loadActiveJobs(limit=10000, offset=0)`, `loadCompletedJobs(limit=10000, offset=0)`, `loadCompletedJobIds(): Set<JobId>`, `countPendingJobs()`, `countActiveJobs()`.
- Cron: `saveCron(cron)`, `loadCronJobs(): CronJob[]`, `deleteCron(name)`, `updateCron(name, executions, nextRun)`.
- Queue control-state (#100): `saveQueueState(name, paused, rateLimit, concurrencyLimit)`, `loadQueueState()`, `deleteQueueState(name)`.
- Health/ops: `flushWriteBuffer(): number`, `get diskFull`,
  `getDiskFullStatus()`, `getCriticalLosses()`, `clearCriticalLosses()`,
  `getSize()`, `close()`. `flushWriteBuffer` throws if a flush attempt leaves
  any row pending; S3 backup uses that fail-closed boundary before snapshotting.

`sqliteBatch.ts` exports:
- `class BatchInsertManager` — `insertJobsBatch(jobs: Job[]): BatchInsertResult` (never throws).
- `class WriteBuffer` — `add`, `addBatch`, `flush(): number`, `hasPending(id)`, `removePending(id)`, `pendingCount`, `stop()`, `stopGracefully(timeoutMs=5000): Promise<number>`, `getRetryState()`.
- Types: `BatchInsertResult { transient: Job[]; conflicts: Job[]; error?: Error }`, `WriteBufferErrorCallback`, `CriticalErrorCallback`.

`sqliteSerializer.ts` exports:
- `pack(data): Uint8Array`, `unpack<T>(buffer, fallback, context): T`.
- `rowToJob(row: DbJob): Job`, `reconstructDlqEntry(entry: DlqEntry): DlqEntry`.
- `CORRUPT_DEPENDS_ON: symbol`, `isCorruptDependsOn(job): boolean`.

`schema.ts` exports: `PRAGMA_SETTINGS`, `SCHEMA`, `MIGRATION_TABLE`,
`SCHEMA_VERSION = 29`, `MIGRATIONS`.
`statements.ts` exports: `SQL_STATEMENTS`, `prepareStatements(db)`, `StatementName`, and DB-row types `DbJob`, `DbCron`, `DbQueueState`.

No TCP commands, HTTP endpoints, CLI commands, or events are emitted directly by this module — it is a library consumed by the application layer.

## Data Models

DB-row types are defined in `statements.ts` and converted to/from domain types in `sqliteSerializer.ts`. See [data-model](../data-model.md) for full domain shapes. Tables (`schema.ts:17-129`):

- `jobs` — `id TEXT PK`, `queue`, `data BLOB` (msgpack), `priority`, `created_at`, `run_at`, `started_at`, `completed_at`, `attempts`, `max_attempts`, `backoff`, `ttl`, `timeout`, `unique_key`, `custom_id`, `depends_on BLOB`, `parent_id`, `children_ids BLOB`, `tags BLOB`, `state` (default `'waiting'`), `lifo`, `group_id`, `progress`, `progress_msg`, `remove_on_complete`, `remove_on_fail`, `fail_parent_on_failure`, `remove_dependency_on_failure`, `continue_parent_on_failure`, `ignore_dependency_on_failure`, `stall_timeout`, `last_heartbeat`, `stall_count` (default 0), `timeline BLOB`, `stacktrace BLOB`.
- `flow_failures` — `(parent_id, child_id)` primary key plus child queue,
  failure mode/error and creation time. It is both the durable propagation
  outbox and the live store for ignored/continued child errors.
- `dependency_completions` — monotonic `sequence`, unique `job_id`, source
  `queue`, `completed_at`, and `pinned`; a payload-free proof for removed
  completed jobs. Unpinned rows are FIFO-bounded; pinned rows are owned by live
  waiting dependency edges.
- `job_results` — `job_id TEXT PK`, `result BLOB`, `completed_at`.
- `dlq` — `id INTEGER PK AUTOINCREMENT`, `job_id`, `queue`, `entry BLOB` (full `DlqEntry`), `entered_at`. Note `dlq` is append-only by `id`; a single `job_id` can have multiple rows.
- `cron_jobs` — keyed by `name`, includes `dedup BLOB`, `skip_missed_on_restart`, `skip_if_no_worker`, `prevent_overlap` (default 1), `job_options BLOB`.
- `queue_state` (#100) — `name TEXT PK`, `paused`, `rate_limit`,
  `concurrency_limit`, rate-window fields, nullable `stall_enabled` /
  `stall_interval` / `max_stalls` / `stall_grace_period`, and nullable
  `dlq_config BLOB` for the effective per-queue DLQ policy.
- `migrations` — `version PK`, `applied_at`.

All blob columns store MessagePack. Encoding and decoding use the canonical
`src/shared/msgpack.ts` codec. Its normal path remains msgpackr's fast decoder;
the rare `__proto__` path materializes maps with safe own-property definitions,
so arbitrary JSON keys remain distinct across restart without prototype
pollution. `rowToJob` rehydrates a full `Job`, defaulting BullMQ-compat fields
that are intentionally not persisted (`backoffConfig`, `stackTraceLimit`,
dedup/debounce flags, etc.).

## Business Logic / Control Flow

Buffered write (default path):
1. `insertJob(job)` → `writeBuffer.add(job)` pushes onto the active buffer (`sqlite.ts:287-293`, `sqliteBatch.ts:230-235`).
2. The buffer auto-flushes every `writeBufferFlushMs` (10ms) via `setInterval`, or immediately when `activeBuffer.length >= bufferSize` (`sqliteBatch.ts:217-245`).
3. `flush()` does an atomic double-buffer swap (active → flush buffer, new active = []) under a `flushing` guard, then calls `BatchInsertManager.insertJobsBatch` (`sqliteBatch.ts:257-337`).
4. `BatchInsertManager` runs one transaction of multi-row `INSERT`s chunked at `MAX_ROWS_PER_INSERT = floor(999/29) = 34` rows (SQLite ~999 bound variables, `COLS_PER_ROW=29`); prepared statements are cached per chunk size for sizes 1–100 (`sqliteBatch.ts:55-128`). The multi-row `INSERT` is an upsert (`ON CONFLICT(id) DO UPDATE`), so a colliding `id` overwrites the existing row in place instead of throwing `UNIQUE` and failing the whole flush.

Durable write (`durable: true`): bypasses the buffer entirely. `insertJobImmediate` runs the single prepared `insertJob` statement under `safeWrite`; `insertJobsBatch(jobs, true)` runs the whole batch inside one explicit `db.transaction` so a mid-batch failure rolls back all rows (`sqlite.ts:296-300`, `sqlite.ts:589-603`). This is the lower-throughput, no-loss mode.

`PUSHF` always selects that immediate batch path when storage exists, regardless
of individual node `durable` flags. The jobs transaction completes before the
locked in-memory graph is published; a manager without `dataPath` has no
`SqliteStorage` and therefore provides atomic visibility without crash
durability.

Persistence normalizes an omitted `Job.stallCount` to `0` on single inserts,
buffered/batch inserts, retry updates, and row decoding. This preserves
compatibility with low-level integrations and legacy fixtures created before
the field existed while retaining the schema's
`stall_count INTEGER NOT NULL DEFAULT 0` invariant; current domain-created jobs
still always provide the field explicitly.

State transitions and the flush-before-update invariant:
`markActive`/`markCompleted`/`markFailed`, `updateJobData`,
`updateJobPriority`, `updateJobProgress`, and delay updates first call
`flushIfBuffered(jobId)` (`sqlite.ts`). If the row's INSERT is still buffered,
the `UPDATE` would match 0 rows and the later buffered INSERT would overwrite
the mutation. Progress persists the clamped value, effective message, and
heartbeat in one synchronous update. Priority persistence writes both
`priority` and the effective `lifo` tie-break so recovery reconstructs the same
heap order. `moveActiveToWait` also uses `updateRunAt`, clearing the persisted
active marker so restart recovery cannot treat a manual requeue as a crash.

External snapshot flush invariant: `flushWriteBuffer()` invokes the synchronous
buffer flush and then checks `pendingCount`. A storage error can re-buffer rows
for retry/backoff; returning only the number inserted would let a caller publish
a database snapshot while accepted jobs still existed only in memory. A
non-zero remainder therefore throws and aborts S3 backup before `VACUUM INTO`.

Delete: `deleteJob` calls `writeBuffer.removePending(id)` so a still-buffered INSERT cannot resurrect a deleted row, then deletes the `jobs` row and `job_results` row in one transaction (atomic cascade, issue #84). DLQ rows are deliberately not cascaded — `moveFailedJobToDlq` writes the DLQ entry then `deleteJob`, keeping the DLQ row (`sqlite.ts:452-469`).

Flow-failure transition: terminal child removal, its optional DLQ row and its
`flow_failures` record commit in one `commitFailedJob` transaction. Parent-side
link removal/promotion also updates both affected job rows transactionally.
Startup replays `loadFlowFailures()` before workers begin. `fail`/`remove`
records are deleted after idempotent application; `ignore`/`continue` records
remain until the live parent reaches a terminal/removal boundary so its worker
can still query the error map.

Removed-completion transition: `commitRemovedCompletion` first removes a
buffered insert for the ID, then one SQLite transaction inserts/updates the
payload-free proof, marks it pinned if a reverse waiter already exists, prunes
only unpinned rows through the monotonic FIFO boundary, and deletes the job,
result, and parent-side flow-failure rows. The ACK publishes the matching RAM
tier, counters, and events only after this transaction succeeds. Single ACK,
optimized ACKB (with or without results), and the late stall-ACK recovery path
all use this operation. A parent accepted after the child completed pins any
recent proof before registering its wait edges. `obliterate(queue)` removes
the source queue's hidden proofs; parent removal releases pin ownership.

Recovery (consumer side, `backgroundTasks.ts`): `recover()` first loads all
`dependency_completions` into temporary classification state without pruning.
It rebuilds pending parents and their reverse indexes, checkpoints ready
parents, then reconciles `pinned` from the authoritative indexes, prunes only
unreferenced rows, and hydrates the exact recent/pinned RAM tiers. This order
also survives a deployment that lowers `maxCompletedJobs`. A `job_results` row
alone is not completion evidence because a crash may leave it beside an
`active` job.
Recovery restores stall/DLQ policies before classifying interrupted active
jobs, then repeatedly reads active jobs from offset zero. Every handled active
row leaves `state='active'`, so advancing an offset over that shrinking result
would skip rows. Each interrupted active job increments `attempts` and
persisted `stall_count`; reaching either bound is terminal. Pending pages use
deterministic priority/run-at/id ordering. A persisted ready state
(`waiting`/`prioritized`/`delayed`) is an authoritative dependency checkpoint,
so bounded-proof eviction cannot regress a promoted parent. Later phases
restore the DLQ, limiter state, and bounded completed cache.

Migrations (`sqlite.ts:255-278`): on construct, ensures `migrations` table,
reads `MAX(version)`; if below `SCHEMA_VERSION` (29) it runs the idempotent base
schema and applies later migrations. Migration 17 adds `jobs.stall_count`;
migrations 18–21 persist the four `StallConfig` fields; migration 22 adds the
atomic MessagePack `queue_state.dlq_config` policy blob; migrations 23–26 add
the four flow failure-policy flags; migration 27 creates `flow_failures`, and
rebuilds the pending indexes so `prioritized` and `waiting-children` rows
participate in recovery, plus the outbox parent index. Migration 28 creates the
`dependency_completions` table and its queue-cleanup index; migration 29 adds
the conservative `pinned` ownership bit.

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
- **Completed-without-result and orphan results.** A retained job acked with no
  result has `state='completed'` but no `job_results` row, so
  `loadCompletedJobIds` uses the jobs state plus payload-free removed-completion
  proofs. It deliberately ignores a standalone result row: that row can be the
  first half of an interrupted legacy ACK while the job is still active.
- **Dependency-result read-through.** Application queries check the normal result LRU, then results protected for live dependency consumers, then `job_results`. SQLite therefore remains the durable fallback after either in-memory cache evicts; in memory-only mode the dependency tracker protects values only while a live edge requires them.
- **Shutdown loss reporting.** `WriteBuffer.stop()` / `stopGracefully(timeoutMs=5000)` flush remaining jobs and report anything still buffered via `reportLostJobs` → `onCriticalError`, so nothing is silently dropped on shutdown (`sqliteBatch.ts:390-486`).
- **Snapshot flush fails closed.** `flushWriteBuffer()` throws whenever
  `pendingCount` remains non-zero after its flush attempt. This is expected
  during storage retry/backoff and prevents an incomplete S3 recovery point
  from being published.
- **Memory bounds.** Recovery loads are paginated (default batch 10,000) to avoid memory spikes; active recovery drains offset zero because it mutates the scanned state, while non-mutating pending/completed loads advance stable pages. Completed-job recovery is capped at `maxCompletedJobs`. Retained critical-loss records are capped at 100.
- **Removed-completion bounds.** SQLite and the in-memory tracker retain at
  most `maxCompletedJobs` unpinned recent proofs. Proofs referenced by live
  `waitingDeps` edges remain pinned outside that FIFO cap until the final
  consumer is durably promoted or removed; interrupted unpins are reconciled
  on recovery. Rows contain no payload/result and remain invisible to normal
  job queries.
- **Eviction is not durable deletion.**
  `test/repro-retention-boundary-invariants.test.ts` runs with
  `maxCompletedJobs=3` and `maxJobResults=2`, completes twelve durable jobs,
  then verifies before and after restart that all states, payloads, and results
  still resolve through SQLite while the hot collections remain capped.

## Configuration

- **Data path** (resolution order, `config/resolve.ts:44-49`): `fileConfig.storage.dataPath` → `BUNQUEUE_DATA_PATH` → `BQ_DATA_PATH` → `DATA_PATH` → `SQLITE_PATH`. If unset, `QueueManager` runs with `storage = null` (pure in-memory, no persistence). Programmatic: `new Queue('q', { embedded: true, dataPath: './data/q.db' })`.
- **PRAGMAs** (`schema.ts:6-14`, fixed at startup): `journal_mode=WAL`, `synchronous=NORMAL`, `cache_size=-64000` (~64 MB), `temp_store=MEMORY`, `mmap_size=268435456` (256 MB), `page_size=4096`, `busy_timeout=5000`.
- **WriteBuffer**: `writeBufferSize` (default 100 rows → force flush), `writeBufferFlushMs` (effective default 10 ms). Throughput/durability trade-off: buffered ≈ up to 10 ms of loss on crash; `durable: true` job option bypasses the buffer for zero-loss writes at lower throughput.
- **`onCriticalLoss`** callback for surfacing dropped jobs to ops tooling.
- Indexes maintained by the schema: `idx_jobs_queue_state`, `idx_jobs_queue_created`, `idx_jobs_queue_state_created`, partial `idx_jobs_run_at WHERE state IN ('waiting','prioritized','waiting-children','delayed')`, `idx_jobs_unique`, `idx_jobs_custom_id`, `idx_jobs_parent`, `idx_jobs_state_started`, `idx_jobs_group_id`, `idx_jobs_pending_priority` over the same four pending states, `idx_jobs_completed_order`, and `dlq` indexes on `queue`/`job_id`/`entered_at`.

## Related Docs

- [architecture](../architecture.md) — where persistence sits in the request/recovery flow.
- [data-model](../data-model.md) — full `Job`, `CronJob`, `DlqEntry` definitions.
- [Core Queue Engine](./core-queue-engine.md) — the in-memory state hydrated from these loads.
- [Background Tasks](./background-tasks.md) — `recover()` and cleanup that drive these read/write APIs.
- [Dead Letter Queue](./dead-letter-queue.md) — DLQ policy over the rows persisted here.
- [Job Lifecycle](./job-lifecycle.md) — push/pull/ack/fail that trigger the state writes.
- [FlowProducer & Job Dependencies](./flow-producer.md) — atomic graph commits
  and recovery of the flow-failure outbox.
- [Scheduler & Cron](./scheduler-and-cron.md) — consumer of the `cron_jobs` table.
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md) — owner of the control-state persisted in `queue_state` (#100).
- [S3 Backup](./backup-s3.md) — file-level backup of the SQLite database.
- [Configuration & Entrypoint](./configuration.md) — data-path resolution and env vars.
