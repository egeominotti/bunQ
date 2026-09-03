# Persistence (SQLite, WriteBuffer, ReadThrough)

> **Category:** Infrastructure · **Source:** `src/infrastructure/persistence/sqlite.ts`, `src/infrastructure/persistence/sqlite/` (including `jobLifecycle.ts`, `completed.ts`, and `telemetryWrites.ts`), `src/infrastructure/persistence/types/`, `src/infrastructure/persistence/sqliteBatch.ts` (compatibility facade), `src/infrastructure/persistence/batchInsert.ts`, `src/infrastructure/persistence/writeBuffer.ts`, `src/infrastructure/persistence/writeBufferPending.ts`, `src/infrastructure/persistence/schema.ts`, `src/infrastructure/persistence/migrations.ts`, `src/infrastructure/persistence/sqliteMigration.ts`, `src/infrastructure/persistence/legacyNameMigration.ts`, `src/infrastructure/persistence/statements.ts`, `src/infrastructure/persistence/sqliteSerializer.ts`

## Purpose

This document covers the existing memory/SQLite engine, which remains unchanged
and is still the default for both embedded and server mode. The ten-line
`SqliteStorage` façade inherits focused lifecycle, job, mutation, query, flow,
control, state and record capabilities from `persistence/sqlite/`; storage
contracts are isolated in `persistence/types/`. It wraps Bun's native
`bun:sqlite` in WAL mode, batches inserts through `WriteBuffer`, serializes
payloads with MessagePack, and exposes recovery queries used to rebuild state
after restart. The optional server-only, database-authoritative backend is
documented separately in
[PostgreSQL 15–18 Multi-Broker Persistence](./postgres-multibroker.md).

## Responsibilities & Scope

Owns:

- The SQLite `Database` handle, PRAGMA tuning, schema creation, and incremental
  migrations (`persistence/sqlite/state.ts`, `schema.ts`).
- Buffered and durable (immediate) job inserts, single and bulk (`insertJob`,
  `insertJobImmediate`, `insertJobsBatch`), plus two-phase admission metadata
  that makes persistence-sensitive queue publication fail closed.
- State-mutating writes: scalar `markActive`/`markCompleted`, transactional
  `markActiveBatch`/`markCompletedBatch`, `markWaitingChildren`, `markFailed`,
  `updateForRetry`, `updateJobData`, `updateJobChildrenIds`, `clearJobUniqueKey`,
  and `deleteJob`.
- Results, DLQ rows, cron rows, queue control-state rows, the durable
  flow-failure outbox, and `removeOnComplete` dependency evidence with bounded
  recent retention plus live-edge pin ownership.
- SQLite-authoritative, deterministic completed-row cleanup plus exact retained
  completion counts maintained by lifecycle triggers.
- A bounded per-queue lifecycle journal, cumulative terminal metric metadata,
  and bounded one-minute completed/failed buckets. Scalar events use
  `SqliteTelemetry`; lifecycle batches use the same semantics through
  `telemetryWrites.ts` in one SQLite transaction.
- Paginated recovery reads (`loadPendingJobs`, `loadActiveJobs`, `loadCompletedJobs`, `loadDlq`, plus the id-set loaders).
- Disk-full detection, write-retry/backoff, and critical-loss accounting.
- MessagePack (de)serialization and DB-row ↔ `Job` conversion.

Does NOT own (delegated elsewhere):

- PostgreSQL transactions, schema, leases, or event replay — see
  [PostgreSQL 15–18 Multi-Broker Persistence](./postgres-multibroker.md). Selecting
  PostgreSQL does not instantiate this SQLite store.
- The recovery orchestration / re-enqueue logic — `recover()` in
  `application/background/recovery/index.ts` consumes these read APIs and decides
  what to enqueue, retry, or quarantine. See
  [Background Tasks](./background-tasks.md).
- Data-path resolution from env/file-config — [Configuration & Entrypoint](./configuration.md) (`config/resolve.ts:44-49`).
- In-memory queue/shard state, dedup maps, and counters — [Core Queue Engine](./core-queue-engine.md).
- DLQ policy (auto-retry, expiry) — [Dead Letter Queue](./dead-letter-queue.md). This layer only persists/loads the rows.
- S3 file backup of the `.db` — [S3 Backup](./backup-s3.md).
- A separate read-through cache object: there is no standalone cache class here. Reads hit prepared statements directly; the "ReadThrough" concept is realized as the in-memory collections owned by the [Core Queue Engine](./core-queue-engine.md), which are hydrated from these load APIs at boot and then served in memory.

## Dependencies

Internal:

- `domain/types/job` (`Job`, `JobId`, `JobTimelineEntry`), `domain/types/cron` (`CronJob`), `domain/types/dlq` (`DlqEntry`, `FailureReason`, `createDlqEntry`).
- `shared/logger` (`storageLog`).
- Sibling modules in this folder: `schema.ts`, `migrations.ts`, `statements.ts`,
  `sqliteSerializer.ts`, `batchInsert.ts`, `writeBuffer.ts`,
  `writeBufferPending.ts`, `sqlite/jobLifecycle.ts`,
  `dependencyCompletionSchema.ts`, `dependencyCompletionStore.ts`,
  `completedJobCountSchema.ts`, `migrationProgressSchema.ts`, and
  `sqliteMigration.ts`; guarded queue-owned auxiliary deletion SQL lives in
  `sqlite/queueDeletionSql.ts`.

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
  writeBufferSize?: number; // default: 100
  writeBufferFlushMs?: number; // constructor default: 10 (JSDoc says 50 — stale)
  onCriticalLoss?: SqliteCriticalLossCallback;
}
type SqliteCriticalLossCallback = (jobs: Job[], lastError: Error, attempts: number) => void;
```

`QueueManager` instantiates storage with only `{ path }` in
`application/queue-manager/state.ts`. `persistence/sqlite/state.ts` therefore
applies the effective defaults `writeBufferSize=100` and
`writeBufferFlushMs=10`.

Key methods (real signatures):

- Inserts:
  `insertJob(job: Job, durable?: boolean, admission?: DurableAdmissionMetadata): void`,
  `insertJobImmediate(job: Job, admission?: DurableAdmissionMetadata): void`,
  `insertJobsBatch(jobs: Job[], durable?: boolean): void`.
- Admission-sensitive replacement/linking:
  `replaceJob(oldJobId, newJob, durable?, admission?)`,
  `transferActiveDedupJob(oldJobId, newJob, durable?, admission?)`, and
  `commitFlowLink(child, parent, parentState, mode, admission?)`.
- State: `markActive(jobId, startedAt, timeline?)`,
  `markActiveBatch(updates: readonly ActiveJobWrite[])`,
  `markWaitingChildren(jobId, timeline?)`,
  `markCompleted(jobId, completedAt, timeline?)`,
  `markCompletedBatch(updates: readonly CompletedJobWrite[])`,
  `markFailed(job, error)`, `updateForRetry(job)`, `updateJobData(jobId, data)`,
  `updateRunAt(jobId, runAt)` (persists moveToDelayed/changeDelay, re-deriving
  `state` from `run_at`), `updateJobChildrenIds(jobId, childrenIds)`,
  `clearJobUniqueKey(jobId)`, `deleteJob(jobId)`.
- Queue destruction: `deleteJobsForQueue(queue, postDeleteReferences, limit)`
  transactionally deletes durable jobs/results, DLQ, flow failures, completion
  proofs, telemetry, and queue/group state, while reconciling proof pins to the
  post-obliterate dependency set. It then removes only that queue's pending
  buffered inserts and does not build an unbounded historical job-ID list.
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
- DLQ: `saveDlqEntry(entry)`, atomic `requeueDlqJob(job)`, `deleteDlqEntry(jobId)`, `clearDlqQueue(queue)`, transactional `purgeDlqEntries(queue, dlqJobIds, terminalJobIds, clearQueue)`, `loadDlq(): Map<string, DlqEntry[]>`, `getDlqEntry(jobId)`, `hasDlqEntry(jobId)`, `loadDlqJobIds(): Set<JobId>`.
- Queries: `getJob(id)`, `getJobStateRaw(jobId)`, `queryJobs(queue, {state|states, limit, offset, asc})`.
- Recovery loads: `loadPendingJobs(limit=10000, offset=0)`, `loadActiveJobs(limit=10000, offset=0)`, `loadCompletedJobs(limit=10000, offset=0)`, `loadCompletedJobIds(requested): Set<JobId>`, `loadCompletedQueueNames()`, `countPendingJobs()`, `countActiveJobs()`.
- Completed maintenance: `cleanCompletedJobs(queue, completedBefore, limit,
isProtected?)` returns committed `{ jobId, queue }` ownership records;
  `loadCompletedJobsForRetry(...)` keyset-pages cold retained rows;
  `countCompletedJobs()` and `countCompletedJobsByQueue(queueNames)` read exact
  trigger-maintained counts.
- Cron: `saveCron(cron)`, `loadCronJobs(): CronJob[]`, `deleteCron(name)`, `updateCron(name, executions, nextRun)`.
- Queue control-state (#100): `saveQueueState(name, { paused, rateLimit, concurrencyLimit, rateLimitDuration?, rateLimitExpiresAt?, stallConfig?, dlqConfig? })`, `loadQueueState()`, `deleteQueueState(name)` (`persistence/sqlite/control.ts:93-150`).
- Job-group control-state: `save/removeGroupRateLimit`,
  `save/removeGroupConcurrency`, `loadGroupStates()`, and
  `deleteGroupStates(queue)`. Rows whose last override is removed are deleted.
- Queue telemetry: `recordQueueEvent(event,maxEvents,maxMetricDataPoints)`,
  `recordQueueEventsBatch(events,maxEvents,maxMetricDataPoints)`,
  `getQueueMetrics(queue,type,maxMetricDataPoints)`,
  `trimQueueEvents(queue,maxLength)`, `countQueueEvents(queue)`, and
  `clearQueueTelemetry(queue)`.
- Health/ops: `flushWriteBuffer(): number`, `get diskFull`,
  `getDiskFullStatus()`, `getCriticalLosses()`, `clearCriticalLosses()`,
  `getSize()`, `close()`. `flushWriteBuffer` throws if a flush attempt leaves
  any row pending; S3 backup uses that fail-closed boundary before snapshotting.

`sqliteBatch.ts` is the stable compatibility facade. It re-exports
`BatchInsertManager` from `batchInsert.ts`, `WriteBuffer` from `writeBuffer.ts`,
and the batch callback/result types from `types/batch.ts`:

- `class BatchInsertManager` — `insertJobsBatch(jobs: Job[]): BatchInsertResult` (never throws).
- `class WriteBuffer` — `add`, `addBatch`, explicit `flush(): number`, backoff-aware `flushIfReady(): number`, `hasPending(id)`, `getPendingJob(s)`, `setPendingState`, `removePending(id)`, `pendingCount`, `stop()`, `stopGracefully(timeoutMs=5000): Promise<number>`, `getRetryState()`.
- Types: `BatchInsertResult { transient: Job[]; conflicts: Job[]; error?: Error }`, `WriteBufferErrorCallback`, `CriticalErrorCallback`.

`sqliteSerializer.ts` exports:

- `pack(data): Uint8Array`, `unpack<T>(buffer, fallback, context): T`.
- `rowToJob(row: DbJob): Job`, `reconstructDlqEntry(entry: DlqEntry): DlqEntry`.
- `persistedJobStateForWrite(job)` plus the buffer-only lifecycle override used
  when a job changes state before its delayed INSERT succeeds.
- `CORRUPT_DEPENDS_ON: symbol`, `isCorruptDependsOn(job): boolean`.

`schema.ts` exports: `PRAGMA_SETTINGS`, `SCHEMA`, `MIGRATION_TABLE`,
`SCHEMA_VERSION = 37`, `MIGRATIONS`.
`statements.ts` exports: `SQL_STATEMENTS`, `prepareStatements(db)`, `StatementName`, and DB-row types `DbJob`, `DbCron`, `DbQueueState`.

No TCP commands, HTTP endpoints, CLI commands, or events are emitted directly by this module — it is a library consumed by the application layer.

`DurableAdmissionMetadata` (`persistence/types/admission.ts`) carries only the
extra persisted ownership needed before a candidate may become visible:

```ts
interface DurableAdmissionMetadata {
  readonly retireGenerationId?: JobId;
  readonly completionPins?: readonly JobId[];
}
```

The type contains IDs, not mutable application objects. Heap membership,
`jobIndex`, counters, custom-ID ownership, dedup ownership, and dependency
indexes remain application-layer state and are published only after the
matching storage call returns successfully.

## Data Models

DB-row types are defined in `statements.ts` and converted to/from domain types in `sqliteSerializer.ts`. See [data-model](../data-model.md) for full domain shapes. Tables (`schema.ts:17-199`):

- `jobs` — `id TEXT PK`, `queue`, `name`, `data BLOB` (msgpack), `priority`, `created_at`, `run_at`, `started_at`, `completed_at`, `attempts`, `max_attempts`, `backoff`, `ttl`, `timeout`, `unique_key`, `custom_id`, `depends_on BLOB`, `parent_id`, `children_ids BLOB`, `tags BLOB`, `state` (default `'waiting'`), `lifo`, `group_id`, `progress`, `progress_msg`, `remove_on_complete`, `remove_on_fail`, the four child-failure flags, `stall_timeout`, `last_heartbeat`, `stall_count`, `timeline BLOB`, `stacktrace BLOB`, `dlq_retry_state BLOB`, `extended_options BLOB`. The options blob stores a hidden FIFO ordinal only for grouped jobs so restart cannot reorder equal-time/custom-ID lanes.
- `flow_failures` — `(parent_id, child_id)` primary key plus child queue,
  failure mode/error and creation time. It is both the durable propagation
  outbox and the live store for ignored/continued child errors.
- `dependency_completions` — monotonic `sequence`, unique `job_id`, source
  `queue`, `completed_at`, and `pinned`; a payload-free proof for removed
  completed jobs. Unpinned rows are FIFO-bounded; pinned rows are owned by live
  waiting dependency edges.
- `completed_job_counts` — one exact retained-completed count per queue,
  maintained by `jobs` insert/update/delete triggers. Empty queue rows are
  removed.
- `job_results` — `job_id TEXT PK`, `result BLOB`, `completed_at`.
- `dlq` — `id INTEGER PK AUTOINCREMENT`, `job_id`, `queue`, `entry BLOB` (full `DlqEntry`), `entered_at`. Note `dlq` is append-only by `id`; a single `job_id` can have multiple rows.
- `cron_jobs` — keyed by `name`, includes `dedup BLOB`, `skip_missed_on_restart`, `skip_if_no_worker`, `prevent_overlap` (default 1), `job_options BLOB`.
- `queue_state` (#100) — `name TEXT PK`, `paused`, `rate_limit`,
  `concurrency_limit`, rate-window fields, nullable `stall_enabled` /
  `stall_interval` / `max_stalls` / `stall_grace_period`, and nullable
  `dlq_config BLOB` for the effective per-queue DLQ policy.
- `group_state` — `(queue,group_id)` primary key plus nullable rate,
  duration, concurrency overrides, and durable pause state. Runtime fixed-window counters remain
  in memory; recovery restores the configuration before pulls begin. Embedded
  Queue group operations and grouped single/bulk admission always reopen the
  Queue's explicit `dataPath` after a shared-manager restart.
- `queue_events` — autoincrement sequence, queue, event type, job id,
  occurrence timestamp, and MessagePack payload. Append and per-queue bound
  enforcement run in one SQLite transaction. `PUSHB`, `PULLB`, and `ACKB`
  retain input event order while committing their journal entries together.
- `queue_metrics_meta` — `(queue,type)` cumulative terminal count and current
  bucket metadata. `queue_metric_buckets` — `(queue,type,minute)` counts,
  pruned to the configured minute window without resetting the cumulative
  total.
- `migrations` — `version PK`, `applied_at`.
- `migration_progress` — `(version,phase)` primary key plus the last key,
  processed row/byte counts, total rows, and timestamps for resumable data
  migrations. A row is deleted atomically when its migration version completes.

All blob columns store MessagePack. Encoding and decoding use the canonical
`src/shared/msgpack.ts` codec. Its normal path remains msgpackr's fast decoder;
the rare `__proto__` path materializes maps with safe own-property definitions,
so arbitrary JSON keys remain distinct across restart without prototype
pollution. `rowToJob` rehydrates a full `Job`, defaulting BullMQ-compat fields
that are intentionally not persisted (`backoffConfig`, `stackTraceLimit`,
dedup/debounce flags, etc.).

## Business Logic / Control Flow

Buffered write (default path):

1. `insertJob(job)` in `persistence/sqlite/jobLifecycle.ts` calls
   `writeBuffer.add(job)` (`persistence/writeBuffer.ts`).
2. The buffer auto-flushes every `writeBufferFlushMs` (10ms), or immediately
   when `activeBuffer.length >= bufferSize` (`persistence/writeBuffer.ts`).
3. A synchronous `pushBatch` defers threshold-triggered flushes across the
   complete admission loop, including nested deferrals, then performs at most
   one automatic threshold flush when the outer scope exits. Explicit
   lifecycle or snapshot operations remain separate flush boundaries.
   Sub-threshold jobs remain buffer-owned and are exposed to queries without
   forcing a write.
4. `flush()` atomically swaps the active and flush buffers under a reentrancy
   guard, then calls `BatchInsertManager.insertJobsBatch`.
5. `BatchInsertManager` (`persistence/batchInsert.ts`) runs one transaction of
   multi-row upserts chunked at `floor(999/38) = 26` rows and caches prepared
   statements for common chunk sizes.

Durable write (`durable: true`): bypasses the buffer entirely.
`insertJobImmediate` in `persistence/sqlite/jobLifecycle.ts` runs the prepared insert
under `safeWrite`; `insertJobsBatch(jobs, true)` in
`persistence/sqlite/records.ts` runs the whole batch inside one explicit
transaction so a mid-batch failure rolls back all rows.

Persistence-sensitive job admission is a two-phase operation coordinated by
`application/operations/pushAdmission.ts` and
`persistence/sqlite/admission.ts`:

1. The application inspects custom-ID, deduplication, dependency-completion and
   parent state without performing destructive RAM mutations.
2. SQLite runs the requested retirement and completion pins in the same
   immediate transaction as a durable insert, dedup replacement, active-key
   transfer, or parent-link insert/update.
3. A terminal retirement deletes the exact old generation from `jobs`,
   `job_results`, `dlq`, `dependency_completions`, and both sides of
   `flow_failures`. Duplicate completion-pin IDs are coalesced.
4. Only after the transaction commits does the application publish the new
   heap/wait-set membership, indexes, counters and ownership maps.

For a buffered successor, any retirement/pin metadata still commits
synchronously before the successor enters the normal `WriteBuffer`. A durable
successor commits metadata and its row atomically. Dedup replacement and
parent-link methods follow the same split. A synchronous storage error cannot
therefore leave an executable RAM-only durable job, remove the previous
completed/DLQ generation, leak a completion pin, or expose half a parent edge.

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

State transitions and the buffered-lifecycle invariant:
`markActive`/`markWaitingChildren`/`markCompleted`/`markFailed`, `updateJobData`,
`updateJobPriority`, `updateJobProgress`, retry, flow-resolution, and delay
updates attempt `flushIfBuffered(jobId)`. Lifecycle methods first stamp an
explicit state on any pending buffer object. Automatic materialization uses
`flushIfReady`, so a scheduled retry backoff is never bypassed merely because
the job is pulled, ACKed, promoted, parked, or requeued. If the INSERT remains
buffered and the SQL `UPDATE` matches zero rows, its eventual upsert carries
the current state and execution fields rather than resurrecting the original
waiting state. This remains correct even after the bounded timeline is full.
Progress persists the clamped value, effective message, and
heartbeat in one synchronous update. Priority persistence writes both
`priority` and the effective `lifo` tie-break so recovery reconstructs the same
heap order. `moveActiveToWait` also uses `updateRunAt`, clearing the persisted
active marker so restart recovery cannot treat a manual requeue as a crash.
`markWaitingChildren` similarly records the dedicated buffered state, clears
`started_at`, and persists the transition timeline when a row is available. Explicit
deduplication-key release uses `clearJobUniqueKey` after its owner-aware
in-memory mutation so recovery cannot recreate the removed key.

`markActiveBatch` and `markCompletedBatch` stamp every pending input, make at
most one backoff-aware materialization attempt for the batch, then open one
transaction. A ten-job lifecycle operation therefore cannot consume ten retry
attempts during one outage. Timeline payloads are MessagePack
encoded before the transaction; its synchronous critical section only reuses
the prepared state statement for each row. `markCompletedBatch` also applies
the scalar completion side effects (`progress=100` and
`dlq_retry_state=NULL`). Any rejected row aborts the complete transaction. The
pull and retained/result-free ACK application paths then retry scalar writes;
pull keeps individual failures non-fatal, while ACK retains its historical
scalar error behavior. Empty batches are no-ops.

Lifecycle telemetry batching is separate from job-state durability. Batch
operations build an ordered `JobEvent` array and `EventsManager.broadcastBatch`
routes the manager-owned telemetry subscriber through
`QueueTelemetryJournal.recordBatch`; ordinary subscribers, completion waiters,
and webhooks still receive every event in order. `telemetryStore.ts` owns the
prepared statements and reusable write/clear transactions for the storage
lifetime; `telemetryWrites.ts` packs payloads before entering that transaction
and groups terminal work by `(queue, completed|failed)`. Exact per-queue event
counts avoid issuing a retention delete before the configured cap is crossed;
at overflow, one oldest-first limited delete removes precisely the excess.
Counts change only after commit and are refreshed or invalidated by explicit
trim, clear, and queue destruction. Zero event retention skips journal inserts
while terminal metrics remain active. The in-memory bucket simulation applies
events in their original per-group order before emitting aggregate increments
and one retention trim, so out-of-order timestamps, a zero metric window,
`prev_ts`/`prev_count`, and cumulative totals match scalar writes exactly. A
batch SQL failure rolls back atomically; the journal then retries events one by
one and isolates a rejected payload/row without suppressing later telemetry.

External snapshot flush invariant: `flushWriteBuffer()` invokes the synchronous
buffer flush and then checks `pendingCount`. A storage error can re-buffer rows
for retry/backoff; returning only the number inserted would let a caller publish
a database snapshot while accepted jobs still existed only in memory. A
non-zero remainder therefore throws and aborts S3 backup before `VACUUM INTO`.

Delete: `deleteJob` in `persistence/sqlite/mutations.ts` calls
`writeBuffer.removePending(id)` so a buffered insert cannot resurrect a deleted
row, then deletes the job and result atomically. DLQ rows are deliberately not
cascaded; terminal failure commits the DLQ entry before deleting the live job
row (`persistence/sqlite/jobs.ts`).

Queue destruction uses `deleteJobsForQueue`. Its synchronous transaction clears
results before jobs, removes `flow_failures` by stored `child_queue`, live-job
ownership, or queue-owned DLQ IDs, and deletes jobs, DLQ rows, completion proofs,
telemetry, and queue/group state together. DLQ-derived result/outbox deletion is
guarded when a persisted same-ID job or DLQ generation belongs to another
queue; job-derived auxiliary deletion applies the reciprocal DLQ guard. The
transaction also reconciles completion pins against dependencies that will survive the
obliterate. Only bounded buffer-owned IDs and the retained completion-proof
snapshot needed by RAM are returned; historical job IDs are never materialized.
The manager invokes this before any runtime mutation, so any SQLite failure
leaves memory unchanged and an obliterate retry is idempotent.

Permanent DLQ cleanup uses `purgeDlqEntries`. It first removes pending buffered
terminal inserts, then transactionally deletes either the selected
`(queue, jobId)` rows or the queue's complete DLQ together with terminal
`jobs`, `job_results`, and parent-owned `flow_failures`. The terminal-ID list is
generation-guarded by the application layer so cleanup of a stale DLQ snapshot
cannot delete a newer live generation with the same ID.

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

Recovery (consumer side, `application/background/recovery/index.ts`):
`recover()` loads payload-free `dependency_completions` into temporary
classification state without pruning. For each pending page it asks SQLite
only for the dependency IDs referenced by that page instead of materializing
every retained completed job ID. It rebuilds pending parents and their reverse
indexes, checkpoints ready parents, then reconciles `pinned` from the
authoritative indexes, prunes only unreferenced rows, and hydrates the exact
recent/pinned RAM tiers. This order also survives a deployment that lowers
`maxCompletedJobs`. A `job_results` row alone is not completion evidence because
a crash may leave it beside an `active` job. Structured start, phase-progress,
and completion diagnostics are emitted at debug level on stderr before listeners
bind, keeping normal command stdout machine-readable.
Recovery restores stall/DLQ policies before classifying interrupted active
jobs, then repeatedly reads active jobs from offset zero. Every handled active
row leaves `state='active'`, so advancing an offset over that shrinking result
would skip rows. Each interrupted active job increments `attempts` and
persisted `stall_count`; reaching either bound is terminal. Pending pages use
deterministic priority/run-at/id ordering. A persisted ready state
(`waiting`/`prioritized`/`delayed`) is an authoritative dependency checkpoint,
so bounded-proof eviction cannot regress a promoted parent. Later phases
restore the DLQ, limiter state, and bounded completed cache.

A pending row already marked `waiting-children` is restored into the parked
collection even when it was moved there manually and has no unresolved
`depends_on` entries. It is never inserted into the runnable priority heap as a
side effect of restart.

Standalone `Queue.add({ parent })` uses `commitFlowLink`: after flushing any
buffered parent generation, one immediate SQLite transaction inserts (or
dedup-replaces) the child and updates the parent's `children_ids`, `depends_on`,
data, timeline, and `state='waiting-children'`. The transaction verifies that
the parent row still exists before commit. In-memory heap/index changes happen
only afterward while the same child/parent shard locks remain held, so restart
cannot recover an orphaned child or a runnable parent with an unpersisted edge.

Migrations (`persistence/sqliteMigration.ts`, `persistence/schema.ts`): on
construct, storage first rejects a recorded version newer than
`SCHEMA_VERSION = 37`, then creates the bookkeeping tables, reads
`MAX(version)`, and idempotently ensures the complete base schema so partial
historical databases remain upgradeable. Each pending schema version commits
and receives its own marker before the next begins. Only exact
duplicate-column/table/index/trigger errors are treated as an already-applied
schema operation; disk-full, I/O, corruption, syntax, constraint, and other
failures abort the current step without marking it complete. Reopening retries
that version. Multi-statement migration 6
retains explicit statement boundaries so a historically partial upgrade can
skip its existing column and still add the missing one. Migration 17 adds
`jobs.stall_count`;
migrations 18–21 persist the four `StallConfig` fields; migration 22 adds the
atomic MessagePack `queue_state.dlq_config` policy blob; migrations 23–26 add
the four flow failure-policy flags; migration 27 creates `flow_failures`, and
rebuilds the pending indexes so `prioritized` and `waiting-children` rows
participate in recovery, plus the outbox parent index. Migration 28 creates the
`dependency_completions` table and its queue-cleanup index; migration 29 adds
the conservative `pinned` ownership bit. Migration 30 adds the MessagePack
`jobs.dlq_retry_state` used to retain one bounded automatic-DLQ-retry generation
while its job is waiting or active. Migrations 31–32 separate job names from
user data; their backfills use keyset checkpoints and transactions bounded to
500 rows or 8 MiB of source payload, with one individually oversized row
processed alone, and structured row/byte progress at least every five seconds.
Migration start, progress, and completion records keep info severity but are
written to stderr, so a long upgrade is visible without contaminating stdout.
The cursor is committed with each batch, so interruption rolls forward by
restarting the same or a newer binary. Migration 33 creates
the event/metrics journal tables, migration 34 adds `jobs.extended_options` so
repeat-chain and advanced job policies survive recovery, migrations 35–36 add
durable per-group override/pause state, and migration 37 adds exact completed
counts plus deterministic retention and recovery indexes. Listeners bind after migration
and recovery; they do not serve a temporary HTTP 503 during this synchronous
startup phase. Downgrade after any committed payload rewrite is unsupported;
restore the pre-upgrade backup or roll forward.

If any post-storage initialization step throws, manager initialization runs all
registered cleanup in reverse order: events, workers, cron/timeout/interval
owners, telemetry memory, and SQLite are stopped or closed before the original
error is rethrown. Invalid manager configuration and structurally corrupt data
therefore fail startup promptly instead of leaving the process alive behind an
unbound listener.

Close (`persistence/sqlite/lifecycle.ts`): stop the buffer timer, perform a
final flush, run `PRAGMA wal_checkpoint(TRUNCATE)` to avoid stale WAL locks on
restart, then close the database. Flush and checkpoint failures are logged.

## Concurrency & Locking

This module is not lock-coordinated with the shard locks documented in [Concurrency & Locking](./concurrency-and-locking.md) — it relies on the single-threaded JS event loop plus SQLite's own locking:

- `PRAGMA busy_timeout = 5000` lets SQLite wait out a busy lock rather than failing immediately (`schema.ts:13`).
- `WriteBuffer.flushing` is a reentrancy guard: a concurrent `flush()` returns
  zero. The timer also skips while a backoff retry is pending
  (`persistence/writeBuffer.ts`).
- Threshold additions, nested deferral exit, and lifecycle-triggered flushes
  use the same backoff-aware gate. Only explicit administrative flushes such as
  snapshot/shutdown attempts may bypass the scheduled delay. A successful
  explicit flush cancels the obsolete retry timer so automatic flushing resumes
  immediately; retry exhaustion and removal of the last buffered job clear it
  for the same reason.
- The double-buffer swap (`activeBuffer` ↔ `flushBuffer`) is the atomicity primitive: new `add()`s land in a fresh active buffer while the snapshot is written, so no job is written twice or missed across a flush boundary.
- All mutations are wrapped in `safeWrite()` which toggles the disk-full flag and re-throws.

## Edge Cases & Failure Modes

- **Per-row constraint isolation (#92 class).** If an atomic batch fails,
  `BatchInsertManager` (`persistence/batchInsert.ts`) retries rows individually,
  classifying constraint failures as permanent conflicts and all other failures
  as transient. Valid siblings still persist.
- **Exponential backoff + critical loss.** `persistence/writeBuffer.ts`
  prepends transient failures and retries from 100ms up to 30s, at most ten
  times. On exhaustion, `handleCriticalLoss` in
  `persistence/sqlite/state.ts` logs and retains at most 100 records, persists
  each job to the DLQ, then invokes the guarded user callback.
- **Disk full.** `isSqliteFullError`, `setDiskFull`, and `safeWrite` live in
  `persistence/sqlite/state.ts`; a successful write clears the health flag.
  Immediate durable admission is fail closed: a rejected single or batch job
  is absent from the heap, wait sets, `jobIndex`, custom-ID/dedup maps, queries,
  and worker delivery in Embedded and TCP modes. Rejected reuse preserves the
  previous completed/DLQ generation and result across restart. Buffered jobs
  retain their documented acknowledgement-before-flush contract; a later
  asynchronous flush failure is surfaced through storage health, retries and
  critical-loss reporting.
- **PRAGMA failure is non-fatal.** `SqliteState` catches and logs transient
  PRAGMA errors instead of letting construction tear down the process.
- **Corrupt `depends_on` blob.** A failed msgpack decode of `depends_on` is NOT collapsed to `[]` (which recovery would treat as "ready, no deps" → out-of-order execution). `decodeDependsOn` returns `corrupt: true` and `rowToJob` stamps the job with the non-enumerable `CORRUPT_DEPENDS_ON` symbol; recovery (`isCorruptDependsOn`) routes it to the DLQ instead (`sqliteSerializer.ts:42-68`, `sqliteSerializer.ts:138-148`).
- **Lossy decode fallback.** `unpack` logs and returns the provided fallback on decode error rather than throwing, so a single corrupt blob does not abort a whole load (`sqliteSerializer.ts:19-27`).
- **Id type round-trip.** `brandId` preserves non-string id types without conversion (msgpackr can round-trip bigint), preserving id equality on the critical-loss → DLQ → restart path (`sqliteSerializer.ts:162-164`).
- **Completed-without-result and orphan results.** A retained job acked with no
  result has `state='completed'` but no `job_results` row, so
  `loadCompletedJobIds` uses the jobs state plus payload-free removed-completion
  proofs. It deliberately ignores a standalone result row: that row can be the
  first half of an interrupted legacy ACK while the job is still active.
- **Dependency-result read-through.** Application queries check the normal result LRU, then results protected for live dependency consumers, then `job_results`. SQLite therefore remains the durable fallback after either in-memory cache evicts; in memory-only mode the dependency tracker protects values only while a live edge requires them.
- **Shutdown loss reporting.** `WriteBuffer.stop()` /
  `stopGracefully(timeoutMs=5000)` flush remaining jobs and report anything
  still buffered through `onCriticalError` (`persistence/writeBuffer.ts`).
- **Snapshot flush fails closed.** `flushWriteBuffer()` throws whenever
  `pendingCount` remains non-zero after its flush attempt. This is expected
  during storage retry/backoff and prevents an incomplete S3 recovery point
  from being published.
- **Buffered reads are immediate and non-destructive.** `getBufferedJobs(queue)`
  exposes both active and in-flight buffer ownership for that queue.
  SQLite-backed queries resolve each pending ID through current `jobIndex`,
  heap/wait/processing/completed membership, exclude any stale persisted copy,
  then merge and paginate once. Completed buffer rows evicted from the bounded
  hot cache remain reachable by ID and keep their queue registered. A read
  never flushes the buffer.
- **Memory bounds.** Recovery loads are paginated (default batch 10,000) to avoid memory spikes; active recovery drains offset zero because it mutates the scanned state, while non-mutating pending/completed loads advance stable pages. Pending dependency completion checks are scoped to each page. Completed-job recovery is capped at `maxCompletedJobs`. Retained critical-loss records are capped at 100.
- **Removed-completion bounds.** SQLite and the in-memory tracker retain at
  most `maxCompletedJobs` unpinned recent proofs. Proofs referenced by live
  `waitingDeps` edges remain pinned outside that FIFO cap until the final
  consumer is durably promoted or removed; interrupted unpins are reconciled
  on recovery. Rows contain no payload/result and remain invisible to normal
  job queries.
- **Hot-cache eviction is not durable deletion.**
  `test/repro-retention-boundary-invariants.test.ts` runs with
  `maxCompletedJobs=3` and `maxJobResults=2`, completes twelve durable jobs,
  then verifies before and after restart that all states, payloads, and results
  still resolve through SQLite while the hot collections remain capped.
  Durable deletion is instead explicit through SQLite-authoritative
  `clean('completed')` or opt-in `completedRetentionMs` cleanup. Both scan
  oldest-first with `(completed_at/created_at,id)` keyset ordering and protect
  results still owned by live dependency consumers. The in-memory fallback
  compares Unicode code points in SQLite `BINARY` UTF-8 order for equal
  timestamps rather than depending on JavaScript UTF-16 or the host locale.
  Job/custom IDs containing isolated UTF-16 surrogates are rejected before
  admission because Bun SQLite cannot round-trip such TEXT values losslessly.
- **Generation-safe completed cleanup and queue destruction.** Each completed
  deletion returns its persisted queue owner before in-memory convergence.
  Result/log owner maps and the current `jobIndex` are checked before deleting
  a projection, so cleanup or obliteration of an old row cannot erase a newer
  same-ID job or DLQ generation in another queue. The owner maps share their
  payload LRU eviction and therefore remain bounded. Completed cleanup also
  preserves result/outbox rows whenever any same-ID DLQ generation survives,
  including one in the cleaned queue. Cold custom-ID reuse asks
  SQLite or the current write buffer for an evicted completed generation and
  retires its pending row and stale result in the same admission boundary as
  the successor.
- **Cold completed retry.** Bulk retry reads at most 500 completed rows per
  oldest-first `(retained timestamp,id)` keyset page, and a specific-ID retry
  uses a primary-key SQLite lookup constrained by `state='completed'`. Completion
  state, not nullable `completed_at`, is authoritative; legacy rows use
  `created_at` for ordering. Each durable completed-to-waiting transition commits
  before the waiting heap and indexes are published.
- **Trigger-aware mutation counts.** Bun's SQLite `changes` result includes the
  counter-table writes performed by completion triggers. Completed-to-waiting
  retry therefore treats zero changes as not found instead of requiring exactly
  one total change; the job update and old-result deletion remain one
  transaction.

## Configuration

- **Data path** (resolution order, `config/resolve.ts:44-49`): `fileConfig.storage.dataPath` → `BUNQUEUE_DATA_PATH` → `BQ_DATA_PATH` → `DATA_PATH` → `SQLITE_PATH`. If unset, `QueueManager` runs with `storage = null` (pure in-memory, no persistence). Programmatic: `new Queue('q', { embedded: true, dataPath: './data/q.db' })`.
- **PRAGMAs** (`schema.ts:6-14`, fixed at startup): `journal_mode=WAL`, `synchronous=NORMAL`, `cache_size=-64000` (~64 MB), `temp_store=MEMORY`, `mmap_size=268435456` (256 MB), `page_size=4096`, `busy_timeout=5000`.
- **WriteBuffer**: `writeBufferSize` (default 100 rows → force flush), `writeBufferFlushMs` (effective default 10 ms). Throughput/durability trade-off: buffered ≈ up to 10 ms of loss on process crash; `durable: true` bypasses that application buffer and commits before return at lower throughput. SQLite runs with `synchronous=NORMAL`, so power-loss durability still depends on the host, filesystem, and storage device.
- **`onCriticalLoss`** callback for surfacing dropped jobs to ops tooling.
- **Completed retention:** `maxCompletedJobs` (default `50_000`) bounds only the
  hot cache. `completedRetentionMs` defaults to `null`; when configured, each
  10-second cleanup tick deletes at most 1,000 eligible SQLite rows. Completed
  queue counts are queried by primary key in batches of at most 500 names for
  stats and empty-queue reconciliation; database errors fail the cleanup pass
  instead of being interpreted as zero completions. Deleted pages return to
  SQLite's freelist for reuse, so steady workloads stop extending the file once
  that space is reused; SQLite does not automatically shrink an already large
  database file. Reclaiming existing filesystem space requires an operator-run
  `VACUUM` during a maintenance window with enough temporary free space.
- Indexes maintained by the schema: `idx_jobs_queue_state`, `idx_jobs_queue_created`, `idx_jobs_queue_state_created`, partial `idx_jobs_run_at WHERE state IN ('waiting','prioritized','waiting-children','delayed')`, `idx_jobs_unique`, `idx_jobs_custom_id`, `idx_jobs_parent`, `idx_jobs_state_started`, `idx_jobs_group_id`, `idx_jobs_pending_priority` over the same four pending states, `idx_jobs_completed_order` on `(completed_at DESC, id DESC)`, queue-scoped `idx_jobs_completed_retention`, global `idx_jobs_completed_retention_global`, and `dlq` indexes on `queue`/`job_id`/`entered_at`.

## Related Docs

- [PostgreSQL 15–18 Multi-Broker Persistence](./postgres-multibroker.md) — optional
  server backend; intentionally separate from this synchronous SQLite path.
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
