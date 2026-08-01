# Dead Letter Queue (DLQ)

> **Category:** Jobs · **Source:** `src/domain/queue/dlqShard.ts`, `src/application/dlqManager.ts`, `src/application/dlqRetry.ts`, `src/domain/types/dlq.ts`, `src/client/queue/dlq.ts`, `src/client/queue/dlqOps.ts`, `src/client/queue/dlqJobMethods.ts`

## Purpose

The Dead Letter Queue is the terminal sink for jobs that can no longer make progress: they exhausted their retry attempts, timed out during processing, were explicitly discarded, stalled past `maxStalls`, lost their lock, or had a parent fail because a child failed. Each DLQ entry preserves the original `Job` plus failure metadata (reason, error, attempt history, timestamps) so operators can inspect, filter, retry, or purge dead jobs. The DLQ also supports optional time-based auto-retry with exponential backoff and age-based auto-purge, both driven by a periodic background task.

## Responsibilities & Scope

What this module owns:

- In-memory per-queue storage of `DlqEntry[]` (`DlqShard`, one instance per `Shard`) and per-queue `DlqConfig` / `StallConfig`.
- Building `DlqEntry` metadata from a failed `Job` (`createDlqEntry`), enforcing `maxEntries` (oldest-first eviction).
- Reading/filtering entries (`getEntries`, `getFiltered`, `getDlqStats`), removing single entries, clearing a queue.
- Re-queuing dead jobs back to their `PriorityQueue` (`retryDlqJob`, `retryDlqJobs`, `retryDlqByFilter`, `processAutoRetry`).
- Lifecycle policy: expiry detection (`isDlqEntryExpired`), auto-retry eligibility (`canAutoRetry`), retry scheduling with exponential backoff (`scheduleNextRetry`).
- Client SDK surface for embedded and TCP modes (`Queue.getDlq/retryDlq/purgeDlq/...`).

What it does NOT own (delegated elsewhere):

- **Deciding when a job enters the DLQ.** That lives in the fail/stall/lock/recovery paths (`src/application/operations/ack/`, `stallDetection.ts`, `lockManager.ts`, `background/recovery/`, `queue-manager/flow-failures.ts`). The DLQ only exposes `addToDlq`. See [Job Lifecycle](./job-lifecycle.md).
- **Persistence.** Writes/reads of the `dlq` SQLite table are owned by [Persistence](./persistence.md) (`saveDlqEntry`, `deleteDlqEntry`, `clearDlqQueue`, `loadDlq`).
- **The 60s maintenance scheduling loop** itself — owned by [Background Tasks](./background-tasks.md) (`performDlqMaintenance`).
- **Stall detection / lock expiry logic** — see [Rate Limiting & Concurrency](./rate-limiting-and-concurrency.md) and [Background Tasks](./background-tasks.md). `DlqShard` only stores the `StallConfig`.

## Dependencies

Internal:

- [Core Queue Engine](./core-queue-engine.md) — `Shard` wraps `DlqShard` and exposes `addToDlq`, `getDlqEntries`, `removeFromDlq`, `clearDlq`, etc.; `dlqManager.ts` reaches the queue via `Shard.getQueue()` and `Shard.incrementQueued()` to re-enqueue.
- [Data Structures](./data-structures.md) — re-queued jobs are pushed back into the `PriorityQueue`.
- [Persistence](./persistence.md) — `SqliteStorage.saveDlqEntry/deleteDlqEntry/clearDlqQueue/loadDlq/insertJob/updateForRetry`.
- `src/shared/hash.ts` — `shardIndex(queue)` selects the owning shard.
- [Client SDK: Queue](./client-queue-sdk.md) — `dlq.ts` / `dlqOps.ts` form the client surface; `manager.ts` resolves the shared embedded manager.

External/runtime:

- Bun's `bun:sqlite` (via the persistence layer). DLQ blobs are msgpack-packed (`pack`/`unpack`). No external runtime dependencies.

## Public Interface

### `DlqShard` (`src/domain/queue/dlqShard.ts`)

```typescript
class DlqShard {
  constructor(stats: DlqStatsCallback);
  getConfig(queue: string): DlqConfig;
  setConfig(queue: string, config: Partial<DlqConfig>): void;
  getStallConfig(queue: string): StallConfig;
  setStallConfig(queue: string, config: Partial<StallConfig>): void;
  add(job: Job, reason?: FailureReason, error?: string | null): DlqEntry;
  restoreEntry(queue: string, entry: DlqEntry): void;
  getEntries(queue: string): DlqEntry[];
  getJobs(queue: string, count?: number): Job[];
  getFiltered(queue: string, filter: DlqFilter): DlqEntry[];
  remove(queue: string, jobId: JobId): DlqEntry | null;
  getAutoRetryEntries(queue: string, now?: number): DlqEntry[];
  getExpiredEntries(queue: string, now?: number): DlqEntry[];
  purgeExpired(queue: string, now?: number): number;
  clear(queue: string): number;
  getCount(queue: string): number;
  getQueueNames(): string[];
  deleteQueue(queue: string): number;
}
```

### Application functions (`src/application/dlqManager.ts`, `dlqRetry.ts`)

`dlqManager.ts` owns reads, statistics, configuration, purge and completed-job retry. `dlqRetry.ts` owns `retryDlqJob`, `retryDlqJobs`, `retryDlqByFilter`, and `processAutoRetry`; the split isolates retry transitions and keeps both modules below 300 lines. All operations take a `DlqContext { shards, jobIndex, jobResults, jobLogs, storage }` (or `RetryCompletedContext`). `storage` is **required (nullable)**: every builder passes it or explicit `null`, so persistence cannot silently disappear. Result/log maps are required because permanent removal clears all global ownership, not only the shard entry.

### Domain helpers (`src/domain/types/dlq.ts`)

`createDlqEntry`, `addAttemptRecord`, `isDlqEntryExpired`, `canAutoRetry`, `scheduleNextRetry`; enum `FailureReason`; `DEFAULT_DLQ_CONFIG`.

### Client SDK (`src/client/queue/queue.ts` → `dlq.ts` → `dlqOps.ts`)

```typescript
queue.setDlqConfig(config: Partial<DlqConfig>): void
queue.getDlqConfig(): DlqConfig
queue.getDlqConfigAsync(): Promise<DlqConfig>
queue.getDlq(filter?: DlqFilter): DlqEntry<T>[]      // embedded only
queue.getDlqAsync(filter?: DlqFilter): Promise<DlqEntry<T>[]>
queue.getDlqJobsAsync(count?: number): Promise<Job<T>[]>
queue.getDlqStats(): DlqStats                        // embedded snapshot
queue.getDlqStatsAsync(): Promise<DlqStats>
queue.retryDlq(id?: string): number
queue.retryDlqAsync(id?: string): Promise<number>
queue.retryDlqByFilter(filter: DlqFilter): number    // embedded snapshot
queue.retryDlqByFilterAsync(filter: DlqFilter): Promise<number>
queue.purgeDlq(): number
queue.purgeDlqAsync(): Promise<number>
queue.retryCompleted(id?: string): number
queue.retryCompletedAsync(id?: string): Promise<number>
queue.retryJobs(opts?: {
  state?: 'failed' | 'completed'; count?: number; timestamp?: number
}): Promise<void>
```

The synchronous read/count-returning methods cannot return a remote response and
remain embedded snapshots or TCP fire-and-forget forms. The async variants are
authoritative in both runtimes. Every `job` nested in a `DlqEntry` is constructed
with the live callbacks from `dlqJobMethods.ts`; all 32 non-serialization `Job`
methods therefore reach the embedded manager or TCP broker instead of returning
placeholder values.

### TCP commands handled

- `Dlq` → `handleDlq` (`src/infrastructure/server/handlers/dlq.ts`) — accepts `count?` and `filter?`, returning both raw jobs and full entries with failure metadata.
- `GetDlqStats` → `handleGetDlqStats` — returns authoritative aggregate statistics.
- `RetryDlq` → `handleRetryDlq` — retries one (`jobId`), a bounded batch (`count`), or entries selected by `filter`; emits `dlq:retried` / `dlq:retry-all`.
- `PurgeDlq` → `handlePurgeDlq` (`:41`) — clears the queue's DLQ; emits `dlq:purged`.
- `RetryCompleted` → `handleRetryCompleted` — accepts `id?`, `count?`, and `timestamp?` and returns the applied count.
- `GetDlqConfig` / `SetDlqConfig` → `handleGetDlqConfig` / `handleSetDlqConfig` (`src/infrastructure/server/handlers/advanced.ts:337` for GetDlqConfig, `:317` for SetDlqConfig).

Dispatched in `src/infrastructure/server/handlerRoutes.ts:249-256` and `:316-319`. See [TCP Server Command Handlers](./tcp-server-handlers.md).

### HTTP endpoints (`src/infrastructure/server/httpRouteQueueConfig.ts`)

These proxy to the TCP commands above: DLQ-stats read (`getDlqStats`), `RetryDlq`, `PurgeDlq`, `GetDlqConfig`, `SetDlqConfig`; `RetryCompleted` is in `httpRouteQueues.ts:324`. See [HTTP / REST / SSE / WebSocket API](./http-api.md).

### Dashboard events emitted

`dlq:added`, `dlq:retried`, `dlq:retry-all`, `dlq:purged`, `dlq:auto-retried`, `dlq:expired`, plus `flow:failed` for parent/child flows. See [Webhooks, Events & Job Logs](./webhooks-and-events.md).

## Data Models

Full definitions in [data-model](../data-model.md). The key shape (`src/domain/types/dlq.ts:43`):

```typescript
interface DlqEntry {
  readonly job: Job;              // original job (data + options preserved)
  readonly enteredAt: number;     // when first moved to DLQ
  readonly reason: FailureReason; // last failure reason
  readonly error: string | null;
  readonly attempts: AttemptRecord[]; // attempt history
  retryCount: number;             // times retried *from* the DLQ (auto-retry)
  lastRetryAt: number | null;
  nextRetryAt: number | null;     // null = no auto-retry scheduled
  readonly expiresAt: number | null; // null = never auto-purge
}
```

`FailureReason` (`:9`): `explicit_fail`, `max_attempts_exceeded`, `timeout`, `stalled`, `ttl_expired`, `worker_lost`, `unknown` (const enum, string-valued).

`DlqConfig` (`:65`) with `DEFAULT_DLQ_CONFIG` (`:79`): `autoRetry: false`, `autoRetryInterval: 3_600_000` (1h), `maxAutoRetries: 3`, `maxAge: 604_800_000` (7d, `null` = never), `maxEntries: 10000`.

`DlqFilter` (`:168`): `reason`, `queue`, `olderThan`, `newerThan`, `retriable`, `expired`, `limit`, `offset`. `DlqStats` (`:188`): `total`, `byReason`, `byQueue`, `pendingRetry`, `expired`, `oldestEntry`, `newestEntry`.

SQLite `dlq` table (`src/infrastructure/persistence/schema.ts:73`): `id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL, queue TEXT NOT NULL, entry BLOB NOT NULL, entered_at INTEGER NOT NULL`. The `entry` BLOB is the msgpack-packed `DlqEntry`. Indexed on `queue`, `job_id`, `entered_at`. Note: `job_id` is **not** UNIQUE.

## Business Logic / Control Flow

### Entering the DLQ

`DlqShard` only stores entries; callers decide eligibility. All entry paths follow the same write order: `shard.addToDlq()` (memory) → set `jobIndex[id] = { type: 'dlq', queueName }` → `storage.saveDlqEntry(entry)` → `storage.deleteJob(id)` (drop the `jobs` row). Entry points:

1. **Processor failure at max attempts** — `moveFailedJobToDlq` (`src/application/operations/ack/failure.ts`), terminal branch of `failJob`. Ordinary processor failures use `MaxAttemptsExceeded`; their prior retryable attempts are recorded as `ExplicitFail`. Emits `dlq:added`.
2. **Processing timeout** — `checkJobTimeouts` (`src/application/background/timeouts.ts`) invokes the same failure transition with an explicit `Timeout` classification. Retryable and terminal timeout attempts therefore remain `Timeout`, including after SQLite recovery. A subsequent ordinary processor failure is classified normally and is not contaminated by the earlier timeout marker.
3. **Explicit discard** — `discardJob` (`src/application/operations/jobManagement.ts`). Default reason `Unknown`.
4. **Stall (runtime)** — `moveStalliedJobToDlq` (`src/application/stallDetection.ts`) when `stallCount >= maxStalls`. Reason `Stalled`. Cron `preventOverlap` jobs are dropped instead.
5. **Lock expiry** — `handleMaxStallsExceeded` (`src/application/lockManager.ts`). Reason `Stalled`.
6. **Startup recovery** — `recover` (`src/application/background/recovery/`) classifies interrupted active work and quarantines corrupt dependency metadata.
7. **Flow parent failure** — `moveParentToFailed` (`src/application/queue-manager/flow-failures.ts`) routes a parent whose child failed; reason `Unknown` with a `Child job <id> failed: …` message.

Retryable failures accumulate as `AttemptRecord`s in a non-enumerable `DlqRetryState` on the live `Job`, persisted in `jobs.dlq_retry_state`. `createDlqEntry` appends the terminal attempt. On first entry it sets `enteredAt = now`, schedules `nextRetryAt` when auto-retry is enabled, and derives `expiresAt` from `maxAge` (`0` = immediately expired, `null` = never). A failure after automatic redelivery reuses the original entry time, expiry, history, retry count, and scheduled backoff.

`FailureReason` also contains `ttl_expired` and `worker_lost` as reserved wire-compatible categories. Current TTL handling drops an expired waiting job when it is pulled, and disconnect recovery uses the stall/lock path, so neither reserved value is currently emitted as a terminal DLQ reason.

`add()` (`dlqShard.ts:66`) enforces `maxEntries` by shifting the oldest entry (FIFO) while `entries.length >= config.maxEntries` (`:81-84`), decrementing the DLQ counter for each eviction, then pushes and increments.

### Querying

`getFiltered` (`dlqShard.ts:125`) applies the filter predicates in-memory: `reason`, `olderThan`/`newerThan` (compared against `enteredAt`), `retriable` (via `canAutoRetry`), `expired` (via `isDlqEntryExpired`), then `offset`/`limit` slicing. `getDlqStats` (`dlqManager.ts:42`) tallies counts by reason, `pendingRetry` (`nextRetryAt <= now && retryCount < maxAutoRetries`), `expired`, and oldest/newest timestamps.

The TCP `Dlq` handler applies the same filter before serializing entries, and
`GetDlqStats` exposes the same aggregate. `getDlqAsync` converts each returned
entry through `toDlqEntry` with a complete live-method context; failure reason,
attempt history, timestamps, and the operational public `Job` survive the wire
round trip.

### Manual retry

`retryDlqJob` (`dlqRetry.ts`) removes the entry, clears its automatic-retry generation, resets `attempts`, `runAt`, `startedAt`, `stallCount`, and `lastHeartbeat`, appends a `waiting` timeline entry, re-enqueues it, restores counters/index ownership, then calls `storage.requeueDlqJob(job)`. That persistence method atomically deletes the old DLQ row and inserts the waiting job; the `jobs` row was deleted on DLQ entry, so the re-insert is essential for restart safety. `retryDlqJobs` with no id clears the whole queue's DLQ in one pass, then re-queues every entry. With an optional `limit` (the `RetryDlq` command's `count`, surfaced client-side as `queue.retryJobs({ state:'failed', count })`) it instead retries only the first `limit` entries by looping the per-entry transition, leaving the remainder in the DLQ — before this the client's `count` was silently dropped and the whole DLQ was drained (a #111-class silent-loss). `retryDlqByFilter` applies the same transition to the filtered entries.

### Auto-retry (background, opt-in)

`processAutoRetry` (`dlqRetry.ts`) runs only if `config.autoRetry`. It removes each due entry, calls `scheduleNextRetry` to advance `retryCount`/`lastRetryAt` and the backoff, and carries that state on the live job. SQLite `requeueDlqJob` atomically deletes the DLQ row and inserts the waiting job, so a crash cannot restore a stale DLQ copy beside it. A terminal re-failure reconstructs the same chain instead of resetting it. Backoff is `autoRetryInterval * 2^(retryCount-1)`; `nextRetryAt` becomes `null` at `maxAutoRetries`.

### Auto-purge + maintenance loop

`performDlqMaintenance` (`src/application/background/dlq.ts`) iterates every cached queue name, calls `processAutoRetry` then `purgeExpiredDlq`, and emits `dlq:auto-retried` / `dlq:expired`. `purgeExpiredDlq` (`dlqManager.ts`) uses one timestamp to snapshot and remove exactly the same expired entries. Both expiration and explicit `purgeDlqJobs` then remove each terminal job from `jobIndex`, results, logs, the WriteBuffer, `jobs`, `job_results`, and `dlq`. SQLite deletion is one transaction (`SqliteStorage.purgeDlqEntries`); an all-queue purge clears only that queue's DLQ rows, while expiration deletes only the captured `(queue, jobId)` pairs. If the ID already identifies a newer live generation, only its stale DLQ row is removed: the current job row, buffer entry, result/log state, and index survive. This prevents a purged terminal job from remaining observable as `failed` or resurrecting after restart without affecting another queue or a retried/recreated job. The interval is registered in `background/lifecycle.ts` from `ctx.config.dlqMaintenanceMs`; the default is **60_000 ms** (`src/application/types/config.ts`).

### `retryCompleted`

`retryCompletedJobs` is a sibling, **not** a DLQ operation. It re-queues
completed jobs from the in-memory completed-job data map, falling back to
SQLite when needed, clears completed/result ownership, and calls
`storage.updateForRetry`. Selection is stable and accepts an optional job id,
non-negative `limit`, and terminal `timestamp` cutoff. This keeps
`queue.retryJobs({ state: 'completed', count, timestamp })` consistent in
memory-only, persisted embedded, and TCP deployments.

## Concurrency & Locking

`DlqShard` itself is single-threaded data (Bun is single-threaded per process); its methods take no locks. Locking is the caller's job and follows the documented hierarchy (`jobIndex` → `completedJobs` → `shards[N]` → `processingShards[N]`):

- `moveFailedJobToDlq` runs inside `failJob`'s `withWriteLock(shardLocks[idx])` after the processing-shard lock released the job (`ack.ts:202,228`).
- `moveParentToFailed` (`queue-manager/flow-failures.ts`) acquires the shard write lock and **re-checks `jobIndex.get(parentId)?.type === 'queue'` inside the lock** — a TOCTOU guard preventing two concurrent child-failure callbacks from creating duplicate DLQ entries for the same parent.
- `discardJob` takes the queue or processing lock to extract the job, then a separate shard write lock to `addToDlq` (`jobManagement.ts:290-317`).
- The maintenance task and `getDlq*` reads are not lock-protected, consistent with the cooperative single-thread model; auto-retry mutates entries it owns before removing them.

## Edge Cases & Failure Modes

- **`maxEntries` overflow** — normalized to a positive integer (minimum 1), default 10,000. `add()`, `restoreEntry()`, and lowering the configured cap evict oldest-first FIFO. Each eviction updates the counter, clears terminal index/result/log ownership, and deletes durable DLQ/job/result rows, so memory and SQLite stay bounded across restarts.
- **Orphan `jobs` row / UNIQUE on retry (#97)** — every DLQ-entry path must persist the terminal entry and delete the live row. `lockManager.ts` documents this explicitly: if the lock-expiry path skipped these writes, the `jobs` row survived in SQLite while the DLQ entry lived only in memory, and a later retry re-inserted the surviving row → `UNIQUE constraint failed: jobs.id`. Symmetrically, retry paths call `requeueDlqJob`, whose SQLite transaction deletes the terminal row and inserts the live row together (`persistence/sqlite/jobs.ts`).
- **`deleteJob` does not cascade the DLQ** — by design (`persistence/sqlite/mutations.ts`): `moveFailedJobToDlq` writes the DLQ row then drops the `jobs` row; cleanup callers that genuinely want the DLQ gone must call `deleteDlqEntry` explicitly.
- **Purge is a full terminal deletion** — clearing only `DlqShard`/the `dlq` table leaves a dangling `jobIndex` location, so `GetState` still reports `failed`. `purgeDlqJobs` and `purgeExpiredDlq` share the same cleanup path and transactionally remove durable job/result rows plus pending buffered inserts before dropping the global indexes. Repeating expiry cleanup is a no-op, and purging one queue cannot delete another queue's entries.
- **Recovery double-count avoidance** — `recover` loads `loadDlqJobIds()` and `recoverActiveJobs` skips stale `active` rows already present in the DLQ (legacy DBs predate the failJob fix), dropping the orphan row (`background/recovery/active.ts`). `quarantineCorruptDependsOn` persists the entry and drops the row but deliberately does **not** add to in-memory DLQ — the later `restoreDlq` pass restores it exactly once (`background/recovery/shared.ts`, `restore.ts`).
- **`job_id` is not UNIQUE in the `dlq` table** — `insertDlq` is a plain `INSERT`; `deleteDlqEntry` deletes every matching row and `loadDlq` orders by `entered_at`. Normal terminal transitions own one generation, retries remove its row before requeue, and capacity eviction removes stale matches.
- **Choose the async remote surface** — `getDlq`, `getDlqStats`, and `retryDlqByFilter` retain synchronous embedded return types, so their TCP forms cannot return broker results. Use `getDlqAsync`, `getDlqStatsAsync`, and `retryDlqByFilterAsync`; they round-trip full entries, statistics, and filtered retry counts. `getDlqConfig` returns the client-side TCP cache or `{}`, while `getDlqConfigAsync` reads the broker.
- **`retryDlq`/`purgeDlq` are fire-and-forget in TCP mode** — they `void tcp.send(...)` and return `0` regardless of server outcome (`dlq.ts:72-89`).
- **Auto-retry default off** — `DEFAULT_DLQ_CONFIG.autoRetry = false`; entries created under it get `nextRetryAt = null` and are never auto-retried until config is changed. Changing config later does not retro-set `nextRetryAt` on existing entries.
- **`maxAge: null`** disables expiry (`expiresAt = null`), so entries persist until manually purged or evicted by `maxEntries`.
- **Timeline cap** — re-queue paths only push a `waiting` timeline entry while `timeline.length < MAX_TIMELINE_ENTRIES`, bounding per-job timeline growth across repeated retries.

## Configuration

DLQ behavior is per-queue via `DlqConfig` (set with `queue.setDlqConfig(...)` / `SetDlqConfig`), not via environment variables:

The effective merged configuration is written through to
`queue_state.dlq_config` as one MessagePack blob. It survives broker restart and
is restored before interrupted active jobs are classified, so recovery uses the
same retry/retention policy that was active before the crash. Resetting every
field to its default removes the control-state row when no other persisted
queue control needs it; `obliterate` removes it unconditionally.

| Field | Default | Meaning |
| --- | --- | --- |
| `autoRetry` | `false` | Enable background auto-retry from DLQ |
| `autoRetryInterval` | `3_600_000` (1h) | Base interval before first auto-retry; backoff multiplies it |
| `maxAutoRetries` | `3` | Auto-retries before `nextRetryAt` becomes `null` |
| `maxAge` | `604_800_000` (7d) | Age before auto-purge; `null` = never |
| `maxEntries` | `10000` | Per-queue cap; oldest evicted FIFO |

Numeric configuration is normalized at the domain boundary: intervals, retry counts and ages are finite non-negative integers; `maxEntries` is a finite positive integer with minimum 1. The same rule applies to embedded calls and sanitized TCP/HTTP commands.

Maintenance cadence: `config.dlqMaintenanceMs` (application config), default **60_000 ms** (`src/application/types/config.ts`). Stall thresholds that feed the `Stalled` reason come from `StallConfig` (also stored in `DlqShard`). See [Configuration & Entrypoint](./configuration.md).

## Related Docs

- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md) — where the fail/discard paths decide DLQ entry.
- [Background Tasks](./background-tasks.md) — the 60s maintenance loop and startup recovery.
- [Persistence (SQLite, WriteBuffer, ReadThrough)](./persistence.md) — `dlq` table and `saveDlqEntry`/`loadDlq`.
- [Core Queue Engine (QueueManager & Shards)](./core-queue-engine.md) — how `Shard` wraps `DlqShard`.
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md) — lock expiry / `maxStalls`.
- [Flow Producer & Job Dependencies](./flow-producer.md) — parent-to-DLQ on child failure.
- [TCP Server Command Handlers](./tcp-server-handlers.md), [HTTP / REST / SSE / WebSocket API](./http-api.md), [Client SDK: Queue](./client-queue-sdk.md).
- [architecture](../architecture.md), [data-model](../data-model.md).
