# Dead Letter Queue (DLQ)

> **Category:** Jobs · **Source:** `src/domain/queue/dlqShard.ts`, `src/application/dlqManager.ts`, `src/domain/types/dlq.ts`, `src/client/queue/dlq.ts`, `src/client/queue/dlqOps.ts`

## Purpose

The Dead Letter Queue is the terminal sink for jobs that can no longer make progress: they exhausted their retry attempts, were explicitly failed as unrecoverable, stalled past `maxStalls`, lost their lock, or had a parent fail because a child failed. Each DLQ entry preserves the original `Job` plus failure metadata (reason, error, attempt history, timestamps) so operators can inspect, filter, retry, or purge dead jobs. The DLQ also supports optional time-based auto-retry with exponential backoff and age-based auto-purge, both driven by a periodic background task.

## Responsibilities & Scope

What this module owns:

- In-memory per-queue storage of `DlqEntry[]` (`DlqShard`, one instance per `Shard`) and per-queue `DlqConfig` / `StallConfig`.
- Building `DlqEntry` metadata from a failed `Job` (`createDlqEntry`), enforcing `maxEntries` (oldest-first eviction).
- Reading/filtering entries (`getEntries`, `getFiltered`, `getDlqStats`), removing single entries, clearing a queue.
- Re-queuing dead jobs back to their `PriorityQueue` (`retryDlqJob`, `retryDlqJobs`, `retryDlqByFilter`, `processAutoRetry`).
- Lifecycle policy: expiry detection (`isDlqEntryExpired`), auto-retry eligibility (`canAutoRetry`), retry scheduling with exponential backoff (`scheduleNextRetry`).
- Client SDK surface for embedded and TCP modes (`Queue.getDlq/retryDlq/purgeDlq/...`).

What it does NOT own (delegated elsewhere):

- **Deciding when a job enters the DLQ.** That lives in the fail/stall/lock/recovery paths (`src/application/operations/ack.ts`, `stallDetection.ts`, `lockManager.ts`, `backgroundTasks.ts`, `queueManager.ts`). The DLQ only exposes `addToDlq`. See [Job Lifecycle](./job-lifecycle.md).
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

### Application functions (`src/application/dlqManager.ts`)

`getDlqJobs`, `getDlqEntries`, `getDlqStats`, `retryDlqJob`, `retryDlqJobs`, `retryDlqByFilter`, `processAutoRetry`, `purgeExpiredDlq`, `purgeDlqJobs`, `configureDlq`, `getDlqConfig`, `retryCompletedJobs` — all take a `DlqContext { shards, jobIndex, storage }` (or `RetryCompletedContext`). `storage` is **required (nullable)**, not optional: every builder must pass it (or an explicit `null` for in-memory mode), because a forgotten field silently no-ops the `storage?.…` persistence calls — the embedded client's `getDlqContext` (`src/client/queue/helpers.ts`) omitted it until the #110 hardening, so embedded `retryDlqByFilter` never persisted (dlq row survived, requeued job unsaved).

### Domain helpers (`src/domain/types/dlq.ts`)

`createDlqEntry`, `addAttemptRecord`, `isDlqEntryExpired`, `canAutoRetry`, `scheduleNextRetry`; enum `FailureReason`; `DEFAULT_DLQ_CONFIG`.

### Client SDK (`src/client/queue/queue.ts` → `dlq.ts` → `dlqOps.ts`)

```typescript
queue.setDlqConfig(config: Partial<DlqConfig>): void
queue.getDlqConfig(): DlqConfig
queue.getDlqConfigAsync(): Promise<DlqConfig>
queue.getDlq(filter?: DlqFilter): DlqEntry<T>[]      // embedded only
queue.getDlqStats(): DlqStats                        // embedded only
queue.retryDlq(id?: string): number
queue.retryDlqByFilter(filter: DlqFilter): number    // embedded only
queue.purgeDlq(): number
queue.retryCompleted(id?: string): number
queue.retryCompletedAsync(id?: string): Promise<number>
```

### TCP commands handled

- `Dlq` → `handleDlq` (`src/infrastructure/server/handlers/dlq.ts:13`) — returns DLQ jobs (`getDlq(queue, count)`).
- `RetryDlq` → `handleRetryDlq` (`:23`) — retry one (`jobId`) or all; emits `dlq:retried` / `dlq:retry-all`.
- `PurgeDlq` → `handlePurgeDlq` (`:40`) — clears the queue's DLQ; emits `dlq:purged`.
- `RetryCompleted` → `handleRetryCompleted` (`:51`).
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

1. **Max attempts exceeded** — `moveFailedJobToDlq` (`src/application/operations/ack.ts:155`), terminal branch of `failJob`. Reason `MaxAttemptsExceeded`. Emits `dlq:added`.
2. **Explicit discard** — `discardJob` (`src/application/operations/jobManagement.ts:276`). Default reason `Unknown`.
3. **Stall (runtime)** — `moveStalliedJobToDlq` (`src/application/stallDetection.ts:131`) when `stallCount >= maxStalls`. Reason `Stalled`. Cron `preventOverlap` jobs are dropped instead (#73, `:142`).
4. **Lock expiry** — `handleMaxStallsExceeded` (`src/application/lockManager.ts:167`). Reason `Stalled`.
5. **Startup recovery** — `recover` (`src/application/backgroundTasks.ts:279`) for active rows that interrupted past `maxStalls`; `quarantineCorruptDependsOn` (`:208`) for jobs with an unreadable `depends_on` blob.
6. **Flow parent failure** — `moveParentToFailed` (`src/application/queueManager.ts:1428`) routes a parent whose child failed; reason `Unknown` with a `Child job <id> failed: …` message.

`createDlqEntry` (`src/domain/types/dlq.ts:88`) snapshots one `AttemptRecord`, sets `enteredAt = now`, computes `nextRetryAt = config.autoRetry ? now + autoRetryInterval : null` and `expiresAt = config.maxAge ? now + maxAge : null`.

`add()` (`dlqShard.ts:66`) enforces `maxEntries` by shifting the oldest entry (FIFO) while `entries.length >= config.maxEntries` (`:81-84`), decrementing the DLQ counter for each eviction, then pushes and increments.

### Querying

`getFiltered` (`dlqShard.ts:125`) applies the filter predicates in-memory: `reason`, `olderThan`/`newerThan` (compared against `enteredAt`), `retriable` (via `canAutoRetry`), `expired` (via `isDlqEntryExpired`), then `offset`/`limit` slicing. `getDlqStats` (`dlqManager.ts:40`) tallies counts by reason, `pendingRetry` (`nextRetryAt <= now && retryCount < maxAutoRetries`), `expired`, and oldest/newest timestamps.

### Manual retry

`retryDlqJob` (`dlqManager.ts:87`): `removeFromDlq` → `storage.deleteDlqEntry` → reset `job.attempts = 0`, `runAt = now`, `stallCount = 0`, `lastHeartbeat = now`, push a `waiting` timeline entry → `shard.getQueue().push(job)` → `incrementQueued` → `jobIndex.set(... 'queue')` → **`storage.insertJob(job, true)`** (`:112`). The re-insert is essential: the `jobs` row was deleted when the job entered the DLQ, so without it the retried job would not survive a restart. `retryDlqJobs` with no id (`:118`) clears the whole queue's DLQ in one pass (`clearDlq` + `clearDlqQueue`), then re-queues every entry. With an optional `limit` (the `RetryDlq` command's `count`, surfaced client-side as `queue.retryJobs({ state:'failed', count })`) it instead retries only the first `limit` entries by looping the per-entry `retryDlqJob`, leaving the remainder in the DLQ — before this the client's `count` was silently dropped and the whole DLQ was drained (a #111-class silent-loss). `retryDlqByFilter` (`:155`) does the same per filtered entry.

### Auto-retry (background, opt-in)

`processAutoRetry` (`dlqManager.ts:191`) runs only if `config.autoRetry`. It collects `getAutoRetryEntries` (entries where `canAutoRetry` is true: `autoRetry` on, `retryCount < maxAutoRetries`, `nextRetryAt !== null`, `now >= nextRetryAt`), calls `scheduleNextRetry(entry, config)` to bump `retryCount`/`lastRetryAt` and compute the next backoff window, removes the entry, and re-queues the job (same reset as manual retry). `scheduleNextRetry` (`src/domain/types/dlq.ts:153`) uses exponential backoff: `nextRetryAt = now + autoRetryInterval * 2^(retryCount-1)`, set to `null` once `retryCount` reaches `maxAutoRetries`.

### Auto-purge + maintenance loop

`performDlqMaintenance` (`src/application/backgroundTasks.ts:169`) iterates every cached queue name, calls `processAutoRetry` then `purgeExpiredDlq`, and emits `dlq:auto-retried` / `dlq:expired`. `purgeExpiredDlq` (`dlqManager.ts:232`) snapshots `getExpiredEntries` first, then `purgeExpired` (in-memory filter rebuild, `dlqShard.ts:175`), then deletes each expired row via `storage.deleteDlqEntry`. Scheduled by `setInterval(..., ctx.config.dlqMaintenanceMs)` (`backgroundTasks.ts:83`), default **60_000 ms** (`src/application/types.ts:43`).

### `retryCompleted`

`retryCompletedJobs` (`dlqManager.ts:278`) is a sibling, **not** a DLQ operation: it re-queues *completed* jobs from `completedJobs`/SQLite (`requeueCompletedJob`, `:299`), clearing `completedJobs`/`jobResults` and calling `storage.updateForRetry`.

## Concurrency & Locking

`DlqShard` itself is single-threaded data (Bun is single-threaded per process); its methods take no locks. Locking is the caller's job and follows the documented hierarchy (`jobIndex` → `completedJobs` → `shards[N]` → `processingShards[N]`):

- `moveFailedJobToDlq` runs inside `failJob`'s `withWriteLock(shardLocks[idx])` after the processing-shard lock released the job (`ack.ts:202,228`).
- `moveParentToFailed` (`queueManager.ts:1443`) acquires the shard write lock and **re-checks `jobIndex.get(parentId)?.type === 'queue'` inside the lock** (`:1445`) — a TOCTOU guard preventing two concurrent child-failure callbacks from creating duplicate DLQ entries for the same parent.
- `discardJob` takes the queue or processing lock to extract the job, then a separate shard write lock to `addToDlq` (`jobManagement.ts:284-310`).
- The maintenance task and `getDlq*` reads are not lock-protected, consistent with the cooperative single-thread model; auto-retry mutates entries it owns before removing them.

## Edge Cases & Failure Modes

- **`maxEntries` overflow** — bounded at 10,000 per queue by default; `add()` and `restoreEntry()` both evict oldest-first FIFO (`dlqShard.ts:81`, `:102`). The global `dlq` counter is decremented per eviction.
- **Orphan `jobs` row / UNIQUE on retry (#97)** — every DLQ-entry path must call `saveDlqEntry` **and** `deleteJob`. `lockManager.ts:179-185` documents this explicitly: if the lock-expiry path skipped these writes, the `jobs` row survived in SQLite while the DLQ entry lived only in memory, and a later retry re-INSERTed the surviving row → `UNIQUE constraint failed: jobs.id`. Symmetrically, retry paths must `insertJob(job, true)` because the row was dropped on entry (`dlqManager.ts:112`).
- **`deleteJob` does not cascade the DLQ** — by design (`sqlite.ts:457-460`): `moveFailedJobToDlq` writes the DLQ row then drops the `jobs` row; cleanup callers that genuinely want the DLQ gone must call `deleteDlqEntry` explicitly.
- **Recovery double-count avoidance** — `recover` loads `loadDlqJobIds()` and skips stale `active` rows already present in the DLQ (legacy DBs predate the failJob fix), dropping the orphan row (`backgroundTasks.ts:254-261`). `quarantineCorruptDependsOn` persists the entry and drops the row but deliberately does **not** add to in-memory DLQ — the later `loadDlq()` pass restores it exactly once (`:202-206`).
- **`job_id` is not UNIQUE in the `dlq` table** — `insertDlq` is a plain `INSERT` (`statements.ts:60`), so re-discarding the same id could create multiple rows; `deleteDlqEntry` deletes by `job_id` (all matching rows) and `loadDlq` orders by `entered_at`. In normal flow a job is removed from its owning collection before `addToDlq`, so duplicates do not arise.
- **TCP mode is read-degraded for rich queries** — in non-embedded mode `getDlq` returns `[]`, `getDlqStats` returns zeroed stats, and `retryDlqByFilter` returns 0 (`src/client/queue/dlq.ts:51-82`); only `Dlq`/`RetryDlq`/`PurgeDlq`/`RetryCompleted`/`Get/SetDlqConfig` cross the wire. `getDlqConfig` (sync) returns the client-side `tcpDlqConfigCache` or `{}`; `getDlqConfigAsync` round-trips `GetDlqConfig`.
- **`retryDlq`/`purgeDlq` are fire-and-forget in TCP mode** — they `void tcp.send(...)` and return `0` regardless of server outcome (`dlq.ts:72-89`).
- **Auto-retry default off** — `DEFAULT_DLQ_CONFIG.autoRetry = false`; entries created under it get `nextRetryAt = null` and are never auto-retried until config is changed. Changing config later does not retro-set `nextRetryAt` on existing entries.
- **`maxAge: null`** disables expiry (`expiresAt = null`), so entries persist until manually purged or evicted by `maxEntries`.
- **Timeline cap** — re-queue paths only push a `waiting` timeline entry while `timeline.length < MAX_TIMELINE_ENTRIES`, bounding per-job timeline growth across repeated retries.

## Configuration

DLQ behavior is per-queue via `DlqConfig` (set with `queue.setDlqConfig(...)` / `SetDlqConfig`), not via environment variables:

| Field | Default | Meaning |
| --- | --- | --- |
| `autoRetry` | `false` | Enable background auto-retry from DLQ |
| `autoRetryInterval` | `3_600_000` (1h) | Base interval before first auto-retry; backoff multiplies it |
| `maxAutoRetries` | `3` | Auto-retries before `nextRetryAt` becomes `null` |
| `maxAge` | `604_800_000` (7d) | Age before auto-purge; `null` = never |
| `maxEntries` | `10000` | Per-queue cap; oldest evicted FIFO |

Maintenance cadence: `config.dlqMaintenanceMs` (application config), default **60_000 ms** (`src/application/types.ts:43`). Stall thresholds that feed the `Stalled` reason come from `StallConfig` (also stored in `DlqShard`). See [Configuration & Entrypoint](./configuration.md).

## Related Docs

- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md) — where the fail/discard paths decide DLQ entry.
- [Background Tasks](./background-tasks.md) — the 60s maintenance loop and startup recovery.
- [Persistence (SQLite, WriteBuffer, ReadThrough)](./persistence.md) — `dlq` table and `saveDlqEntry`/`loadDlq`.
- [Core Queue Engine (QueueManager & Shards)](./core-queue-engine.md) — how `Shard` wraps `DlqShard`.
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md) — lock expiry / `maxStalls`.
- [Flow Producer & Job Dependencies](./flow-producer.md) — parent-to-DLQ on child failure.
- [TCP Server Command Handlers](./tcp-server-handlers.md), [HTTP / REST / SSE / WebSocket API](./http-api.md), [Client SDK: Queue](./client-queue-sdk.md).
- [architecture](../architecture.md), [data-model](../data-model.md).
