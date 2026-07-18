# Background Tasks

> **Category:** Scheduling · **Source:** `src/application/backgroundTasks.ts`, `src/application/cleanupTasks.ts`, `src/application/dependencyProcessor.ts`, `src/application/monitoringChecks.ts`, `src/application/taskErrorTracking.ts`

## Purpose

This module orchestrates all of the periodic, server-side maintenance work that keeps a `QueueManager` healthy without a client request driving it. It owns the `setInterval` timers for job-timeout sweeps, stall detection, expired-lock recovery, DLQ auto-retry/expiry, dependency resolution, memory-bound garbage collection, and dashboard threshold monitoring. It also owns startup `recover()` — the one-shot pass that rebuilds in-memory shard state from SQLite after a restart. It exists so the rest of the engine can stay request-driven: the timers reconcile drift, reclaim leaked resources, and bound memory growth in the background.

## Responsibilities & Scope

Owns:

- The interval lifecycle: `startBackgroundTasks` / `stopBackgroundTasks` and the `BackgroundTaskHandles` returned to `QueueManager` (`backgroundTasks.ts:39`, `backgroundTasks.ts:114`).
- Job-timeout enforcement (`checkJobTimeouts`, `backgroundTasks.ts:147`).
- DLQ maintenance dispatch — iterating queues and calling into the DLQ manager (`performDlqMaintenance`, `backgroundTasks.ts:175`).
- Startup recovery: rebuilding active/pending/completed jobs, DLQ entries, and queue control-state from disk (`recover`, `backgroundTasks.ts:227`).
- Memory-bound cleanup of orphaned processing entries, stale waiting-deps, unique keys/groups, stalled candidates, orphaned `jobIndex`/`jobLocks`, and empty queues (`cleanup`, `cleanupTasks.ts:15`).
- Dependency resolution as a safety fallback to the event-driven fast path (`processPendingDependencies`, `dependencyProcessor.ts:16`).
- Dashboard threshold monitoring and hysteresis state (`runMonitoringChecks`, `monitoringChecks.ts:56`).
- Circuit-breaker error tracking for the `cleanup`, `dependency`, and `lockExpiration` tasks (`taskErrorTracking.ts`).

Does NOT own (delegated):

- Stall classification/handling — delegated to `checkStalledJobs` in `stallDetection.ts` (see [Concurrency & Locking](./concurrency-and-locking.md)).
- Expired-lock requeue/DLQ logic — delegated to `checkExpiredLocks` in `lockManager.ts`.
- The actual DLQ re-queue/purge mechanics — delegated to `processAutoRetry` / `purgeExpiredDlq` in `dlqManager.ts` (see [Dead Letter Queue](./dead-letter-queue.md)).
- Cron/delayed scheduling — owned by `CronScheduler` (started/stopped here but implemented in [Scheduler & Cron](./scheduler-and-cron.md)).
- S3 backup intervals — handled outside this module (see [S3 Backup](./backup-s3.md)).
- SQLite I/O — delegated to the storage layer (see [Persistence](./persistence.md)).

## Dependencies

Internal:

- [Core Queue Engine](./core-queue-engine.md) — operates on `ctx.shards`, `ctx.processingShards`, `ctx.jobIndex`, and the `BackgroundContext` built by `QueueManager`'s context factory.
- `stallDetection.ts` (`checkStalledJobs`) and `lockManager.ts` (`checkExpiredLocks`) — invoked directly from the interval bodies.
- `dlqManager.ts` (`processAutoRetry`, `purgeExpiredDlq`) — see [Dead Letter Queue](./dead-letter-queue.md).
- [Persistence](./persistence.md) — `ctx.storage` (`loadActiveJobs`, `loadPendingJobs`, `loadCompletedJobs`, `loadDlq`, `loadQueueState`, `saveDlqEntry`, `deleteJob`, `updateForRetry`) and `isCorruptDependsOn` from `sqliteSerializer`.
- [Scheduler & Cron](./scheduler-and-cron.md) — `CronScheduler.start()` / `.stop()`.
- [Data Structures](./data-structures.md) — `BoundedSet`/`BoundedMap`/`LRUMap` collections, priority-queue `compact()`/`needsCompaction()`, and the per-shard temporal index.
- `src/shared/hash` — `shardIndex`, `processingShardIndex`, `SHARD_COUNT`. `src/shared/lock` — `withWriteLock`.
- [Worker Registry & Management](./workers-management.md) — `ctx.workerManager.list()` for worker-overload monitoring.

External/runtime:

- Bun/Node timers (`setInterval`/`clearInterval`).
- `process.memoryUsage()` for memory-pressure monitoring (`monitoringChecks.ts:166`).
- No third-party runtime dependencies.

## Public Interface

Exported from `backgroundTasks.ts`:

```typescript
export interface BackgroundTaskHandles {
  cleanupInterval: ReturnType<typeof setInterval>;
  timeoutInterval: ReturnType<typeof setInterval>;
  depCheckInterval: ReturnType<typeof setInterval>;
  stallCheckInterval: ReturnType<typeof setInterval>;
  dlqMaintenanceInterval: ReturnType<typeof setInterval>;
  lockCheckInterval: ReturnType<typeof setInterval>;
  cronScheduler: CronScheduler;
}

export function startBackgroundTasks(
  ctx: BackgroundContext,
  cronScheduler: CronScheduler
): BackgroundTaskHandles;

export function stopBackgroundTasks(handles: BackgroundTaskHandles): void;

export function checkJobTimeouts(ctx: BackgroundContext): void;
export function recover(ctx: BackgroundContext): void;

// Re-exports
export { getTaskErrorStats };          // from taskErrorTracking
export { processPendingDependencies }; // from dependencyProcessor
```

Exported from `cleanupTasks.ts`:

```typescript
export async function cleanup(ctx: BackgroundContext): Promise<void>;
```

Exported from `dependencyProcessor.ts`:

```typescript
export async function processPendingDependencies(ctx: BackgroundContext): Promise<void>;
```

Exported from `monitoringChecks.ts`:

```typescript
export interface MonitoringState {
  queueIdleSince: Map<string, number>;
  queueThresholdEmitted: Set<string>;
  workerOverloadedSince: Map<string, number>;
  storageWarningEmitted: boolean;
  memoryWarningEmitted: boolean;
}
export function createMonitoringState(): MonitoringState;
export function runMonitoringChecks(ctx: MonitoringContext): void;
```

Exported from `taskErrorTracking.ts`:

```typescript
export interface TaskErrorState {
  consecutiveFailures: number;
  lastError?: string;
  lastFailureAt?: number;
}
export function handleTaskError(taskName: string, err: unknown): void;
export function handleTaskSuccess(taskName: string): void;
export function getTaskErrorStats(): Record<string, TaskErrorState>;
```

No TCP commands, HTTP endpoints, or CLI commands are defined here. `getTaskErrorStats()` is surfaced via `QueueManager` for monitoring (see [Stats, Metrics & Monitoring](./stats-and-monitoring.md)). `getMemoryStats()` reflects the collections these tasks bound.

### Dashboard events emitted

Emitted via `ctx.dashboardEmit?.(event, data)` (consumed by [bunqueue Cloud Dashboard Integration](./cloud-integration.md)):

- `job:timeout` — `checkJobTimeouts` (`backgroundTasks.ts:152`)
- `dlq:auto-retried`, `dlq:expired` — `performDlqMaintenance` (`backgroundTasks.ts:186`, `:189`)
- `cleanup:orphans-removed` — `cleanOrphanedProcessingEntries` (`cleanupTasks.ts:69`)
- `cleanup:stale-deps-removed` — `cleanStaleWaitingDependencies` (`cleanupTasks.ts:91`)
- `queue:removed` — `cleanEmptyQueues` (`cleanupTasks.ts:257`)
- `job:dependencies-resolved` — `promoteJobsToQueue` (`dependencyProcessor.ts:105`)
- `queue:idle`, `queue:threshold`, `worker:overloaded`, `server:memory-warning`, `storage:size-warning` — `monitoringChecks.ts`

The stall and lock-expiry intervals additionally drive `job:stalled` / `job:lock-expired` events plus `EventType.Stalled`/`EventType.Failed` broadcasts and `stalled` webhooks from their respective sibling modules (see [Webhooks, Events & Job Logs](./webhooks-and-events.md)).

## Data Models

See [data-model](../data-model.md) for full definitions. The most relevant shapes:

- `BackgroundContext` (`src/application/types.ts:113`) extends `QueueManagerState` and adds the callbacks/collections the tasks need: `fail(jobId, error?)`, `registerQueueName`/`unregisterQueueName`, `dashboardEmit`, `workerManager`, `monitoringState`, `completedJobsData: BoundedMap<JobId, Job>`, `depCompletions?: BoundedSet<JobId>` (bare ids of `removeOnComplete` parents), and `timedOutJobs?: BoundedSet<JobId>` (guard against late ACKs from timed-out workers).
- `LockContext` (`src/application/types.ts:95`) — narrowed view passed to `checkExpiredLocks`, built by `getLockContext` (`backgroundTasks.ts:125`). It MUST carry `storage: ctx.storage`: this is the only production path to `checkExpiredLocks`, and without it the `saveDlqEntry`/`deleteJob` persistence inside `handleMaxStallsExceeded` silently no-ops through optional chaining (issue #110 — the #97 fix never executed on this path from 2.8.17 to 2.8.27, leaving orphan `active` rows in SQLite and memory-only DLQ entries).
- `DEFAULT_CONFIG` (`src/application/types.ts:33`) — the interval defaults (see [Configuration](#configuration)).
- `Job` — fields read/written by these tasks: `timeout`, `startedAt`, `lastHeartbeat`, `stallCount`, `attempts`, `runAt`, `dependsOn`, `uniqueKey`, `customId`, `deduplicationTtl`, `timeline`.
- `TaskErrorState` / `MonitoringState` — module-local tracking shapes shown above.

## Business Logic / Control Flow

### Startup: `recover(ctx)` (`backgroundTasks.ts:227`)

Runs once before the intervals start (called from `QueueManager` constructor, `queueManager.ts:202`). No-op if `ctx.storage` is null (in-memory mode). It loads `loadCompletedJobIds()` and `loadDlqJobIds()` up front, then:

1. **Phase 1 — active jobs** (`:238`): before scanning, recovery restores each persisted custom `StallConfig` from `queue_state`, because its `maxStalls` value is needed to classify interrupted work. It then repeatedly loads the first `RECOVERY_BATCH_SIZE = 10000` rows. Every handled row leaves the active result set (retry changes it to waiting; terminal/cron/DLQ-orphan handling deletes it), so incrementing `OFFSET` over the shrinking set would skip rows. Each row is treated as stalled: cron-`preventOverlap` jobs are dropped, stale DLQ duplicates are deleted, corrupt dependencies are quarantined, and normal jobs increment both `attempts` and persisted `stall_count`. Reaching `maxAttempts` or `maxStalls` persists one DLQ row but does not add it to the in-memory shard; the later `loadDlq()` pass is the single restore path, preventing duplicate entries. Otherwise Phase 1 persists one retry with backoff but does not enqueue it.
2. **Phase 2 — pending jobs** (`:312`): paginated by deterministic `priority DESC, run_at ASC, id ASC`. This phase is the single authoritative enqueue path for both original pending jobs and retries persisted by Phase 1, preventing duplicate heap entries/counter increments. Corrupt-deps are quarantined; unsatisfied dependencies enter `waitingDeps`; dedup mappings are restored.
3. **DLQ restore** (`:378`): `loadDlq()` restores every persisted entry into memory exactly once (this is why `quarantineCorruptDependsOn` deliberately does NOT touch in-memory DLQ — it only persists + drops the job row).
4. **Queue control-state restore** (`:391`, issue #100): the `loadQueueState()` snapshot used before Phase 1 for stall policy is reused to apply `paused` / `rateLimit` / `concurrencyLimit` directly to the owning shard; without it every queue silently un-pauses on restart. In-memory only, no write-back loop.
5. **Phase 3 — completed jobs** (`:404`): loads up to `maxCompletedJobs` rows into `completedJobs`/`completedJobsData` so `clean('completed')`, stats, and lookups work post-restart (issue #84). `customIdMap` is intentionally NOT populated here to avoid LRU-evicting pending-job mappings.

### `startBackgroundTasks` (`backgroundTasks.ts:39`)

Registers six intervals plus `cronScheduler.start()`:

| Interval handle | Config key | Default | Body |
| --- | --- | --- | --- |
| `cleanupInterval` | `cleanupIntervalMs` | 10s | `cleanup(ctx)` → on success `handleTaskSuccess('cleanup')` + `runMonitoringChecks(...)`; on reject `handleTaskError('cleanup', err)` |
| `timeoutInterval` | `jobTimeoutCheckMs` | 5s | `checkJobTimeouts(ctx)` (synchronous) |
| `depCheckInterval` | `dependencyCheckMs` | 30s | early-return if `pendingDepChecks.size === 0`, else `processPendingDependencies(ctx)` with `dependency` error tracking |
| `stallCheckInterval` | `stallCheckMs` | 5s | `checkStalledJobs(ctx)` |
| `dlqMaintenanceInterval` | `dlqMaintenanceMs` | 60s | `performDlqMaintenance(ctx)` |
| `lockCheckInterval` | `stallCheckMs` | 5s | `checkExpiredLocks(getLockContext(ctx))` with `lockExpiration` error tracking |

Note: monitoring runs on the cleanup tick (10s), not its own timer — it is invoked inside the `cleanup().then()` callback (`:48`). The dependency interval is a **safety fallback only**; the fast path is event-driven in `QueueManager` (the 30s default and the `pendingDepChecks.size` guard reflect this — it is not a 100ms hot loop).

### `checkJobTimeouts` (`backgroundTasks.ts:147`)

Scans every `processingShards` map. If `job.timeout && job.startedAt && now - job.startedAt > job.timeout`, it emits `job:timeout`, **adds the id to `timedOutJobs` BEFORE** calling `ctx.fail(jobId, 'Job timeout exceeded')`. The ordering is load-bearing: it ensures a late ACK from the still-hung worker is discarded by `ack`'s `timedOutJobs` guard rather than phantom-completing the job and skipping the retry.

### `performDlqMaintenance` (`backgroundTasks.ts:175`)

For each queue in `queueNamesCache`, calls `processAutoRetry` (re-queues entries whose retry schedule is due, when `autoRetry` is enabled) and `purgeExpiredDlq` (drops entries past `maxAge`), emitting `dlq:auto-retried`/`dlq:expired` with the counts. Per-queue `try/catch` logs `DLQ maintenance failed` and continues — one bad queue cannot stall the rest.

### `cleanup` (`cleanupTasks.ts:15`)

Runs in order each tick: refresh delayed counters per shard; compact any priority queue with `needsCompaction(0.2)` (>20% tombstones); then `cleanOrphanedProcessingEntries`, `cleanStaleWaitingDependencies`, `cleanUniqueKeysAndGroups`, `cleanStalledCandidates`, `cleanOrphanedJobIndex`, `cleanOrphanedJobLocks`, `cleanEmptyQueues`. A dependency-gated job older than one hour is removed under its shard write lock after a TOCTOU age re-check. Its SQLite row or pending buffered insert is deleted first, then the reverse dependency index, `jobIndex`, owned unique/custom ID reservations, and dependency-result consumer edges are released together.

### `processPendingDependencies` (`dependencyProcessor.ts:16`)

Drains `pendingDepChecks` into a local array (clearing the set), uses each shard's reverse index (`getJobsWaitingFor`) to find waiting jobs — O(m) in waiters, not O(n) in all jobs — then per shard, under the shard write lock, re-checks every dependent's `dependsOn` against `completedJobs` OR `depCompletions` and promotes satisfied jobs to the queue (`promoteJobsToQueue`, `:79`), calling `shard.notify(job.queue)` and emitting `job:dependencies-resolved`. See [FlowProducer & Job Dependencies](./flow-producer.md).

### `runMonitoringChecks` (`monitoringChecks.ts:56`)

Returns immediately if `dashboardEmit` is unset. Otherwise runs `checkQueueIdle`, `checkQueueThreshold`, `checkWorkerOverload`, `checkMemoryPressure`, `checkStorageSize`. Idle/overload use a "since" timestamp so the event only fires once the threshold duration has elapsed; threshold/memory/storage use one-shot "emitted" flags with hysteresis (re-armed below 90% of the threshold).

## Concurrency & Locking

The lock hierarchy is `jobIndex → completedJobs → shards[N] → processingShards[N]` (see [Concurrency & Locking](./concurrency-and-locking.md)). Within this module:

- `cleanOrphanedProcessingEntries`, `cleanStaleWaitingDependencies`, and `cleanOrphanedJobIndex` use a **two-phase** pattern: collect candidates lock-free, then mutate under the owning write lock and re-check membership/age inside the lock.
- `processPendingDependencies` acquires `shardLocks[i]` **before** reading `waitingDeps`, then runs shards in parallel via `Promise.all`.
- The stall and lock-expiry delegates (`stallDetection.ts`, `lockManager.ts`) acquire `shardLocks` **before** `processingLocks` (hierarchy order), verify the job is still in `processingShards` inside the lock, and only then broadcast — preventing false-positive `stalled` events.
- `checkJobTimeouts`, `cleanUniqueKeysAndGroups`, `cleanStalledCandidates`, `cleanOrphanedJobLocks`, and `cleanEmptyQueues` run without locks and tolerate benign staleness. Stale dependency removal does take the shard write lock because it updates ownership, persistence, and reverse indexes as one lifecycle.
- `recover` runs in the constructor before any concurrent traffic, so it is lock-free by construction.

Stall detection uses two-phase confirmation (a job must be flagged in two consecutive 5s cycles via `stalledCandidates`) so a brief GC pause does not trigger a false stall. Lock expiry and stall detection both reset `startedAt`, bump `attempts`/`stallCount`, and call `releaseJobResources` to free the concurrency slot + group + unique key before re-pushing or moving to DLQ. Both reclaim paths enforce `attempts < maxAttempts` and `stallCount < maxStalls` before requeueing; `updateForRetry` persists both counters so a restart cannot replenish either budget.

## Edge Cases & Failure Modes

- **Circuit breaker (log-only):** `taskErrorTracking` counts consecutive failures per task (`cleanup`, `dependency`, `lockExpiration`). At `MAX_CONSECUTIVE_FAILURES = 5` it logs `CRITICAL: Background <task> repeatedly failing`. It does **not** stop the interval — the timer keeps firing and `handleTaskSuccess` resets the counter on the next clean run. `checkJobTimeouts`, `checkStalledJobs`, and `performDlqMaintenance` are not wired into this tracker (timeouts/stalls are synchronous; DLQ has its own per-queue try/catch).
- **Timeout / late-ACK race:** `timedOutJobs` is populated before `fail()` so a hung worker's late ACK is dropped instead of phantom-completing — the canonical retry-correctness invariant of this module.
- **Corrupt `depends_on`:** quarantined to DLQ on recovery so a job with unrecoverable dependency metadata is never enqueued as ready (out-of-order execution) nor parked in `waitingDeps` forever (unbounded leak).
- **Stale `active`/DLQ rows from legacy DBs:** Phase 1 drops orphan rows for jobs already present in the DLQ table so they are not double-counted (predates the `failJob` DLQ-row cleanup fix; issue #97 lineage).
- **Cron `preventOverlap` jobs:** never re-queued by recovery, stall, or lock-expiry paths — they are deleted and left to the scheduler to recreate (issues #73/#75).
- **Memory bounds:** cleanup compacts priority queues at >20% tombstones; trims `uniqueKeys`/`activeGroups` by half when a queue exceeds 1000 entries; only walks `jobIndex` when `size > 100_000` (the full scan is expensive); evicts via `BoundedSet`/`LRUMap` caps elsewhere.
- **`perQueueMetrics` not pruned on empty-queue removal** (`cleanupTasks.ts:252`): intentional — counters are cumulative and must survive a transient drain; growth is bounded by the LRU cap and `obliterate()` reclaims explicitly.
- **`depCompletions` not pruned** in `processPendingDependencies` (`dependencyProcessor.ts:71`): intentional — it is a FIFO `BoundedSet` that self-bounds; eager pruning would orphan a dependent pushed after a `removeOnComplete` parent completed.
- **Stale dependency persistence ordering:** the SQLite/write-buffer delete runs before in-memory removal. If storage throws, the lifecycle remains live in memory and can be retried on the next cleanup tick instead of leaving disk as the only surviving copy.
- **`recover` partial state:** if `ctx.storage` is null the whole pass is skipped. Active recovery drains offset zero because its dataset mutates; pending/completed scans use deterministic pages. Phase 3 is hard-capped at `maxCompletedJobs`.

## Configuration

Interval timings come from `DEFAULT_CONFIG` (`src/application/types.ts:33`), overridable via the `QueueManagerConfig` passed to `QueueManager`:

| Option | Default | Effect |
| --- | --- | --- |
| `cleanupIntervalMs` | `10_000` | Cleanup + monitoring tick |
| `jobTimeoutCheckMs` | `5_000` | Job-timeout sweep |
| `dependencyCheckMs` | `30_000` | Dependency-resolution **safety fallback** (fast path is event-driven) |
| `stallCheckMs` | `5_000` | Stall detection **and** lock-expiry checks (shared) |
| `dlqMaintenanceMs` | `60_000` | DLQ auto-retry + expiry |
| `maxCompletedJobs` | `50_000` | Cap for Phase 3 completed-job recovery |

Monitoring thresholds are read from env vars at module load (`monitoringChecks.ts:36`), and a value of `0` disables that check:

| Env var | Default | Effect |
| --- | --- | --- |
| `QUEUE_IDLE_THRESHOLD_MS` | `30000` | Emit `queue:idle` after this idle duration (`<=0` disables) |
| `QUEUE_SIZE_THRESHOLD` | `0` (disabled) | Emit `queue:threshold` when waiting count reaches it |
| `WORKER_OVERLOAD_THRESHOLD_MS` | `30000` | Emit `worker:overloaded` after sustained at-capacity duration |
| `MEMORY_WARNING_MB` | `0` (disabled) | Emit `server:memory-warning` when heap reaches it (re-arms below 90%) |
| `STORAGE_WARNING_MB` | `0` (disabled) | Emit `storage:size-warning` when SQLite size reaches it (re-arms below 90%) |

DLQ behavior (`autoRetry`, `maxAge`) is configured per queue via `setDlqConfig`; see [Dead Letter Queue](./dead-letter-queue.md). Stall behavior (`maxStalls`, `stallInterval`, `gracePeriod`) is per queue via `setStallConfig`. General env vars live in [Configuration & Entrypoint](./configuration.md).

## Related Docs

- [architecture](../architecture.md) — overall request/background-task flow.
- [data-model](../data-model.md) — `Job`, `BackgroundContext`, `LockContext`, DLQ entry shapes.
- [Core Queue Engine](./core-queue-engine.md) — shards, `processingShards`, `jobIndex`.
- [Concurrency & Locking](./concurrency-and-locking.md) — lock hierarchy and two-phase patterns.
- [Dead Letter Queue](./dead-letter-queue.md) — auto-retry/expiry mechanics.
- [Scheduler & Cron](./scheduler-and-cron.md) — `CronScheduler` lifecycle.
- [FlowProducer & Job Dependencies](./flow-producer.md) — dependency graph that `processPendingDependencies` resolves.
- [Persistence](./persistence.md) — recovery queries and corrupt-blob detection.
- [Deduplication & Unique Jobs](./deduplication-and-unique.md) — unique-key restoration on recovery.
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md) — `getTaskErrorStats`, memory stats.
- [bunqueue Cloud Dashboard Integration](./cloud-integration.md) — consumer of the dashboard events.
- [Webhooks, Events & Job Logs](./webhooks-and-events.md) — `stalled`/`failed` event propagation.
