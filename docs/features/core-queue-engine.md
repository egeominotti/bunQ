# Core Queue Engine (QueueManager & Shards)

> **Category:** Engine · **Source:** `src/application/queueManager.ts`, `src/application/queue-manager/`, `src/application/operations/`, `src/application/types/`, `src/domain/queue/shard.ts`, `src/domain/queue/shard/`, `src/shared/hash.ts`

## Purpose

`QueueManager` is the stable public façade for the central coordinator. The façade itself is six lines; a capability chain under `queue-manager/` owns state and exposes push / pull / ack / fail, queries, queue control, DLQ, cron, locks, stats, dependencies and lifecycle. It partitions queues across a fixed array of `Shard` instances, maintains the global indexes that map a job ID to its current location, and keeps hot-path operations sharded and lock-scoped while delegating algorithms to focused operation modules through typed contexts.

## Responsibilities & Scope

Owns:

- The shard arrays: `shards`, `shardLocks`, `processingShards`, `processingLocks` (`queue-manager/state.ts`), one entry per `SHARD_COUNT`.
- The global indexes: `jobIndex` (Map), `completedJobs` (BoundedSet),
  `completedJobsData` (BoundedMap), `depCompletions`
  (`DependencyCompletionTracker`: exact recent FIFO plus live pins),
  `timedOutJobs` / `retiredTimeoutLeaseTokens` (BoundedMap generation proofs),
  `retiredCronLeaseTokens` (BoundedMap), and
  `jobResults`/`customIdMap`/`jobLogs` (LRUMap).
- Lock ownership and flow state in `queue-manager/state.ts`; ACK/lease recovery in `queue-manager/ack.ts`, `delivery.ts` and `locks.ts`; flow propagation in `flow-failures.ts`, `flow-options.ts` and `dependency-runtime.ts`.
- Shard selection and routing of every operation to the owning shard.
- Lifecycle: recovery from storage at construction, background-task startup, and `shutdown()` teardown of all collections.
- `Shard` owns per-shard queue containers (`IndexedPriorityQueue` per queue name), unique-key dedup, DLQ, rate/concurrency limiters, dependency tracking, temporal index, waiter notifications, and O(1) running counters. The façade is `src/domain/queue/shard.ts`; the focused capability chain lives under `src/domain/queue/shard/`, starting with `state.ts:13-100`.

Does NOT own (delegated):

- The push/pull/ack/fail algorithms — delegated to `operations/push`, `operations/pull`, `operations/ack` via contexts (see [Job Lifecycle](./job-lifecycle.md)).
- Lock token creation/verification/expiry — delegated to `lockManager` (see [Concurrency & Locking](./concurrency-and-locking.md)).
- Persistence — delegated to `SqliteStorage` / WriteBuffer (see [Persistence](./persistence.md)).
- Queries & queue control logic — `operations/queryOperations`, `operations/queueControl` (see [Job Queries & Control](./job-queries-and-control.md)).
- DLQ, cron, stats, webhooks, workers, background tasks — separate managers/modules (see [DLQ](./dead-letter-queue.md), [Scheduler & Cron](./scheduler-and-cron.md), [Background Tasks](./background-tasks.md), [Stats](./stats-and-monitoring.md)).
- The priority queue / heap / map data structures — see [Data Structures](./data-structures.md).

## Dependencies

Internal:

- `Shard` (`src/domain/queue/shard.ts`) and `ShardCounters` (`src/domain/queue/shardCounters.ts`).
- `shardIndex`, `SHARD_COUNT`, `SHARD_MASK`, `fnv1a`, `processingShardIndex`, `uuid` from `src/shared/hash.ts`.
- `RWLock` / `withWriteLock` from `src/shared/lock.ts` (see [Concurrency & Locking](./concurrency-and-locking.md)).
- `LRUMap`, `BoundedSet`, `BoundedMap` from `src/shared/lru.ts` (see [Data Structures](./data-structures.md)).
- `ContextFactory` (`src/application/contextFactory.ts`) — builds the per-operation context objects passed to delegated modules.
- Operation modules: `operations/push`, `operations/pull`, `operations/ack`, `operations/queueControl`, `operations/jobManagement`, `operations/jobMoveOperations`, `operations/jobClaim`, `operations/jobStateTransitions`, `operations/queryOperations`.
- Managers: `WebhookManager`, `WorkerManager`, `EventsManager`, `CronScheduler`, `dlqManager`, `jobLogsManager`, `lockManager`, `statsManager`, `backgroundTasks`, `dependencyProcessor`.
- `SqliteStorage` (`src/infrastructure/persistence/sqlite.ts`) — optional; only constructed by `queue-manager/state.ts` when `config.dataPath` is set.

External / runtime:

- Bun APIs: `Bun.randomUUIDv7()` and `navigator.hardwareConcurrency` in
  `shared/hash.ts`; `queueMicrotask` for dependency-flush coalescing in
  `queue-manager/dependency-runtime.ts`.

## Public Interface

`QueueManager` is exported from `queueManager.ts`; all methods below are inherited
from focused classes in `queue-manager/`. Selected real signatures:

Core operations:

- `push(queue: string, input: JobInput): Promise<Job>` / `pushBatch(...)`
- `pull(queue: string, timeoutMs?: number): Promise<Job | null>` / `pullBatch(...)`
- `pullWithLock(...)` / `pullBatchWithLock(...)`
- `ack(...): Promise<AckOutcome>` / `ackBatch(...): Promise<AckBatchOutcome>` /
  `ackBatchWithResults(...): Promise<AckBatchOutcome>`
- `fail(jobId, error?, token?, unrecoverable?, stack?): Promise<AckOutcome>`
- `jobHeartbeat(jobId, token?): boolean` / `jobHeartbeatBatch(...)`

Locks: `createLock`, `verifyLock`, `renewJobLock`, `renewJobLockBatch`,
`releaseLock`, `getLockInfo`, `removeLock`, `extendLock`
(`queue-manager/locks.ts`, `queue-manager/job-management.ts`).

Queries: `getJob`, `getJobState`, `getResult`, `getChildrenValues`,
`getJobByCustomId`, `getProgress`, `count`, `getJobs`, `getCountsPerPriority`
(`queue-manager/queries.ts`, `queue-manager/limits.ts`).

Queue control: `pause`, `resume`, `isPaused`, `drain`, `obliterate`, `clean`,
`listQueues` (`queue-manager/control.ts`).

Rate/concurrency: `setRateLimit`, `clearRateLimit`, `setConcurrency`, `clearConcurrency`, scalar `getQueueLimits`, and full `getQueueLimitStatus`. See [Rate Limiting & Concurrency](./rate-limiting-and-concurrency.md).

Deduplication introspection: `getDeduplicationJobId`, `removeDeduplicationKey`, and owner-aware `removeJobDeduplicationKey`.

DLQ: `getDlq`, `getDlqEntries`, `getDlqCount`, `getDlqStats`, filtered/bounded `retryDlq`, `purgeDlq`, and selector-aware `retryCompleted`.

Job management: `cancel`, `updateProgress`, `updateJobData`, `changePriority`,
`promote`, `moveToDelayed`, `changeDelay`, `moveActiveToWait`,
`moveToWaitingChildren`, `discard` (`queue-manager/job-management.ts`).

Flow/dependencies: `updateJobParent`, `removeChildDependency`,
`removeUnprocessedChildren`, `getFailedChildrenValues`,
`getIgnoredChildrenFailures` (`queue-manager/queries.ts`,
`queue-manager/dependencies.ts`, `queue-manager/flow-options.ts`). See
[FlowProducer](./flow-producer.md).

Stats/lifecycle: `getStats`, `getQueuesSummary`, `getQueueJobCounts`,
`getMemoryStats`, `getStorageStatus`, `compactMemory`, `getPrometheusMetrics`,
`shutdown` (`queue-manager/stats.ts`, `queue-manager/observability.ts`,
`queue-manager/lifecycle.ts`). See [Stats & Monitoring](./stats-and-monitoring.md).

This class is invoked by the TCP and HTTP command handlers; it does not itself parse the wire protocol or expose endpoints. See [TCP Server Handlers](./tcp-server-handlers.md) and [HTTP API](./http-api.md) for the command/endpoint surface that maps onto these methods.

Hashing functions (`src/shared/hash.ts`):

- `fnv1a(str: string): number` (`hash.ts:13`) — 32-bit FNV-1a, unsigned.
- `shardIndex(key: string): number` → `fnv1a(key) & SHARD_MASK` (`hash.ts:47`).
- `processingShardIndex(jobId: string): number` → `fnv1a(jobId) & SHARD_MASK` (`hash.ts:54`).
- `SHARD_COUNT` / `SHARD_MASK` (`hash.ts:44-45`), `uuid()` → `Bun.randomUUIDv7()` (`hash.ts:62`), `constantTimeEqual(a, b)` (`hash.ts:70`).

`Shard` (exported façade, `src/domain/queue/shard.ts:8`) — selected methods:
`getQueue(name)`, `getState`/`isPaused`/`pause`/`resume`, unique-key methods
including owner-aware release, FIFO group methods,
`releaseJobResources(queue, uniqueKey, groupId, ownerId?)`, queue-scoped
`notify`/`notifyBatch`/`waitForJob`, DLQ delegates, counters, `drain(queue)`, and
`obliterate(queue)`. Their implementations are split by responsibility under
`src/domain/queue/shard/`.

`ShardCounters` (exported, `shardCounters.ts:19`) + `ShardStats` interface (`shardCounters.ts:10`).

`ContextFactory` (exported, `src/application/contextFactory.ts:20`) with `ContextDependencies` / `ContextCallbacks` interfaces in `src/application/types/contextFactory.ts`.

## Data Models

See [data-model](../data-model.md) for full definitions. Most relevant here:

- `JobLocation` (`src/domain/types/queue.ts`) — discriminated union stored in `jobIndex`:
  - `{ type: 'queue'; shardIdx: number; queueName: string }`
  - `{ type: 'processing'; shardIdx: number }`
  - `{ type: 'completed'; queueName: string }`
  - `{ type: 'dlq'; queueName: string }`
- `QueueState` (`src/domain/types/queue.ts:7-17`) — `{ name; paused; rateLimit; rateLimitDuration; rateLimitExpiresAt; concurrencyLimit; activeCount }`, per-queue control state held in the shard's `LimiterManager`.
- `ShardStats` (`shardCounters.ts:10-17`) — `{ queuedJobs; delayedJobs; dlqJobs }`, O(1) running counters.
- `QueueManagerConfig` / `DEFAULT_CONFIG` (`src/application/types/config.ts`) — see [Configuration](#configuration).
- `EventType` enum and `JobEvent` (`src/domain/types/queue.ts`) — events broadcast via `EventsManager` on push/pull/complete/fail/pause/resume/etc.
- `JobLock` — held in `jobLocks` Map; `lock.token` and `lock.createdAt` are load-bearing for the #101 grace window.

## Business Logic / Control Flow

**Construction & recovery (`queue-manager/state.ts`):**

1. Merge config over `DEFAULT_CONFIG`; create `SqliteStorage` only if `config.dataPath` is set.
2. Allocate bounded collections from config sizes.
3. Allocate `SHARD_COUNT` shards, each with its own `RWLock` for the waiting/delayed queue and a separate `RWLock` + `Map` for processing.
4. Wire `CronScheduler` push/persist/worker-check callbacks; construct `WebhookManager`/`WorkerManager`/`EventsManager`.
5. Build `ContextFactory` from `getContextDependencies()` + `getContextCallbacks()`.
6. `bgTasks.recover(...)` reloads persisted jobs/queues; record `recoveryStats`; load crons; `startBackgroundTasks(...)`.

**Sharding:** `shardIndex(queue)` routes a queue name to a single shard; the
waiting/delayed state for that queue lives only there
(`queue-manager/state.ts`, `queue-manager/delivery.ts`). `processingShards` is
indexed by the job's *location* (`loc.shardIdx`), captured when the job was
pulled — not re-derived from the queue name. `SHARD_COUNT` is computed once at
module load as the next power of two ≥ `navigator.hardwareConcurrency`
(fallback 4), capped at 64 (`shared/hash.ts`); `SHARD_MASK = SHARD_COUNT - 1`
enables a bitwise-AND modulo.

**Delegation pattern:** The public façade contains no operational logic. Each
capability class either builds the appropriate context through `ContextFactory`
and calls a stateless operation, or owns one cohesive orchestration concern.
Context contracts are isolated in `application/types/`, so operation modules do
not close over the concrete manager.

**ACK/FAIL with lock & recovery paths (`queue-manager/ack.ts`,
`queue-manager/delivery.ts`):** If a token is supplied and `verifyLock` fails,
the manager checks `isExpiredButOwned` (#101 grace) and
`throwIfOwnershipConflict`. Timeout failure claims record the exact
`{ jobId, startedAt, token }` while holding the processing lock. A later ACK or
FAIL for that retired generation becomes the explicit successful no-op
`{ applied: false, reason: 'already-finalized' }`; a current retry token still
applies and a wrong or missing token still fails. Batch ACKs report exact
`ignoredIndices` (plus diagnostic `ignoredIds`), so duplicate IDs remain
positionally unambiguous. A requeued non-timeout stall generation can still use
`completeStallRetriedJob` to prevent duplicate execution (Issue #33). The
retired-generation check is repeated after the processing claim to close the
validation-to-claim race.

**Dependency flush (`queue-manager/dependency-runtime.ts`):** On job completion,
IDs accumulate in `pendingDepChecks`. `scheduleDependencyFlush` coalesces
multiple completions in a tick via `queueMicrotask`; `runDependencyFlush` loops
`processPendingDependencies` until the set drains, with a reentrancy guard
(`depFlushRunning`) and re-scheduling if new IDs arrive mid-flush.

**Obliterate (`queue-manager/control.ts`):** Clears the shard's waiting/delayed
queue and DLQ, then explicitly sweeps every global index (`jobIndex`,
`completedJobs`, `completedJobsData`, `jobResults`, `jobLogs`, `jobLocks`, flow
maps, `repeatChain`, `customIdMap`), purges per-queue metrics + persisted
queue-state row, deletes processing-shard entries for the queue, and removes
SQLite rows. This is the documented way to reclaim ALL state for a queue.

**Counters and snapshots:** `incrementQueued`/`decrementQueued` keep shard totals in sync and feed `TemporalManager`. Public global/per-queue state snapshots still classify current `runAt` values so a matured delayed job cannot be counted as both delayed and ready before the periodic counter refresh. `queueStatsAggregator.ts` batches all requested queue counts into one pass over shared collections; WS/SSE count events are coalesced by `QueueCountsScheduler`.

## Concurrency & Locking

Per-shard `RWLock`s are the synchronization primitive; there is no global queue
lock. Mutations to a shard's waiting/delayed queue run inside
`withWriteLock(this.shardLocks[idx], () => { ... })` (for example
`completeStallRetriedJob` in `queue-manager/delivery.ts` and parent transitions
in `queue-manager/flow-failures.ts`, `flow-options.ts`, and `dependencies.ts`).
The processing map has its own `processingLocks[idx]`.

The documented acquisition order is `jobIndex` → `completedJobs` → `shards[N]`
→ `processingShards[N]`: read prerequisite global-index state first, then take
the shard write lock. Flow paths re-check `jobIndex.get(id)?.type` *inside* the
shard lock as a TOCTOU guard before mutating (`queue-manager/flow-failures.ts`,
`flow-options.ts`, `dependencies.ts`).

Lock-token lifecycle (lease/heartbeat) is delegated to `lockManager`;
`QueueManager` only adds the #101 grace window. `isExpiredButOwned`
(`queue-manager/delivery.ts`) honors a late ACK only when the job is still
`processing`, the stored `lock.token` still matches, and
`job.startedAt <= lock.createdAt` (the re-lease guard: a re-pulled job has a
newer `startedAt`, so a stale token is rejected). See
[Concurrency & Locking](./concurrency-and-locking.md).

## Edge Cases & Failure Modes

- **Memory bounds / eviction.** `completedJobs`, `completedJobsData`,
  `timedOutJobs`, `retiredTimeoutLeaseTokens`, and
  `retiredCronLeaseTokens` use `BoundedSet`/`BoundedMap` sized at
  `maxCompletedJobs` (50k); `BoundedSet` evicts a **10% batch** when full.
  `depCompletions` instead holds exactly that many recent bare IDs plus proofs
  pinned by live reverse dependency edges. ACK prunes only unpinned SQLite
  rows; the last consumer release moves a pin into the recent FIFO and
  re-applies the cap. `jobResults`, `customIdMap`, `jobLogs`, and
  `perQueueMetrics` use `LRUMap`, which evicts **one** tail entry per insert at
  capacity. `jobIndex` is a plain `Map`, kept bounded indirectly: the
  `completedJobs` eviction callback deletes the corresponding `jobIndex` and
  `completedJobsData` entries.
- **Stale-token / duplicate execution.** Issue #33 (lock removed but job still
  present), #75 (lock expired + requeued), and #101 (expired-but-owned grace)
  are handled in `queue-manager/ack.ts` and `delivery.ts`. Exact retired
  timeout generations reject neither the transport nor the Worker loop: their
  late ACK/FAIL receives authoritative ignored evidence, while the retry
  proceeds and emits no false local terminal event.
  Deleted `cron:` generations accept repeat delivery of only their own retired
  lease outcome; the marker is cleared before a custom ID is admitted again.
- **`removeOnComplete`.** Completed jobs with `removeOnComplete` are dropped
  from normal indexes; their bare ID is kept as recent evidence, or pinned
  while a waiting parent owns it. Recovery reconstructs ownership before
  pruning, so lowering the cap cannot strand an accepted parent.
- **Repeat-chain leak guard.** `repeatChain` is capped at 10,000 entries; the
  oldest key is evicted past the cap (`queue-manager/context.ts`).
- **Flow-failure map leak guard.** `failedChildrenValues` /
  `ignoredChildrenFailures` are released on parent terminal completion or DLQ
  move (`queue-manager/flow-failures.ts`) and on `obliterate` / `shutdown`
  (`control.ts`, `lifecycle.ts`).
- **Cron orphan removal.** On `preventOverlap` upsert, a stale waiting cron job
  is removed by unique key so a reconnecting worker does not pick it up
  immediately (#73, `queue-manager/services.ts`).
- **Per-queue metric growth.** `perQueueMetrics` is LRU-bounded by
  `maxCustomIds` in `queue-manager/state.ts`; `obliterate` deletes the entry in
  `queue-manager/control.ts`.
- **Storage optional.** All `storage?.` calls are guarded; with no `dataPath`, the manager runs fully in-memory and recovery is a no-op.
- **`shutdown()`** clears every in-memory collection and per-shard structure
  (`queue-manager/lifecycle.ts`); it does not flush in-flight work beyond
  closing storage.

## Configuration

`QueueManagerConfig` / `DEFAULT_CONFIG` (`src/application/types/config.ts`):

| Option | Default | Effect |
| --- | --- | --- |
| `dataPath` | _(unset)_ | Enables `SqliteStorage`; unset ⇒ in-memory only |
| `maxCompletedJobs` | `50_000` | Size of `completedJobs`, `completedJobsData`, and `timedOutJobs`; exact cap for recent removed-completion proofs. Proofs owned by live waiting edges stay pinned outside the recent cap until release. |
| `maxJobResults` | `10_000` | Size of `jobResults` LRU |
| `maxJobLogs` | `10_000` | Size of `jobLogs` LRU |
| `maxCustomIds` | `50_000` | Size of `customIdMap` and `perQueueMetrics` LRU |
| `maxWaitingDeps` | `10_000` | Compatibility setting; currently not enforced as an admission or eviction bound |
| `maxQueueEvents` | `10_000` | Retained lifecycle journal entries per queue, in memory or SQLite |
| `maxMetricDataPoints` | `20_160` | Retained one-minute buckets per queue and terminal state; cumulative totals are not pruned |
| `cleanupIntervalMs` | `10_000` | Background cleanup cadence |
| `jobTimeoutCheckMs` | `5_000` | Retry delay after a timeout transition error |
| `dependencyCheckMs` | `30_000` | Safety fallback (event-driven flush is the fast path) |
| `stallCheckMs` | `5_000` | Stall-detection cadence |
| `dlqMaintenanceMs` | `60_000` | DLQ auto-retry / expiry cadence |
| `validateWebhookUrls` | _(unset)_ | Passed to `WebhookManager` |

`SHARD_COUNT` is not configurable at runtime — it is derived once from `navigator.hardwareConcurrency` (power of two, capped at 64) at module load (`hash.ts:28-44`). Server-level env vars (`BUNQUEUE_DATA_PATH`, etc.) are resolved by the entrypoint and surface here as `config.dataPath`; see [Configuration & Entrypoint](./configuration.md).

## Related Docs

- [Data Structures (PriorityQueue, heaps, maps)](./data-structures.md)
- [Concurrency & Locking](./concurrency-and-locking.md)
- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md)
- [Job Queries & Queue Control](./job-queries-and-control.md)
- [Dead Letter Queue (DLQ)](./dead-letter-queue.md)
- [Deduplication & Unique Jobs](./deduplication-and-unique.md)
- [Persistence (SQLite, WriteBuffer, ReadThrough)](./persistence.md)
- [Scheduler & Cron](./scheduler-and-cron.md)
- [Background Tasks](./background-tasks.md)
- [FlowProducer & Job Dependencies](./flow-producer.md)
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md)
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md)
- [Configuration & Entrypoint](./configuration.md)
- [Architecture](../architecture.md)
- [Data Model](../data-model.md)
