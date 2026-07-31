# Core Queue Engine (QueueManager & Shards)

> **Category:** Engine · **Source:** `src/application/queueManager.ts`, `src/domain/queue/shard.ts`, `src/domain/queue/shardCounters.ts`, `src/application/contextFactory.ts`, `src/application/types.ts`, `src/shared/hash.ts`, `src/domain/types/queue.ts`

## Purpose

`QueueManager` is the central coordinator that owns all in-memory queue state and orchestrates every job operation in the server. It partitions queues across a fixed array of `Shard` instances (selected by hashing the queue name), maintains the global indexes that map a job ID to its current location, and exposes the full operation surface (push / pull / ack / fail, queries, queue control, DLQ, cron, locks, stats). It exists to keep hot-path operations sharded and lock-scoped while delegating the actual algorithms to focused operation modules via context objects.

## Responsibilities & Scope

Owns:

- The shard arrays: `shards`, `shardLocks`, `processingShards`, `processingLocks` (`queueManager.ts:55-58`), one entry per `SHARD_COUNT`.
- The global indexes: `jobIndex` (Map), `completedJobs` (BoundedSet),
  `completedJobsData` (BoundedMap), `depCompletions`
  (`DependencyCompletionTracker`: exact recent FIFO plus live pins),
  `timedOutJobs` (BoundedSet), and
  `jobResults`/`customIdMap`/`jobLogs` (LRUMap).
- Lock-ownership tracking: `jobLocks` and `clientJobs` (`queueManager.ts:89-90`); flow-failure maps `failedChildrenValues`/`ignoredChildrenFailures` (`queueManager.ts:97-99`); `repeatChain` for repeat-job succession (`queueManager.ts:93`).
- Shard selection and routing of every operation to the owning shard.
- Lifecycle: recovery from storage at construction, background-task startup, and `shutdown()` teardown of all collections.
- `Shard` owns per-shard queue containers (`IndexedPriorityQueue` per queue name), unique-key dedup, DLQ, rate/concurrency limiters, dependency tracking, temporal index, waiter notifications, and O(1) running counters (`shard.ts:41-79`).

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
- `SqliteStorage` (`src/infrastructure/persistence/sqlite.ts`) — optional; only constructed when `config.dataPath` is set (`queueManager.ts:148`).

External / runtime:

- Bun APIs: `Bun.randomUUIDv7()` (`hash.ts:62`), `navigator.hardwareConcurrency` for shard sizing (`hash.ts:29`), `queueMicrotask` for dependency-flush coalescing (`queueManager.ts:1765`).

## Public Interface

`QueueManager` (exported class, `queueManager.ts:50`). Selected real signatures:

Core operations:

- `push(queue: string, input: JobInput): Promise<Job>` (`queueManager.ts:299`)
- `pushBatch(queue: string, inputs: JobInput[]): Promise<JobId[]>` (`queueManager.ts:304`)
- `pull(queue: string, timeoutMs?: number): Promise<Job | null>` (`queueManager.ts:309`)
- `pullWithLock(queue, owner, timeoutMs?, lockTtl?): Promise<{ job: Job | null; token: string | null }>` (`queueManager.ts:313`)
- `pullBatch(queue, count, timeoutMs?): Promise<Job[]>` (`queueManager.ts:325`)
- `pullBatchWithLock(queue, count, owner, timeoutMs?, lockTtl?): Promise<{ jobs: Job[]; tokens: string[] }>` (`queueManager.ts:329`)
- `ack(jobId, result?, token?): Promise<void>` (`queueManager.ts:350`)
- `ackBatch(jobIds, tokens?): Promise<void>` (`queueManager.ts:408`)
- `ackBatchWithResults(items): Promise<void>` (`queueManager.ts:457`)
- `fail(jobId, error?, token?, unrecoverable?, stack?): Promise<void>` (`queueManager.ts:496`)
- `jobHeartbeat(jobId, token?): boolean` / `jobHeartbeatBatch(...)` (`queueManager.ts:643`, `660`)

Locks: `createLock`, `verifyLock`, `renewJobLock`, `renewJobLockBatch`, `releaseLock`, `getLockInfo`, `removeLock`, `extendLock` (`queueManager.ts:668-697`, `1206`).

Queries: `getJob`, `getJobState`, `getResult`, `getChildrenValues`, `getJobByCustomId`, `getProgress`, `count`, `getJobs`, `getCountsPerPriority` (`queueManager.ts:725-986`).

Queue control: `pause`, `resume`, `isPaused`, `drain`, `obliterate`, `clean`, `listQueues` (`queueManager.ts:836-964`).

Rate/concurrency: `setRateLimit`, `clearRateLimit`, `setConcurrency`, `clearConcurrency`, `getQueueLimits` (`queueManager.ts:1020-1063`). See [Rate Limiting & Concurrency](./rate-limiting-and-concurrency.md).

DLQ: `getDlq`, `getDlqEntries`, `getDlqCount`, `getDlqStats`, `retryDlq`, `purgeDlq`, `retryCompleted` (`queueManager.ts:990-1016`).

Job management: `cancel`, `updateProgress`, `updateJobData`, `changePriority`, `promote`, `moveToDelayed`, `changeDelay`, `moveActiveToWait`, `moveToWaitingChildren`, `discard` (`queueManager.ts:1141-1221`).

Flow/dependencies: `updateJobParent`, `removeChildDependency`, `removeUnprocessedChildren`, `getFailedChildrenValues`, `getIgnoredChildrenFailures` (`queueManager.ts:768-1743`). See [FlowProducer](./flow-producer.md).

Stats/lifecycle: `getStats`, `getQueuesSummary`, `getQueueJobCounts`, `getMemoryStats`, `getStorageStatus`, `compactMemory`, `getPrometheusMetrics`, `shutdown` (`queueManager.ts:1802-1899`). See [Stats & Monitoring](./stats-and-monitoring.md).

This class is invoked by the TCP and HTTP command handlers; it does not itself parse the wire protocol or expose endpoints. See [TCP Server Handlers](./tcp-server-handlers.md) and [HTTP API](./http-api.md) for the command/endpoint surface that maps onto these methods.

Hashing functions (`src/shared/hash.ts`):

- `fnv1a(str: string): number` (`hash.ts:13`) — 32-bit FNV-1a, unsigned.
- `shardIndex(key: string): number` → `fnv1a(key) & SHARD_MASK` (`hash.ts:47`).
- `processingShardIndex(jobId: string): number` → `fnv1a(jobId) & SHARD_MASK` (`hash.ts:54`).
- `SHARD_COUNT` / `SHARD_MASK` (`hash.ts:44-45`), `uuid()` → `Bun.randomUUIDv7()` (`hash.ts:62`), `constantTimeEqual(a, b)` (`hash.ts:70`).

`Shard` (exported class, `shard.ts:41`) — selected methods: `getQueue(name)` (`shard.ts:97`), `getState`/`isPaused`/`pause`/`resume` (`shard.ts:106-121`), unique-key methods (`shard.ts:125-155`), FIFO group methods (`shard.ts:159-173`), `releaseJobResources(queue, uniqueKey, groupId)` (`shard.ts:216`), queue-scoped `notify`/`notifyBatch`/`waitForJob`, DLQ delegates, counters, `drain(queue)`, and `obliterate(queue)`.

`ShardCounters` (exported, `shardCounters.ts:19`) + `ShardStats` interface (`shardCounters.ts:10`).

`ContextFactory` (exported, `contextFactory.ts:80`) with `ContextDependencies` / `ContextCallbacks` interfaces.

## Data Models

See [data-model](../data-model.md) for full definitions. Most relevant here:

- `JobLocation` (`queue.ts:104-108`) — discriminated union stored in `jobIndex`:
  - `{ type: 'queue'; shardIdx: number; queueName: string }`
  - `{ type: 'processing'; shardIdx: number }`
  - `{ type: 'completed'; queueName: string }`
  - `{ type: 'dlq'; queueName: string }`
- `QueueState` (`queue.ts:7-13`) — `{ name; paused; rateLimit; concurrencyLimit; activeCount }`, per-queue control state held in the shard's `LimiterManager`.
- `ShardStats` (`shardCounters.ts:10-17`) — `{ queuedJobs; delayedJobs; dlqJobs }`, O(1) running counters.
- `QueueManagerConfig` / `DEFAULT_CONFIG` (`types.ts:18-44`) — see [Configuration](#configuration).
- `EventType` enum and `JobEvent` (`queue.ts:111-142`) — events broadcast via `EventsManager` on push/pull/complete/fail/pause/resume/etc.
- `JobLock` — held in `jobLocks` Map; `lock.token` and `lock.createdAt` are load-bearing for the #101 grace window.

## Business Logic / Control Flow

**Construction & recovery (`queueManager.ts:146-211`):**

1. Merge config over `DEFAULT_CONFIG`; create `SqliteStorage` only if `config.dataPath` is set (`:148`).
2. Allocate bounded collections from config sizes (`:151-163`).
3. Allocate `SHARD_COUNT` shards, each with its own `RWLock` for the waiting/delayed queue and a separate `RWLock` + `Map` for processing (`:166-171`).
4. Wire `CronScheduler` push/persist/worker-check callbacks; construct `WebhookManager`/`WorkerManager`/`EventsManager`.
5. Build `ContextFactory` from `getContextDependencies()` + `getContextCallbacks()`.
6. `bgTasks.recover(...)` reloads persisted jobs/queues; record `recoveryStats`; load crons; `startBackgroundTasks(...)`.

**Sharding:** `shardIndex(queue)` routes a queue name to a single shard; the waiting/delayed state for that queue lives only there (`queueManager.ts:966-968`, `981`). `processingShards` is indexed by the job's *location* (`loc.shardIdx`), captured when the job was pulled — not re-derived from the queue name. `SHARD_COUNT` is computed once at module load as the next power of two ≥ `navigator.hardwareConcurrency` (fallback 4), capped at 64 (`hash.ts:28-44`); `SHARD_MASK = SHARD_COUNT - 1` enables a bitwise-AND modulo.

**Delegation pattern:** Public methods are thin. Each builds the appropriate context via `contextFactory.getXxxContext()` and calls a stateless operation function. Contexts bundle exactly the collections/managers a module needs (`contextFactory.ts:86-281`), so the operation modules never close over `QueueManager` itself.

**ACK with lock & recovery paths (`queueManager.ts:350-406`):** If a token is supplied and `verifyLock` fails, the manager checks `isExpiredButOwned` (#101 grace) and `throwIfOwnershipConflict`. It then handles three recovery cases against `jobIndex`: still `processing` → proceed to `ackJob`; requeued to `queue` and not in `timedOutJobs` → `completeStallRetriedJob` to prevent duplicate execution (Issue #33); in `timedOutJobs` → discard so the retry wins. A "not found" error from `ackJob` triggers the same stall-retry recovery.

**Dependency flush (`queueManager.ts:1753-1791`):** On job completion, IDs accumulate in `pendingDepChecks`. `scheduleDependencyFlush` coalesces multiple completions in a tick via `queueMicrotask`; `runDependencyFlush` loops `processPendingDependencies` until the set drains, with a reentrancy guard (`depFlushRunning`) and re-scheduling if new IDs arrive mid-flush.

**Obliterate (`queueManager.ts:870-940`):** Clears the shard's waiting/delayed queue and DLQ, then explicitly sweeps every global index (`jobIndex`, `completedJobs`, `completedJobsData`, `jobResults`, `jobLogs`, `jobLocks`, flow maps, `repeatChain`, `customIdMap`), purges per-queue metrics + persisted queue-state row, deletes processing-shard entries for the queue, and removes SQLite rows. This is the documented way to reclaim ALL state for a queue.

**Counters and snapshots:** `incrementQueued`/`decrementQueued` keep shard totals in sync and feed `TemporalManager`. Public global/per-queue state snapshots still classify current `runAt` values so a matured delayed job cannot be counted as both delayed and ready before the periodic counter refresh. `queueStatsAggregator.ts` batches all requested queue counts into one pass over shared collections; WS/SSE count events are coalesced by `QueueCountsScheduler`.

## Concurrency & Locking

Per-shard `RWLock`s are the synchronization primitive; there is no global queue lock. Mutations to a shard's waiting/delayed queue run inside `withWriteLock(this.shardLocks[idx], () => { ... })` (e.g. `completeStallRetriedJob` `queueManager.ts:601`, parent promotion `:1443`, `:1556`, `:1624`, `:1682`). The processing map has its own `processingLocks[idx]`.

The documented acquisition order is `jobIndex` → `completedJobs` → `shards[N]` → `processingShards[N]`: read the unguarded global indexes first, then take the shard write lock. Several paths re-check `jobIndex.get(id)?.type` *inside* the shard lock as a TOCTOU guard before mutating (`queueManager.ts:1445`, `1558`, `1625`, `1683`).

Lock-token lifecycle (lease/heartbeat) is delegated to `lockManager`; `QueueManager` only adds the #101 grace window. `isExpiredButOwned` (`queueManager.ts:562-576`) honors a late ACK only when the job is still `processing`, the stored `lock.token` still matches, and `job.startedAt <= lock.createdAt` (the re-lease guard: a re-pulled job has a newer `startedAt`, so a stale token is rejected — preventing the double-completion the skeptic confirmed). See [Concurrency & Locking](./concurrency-and-locking.md).

## Edge Cases & Failure Modes

- **Memory bounds / eviction.** `completedJobs`, `completedJobsData`, and
  `timedOutJobs` use `BoundedSet`/`BoundedMap` sized at
  `maxCompletedJobs` (50k); `BoundedSet` evicts a **10% batch** when full.
  `depCompletions` instead holds exactly that many recent bare IDs plus proofs
  pinned by live reverse dependency edges. ACK prunes only unpinned SQLite
  rows; the last consumer release moves a pin into the recent FIFO and
  re-applies the cap. `jobResults`, `customIdMap`, `jobLogs`, and
  `perQueueMetrics` use `LRUMap`, which evicts **one** tail entry per insert at
  capacity. `jobIndex` is a plain `Map`, kept bounded indirectly: the
  `completedJobs` eviction callback deletes the corresponding `jobIndex` and
  `completedJobsData` entries.
- **Stale-token / duplicate execution.** Issue #33 (lock removed but job still present), #75 (lock expired + requeued), and #101 (expired-but-owned grace) are all handled in `ack`/`ackBatch`/`ackBatchWithResults`. Jobs in `timedOutJobs` are never completed by a late ACK so the retry proceeds (`queueManager.ts:372-375`, `393-396`, `428`, `476`).
- **`removeOnComplete`.** Completed jobs with `removeOnComplete` are dropped
  from normal indexes; their bare ID is kept as recent evidence, or pinned
  while a waiting parent owns it. Recovery reconstructs ownership before
  pruning, so lowering the cap cannot strand an accepted parent.
- **Repeat-chain leak guard.** `repeatChain` is capped at 10,000 entries; the oldest key is evicted past the cap (`queueManager.ts:290-293`).
- **Flow-failure map leak guard.** `failedChildrenValues`/`ignoredChildrenFailures` are released on parent terminal completion (`onJobCompleted` `:1388-1389`), on DLQ move (`:1476-1477`), and on `obliterate`/`shutdown` (AUDIT H8).
- **Cron orphan removal.** On `preventOverlap` upsert, a stale waiting cron job is removed by unique key so a reconnecting worker doesn't pick it up immediately (#73, `queueManager.ts:1254-1289`).
- **Per-queue metric growth.** `perQueueMetrics` is LRU-bounded by `maxCustomIds` so dynamically-named queues can't grow it unbounded; live queues stay resident, `obliterate` deletes the entry (`queueManager.ts:123-132`, `937-940`).
- **Storage optional.** All `storage?.` calls are guarded; with no `dataPath`, the manager runs fully in-memory and recovery is a no-op.
- **`shutdown()`** clears every in-memory collection and per-shard structure (`queueManager.ts:1864-1898`); it does not flush in-flight work beyond closing storage.

## Configuration

`QueueManagerConfig` / `DEFAULT_CONFIG` (`types.ts:18-44`):

| Option | Default | Effect |
| --- | --- | --- |
| `dataPath` | _(unset)_ | Enables `SqliteStorage`; unset ⇒ in-memory only |
| `maxCompletedJobs` | `50_000` | Size of `completedJobs`, `completedJobsData`, and `timedOutJobs`; exact cap for recent removed-completion proofs. Proofs owned by live waiting edges stay pinned outside the recent cap until release. |
| `maxJobResults` | `10_000` | Size of `jobResults` LRU |
| `maxJobLogs` | `10_000` | Size of `jobLogs` LRU |
| `maxCustomIds` | `50_000` | Size of `customIdMap` and `perQueueMetrics` LRU |
| `maxWaitingDeps` | `10_000` | Compatibility setting; currently not enforced as an admission or eviction bound |
| `cleanupIntervalMs` | `10_000` | Background cleanup cadence |
| `jobTimeoutCheckMs` | `5_000` | Timeout sweep cadence |
| `dependencyCheckMs` | `30_000` | Safety fallback (event-driven flush is the fast path) |
| `stallCheckMs` | `5_000` | Stall-detection cadence |
| `dlqMaintenanceMs` | `60_000` | DLQ auto-retry / expiry cadence |
| `validateWebhookUrls` | _(unset)_ | Passed to `WebhookManager` |

`SHARD_COUNT` is not configurable at runtime — it is derived once from `navigator.hardwareConcurrency` (power of two, capped at 64) at module load (`hash.ts:28-44`). Server-level env vars (`BUNQUEUE_DATA_PATH`, etc.) are resolved by the entrypoint and surface here as `config.dataPath`; see [Configuration & Entrypoint](./configuration.md).

> Note: the CLAUDE.md memory-bounds table lists `jobResults` at 5,000; the code default (`DEFAULT_CONFIG.maxJobResults`) is `10_000`.

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
