# Job Lifecycle (push / pull / ack / fail)

> **Category:** Jobs · **Source:** `src/application/operations/push.ts`, `src/application/operations/pull.ts`, `src/application/operations/pullStateTransition.ts`, `src/application/operations/ack.ts`, `src/application/operations/ackHelpers.ts`, `src/application/operations/jobStateTransitions.ts`, `src/application/dependencyCompletions.ts`, `src/domain/types/job.ts`, `src/domain/queue/waiterManager.ts`

## Purpose

This module implements the four primitive operations that move a job through its life: **push** (enqueue), **pull** (dequeue into processing), **ack** (complete), and **fail** (retry / DLQ). It is the lowest pure-logic layer beneath the TCP/HTTP servers and the embedded SDK — each operation takes a queue/job id, a typed context object (`PushContext`, `PullContext`, `AckContext`) holding the shard arrays, locks, and in-memory indexes, and mutates the in-memory state plus the SQLite write path. The `QueueManager` (`src/application/queueManager.ts`) wires these functions to contexts via its `contextFactory` and exposes `push`/`pull`/`ack`/`fail` plus their batch and lock-aware variants.

## Responsibilities & Scope

Owns:

- The state machine for a single job: `waiting`/`prioritized`/`delayed` → `active` → `completed` | `failed` (retry back to queue or `waiting-children`/DLQ). Timeline entries are appended at each transition (`Job.timeline`, capped at `MAX_TIMELINE_ENTRIES`).
- customId idempotency and uniqueKey deduplication at push time (`handleCustomId`, `handleDeduplication`).
- Routing a pushed job to either the shard's priority queue or `waitingDeps` (dependency gate).
- Retry decision (`canRetry`), backoff computation (`calculateBackoff`), and the terminal failure → DLQ handoff.
- `removeOnComplete` / `removeOnFail` cleanup and the `depCompletions` bookkeeping that keeps dependents unblockable after the parent job is dropped.
- Manual BullMQ-style transitions: `moveActiveToWait`, `changeWaitingDelay`, `moveToWaitingChildren`.
- Long-poll worker waiting (`WaiterManager`).

Does NOT own:

- The priority-queue / heap data structures themselves — see [Data Structures](./data-structures.md).
- Shard ownership, locking primitives, and `releaseJobResources` mechanics — see [Core Queue Engine](./core-queue-engine.md) and [Concurrency & Locking](./concurrency-and-locking.md).
- Lock token creation/verification and stall/timeout sweeps — see [Worker Registry & Management](./workers-management.md) and [Background Tasks](./background-tasks.md). The `QueueManager` wraps these operations with lock verification and stall-retry recovery; the raw functions here are lock-agnostic.
- DLQ storage and retry policy — see [Dead Letter Queue](./dead-letter-queue.md).
- Dependency-graph processing after completion — see [FlowProducer & Job Dependencies](./flow-producer.md).
- Persistence batching / WAL — see [Persistence](./persistence.md).

## Dependencies

Internal:

- `src/domain/types/job.ts` — `Job`, `JobInput`, `createJob`, `calculateBackoff`, `canRetry`, `isReady`, `isExpired`, `normalizeStacktrace`, `MAX_TIMELINE_ENTRIES`, `JOB_DEFAULTS`.
- `src/domain/queue/shard.ts` — per-shard `getQueue`, `incrementQueued`/`decrementQueued`, `tryAcquireConcurrency`/`tryAcquireRateLimit`, `releaseJobResources`, `addToDlq`, `waitingDeps`/`waitingChildren`, `notify`/`notifyBatch`/`waitForJob` (delegated to `WaiterManager`). See [Core Queue Engine](./core-queue-engine.md).
- `src/domain/queue/waiterManager.ts` — long-poll notification fan-out.
- `src/shared/lock.ts` — `withWriteLock`, `RWLock`. See [Concurrency & Locking](./concurrency-and-locking.md).
- `src/shared/hash.ts` — `shardIndex`, `processingShardIndex`.
- `src/shared/lru.ts` — `SetLike` / `MapLike` (bounded `completedJobs`, `jobResults`, `customIdMap`).
- `src/application/dependencyCompletions.ts` — two-tier removed-completion
  tracker plus pin, unpin, and recovery reconciliation helpers.
- `src/infrastructure/persistence/sqlite.ts` — `insertJob`, `insertJobsBatch`, `markActive`, `markCompleted`, `updateForRetry`, `storeResult`, `saveDlqEntry`, `deleteJob`. See [Persistence](./persistence.md).
- `src/application/latencyTracker.ts`, `src/application/throughputTracker.ts` — observability counters.

External / runtime: Bun APIs — `Bun.nanoseconds()` (latency), `Bun.randomUUIDv7()` (`generateJobId`, `generateLockToken`), `setTimeout` (waiter timeout). SQLite via the storage layer.

## Public Interface

Exported operation functions (real signatures):

```ts
// push.ts
export async function pushJob(queue: string, input: JobInput, ctx: PushContext): Promise<Job>;
export async function pushJobBatch(queue: string, inputs: JobInput[], ctx: PushContext): Promise<JobId[]>;

// pull.ts
export async function pullJob(queue: string, timeoutMs: number, ctx: PullContext): Promise<Job | null>;
export async function pullJobBatch(queue: string, count: number, timeoutMs: number, ctx: PullContext): Promise<Job[]>;

// ack.ts
export async function ackJob(jobId: JobId, result: unknown, ctx: AckContext): Promise<void>;
export async function failJob(jobId: JobId, error: string | undefined, ctx: AckContext, unrecoverable?: boolean, stack?: string[]): Promise<void>;
export async function ackJobBatch(jobIds: JobId[], ctx: AckContext): Promise<void>;
export async function ackJobBatchWithResults(items: Array<{ id: JobId; result: unknown }>, ctx: AckContext): Promise<void>;

// jobStateTransitions.ts
export async function moveActiveToWait(jobId: JobId, ctx: JobManagementContext): Promise<boolean>;
export async function changeWaitingDelay(jobId: JobId, delay: number, ctx: JobManagementContext): Promise<boolean>;
export async function moveToWaitingChildren(jobId: JobId, ctx: JobManagementContext): Promise<boolean>;
```

Exported context interfaces: `PushContext` (`push.ts`), `PullContext`
(`pullStateTransition.ts`, re-exported by `pull.ts`), `AckContext` (`ack.ts`),
plus the batch helpers `ExtractedJob`/`BatchContext`/`FinalizeContext` and
`groupByProcShard`/`extractJobs`/`groupByQueueShard`/`releaseResources`/
`finalizeBatchAck` in `ackHelpers.ts`.

Job-type helpers exported from `job.ts`: `createJob`, `generateJobId`, `jobId`, `calculateBackoff`, `canRetry`, `isReady`, `isDelayed`, `isExpired`, `isTimedOut`, `normalizeStacktrace`, `createJobLock`, `renewLock`, `isLockExpired`, and the `JobState` const enum.

TCP commands handled (routed in `src/infrastructure/server/handlerRoutes.ts:105`): **`PUSH`**, **`PUSHB`**, **`PULL`**, **`PULLB`**, **`ACK`**, **`ACKB`**, **`FAIL`**. The batch commands (`PUSHB`/`PULLB`/`ACKB`) map to `pushJobBatch`/`pullJobBatch`/`ackJobBatch`(`WithResults`).

Events broadcast (`EventType`): `pushed`, `pulled`, `completed`, `failed`, `Retried`, `Duplicated`, plus `waiting` (from `moveActiveToWait`). Dashboard-only events: `job:waiting-children`, `job:expired`, `job:deduplicated`, `batch:pushed`, `batch:pulled`, `dlq:added`, `flow:failed`, `ratelimit:rejected`, `concurrency:rejected`.

## Data Models

See [data-model](../data-model.md) for full definitions. The central shape is `Job` (`job.ts:81`). Most relevant fields for the lifecycle:

- Scheduling: `runAt` (createdAt + delay; also the next-retry timestamp), `startedAt`, `completedAt`.
- Retry: `attempts`, `maxAttempts` (default `3`), `backoff` (default `1000` ms), `backoffConfig` (`{ type: 'fixed' | 'exponential'; delay; maxDelay? }`).
- Lifetime: `ttl`, `timeout`, `removeOnComplete`, `removeOnFail`.
- Dedup / identity: `uniqueKey`, `customId`.
- Dependencies / flow: `dependsOn`, `parentId`, `failParentOnFailure`, `removeDependencyOnFailure`, `ignoreDependencyOnFailure`, `continueParentOnFailure`.
- Failure: `stacktrace` (capped at `stackTraceLimit`, default `10`).
- `timeline: JobTimelineEntry[]` — in-memory only, NOT persisted, capped at `MAX_TIMELINE_ENTRIES = 20`.

`JobInput` (`job.ts:171`) is the wire/SDK input; `createJob` (`job.ts:370`) fills defaults from `JOB_DEFAULTS` (`job.ts:251`). Note `removeOnComplete`/`removeOnFail` are coerced via `toBoolean` because the wire boundary is not runtime-type-safe (#90, `job.ts:295`).

`JobState` enum values: `waiting`, `prioritized`, `delayed`, `active`, `completed`, `failed` (`job.ts:20`). The additional logical states `waiting-children` (dependency gate) and `paused` (queue-level) are represented via shard membership (`waitingDeps`) and `QueueState.paused`, not the enum.

## Business Logic / Control Flow

### PUSH (`pushJob`, `push.ts:251`)

1. Compute `idx = shardIndex(queue)`; take `shardLocks[idx]` (write).
2. **customId idempotency** (`handleCustomId`, `customId.ts`): if `input.customId` maps to a live job, skip and return the existing id. On terminal reuse, a completed generation is evicted from its completed collections and `jobs` row, while a DLQ generation is removed from its owning shard, DLQ counter, `jobIndex`, and SQLite before the new generation is admitted. The recycled id therefore starts fresh as `waiting` with exactly one observable generation; any stale timeout marker is also cleared to avoid resurrecting the #33/#75 duplicate-execution guard.
3. `createJob(id, queue, input, now)`.
4. **Deduplication** (`handleDeduplication`, `push.ts:133`): only if `job.uniqueKey` is set. Strategies — `replace` (remove old, register new), `extend` (reset TTL, return existing), default BullMQ-style (return existing if it is still waiting or active; broadcast `Duplicated`). If the existing job is completed/failed, a fresh insert is allowed.
5. **Insert** (`insertJobToShard`): if `dependsOn` has unmet entries (not all in `completedJobs`/`depCompletions`), any already-present removed-completion proofs are durably pinned first, then the job goes to `shard.waitingDeps` and every dependency is registered (timeline `waiting-children`). This protects a late parent that finds one child complete but must still wait for another. Otherwise the job is pushed to the priority queue with `incrementQueued`; initial timeline state is `delayed` (if `runAt > now`), `prioritized` (if `priority > 0`), else `waiting`. `jobIndex` is set to `{ type: 'queue', shardIdx, queueName }`.
6. `shard.notify(queue)` wakes one long-poll waiter for that queue, or records one coalesced retry hint when none is waiting.
7. After the lock: persist via `storage.insertJob(job, input.durable)`, bump counters, broadcast `pushed`. **Durable** jobs bypass the 10 ms write buffer (immediate write); `pushJobBatch` (`push.ts:323`) splits durable jobs into a separate `insertJobsBatch(durableJobs, true)` so `addBulk` does not silently downgrade the durability guarantee (`push.ts:384`).

### PULL (`pullJob`, `pull.ts`)

1. `deadline = timeoutMs > 0 ? now + timeoutMs : 0`. Loop:
2. `tryPullFromShard` under `shardLocks[idx]` returns `null` if the queue is
   paused, then delegates selection to `tryDequeueNextJob`
   (`pullStateTransition.ts`).
3. `tryDequeueNextJob` inspects jobs in priority order. Expired entries are
   discarded exactly once from persistence, the heap, counters, and `jobIndex`.
   Delayed jobs and jobs whose FIFO group is active are moved into a dequeue
   scratch object while the scan looks for eligible work. Capacity is consumed
   only after an eligible job is found. The queue pop, processing insert, and
   `jobIndex` flip remain in the same synchronous shard critical section.
4. A single pull creates one scratch and restores its parked jobs in `finally`.
   A batch shares one scratch across all requested jobs and restores it once
   after the entire batch. Readiness cannot change inside that synchronous
   critical section: `now` is fixed and the active-group set only grows.
   Consequently the batch cost is `O((k + b) log n)`, rather than repeatedly
   paying `O(k log n)` for each of the `b` delivered jobs. `nextRunAt` retains
   the minimum deadline across every parked delayed job so long-poll wake-up
   semantics remain unchanged.
5. `finalizeProcessing`: `storage.markActive(...)` (non-fatal on error —
   in-memory is source of truth), bump counters, broadcast `pulled`. It returns
   `false` when a management operation claimed the job before handoff.
6. If no job and deadline not reached, `await shard.waitForJob(queue, remaining)`
   and loop; otherwise return `null`.

`pullJobBatch` pulls up to `count` jobs in one shard lock, acquiring exactly one
rate-limit and concurrency slot per selected job. A blocked or delayed entry
consumes neither. If a limiter stops a partially filled batch, every parked job
is still restored by the outer `finally`; `finalizeProcessingBatch` returns only
the jobs actually delivered.

**Safety net:** if `finalizeProcessing` throws, `requeueJob`
(`pullStateTransition.ts`) restores the job with the same observer-atomicity
rule in reverse. It skips entirely if a management operation already claimed
the job. Lock order shard → processing matches the documented hierarchy.

### ACK (`ackJob`, `ack.ts:75`)

1. Under `processingLocks[procIdx]`, remove the job from `processingShards`; if absent, **throw** `Job not found or not in processing state` (the `QueueManager` catches this to recover stall-retried jobs — see Edge Cases).
2. Under `shardLocks[idx]`, `releaseJobResources(queue, uniqueKey, groupId, job.id)` frees the concurrency slot/group and releases the unique key only if this job generation still owns it.
3. Release `customId` from `customIdMap` so it can be reused.
4. If `!removeOnComplete`: set `completedAt`, append `completed` timeline, add
   to `completedJobs` + `completedJobsData`, store a defined result, set
   `jobIndex` to completed, and persist the completed row. If
   `removeOnComplete`: `commitRemovedCompletion` atomically deletes the
   jobs/result rows and inserts a payload-free `dependency_completions` record.
   It is marked `pinned` when a reverse waiter exists; otherwise it enters the
   bounded recent FIFO. Only after commit does the matching in-memory marker
   become visible. The job remains absent from Job/state/result/completed
   queries.
5. Bump counters, broadcast `completed`, call `onJobCompleted` (dependency processing), and re-schedule repeatable jobs via `onRepeat` if under `repeat.limit`.

### FAIL (`failJob`, `ack.ts:193`)

1. Extract from `processingShards` (throw if absent, as above).
2. `attempts++`; if a `stack` is supplied, store `normalizeStacktrace(stack, stackTraceLimit)` BEFORE branching so both a retry and a DLQ entry carry it (#74, `ack.ts:218`). Append `failed` timeline entry.
3. Under `shardLocks[idx]`, `releaseJobResources`, then branch:
   - **Retry** (`!unrecoverable && canRetry(job)`, i.e. `attempts < maxAttempts`): `runAt = now + calculateBackoff(job)`, push back to queue, `incrementQueued(..., isDelayed=true, ...)`, `storage.updateForRetry`, set `wasRetried`. Appends a `waiting` timeline entry for the next attempt.
   - **removeOnFail**: delete from index + disk, bump `totalFailed`, release customId.
   - **Terminal → DLQ** (`moveFailedJobToDlq`, `ack.ts:156`): `addToDlq(job, MaxAttemptsExceeded, error)`, set `jobIndex` to `dlq`, `saveDlqEntry`, `deleteJob`, bump `totalFailed`, emit `dlq:added` (+ `flow:failed` if `parentId`).
4. Broadcast `failed`; if retried, also broadcast `Retried` (prev `failed`).
5. Flow propagation when NOT retried: `failParentOnFailure` → `onChildTerminalFailure`; `removeDependencyOnFailure`/`ignoreDependencyOnFailure`/`continueParentOnFailure` → `onChildDependencyOption`.

`calculateBackoff` (`job.ts:453`): fixed = `delay * (0.8 + rand*0.4)` (±20% jitter); exponential / default = `base * 2^attempts * (0.5 + rand)` (±50% jitter), capped at `backoffConfig.maxDelay ?? DEFAULT_MAX_BACKOFF` (1 h).

### Batch ack (`ackHelpers.ts`)

`ackJobBatch`/`ackJobBatchWithResults` short-circuit to parallel per-job
`ackJob` for ≤4 ids. Larger batches: `groupByProcShard` → `extractJobs` (one
lock per processing shard, parallel) → `groupByQueueShard` →
`releaseResources` (one lock per queue shard) → `finalizeBatchAck`.
`removeOnComplete` entries use the same delete-plus-proof transaction in both
optimized variants, including result-bearing ACKB. For retained jobs,
`jobResults.set` happens before `completedJobs.add`, so a live dependent never
observes completion before its in-memory result.

### Manual transitions (`jobStateTransitions.ts`)

`moveActiveToWait` (`:16`): processing → queue (`runAt = now`,
`startedAt = null`, release resources, push, persist via `updateRunAt`, broadcast
`waiting` prev `active`). Persisting the transition prevents restart recovery
from misclassifying the manually requeued job as an interrupted active attempt.
`changeWaitingDelay` (`:59`) updates `runAt` in place.
`moveToWaitingChildren` (`:84`) parks processing work in
`shard.waitingChildren`. Every management claim of active work also releases
the lease and client ownership through `releaseClaimedJobOwnership`.

## Concurrency & Locking

Locks are per-shard `RWLock`s acquired via `withWriteLock` and are **sequential, not nested** within each operation, which avoids deadlock despite differing acquisition orders:

- `pushJob`: `shardLocks[idx]` only (persistence + broadcast happen after release).
- `pullJob`: `shardLocks[idx]` owns the synchronous queue → processing
  transition; persistence and delivery bookkeeping run after release.
- `ackJob` / `failJob`: `processingLocks[procIdx]` (extract) released, then `shardLocks[idx]` (release resources / requeue / DLQ).

Batch ack acquires each processing-shard lock and each queue-shard lock at most once, in parallel across distinct shards (`O(shards)` not `O(n)` — `ack.ts:303`).

Lock-token verification, the **#101 grace window** (`isExpiredButOwned`: an expired-but-still-ours lock on a still-processing job is accepted, not lost), and stall-retry recovery live in `QueueManager.ack`/`fail` (`queueManager.ts:350`, `:496`), wrapping these raw functions. `WaiterManager` partitions waiters by queue, tracks the active count in O(1), and consumes entries through a head cursor. Notifications clear the waiter's timer immediately; surplus notifications coalesce into one edge-triggered `pending` bit instead of accumulating notification debt. The array is compacted only after the consumed prefix reaches 1,024 entries and at least half the array. ACK/fail and every path that releases concurrency/group ownership notify the released job's queue, so an unrelated queue cannot steal the wake-up.

Long-poll waiters also accept an optional `AbortSignal`. TCP creates one signal
per connection and aborts it before disconnect-time lease release. Aborting
removes the waiter and timer without creating a pending notification. Pull
checks the signal before attempting a dequeue and immediately after acquiring
the shard lock, before rate/concurrency acquisition. Thus cancellation while
queued on the lock consumes no limiter token. If cancellation lands at the
handoff boundary, the final guard requeues the job and releases
group/concurrency ownership before persistence, pulled counters, events, or
lock-token creation. The same contract applies to single, batch, owner-lock,
detached CLI, and durable recovery paths.

## Edge Cases & Failure Modes

- **Idempotent push**: a live `customId` returns the existing job (no insert). A recycled terminal `customId` evicts the stale completed row or DLQ entry first, including persisted terminal state and counters, so state queries expose only the new generation; timeout markers are cleared too (#33/#75).
- **Dedup of active jobs**: default strategy treats an active job (in `jobIndex`, not in queue) as a duplicate and returns its id without inserting (`push.ts:200`); the returned placeholder carries the correct existing id (`push.ts:286`).
- **Pull loss prevention**: `requeueJob` restores any job that fails to move to processing.
- **Expired (TTL) jobs** are silently dropped during pull (`isExpired`) and never delivered. Their SQLite row (or pending buffered INSERT) is deleted before the heap/counter/index removal, preventing restart resurrection while keeping the shard critical section synchronous.
- **markActive / persistence errors** during pull are swallowed — in-memory `processingShards` is the source of truth; SQLite recovery reconciles on restart.
- **Late / stale ACK**: `ackJob` throws when the job is no longer in processing. `QueueManager` recovers via `completeStallRetriedJob` to prevent duplicate execution (#33/#75), EXCEPT when the job is in `timedOutJobs` (a timeout sweep re-queued it for retry, which must win — the late ack is discarded, `queueManager.ts:372`).
- **Retry vs. terminal**: `canRetry` uses `attempts < maxAttempts` after `attempts++`; `unrecoverable=true` (from `failJob`) forces the terminal path regardless of attempts.
- **Stack trace persistence** (#74): the last failure's stack is normalized and capped at `stackTraceLimit` before branching; an absent stack (old clients) leaves any prior stack intact; `stackTraceLimit: 0` yields `null`.
- **Memory bounds**: `timeline` is capped at 20 entries (older transitions are not recorded once full). `completedJobs`, `jobResults`, `customIdMap` are bounded LRU/FIFO collections (eviction in [Core Queue Engine](./core-queue-engine.md) / [Background Tasks](./background-tasks.md)).
- **removeOnComplete + dependencies**: the full job is dropped but its bare ID
  enters the two-tier `depCompletions` tracker. Proofs referenced by
  `waitingDeps` stay pinned even when a batch is larger than
  `maxCompletedJobs`; after the last reverse edge is durably resolved or
  removed, the proof becomes recent and ordinary FIFO pruning applies.
- **Repeatable jobs**: re-scheduled via `onRepeat` only while `repeat.limit` is undefined or `repeat.count < limit`.

## Configuration

These operations read no environment variables directly; behavior is driven by per-job `JobInput` options and constants:

| Option / constant | Default | Effect |
| --- | --- | --- |
| `JobInput.maxAttempts` | `3` (`JOB_DEFAULTS`) | Retry ceiling (`canRetry`). |
| `JobInput.backoff` | `1000` ms | Base retry delay; object form selects `fixed`/`exponential`. |
| `JobInput.priority` | `0` | Higher = dequeued sooner; >0 sets `prioritized` timeline state. |
| `JobInput.delay` | `0` | Adds to `runAt`; >0 → `delayed`. |
| `JobInput.ttl` | `null` | Expiry; expired jobs skipped on pull. |
| `JobInput.timeout` | `null` | Processing timeout (enforced by stall/timeout sweep, not here). |
| `JobInput.removeOnComplete` / `removeOnFail` | `false` | Drop job on success / failure. |
| `JobInput.durable` | `false` | Bypass the ~10 ms write buffer (immediate disk write). |
| `JobInput.stackTraceLimit` | `10` | Max stored stack lines. |
| `DEFAULT_MAX_BACKOFF` | `3_600_000` ms | Backoff cap. |
| `MAX_TIMELINE_ENTRIES` | `20` | Timeline cap. |
| `pullJob` `timeoutMs` | `0` (no wait) | Long-poll deadline; Worker `pollTimeout` max 30 000 ms. |
| `DEFAULT_LOCK_TTL` | `30_000` ms | Lock duration (used by `pullWithLock`, not the raw pull). |

## Related Docs

- [Core Queue Engine](./core-queue-engine.md) — QueueManager, shards, `releaseJobResources`.
- [Data Structures](./data-structures.md) — PriorityQueue, heaps, indexes.
- [Concurrency & Locking](./concurrency-and-locking.md) — RWLock, lock hierarchy, grace window.
- [Persistence](./persistence.md) — WriteBuffer, durable writes, recovery.
- [Dead Letter Queue](./dead-letter-queue.md) — terminal failure handling.
- [Deduplication & Unique Jobs](./deduplication-and-unique.md) — uniqueKey / customId.
- [FlowProducer & Job Dependencies](./flow-producer.md) — `dependsOn`, `waiting-children`, parent propagation.
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md) — pull-time gating.
- [Client SDK: Worker](./client-worker-sdk.md) — locks, heartbeats, long poll.
- [TCP Server Command Handlers](./tcp-server-handlers.md) — `PUSH`/`PULL`/`ACK`/`FAIL` routing.
- [Background Tasks](./background-tasks.md) — stall/timeout sweeps, cleanup.
- [architecture](../architecture.md) · [data-model](../data-model.md)
