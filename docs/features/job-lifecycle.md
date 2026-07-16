# Job Lifecycle (push / pull / ack / fail)

> **Category:** Jobs · **Source:** `src/application/operations/push.ts`, `src/application/operations/pull.ts`, `src/application/operations/ack.ts`, `src/application/operations/ackHelpers.ts`, `src/application/operations/jobStateTransitions.ts`, `src/domain/types/job.ts`, `src/domain/queue/waiterManager.ts`

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

Exported context interfaces: `PushContext` (`push.ts:25`), `PullContext` (`pull.ts:23`), `AckContext` (`ack.ts:35`), plus the batch helpers `ExtractedJob`/`BatchContext`/`FinalizeContext` and `groupByProcShard`/`extractJobs`/`groupByQueueShard`/`releaseResources`/`finalizeBatchAck` in `ackHelpers.ts`.

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
2. **customId idempotency** (`handleCustomId`, `push.ts:61`): if `input.customId` maps to a job still in the queue, skip and return the existing job. On reuse where the prior job COMPLETED, the surviving completed row is evicted from `completedJobs`/`completedJobsData`/`jobResults`/`jobIndex` and disk (`push.ts:105`) so the recycled id starts fresh as `waiting` (#92); any stale timeout marker is cleared (`push.ts:124`) to avoid resurrecting the #33/#75 duplicate-execution guard.
3. `createJob(id, queue, input, now)`.
4. **Deduplication** (`handleDeduplication`, `push.ts:133`): only if `job.uniqueKey` is set. Strategies — `replace` (remove old, register new), `extend` (reset TTL, return existing), default BullMQ-style (return existing if it is still waiting or active; broadcast `Duplicated`). If the existing job is completed/failed, a fresh insert is allowed.
5. **Insert** (`insertJobToShard`, `push.ts:211`): if `dependsOn` has unmet entries (not all in `completedJobs`/`depCompletions`), the job goes to `shard.waitingDeps` and dependencies are registered (timeline `waiting-children`). Otherwise it is pushed to the priority queue with `incrementQueued`; initial timeline state is `delayed` (if `runAt > now`), `prioritized` (if `priority > 0`), else `waiting`. `jobIndex` is set to `{ type: 'queue', shardIdx, queueName }`.
6. `shard.notify(queue)` wakes one long-poll waiter for that queue, or records one coalesced retry hint when none is waiting.
7. After the lock: persist via `storage.insertJob(job, input.durable)`, bump counters, broadcast `pushed`. **Durable** jobs bypass the 10 ms write buffer (immediate write); `pushJobBatch` (`push.ts:323`) splits durable jobs into a separate `insertJobsBatch(durableJobs, true)` so `addBulk` does not silently downgrade the durability guarantee (`push.ts:384`).

### PULL (`pullJob`, `pull.ts:210`)

1. `deadline = timeoutMs > 0 ? now + timeoutMs : 0`. Loop:
2. `tryPullFromShard` (`pull.ts:253`) under `shardLocks[idx]`: return `null` if `state.paused`; then `tryAcquireRateLimit` and `tryAcquireConcurrency` gate (each rejection emits a dashboard event and returns null). Then loop `tryDequeueNextJob`.
3. `tryDequeueNextJob` (`pull.ts`): inspect jobs in priority order. Expired entries are discarded. Delayed jobs and jobs whose FIFO group is already active are temporarily set aside while the scan looks for another eligible job, then restored before returning. The first eligible job is activated and moved to `processingShards`; this makes the pull work-conserving across groups (for order A1, A2, B1, an active A group no longer hides B1). The queue pop, processing insert, and `jobIndex` flip remain in the same synchronous shard critical section.
4. `finalizeProcessing` (`pull.ts:132`): `storage.markActive(...)` (non-fatal on error — in-memory is source of truth), bump counters, broadcast `pulled`. Returns `false` when the job is no longer in `processingShards` — a management op (`discardJob`, `moveJobToDelayed`) claimed it between the dequeue and the handoff; the pull then does NOT deliver it to a worker (`pullJob` tries the next job, `pullJobBatch` drops it from the delivered set).
5. If no job and deadline not reached, `await shard.waitForJob(queue, remaining)` and loop; otherwise return `null`.

`pullJobBatch` (`pull.ts:290`) pulls up to `count` jobs in one shard lock, acquiring a rate-limit + concurrency slot per job and releasing the slot when a dequeue yields `stop`/`skip` (`pull.ts:371`); `finalizeProcessingBatch` (`pull.ts:158`) returns only the jobs actually delivered.

**Safety net:** if `finalizeProcessing` throws, `requeueJob` (`pull.ts:175`) restores the job with the same observer-atomicity rule in reverse: under `shardLocks[idx]` it releases the group + concurrency slot, resets `startedAt = null`, pushes the job back to the queue AND flips `jobIndex` back to `'queue'` in one critical section, then (guarded, under `processingLocks[procIdx]`) removes the `processingShards` entry only if the index no longer says `'processing'` (a concurrent pull may have re-popped it). It skips entirely if a management op already claimed the job. Lock order shard -> processing matches the documented hierarchy.

### ACK (`ackJob`, `ack.ts:75`)

1. Under `processingLocks[procIdx]`, remove the job from `processingShards`; if absent, **throw** `Job not found or not in processing state` (the `QueueManager` catches this to recover stall-retried jobs — see Edge Cases).
2. Under `shardLocks[idx]`, `releaseJobResources(queue, uniqueKey, groupId)` (frees concurrency slot, uniqueKey, group).
3. Release `customId` from `customIdMap` so it can be reused.
4. If `!removeOnComplete`: set `completedAt`, append `completed` timeline, add to `completedJobs` + `completedJobsData`, store result in `jobResults` and `storage.storeResult` (only if `result !== undefined`), set `jobIndex` to `completed`, `storage.markCompleted`. If `removeOnComplete`: delete from `jobIndex`, `storage.deleteJob`, and record a bare id in `depCompletions` so dependents still unblock without the job surfacing in queries (`ack.ts:122`).
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

`ackJobBatch`/`ackJobBatchWithResults` short-circuit to parallel per-job `ackJob` for ≤4 ids (`ack.ts:309`). Larger batches: `groupByProcShard` → `extractJobs` (one lock per processing shard, parallel) → `groupByQueueShard` → `releaseResources` (one lock per queue shard) → `finalizeBatchAck`. **Invariant** (`ackHelpers.ts:215`): for each completed job, `jobResults.set` happens BEFORE `completedJobs.add`, so any dependent observing `completedJobs.has(id)` always finds the result available.

### Manual transitions (`jobStateTransitions.ts`)

`moveActiveToWait` (`:16`): processing → queue (`runAt = now`, `startedAt = null`, release resources, push, broadcast `waiting` prev `active`). `changeWaitingDelay` (`:59`): updates `runAt` of an in-queue job via `q.updateRunAt`. `moveToWaitingChildren` (`:84`): processing → `shard.waitingChildren` (release resources; jobIndex stays `queue`-typed).

## Concurrency & Locking

Locks are per-shard `RWLock`s acquired via `withWriteLock` and are **sequential, not nested** within each operation, which avoids deadlock despite differing acquisition orders:

- `pushJob`: `shardLocks[idx]` only (persistence + broadcast happen after release).
- `pullJob`: `shardLocks[idx]` (dequeue) released, then `processingLocks[procIdx]` (move).
- `ackJob` / `failJob`: `processingLocks[procIdx]` (extract) released, then `shardLocks[idx]` (release resources / requeue / DLQ).

Batch ack acquires each processing-shard lock and each queue-shard lock at most once, in parallel across distinct shards (`O(shards)` not `O(n)` — `ack.ts:303`).

Lock-token verification, the **#101 grace window** (`isExpiredButOwned`: an expired-but-still-ours lock on a still-processing job is accepted, not lost), and stall-retry recovery live in `QueueManager.ack`/`fail` (`queueManager.ts:350`, `:496`), wrapping these raw functions. `WaiterManager` partitions waiters by queue, tracks the active count in O(1), and consumes entries through a head cursor. Notifications clear the waiter's timer immediately; surplus notifications coalesce into one edge-triggered `pending` bit instead of accumulating notification debt. The array is compacted only after the consumed prefix reaches 1,024 entries and at least half the array. ACK/fail and every path that releases concurrency/group ownership notify the released job's queue, so an unrelated queue cannot steal the wake-up.

## Edge Cases & Failure Modes

- **Idempotent push**: a queued `customId` returns the existing job (no insert). A recycled completed `customId` evicts the stale completed row first (#92) so state queries don't wrongly report `completed`, and clears any timeout marker (#33/#75).
- **Dedup of active jobs**: default strategy treats an active job (in `jobIndex`, not in queue) as a duplicate and returns its id without inserting (`push.ts:200`); the returned placeholder carries the correct existing id (`push.ts:286`).
- **Pull loss prevention**: `requeueJob` restores any job that fails to move to processing.
- **Expired (TTL) jobs** are silently dropped during pull (`isExpired`) and never delivered.
- **markActive / persistence errors** during pull are swallowed — in-memory `processingShards` is the source of truth; SQLite recovery reconciles on restart.
- **Late / stale ACK**: `ackJob` throws when the job is no longer in processing. `QueueManager` recovers via `completeStallRetriedJob` to prevent duplicate execution (#33/#75), EXCEPT when the job is in `timedOutJobs` (a timeout sweep re-queued it for retry, which must win — the late ack is discarded, `queueManager.ts:372`).
- **Retry vs. terminal**: `canRetry` uses `attempts < maxAttempts` after `attempts++`; `unrecoverable=true` (from `failJob`) forces the terminal path regardless of attempts.
- **Stack trace persistence** (#74): the last failure's stack is normalized and capped at `stackTraceLimit` before branching; an absent stack (old clients) leaves any prior stack intact; `stackTraceLimit: 0` yields `null`.
- **Memory bounds**: `timeline` is capped at 20 entries (older transitions are not recorded once full). `completedJobs`, `jobResults`, `customIdMap` are bounded LRU/FIFO collections (eviction in [Core Queue Engine](./core-queue-engine.md) / [Background Tasks](./background-tasks.md)).
- **removeOnComplete + dependencies**: the full job is dropped but its bare id is added to `depCompletions` so dependents still gate correctly (`ack.ts:122`, `ackHelpers.ts:250`).
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
