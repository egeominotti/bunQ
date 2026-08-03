# Job Lifecycle (push / pull / ack / fail)

> **Category:** Jobs · **Source:** `src/application/operations/push.ts`, `src/application/operations/pushBatch.ts`, `src/application/operations/pushAdmission.ts`, `src/application/operations/pull.ts`, `src/application/operations/pullStateTransition.ts`, `src/application/operations/ack.ts`, `src/application/operations/ack/`, `src/application/operations/ackHelpers.ts`, `src/application/operations/jobStateTransitions.ts`, `src/application/dependencyCompletions.ts`, `src/domain/types/jobs/model.ts`, `src/domain/job/`, `src/domain/queue/waiterManager.ts`

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
- Lock token creation/verification, stall sweeps, and timeout deadline scheduling — see [Worker Registry & Management](./workers-management.md) and [Background Tasks](./background-tasks.md). The `QueueManager` wraps these operations with lock verification and stall-retry recovery; the raw functions here are lock-agnostic.
- DLQ storage and retry policy — see [Dead Letter Queue](./dead-letter-queue.md).
- Dependency-graph processing after completion — see [FlowProducer & Job Dependencies](./flow-producer.md).
- Persistence batching / WAL — see [Persistence](./persistence.md).

## Dependencies

Internal:

- `src/domain/types/job.ts` is the stable facade: model types live in `src/domain/types/jobs/model.ts`, while creation, ids, locks, payload normalization, state predicates/backoff, and constants are split under `src/domain/job/`.
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

// pushBatch.ts (re-exported by push.ts)
export async function pushJobBatch(queue: string, inputs: JobInput[], ctx: PushContext): Promise<JobId[]>;

// pull.ts
export async function pullJob(queue: string, timeoutMs: number, ctx: PullContext): Promise<Job | null>;
export async function pullJobBatch(queue: string, count: number, timeoutMs: number, ctx: PullContext): Promise<Job[]>;

// ack/completion.ts, ack/failure.ts, ack/batch.ts
export async function ackJob(jobId: JobId, result: unknown, ctx: AckContext, options?: CompletionOptions): Promise<boolean>;
export async function failJob(jobId: JobId, error: string | undefined, ctx: AckContext, options?: FailJobOptions): Promise<void>;
export async function ackJobBatch(jobIds: JobId[], ctx: AckContext, leaseTokens?: Array<string | undefined>): Promise<boolean[]>;
export async function ackJobBatchWithResults(items: Array<{ id: JobId; result: unknown; token?: string } & CompletionOptions>, ctx: AckContext): Promise<boolean[]>;

// jobStateTransitions.ts
export async function moveActiveToWait(jobId: JobId, ctx: JobManagementContext): Promise<boolean>;
export async function changeWaitingDelay(jobId: JobId, delay: number, ctx: JobManagementContext): Promise<boolean>;
export async function moveToWaitingChildren(jobId: JobId, ctx: JobManagementContext): Promise<boolean>;
```

Exported context interfaces: `PushContext` (`pushContext.ts`, re-exported by `push.ts`), `PullContext`
(`pullStateTransition.ts`, re-exported by `pull.ts`), `AckContext` (`src/application/types/ack.ts:12-49`, re-exported by the `ack.ts` facade),
plus the batch helpers `ExtractedJob`/`BatchContext`/`FinalizeContext` and
`groupByProcShard`/`extractJobs`/`groupByQueueShard`/`releaseResources`/
`finalizeBatchAck` in `ackHelpers.ts`.

Job-type helpers re-exported from the `src/domain/types/job.ts` facade: `createJob`, `generateJobId`, `jobId`, `calculateBackoff`, `canRetry`, `isReady`, `isDelayed`, `isExpired`, `isTimedOut`, `normalizeStacktrace`, `createJobLock`, `renewLock`, `isLockExpired`, and the `JobState` const enum.

TCP commands handled (routed in `src/infrastructure/server/handler-routes/jobs.ts:52-76`): **`PUSH`**, **`PUSHB`**, **`PULL`**, **`PULLB`**, **`ACK`**, **`ACKB`**, **`FAIL`**. The batch commands (`PUSHB`/`PULLB`/`ACKB`) map to `pushJobBatch`/`pullJobBatch`/`ackJobBatch`(`WithResults`).

Events broadcast (`EventType`): `pushed`, `pulled`, `completed`, `failed`, `Retried`, `Duplicated`, plus `waiting` (from `moveActiveToWait`). Dashboard-only events: `job:waiting-children`, `job:expired`, `job:deduplicated`, `batch:pushed`, `batch:pulled`, `dlq:added`, `flow:failed`, `ratelimit:rejected`, `concurrency:rejected`.

## Data Models

See [data-model](../data-model.md) for full definitions. The central shape is `Job` (`src/domain/types/jobs/model.ts:41-90`). Most relevant fields for the lifecycle:

- Scheduling: `runAt` (createdAt + delay; also the next-retry timestamp), `startedAt`, `completedAt`.
- Retry: `attempts`, `maxAttempts` (default `3`), `backoff` (default `1000` ms), `backoffConfig` (`{ type: 'fixed' | 'exponential'; delay; maxDelay? }`).
- Lifetime: `ttl`, `timeout`, `removeOnComplete`, `removeOnFail`.
- Dedup / identity: `uniqueKey`, `customId`.
- Dependencies / flow: `dependsOn`, `parentId`, `failParentOnFailure`, `removeDependencyOnFailure`, `ignoreDependencyOnFailure`, `continueParentOnFailure`.
- Failure: `stacktrace` (capped at `stackTraceLimit`, default `10`).
- `timeline: JobTimelineEntry[]` — capped at `MAX_TIMELINE_ENTRIES = 20` and persisted as a MessagePack BLOB on lifecycle transitions, so it survives SQLite recovery.

`JobInput` (`src/domain/types/jobs/model.ts:92-137`) is the wire/SDK input; `createJob` (`src/domain/job/create.ts:77-118`) fills defaults from `JOB_DEFAULTS` (`src/domain/job/constants.ts:5-13`). Note `removeOnComplete`/`removeOnFail` are coerced via `toBoolean` because the wire boundary is not runtime-type-safe (#90, `src/domain/job/create.ts:34-45`).

`JobState` enum values: `waiting`, `prioritized`, `delayed`, `active`, `completed`, `failed` (`src/domain/types/jobs/model.ts:4-11`). The additional logical states `waiting-children` (dependency gate) and `paused` (queue-level) are represented via shard membership (`waitingDeps`/`waitingChildren`) and `QueueState.paused`, not the enum.

## Business Logic / Control Flow

### PUSH (`pushJob`, `src/application/operations/push.ts`)

1. Compute `idx = shardIndex(queue)` and acquire every target, custom-ID owner,
   and parent shard write lock required by the request in ascending order.
2. **Inspect custom-ID idempotency** (`handleCustomId`, `customId.ts`). A live
   generation is an idempotent skip. Reusing a completed, DLQ, or
   payload-free dependency-completion generation produces a retirement plan;
   it does not delete the old generation yet.
3. Create the candidate job, then inspect unique-key deduplication
   (`pushDeduplication.ts`). Default/extend suppression returns the existing id.
   Replace records the exact pending or active owner without changing heap,
   index, counter, or key ownership.
4. Prepare the candidate's initial state and any removed-completion pins without
   publishing them (`pushInsert.ts`). A normal candidate is
   `waiting`/`prioritized`/`delayed`, while an unresolved dependency candidate
   is `waiting-children`. A `parentId` also prepares the child/parent topology.
5. **Commit persistence before visibility whenever admission can reject.** A
   durable insert, terminal-ID retirement, completion pin, dedup replacement,
   active-key transfer, or parent link goes through the matching SQLite
   admission transaction. Terminal cleanup (`jobs`, result, DLQ,
   dependency-completion and flow-failure rows), completion pins, the candidate
   insert, and parent updates commit together where applicable. If SQLite
   rejects the transaction, including `SQLITE_FULL`, the old generation and all
   RAM structures remain unchanged and the candidate is neither queryable nor
   executable.
6. Publish the committed plan to RAM: retire the old custom-ID generation,
   transfer dedup ownership, install dependency pins/edges, insert into the heap
   or wait set, update `jobIndex` and counters, then notify the queue. This
   publication contains no expected throwing persistence operation. Dashboard
   callbacks are guarded so observability cannot turn an accepted job into a
   client-visible rejection.
7. Plain non-durable inserts without admission metadata keep the throughput
   path: publish in RAM and enqueue the row in the 10 ms `WriteBuffer`. Durable
   inserts bypass that buffer. Only accepted jobs increment push telemetry and
   emit `pushed`.

`pushJobBatch` lives in `src/application/operations/pushBatch.ts` and preserves
ordered accepted-prefix semantics. Each item completes the same admission
sequence before the next item begins. If item N is rejected, items before N
remain accepted and recoverable according to their durability mode; item N and
all later items remain absent. A rejection of the first durable item therefore
leaves no phantom batch in either Embedded or TCP mode.

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

### ACK (`ackJob`, `src/application/operations/ack/completion.ts:11-93`)

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

### FAIL (`failJob`, `src/application/operations/ack/failure.ts:63-175`)

1. Extract from `processingShards` (throw if absent, as above).
2. `attempts++`; if a `stack` is supplied, store `normalizeStacktrace(stack, stackTraceLimit)` BEFORE branching so both a retry and a DLQ entry carry it (#74, `ack/failure.ts:79-84`). Append `failed` timeline entry.
3. Under `shardLocks[idx]`, `releaseJobResources`, then branch:
   - **Retry** (`!unrecoverable && canRetry(job)`, i.e. `attempts < maxAttempts`): `runAt = now + calculateBackoff(job)`, push back to queue, `incrementQueued(..., isDelayed=true, ...)`, `storage.updateForRetry`, set `wasRetried`. Appends a `waiting` timeline entry for the next attempt.
   - **removeOnFail**: delete from index + disk, bump `totalFailed`, release customId.
   - **Terminal → DLQ** (`moveFailedJobToDlq`, `ack/failure.ts:17-43`): `addToDlq(job, MaxAttemptsExceeded, error)`, set `jobIndex` to `dlq`, atomically commit the DLQ row plus job-row removal (and any flow-failure outbox record), bump `totalFailed`, emit `dlq:added` (+ `flow:failed` if `parentId`).
4. Broadcast `failed`; if retried, also broadcast `Retried` (prev `failed`).
5. Flow propagation when NOT retried: `failParentOnFailure` → `onChildTerminalFailure`; `removeDependencyOnFailure`/`ignoreDependencyOnFailure`/`continueParentOnFailure` → `onChildDependencyOption`.

`calculateBackoff` (`src/domain/job/state.ts:37-54`): fixed = `delay * (0.8 + rand*0.4)` (±20% jitter); exponential / default = `base * 2^attempts * (0.5 + rand)` (±50% jitter), capped at `backoffConfig.maxDelay ?? DEFAULT_MAX_BACKOFF` (1 h).

### Batch ack (`ackHelpers.ts`)

`ackJobBatch`/`ackJobBatchWithResults` short-circuit to parallel per-job
`ackJob` for ≤4 ids. Larger batches: `groupByProcShard` → `extractJobs` (one
lock per processing shard, parallel) → `groupByQueueShard` →
`releaseResources` (one lock per queue shard) → `finalizeBatchAck`.
Both paths return one extraction boolean per input position. `QueueManagerAck`
classifies every `false` only after the processing lock: an exact retired
timeout/cron generation becomes `ignored`, while an arbitrary missing job or
wrong token remains an error. Positional evidence prevents duplicate job IDs
from hiding which generation was retired.
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

Batch ack acquires each processing-shard lock and each queue-shard lock at most once, in parallel across distinct shards (`O(shards)` not `O(n)` — `src/application/operations/ack/batch.ts:23-74`).

Lock-token verification and stall-retry recovery live in
`queue-manager/ack.ts`; the shared `assertLeaseToken` generation check lives in
`queue-manager/delivery.ts`. Whenever a lease record exists, ACK, FAIL, both
ACKB forms, and active manual moves require its exact token in embedded and TCP
mode. Batch ownership is preflighted before the first extraction. An expired
but still-current token is accepted (#101), while a token from an older
processing generation is rejected. With no lease, an administrative active
transition remains valid. `WaiterManager` partitions waiters by queue, tracks
the active count in O(1), and consumes entries through a head cursor.
Notifications clear the waiter's timer immediately; surplus notifications
coalesce into one edge-triggered `pending` bit instead of accumulating
notification debt. The array is compacted only after the consumed prefix
reaches 1,024 entries and at least half the array. ACK/fail and every path that
releases concurrency/group ownership notify the released job's queue, so an
unrelated queue cannot steal the wake-up.

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
- **Dedup of active jobs**: default strategy treats an active job (in `jobIndex`, not in queue) as a duplicate and returns its id without inserting (`src/application/operations/pushDeduplication.ts:96-110`); the returned placeholder carries the correct existing id (`src/application/operations/push.ts:76-85`).
- **Pull loss prevention**: `requeueJob` restores any job that fails to move to processing.
- **Expired (TTL) jobs** are silently dropped during pull (`isExpired`) and never delivered. Their SQLite row (or pending buffered INSERT) is deleted before the heap/counter/index removal, preventing restart resurrection while keeping the shard critical section synchronous.
- **markActive / persistence errors** during pull are swallowed — in-memory `processingShards` is the source of truth; SQLite recovery reconciles on restart.
- **Late processor outcomes**: the timeout transition records the exact claimed
  processing generation (`jobId`, `startedAt`, and lease token when present)
  while holding the processing lock. ACK and FAIL classify again after their
  own claim point, closing the validation-to-claim race. An exact retired
  timeout or lock-expired `cron:` lease returns
  `{ applied: false, reason: 'already-finalized' }`; ACKB additionally returns
  ordered `ignoredIds` and `ignoredIndices`. Workers suppress local terminal
  events for those outcomes. Missing IDs, missing/wrong tokens, and duplicate
  outcomes against ordinary completed jobs still throw and cannot release a
  newer lease.
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
| `JobInput.timeout` | `null` | Processing timeout enforced by the next-deadline scheduler. |
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
- [Background Tasks](./background-tasks.md) — timeout deadlines, stall sweeps, cleanup.
- [architecture](../architecture.md) · [data-model](../data-model.md)
