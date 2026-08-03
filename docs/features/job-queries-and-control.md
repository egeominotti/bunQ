# Job Queries & Queue Control

> **Category:** Jobs · **Source:** `src/application/operations/query/`, `src/application/operations/queryOperations.ts`, `src/application/operations/jobManagement.ts`, `src/application/operations/jobMoveOperations.ts`, `src/application/operations/jobClaim.ts`, `src/application/operations/queueControl.ts`, `src/shared/pausedView.ts`

## Purpose

This module is the read/control surface of the `QueueManager`. It answers point and list queries about jobs (by id, by custom id, by state, results, progress, counts), mutates individual jobs that are not yet completed (cancel, promote, discard, re-prioritize, re-delay, update data/progress), and drives queue-wide lifecycle operations (pause, resume, drain, obliterate, clean). It exists to keep all of this logic as pure functions that operate on an explicit context object (`shards`, `jobIndex`, locks, storage) rather than baking it into the manager, so the same code paths serve embedded callers, the TCP server, and the HTTP/MCP backends identically.

## Responsibilities & Scope

Owns:

- Job lookups across every location: in-shard run queue, `waitingDeps`/`waitingChildren` maps, processing shards, completed cache, and DLQ — plus SQLite fallback after restart (`src/application/operations/query/jobLookup.ts:6-59`).
- Result lookup with a stable public missing-value contract across memory and
  SQLite: retained `null` remains a real result; an ID with no result is
  `undefined`.
- State resolution including BullMQ v5 distinctions (`waiting` vs `prioritized` by priority, `delayed` by `runAt`, `waiting-children`) and the paused-view derivation (`pausedView.ts`).
- Paginated, multi-source, state-filtered list queries with offset-correct merging (`getJobs`, `src/application/operations/query/pagination.ts:46-138`).
- Single-job mutations on non-terminal jobs: `cancelJob`, `updateJobProgress`, `updateJobData`, `changeJobPriority`, `promoteJob` (`jobManagement.ts`), plus `moveJobToDelayed` and `discardJob` (`jobMoveOperations.ts`).
- Queue control: `pauseQueue`, `resumeQueue`, `isQueuePaused`, `drainQueue`, `obliterateQueue`, `cleanQueue`, `getQueueCount`, `listAllQueues` (`queueControl.ts`).

Does NOT own:

- Push/pull/ack/fail lifecycle transitions — see [Job Lifecycle](./job-lifecycle.md).
- Per-state numeric aggregation (`getQueueJobCounts`) — lives in `statsManager`, see [Stats, Metrics & Monitoring](./stats-and-monitoring.md). This module only consumes those counts through `pausedView`.
- DLQ insertion mechanics and retry/purge — see [Dead Letter Queue](./dead-letter-queue.md). `discardJob` calls `shard.addToDlq` but the DLQ store/maintenance is elsewhere.
- The manager-level extra cleanup that `obliterate` requires (processing shards, completed/result/log/lock indexes, `customIdMap`, metrics, persisted queue state) — that lives in `QueueManagerControl.obliterate` (`queue-manager/control.ts`), not in `obliterateQueue` here.
- Counters/temporal index internals — owned by `Shard`, see [Core Queue Engine](./core-queue-engine.md) and [Data Structures](./data-structures.md).
- Wire framing, command dispatch, and response shaping — see [TCP Server Command Handlers](./tcp-server-handlers.md).

## Dependencies

Internal:

- [Core Queue Engine](./core-queue-engine.md) — `Shard` methods: `getQueue`, `getDlq`/`getDlqEntries`/`removeFromDlq`, `drain`, `obliterate`, `getOldJobs`, `decrementQueued`/`incrementQueued`, `removeFromTemporalIndex`, `releaseUniqueKey`, `unregisterDependencies`, `addToDlq`, `getCountsPerPriority`.
- [Concurrency & Locking](./concurrency-and-locking.md) — `RWLock`, `withReadLock`, `withWriteLock` from `src/shared/lock`.
- [Persistence](./persistence.md) — `SqliteStorage` (`getJob`, `getJobStateRaw`, `getDlqEntry`/`hasDlqEntry`, `getResult`, `queryJobs`, `deleteJob`, `deleteDlqEntry`, `saveDlqEntry`).
- [Data Structures](./data-structures.md) — `SetLike`/`MapLike` (LRU/Set abstractions) for `completedJobs`, `completedJobsData`, `jobResults`, `customIdMap`.
- `src/shared/hash` — `shardIndex`, `processingShardIndex`, `SHARD_COUNT`.
- [Webhooks, Events & Job Logs](./webhooks-and-events.md) — `EventsManager.broadcast`, `WebhookManager.trigger` (used by `updateJobProgress`, `cancelJob`, `moveJobToDelayed`).

External/runtime: Bun's `bun:sqlite` (via `SqliteStorage`), `Date.now()` for temporal comparisons. Zero third-party runtime deps.

## Public Interface

### Exported functions

`queryOperations.ts`:

```typescript
function getJob(jobId: JobId, ctx: QueryContext): Promise<Job | null>
function getJobResult(jobId: JobId, ctx: QueryContext): unknown
function getJobByCustomId(customId: string, ctx: QueryContext): Job | null
function getJobProgress(jobId: JobId, ctx: QueryContext): { progress: number; message: string | null } | null
function getJobState(jobId: JobId, ctx: QueryContext): Promise<JobState | 'unknown'>
function getJobs(queue: string, shardIdx: number,
  options: { state?: string | string[]; start?: number; end?: number; asc?: boolean },
  ctx: GetJobsContext): Job[]
```

`getJobResult` first uses the two in-memory result maps. On SQLite fallback,
`getResult()` alone is intentionally nullable, so `hasResult()` disambiguates a
stored `null` from a missing row. This normalization happens at the
QueueManager boundary and is shared by embedded calls, TCP `GetResult`,
`Queue.waitJobUntilFinished`, and FlowProducer result helpers. No truthiness
check is allowed because `0`, `false`, and `''` are valid results.

`jobManagement.ts`:

```typescript
function cancelJob(jobId: JobId, ctx: JobManagementContext): Promise<boolean>
function updateJobProgress(jobId: JobId, progress: number, ctx: JobManagementContext, message?: string): Promise<boolean>
function updateJobData(jobId: JobId, data: unknown, ctx: JobManagementContext): Promise<boolean>
function changeJobPriority(jobId: JobId, priority: number, ctx: JobManagementContext, lifo?: boolean): Promise<boolean>
function promoteJob(jobId: JobId, ctx: JobManagementContext): Promise<boolean>
function moveJobToDelayed(jobId: JobId, delay: number, ctx: JobManagementContext): Promise<boolean>
function discardJob(jobId: JobId, ctx: JobManagementContext): Promise<boolean>
```

`queueControl.ts`:

```typescript
function pauseQueue(queue: string, ctx: QueueControlContext): void
function resumeQueue(queue: string, ctx: QueueControlContext): void
function isQueuePaused(queue: string, ctx: QueueControlContext): boolean
function drainQueue(queue: string, ctx: QueueControlContext): number
function obliterateQueue(queue: string, ctx: QueueControlContext): JobId[]
function listAllQueues(ctx: QueueControlContext): string[]
function cleanQueue(queue: string, graceMs: number, ctx: QueueControlContext, state?: string, limit?: number): JobId[]
function getQueueCount(queue: string, ctx: QueueControlContext): number
```

`pausedView.ts`:

```typescript
interface PausedDerivedCounts { waiting: number; prioritized: number; paused: number }
function pausedView(waiting: number, prioritized: number, isPaused: boolean): PausedDerivedCounts
```

### Context types

`QueryContext`/`GetJobsContext` (`src/application/types/query.ts:9-25`), `JobManagementContext` (`src/application/operations/jobManagement.ts:59-74`, optional `repeatChain`), `QueueControlContext` (`queueControl.ts:14-26`). Callers pass these via `QueueManager.contextFactory.get*Context()`.

### TCP commands handled (via `QueueManager` wrappers)

- Query: `GetJob`, `GetState`, `GetResult`, `GetProgress`, `GetJobByCustomId`, `GetJobs`, `GetJobCounts`, `GetCountsPerPriority`, `Count` (`handlers/query.ts`, `handlers/management.ts`, `handlers/advanced.ts`).
- Job management: `Cancel`, `Progress`, `Promote`, `Discard`, `Update`, `ChangePriority`, `MoveToDelayed`, `ChangeDelay` (`handlers/management.ts`, `handlers/advanced.ts`).
- Queue control: `Pause`, `Resume`, `IsPaused`, `Drain`, `Obliterate`, `Clean`, `ListQueues` (`handlers/management.ts`, `handlers/advanced.ts`).

`ChangeDelay` and `MoveToDelayed` share the manager-level dispatcher in `queue-manager/job-management.ts`: for jobs already queued (`waiting`/`prioritized`/`delayed`) it calls `changeWaitingDelay` (in-place `runAt` mutation → the job becomes/stays `delayed`); for active (`processing`) jobs it falls back to the two-phase `moveJobToDelayed`. `QueueManager.moveToDelayed` delegates to `changeDelay` there so the two stay in lock-step. Previously `moveToDelayed` only handled `processing` jobs, so a waiting job was a **silent no-op over TCP/HTTP/MCP** while the embedded SDK special-cased it; routing through `changeDelay` fixes parity for waiting **and** active jobs. Wire fields: both commands carry a relative `delay` (ms), while the public `moveToDelayed(id, timestamp)` API takes an **absolute** timestamp and the TCP client (`jobMove.ts`) converts it with `delay = max(0, timestamp - now)`. Both commands also accept an optional `token`; the Worker binds its current lease automatically for `Job.changeDelay(delay)`, whose public signature has no token parameter. The HTTP route `POST /jobs/:id/move-to-delayed` sends no token, so its administrative active-job form succeeds only when the broker has no lock for that job.

For an active job, `MoveToDelayed`, `ChangeDelay`, `MoveToWait`, and
`MoveToWaitingChildren` use the same lease preflight as ACK/FAIL. A current
lock makes its exact token mandatory; missing/wrong tokens leave the active
job and its lock unchanged. When no lock exists, the same transitions are
available as administrative operations. Queue, Worker, Flow, DLQ, sandboxed,
TCP, and embedded proxies all forward the optional wire token consistently.
Because `Job.changeDelay()`, `Job.retry()`, and `Job.discard()` do not expose a
token parameter, processor-created Job objects capture the current Worker
delivery token and forward it implicitly in embedded and TCP modes. `Discard`
also accepts `token?: string` on the wire and `QueueManager.discard(id, token?)`
performs the same lease preflight before removing the job. A stale processor
therefore cannot dead-letter a newer generation; an active job without a lock
retains the administrative form.

### Events emitted

- `cancelJob` → `EventType.Removed` with `prev: 'waiting'` (`src/application/operations/jobManagement.ts:77-142`).
- `updateJobProgress` → `progress` event + `job.progress` webhook (`src/application/operations/jobManagement.ts:148-192`).
- `moveJobToDelayed` → `EventType.Delayed` (`jobMoveOperations.ts`).
- `QueueManager.pause`/`resume` wrap `pauseQueue`/`resumeQueue` and emit `EventType.Paused`/`EventType.Resumed` plus dashboard events `queue:paused`/`queue:resumed` (`queue-manager/control.ts`).
- Dashboard-only events from handlers: `job:priority-changed`, `job:promoted`, `job:discarded`, `job:data-updated`, `job:moved-to-delayed`, `queue:drained`, `queue:obliterated`, `queue:removed`, `queue:cleaned`.

`updateJobData` mutates and persists the payload while holding the job's owning
shard lock. This applies to queued jobs, active jobs, and the queued successor
resolved through `repeatChain`, so a successful `Update` response is durable
across broker restarts. SQLite flushes a pending buffered insert before updating
the data blob; otherwise an update performed immediately after a non-durable
push could match no row and the later insert would restore the original payload.

`changeJobPriority` updates the indexed heap and persists both the effective
`priority` and `lifo` tie-break while holding the owning shard lock. The
effective `lifo` value comes from the heap's replacement `Job`, so omitting the
optional flag preserves its current value on disk. As with other post-insert
mutations, SQLite first flushes a pending buffered insert. A successful
`ChangePriority` therefore retains identical scheduling order after broker
recovery.

## Data Models

See [data-model](../data-model.md) for full `Job` and the events shape. Most relevant here:

- `JobLocation` (`src/domain/types/queue.ts:122-126`) — the discriminated union stored in `jobIndex` that drives every dispatch:
  ```typescript
  | { type: 'queue'; shardIdx: number; queueName: string }
  | { type: 'processing'; shardIdx: number }
  | { type: 'completed'; queueName: string }
  | { type: 'dlq'; queueName: string }
  ```
- `JobState` (`src/domain/types/jobs/model.ts:4-11`): `waiting | prioritized | delayed | active | completed | failed`. `getJobState` may additionally return the literal `'waiting-children'` (jobs parked in `waitingDeps`/`waitingChildren`) and the sentinel `'unknown'`.
- `PausedDerivedCounts` (`pausedView.ts:14`): when paused, `waiting`+`prioritized` collapse into `paused` to avoid double-counting (#92). Same shape feeds `GetJobCounts` (`handlers/query.ts:57`), the client SDK, and the dashboard.

## Business Logic / Control Flow

### getJob (`src/application/operations/query/jobLookup.ts:6-59`)

1. Read `jobIndex.get(jobId)`. If absent, fall back to SQLite: `storage.getJob` then `storage.getDlqEntry`, finally `completedJobsData` — this keeps `getJob` working after a restart before the index is repopulated.
2. Otherwise dispatch on `location.type`. For `queue`, take a read lock and probe run queue → `waitingDeps` → `waitingChildren`. For `processing`, read under the processing lock. For `completed`, prefer SQLite then cache. For `dlq`, prefer the SQLite DLQ entry, then the in-memory DLQ shard.
3. **Stale-snapshot chase (false-null fix):** the location snapshot is only valid until the first await — while the reader waits on the shard read lock (writers have priority), a concurrent pull can pop the job and move it queue -> processing (the pull flips `jobIndex` atomically with the pop, see job-lifecycle.md). On a lookup miss, `getJob` re-reads the index: if the entry object changed identity, the job MOVED and the lookup retries at the fresh location (up to 4 passes, each requiring a further transition of that very job); if unchanged, the miss is genuine and `null` is returned. This preserves the invariant that `getJob(id) === null` is permanent for never-reused uuidv7 ids (no `JOB -> null -> JOB(active)` flicker). Index entries are always replaced, never mutated, so the identity comparison is exact.

### getJobState (`src/application/operations/query/state.ts:22-57`)

1. Fast path: `completedJobs.has(jobId)` → `Completed`.
2. No index entry → `resolveStateFromStorage` (`src/application/operations/query/state.ts:6-19`): DLQ → `Failed`; raw state `completed`/`active` map through; `waiting`/`delayed` load the row and decide `Delayed` (future `runAt`), else `Prioritized` (priority > 0) vs `Waiting`.
3. `queue` location → under read lock, classify by map membership and `runAt`/`priority`; `processing` → `Active`; `dlq` → `Failed`.
4. Same stale-snapshot chase as `getJob`: a `queue`-location miss with a changed index entry retries at the fresh location instead of reporting a false `'unknown'` mid-pull.

### getJobs (`src/application/operations/query/pagination.ts`)

The hard part is correct pagination when results come from multiple non-offset-aware sources (SQLite jobs table + in-memory DLQ, `waiting-children` maps, and the paused view).

- Normalizes `state` (string | string[] | empty → `null` = unfiltered).
- Storage path: SQL applies the requested logical-state predicate, stable ordering, and pagination together. `waiting`, `prioritized`, and `delayed` are translated to predicates over persisted `state`, `run_at`, and `priority`; ordering is `(created_at, id)` in the requested direction and uses the schema-v14 queue indexes.
- If a derived source contributes (DLQ, paused jobs, or `waiting-children`), every source is gathered from index zero, deduplicated by job ID, globally sorted by `(createdAt, id)`, and sliced `[start, end)` exactly once. Pushing an offset into only one source would drop or duplicate rows across pages.
- Dependency-gated jobs in `waitingDeps` may retain their ready-state row, while jobs explicitly moved into `waitingChildren` persist `state='waiting-children'`. In-memory membership is authoritative while running; their IDs are deduplicated against SQL before global pagination, and they appear only when that logical state is requested.
- Paused semantics: `resolveStateNeeds` (`src/application/operations/query/collect.ts:96-109`) suppresses explicit `waiting`/`prioritized` queries on a paused queue; those jobs are returned only under `paused` (`src/application/operations/query/collect.ts:111-149`). An unfiltered query still lists them by their temporal state.
- In-memory path (embedded, no storage): `collectJobsByState` gathers every matching source before sorting and slicing; it never truncates insertion order before applying descending order.

### cancelJob (`jobManagement.ts:30`)

Only acts on `type === 'queue'`. Under a write lock it tries three locations in order, each with different bookkeeping:

1. Run queue (`:37`): `remove`, `decrementQueued`, release `uniqueKey`, delete index + SQLite row.
2. `waitingChildren` (`:49`): delete map entry + index + row — does NOT decrement (these were never counted as queued).
3. `waitingDeps` (`:61`, #102 fix): delete, `unregisterDependencies(jobId, dependsOn)`, release `uniqueKey`, delete index + row — no decrement (flow-chain dependents skip `incrementQueued`), but the uniqueKey reservation and dependency-index entries MUST be released here.

On success emits `Removed`. Returns `false` for active/completed/DLQ jobs.

### moveJobToDelayed (`jobMoveOperations.ts`)

Handles **active** (`processing`) jobs only. Two-phase: remove from the processing shard under `processingLocks[procIdx]` (`:238`), then re-push into the destination shard under `shardLocks[idx]`, resetting `startedAt = null`, `runAt = now + delay`, and calling `incrementQueued` with the temporal flag (`:255`). Inside the shard-lock section, before the re-push, it calls `shard.releaseJobResources(queue, uniqueKey, groupId, job.id)` and then `shard.notify()`, mirroring `moveActiveToWait`: the concurrency slot (+group+owned uniqueKey) acquired at pull is returned, otherwise `setConcurrency(N)` wedges after N moves (repro: `test/repro-slot-release-claim-paths.test.ts`). Emits `Delayed`. Jobs already **in the queue** (`waiting`/`prioritized`/`delayed`) never reach this op — the `QueueManager.moveToDelayed`/`changeDelay` dispatcher routes them to `changeWaitingDelay` (in-place `runAt` update). Like the embedded `changeDelay` path, that in-queue route does **not** emit a `Delayed` event nor bump the O(1) `delayedJobs` aggregate, but `getJobState`/`getJob` correctly report the job as `delayed` from its future `runAt` and it is no longer pullable. Both routes **persist** the new `run_at` via `storage.updateRunAt(jobId, runAt)` (re-deriving `state` from the future timestamp and clearing `started_at`), so the delay survives a restart — without it, recovery would reload the stale on-disk `run_at` (the active path's row would still read `state='active'`) and the job would be immediately pullable again.

### moveToWaitingChildren (`jobStateTransitions.ts`)

Claims an active job from its processing shard, then under the queue shard lock
releases its concurrency/group/owned unique-key resources, clears `startedAt`,
stores it in `waitingChildren`, updates `jobIndex`, appends the transition,
persists it through `markWaitingChildren`, and emits `waiting-children` once.
The synchronous critical section contains no `await`. The TCP command and all
public Queue/Worker/Job factories use this same transition, and restart
recovery preserves the parked row.

### discardJob (`jobMoveOperations.ts`)

Removes the job from its run queue (with `decrementQueued`) or from the processing shard, then under the destination shard lock releases the job's reservations, calls `addToDlq`, sets the index to `dlq`, persists the DLQ entry, and deletes the jobs-table row. The release is branch-specific: a **processing** job returns the full set via `releaseJobResources` (concurrency slot + group + uniqueKey, matching the fail-to-DLQ paths in `failJob` and `handleMaxStallsExceeded`, which free the reservation on DLQ entry) followed by `shard.notify()`; a **queued** job only releases its `uniqueKey` (parity with `cancelJob`), because it never acquired a slot or group at pull and a full release would free a slot held by another active job.

The manager wrapper authorizes the optional lease token before entering this
two-phase claim. Worker `Job.discard()` is public `void`, so the Worker records
and awaits the internal broker settlement: the processor's later return or
throw cannot publish a second terminal outcome, graceful close cannot tear down
the pool under the command, and duplicate calls share one transition.

### Queue control

- `drainQueue` (`queueControl.ts:45`): `shard.drain` returns `{count, jobIds}`; the operation then deletes each `jobIndex` entry and calls `safeDeleteJob` so a buffered/on-disk add cannot resurrect a drained job.
- `cleanQueue` (`queueControl.ts:188`): normalizes `wait`→`waiting`, dispatches to `cleanWaitingLike` (`:100`, also covers `delayed`/`prioritized`/`paused`/undefined), `cleanCompleted` (`:124`), or `cleanFailed` (`:152`). Each respects `graceMs` (age threshold) and `maxJobs` (default 1000). Returns removed `JobId[]`.
- `obliterateQueue` (`queueControl.ts:60`): calls `shard.obliterate(queue)`, which clears the run queue, DLQ, dependency-gated `waitingDeps`/`waitingChildren` jobs, reverse dependency registrations, unique keys, limiters, and temporal indexes. It returns every removed id so `QueueManagerControl.obliterate` can purge global indexes and SQLite even if a prior inconsistency left an id out of `jobIndex`. The manager also discovers and purges processing/completed/results/logs/locks/customIdMap/metrics/queue-state (`queue-manager/control.ts`).

## Concurrency & Locking

Lock acquisition follows the project hierarchy: `jobIndex` (plain `Map`, read without a lock) → `completedJobs` → `shards[N]` → `processingShards[N]`. Pattern throughout: read the index entry first, then acquire the matching `RWLock`.

- Queries use `withReadLock` on `shardLocks[idx]` / `processingLocks[idx]`. `getJobResult`, `getJobProgress`, `getJobByCustomId` read fast paths without shard locks.
- Mutations use `withWriteLock`. `changeJobPriority`, `promoteJob`, `updateJobData` (queue branch) take a single shard write lock.
- `moveJobToDelayed` and `discardJob` are **two-phase** (release processing lock, then acquire a shard lock). There is a window where the job is in neither structure; concurrent queries can briefly report it as `unknown`/not-found. This is accepted — the source job was already removed from processing, so no double-execution occurs.
- `queueControl.pauseQueue`/`resumeQueue`/`getQueueCount` delegate to `Shard` methods that manage their own internal state; no lock taken at this layer.
- Lock acquisition is bounded by `LOCK_TIMEOUT_MS` (default 5000ms) — see [Concurrency & Locking](./concurrency-and-locking.md).

## Edge Cases & Failure Modes

- **Post-restart recovery:** `getJob`/`getJobState` fall back to SQLite (jobs table + DLQ + raw state) when `jobIndex` has no entry (`src/application/operations/query/jobLookup.ts:9-16`, `src/application/operations/query/state.ts:6-19`). Without storage (pure embedded) they return `null`/`'unknown'`.
- **Pull-transition visibility:** during a pull, the queue pop, the `processingShards` insert, and the `jobIndex` flip happen in one synchronous critical section (`src/application/operations/pullStateTransition.ts:115-135`), and `getJob`/`getJobState` chase a moved location on miss — a poller can no longer observe a transient `null`/`'unknown'` for a job that is being handed to a worker (repro: `test/repro-getjob-false-null-during-pull.test.ts`).
- **Paused double-count avoidance (#92):** `pausedView` and `resolveStateNeeds` guarantee a single job is reported in exactly one bucket. Verified by the shared `pausedView` helper used across SDK/TCP/dashboard so surfaces cannot drift.
- **Pagination correctness:** logical-state filtering happens before SQL `LIMIT/OFFSET`; derived sources are merged and deduplicated before one final slice. `(created_at, id)` is the deterministic tie-breaker for both ascending and descending pages.
- **`cleanQueue` with `state='active'` is intentionally unsupported** (`queueControl.ts:189-216`): cleaning in-flight jobs would race the worker ack path and leak concurrency/uniqueKey/group slots. Use `cancelJob` or `fail` instead. Unknown states also return `[]`.
- **Idempotency / not-found:** all mutations return `false` (or `[]`) when the job is absent or in the wrong location; `cancelJob`/`promoteJob`/`changeJobPriority` only act on queued jobs, and `updateJobProgress` plus the low-level `moveJobToDelayed` claim operation require an active job. The public manager-level `moveToDelayed`/`changeDelay` dispatcher supports both queued and active jobs as described above. `promoteJob` no-ops if `runAt <= now` (already due).
- **Progress clamping and durability:** `updateJobProgress` clamps to `[0,100]`,
  preserves the prior message when a later update omits one, refreshes
  `lastHeartbeat`, and writes all three values through to SQLite while the
  processing lock is held. A confirmed update therefore survives active-job
  crash recovery. The progress webhook failure is caught and logged, never
  propagated.
- **Active-to-waiting durability:** `moveActiveToWait` persists `runAt`, derived
  `state='waiting'`, and `startedAt=NULL` after the in-memory claim/requeue.
  Restart therefore reloads the job as ready work without charging a phantom
  crash attempt.
- **`updateJobData` repeat-chain follow:** when the target id is completed/missing, it follows `repeatChain` to patch the successor job created by `handleRepeat` (`src/application/operations/jobManagement.ts:231-260`).
- **SQLite write failures swallowed:** `safeDeleteJob`/`safeDeleteDlqEntry` (`queueControl.ts:83`) catch errors (e.g. `SQLITE_FULL`); in-memory state is already cleared and the orphan row is GC'd by crash-recovery on restart.
- **Memory-bound visibility:** completed-job queries depend on bounded LRU/Set collections — `completedJobs` (50k), `jobResults` (10k), `jobLogs` (10k), `customIdMap` (50k). Once evicted, results/custom-id lookups fall back to SQLite or return `null`. `getJobByCustomId` returns `null` if the LRU has evicted the mapping.
- **Dependency-safe obliterate:** `shard.obliterate` removes queue-owned jobs from both `waitingDeps` and `waitingChildren`. Removing a `waitingDeps` job goes through `DependencyTracker.removeWaitingJob`, so its waiter id is also deleted from every reverse `dependencyIndex` entry. The returned id set then drives global-index and SQLite cleanup; no `waiting-children` ghost remains queryable after the operation.
- **Dependency counts on the public Queue client:** `Queue.getJobCounts()` and `getJobCountsAsync()` expose the server/manager's `'waiting-children'` bucket in both embedded and TCP modes. An empty or obliterated queue returns the key with value `0`, preserving a stable shape and allowing count/list conservation checks at the public boundary.
- **`finishedOn`/`processedOn` on list queries (#104):** these fields are populated client-side. `getJob` (single) already returned them via `buildJobProperties`, but list paths build results with `createSimpleJob`, which hard-codes them `undefined`. The fix patches `src/client/queue/operations/query.ts` to mirror the single-job mapping (numeric `startedAt`/`completedAt` → `processedOn`/`finishedOn`, `null`→`undefined`). Failed jobs intentionally keep `finishedOn` undefined (no `completedAt`). See [Client SDK: Queue](./client-queue-sdk.md).

## Configuration

- `cleanQueue` `limit` default: 1000 (`queueControl.ts:195`); `getJobs` default `end`: 100. An explicit `end: -1` is exhaustive: embedded reads use an effectively unbounded end, while the TCP client drains 1,000-row pages until exhaustion.
- `LOCK_TIMEOUT_MS` (default 5000) bounds the read/write locks taken by these operations.
- Memory-bound sizes (affect query result availability): `completedJobs=50000`, `jobResults=10000`, `jobLogs=10000`, `customIdMap=50000`; cleanup runs every 10s. See [Configuration & Entrypoint](./configuration.md).
- `BUNQUEUE_DATA_PATH` (and the `BQ_DATA_PATH`/`DATA_PATH`/`SQLITE_PATH` fallbacks) determine whether the SQLite fallback paths in queries are active.

## Related Docs

- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md)
- [Core Queue Engine (QueueManager & Shards)](./core-queue-engine.md)
- [Dead Letter Queue (DLQ)](./dead-letter-queue.md)
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md)
- [TCP Server Command Handlers](./tcp-server-handlers.md)
- [Concurrency & Locking](./concurrency-and-locking.md)
- [Persistence (SQLite, WriteBuffer, ReadThrough)](./persistence.md)
- [FlowProducer & Job Dependencies](./flow-producer.md)
- [Client SDK: Queue](./client-queue-sdk.md)
- [Architecture](../architecture.md) · [Data Model](../data-model.md)
