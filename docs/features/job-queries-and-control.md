# Job Queries & Queue Control

> **Category:** Jobs · **Source:** `src/application/operations/queryOperations.ts`, `src/application/operations/jobManagement.ts`, `src/application/operations/queueControl.ts`, `src/shared/pausedView.ts`

## Purpose

This module is the read/control surface of the `QueueManager`. It answers point and list queries about jobs (by id, by custom id, by state, results, progress, counts), mutates individual jobs that are not yet completed (cancel, promote, discard, re-prioritize, re-delay, update data/progress), and drives queue-wide lifecycle operations (pause, resume, drain, obliterate, clean). It exists to keep all of this logic as pure functions that operate on an explicit context object (`shards`, `jobIndex`, locks, storage) rather than baking it into the manager, so the same code paths serve embedded callers, the TCP server, and the HTTP/MCP backends identically.

## Responsibilities & Scope

Owns:

- Job lookups across every location: in-shard run queue, `waitingDeps`/`waitingChildren` maps, processing shards, completed cache, and DLQ — plus SQLite fallback after restart (`queryOperations.ts:30`).
- State resolution including BullMQ v5 distinctions (`waiting` vs `prioritized` by priority, `delayed` by `runAt`, `waiting-children`) and the paused-view derivation (`pausedView.ts`).
- Paginated, multi-source, state-filtered list queries with offset-correct merging (`getJobs`, `queryOperations.ts:435`).
- Single-job mutations on non-terminal jobs: `cancelJob`, `updateJobProgress`, `updateJobData`, `changeJobPriority`, `promoteJob`, `moveJobToDelayed`, `discardJob` (`jobManagement.ts`).
- Queue control: `pauseQueue`, `resumeQueue`, `isQueuePaused`, `drainQueue`, `obliterateQueue`, `cleanQueue`, `getQueueCount`, `listAllQueues` (`queueControl.ts`).

Does NOT own:

- Push/pull/ack/fail lifecycle transitions — see [Job Lifecycle](./job-lifecycle.md).
- Per-state numeric aggregation (`getQueueJobCounts`) — lives in `statsManager`, see [Stats, Metrics & Monitoring](./stats-and-monitoring.md). This module only consumes those counts through `pausedView`.
- DLQ insertion mechanics and retry/purge — see [Dead Letter Queue](./dead-letter-queue.md). `discardJob` calls `shard.addToDlq` but the DLQ store/maintenance is elsewhere.
- The manager-level extra cleanup that `obliterate` requires (processing shards, completed/result/log/lock indexes, `customIdMap`, metrics, persisted queue state) — that lives in `QueueManager.obliterate` (`queueManager.ts:870`), not in `obliterateQueue` here.
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
function obliterateQueue(queue: string, ctx: QueueControlContext): void
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

`QueryContext`/`GetJobsContext` (`queryOperations.ts:16`, `:132`), `JobManagementContext` (`jobManagement.ts:17`, optional `repeatChain`), `QueueControlContext` (`queueControl.ts:13`). Callers pass these via `QueueManager.contextFactory.get*Context()`.

### TCP commands handled (via `QueueManager` wrappers)

- Query: `GetJob`, `GetState`, `GetResult`, `GetProgress`, `GetJobByCustomId`, `GetJobs`, `GetJobCounts`, `GetCountsPerPriority`, `Count` (`handlers/query.ts`, `handlers/management.ts`, `handlers/advanced.ts`).
- Job management: `Cancel`, `Progress`, `Promote`, `Discard`, `Update`, `ChangePriority`, `MoveToDelayed`, `ChangeDelay` (`handlers/management.ts`, `handlers/advanced.ts`).
- Queue control: `Pause`, `Resume`, `IsPaused`, `Drain`, `Obliterate`, `Clean`, `ListQueues` (`handlers/management.ts`, `handlers/advanced.ts`).

`ChangeDelay` is a manager-level dispatcher (`queueManager.ts:1175`): for jobs already queued it calls `changeWaitingDelay` (in-place `runAt` mutation); for active jobs it falls back to `moveJobToDelayed`.

### Events emitted

- `cancelJob` → `EventType.Removed` with `prev: 'waiting'` (`jobManagement.ts:75`).
- `updateJobProgress` → `progress` event + `job.progress` webhook (`jobManagement.ts:108`).
- `moveJobToDelayed` → `EventType.Delayed` (`jobManagement.ts:265`).
- `QueueManager.pause`/`resume` wrap `pauseQueue`/`resumeQueue` and emit `EventType.Paused`/`EventType.Resumed` plus dashboard events `queue:paused`/`queue:resumed` (`queueManager.ts:836`).
- Dashboard-only events from handlers: `job:priority-changed`, `job:promoted`, `job:discarded`, `job:data-updated`, `job:moved-to-delayed`, `queue:drained`, `queue:obliterated`, `queue:removed`, `queue:cleaned`.

## Data Models

See [data-model](../data-model.md) for full `Job` and the events shape. Most relevant here:

- `JobLocation` (`domain/types/queue.ts:104`) — the discriminated union stored in `jobIndex` that drives every dispatch:
  ```typescript
  | { type: 'queue'; shardIdx: number; queueName: string }
  | { type: 'processing'; shardIdx: number }
  | { type: 'completed'; queueName: string }
  | { type: 'dlq'; queueName: string }
  ```
- `JobState` (`domain/types/job.ts:20`): `waiting | prioritized | delayed | active | completed | failed`. `getJobState` may additionally return the literal `'waiting-children'` (jobs parked in `waitingDeps`/`waitingChildren`) and the sentinel `'unknown'`.
- `PausedDerivedCounts` (`pausedView.ts:14`): when paused, `waiting`+`prioritized` collapse into `paused` to avoid double-counting (#92). Same shape feeds `GetJobCounts` (`handlers/query.ts:57`), the client SDK, and the dashboard.

## Business Logic / Control Flow

### getJob (`queryOperations.ts:30`)

1. Read `jobIndex.get(jobId)`. If absent, fall back to SQLite: `storage.getJob` then `storage.getDlqEntry`, finally `completedJobsData` — this keeps `getJob` working after a restart before the index is repopulated (`queryOperations.ts:33`).
2. Otherwise dispatch on `location.type`. For `queue`, take a read lock and probe run queue → `waitingDeps` → `waitingChildren` (`:46`). For `processing`, read under the processing lock. For `completed`, prefer SQLite then cache. For `dlq`, prefer the SQLite DLQ entry, then the in-memory DLQ shard (`:63`).

### getJobState (`queryOperations.ts:154`)

1. Fast path: `completedJobs.has(jobId)` → `Completed`.
2. No index entry → `resolveStateFromStorage` (`:137`): DLQ → `Failed`; raw state `completed`/`active` map through; `waiting`/`delayed` load the row and decide `Delayed` (future `runAt`), else `Prioritized` (priority > 0) vs `Waiting`.
3. `queue` location → under read lock, classify by map membership and `runAt`/`priority`; `processing` → `Active`; `dlq` → `Failed`.

### getJobs (`queryOperations.ts:435`)

The hard part is correct pagination when results come from multiple non-offset-aware sources (SQLite jobs table + in-memory DLQ, `waiting-children` maps, and the paused view).

- Normalizes `state` (string | string[] | empty → `null` = unfiltered).
- Storage path: if unfiltered and the queue has no DLQ entries, SQL paginates directly with `offset=start` (`:470`). If any derived source contributes, it gathers `[0, end)` from every source, then `mergePage` concatenates, sorts by `createdAt`, and slices `[start, end)` exactly once (`:428`) — pushing `offset` into SQL would drop/duplicate rows across pages (#92).
- `prioritized`/`waiting` have no dedicated SQLite state, so `querySqliteWithPriority` (`:390`) maps `prioritized`→`waiting`, over-fetches 2×, then post-filters by `priority > 0` / `priority <= 0`.
- Paused semantics: `resolveStateNeeds` (`:309`) suppresses explicit `waiting`/`prioritized` queries on a paused queue; those jobs are returned only under `paused` (`:487`). An unfiltered query still lists them by their temporal state.
- In-memory path (embedded, no storage): `collectJobsByState` gathers per state, sorts, slices.

### cancelJob (`jobManagement.ts:30`)

Only acts on `type === 'queue'`. Under a write lock it tries three locations in order, each with different bookkeeping:

1. Run queue (`:37`): `remove`, `decrementQueued`, release `uniqueKey`, delete index + SQLite row.
2. `waitingChildren` (`:49`): delete map entry + index + row — does NOT decrement (these were never counted as queued).
3. `waitingDeps` (`:61`, #102 fix): delete, `unregisterDependencies(jobId, dependsOn)`, release `uniqueKey`, delete index + row — no decrement (flow-chain dependents skip `incrementQueued`), but the uniqueKey reservation and dependency-index entries MUST be released here.

On success emits `Removed`. Returns `false` for active/completed/DLQ jobs.

### moveJobToDelayed (`jobManagement.ts:227`)

Two-phase: remove from the processing shard under `processingLocks[procIdx]` (`:238`), then re-push into the destination shard under `shardLocks[idx]`, resetting `startedAt = null`, `runAt = now + delay`, and calling `incrementQueued` with the temporal flag (`:255`). Emits `Delayed`.

### discardJob (`jobManagement.ts:277`)

Removes the job from its run queue (with `decrementQueued`) or from the processing shard, then under the destination shard lock calls `addToDlq`, sets the index to `dlq`, persists the DLQ entry, and deletes the jobs-table row (`:302`).

### Queue control

- `drainQueue` (`queueControl.ts:43`): `shard.drain` returns `{count, jobIds}`; the operation then deletes each `jobIndex` entry and calls `safeDeleteJob` so a buffered/on-disk add cannot resurrect a drained job.
- `cleanQueue` (`queueControl.ts:186`): normalizes `wait`→`waiting`, dispatches to `cleanWaitingLike` (`:98`, also covers `delayed`/`prioritized`/`paused`/undefined), `cleanCompleted` (`:122`), or `cleanFailed` (`:150`). Each respects `graceMs` (age threshold) and `maxJobs` (default 1000). Returns removed `JobId[]`.
- `obliterateQueue` (`queueControl.ts:58`): only calls `shard.obliterate(queue)` (clears the run queue, DLQ, unique keys, limiter, temporal index for that shard). The wider purge of processing/completed/results/logs/locks/customIdMap/metrics/queue-state is done by `QueueManager.obliterate` (`queueManager.ts:870`).

## Concurrency & Locking

Lock acquisition follows the project hierarchy: `jobIndex` (plain `Map`, read without a lock) → `completedJobs` → `shards[N]` → `processingShards[N]`. Pattern throughout: read the index entry first, then acquire the matching `RWLock`.

- Queries use `withReadLock` on `shardLocks[idx]` / `processingLocks[idx]`. `getJobResult`, `getJobProgress`, `getJobByCustomId` read fast paths without shard locks.
- Mutations use `withWriteLock`. `changeJobPriority`, `promoteJob`, `updateJobData` (queue branch) take a single shard write lock.
- `moveJobToDelayed` and `discardJob` are **two-phase** (release processing lock, then acquire a shard lock). There is a window where the job is in neither structure; concurrent queries can briefly report it as `unknown`/not-found. This is accepted — the source job was already removed from processing, so no double-execution occurs.
- `queueControl.pauseQueue`/`resumeQueue`/`getQueueCount` delegate to `Shard` methods that manage their own internal state; no lock taken at this layer.
- Lock acquisition is bounded by `LOCK_TIMEOUT_MS` (default 5000ms) — see [Concurrency & Locking](./concurrency-and-locking.md).

## Edge Cases & Failure Modes

- **Post-restart recovery:** `getJob`/`getJobState` fall back to SQLite (jobs table + DLQ + raw state) when `jobIndex` has no entry (`queryOperations.ts:33`, `:137`). Without storage (pure embedded) they return `null`/`'unknown'`.
- **Paused double-count avoidance (#92):** `pausedView` and `resolveStateNeeds` guarantee a single job is reported in exactly one bucket. Verified by the shared `pausedView` helper used across SDK/TCP/dashboard so surfaces cannot drift.
- **Pagination correctness (#92):** `mergePage` slices once after merge+sort; SQL `offset` is only used on the fast single-source path. `querySqliteWithPriority` over-fetches 2× to compensate for priority post-filtering — a pathological page where more than `limit` rows are filtered out could under-return for that page (mitigated, not fully eliminated, by the 2× factor).
- **`cleanQueue` with `state='active'` is intentionally unsupported** (`queueControl.ts:207`): cleaning in-flight jobs would race the worker ack path and leak concurrency/uniqueKey/group slots. Use `cancelJob` or `fail` instead. Unknown states also return `[]`.
- **Idempotency / not-found:** all mutations return `false` (or `[]`) when the job is absent or in the wrong location; `cancelJob`/`promoteJob`/`changeJobPriority` only act on queued jobs, `updateJobProgress`/`moveJobToDelayed` only on processing jobs. `promoteJob` no-ops if `runAt <= now` (already due).
- **Progress clamping:** `updateJobProgress` clamps to `[0,100]` and refreshes `lastHeartbeat`, so a progress update doubles as a heartbeat for stall detection. The progress webhook failure is caught and logged, never propagated.
- **`updateJobData` repeat-chain follow:** when the target id is completed/missing, it follows `repeatChain` to patch the successor job created by `handleRepeat` (`jobManagement.ts:170`).
- **SQLite write failures swallowed:** `safeDeleteJob`/`safeDeleteDlqEntry` (`queueControl.ts:81`) catch errors (e.g. `SQLITE_FULL`); in-memory state is already cleared and the orphan row is GC'd by crash-recovery on restart.
- **Memory-bound visibility:** completed-job queries depend on bounded LRU/Set collections — `completedJobs` (50k), `jobResults` (10k), `jobLogs` (10k), `customIdMap` (50k). Once evicted, results/custom-id lookups fall back to SQLite or return `null`. `getJobByCustomId` returns `null` if the LRU has evicted the mapping.
- **Known invariant gap (obliterate):** `shard.obliterate` (`shard.ts:466`) clears the run queue/DLQ/unique-keys/temporal index but does NOT clear `waitingDeps`/`waitingChildren` maps; the manager loop deletes their `jobIndex`/SQLite entries but the shard maps can retain ghost flow-chain dependents. Tracked as audit follow-up FP-2.
- **`finishedOn`/`processedOn` on list queries (#104):** these fields are populated client-side. `getJob` (single) already returned them via `buildJobProperties`, but list paths build results with `createSimpleJob`, which hard-codes them `undefined`. The fix patches `src/client/queue/operations/query.ts` to mirror the single-job mapping (numeric `startedAt`/`completedAt` → `processedOn`/`finishedOn`, `null`→`undefined`). Failed jobs intentionally keep `finishedOn` undefined (no `completedAt`). See [Client SDK: Queue](./client-queue-sdk.md).

## Configuration

- `cleanQueue` `limit` default: 1000 (`queueControl.ts:193`); `getJobs` default `end`: 100 (handler `cmd.limit ?? 100`).
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
