# FlowProducer & Job Dependencies

> **Category:** Orchestration · **Source:** `src/client/flow.ts`, `src/client/flowJobFactory.ts`, `src/client/flowPush.ts`, `src/client/flowTypes.ts`, `src/domain/queue/dependencyTracker.ts`, `src/application/dependencyResultTracker.ts`, `src/client/queueGroup.ts`

## Purpose

`FlowProducer` is the client-side API for building **parent/child job trees** and **dependency chains** that span one or more queues, with BullMQ v5 compatibility. It lets callers express "run these children, then run the parent with their results" without manually wiring `dependsOn`. The actual dependency bookkeeping lives server-side in the `DependencyTracker` (one per shard) and in `QueueManager`'s dependency-resolution path; `FlowProducer` is a thin orchestration/translation layer over `Queue.add`-equivalent pushes. `QueueGroup` is an unrelated namespace-isolation helper documented here because it shares the multi-queue concern.

## Responsibilities & Scope

**Owns:**

- The DSL for flows: `add`/`addBulk` (BullMQ tree), `addChain` (sequential), `addBulkThen` (fan-in), `addTree` (legacy tree) — `src/client/flow.ts`.
- Translating a flow node into a single push that carries `parentId` / `__parentQueue`, `childrenIds` / `__childrenIds`, and `dependsOn` (`src/client/flowPush.ts:164`).
- Atomic rollback of partially-created flows on error (`cleanupJobs`).
- Constructing lightweight `Job` result objects with live methods (`getChildrenValues`, `getDependencies`, `remove`, …) that proxy to embedded manager or TCP (`src/client/flowJobFactory.ts`).
- Reading back a flow tree via `getFlow` (recursive, depth/maxChildren bounded).

**Does NOT own (delegated):**

- Dependency storage & O(1) resolution — `DependencyTracker` (`src/domain/queue/dependencyTracker.ts`) per shard, driven by `QueueManager`/`dependencyProcessor`.
- Parking a job in `waitingDeps` and promoting it when deps complete — see [Background Tasks](./background-tasks.md) and `src/application/dependencyProcessor.ts`.
- Failure-propagation policy (`failParentOnFailure`, `continueParentOnFailure`, …) — implemented in `QueueManager` ([Job Lifecycle](./job-lifecycle.md)).
- The push/pull/ack mechanics themselves — [Core Queue Engine](./core-queue-engine.md), [Job Lifecycle](./job-lifecycle.md).
- Persistence of `parentId`/`childrenIds` to SQLite — [Persistence](./persistence.md).

## Dependencies

**Internal:**

- `getSharedManager()` (`src/client/manager.ts`) — embedded path target for all operations.
- `TcpConnectionPool` / `getSharedPool` / `releaseSharedPool` (`src/client/tcpPool.ts`) — TCP path; see [Client Transport](./client-transport.md).
- `pushJob` / `pushJobWithParent` / `cleanupJobs` (`src/client/flowPush.ts`).
- `createFlowJobObject` / `extractUserDataFromInternal` (`src/client/flowJobFactory.ts`).
- `queue/operations/management` (`updateJobData`, `promoteJob`, `removeAsync`, …) wired as flow-job callbacks.
- `DependencyTracker` (`src/domain/queue/dependencyTracker.ts`), surfaced on each `Shard` via getters (`waitingDeps`, `dependencyIndex`, `waitingChildren`).
- `dependencyProcessor.processPendingDependencies` (`src/application/dependencyProcessor.ts`).

**External / runtime:** Node `events.EventEmitter` (base class), Bun (`Bun.env.BUNQUEUE_EMBEDDED`). No third-party deps.

## Public Interface

### Exported classes

`FlowProducer extends EventEmitter` (`src/client/flow.ts:65`):

```typescript
constructor(opts: FlowProducerOptions = {})
add<T>(flow: FlowJob<T>, opts?: FlowOpts): Promise<JobNode<T>>          // flow.ts:141
addBulk<T>(flows: FlowJob<T>[]): Promise<JobNode<T>[]>                  // flow.ts:153
getFlow<T>(opts: GetFlowOpts): Promise<JobNode<T> | null>              // flow.ts:172
addChain<T>(steps: FlowStep<T>[]): Promise<FlowResult>                 // flow.ts:185
addBulkThen<T>(parallel: FlowStep<T>[], final: FlowStep<T>):
  Promise<{ parallelIds: string[]; finalId: string }>                  // flow.ts:213
addTree<T>(root: FlowStep<T>): Promise<FlowResult>                     // flow.ts:268
getParentResult<R>(parentId: string): R | undefined                   // flow.ts:284 (embedded only)
getParentResults<R>(parentIds: string[]): Map<string, R>              // flow.ts:293 (embedded only)
close(): Promise<void>                                                 // flow.ts:115
disconnect(): Promise<void>   // alias for close()                     // flow.ts:126
waitUntilReady(): Promise<void>                                        // flow.ts:131
```

`QueueGroup` (`src/client/queueGroup.ts:26`) — embedded-only namespace helper:

```typescript
constructor(namespace: string)                  // prefix = namespace ending in ':'
getQueue<T>(name, opts?): Queue<T>               // new Queue(prefix + name)
getWorker<T,R>(name, processor, opts?): Worker   // new Worker(prefix + name, …)
listQueues(): string[]                           // prefix-filtered, prefix stripped
pauseAll() / resumeAll() / drainAll() / obliterateAll(): void
```

### Exported types (`src/client/flowTypes.ts`)

`FlowProducerOptions`, `FlowStep<T>`, `FlowResult`, `FlowJob<T>`, `JobNode<T>`, `GetFlowOpts`, `FlowOpts` (all re-exported from `flow.ts:29`).

### TCP commands emitted (non-embedded path)

`PUSH` (with `parentId`, `childrenIds`, `dependsOn`), `UpdateParent`, `GetJob`, `GetState`, `GetResult`, `GetChildrenValues`, `GetFailedChildrenValues`, `GetIgnoredChildrenFailures`, `RemoveChildDependency`, `RemoveUnprocessedChildren`, `Cancel`, `Ping`, plus the per-job mutators on the result `Job`: `Progress`, `AddLog`, `Promote`, `Update`, `ChangeDelay`, `ChangePriority`, `ClearLogs`, `ExtendLock`, `ACK`, `FAIL`, `MoveToWait`, `MoveToDelayed`, `WaitJob`, `Discard`. Server handlers live in `src/infrastructure/server/handlers/advanced.ts` and `query.ts` — see [TCP Server Command Handlers](./tcp-server-handlers.md).

### Events

`FlowProducer` extends `EventEmitter` but emits no events itself (none are `emit`ed in source). Dependency lifecycle is surfaced via the dashboard/event system (`job:waiting-children`, `job:dependencies-resolved`, `flow:completed`) from `QueueManager` — see [Webhooks, Events & Job Logs](./webhooks-and-events.md).

## Data Models

See [data-model](../data-model.md) for full `Job`. Fields most relevant here:

- `Job.dependsOn: JobId[]` — IDs that must complete before this job leaves `waitingDeps`. Drives ordering for **all** flow shapes.
- `Job.childrenIds: JobId[]` — children of a fan-in/tree parent; consumed by `getChildrenValues`.
- `Job.parentId: JobId | null` — back-reference used by failure-propagation.
- Internal `data` fields injected by the producer: `name`, `__parentId`, `__parentQueue`, `__childrenIds`, and (chain/tree legacy) `__flowParentId` / `__flowParentIds`. `extractUserDataFromInternal` strips any key starting with `__` plus `name` when reconstructing user data (`flowJobFactory.ts:29`).

`DependencyTracker` (`src/domain/queue/dependencyTracker.ts`) holds three maps per shard:

- `waitingDeps: Map<JobId, Job>` — jobs blocked on unmet `dependsOn`.
- `dependencyIndex: Map<JobId, Set<JobId>>` — **reverse index** `depId → waiters`, giving O(1) "who is waiting for this completed job" (`getJobsWaitingFor`, line 58).
- `waitingChildren: Map<JobId, Job>` — parents explicitly parked via `moveToWaitingChildren` (distinct from `waitingDeps`).

`JobNode<T> = { job: Job<T>; children?: JobNode<T>[] }`; `FlowJob<T> = { name; queueName; data?; opts?; children? }`.

## Business Logic / Control Flow

### Building a flow (`add` → `addFlowNode`, `flow.ts:430`)

1. Children are recursed **first**, concurrently (`Promise.all`, line 442), with a placeholder `tempParentRef = { id: 'pending', queue }` so children record a parent before the parent exists.
2. Per-queue defaults from `FlowOpts.queuesOptions[queueName]` are merged under node `opts` (`flow.ts:454`).
3. The node's `data` gets `__parentId`/`__parentQueue` (if it has a parent) and `__childrenIds` (if it has children) injected (`flow.ts:461-465`).
4. `pushJobWithParent` pushes the node. In embedded mode it calls `manager.push(...)` with `parentId`, `dependsOn = childIds`, `childrenIds = childIds`, then back-patches each child with the **real** parent id via `manager.updateJobParent(childId, parentId)` (`flowPush.ts:183-187`). TCP mode pushes then sends `UpdateParent` per child (`flowPush.ts:212-216`).
5. All created ids are tracked in `createdJobIds`; any throw triggers `cleanupJobs` which `Cancel`s every created id (`flow.ts:147`).

### Fan-in (`addBulkThen`, `flow.ts:213`)

Parallel steps are pushed concurrently via `Promise.allSettled`; successful ids collected, and if any rejected the successes are cleaned up before rethrowing the first error (`flow.ts:237-241`). The `final` job is pushed with `pushJobWithParent` and `childIds = parallelIds` so it both waits on all parallel jobs (`dependsOn`) **and** records them as `childrenIds`, enabling `getChildrenValues()` fan-in result access (`flow.ts:248-258`).

### Chain (`addChain`, `flow.ts:185`)

Sequential: each step pushes with `dependsOn = [prevId]` and `__flowParentId` in data. **Note:** chains use `dependsOn` for ordering only — they do **not** set `childrenIds`, so `getChildrenValues()` on a chain step returns `{}`. Same is true of `addTree` (`addTreeNode`, `flow.ts:487`).

### Dependency resolution (server-side)

1. **Park on push:** `insertJobToShard` (`src/application/operations/push.ts:183`) computes `needsWaiting` = has `dependsOn` AND not all deps already in `completedJobs`/`depCompletions` (line 191). If waiting, the job goes into `shard.waitingDeps` and `registerDependencies` indexes it; timeline gets `waiting-children` (`push.ts:199-206`). Otherwise it enters the `PriorityQueue` immediately. Its query state is reported as `waiting-children` (`queryOperations.ts:180`).
2. **On completion:** `QueueManager.onJobCompleted` (`queueManager.ts:1380`) adds the completed id to `pendingDepChecks`, schedules a microtask flush, and runs `checkFlowCompleted`.
3. **Flush:** `runDependencyFlush` (`queueManager.ts:1768`) loops `processPendingDependencies` (`dependencyProcessor.ts:16`): for each completed id it gathers waiters per shard via the O(1) reverse index, acquires the **shard write lock before reading `waitingDeps`** (TOCTOU guard, line 42), re-checks `job.dependsOn.every(dep => completedJobs.has(dep) || depCompletions.has(dep))`, and promotes ready jobs.
4. **Promote:** `promoteJobsToQueue` (`dependencyProcessor.ts:79`) deletes from `waitingDeps`, `unregisterDependencies`, pushes into the queue, `incrementQueued`, updates `jobIndex` to `{type:'queue'}`, appends timeline, and `shard.notify()`. A `job:dependencies-resolved` dashboard event fires. A fallback poll in `backgroundTasks.ts:69` drains `pendingDepChecks` if the microtask path was missed.

`DependencyResultTracker` separately records consumer→dependency edges at accepted push/recovery. When a dependency ACKs, its result is copied into protected storage only if a live consumer exists. The ordinary `jobResults` LRU remains bounded and unchanged; query order is LRU → protected dependency result → SQLite. Promotion and reads do not release the protected value because the consumer may read it repeatedly while processing. Terminal completion/failure, cancel, clean, stale-GC, explicit edge removal, obliterate, and shutdown release consumer edges; fan-out keeps a result until the last consumer exits. Retries keep their edges.

### `getFlow` (`flow.ts:172`)

Recursively builds a `JobNode` tree, default `depth = Infinity`, `maxChildren` slices each level. Embedded reads `job.childrenIds`; TCP reads `__childrenIds` from `job.data` (`flow.ts:399`).

## Concurrency & Locking

- Dependency promotion takes the **per-shard write lock before reading `waitingDeps`** to prevent a complete-vs-check TOCTOU race (`dependencyProcessor.ts:42`). This respects the global order (`shards[N]` lock; see [Concurrency & Locking](./concurrency-and-locking.md)).
- `moveToWaitingChildren` (`jobStateTransitions.ts:81`) moves an **active** job out of its processing shard (under `processingLocks[idx]`) into `shard.waitingChildren` (under `shardLocks[idx]`), releasing concurrency/uniqueKey/group slots first (`releaseJobResources`).
- Flush coalescing: `scheduleDependencyFlush` debounces via a single `queueMicrotask`; `runDependencyFlush` uses a `while` re-entrancy loop + `depFlushRunning` guard so completions arriving during async lock waits are not lost (`queueManager.ts:1754-1783`).
- `addFlowNode` recurses children with `Promise.all`; `addBulkThen` pushes parallels with `Promise.allSettled` — concurrency is at the client/push level, ordering correctness comes from server-side `dependsOn`.

## Edge Cases & Failure Modes

- **Atomic rollback:** `add`, `addBulk`, `addChain`, `addBulkThen`, `addTree` all collect created job ids and call `cleanupJobs` (best-effort `Cancel`, errors swallowed) on any error so a partially-built flow is not left dangling (`flowPush.ts:222`).
- **Pending-parent race:** children are pushed with `parentId='pending'` then back-patched. `updateJobParent` (`queueManager.ts:768`) handles a child that **already terminally failed** before linkage: if the child is in DLQ it re-applies `failParentOnFailure` / `removeDependencyOnFailure` / `ignoreDependencyOnFailure` / `continueParentOnFailure` against the real parent id (lines 803-819).
- **`__parentQueue` correctness (#104 / audit #102 follow-up):** `__parentQueue` is set to the **parent's** queue, not the child's, so cross-queue `Job.parent`/`parentKey`/`opts.parent` navigation is correct (`queueManager.ts:777-783`).
- **`removeOnComplete` parents:** their full record is evicted from `completedJobs` to bound memory, so a bare-id `depCompletions` (`BoundedSet`, same cap as `completedJobs`, FIFO) is consulted both on push (`push.ts:194`) and on resolution (`dependencyProcessor.ts:56`). `depCompletions` is deliberately **not** pruned eagerly to avoid orphaning dependents pushed after a parent completed (`dependencyProcessor.ts:71-75`).
- **Failure propagation policies** (forwarded by `flowPush` `managerOptions`/`tcpOptions`): `failParentOnFailure` → `moveParentToFailed`; `continueParentOnFailure` → promote parent + record `failedChildrenValues`; `removeDependencyOnFailure`/`ignoreDependencyOnFailure` → drop the child from parent deps (ignore variant records `ignoredChildrenFailures`). These maps are keyed by `parentId` and **cleared on parent completion** to avoid a permanent leak (`queueManager.ts:1388`), and on `obliterate`/shutdown.
- **`removeChildDependency`** (`queueManager.ts:1668`): only valid while the parent is still in `waitingDeps`; if removing the last pending dep, promotes the parent. Throws if the job has no parent.
- **`removeUnprocessedChildren`** (`queueManager.ts:1728`): cancels only children whose `jobIndex` location is `'queue'` (waiting/delayed); active/completed/failed children are untouched.
- **Stale-deps GC:** a `waitingDeps` job older than **1 hour** is re-checked under the shard write lock, deleted from SQLite/write-buffer first, then removed from reverse indexes, `jobIndex`, unique/custom ownership, and protected-result consumer tracking.
- **Late consumer boundary:** registering a dependency immediately pins a result still present in the normal LRU; SQLite remains the durable fallback. In memory-only mode, a result already evicted before any consumer edge existed cannot be reconstructed. Normal predeclared flows register their edges before dependency completion.
- **Embedded-only methods:** `getParentResult`/`getParentResults` throw outside embedded mode (`flow.ts:285`,`294`); `moveToWaitingChildren` over TCP throws "not supported" (`flowJobFactory.ts:294`); `removeDeduplicationKey` always rejects ("not implemented", `flowJobFactory.ts:341`).
- **Shared pool semantics:** when `poolSize === 4` and no `token`, a shared pool is used; `close()` only releases (ref-counts) it, while a dedicated pool is fully closed (`flow.ts:115-123`).
- **`QueueGroup` is embedded-only:** every method calls `getSharedManager()`; there is no TCP variant. `getQueue`/`getWorker` simply prepend the `:`-terminated prefix.

## Configuration

`FlowProducerOptions` (`flowTypes.ts:9`):

| Option | Default | Notes |
| --- | --- | --- |
| `embedded` | `Bun.env.BUNQUEUE_EMBEDDED === '1'` | `flow.ts:39,73`. In-process manager, no server. |
| `connection.host` | `'localhost'` | dedicated-pool path (`flow.ts:95`) |
| `connection.port` | `6789` | dedicated-pool path |
| `connection.poolSize` | `4` | `poolSize===4 && !token` → shared pool (`flow.ts:82`) |
| `connection.token` | — | presence forces a dedicated pool |
| `connection.pingInterval` / `commandTimeout` / `pipelining` / `maxInFlight` | pool defaults | forwarded to `TcpConnectionPool` |

Per-flow option `FlowOpts.queuesOptions: Record<queueName, Partial<JobOptions>>` supplies per-queue defaults merged under each node's `opts` (`flow.ts:454`). `depCompletions` is capped at `maxCompletedJobs`; protected dependency results are bounded by the live dependency graph and are released with its consumer edges.

## Related Docs

- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md)
- [Core Queue Engine (QueueManager & Shards)](./core-queue-engine.md)
- [Background Tasks](./background-tasks.md)
- [Concurrency & Locking](./concurrency-and-locking.md)
- [Workflow Engine (saga orchestration)](./workflow-engine.md)
- [Deduplication & Unique Jobs](./deduplication-and-unique.md)
- [TCP Server Command Handlers](./tcp-server-handlers.md)
- [Client Transport (TCP pool, reconnect, batching)](./client-transport.md)
- [Persistence (SQLite, WriteBuffer, ReadThrough)](./persistence.md)
- [architecture](../architecture.md)
- [data-model](../data-model.md)
