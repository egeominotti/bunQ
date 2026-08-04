# Client SDK: Queue

> **Category:** Client SDK · **Source:** `src/client/queue/queue.ts`, `src/client/queue/runtime/`, `src/client/queue/types/`, `src/client/queue/operations/`, `src/client/queue/job-proxy/`, `src/client/queue/dlq.ts`, `src/client/queue/addBatcher.ts`, `src/client/events.ts`, `src/client/queue-events/`, `src/client/types/events.ts`, `src/client/jobConversion.ts`, `src/client/manager.ts`, `src/client/queueGroup.ts`

## Purpose

`Queue<T>` is the producer-side, BullMQ-style SDK surface for adding and managing jobs. Its 27-line public façade inherits focused runtime capabilities for state, queries, control, configuration, scheduling, compatibility and connection lifecycle. Those layers transparently drive **embedded mode** or **TCP mode**, while public and internal contracts live separately under `queue/types/`.

## Responsibilities & Scope

Owns:

- The stable `Queue<T>` façade and inherited capability chain under `queue/runtime/`.
- Constructor/state wiring in `runtime/state.ts`, transport cleanup in `runtime/connection.ts`, and per-concern contexts consumed by thin operation modules.
- Job-add option translation: merging `defaultJobOptions`, injecting `__parentId`/`__parentQueue` into data, mapping public `JobOptions` to the embedded `manager.push` shape and to the compacted `PUSH`/`PUSHB` wire payload (`add.ts`).
- Constructing the public `Job<T>` object via three builders — `createJobProxy` (TCP single add), `createSimpleJob` (embedded + TCP query results), and `toPublicJob`/`createPublicJob` (`jobProxy.ts`, `jobConversion.ts`). Conversion helpers receive grouped presentation metadata so name, data, result, failure, token, and serialization fields cannot drift between construction paths.
- Auto-batching `add()` calls into `PUSHB` in TCP mode (`addBatcher.ts`).
- `QueueEvents`, the read-only embedded/TCP lifecycle-event listener
  (`events.ts`, `queue-events/tcpSubscription.ts`).
- BullMQ-compatible error classes `UnrecoverableError` / `DelayedError` (`errors.ts`).

Does NOT own:

- Job consumption / processing — see [Client SDK: Worker](./client-worker-sdk.md).
- The actual queue engine, state transitions, and persistence — see [Core Queue Engine](./core-queue-engine.md), [Job Lifecycle](./job-lifecycle.md), [Persistence](./persistence.md). `Queue` only forwards to `getSharedManager()` (embedded) or sends commands.
- TCP framing, pooling, reconnect, and the auto-batch transport mechanics — see [Client Transport](./client-transport.md).
- Server-side command handling — see [TCP Server Command Handlers](./tcp-server-handlers.md).
- DLQ, scheduler/cron, dedup, rate-limit, flow logic — delegated to sibling operation modules under `src/client/queue/` (`dlq.ts`, `scheduler.ts`, `deduplication.ts`, `rateLimit.ts`, `bullmqCompat.ts`); documented in [Dead Letter Queue](./dead-letter-queue.md), [Scheduler & Cron](./scheduler-and-cron.md), [Deduplication & Unique Jobs](./deduplication-and-unique.md), [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md), [FlowProducer & Job Dependencies](./flow-producer.md).
- Store-and-forward (`forward()` returns a `Forwarder`) — see [Store-and-Forward & BullMQ Compatibility](./store-and-forward.md).

## Dependencies

Internal:

- `getSharedManager(dataPath?)` — process-wide `QueueManager` singleton; lazily created, env-var data path resolution `BUNQUEUE_DATA_PATH > BQ_DATA_PATH > DATA_PATH > SQLITE_PATH`, with a programmatic `dataPath` override. The first effective path is canonicalized and retained; a later explicit path must identify the same database or construction throws synchronously. See [Core Queue Engine](./core-queue-engine.md), [Configuration & Entrypoint](./configuration.md).
- `TcpConnectionPool`, `getSharedPool`, `releaseSharedPool` — TCP transport. See [Client Transport](./client-transport.md).
- `AddBatcher` — concurrent `add()` batching into `PUSHB` (`addBatcher.ts`).
- `resolveToken`, `Forwarder`, operation modules (`operations/*`, `stall`, `dlq`, `rateLimit`, `scheduler`, `deduplication`, `jobMove`, `workers`, `bullmqCompat`).
- `jobId()` from `src/domain/types/job` (string → internal job id), `pausedView` from `src/shared/pausedView`, `shardIndex` from `src/shared/hash`.

External / runtime: Bun only (`import '../require-bun'` guard in `index.ts:23`), `Bun.env`, Node `events.EventEmitter` (for `QueueEvents`). No third-party runtime deps.

## Public Interface

`new Queue<T>(name: string, opts: QueueOptions = {})` — exported from
`src/client/index.ts` (and `bunqueue/client`). The server-side key is
`prefixKey + name`; `queue.name` stays the logical name
(`queue/runtime/state.ts`).

Add (`queue/runtime/queries.ts`, `queue/operations/add/`):

- `add(name: string, data: T, opts?: JobOptions): Promise<Job<T>>` — routes
  through `AddBatcher` unless `opts.durable` or batching is disabled.
- `addBulk(jobs: Array<{ name; data: T; opts? }>): Promise<Job<T>[]>`

Query (`queue/runtime/queries.ts`, `queue/operations/query.ts`): `getJob`,
`getJobState`, `getChildrenValues`, `getJobs(opts?)` / `getJobsAsync(opts?)`, and
per-state pairs `getWaiting`/`getWaitingAsync`, `getDelayed[Async]`,
`getActive[Async]`, `getCompleted[Async]`, `getFailed[Async]`. **Sync variants
return `[]` in TCP mode**; use the `Async` form over TCP.

For all list methods, `end: -1` means exhaustive traversal. Embedded queries
pass an effectively unbounded end to the manager; TCP queries page in chunks of
1,000 until exhaustion (`queryTcpPages.ts`). Explicit finite ends retain the
existing half-open `[start,end)` contract. Per-state aliases live in
`queryStates.ts` so the generic conversion/query module stays within the
repository size boundary. `getJobs[Async]({ asc: false })` applies descending
createdAt/job-id order before slicing, in both runtimes and on every TCP page.

Counts (`queue/runtime/queries.ts`, `queue/operations/counts.ts`):
`getJobCounts()` / `getJobCountsAsync()`, `getWaitingCount`, `getActiveCount`,
`getCompletedCount`, `getFailedCount`, `getDelayedCount`, `count()` /
`countAsync()`, `getCountsPerPriority()` / `getCountsPerPriorityAsync()`. Sync
`count()` / `getCountsPerPriority()` return `0` / `{}` over TCP.

Control (`queue/runtime/control.ts`, `queue/operations/control.ts`): `pause()`,
`resume()`, `drain()`, `obliterate()` (all sync, fire-and-forget),
`pauseAsync()`, `resumeAsync()`, `drainAsync()` (resolves with the removed
count), `obliterateAsync()`, `isPaused()` / `isPausedAsync()`,
`waitUntilReady()`.

The async control variants resolve only after the server has processed the command; `drainAsync()`/`retryDlqAsync()`/`purgeDlqAsync()` also return the server count that the fire-and-forget forms discard (they always return 0 over TCP).

`obliterateAsync()` resolves only after the server has processed the wipe. The fire-and-forget `obliterate()` gives no ordering guarantee over the multi-connection TCP pool (default 4 sockets, round-robin): a `PUSH` sent right after it can travel on a different socket, be processed first, and then be wiped by the late-arriving obliterate, even with a sleep in between, if the server event loop is busy. Await `obliterateAsync()` before enqueuing follow-up jobs on the same queue (`control.ts:44`).

Management (`queue/runtime/control.ts`, `queue/operations/management.ts`):
`remove(id)` (sync) / `removeAsync(id)`, `retryJob(id)`, `retryJobs(opts?)`,
`clean(grace, limit, type?)` / `cleanAsync(...)`, `promoteJobs(opts?)`,
`promoteJob(id)`, `updateJobProgress`, `getJobLogs`, `addJobLog`,
`clearJobLogs`, `updateJobData`, `changeJobDelay`, `changeJobPriority`,
`extendJobLock`.

`retryJobs({ state, count, timestamp })` supports both declared states. `failed`
retries matching DLQ entries and `completed` re-queues completed jobs; `count`
is a non-negative cap and `timestamp` includes only entries whose terminal
timestamp is at or before the cutoff. Embedded and TCP paths apply the same
selection rules.

`promoteJobs({ count? })` delegates to the manager/server bulk operation in both embedded and TCP modes. The operation selects delayed jobs from the live shard queue in stable `(createdAt, id)` order rather than from the eventually consistent SQLite `GetJobs` view; `count: 0` promotes none. Each promotion updates the priority queue, delayed counter/temporal tracking, persisted `run_at`, and queue waiter notification before the call resolves.

Move / BullMQ-v5 (`queue/runtime/scheduling.ts`, `queue/jobMove.ts`):
`moveJobToCompleted`, `moveJobToFailed`, `moveJobToWait`,
`moveJobToDelayed`, `moveJobToWaitingChildren`, `waitJobUntilFinished`.
`moveJobToFailed(id, error)` forwards the error's stacktrace (#74) and honours
`UnrecoverableError` (skip retry) via the shared `failWire` helper, matching the
worker failure path. `moveJobToDelayed(id, timestamp)` takes an **absolute**
timestamp; embedded routes waiting/active jobs via
`changeWaitingDelay`/`changeDelay`, while the TCP path sends `MoveToDelayed`
with `delay = max(0, timestamp - now)` and surfaces `ok:false` as an error.

Stall and DLQ configuration live in `queue/runtime/configuration.ts`, backed by
`queue/stall.ts`, `queue/dlq.ts`, and `queue/dlqOps.ts`:
`setStallConfig[Async]`, `getStallConfig[Async]`, `setDlqConfig[Async]`,
`getDlqConfig[Async]`, embedded snapshot reads `getDlq` / `getDlqStats`, and
authoritative cross-runtime reads `getDlqAsync(filter?)` /
`getDlqStatsAsync`. Retry and purge have both fire-and-forget and acknowledged
forms. Rate-limit methods include setters/removers, `rateLimit(expireTimeMs)`,
and live getters in both runtimes. Scheduler and compatibility methods are in
`runtime/scheduling.ts` and `runtime/compatibility.ts`.

`QueueGroup` keeps track of queues created through the group. Its synchronous
bulk methods operate on the embedded manager; `listQueuesAsync`,
`pauseAllAsync`, `resumeAllAsync`, `drainAllAsync`, and `obliterateAllAsync`
are the authoritative operations for either runtime. `drainAllAsync` returns
the aggregate removed count.

Connection: `disconnect()` (flushes + waits for the in-flight batcher, then
closes) and `close()` (`queue/runtime/connection.ts`). Both operations are
idempotent at the Queue ownership boundary: a TCP pool reference acquired by
the constructor is released exactly once. Repeated `close()` calls, including
`disconnect()` followed by `close()`, therefore cannot close a shared pool
still owned by another Queue.

Also exported: `QueueEvents<R, P>`, `QueueEventsOptions`, `QueueMetrics`,
`QueueMetricsMeta`, `QueueMetricType`,
`UnrecoverableError`, and `DelayedError`.

`new QueueEvents(name, options?)` preserves the historical no-options embedded
default. Pass `{ embedded: false, connection }` (or simply `{ connection }`) for
broker events. TCP mode owns one dedicated authenticated subscription socket,
re-subscribes after reconnect, filters by the prefixed queue key, and is ready
only after the broker acknowledges `SubscribeEvents`.

TCP commands emitted by this module (exact names): `PUSH`, `PUSHB`, `GetJob`, `GetState`, `GetChildrenValues`, `GetJobs`, `GetJobCounts`, `GetCountsPerPriority`, `Count`, `Pause`, `Resume`, `Drain`, `Obliterate`, `IsPaused`, `Ping`, `Cancel`, `MoveToWait`, `MoveToWaitingChildren`, `RetryDlq`, `RetryCompleted`, `GetDlqStats`, `Clean`, `Promote`, `PromoteJobs`, `Progress`, `GetLogs`, `AddLog`, `ClearLogs`, `Update`, `ChangeDelay`, `ChangePriority`, `ExtendLock`, `ACK`, `FAIL`, `MoveToDelayed`, `WaitJob`, `SetStallConfig`, `GetStallConfig`, `GetQueueLimits`, `GetDeduplicationJobId`, `RemoveDeduplicationKey`, `RemoveJobDeduplicationKey`, `ListWorkers`, `Metrics`, `TrimEvents`, `GetResult`, `GetFailedChildrenValues`, `GetIgnoredChildrenFailures`, `RemoveChildDependency`, `RemoveUnprocessedChildren`, `Discard`, `SubscribeEvents`, `UnsubscribeEvents`.

`QueueEvents` events emitted: `waiting`, `active`, `completed`, `failed`, `progress`, `stalled`, `removed`, `delayed`, `duplicated`, `retried`, `waiting-children`, `drained`, `paused`, `resumed`, `error` (`events.ts:150`).

## Data Models

See [data-model](../data-model.md) for full definitions. Public types are split
by responsibility under `src/client/types/`:

- `Job<T>`, `JobStateType`, `JobJson`, and `JobJsonRaw`:
  `client/types/job.ts`.
- `JobOptions`, backoff, repeat, deduplication, debounce, and parent options:
  `client/types/options.ts`.
- `QueueOptions`, `ConnectionOptions`, and `AutoBatchOptions`:
  `client/types/connection.ts`.
- `StallConfig`: `client/types/worker.ts`; DLQ types:
  `client/types/dlq.ts`; `FlowJobData`: `client/types/flow.ts`.
- Queue metric response and state discriminator types: `client/types/metrics.ts`.
- `QueueEventsOptions` and typed event payloads: `client/types/events.ts`.
- Queue-internal contexts, reflection metadata, and runtime contracts:
  `client/queue/types/`.

## Business Logic / Control Flow

**Construction** (`queue/runtime/state.ts`): `embedded = opts.embedded ??
FORCE_EMBEDDED` (`FORCE_EMBEDDED` lives in `queue/helpers.ts`). Embedded mode
warms `getSharedManager(opts.dataPath)` and leaves `tcpPool` / `addBatcher`
null. TCP mode reuses the shared pool for the default unauthenticated
four-connection case, otherwise creates a dedicated `TcpConnectionPool`. The
`AddBatcher` is created unless `autoBatch.enabled === false`.

The embedded manager is process-wide. Its first effective `dataPath` is
resolved to a canonical absolute file identity (`:memory:` remains a distinct
SQLite in-memory identity). Later clients that omit `dataPath` join that
manager without re-reading environment variables. A later explicit path that
does not identify the active database throws before the client is constructed;
it is never silently ignored. Relative, absolute, and symlink spellings of the
same existing database are accepted. To switch databases, close every embedded
client and call `shutdownManager()` first. Concurrent databases require
separate processes or TCP brokers.

**add()** (`queue/operations/add/single.ts`): merges `defaultJobOptions` then
per-call `opts`, injects `__parentId` / `__parentQueue` when a parent is set,
and maps to `manager.push` in embedded mode. TCP uses `buildPushPayload` from
`add/payload.ts`, throws on `!response.ok`, and builds a live job through the
split proxy modules under `queue/job-proxy/`. A `parent` option is authoritative:
the broker locks both queue shards, persists the child and parent edge together,
and moves the existing pending parent to `waiting-children` before the child is
visible. A non-linkable parent rejects the add without publishing the child.

**addBulk()** (`queue/operations/add/bulk.ts`): returns `[]` immediately for an
empty input and merges defaults once per job. Embedded uses
`manager.pushBatch`; TCP sends one `PUSHB`. A non-ok response throws so the
batcher rejects every caller; an ok response with zero IDs is a legitimate
empty result. Parent references are preflighted for the complete batch while
all affected shards are locked, preventing an invalid later item from leaving
an accepted prefix. Multiple children of one cross-queue parent are serialized
under that parent's shard lock, so concurrent adds cannot overwrite an edge.

**Job object construction.** `queue/job-proxy/tcp.ts` builds a TCP-backed job;
`queue/job-proxy/simple.ts` builds the dual-mode form used by query results;
`queue/job-proxy/reflection.ts` derives reflected option fields. Public
conversion lives in `client/jobConversion.ts`. Full DLQ entries use
`queue/dlqJobMethods.ts` so broker-returned jobs keep live methods rather than
detached placeholders.

**getJob()** (`query.ts`): embedded uses full `toPublicJob` wiring when
`ctx.updateJobData` is present (all callbacks route to the shared manager); TCP
sends `GetJob` and returns null on `!ok`. The single `metadataFromJob` reflection
path now supplies `attemptsMade`, `attemptsStarted`, `stalledCounter`, progress,
priority, `processedOn`, `finishedOn`, options, stacktrace, return value, and
failure reason to both `getJob()` and `getJobs[Async]()`. The live properties,
`toJSON()`, and `asJSON()` therefore describe the same broker generation in
embedded and TCP mode instead of query proxies resetting lifecycle counters to
zero.

**getJobState() / mapState()** (`query.ts:169`): normalizes server/manager states — `processing → active`, `dlq → failed`, unknown → `unknown` (`query.ts:180`).

**Counts.** `getJobCounts()` returns synchronously only in embedded mode; in TCP mode it delegates to `getJobCountsAsync()` so callers get real counts, not zeros (`counts.ts:33`). Embedded applies `pausedView`: when paused, ready jobs (waiting + prioritized) are reported under `paused` to avoid double-counting (`counts.ts:44`, #92).

**Stall config** (`stall.ts`): embedded writes through to `dlqOps`; TCP sends `SetStallConfig` and keeps a client-side `tcpConfigCache` so the sync `getStallConfig()` returns the last-set value (server remains source of truth; use `getStallConfigAsync()` for the authoritative value, `stall.ts:24`, `stall.ts:48`).

## Concurrency & Locking

`Queue` itself takes no shard/job locks; in embedded mode all locking happens inside `QueueManager` (see [Concurrency & Locking](./concurrency-and-locking.md)). The client-side concurrency surface is the `AddBatcher`:

- **Strategy** (`addBatcher.ts:61`): if no flush is in flight, flush immediately (zero latency for sequential `await`); if a flush is in flight, buffer until `maxSize` or a `maxDelayMs` timer fires. After each flush completes, accumulated items are drained immediately (`doFlush` loops while `pending.length > 0`, `addBatcher.ts:108`).
- **In-flight tracking**: `triggerFlush` registers each flush promise in
  `inFlightFlushes`; `disconnect()` in `queue/runtime/connection.ts` calls
  `flush()` then `waitForInFlight()` before closing.
- **`removeAsync` ordering invariant** (`management.ts:26`): the embedded path *must* `await manager.cancel()` because the removal happens inside an async write-lock; without the await the promise would resolve before the job is gone and cancel errors would be swallowed — divergent from the TCP path.

## Edge Cases & Failure Modes

- **Durable bypass**: `opts.durable` jobs skip the `AddBatcher` in
  `queue/runtime/queries.ts` and are sent as individual `PUSH` operations.
- **Batcher overflow**: when `pending.length >= maxPending` (default `10000`), the oldest ~10% are spliced and rejected with `"Add buffer overflow - oldest entries dropped"` (`addBatcher.ts:69`). `stop()` rejects all remaining entries with `"AddBatcher stopped"`.
- **Error propagation**: `add`/`addBulk` throw on `!response.ok`, ensuring the
  batcher rejects queued callers (e.g. auth failure) rather than resolving them
  with `undefined` jobs (`operations/add/single.ts:109-113`,
  `operations/add/bulk.ts:130-133`).
- **Synchronous TCP boundaries**: `getJobs`/`getWaiting`/… (sync) return `[]`, `count()` returns `0`, `getCountsPerPriority()` returns `{}`, and `isPaused()` returns `false` in TCP mode because their signatures cannot await a round trip. Use the corresponding `Async` variants for authoritative remote results. The same rule applies to synchronous DLQ reads and fire-and-forget mutation forms; use `getDlqAsync`, `getDlqStatsAsync`, `retryDlqAsync`, `retryDlqByFilterAsync`, `purgeDlqAsync`, and `retryCompletedAsync` when the result matters. Limit getters, worker discovery, dependency methods, deduplication methods, and `moveToWaitingChildren` are asynchronous and now query or mutate the selected broker runtime directly.
- **Detached conversion helpers**: broker-returned `Job` instances always receive a complete live operation context. Low-level callers that invoke `createPublicJob` without a context receive only detached fallback behavior and must not treat that helper as a broker client.
- **Idempotency**: `jobId`/`deduplication.id` make `add` idempotent (custom-id dedup, server-side, retention-window-bounded). `forward()` uses deterministic remote ids (`fwd:<queue>:<localId>`) so re-forwards don't duplicate (see [Store-and-Forward](./store-and-forward.md)).
- **Metrics/event retention**: `getMetrics(type,start,end)` returns queue-scoped,
  newest-first one-minute buckets over identical embedded/TCP paths.
  `trimEvents(maxLength)` returns the exact number removed from that queue's
  separate persistent event journal; repeated trims are idempotent.
- **`retryJob` state machine** (embedded, `management.ts:40`): `failed` → `retryDlq` (throws if not in DLQ), `active` → `moveActiveToWait`, `waiting`/`prioritized`/`delayed` → no-op, anything else throws. TCP path issues `MoveToWait` and throws on `ok !== true`.
- **`waitUntilFinished` TTL**: defaults to `30000ms`; rejects on timeout. The
  method subscribes before checking state so it cannot miss a completion race.
  An already-completed TCP job is followed by an authoritative `GetResult`, and
  a live completion resolves with the event's exact return value. Both paths
  settle and remove listeners exactly once.
- **`QueueEvents` transport**: embedded mode subscribes to the shared manager;
  TCP mode uses a dedicated socket so unsolicited event frames cannot consume a
  pooled command response. The subscription authenticates before subscribing,
  re-subscribes after a broker reconnect, and stops delivery on `close()`. A
  transport or handler error is emitted only when an `error` listener exists,
  avoiding Node's unhandled `error` behavior. Progress payloads expose the
  public progress value rather than the manager's internal event envelope.
- **`prefixKey` isolation invariant**: every context created by
  `queue/runtime/state.ts` forwards `queueKey = prefixKey + name`, so two queues
  with the same logical name but different prefixes never collide; a consuming
  Worker must use the same prefix.
- **Processor error classes**: throwing `UnrecoverableError` skips remaining retries (straight to failed/DLQ); `DelayedError` re-delays without counting as a failure (`errors.ts`).

## Configuration

Constructor `QueueOptions` (`client/types/connection.ts`): `embedded` (default
falls back to `BUNQUEUE_EMBEDDED=1`), `dataPath` (embedded; overrides env),
`defaultJobOptions`, `connection`, `autoBatch`, `prefixKey`.

`ConnectionOptions` defaults applied in `queue/runtime/state.ts` are
`poolSize = 4` (`4` + no token ⇒ shared pool), `host = 'localhost'`, and
`port = 6789`. Timeout, ping, pipelining, and in-flight defaults are owned by
the transport — see [Client Transport](./client-transport.md).

`AutoBatchOptions`: `enabled` default true for TCP / disabled for embedded, `maxSize = 50`, `maxDelayMs = 5`.

Embedded data path env precedence (via `getSharedManager`): `BUNQUEUE_DATA_PATH > BQ_DATA_PATH > DATA_PATH > SQLITE_PATH`. The environment is read only when a fresh manager is created; `shutdownManager()` resets both the manager and its path identity. `StallConfig` defaults (TCP cache + fallback): `enabled: true`, `stallInterval: 30000`, `maxStalls: 3`, `gracePeriod: 5000` (`stall.ts:16`).

## Related Docs

- [Client SDK: Worker (& sandboxed)](./client-worker-sdk.md)
- [Client Transport (TCP pool, reconnect, batching)](./client-transport.md)
- [Core Queue Engine (QueueManager & Shards)](./core-queue-engine.md)
- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md)
- [Job Queries & Queue Control](./job-queries-and-control.md)
- [Dead Letter Queue (DLQ)](./dead-letter-queue.md)
- [Deduplication & Unique Jobs](./deduplication-and-unique.md)
- [Scheduler & Cron](./scheduler-and-cron.md)
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md)
- [FlowProducer & Job Dependencies](./flow-producer.md)
- [Webhooks, Events & Job Logs](./webhooks-and-events.md)
- [Store-and-Forward & BullMQ Compatibility](./store-and-forward.md)
- [Simple Mode (Bunqueue all-in-one)](./simple-mode.md)
- [TCP Server Command Handlers](./tcp-server-handlers.md)
- [architecture](../architecture.md)
- [data-model](../data-model.md)
