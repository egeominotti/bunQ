# Client SDK: Queue

> **Category:** Client SDK · **Source:** `src/client/queue/queue.ts`, `src/client/queue/operations/add.ts`, `src/client/queue/operations/query.ts`, `src/client/queue/operations/management.ts`, `src/client/queue/operations/counts.ts`, `src/client/queue/operations/control.ts`, `src/client/queue/jobMove.ts`, `src/client/queue/stall.ts`, `src/client/queue/workers.ts`, `src/client/queue/jobProxy.ts`, `src/client/queue/addBatcher.ts`, `src/client/events.ts`, `src/client/jobConversion.ts`, `src/client/jobConversionTypes.ts`, `src/client/jobConversionHelpers.ts`, `src/client/jobHelpers.ts`, `src/client/types.ts`, `src/client/errors.ts`, `src/client/manager.ts`, `src/client/index.ts`

## Purpose

`Queue<T>` is the producer-side, BullMQ-style SDK surface for adding and managing jobs. A single class transparently drives two backends: **embedded mode** (in-process `QueueManager` over SQLite, no network) and **TCP mode** (msgpack commands to a bunqueue server over `TcpConnectionPool`). It exists to give application code one BullMQ-compatible API (`add`, `addBulk`, `getJob`, `pause`, DLQ, scheduler, rate-limit, flow dependencies, etc.) regardless of where the broker actually lives, and to normalize the difference into per-operation context objects rather than scattering `if (embedded)` branches across the class.

## Responsibilities & Scope

Owns:

- The `Queue<T>` class: constructor wiring (embedded vs TCP, shared vs dedicated pool, auto-batcher, `prefixKey` namespacing) and the full public method surface (`queue.ts:48`).
- Building the per-concern context objects (`ctx`, `addCtx`, `queryCtx`, `moveCtx`) that thin operation modules consume (`queue.ts:131`–`queue.ts:211`).
- Job-add option translation: merging `defaultJobOptions`, injecting `__parentId`/`__parentQueue` into data, mapping public `JobOptions` to the embedded `manager.push` shape and to the compacted `PUSH`/`PUSHB` wire payload (`add.ts`).
- Constructing the public `Job<T>` object via three builders — `createJobProxy` (TCP single add), `createSimpleJob` (embedded + TCP query results), and `toPublicJob`/`createPublicJob` (`jobProxy.ts`, `jobConversion.ts`).
- Auto-batching `add()` calls into `PUSHB` in TCP mode (`addBatcher.ts`).
- `QueueEvents`, the read-only embedded event listener (`events.ts`).
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

- `getSharedManager(dataPath?)` — process-wide `QueueManager` singleton; lazily created, env-var data path resolution `BUNQUEUE_DATA_PATH > BQ_DATA_PATH > DATA_PATH > SQLITE_PATH`, with a programmatic `dataPath` override (`manager.ts:13`, `manager.ts:20`). See [Core Queue Engine](./core-queue-engine.md), [Configuration & Entrypoint](./configuration.md).
- `TcpConnectionPool`, `getSharedPool`, `releaseSharedPool` — TCP transport. See [Client Transport](./client-transport.md).
- `AddBatcher` — concurrent `add()` batching into `PUSHB` (`addBatcher.ts`).
- `resolveToken`, `Forwarder`, operation modules (`operations/*`, `stall`, `dlq`, `rateLimit`, `scheduler`, `deduplication`, `jobMove`, `workers`, `bullmqCompat`).
- `jobId()` from `src/domain/types/job` (string → internal job id), `pausedView` from `src/shared/pausedView`, `shardIndex` from `src/shared/hash`.

External / runtime: Bun only (`import '../require-bun'` guard in `index.ts:23`), `Bun.env`, Node `events.EventEmitter` (for `QueueEvents`). No third-party runtime deps.

## Public Interface

`new Queue<T>(name: string, opts: QueueOptions = {})` — exported from `src/client/index.ts` (and `bunqueue/client`). The server-side key is `prefixKey + name`; `queue.name` stays the logical name (`queue.ts:64`–`queue.ts:67`).

Add (`queue.ts:214`):

- `add(name: string, data: T, opts?: JobOptions): Promise<Job<T>>` — routes through `AddBatcher` unless `opts.durable` or batcher disabled (`queue.ts:216`).
- `addBulk(jobs: Array<{ name; data: T; opts? }>): Promise<Job<T>[]>`

Query (`queue.ts:226`): `getJob`, `getJobState`, `getChildrenValues`, `getJobs(opts?)` / `getJobsAsync(opts?)`, and per-state pairs `getWaiting`/`getWaitingAsync`, `getDelayed[Async]`, `getActive[Async]`, `getCompleted[Async]`, `getFailed[Async]`. **Sync variants return `[]` in TCP mode** (`query.ts:222`); use the `Async` form over TCP.

Counts (`queue.ts:278`): `getJobCounts()` / `getJobCountsAsync()`, `getWaitingCount`, `getActiveCount`, `getCompletedCount`, `getFailedCount`, `getDelayedCount`, `count()` / `countAsync()`, `getCountsPerPriority()` / `getCountsPerPriorityAsync()`. Sync `count()`/`getCountsPerPriority()` return `0`/`{}` over TCP (`counts.ts:135`, `counts.ts:150`).

Control (`queue.ts:313`): `pause()`, `resume()`, `drain()`, `obliterate()` (all sync, fire-and-forget), `pauseAsync()`, `resumeAsync()`, `drainAsync()` (resolves with the removed count), `obliterateAsync()`, `isPaused()` / `isPausedAsync()`, `waitUntilReady()`.

The async control variants resolve only after the server has processed the command; `drainAsync()`/`retryDlqAsync()`/`purgeDlqAsync()` also return the server count that the fire-and-forget forms discard (they always return 0 over TCP).

`obliterateAsync()` resolves only after the server has processed the wipe. The fire-and-forget `obliterate()` gives no ordering guarantee over the multi-connection TCP pool (default 4 sockets, round-robin): a `PUSH` sent right after it can travel on a different socket, be processed first, and then be wiped by the late-arriving obliterate, even with a sleep in between, if the server event loop is busy. Await `obliterateAsync()` before enqueuing follow-up jobs on the same queue (`control.ts:44`).

Management (`queue.ts:336`): `remove(id)` (sync) / `removeAsync(id)`, `retryJob(id)`, `retryJobs(opts?)`, `clean(grace, limit, type?)` / `cleanAsync(...)`, `promoteJobs(opts?)`, `promoteJob(id)`, `updateJobProgress`, `getJobLogs`, `addJobLog`, `clearJobLogs`, `updateJobData`, `changeJobDelay`, `changeJobPriority`, `extendJobLock`.

`promoteJobs({ count? })` delegates to the manager/server bulk operation in both embedded and TCP modes. The operation selects delayed jobs from the live shard queue in stable `(createdAt, id)` order rather than from the eventually consistent SQLite `GetJobs` view; `count: 0` promotes none. Each promotion updates the priority queue, delayed counter/temporal tracking, persisted `run_at`, and queue waiter notification before the call resolves.

Move / BullMQ-v5 (`queue.ts:502`): `moveJobToCompleted`, `moveJobToFailed`, `moveJobToWait`, `moveJobToDelayed`, `moveJobToWaitingChildren`, `waitJobUntilFinished`. `moveJobToFailed(id, error)` forwards the error's stacktrace (#74) and honours `UnrecoverableError` (skip retry) via the shared `failWire` helper, matching the worker failure path — previously both were silently dropped on this and the job-proxy paths. `moveJobToDelayed(id, timestamp)` takes an **absolute** timestamp; embedded routes waiting/active jobs via `changeWaitingDelay`/`changeDelay`, while the TCP path (`jobMove.ts`) sends the `MoveToDelayed` command with a **relative** `delay = max(0, timestamp - now)` (not the raw timestamp) and surfaces a server `ok:false` as a thrown error. Works for both waiting and active jobs.

Stall (`queue.ts:396`): `setStallConfig` / `setStallConfigAsync`, `getStallConfig`, `getStallConfigAsync`. DLQ (`setDlqConfig` / `setDlqConfigAsync`, `getDlq` (embedded-only entries with metadata), `getDlqJobsAsync(count?)` (dead jobs as public Job objects, works over TCP via the `Dlq` command), `retryDlq` / `retryDlqAsync`, `purgeDlq` / `purgeDlqAsync`), rate-limit (`setGlobalRateLimit(max, duration?)` honoring the window in both modes, plus `setGlobalRateLimitAsync` / `removeGlobalRateLimitAsync` / `setGlobalConcurrencyAsync` / `removeGlobalConcurrencyAsync`, and `rateLimit(expireTimeMs)` with broker-side TTL expiry), scheduler, dedup, dependency, BullMQ-compat (`getPrioritized`, `getWaitingChildren`, …), worker/metrics (`getWorkers`, `getWorkersCount`, `getMetrics`, `trimEvents`), `forward(options)`.

Connection: `disconnect()` (flushes + waits for in-flight batcher, then closes) and `close()` (`queue.ts:607`, `queue.ts:616`).

Also exported: `QueueEvents<R, P>` (`events.ts:98`), `UnrecoverableError`, `DelayedError` (`errors.ts`).

TCP commands emitted by this module (exact names): `PUSH`, `PUSHB`, `GetJob`, `GetState`, `GetChildrenValues`, `GetJobs`, `GetJobCounts`, `GetCountsPerPriority`, `Count`, `Pause`, `Resume`, `Drain`, `Obliterate`, `IsPaused`, `Ping`, `Cancel`, `MoveToWait`, `RetryDlq`, `Clean`, `Promote`, `PromoteJobs`, `Progress`, `GetLogs`, `AddLog`, `ClearLogs`, `Update`, `ChangeDelay`, `ChangePriority`, `ExtendLock`, `ACK`, `FAIL`, `MoveToDelayed`, `WaitJob`, `SetStallConfig`, `GetStallConfig`, `ListWorkers`, `Metrics`, `GetResult`, `GetFailedChildrenValues`, `GetIgnoredChildrenFailures`, `RemoveChildDependency`, `RemoveUnprocessedChildren`, `Discard`.

`QueueEvents` events emitted: `waiting`, `active`, `completed`, `failed`, `progress`, `stalled`, `removed`, `delayed`, `duplicated`, `retried`, `waiting-children`, `drained`, `paused`, `resumed`, `error` (`events.ts:150`).

## Data Models

See [data-model](../data-model.md) for full definitions. Key shapes (all in `types.ts`):

- `Job<T>` (`types.ts:81`): `id`, `name`, `data`, `queueName`, `attemptsMade`, `timestamp`, `progress`, `delay`, `priority`, `processedOn?`, `finishedOn?`, `stacktrace`, `stalledCounter`, `parent?`, `parentKey?`, `opts`, `deduplicationId?`, `repeatJobKey?`, `attemptsStarted`, plus BullMQ-v5 methods (`updateProgress`, `log`, `getState`, `is*`, `updateData`, `promote`, `changeDelay`, `changePriority`, `extendLock`, dependency/move/serialization methods).
- `JobOptions` (`types.ts:332`): `priority`, `delay`, `attempts`, `backoff` (`number | BackoffOptions`), `timeout`, `jobId`, `removeOnComplete?`/`removeOnFail?` (**boolean only** — age/count retention is unimplemented and the type was narrowed, #90), `stallTimeout`, `repeat`, `durable`, `parent`, `lifo`, `stackTraceLimit`, `keepLogs`, `sizeLimit`, `failParentOnFailure`, `removeDependencyOnFailure`, `continueParentOnFailure`, `ignoreDependencyOnFailure`, `timestamp`, `deduplication`, `debounce`.
- `QueueOptions` (`types.ts:443`): `defaultJobOptions`, `connection`, `embedded`, `dataPath`, `autoBatch`, `prefixKey`.
- `ConnectionOptions` (`types.ts:403`): `host` (default `localhost`), `port` (default `6789`), `socketPath` (declared in the type but currently ignored, the client transport always dials `host`/`port`), `tls`, `token`, `poolSize` (default `4` in the constructor), `pingInterval`, `commandTimeout`, `maxCommandTimeouts`, `pipelining`, `maxInFlight`.
- `AutoBatchOptions` (`types.ts:433`): `enabled`, `maxSize` (default `50`), `maxDelayMs` (default `5`).
- `StallConfig` (`types.ts:531`), `DlqConfig` (`types.ts:543`), `DlqEntry<T>` / `DlqStats` / `DlqFilter` (`types.ts:567`), `JobStateType` union (`types.ts:7`), `JobJson` / `JobJsonRaw` for `toJSON()` / `asJSON()`.
- `FlowJobData` (`types.ts:611`): internal fields (`__parentId`, `__parentQueue`, `__childrenIds`, …) merged into `job.data` by FlowProducer.

## Business Logic / Control Flow

**Construction** (`queue.ts:64`): `embedded = opts.embedded ?? FORCE_EMBEDDED` (the latter is `Bun.env.BUNQUEUE_EMBEDDED === '1'`, `helpers.ts:14`). Embedded path calls `getSharedManager(opts.dataPath)` and leaves `tcpPool`/`addBatcher` null (`queue.ts:71`). TCP path: when `poolSize === 4` and no token, a *shared* pool is reused (`getSharedPool`, `useSharedPool = true`); otherwise a dedicated `TcpConnectionPool` is built with `host`/`port` defaults (`queue.ts:82`–`queue.ts:109`). The `AddBatcher` is created unless `autoBatch.enabled === false` (`queue.ts:112`).

**add()** (`add.ts:63`): merges `defaultJobOptions` then per-call `opts`, injects `__parentId`/`__parentQueue` if `parent` set (`add.ts:73`). Embedded: maps to `manager.push(name, {...})` and wraps the returned internal job via `toPublicJob` (`add.ts:100`, `add.ts:140`). TCP: `buildPushPayload` builds a `compact`-ed `PUSH` frame — undefined keys are stripped to keep msgpack frames small (`add.ts:195`) — sends it, throws on `!response.ok`, then builds a `Job` via `createJobProxy(id, ...)` carrying reflection meta so the returned object mirrors requested `priority`/`delay`/`opts` (`add.ts:164`–`add.ts:186`, #88).

**addBulk()** (`add.ts:276`): returns `[]` immediately for empty input; merges defaults once per job to keep payload and reflected meta in sync. Embedded uses `manager.pushBatch` → `createSimpleJob` per id; TCP sends one `PUSHB` and maps `response.ids` to `createJobProxy`. A non-ok response throws (so the batcher rejects all callers); an ok response with zero ids is a legitimate empty result, not an error (`add.ts:421`).

**Job object construction.** `createJobProxy` (`jobProxy.ts:72`) builds a TCP-backed job whose methods send commands. `createSimpleJob` (`jobProxy.ts:291`) builds a dual-mode job (branches on `embedded`) used by query results. Both call `reflectFields` (`jobProxy.ts:53`) to derive `deduplicationId`, `parentKey`, `parent`, `repeatJobKey` from `meta.opts`. `toPublicJob`/`createPublicJob` (`jobConversion.ts`) build from an internal job using `buildJobProperties`/`buildSerializationMethods` (`jobConversionHelpers.ts`), with `extractUserData` stripping the internal `name` field from `data` (`jobHelpers.ts:10`).

**getJob()** (`query.ts:85`): embedded uses full `toPublicJob` wiring when `ctx.updateJobData` is present (all callbacks route to the shared manager); the simple-job fallback reflects opts via `metaFromJob`. TCP sends `GetJob`, returns null on `!ok`, and copies `progress`, `processedOn` (from `startedAt`), `finishedOn` (from `completedAt`) only when numeric (`query.ts:160`, #104).

**getJobState() / mapState()** (`query.ts:169`): normalizes server/manager states — `processing → active`, `dlq → failed`, unknown → `unknown` (`query.ts:180`).

**Counts.** `getJobCounts()` returns synchronously only in embedded mode; in TCP mode it delegates to `getJobCountsAsync()` so callers get real counts, not zeros (`counts.ts:33`). Embedded applies `pausedView`: when paused, ready jobs (waiting + prioritized) are reported under `paused` to avoid double-counting (`counts.ts:44`, #92).

**Stall config** (`stall.ts`): embedded writes through to `dlqOps`; TCP sends `SetStallConfig` and keeps a client-side `tcpConfigCache` so the sync `getStallConfig()` returns the last-set value (server remains source of truth; use `getStallConfigAsync()` for the authoritative value, `stall.ts:24`, `stall.ts:48`).

## Concurrency & Locking

`Queue` itself takes no shard/job locks; in embedded mode all locking happens inside `QueueManager` (see [Concurrency & Locking](./concurrency-and-locking.md)). The client-side concurrency surface is the `AddBatcher`:

- **Strategy** (`addBatcher.ts:61`): if no flush is in flight, flush immediately (zero latency for sequential `await`); if a flush is in flight, buffer until `maxSize` or a `maxDelayMs` timer fires. After each flush completes, accumulated items are drained immediately (`doFlush` loops while `pending.length > 0`, `addBatcher.ts:108`).
- **In-flight tracking**: `triggerFlush` registers each flush promise in `inFlightFlushes`; `disconnect()` calls `flush()` then `waitForInFlight()` before closing so no buffered job is silently dropped (`queue.ts:607`, `addBatcher.ts:167`).
- **`removeAsync` ordering invariant** (`management.ts:26`): the embedded path *must* `await manager.cancel()` because the removal happens inside an async write-lock; without the await the promise would resolve before the job is gone and cancel errors would be swallowed — divergent from the TCP path.

## Edge Cases & Failure Modes

- **Durable bypass**: `opts.durable` jobs skip the `AddBatcher` and are sent as individual `PUSH` (immediate disk write) instead of being batched (`queue.ts:216`).
- **Batcher overflow**: when `pending.length >= maxPending` (default `10000`), the oldest ~10% are spliced and rejected with `"Add buffer overflow - oldest entries dropped"` (`addBatcher.ts:69`). `stop()` rejects all remaining entries with `"AddBatcher stopped"`.
- **Error propagation**: `add`/`addBulk` throw on `!response.ok`, ensuring the batcher rejects queued callers (e.g. auth failure) rather than resolving them with `undefined` jobs (`add.ts:168`, `add.ts:421`).
- **Sync-over-TCP no-ops**: `getJobs`/`getWaiting`/… (sync) return `[]`, `count()` returns `0`, `getCountsPerPriority()` returns `{}`, and `isPaused()` returns `false` in TCP mode (each guards with `if (!ctx.embedded) return …`, e.g. `query.ts:222`, `counts.ts:135`, `counts.ts:150`, `control.ts:41`) — silent empties, not errors. Use the `Async` variants for correct TCP results. (`getWorkers()` is the inverse: an async method that returns `[]` in *embedded* mode, since worker registration only exists server-side, `workers.ts:23`.) The same pattern extends to the DLQ and rate-limit surfaces: the sync DLQ getters `getDlq()`/`getDlqStats()` return `[]`/zeroed stats over TCP, and sync `retryDlq()`/`purgeDlq()`/`retryCompleted()` fire the command but return `0` (`dlq.ts:51`; use `retryDlqAsync()`/`purgeDlqAsync()`/`retryCompletedAsync()` for the real count). The four rate-limit getters `getGlobalConcurrency()`, `getGlobalRateLimit()`, `getRateLimitTtl()`, `isMaxed()` are stubs that resolve `null`/`null`/`0`/`false` in *both* modes (`rateLimit.ts:33`).
- **TCP-unsupported methods reject explicitly**: `Job.moveToWaitingChildren()` over TCP rejects with "not supported in TCP mode" (`jobProxy.ts:203`, `jobProxy.ts:481`); `queue.moveJobToWaitingChildren` returns `false` over TCP (`jobMove.ts:152`). `removeDeduplicationKey()` always rejects ("no server primitive available") in both modes (`jobProxy.ts:234`, `jobConversion.ts:126`).
- **Idempotency**: `jobId`/`deduplication.id` make `add` idempotent (custom-id dedup, server-side, retention-window-bounded). `forward()` uses deterministic remote ids (`fwd:<queue>:<localId>`) so re-forwards don't duplicate (see [Store-and-Forward](./store-and-forward.md)).
- **`retryJob` state machine** (embedded, `management.ts:40`): `failed` → `retryDlq` (throws if not in DLQ), `active` → `moveActiveToWait`, `waiting`/`prioritized`/`delayed` → no-op, anything else throws. TCP path issues `MoveToWait` and throws on `ok !== true`.
- **`waitUntilFinished` TTL**: defaults to `30000ms`; rejects on timeout. The embedded short-circuit returns the persisted result if `job.completedAt` is already set (`jobProxy.ts:494`). The `jobMove.ts` variant additionally subscribes to a `QueueEvents` instance and races against an already-finished state check (`jobMove.ts:166`).
- **`QueueEvents` is embedded-only**: it subscribes to the shared manager's event bus and filters by queue name; it has no TCP transport. Handler exceptions are caught and re-emitted as `error` (`events.ts:204`–`events.ts:220`).
- **`prefixKey` isolation invariant**: every operation forwards `this.queueKey` (`prefixKey + name`), so two queues with the same logical name but different `prefixKey` never collide on the broker; a consuming `Worker` must use the same `prefixKey` (`queue.ts:56`, `types.ts:467`).
- **Processor error classes**: throwing `UnrecoverableError` skips remaining retries (straight to failed/DLQ); `DelayedError` re-delays without counting as a failure (`errors.ts`).

## Configuration

Constructor `QueueOptions` (`types.ts:443`): `embedded` (default falls back to `BUNQUEUE_EMBEDDED=1`), `dataPath` (embedded; overrides env), `defaultJobOptions`, `connection` (`ConnectionOptions`), `autoBatch`, `prefixKey`.

`ConnectionOptions` defaults applied here: `poolSize = 4` (`queue.ts:79`; `4` + no token ⇒ shared pool), `host = 'localhost'`, `port = 6789` (`queue.ts:97`–`queue.ts:98`). `commandTimeout`/`pingInterval`/`maxCommandTimeouts`/`pipelining`/`maxInFlight` defaults are owned by the transport — see [Client Transport](./client-transport.md).

`AutoBatchOptions`: `enabled` default true for TCP / disabled for embedded, `maxSize = 50`, `maxDelayMs = 5`.

Embedded data path env precedence (via `getSharedManager`): `BUNQUEUE_DATA_PATH > BQ_DATA_PATH > DATA_PATH > SQLITE_PATH` (`manager.ts:13`). `StallConfig` defaults (TCP cache + fallback): `enabled: true`, `stallInterval: 30000`, `maxStalls: 3`, `gracePeriod: 5000` (`stall.ts:16`).

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
