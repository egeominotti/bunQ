# Client SDK: Worker (& sandboxed)

> **Category:** Client SDK · **Source:** `src/client/worker/worker.ts`, `src/client/worker/processor.ts`, `src/client/worker/processorHandlers.ts`, `src/client/worker/ackBatcher.ts`, `src/client/worker/workerPull.ts`, `src/client/worker/workerHeartbeat.ts`, `src/client/worker/jobParser.ts`, `src/client/worker/types.ts`, `src/client/sandboxed/worker.ts`, `src/client/sandboxed/wrapper.ts`, `src/client/sandboxed/queueOps.ts`, `src/client/sandboxed/types.ts`, `src/client/sandboxedWorker.ts`

## Purpose

The Worker SDK is the consumer side of bunqueue: a BullMQ-style polling worker that pulls jobs from a queue, runs a user processor, and reports the outcome (ack/fail) back to the broker. It works in two transports — **embedded** (in-process `QueueManager` via `getSharedManager()`) and **TCP** (pooled connection to the server on :6789) — behind a single API. `SandboxedWorker` is a sibling implementation that runs the processor inside isolated Bun `Worker` threads (process-level isolation) for untrusted/CPU-heavy code. Lock-based ownership, batched acks, and heartbeat-driven stall protection are all handled here on the client.

## Responsibilities & Scope

Owns:
- Job pull loop with a concurrency gate, batch pulling, long-poll, and exponential backoff on pull errors (`worker.ts:639`, `worker.ts:915`).
- Lease accounting: tracking pulled/active jobs and lock tokens, enforcing the `concurrency` cap as a **leased** cap (running + buffered + in-flight pulls), the #96/#98 overshoot fixes (`worker.ts:806`, `worker.ts:849`).
- Processor invocation, auto-ack on success, fail/retry dispatch, `DelayedError`/`UnrecoverableError` handling, manual `moveToCompleted`/`moveToFailed` (`processor.ts`).
- ACK batching with backpressure and retry (`ackBatcher.ts`).
- Job and worker heartbeats / lock renewal (`workerHeartbeat.ts`, `worker.ts:962`).
- Worker registration/unregistration with the server, and re-registration on reconnect (`worker.ts:939`, `worker.ts:203`).
- Client-side rate limiting and per-group concurrency (`workerRateLimiter.ts`, `groupConcurrency.ts`).
- Graceful/forced close, releasing buffered jobs back to the queue (`worker.ts:516`, `worker.ts:610`).
- Sandboxed-process lifecycle: spawn, dispatch via IPC, timeout, crash restart, idle recycle/shutdown (`sandboxed/worker.ts`).

Does NOT own (delegated):
- Queue state, priority ordering, persistence, stall *detection itself*, lock storage — all server/`QueueManager` side. See [Core Queue Engine](./core-queue-engine.md), [Job Lifecycle](./job-lifecycle.md), [Persistence](./persistence.md).
- The TCP wire framing and connection pool/reconnect — see [Client Transport](./client-transport.md) and [TCP Wire Protocol](./tcp-protocol.md).
- Producing jobs — see [Client SDK: Queue](./client-queue-sdk.md).
- DLQ routing on max-attempts — see [Dead Letter Queue](./dead-letter-queue.md).

## Dependencies

Internal:
- `../manager` (`getSharedManager`) — embedded `QueueManager` access. See [Core Queue Engine](./core-queue-engine.md).
- `../tcpPool` (`TcpConnectionPool`, `getSharedPool`/`releaseSharedPool`) — TCP transport. See [Client Transport](./client-transport.md).
- `./processor` + `./processorHandlers` — execution and the `Job` method handlers (progress/log/state/children/mutations).
- `./ackBatcher`, `./workerPull`, `./workerHeartbeat`, `./jobParser`.
- `./workerRateLimiter`, `./groupConcurrency`. See [Rate Limiting & Concurrency](./rate-limiting-and-concurrency.md).
- `../resolveToken`, `../types` (`WorkerOptions`, `Processor`, `Job`, `RateLimiterOptions`).

External / runtime:
- Bun APIs: `Worker` (sandboxed threads), `Bun.sleep`, `Bun.file`, `Bun.env`, `Bun.gc` indirectly via `smol`.
- Node `events.EventEmitter`, `os.hostname`, `node:fs`/`node:fs/promises`/`node:path` (sandboxed wrapper file generation).

## Public Interface

### `Worker<T, R>` (`worker.ts:66`)

```typescript
class Worker<T = unknown, R = unknown> extends EventEmitter {
  constructor(name: string, processor: Processor<T, R>, opts?: WorkerOptions)

  run(): void
  pause(): void
  resume(): void
  isRunning(): boolean
  isPaused(): boolean
  isClosed(): boolean
  get concurrency(): number
  set concurrency(val: number)        // clamped to >= 1; bumps poll if raised
  get closing(): Promise<void> | null
  waitUntilReady(): Promise<void>     // TCP: sends Ping; embedded: no-op

  // Manual job control
  getNextJob(token?: string, opts?: { block?: boolean }): Promise<InternalJob | undefined>
  processJobManually(job, token?, fetchNextCallback?): Promise<InternalJob | undefined>
  extendJobLocks(jobIds: string[], tokens: string[], duration: number): Promise<number>

  // Cancellation (cooperative; processor must check isJobCancelled)
  cancelJob(jobId: string, reason?: string): boolean
  cancelAllJobs(reason?: string): void
  isJobCancelled(jobId: string): boolean

  // Rate limiter (delegated to WorkerRateLimiter)
  getRateLimiterInfo(): { current: number; max: number; duration: number } | null
  rateLimit(expireTimeMs: number): void
  isRateLimited(): boolean

  // BullMQ v5 compat
  startStalledCheckTimer(): Promise<void>   // no-op (stall detection is server-side)
  delay(ms?: number, abortController?: AbortController): Promise<void>

  close(force?: boolean): Promise<void>
}
```

### `SandboxedWorker<T>` (`sandboxed/worker.ts:75`)

```typescript
class SandboxedWorker<T = unknown> extends EventEmitter {
  constructor(queueName: string, options: SandboxedWorkerOptions)
  start(): Promise<void>
  stop(force?: boolean): Promise<void>
  isRunning(): boolean
  getStats(): { total: number; busy: number; idle: number; recycled: number; restarts: number }
}
```
Re-exported (with a `@deprecated` alias) from `src/client/sandboxedWorker.ts`.

### Helper modules
- `processJob(internalJob, ProcessorConfig)` (`processor.ts:60`).
- `AckBatcher` (`ackBatcher.ts:25`) with `queue/flush/stop/waitForInFlight/hasPending`.
- `pullEmbedded(config, count)` / `pullTcp(config, tcp, count, closing)` (`workerPull.ts`).
- `startHeartbeat(deps, intervalMs)` / `sendHeartbeat(deps)` (`workerHeartbeat.ts`).
- `parseJobFromResponse(jobData, queueName)` (`jobParser.ts:13`).

### TCP commands sent (client → server)

`PULL`, `PULLB`; `ACKB` (batch ack), `FAIL`; `MoveToWait`, `MoveToDelayed`; `JobHeartbeat`, `JobHeartbeatB`, `Heartbeat`; `RegisterWorker`, `UnregisterWorker`; `Ping`; `ExtendLock`, `ExtendLocks`. Processor `Job` handlers additionally send `Progress`, `AddLog`, `GetState`, `GetResult`, `GetChildrenValues`, `GetFailedChildrenValues`, `GetIgnoredChildrenFailures`, `RemoveChildDependency`, `RemoveUnprocessedChildren`, `Cancel` (remove), `Update`, `Promote`, `ChangeDelay`, `ChangePriority`, `ClearLogs`, `Discard`, `WaitJob`. `SandboxedWorker` TCP ops also use plain `ACK` and `GetJobCounts`. See [TCP Server Command Handlers](./tcp-server-handlers.md).

### Events emitted

`Worker`: `ready`, `active(job)`, `completed(job, result)`, `failed(job, error)`, `progress(job, n)`, `stalled(jobId, reason)` (embedded only, via `QueueManager.subscribe`), `error(err)`, `cancelled({jobId, reason})`, `log(job, msg)`, `drained`, `closed` (`worker.ts:129-169`).
`SandboxedWorker`: `ready`, `active`, `completed`, `failed`, `progress`, `log`, `error`, `closed` (no `drained`/`stalled`/`cancelled`) (`sandboxed/worker.ts:78-100`).

## Data Models

See [data-model](../data-model.md) for the full `Job` shape. Key types used here:

- `WorkerOptions` (`types.ts:481`): `concurrency?`, `autorun?`, `heartbeatInterval?`, `connection?`, `embedded?`, `dataPath?`, `batchSize?`, `pollTimeout?`, `useLocks?`, `limiter?: RateLimiterOptions`, `lockDuration?`, `maxStalledCount?`, `skipStalledCheck?`, `skipLockRenewal?`, `drainDelay?`, `removeOnComplete?`, `removeOnFail?`, `prefixKey?`.
- `ExtendedWorkerOptions` (`worker/types.ts:18`): resolved options with defaults applied by `resolveWorkerOptions` (`worker.ts:25`).
- `Processor<T, R> = (job: Job<T & FlowJobData>) => Promise<R> | R` (`types.ts:625`).
- `PendingAck` (`worker/types.ts:9`): `{ id, result, token?, resolve, reject }`.
- `SandboxedWorkerOptions` (`sandboxed/types.ts:11`): `processor` (file path exporting `default`), `concurrency?`, `maxMemory?`, `timeout?`, `autoRestart?`, `maxRestarts?`, `pollInterval?`, `manager?`, `connection?`, `heartbeatInterval?`, `idleTimeout?`, `idleRecycleMs?`, `autoStart?`, `autoStartPollMs?`.
- `WorkerProcess` (`sandboxed/types.ts:54`): `{ worker, busy, currentJob, currentToken, restarts, timeoutId, lastIdleAt, terminated }`.
- `IPCRequest`/`IPCResponse` (`sandboxed/types.ts:66/78`): main↔thread messages (`type: 'job' | 'result' | 'error' | 'progress' | 'log' | 'fail' | 'ready'`).
- `jobParser` builds an `InternalJob` from a TCP response, defaulting `maxAttempts=3`, `backoff=1000`, `attempts=0`, etc. (`jobParser.ts:13`).

## Business Logic / Control Flow

### Construction & startup
`resolveWorkerOptions` applies defaults: `concurrency=1`, `autorun=true`, `heartbeatInterval=10000`, `batchSize=min(opts,1000) default 10`, `pollTimeout=min(opts, 30000) default 0`, `useLocks=true`, `drainDelay=50`, `lockDuration=30000`, `maxStalledCount=1` (`worker.ts:25`). `queueKey = (prefixKey ?? '') + name` — all broker traffic uses `queueKey`, the user-facing `name` stays logical (`worker.ts:174`). Embedded mode warms the shared manager; TCP mode builds a pool (`poolSize = connection.poolSize ?? min(concurrency, 8)`), wires the ackBatcher to it, and installs an `onReconnect` hook that re-sends `RegisterWorker` after a reconnect (server drops registration on socket close) (`worker.ts:190-208`). If `autorun`, `run()` is called.

`run()` (`worker.ts:214`) sets `running`, defers a `ready` emit via `queueMicrotask` (so listeners attached right after `new Worker(...)` still fire — issue #76), subscribes to embedded `Stalled` events (unless `skipStalledCheck`), registers with the server (TCP), and starts heartbeat timers when `heartbeatInterval > 0 && !skipLockRenewal`. Embedded heartbeat loops `pulledJobIds` calling `manager.jobHeartbeat`; TCP uses `startHeartbeat` plus a separate worker-level `Heartbeat` timer (`worker.ts:240-255`, `worker.ts:962`). Then it kicks `poll()`.

### Pull loop
`poll()` (`worker.ts:639`): returns if not running/closing; if `activeJobs >= concurrency` reschedules in 10ms; if the rate limiter is blocking, reschedules after `getTimeUntilNextSlot()` (min 10ms); otherwise calls `tryProcess()`.

`tryProcess()` (`worker.ts:664`): picks an eligible buffered job (`getNextEligibleJob`); if none, `doPullBatch()` fetches more and buffers them. After the async pull it **re-checks** `running/_closing`, then re-checks the concurrency gate atomically before `startJob` — if full it requeues the item to the buffer front and reschedules (issue #96, `worker.ts:696`). When nothing is runnable it may emit `drained` (throttled to once/sec) and reschedule using `pollTimeout>0 ? 10 : drainDelay`.

`doPullBatch()` (`worker.ts:806`) computes the **leased** count: `groupBlockedBuffer ? activeJobs : pulledJobIds.size`, then `slots = concurrency - leased - pendingPull`; `batchSize = min(opts.batchSize, slots, 1000)`. It reserves `pendingPull += batchSize` across the in-flight pull so overlapping pulls see each other (issue #98 over-lease fix, `worker.ts:830-846`). Pull is dispatched to `pullEmbedded`/`pullTcp`. With locks + multi-connection pool, freshly pulled locks are renewed immediately to close a re-dispatch window (`worker.ts:749`).

`startJob()` (`worker.ts:849`): dedups jobs already in `activeJobIds` (issue #33 stall-retry race), increments `activeJobs`, applies worker-level `removeOnComplete/removeOnFail` defaults, increments the group counter, stores the lock token, then runs `processJob`. The `.finally` decrements counters, drops the job from all tracking sets, records the rate-limiter token, and re-polls. If a slot remains it schedules one `setImmediate(tryProcess)` (guarded by `processingScheduled`) for pipelining (`worker.ts:906`).

### Processing & outcome (`processor.ts:60`)
`processJob` builds the public `Job` with all handlers (`processorHandlers.ts`), emits `active`, then awaits `processor(job)`. Success path: if the processor called `moveToCompleted`/`moveToFailed` (`manualMove`), normal auto-ack is skipped (`handleManualMove`, `processor.ts:176`). Otherwise it acks — embedded `manager.ack(id, result, token)` or `ackBatcher.queue(...)` — then emits `completed`. A "job not found"/"not in processing" ack error is treated as an expected stale-ack (stall detection already moved the job) and only emits `error` with context `ack-stale` (`processor.ts:158`, `isJobNotFoundError` `processor.ts:215`).

Failure path (`handleJobFailure`, `processor.ts:249`): `DelayedError` → `handleDelayedError` re-delays the job (`backoff || 1000`) without counting a failure; `UnrecoverableError` → forces `maxAttempts=1, attempts=0` so retries are skipped; stack lines are computed *before* the send (capped at 50 on the wire, authoritative cap server-side — bug #74), then `FAIL` is sent (embedded `manager.fail`, TCP `FAIL` with `stack`/`token`/`unrecoverable`). `failedReason` and `stacktrace` are populated on the event object, then `failed` is emitted.

### Sandboxed flow (`sandboxed/worker.ts`)
`start()` (`:152`) writes a wrapper `.ts` (with fsync + visibility polling for a macOS Worker spawn race, `wrapper.ts`), spawns worker 0 synchronously then the rest in parallel, starts heartbeat, emits `ready`, and runs `pullLoop()`. `pullLoop` (`:320`) finds an idle (or respawns a recycled) worker, pulls via `ops.pull(queue, workerId, 1000)`, and `dispatch`es over `postMessage`. `dispatch` (`:390`) sets a timeout timer and emits `active`. `handleMessage` (`:440`) routes `result`→`complete` (ack), `error`/`fail`→`fail`, `progress`/`log` to ops + events; messages whose `jobId` doesn't match the worker's current job are ignored. `handleTimeout` (`:509`) terminates the thread, fails the job, and restarts it via `handleCrash`. `handleCrash` (`:530`) fails the in-flight job, increments `restarts`, and respawns while `restarts < maxRestarts`. Idle workers are recycled after `idleRecycleMs` (keeping ≥1 alive); after `idleTimeout` the pool stops (or `stopAndWatch` polls `countWaiting` and restarts when `autoStart`).

## Concurrency & Locking

- **Concurrency cap** is enforced as a leased cap (`pulledJobIds.size`), not just `activeJobs`, plus the `pendingPull` reservation, to prevent over-leasing across overlapping async pulls (issues #96/#98, `worker.ts:806`). The gate is re-checked atomically just before `startJob` (no await between check and `activeJobs++`).
- **Lock-based ownership** (`useLocks`, default true): each pulled job gets a `token`; heartbeats renew the lock for **all** `pulledJobIds` (active and buffered) so buffered jobs don't expire (`workerHeartbeat.ts:24`). Lock TTL = `lockDuration` (default 30000), propagated to the server via `lockTtl` on pull (`workerPull.ts:79`). Acks/fails carry the token for ownership verification server-side; see [Concurrency & Locking](./concurrency-and-locking.md).
- **Stall race (#33)**: stall detection may re-dispatch a job the worker is still running. `startJob` dedups on `activeJobIds`; a stale ack/fail is swallowed (treated as expected).
- **Re-dispatch window (multi-connection)**: with `poolSize>1`, PULL and heartbeats travel on different sockets; the worker fires an immediate `sendHeartbeat` after a lock-based batch pull to renew before the periodic timer (`worker.ts:749`).
- **Group concurrency**: when `limiter.groupKey` is set, `GroupConcurrencyLimiter` caps `limiter.max` active jobs per group value; `getNextEligibleJob` scans the buffer for a runnable group and `doPullBatch` is allowed to pull-ahead when the buffer is group-blocked (`worker.ts:779`, `worker.ts:828`).

## Edge Cases & Failure Modes

- **Pull errors**: `handlePullError` (`worker.ts:915`) emits `error` (with `consecutiveErrors`/`context:'pull'`) and backs off exponentially `BASE_BACKOFF_MS=100 * 2^(n-1)` capped at `MAX_BACKOFF_MS=30000`. A successful pick resets `consecutiveErrors`.
- **ACK batching/backpressure** (`ackBatcher.ts`): flush triggers at `config.batchSize` or after `interval` (`DEFAULT_ACK_INTERVAL=50ms`). The buffer is bounded at `MAX_PENDING_ACKS=10000`; `queue()` blocks (awaits in-flight, then flushes) rather than dropping acks (`ackBatcher.ts:45`). `sendBatchWithRetry` retries up to `maxRetries=3` with exponential backoff (`100,200,400ms`); on exhaustion it logs `(N acks lost)` and rejects each pending promise (`ackBatcher.ts:105`). `stop()` rejects/clears any remaining acks.
- **Graceful close** (`worker.ts:516`): `close(false)` stops timers, then `releaseBufferedJobs()` moves buffered (pulled-but-unstarted) jobs back to `waiting` (`MoveToWait`/`moveActiveToWait` + lock release) so a drain never hangs on a buffer that can't advance while `_closing` — this addresses the historical close-hang on buffered jobs. The drain then waits only on genuinely in-flight `activeJobs` (`Bun.sleep(50)` loop). Acks are flushed and awaited, the worker is unregistered, then the pool closes. `close(true)` (force) sets `_forceClose` to break out of an in-progress graceful drain immediately and skips the wait. Best-effort: a job that can't be released is dropped locally and its server lock will expire/requeue it.
- **Embedded `removeDeduplicationKey`** and **`moveToWaitingChildren` over TCP** throw explicit "not supported" errors instead of silently no-oping (`processorHandlers.ts:391`, `:491`).
- **Job mutation handlers** (`processorHandlers.ts`) are state-aware: `retry` dispatches by job state (failed → `retryDlq`, active → `moveActiveToWait`, waiting/prioritized/delayed → no-op, else throw); `moveToDelayed` converts an absolute timestamp to a relative delay.
- **Sandboxed dispatch failure** fails the job immediately (`Dispatch failed: worker terminated`) and resets the worker (`sandboxed/worker.ts:417`). Worker crash/timeout fails the in-flight job and respawns up to `maxRestarts=10`. `safeEmitError` only emits `error` when listeners exist, avoiding uncaught exceptions (`sandboxed/worker.ts:590`). The wrapper script path is escaped against template-literal injection (`wrapper.ts:14`).
- **`prefixKey` mismatch**: a Worker only consumes jobs whose producing Queue used the same `prefixKey`; stalled-event scoping compares against `queueKey`, not `name` (`worker.ts:267`).

## Configuration

- **`WorkerOptions`** (see Data Models) — primary knobs. Defaults: `concurrency=1`, `heartbeatInterval=10000` (`0` disables), `batchSize=10` (max `1000`), `pollTimeout=0` (max `30000`), `useLocks=true`, `lockDuration=30000`, `drainDelay=50`, `maxStalledCount=1`, `autorun=true`.
- **`FORCE_EMBEDDED`** (`worker/types.ts:42`): `Bun.env.BUNQUEUE_EMBEDDED === '1'` forces embedded mode when `opts.embedded` is unset (used in tests).
- **`WORKER_CONSTANTS`** (`worker/types.ts:45`): `MAX_BACKOFF_MS=30000`, `BASE_BACKOFF_MS=100`, `MAX_POLL_TIMEOUT=30000`, `DEFAULT_ACK_INTERVAL=50`.
- **`SandboxedWorkerOptions`** defaults: `concurrency=1`, `maxMemory=256` (≤64 → Bun `smol` mode), `timeout=30000` (`0` disables), `autoRestart=true`, `maxRestarts=10`, `pollInterval=10`, `heartbeatInterval=10000` (TCP) / `5000` (embedded), `idleTimeout=0` (disabled), `idleRecycleMs=30000`, `autoStart=false`, `autoStartPollMs=5000`.
- TCP connection options (`connection`) — host (`localhost`), port (`6789`), `token`, `tls`, `poolSize`, timeouts, pipelining — resolved by `createTcpPool` (`worker.ts:45`). Server env (`WORKER_TIMEOUT_MS`, `LOCK_TIMEOUT_MS`) affects server-side stall/lock handling, not the client.

## Related Docs

- [Client SDK: Queue](./client-queue-sdk.md) — the producer counterpart.
- [Client Transport (TCP pool, reconnect, batching)](./client-transport.md)
- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md)
- [Concurrency & Locking](./concurrency-and-locking.md)
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md)
- [Core Queue Engine (QueueManager & Shards)](./core-queue-engine.md)
- [Dead Letter Queue (DLQ)](./dead-letter-queue.md)
- [FlowProducer & Job Dependencies](./flow-producer.md)
- [Simple Mode (Bunqueue all-in-one)](./simple-mode.md)
- [TCP Server Command Handlers](./tcp-server-handlers.md)
- [architecture](../architecture.md) · [data-model](../data-model.md)
