# Client SDK: Worker (& sandboxed)

> **Category:** Client SDK · **Source:** `src/client/worker/worker.ts`, `src/client/worker/runtime/`, `src/client/worker/types/`, `src/client/worker/handlers/`, `src/client/worker/processor.ts`, `src/client/worker/ackBatcher.ts`, `src/client/worker/workerPull.ts`, `src/client/worker/workerHeartbeat.ts`, `src/client/sandboxed/worker.ts`, `src/client/sandboxed/runtime/`, `src/client/sandboxed/types/`, `src/client/sandboxed/wrapper.ts`, `src/client/sandboxed/queueOps.ts`

## Purpose

The Worker SDK is the consumer side of bunqueue: a BullMQ-style polling worker that pulls jobs, runs a processor, and reports the outcome to the broker. The four-line `Worker` façade inherits focused state, control, manual-processing, lifecycle, buffer, polling and execution layers under `worker/runtime/`; its contracts live under `worker/types/`. `SandboxedWorker` follows the same four-line façade pattern with state, lifecycle, pool, dispatch and recovery modules under `sandboxed/runtime/` and separate types under `sandboxed/types/`. Both embedded and TCP transports use the same public API.

## Responsibilities & Scope

Owns:
- Job pull loop, concurrency gate, batch pulling, long-poll and pull-error backoff (`runtime/polling.ts`).
- Lease accounting and buffered-job selection (`runtime/state.ts`, `runtime/buffer.ts`, `runtime/execution.ts`).
- Processor invocation, auto-ack on success, fail/retry dispatch, `DelayedError`/`UnrecoverableError` handling, manual `moveToCompleted`/`moveToFailed` (`processor.ts`).
- ACK batching with backpressure and retry (`ackBatcher.ts`).
- Job and worker heartbeats / lock renewal (`workerHeartbeat.ts`,
  `runtime/control.ts`, `runtime/execution.ts`).
- Worker registration/unregistration with the server, and re-registration on
  reconnect (`runtime/state.ts`, `runtime/execution.ts`, `runtime/lifecycle.ts`).
- Client-side rate limiting and per-group concurrency (`workerRateLimiter.ts`, `groupConcurrency.ts`).
- Graceful/forced close and buffered-job release (`runtime/lifecycle.ts`).
- Sandboxed thread lifecycle, dispatch, timeout, crash restart and idle recycle (`sandboxed/runtime/`).

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

### `Worker<T, R>` (`worker/worker.ts`, `worker/runtime/`)

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

### `SandboxedWorker<T>` (`sandboxed/worker.ts`, `sandboxed/runtime/`)

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

`PULL`, `PULLB`; `ACKB` (batch ack), `FAIL`; `MoveToWait`, `MoveToDelayed`, `MoveToWaitingChildren`; `JobHeartbeat`, `JobHeartbeatB`, `Heartbeat`; `RegisterWorker`, `UnregisterWorker`; `Ping`; `ExtendLock`, `ExtendLocks`. Processor `Job` handlers additionally send `Progress`, `AddLog`, `GetState`, `GetResult`, `GetChildrenValues`, `GetFailedChildrenValues`, `GetIgnoredChildrenFailures`, `RemoveChildDependency`, `RemoveJobDeduplicationKey`, `RemoveUnprocessedChildren`, `Cancel` (remove), `Update`, `Promote`, `ChangeDelay`, `ChangePriority`, `ClearLogs`, `Discard`, `WaitJob`. `SandboxedWorker` TCP ops also use plain `ACK` and `GetJobCounts`. See [TCP Server Command Handlers](./tcp-server-handlers.md).

### Events emitted

`Worker`: `ready`, `active(job)`, `completed(job, result)`, `failed(job,
error)`, `progress(job, n)`, `stalled(jobId, reason)` (embedded only, via
`QueueManager.subscribe`), `error(err)`, `cancelled({jobId, reason})`,
`log(job, msg)`, `drained`, `closed` (`worker/runtime/state.ts`).
`SandboxedWorker`: `ready`, `active`, `completed`, `failed`, `progress`, `log`,
`error`, `closed` (no `drained` / `stalled` / `cancelled`)
(`sandboxed/runtime/state.ts`).

## Data Models

See [data-model](../data-model.md) for the full `Job` shape. Key types used here:

- `WorkerOptions`: `client/types/worker.ts`.
- `ExtendedWorkerOptions`: `client/worker/types/options.ts`, resolved by
  `worker/runtime/options.ts`.
- `Processor<T, R>`: `client/types/flow.ts`; processor-internal contracts:
  `client/worker/types/processor.ts`.
- `PendingAck`: `client/worker/types/transport.ts`.
- `SandboxedWorkerOptions`: `client/sandboxed/types/options.ts`.
- `WorkerProcess` and IPC requests/responses:
  `client/sandboxed/types/process.ts`.
- `jobParser` builds an `InternalJob` from a TCP response, defaulting `maxAttempts=3`, `backoff=1000`, `attempts=0`, etc. (`jobParser.ts:13`).

## Business Logic / Control Flow

### Construction & startup
`resolveWorkerOptions` in `worker/runtime/options.ts` applies defaults:
`concurrency=1`, `autorun=true`, `heartbeatInterval=10000`,
`batchSize=min(opts,1000)` default 10, `pollTimeout=min(opts,30000)` default 0,
`useLocks=true`, `drainDelay=50`, `lockDuration=30000`,
`maxStalledCount=1`. `worker/runtime/state.ts` owns
`queueKey = (prefixKey ?? '') + name`, transport construction, ACK-batcher
wiring, reconnect registration, and the `autorun` decision.

`run()` (`worker/runtime/control.ts`) sets `running`, defers a `ready` emit via
`queueMicrotask` (so immediately attached listeners still fire), subscribes to
embedded stalled events, registers the worker, and starts job-lease and
worker-registration heartbeat timers. It then kicks `poll()`.

### Pull loop
`poll()` (`worker/runtime/polling.ts`) returns if not running/closing; if
`activeJobs >= concurrency` it reschedules in 10ms; if the rate limiter blocks,
it waits for the next slot; otherwise it calls `tryProcess()`.

`tryProcess()` (`worker/runtime/polling.ts`) picks an eligible buffered job
(`runtime/buffer.ts`) or pulls a batch. After the async pull it **re-checks**
`running/_closing` and the concurrency gate before `startJob`; a full gate puts
the item back at the buffer front (issue #96). Empty polls emit `drained` at
most once per second.

`doPullBatch()` (`worker/runtime/polling.ts`) computes the **leased** count,
subtracts `pendingPull`, caps the batch at 1,000, and reserves the requested
slots across the asynchronous pull (issue #98). `runtime/buffer.ts` registers
the leases and immediately renews freshly pulled locks when a multi-connection
pool could otherwise create a re-dispatch window.

`startJob()` (`worker/runtime/execution.ts`) deduplicates `activeJobIds`, claims
the slot and lock token, applies worker-level retention defaults, invokes
`processJob`, and releases every counter/tracking set exactly once in
`.finally`. A guarded `setImmediate(tryProcess)` pipelines the next free slot.

### Processing & outcome (`processor.ts:60`)
`processJob` builds the public `Job` with all handlers (`processorHandlers.ts`), emits `active`, then awaits `processor(job)`. Success path: if the processor called `moveToCompleted`/`moveToFailed` (`manualMove`), normal auto-ack is skipped (`handleManualMove`, `processor.ts:176`). Otherwise it acks — embedded `manager.ack(id, result, token)` or `ackBatcher.queue(...)` — then emits `completed`. A "job not found"/"not in processing" ack error is treated as an expected stale-ack (stall detection already moved the job) and only emits `error` with context `ack-stale` (`processor.ts:158`, `isJobNotFoundError` `processor.ts:215`).

Failure path (`handleJobFailure`, `processor.ts:249`): `DelayedError` → `handleDelayedError` re-delays the job (`backoff || 1000`) without counting a failure; `UnrecoverableError` → forces `maxAttempts=1, attempts=0` so retries are skipped; stack lines are computed *before* the send (capped at 50 on the wire, authoritative cap server-side — bug #74), then `FAIL` is sent (embedded `manager.fail`, TCP `FAIL` with `stack`/`token`/`unrecoverable`). `failedReason` and `stacktrace` are populated on the event object, then `failed` is emitted.

### Sandboxed flow (`sandboxed/runtime/`)

`lifecycle.ts` writes the wrapper, starts the process pool, owns heartbeat and
shutdown. `pool.ts` spawns/recycles Bun Workers and runs the pull loop.
`dispatch.ts` owns the per-job timeout and routes result/error/progress/log IPC
messages, rejecting messages that do not match the worker's current job.
`recovery.ts` fails crashed work, bounds respawns by `maxRestarts`, renews
leases, and creates the public event job. Idle recycling keeps at least one
process alive; `autoStart` can watch the queue after an idle stop.

## Concurrency & Locking

- **Concurrency cap** is enforced as a leased cap (`pulledJobIds.size`), not
  just `activeJobs`, plus the `pendingPull` reservation in
  `worker/runtime/polling.ts`. The gate is re-checked immediately before
  `startJob` in `runtime/execution.ts`.
- **Lock-based ownership** (`useLocks`, default true): each pulled job gets a `token`; heartbeats renew the lock for **all** `pulledJobIds` (active and buffered) so buffered jobs don't expire (`workerHeartbeat.ts:24`). Lock TTL = `lockDuration` (default 30000), propagated to the server via `lockTtl` on pull (`workerPull.ts:79`). Acks/fails carry the token for ownership verification server-side; see [Concurrency & Locking](./concurrency-and-locking.md).
- **Stall race (#33)**: stall detection may re-dispatch a job the worker is still running. `startJob` dedups on `activeJobIds`; a stale ack/fail is swallowed (treated as expected).
- **Re-dispatch window (multi-connection)**: with `poolSize>1`, PULL and
  heartbeats may travel on different sockets; `worker/runtime/buffer.ts` sends
  an immediate heartbeat after a lock-based batch pull.
- **Group concurrency**: `GroupConcurrencyLimiter` caps `limiter.max` active
  jobs per group; `runtime/buffer.ts` scans for a runnable group and
  `runtime/polling.ts` permits bounded pull-ahead when the buffer is blocked.

## Edge Cases & Failure Modes

- **Pull errors**: `handlePullError` in `worker/runtime/polling.ts` emits
  `error` with `consecutiveErrors` / `context:'pull'` and backs off
  exponentially from 100ms to 30s. A successful pick resets the counter.
- **ACK batching/backpressure** (`ackBatcher.ts`): flush triggers at `config.batchSize` or after `interval` (`DEFAULT_ACK_INTERVAL=50ms`). The buffer is bounded at `MAX_PENDING_ACKS=10000`; `queue()` blocks (awaits in-flight, then flushes) rather than dropping acks (`ackBatcher.ts:45`). `sendBatchWithRetry` retries up to `maxRetries=3` with exponential backoff (`100,200,400ms`); on exhaustion it logs `(N acks lost)` and rejects each pending promise (`ackBatcher.ts:105`). `stop()` clears any still-queued acks *without settling their promises* (callers are expected to `flush()` + `waitForInFlight()` first, as `Worker.close()` does); a batch already mid-retry when `stop()` lands is rejected with `AckBatcher stopped`.
- **Graceful close** (`worker/runtime/lifecycle.ts`): `close(false)` stops
  timers, moves buffered leased jobs back to waiting, waits only for active
  processors, flushes ACKs, unregisters, and closes the pool. `close(true)`
  breaks an in-progress graceful drain. A job that cannot be released is
  recovered by server lock expiration.
- **Ownership and waiting-children transitions:** processor and sandboxed `Job` objects route `removeDeduplicationKey` through the owner-aware manager/wire operation and route `moveToWaitingChildren` through the real broker transition in both modes. A stale deduplication owner returns `false`; moving a non-active job returns `false` or a broker error rather than silently changing unrelated state.
- **Job mutation handlers** (`processorHandlers.ts`) are state-aware: `retry` dispatches by job state (failed → `retryDlq`, active → `moveActiveToWait`, waiting/prioritized/delayed → no-op, else throw); `moveToDelayed` converts an absolute timestamp to a relative delay.
- **Sandboxed dispatch failure** fails the job immediately and resets the worker
  (`sandboxed/runtime/dispatch.ts`). Crash/timeout recovery and guarded error
  emission live in `sandboxed/runtime/recovery.ts`; wrapper-path escaping lives
  in `sandboxed/wrapper.ts`.
- **`prefixKey` mismatch**: a Worker only consumes jobs whose producing Queue
  used the same prefix; `worker/runtime/state.ts` creates `queueKey`, and
  `runtime/control.ts` scopes stalled events to it.

## Configuration

- **`WorkerOptions`** (see Data Models) — primary knobs. Defaults: `concurrency=1`, `heartbeatInterval=10000` (`0` disables), `batchSize=10` (max `1000`), `pollTimeout=0` (max `30000`), `useLocks=true`, `lockDuration=30000`, `drainDelay=50`, `maxStalledCount=1`, `autorun=true`.
- **`FORCE_EMBEDDED` / `WORKER_CONSTANTS`** (`worker/constants.ts`): the env
  override plus backoff, poll-timeout, and ACK-interval constants.
- **`SandboxedWorkerOptions`** defaults: `concurrency=1`, `maxMemory=256` (≤64 → Bun `smol` mode), `timeout=30000` (`0` disables), `autoRestart=true`, `maxRestarts=10`, `pollInterval=10`, `heartbeatInterval=10000` (TCP) / `5000` (embedded), `idleTimeout=0` (disabled), `idleRecycleMs=30000`, `autoStart=false`, `autoStartPollMs=5000`.
- TCP connection options are resolved by `createTcpPool` in
  `worker/runtime/options.ts`. Server env (`WORKER_TIMEOUT_MS`,
  `LOCK_TIMEOUT_MS`) affects server-side stall/lock handling, not the client.

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
