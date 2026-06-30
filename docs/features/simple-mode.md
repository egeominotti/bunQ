# Simple Mode (Bunqueue all-in-one)

> **Category:** Client SDK · **Source:** `src/client/bunqueue.ts`, `src/client/bunqueue/types.ts`, `src/client/bunqueue/circuitBreaker.ts`, `src/client/bunqueue/batch.ts`, `src/client/bunqueue/retry.ts`, `src/client/bunqueue/aging.ts`, `src/client/bunqueue/cancellation.ts`, `src/client/bunqueue/triggers.ts`, `src/client/bunqueue/ttl.ts`, `src/client/bunqueue/dlqRateLimit.ts`, `src/client/bunqueue/dedupDebounce.ts`

## Purpose

`Bunqueue` is a thin all-in-one wrapper that constructs a [`Queue`](./client-queue-sdk.md) and a [`Worker`](./client-worker-sdk.md) for the same queue name and stitches together a single processing pipeline. It exists so a single process can both produce and consume jobs through one object, while layering opt-in conveniences — named route handlers, an onion middleware chain, in-process retry with backoff, a circuit breaker, batch accumulation, priority aging, TTL expiry, graceful cancellation, event triggers, dedup/debounce defaults, and DLQ/rate-limit passthrough — that the bare `Queue`/`Worker` do not provide. It does not implement a new transport or storage path: every queue operation delegates to the underlying `Queue`/`Worker` (TCP or embedded).

## Responsibilities & Scope

Owns:

- Construction and lifecycle of a paired `Queue` + `Worker` (`src/client/bunqueue.ts:93-94`).
- Mode selection: exactly one of `processor`, `routes`, or `batch` (`src/client/bunqueue.ts:71-87`).
- The per-job processing pipeline: circuit-breaker gate → TTL gate → cancellation registration → optional retry wrapper → middleware onion → base processor (`src/client/bunqueue.ts:148-192`).
- In-process subsystems: `WorkerCircuitBreaker`, `BatchAccumulator`, `PriorityAger`, `CancellationManager`, `TtlChecker`, `TriggerManager`, `DedupDebounceMerger`, `DlqRateLimitManager`.
- Re-exposing `add`/`addBulk`/`getJob`/counts, cron helpers, pause/resume, and event subscription as a flat API.

Does NOT own (delegated):

- Actual enqueue/dequeue/ack/fail, persistence, sharding — owned by [Core Queue Engine](./core-queue-engine.md) and [Job Lifecycle](./job-lifecycle.md) via `Queue`/`Worker`.
- Cron/scheduler semantics — `cron`/`every`/`removeCron`/`listCrons` delegate to `Queue.upsertJobScheduler` / `removeJobScheduler` / `getJobSchedulers` ([Scheduler & Cron](./scheduler-and-cron.md)).
- DLQ storage and rate limiting — `DlqRateLimitManager` is a pure mixin over `Queue` methods ([Dead Letter Queue](./dead-letter-queue.md), [Rate Limiting & Concurrency](./rate-limiting-and-concurrency.md)).
- Deduplication/debounce enforcement — `DedupDebounceMerger` only injects `JobOptions.deduplication`/`debounce`; the server enforces them ([Deduplication & Unique Jobs](./deduplication-and-unique.md)).
- Worker-level event emission — `on`/`once`/`off` forward straight to `Worker` ([Webhooks, Events & Job Logs](./webhooks-and-events.md)).

## Dependencies

Internal:

- [`Queue`](./client-queue-sdk.md) (`src/client/queue/queue.ts`) and [`Worker`](./client-worker-sdk.md) (`src/client/worker/worker.ts`) — the two objects constructed in the ctor.
- `Queue.upsertJobScheduler` / `getJobSchedulers` / `removeJobScheduler` for cron ([Scheduler & Cron](./scheduler-and-cron.md)).
- `Queue.changeJobPriority`, `Queue.getWaitingAsync`, `Queue.getJobsAsync({ state: 'prioritized' })` for priority aging (`src/client/bunqueue/aging.ts:33-46`).
- `Queue.setDlqConfig/getDlq/retryDlq/purgeDlq` and `setGlobalRateLimit/removeGlobalRateLimit` via `DlqRateLimitManager`.
- Types from `src/client/types.ts` (`Job`, `JobOptions`, `Processor`, `DeduplicationOptions`, `DebounceOptions`, DLQ types).

External/runtime:

- `AbortController` / `AbortSignal` (Web standard, Bun-native) for cancellation (`src/client/bunqueue/cancellation.ts`).
- `setTimeout`/`setInterval` for retry backoff, circuit reset, batch flush, and aging ticks. No external npm dependencies.

## Public Interface

Exported class (`src/client/bunqueue.ts:54`):

```typescript
class Bunqueue<T = unknown, R = unknown> {
  readonly name: string;
  readonly queue: Queue<T>;
  readonly worker: Worker<T, R>;

  constructor(name: string, opts: BunqueueOptions<T, R>);

  use(middleware: BunqueueMiddleware<T, R>): this;

  add(name: string, data: T, opts?: JobOptions): Promise<Job<T>>;
  addBulk(jobs: Array<{ name: string; data: T; opts?: JobOptions }>): Promise<Job<T>[]>;
  getJob(id: string): Promise<Job<T> | null>;
  getJobCounts(); getJobCountsAsync(); count(); countAsync();

  cron(id: string, pattern: string, data?: T, opts?: { timezone?: string; jobOpts?: JobOptions }): Promise<SchedulerInfo | null>;
  every(id: string, intervalMs: number, data?: T, opts?: { jobOpts?: JobOptions }): Promise<SchedulerInfo | null>;
  removeCron(id: string); listCrons();

  cancel(jobId: string, gracePeriodMs?: number): void;
  isCancelled(jobId: string): boolean;
  getSignal(jobId: string): AbortSignal | null;

  getCircuitState(): CircuitState; resetCircuit(): void;

  trigger(rule: TriggerRule<T>): this;

  setDefaultTtl(ttlMs: number): void; setNameTtl(name: string, ttlMs: number): void;

  setDlqConfig(config: Partial<DlqConfig>): void; getDlqConfig(): DlqConfig;
  getDlq(filter?: DlqFilter): DlqEntry<T>[]; getDlqStats(): DlqStats;
  retryDlq(id?: string); purgeDlq();

  setGlobalRateLimit(max: number, duration?: number): void; removeGlobalRateLimit(): void;

  on(event, listener): this; once(event, listener): this; off(event, listener): this;

  pause(): void; resume(): void;
  close(force?: boolean): Promise<void>;
  isRunning(): boolean; isPaused(): boolean; isClosed(): boolean;
}
```

Middleware type (`src/client/bunqueue/types.ts:16-19`):

```typescript
type BunqueueMiddleware<T, R> = (job: Job<T>, next: () => Promise<R>) => Promise<R>;
```

Events (forwarded to `Worker`, `src/client/bunqueue.ts:334-358`): `ready`, `drained`, `closed`, `active`, `completed`, `failed`, `progress`, `stalled`, `error`. `once` is typed for `ready`/`drained`/`closed`/`completed`/`failed`.

No TCP commands, HTTP endpoints, or CLI commands are defined here — those belong to [TCP Server Handlers](./tcp-server-handlers.md), [HTTP API](./http-api.md), and the [CLI](./cli.md). `add`/`cron`/etc. translate to the same `Queue` calls those layers expose.

## Data Models

All option shapes live in `src/client/bunqueue/types.ts`. See [data-model](../data-model.md) for `Job`/`JobOptions`/`DlqEntry`.

`BunqueueOptions<T, R>` (`types.ts:137-173`) — superset of `QueueOptions` + `WorkerOptions` knobs plus feature configs:

- Mode (exactly one): `processor?: Processor<T,R>`, `routes?: Record<string, Processor<T,R>>`, `batch?: BatchConfig<T,R>`.
- Connection/transport: `connection`, `embedded`, `dataPath`, `prefixKey` (forwarded to both Queue and Worker), `autoBatch`, `defaultJobOptions`.
- Worker tuning: `concurrency`, `autorun`, `heartbeatInterval`, `batchSize`, `pollTimeout`, `rateLimit`/`limiter`, `removeOnComplete`, `removeOnFail`.
- Feature configs: `retry?: RetryConfig`, `circuitBreaker?: CircuitBreakerConfig`, `ttl?: JobTtlConfig`, `priorityAging?: PriorityAgingConfig`, `deduplication?: BunqueueDeduplicationConfig`, `debounce?: BunqueueDebounceConfig`, `dlq?: BunqueueDlqConfig`.

`RetryConfig` (`types.ts:25-36`): `maxAttempts?` (3), `delay?` (1000), `strategy?` (`'exponential'`), `customBackoff?(attempt, error)`, `retryIf?(error, attempt)`.

`CircuitBreakerConfig` (`types.ts:39-50`): `threshold?` (5), `resetTimeout?` (30000), `onOpen?(failures)`, `onClose?()`, `onHalfOpen?()`. `CircuitState = 'closed' | 'open' | 'half-open'`.

`TriggerRule<T>` (`types.ts:56-69`): `on` (source job name), `event?` (`'completed'` default | `'failed'`), `create` (new job name), `data(result, job) => T`, `opts?`, `condition?(result, job) => boolean`.

`PriorityAgingConfig` (`types.ts:72-83`): `interval?` (60000), `minAge?` (60000), `boost?` (1), `maxPriority?` (100), `maxScan?` (100).

`BatchConfig<T,R>` (`types.ts:89-96`): `size` (required), `timeout?` (5000), `processor: (jobs: Job<T>[]) => Promise<R[]>`.

`JobTtlConfig` (`types.ts:99-104`): `defaultTtl?` (0 = off), `perName?: Record<string, number>`.

`BunqueueDeduplicationConfig` (`types.ts:107-114`): `ttl?` (3600000), `extend?`, `replace?`.
`BunqueueDebounceConfig` (`types.ts:117-120`): `ttl` (required).
`BunqueueDlqConfig` (`types.ts:123-134`): `autoRetry?`, `autoRetryInterval?` (3600000), `maxAutoRetries?` (3), `maxAge?` (604800000), `maxEntries?` (10000).

## Business Logic / Control Flow

**Construction** (`src/client/bunqueue.ts:70-107`):

1. Validate mode count: error if zero or more than one of `processor`/`routes`/`batch` is set (`:71-73`).
2. Build `baseProcessor`: if `batch`, create a `BatchAccumulator` and use its buffering processor; else use `routes`-derived dispatcher or the raw `processor` (`:81-87`). The route dispatcher looks up `routeMap[job.name]` and throws `No route for job "<name>" in queue "<queue.name>"` if missing (`:109-116`).
3. Wrap the base processor so the `Worker` always calls `processJob(job)` (`:91`).
4. Construct `Queue` (`buildQueueOpts`, `:118-127`) and `Worker` (`buildWorkerOpts`, `:129-144`) for `name`.
5. Instantiate `DlqRateLimitManager` (apply `opts.dlq` if present), `WorkerCircuitBreaker` (only if `opts.circuitBreaker`), `TriggerManager`, and `PriorityAger` (started immediately if `opts.priorityAging`) (`:96-106`).

**Per-job pipeline** (`processJob`, `src/client/bunqueue.ts:148-174`), in order:

1. If circuit breaker `isOpen()` → reject with `Circuit breaker is open` (`:150-152`).
2. If `TtlChecker.isExpired(job.name, job.timestamp)` → reject with `Job expired (age: …ms)` (`:154-156`). Expiry compares `Date.now() - jobTimestamp > ttl` (`ttl.ts:21-25`); creation timestamp is used, so the gate fires only when the job is actually pulled, not proactively.
3. Register an `AbortController` for `job.id` (`:158`, `cancellation.ts:10-14`).
4. Build `runChain = () => runMiddlewareChain(job, ac)`. If `retryConfig` is set, wrap in `executeWithRetry`; otherwise call `runChain()` once (`:159-160`).
5. On resolve → `cb.onSuccess()`, unregister cancellation, return result. On reject → `cb.onFailure()`, unregister cancellation, rethrow (`:162-173`).

**Middleware onion** (`runMiddlewareChain`, `src/client/bunqueue.ts:176-192`): with zero middlewares, the base processor runs directly. Otherwise `next()` walks `middlewares[0..n)` then the base processor, forming `mw1 → mw2 → … → base → … → mw2 → mw1`. Before each step `next()` checks `ac.signal.aborted` and rejects with `Job cancelled` (`:186`).

**In-process retry** (`executeWithRetry`, `retry.ts:50-71`): re-invokes the chain up to `maxAttempts`. On failure, if `retryIf` returns false it rethrows immediately; otherwise it sleeps `calculateBackoff(...)` and retries. Backoff strategies (`retry.ts:7-47`): `fixed`, `exponential` (`base·2^(n-1)`), `jitter` (`exp·(0.5+random)`), `fibonacci` (`base·fib(n)`), `custom` (`customBackoff(attempt, error)`, falls back to base if absent).

**Circuit breaker** (`circuitBreaker.ts`): `onFailure` increments a consecutive-failure counter; once `failures >= threshold` (or any failure while `half-open`) it calls `open()` (`:38-45`). `open()` sets state `open`, fires `onOpen`, **pauses the Worker**, and schedules a `resetTimeout` timer that flips to `half-open`, fires `onHalfOpen`, and **resumes the Worker** (`:47-59`). The next success in `half-open` closes the circuit (`:28-36`). `reset()` clears state/timer and resumes the worker if paused (`:61-71`).

**Batch mode** (`batch.ts`): the buffering processor pushes `{job, resolve, reject}` and flushes when `buffer.length >= size`, else arms a `timeout` timer (`:25-40`). `flush()` splices the buffer, calls the user `processor(jobs)`, and resolves each entry with `results[i]` positionally (`undefined` if the array is shorter) or rejects all on error (`:42-65`).

**Triggers** (`triggers.ts`): the first `trigger()` lazily subscribes to the worker's `completed`/`failed` events (`:27-38`). On fire, every rule whose `on === job.name`, whose `event` matches, and whose `condition` passes enqueues `queue.add(rule.create, rule.data(...), rule.opts)` (`:40-49`).

**Priority aging** (`aging.ts`): every `interval`, fetch up to `maxScan` waiting + prioritized jobs, and for each with `age >= minAge` and `priority < maxPriority`, call `changeJobPriority` to `min(priority + boost, maxPriority)`; failures are swallowed best-effort (`:26-53`).

**Dedup/debounce merge** (`dedupDebounce.ts`): on every `add`/`addBulk`, if configured and the caller did not already set `deduplication`/`debounce`, inject `deduplication.id = \`${name}:${JSON.stringify(data)}\`` (ttl default 3600000) and/or `debounce = { id: name, ttl }` (`:21-40`).

**Shutdown** (`close`, `src/client/bunqueue.ts:371-378`): destroy ager (clears interval), circuit breaker (clears timer), batch accumulator (`destroy()` flushes any remaining buffered jobs, `batch.ts:67-76`), abort all pending cancellations, then `worker.close(force)` and `queue.close()`.

## Concurrency & Locking

This module holds no shard locks; all locking happens inside `Queue`/`Worker`/`QueueManager` ([Concurrency & Locking](./concurrency-and-locking.md)). Local concurrency concerns:

- The `Worker`'s `concurrency` determines how many `processJob` invocations run in parallel. Batch mode relies on this: a batch only fills to `size` if at least `size` jobs are processed concurrently, otherwise the `timeout` flush is what closes a partial batch. With `concurrency` below `batch.size`, batches are bounded by `timeout`.
- `WorkerCircuitBreaker` mutates shared `state`/`failures` from `onSuccess`/`onFailure` callbacks driven by concurrent job completions; these run on the single JS event loop, so updates are serialized (no atomics needed), but the failure counter is consecutive-style and a burst of concurrent failures all increment it before `open()` pauses the worker.
- Cancellation is keyed by `job.id` in a `Map`; `unregister` runs in both resolve and reject paths so the controller is dropped once per job.

## Edge Cases & Failure Modes

- **Two independent retry layers.** `RetryConfig` retries the processor **in-process** (same pull, same job, no requeue) and is entirely separate from the queue-level `JobOptions.attempts`/`backoff`. A job can be retried `maxAttempts` times inside one `processJob`, and only if it still throws does the `Worker` mark it failed (which may then trigger queue-level retry/DLQ). Total attempts multiply.
- **Cancellation is cooperative and middleware-bound.** `ac.signal.aborted` is only checked at `next()` boundaries (`bunqueue.ts:186`). With zero middlewares the base processor runs to completion regardless of `cancel()`; to honor cancellation a processor must read `getSignal(jobId)` itself. `cancel()` on an unknown/finished job id is a no-op (`cancellation.ts:23-25`).
- **TTL rejects, it does not delete.** An expired job is rejected at processing time and flows through normal `Worker` failure handling (retry/DLQ); it is not silently dropped, and a job that is never pulled is never expired.
- **Circuit open pauses the whole worker.** While open, no jobs of any name are processed until `resetTimeout` elapses. A single failure during `half-open` re-opens immediately.
- **Route miss throws.** Unrouted job names throw synchronously inside the processor and become job failures, not silent drops (`bunqueue.ts:113`).
- **Batch result alignment is positional.** If the batch `processor` returns fewer results than jobs (or reorders), jobs receive the wrong/`undefined` result; a thrown error rejects every job in the batch. Buffered-but-not-flushed jobs are flushed on `close()`, but jobs still in the worker's poll loop are not part of `BatchAccumulator`.
- **Dedup default key uses `JSON.stringify(data)`.** Non-deterministic key ordering or unstringifiable data affects the dedup id; debounce id is just the job `name`, so all jobs of one name share a debounce window.
- **Trigger errors are unobserved.** `fire()` calls `void this.queue.add(...)` with no catch; a failed trigger enqueue is dropped silently. Triggers also only attach after the first `trigger()` call.
- **Priority aging is best-effort.** `changeJobPriority` failures are swallowed; aging only scans the first `maxScan` waiting + prioritized jobs per tick, so deep backlogs age slowly.
- **`pause()`/`resume()` act on both** the queue and the worker (`bunqueue.ts:362-369`); the circuit breaker also calls `worker.pause()/resume()` directly, so an externally-paused worker can be resumed by a circuit half-open transition.

## Configuration

Behavior is configured entirely through `BunqueueOptions` (no env vars are read in this module). Transport-related env vars are honored by the underlying `Queue`/`Worker` ([Configuration & Entrypoint](./configuration.md)). Defaults (from `src/client/bunqueue/types.ts`):

| Option | Default |
| ------ | ------- |
| `retry.maxAttempts` / `delay` / `strategy` | 3 / 1000ms / `exponential` |
| `circuitBreaker.threshold` / `resetTimeout` | 5 / 30000ms |
| `priorityAging.interval` / `minAge` / `boost` / `maxPriority` / `maxScan` | 60000 / 60000 / 1 / 100 / 100 |
| `batch.size` / `timeout` | required / 5000ms |
| `ttl.defaultTtl` | 0 (disabled) |
| `deduplication.ttl` | 3600000ms |
| `dlq.autoRetryInterval` / `maxAutoRetries` / `maxAge` / `maxEntries` | 3600000 / 3 / 604800000 / 10000 |
| `concurrency`, `heartbeatInterval`, `batchSize`, `pollTimeout` | inherited from `Worker` defaults |

`prefixKey` namespaces the queue name on the broker and is forwarded to both the `Queue` and the `Worker`, isolating jobs, workers, cron schedulers, stats, and DLQ between environments.

## Related Docs

- [Client SDK: Queue](./client-queue-sdk.md) · [Client SDK: Worker](./client-worker-sdk.md)
- [Scheduler & Cron](./scheduler-and-cron.md) · [Dead Letter Queue](./dead-letter-queue.md) · [Rate Limiting & Concurrency](./rate-limiting-and-concurrency.md)
- [Deduplication & Unique Jobs](./deduplication-and-unique.md) · [Webhooks, Events & Job Logs](./webhooks-and-events.md)
- [Job Lifecycle](./job-lifecycle.md) · [Core Queue Engine](./core-queue-engine.md) · [Workflow Engine](./workflow-engine.md)
- [architecture](../architecture.md) · [data-model](../data-model.md)
