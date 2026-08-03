# Rate Limiting & Concurrency Control

> **Category:** Control · **Source:** `src/domain/queue/limiterManager.ts`, `src/domain/types/queue.ts`, `src/infrastructure/server/rateLimiter.ts`, `src/client/worker/workerRateLimiter.ts`, `src/client/queue/rateLimit.ts`, `src/client/worker/groupConcurrency.ts`

## Purpose

bunqueue throttles work at three independent layers: (1) **per-queue rate + concurrency limits** enforced server-side at pull time (`LimiterManager` + token-bucket `RateLimiter` + `ConcurrencyLimiter`), (2) a **protocol-level abuse limiter** that caps raw TCP/HTTP requests per client IP (`ProtocolRateLimiter`), and (3) **worker-side limiters** in the client SDK that gate how fast a single `Worker` starts jobs (`WorkerRateLimiter`, BullMQ v5 `{max, duration}`) and how many jobs run concurrently per group key (`GroupConcurrencyLimiter`). These layers are orthogonal — a queue can have a server rate limit while a worker independently applies its own limiter, and both fire on top of the always-on protocol limiter.

## Responsibilities & Scope

Owns:
- Per-queue rate limiting (token bucket, jobs per configured window) and concurrency capping, stored per-shard in `LimiterManager` (`src/domain/queue/limiterManager.ts:10`).
- Token-bucket refill math and concurrency slot accounting (`src/domain/types/queue.ts:33`, `:84`).
- Protocol-level per-client sliding-window request limiting for TCP and HTTP (`src/infrastructure/server/rateLimiter.ts:70`).
- Worker-local rate limiting and per-group concurrency in the client SDK (`src/client/worker/workerRateLimiter.ts:12`, `src/client/worker/groupConcurrency.ts:11`).
- The `RateLimit` / `RateLimitClear` / `SetConcurrency` / `ClearConcurrency` TCP commands, their HTTP routes, and CLI subcommands.

Does NOT own:
- The actual pull/dequeue decision and queue locking — delegated to [Job Lifecycle](./job-lifecycle.md) and [Concurrency & Locking](./concurrency-and-locking.md). Limiters are consulted inside the pull lock but don't manage it.
- Pausing/resuming queues (also lives in `LimiterManager` via `QueueState.paused`, but documented under [Job Queries & Queue Control](./job-queries-and-control.md)).
- Worker concurrency cap (`WorkerOptions.concurrency`) — that is a plain counter check in the worker poll loop, separate from the limiter classes here. See [Client SDK: Worker](./client-worker-sdk.md).
- Persistence of limit state to SQLite — write-through/recovery lives in [Persistence](./persistence.md) (`queue_state` table).

## Dependencies

Internal:
- `QueueState` / `createQueueState` / `RateLimiter` / `ConcurrencyLimiter` from `src/domain/types/queue.ts` — consumed by `LimiterManager`.
- `Shard` composes one `LimiterManager` per shard and exposes `setRateLimit` / `tryAcquireRateLimit` / `tryAcquireConcurrency` / `releaseConcurrency` through `ShardLimits` (`src/domain/queue/shard/limits.ts:7-35`).
- `QueueManager` routes limit mutations to the owning shard and writes them
  through to SQLite (`src/application/queue-manager/limits.ts`).
- Pull operations gate on the limiters (`src/application/operations/pullStateTransition.ts:104-118`); `releaseJobResources` releases the concurrency slot on terminal transitions (`src/domain/queue/shard/limits.ts:49-60`).
- `RateLimiterOptions` from `src/client/types/worker.ts:3-7` configures the worker-side limiters.

External/runtime:
- Bun only: `Date.now()` for windows, `Bun.env` for protocol-limiter defaults, `setInterval`/`setTimeout`. No external libraries, no SQLite calls inside the limiter classes themselves.

## Public Interface

### Server-side classes

- `class LimiterManager` (`src/domain/queue/limiterManager.ts:10`)
  - `getState(name): QueueState`, `isPaused(name): boolean`, `pause(name)`, `resume(name)`
  - `setRateLimit(queue, limit, durationMs?, ttlMs?): void` — creates `new RateLimiter(limit, limit / ((duration ?? 1000)/1000))`, so the limit means "`limit` per `duration` ms" (default: per second). `ttlMs` stamps `rateLimitExpiresAt = now + ttl` for broker-side auto-expiry. Non-finite / non-positive `duration`/`ttl` degrade to the defaults (1s window, permanent).
  - `clearRateLimit(queue): void` — also nulls `rateLimitDuration`/`rateLimitExpiresAt`
  - `expireRateLimitIfNeeded(queue): void` — lazy TTL check; called from the acquire path and from limit reads, so no timer exists and an expired limit can never throttle a pull
  - `tryAcquireRateLimit(queue): boolean` — `true` if no limiter is set (runs the TTL check first)
  - `getRateLimit(queue): { max; duration } | null`, `getRateLimitTtl(queue, maxJobs?): number`
  - `setConcurrency(queue, limit): void` — reuses/updates an existing `ConcurrencyLimiter`
  - `clearConcurrency(queue): void`
  - `tryAcquireConcurrency(queue): boolean` — `true` if no limiter is set
  - `releaseConcurrency(queue): void`
  - `getConcurrency(queue): number | null`, `isConcurrencyMaxed(queue): boolean`
  - `deleteQueue(queue): void`, `getQueueNames(): string[]`, `getStateMap(): Map<string, QueueState>`
- `class RateLimiter` (`src/domain/types/queue.ts:33-81`) — token bucket. `constructor(capacity, refillRate = capacity)`, `tryAcquire(): boolean`, `getTokens(): number`, `getTtl(maxJobs?)`.
- `class ConcurrencyLimiter` (`src/domain/types/queue.ts:84-119`) — `constructor(limit)`, `tryAcquire(): boolean`, `release(): void`, `getActive()`, `getLimit()`, `setLimit(limit)`.
- `class ProtocolRateLimiter` (`src/infrastructure/server/rateLimiter.ts:70`) — `constructor(config?: Partial<RateLimiterConfig>)`, `isAllowed(clientId): boolean`, `getRemaining(clientId): number`, `removeClient(clientId)`, `stop()`. Module helpers `getRateLimiter(config?)` (lazy singleton) and `stopRateLimiter()`.

### Client-side classes

- `class WorkerRateLimiter` (`src/client/worker/workerRateLimiter.ts:12`) — `constructor(limiter: RateLimiterOptions | null)`, `canProcessWithinLimit(): boolean`, `tryAcquire(): boolean`, `getAvailableSlots(): number`, `recordJobForLimiter(): void`, `getTimeUntilNextSlot(): number`, `getRateLimiterInfo()`, `rateLimit(expireTimeMs): void`, `isRateLimited(): boolean`.
- `class GroupConcurrencyLimiter` (`src/client/worker/groupConcurrency.ts:11`) — `static fromOptions(limiter): GroupConcurrencyLimiter | null`, `canProcess(job): boolean`, `increment(job)`, `decrement(job)`, `getGroupValue(job)`, `getGroupCount(group)`, `getMax()`, `getGroupKey()`, `clear()`.

### Client SDK (Queue) methods (`src/client/queue/rateLimit.ts`, surfaced by `src/client/queue/runtime/configuration.ts`)

- `setGlobalRateLimit(max: number, duration?: number)` — `max` jobs per `duration` ms (default 1000). The window is honored in both embedded and TCP modes; fire-and-forget over TCP.
- `setGlobalRateLimitAsync(max, duration?)` / `removeGlobalRateLimitAsync()` / `setGlobalConcurrencyAsync(n)` / `removeGlobalConcurrencyAsync()` — awaitable variants: resolve once the server has applied the change (no set-then-pull race).
- `removeGlobalRateLimit()`, `setGlobalConcurrency(concurrency)`, `removeGlobalConcurrency()` — fire-and-forget forms.
- `rateLimit(expireTimeMs)` — temporary throttle (`limit: 1` + broker-side `ttl`); throws on non-finite or non-positive input. The expiry lives on the broker, so it works identically embedded/TCP and survives client exit (see Edge Cases).
- `getGlobalRateLimit()` — returns `{ max, duration }` for the active queue limit, or `null`.
- `getGlobalConcurrency()` — returns the configured global cap, or `null`.
- `getRateLimitTtl(maxJobs?)` — returns the remaining temporary-limit TTL or token-bucket wait; `-2` means no rate limit.
- `isMaxed()` — reports whether all configured global-concurrency slots are currently occupied.

All four getters read the live `QueueManager` state in embedded mode and use
`GetQueueLimits` in TCP mode. Reads apply lazy rate-limit expiry before
returning, so an elapsed temporary limit is reported as absent.

### TCP commands (`src/domain/types/commands/limits.ts:27-46`, handlers `src/infrastructure/server/handlers/advanced/queue.ts:69-110`)

- `RateLimit { queue, limit, duration?, ttl? }` — `limit` validated as finite number, else error `"limit must be a finite number"`. `duration` = window ms (default 1000), `ttl` = broker-side auto-expiry ms; invalid values for either degrade to the defaults instead of failing.
- `RateLimitClear { queue }`
- `SetConcurrency { queue, limit }` — same finite-number validation.
- `ClearConcurrency { queue }`
- `GetQueueLimits { queue, maxJobs? }` — returns
  `{ data: { limits: { rateLimit, rateLimitTtl, concurrencyLimit, maxed } } }`.

### HTTP endpoints (`src/infrastructure/server/httpRouteQueueConfig.ts:84`)

- `PUT /queues/:queue/rate-limit` — body `{ limit, duration?, ttl? }` → `RateLimit`
- `DELETE /queues/:queue/rate-limit` → `RateLimitClear`
- `PUT /queues/:queue/concurrency` — body `{ concurrency }` or `{ limit }` → `SetConcurrency`
- `DELETE /queues/:queue/concurrency` → `ClearConcurrency`
- Protocol limiter rejects raw requests with HTTP `429` + `{ ok:false, error:'Rate limit exceeded' }` (`src/infrastructure/server/http.ts:148`).

### CLI (`src/cli/commands/rateLimit.ts`)

- `rate-limit set <queue> <limit>` / `rate-limit clear <queue>`
- `concurrency set <queue> <limit>` / `concurrency clear <queue>`
- `limit` must be a positive number (jobs/sec for rate-limit, max concurrent for concurrency), else `CommandError`.

### Dashboard events emitted

`ratelimit:set`, `ratelimit:cleared`, `concurrency:set`, `concurrency:cleared` (handlers, `handlers/advanced/queue.ts:69-120`); `ratelimit:rejected`, `concurrency:rejected` (pull gate, `pullStateTransition.ts:104-112`); `ratelimit:hit` (protocol limiter, `tcp.ts:67-69`, `http.ts:141-148`).

## Data Models

See [data-model](../data-model.md) for full definitions. Most relevant shapes:

```typescript
// src/domain/types/queue.ts:7 — in-memory per-queue control state
interface QueueState {
  readonly name: string;
  paused: boolean;
  rateLimit: number | null;        // bucket capacity, null = unlimited
  rateLimitDuration: number | null; // configured window ms; null = 1000
  rateLimitExpiresAt: number | null; // temporary-limit deadline; null = permanent
  concurrencyLimit: number | null; // max concurrent, null = unlimited
  activeCount: number;
}

// src/infrastructure/server/rateLimiter.ts:7 — protocol abuse limiter config
interface RateLimiterConfig {
  windowMs: number;        // default 60000
  maxRequests: number;     // default 10000
  cleanupIntervalMs?: number; // default 60000
}

// src/client/types/worker.ts:3 — worker limiter (BullMQ v5 compatible)
interface RateLimiterOptions {
  max: number;       // jobs per window OR per-group cap when groupKey set
  duration: number;  // window in ms
  groupKey?: string; // job-data field; switches max to per-group concurrency
}
```

Persisted in the `queue_state` SQLite table (`src/infrastructure/persistence/schema.ts:147-159`), including `rate_limit`, `rate_limit_duration`, `rate_limit_expires_at`, and `concurrency_limit` alongside pause, stall, and DLQ policy state.

## Business Logic / Control Flow

### Per-queue server limiting (pull path)

`tryPullFromShard` (`src/application/operations/pull.ts`) runs entirely inside the shard write lock:

1. If `state.paused`, return no job.
2. Scan priority-ordered entries, temporarily parking delayed jobs and jobs whose FIFO group is active; expired jobs are removed. This proves an eligible job exists before consuming capacity.
3. `shard.tryAcquireConcurrency(queue)` atomically reserves the active slot. On miss, emit `concurrency:rejected` and return no job.
4. `shard.tryAcquireRateLimit(queue)` consumes the token only after the concurrency check; if it rejects, release the just-acquired concurrency slot because rate-limit tokens have no rollback operation.
5. Pop the selected job and keep its concurrency slot for the entire active lifetime. Restore every parked queue entry in `finally` without touching logical counters or indexes.

Batch pull repeats the same selection and acquires **one rate token + one concurrency slot per delivered job**. The concurrency slot is released only when the job exits the active state — ack, fail-to-DLQ, stall handling, lock expiry, and the claim operations — all routed through `shard.releaseJobResources` → `releaseConcurrency`. Each release also notifies the same queue, so a long-poll waiter blocked by the cap retries immediately; another queue sharing the shard cannot consume that wake-up.

**Token bucket math** (`LimiterManager.setRateLimit`, `src/domain/queue/limiterManager.ts:54-62`; `RateLimiter.refill`, `src/domain/types/queue.ts:55-62`): capacity is `limit`, while refill rate is `limit / (durationMs / 1000)` tokens per second. On each `tryAcquire`, elapsed-time tokens are added up to the capacity and one token is consumed when available. The queue can burst up to `limit` jobs and then sustains exactly `limit` starts per configured window (one second when `duration` is omitted). The bucket is consumed at pull time, not at completion.

### Mutation + persistence

`QueueManager.setRateLimit/setConcurrency` (and their clears) route to the
owning shard's `LimiterManager`, then call `persistQueueState`
(`application/queue-manager/limits.ts`). The persistence helper upserts the row
or deletes it when all queue policy has returned to defaults. Startup recovery
reapplies the state directly to the shard without a write-back loop
(`application/background/recovery/`).

`QueueManager.getQueueLimitStatus(queue, maxJobs?)` is the single read model
for the four public getters. It combines the configured `{max, duration}`, the
remaining TTL/cooldown, the concurrency limit, and current slot saturation
without mutating capacity except for the intentional lazy TTL expiry.

### Protocol-level limiting

Every inbound TCP frame and HTTP request first calls `getRateLimiter().isAllowed(clientId)` (`src/infrastructure/server/tcp.ts:34-103`, `src/infrastructure/server/http.ts:141-149`). `clientId` is the socket-derived id (TCP) or `x-forwarded-for`/`x-real-ip`/`'unknown'` (HTTP). `isAllowed` reads the per-client `SlidingWindowDeque` count and rejects when `count >= maxRequests` within `windowMs`; otherwise it records the timestamp (`src/infrastructure/server/rateLimiter.ts:81-100`). On disconnect, `removeClient` drops the client's deque (`src/infrastructure/server/tcp/connections.ts:87-97`, `src/infrastructure/server/http.ts:237-240`). The singleton is torn down via `stopRateLimiter()` on shutdown (`src/infrastructure/server/bootstrap.ts:234-237`).

### Worker-side rate limiting

In the worker state constructor (`src/client/worker/runtime/state.ts`):
```typescript
this.rateLimiter = new WorkerRateLimiter(opts.limiter?.groupKey ? null : (opts.limiter ?? null));
this.groupLimiter = GroupConcurrencyLimiter.fromOptions(opts.limiter);
```
Key invariant: **if `limiter.groupKey` is set, the `WorkerRateLimiter` is disabled** and `limiter.max` instead becomes a per-group concurrency cap. A single `limiter` option is therefore *either* a global rate limit *or* a per-group concurrency limit, never both.

- `poll()` and `tryProcess()` (`worker/runtime/polling.ts`) check availability
  before pulling. `doPullBatch()` also caps the requested batch by
  `getAvailableSlots() - pendingPull`, so concurrent pulls cannot lease more
  work than the current budget.
- `startJob()` performs the authoritative synchronous `tryAcquire()` immediately
  before dispatch. The token records a **job start** and is never consumed at
  completion; success, failure, retry attempts and long-running jobs therefore
  obey the same rolling start-time window.
- `processJobManually()` uses the same admission boundary after checking worker
  concurrency, duplicate job ids and group capacity. A waiting manual lease is
  returned to the broker if the worker closes before admission.
- `getTimeUntilNextSlot()` returns the longer of the rolling-window wait and a
  temporary override wait, while retaining O(1) access to the oldest live token.
- `Worker.rateLimit(expireTimeMs)` sets that independent temporary override. It
  works without a configured rolling limiter and in `groupKey` mode, and does
  not inject synthetic timestamps into the rolling window.

### Worker-side group concurrency

`GroupConcurrencyLimiter` (`src/client/worker/groupConcurrency.ts`) tracks `activeByGroup: Map<groupValue, count>`:
- `getGroupValue(job)` reads `job.data[groupKey]`; `null`/`undefined`/missing → `null` (not subject to the limit); non-strings are stringified.
- `canProcess(job)` → `current < maxPerGroup` (jobs without a group always pass).
- `getNextEligibleJob()` (`worker/runtime/buffer.ts`) scans for the first group
  with capacity. `startJob()` in `runtime/execution.ts` owns the exactly-once
  increment/decrement pair.
- **Group pull-ahead exception** (`worker/runtime/polling.ts`): when the buffer
  is non-empty but every buffered group is blocked, the leased count uses
  `activeJobs` so the worker may discover another runnable group.

## Concurrency & Locking

- Server per-queue rate/concurrency checks (`tryAcquireRateLimit`, `tryAcquireConcurrency`, `releaseConcurrency`) run **inside the shard write lock** held during pull (`pull.ts:255`), so check-and-acquire is atomic with respect to other pullers on the same shard. There is no separate limiter lock; the limiter state lives within the shard's lock domain. See the lock hierarchy in [Concurrency & Locking](./concurrency-and-locking.md).
- `LimiterManager` and its `RateLimiter`/`ConcurrencyLimiter` are single-threaded JS objects with no internal locking; safety comes entirely from the surrounding shard lock.
- The acquire-at-pull / release-at-terminal pattern means a slot is held for the full active lifetime of a job. Any exit path that forgets to call `releaseJobResources` would leak a concurrency slot; all known paths (ack, fail, stall, lock-expiry, cancel, obliterate-time release, and the management claim ops `moveActiveToWait`/`moveToWaitingChildren`/`moveJobToDelayed`/`discardJob`) route through it (repro: `test/repro-slot-release-claim-paths.test.ts`).
- `ProtocolRateLimiter`, `WorkerRateLimiter`, and `GroupConcurrencyLimiter` are not lock-protected; each is owned by a single connection/worker context.

## Edge Cases & Failure Modes

- **Default = unlimited per queue.** No `RateLimiter`/`ConcurrencyLimiter` exists until explicitly set; `tryAcquire*` returns `true` when absent (`limiterManager.ts:63`, `:90`). The only always-on throttle is the protocol limiter at 10000 req/60s per client (the known "rate limit defaults to Infinity" caveat refers to per-queue limits being off by default).
- **`setGlobalRateLimit(max, duration)` honors `duration` end-to-end** (client → wire `duration` field → `LimiterManager` refill rate), matching BullMQ's `{max per duration}` semantics in both modes. Servers older than 2.8.35 ignore the field and fall back to the 1s bucket. The worker-side `WorkerRateLimiter` independently honors its own `{max, duration}`.
- The official TypeScript and Python network SDKs forward the same custom
  window as wire `duration`; omitting it preserves the broker's 1,000 ms
  default. Real-broker regressions read the applied window through
  `GetQueueLimits` so a silently dropped field cannot pass.
- **`Queue.rateLimit(expireTimeMs)` expires broker-side** (`rateLimit.ts`): both modes set `limit: 1` with a broker-side `ttl`; there is no client timer, so the expiry survives client exit and behaves identically embedded/TCP. Lazy expiry: the limit clears on the first pull or limit read past the deadline. During the window jobs still trickle at 1/sec (token refill), matching the previous approximation. Invalid `expireTimeMs` (non-finite or ≤ 0) throws. A TTL'd limit persisted to `queue_state` is restored with its remaining time on restart and never resurrects once expired.
- **TTL sentinel:** `getRateLimitTtl` returns `-2` when no rate limit exists. A temporary limit returns its remaining broker-side lifetime; a permanent token bucket returns the wait required for the requested token count.
- **`isMaxed` scope:** it reflects the global concurrency limiter, not worker-local concurrency or rate-token availability. With no configured global concurrency it is `false`.
- **Memory bounds.**
  - `SlidingWindowDeque` advances a head pointer for O(1) amortized expiry and compacts the array when `head > 1000` (`rateLimiter.ts:36`); the cleanup interval deletes empty per-client deques every `cleanupIntervalMs` (`rateLimiter.ts:130`). The maintenance timer is unreferenced: it continues while TCP/HTTP work keeps the process alive but cannot prevent a stopped broker from exiting. Request-time lazy expiry remains authoritative, so timer scheduling never changes admission correctness.
  - `WorkerRateLimiter.evictExpired` advances head and compacts when more than half the token array is dead space (`workerRateLimiter.ts:108`).
  - `GroupConcurrencyLimiter.decrement` deletes a group's map entry once its count hits 0, so idle groups don't accumulate (`groupConcurrency.ts:74`).
- **Negative/non-positive limits:** CLI rejects `limit <= 0`; TCP handlers only require a finite number, so `RateLimit limit:0` would create a token bucket that never refills enough to grant a token (effectively blocks the queue). `ConcurrencyLimiter` with limit `0` blocks all pulls.
- **Batch pull partial fill:** if rate or concurrency runs out mid-batch, `tryDequeueNextJob` returns `stop`; the batch loop breaks and returns the jobs gathered so far (`pullStateTransition.ts:104-112`, `pull.ts:268-279`) — never an error.
- **Slot release on requeue:** a failed processing handoff triggers `requeueJob`, which releases the concurrency slot and group slot before pushing the job back, preventing a permanent slot leak on transient pull failures (`pullStateTransition.ts:139-169`).
- **Worker rate tokens represent starts:** admission consumes exactly one token
  before the processor is invoked. Completion does not release or add a token;
  capacity returns only when the recorded start leaves the rolling window.

## Configuration

Protocol-level limiter (`src/infrastructure/server/rateLimiter.ts:13`):

| Env var                   | Default | Meaning                                   |
| ------------------------- | ------- | ----------------------------------------- |
| `RATE_LIMIT_WINDOW_MS`    | 60000   | Sliding window size per client            |
| `RATE_LIMIT_MAX_REQUESTS` | 10000   | Max raw requests per client per window    |
| `RATE_LIMIT_CLEANUP_MS`   | 60000   | Interval to evict idle client deques      |

Per-queue limits are runtime-set (TCP/HTTP/CLI/SDK), persisted in `queue_state`, and have no env defaults (unset = unlimited).

Worker limits are set via `WorkerOptions.limiter: { max, duration, groupKey? }` (`src/client/types/worker.ts:9-28`). `WorkerOptions.concurrency` (default 1) is a separate in-worker counter, not part of these limiter classes.

## Related Docs

- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md)
- [Concurrency & Locking](./concurrency-and-locking.md)
- [Core Queue Engine (QueueManager & Shards)](./core-queue-engine.md)
- [Job Queries & Queue Control](./job-queries-and-control.md)
- [Client SDK: Worker (& sandboxed)](./client-worker-sdk.md)
- [Client SDK: Queue](./client-queue-sdk.md)
- [Persistence (SQLite, WriteBuffer, ReadThrough)](./persistence.md)
- [TCP Server Command Handlers](./tcp-server-handlers.md)
- [HTTP / REST / SSE / WebSocket API](./http-api.md)
- [CLI](./cli.md)
- [architecture](../architecture.md)
- [data-model](../data-model.md)
