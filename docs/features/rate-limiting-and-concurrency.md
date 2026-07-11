# Rate Limiting & Concurrency Control

> **Category:** Control · **Source:** `src/domain/queue/limiterManager.ts`, `src/domain/types/queue.ts`, `src/infrastructure/server/rateLimiter.ts`, `src/client/worker/workerRateLimiter.ts`, `src/client/queue/rateLimit.ts`, `src/client/worker/groupConcurrency.ts`

## Purpose

bunqueue throttles work at three independent layers: (1) **per-queue rate + concurrency limits** enforced server-side at pull time (`LimiterManager` + token-bucket `RateLimiter` + `ConcurrencyLimiter`), (2) a **protocol-level abuse limiter** that caps raw TCP/HTTP requests per client IP (`ProtocolRateLimiter`), and (3) **worker-side limiters** in the client SDK that gate how fast a single `Worker` starts jobs (`WorkerRateLimiter`, BullMQ v5 `{max, duration}`) and how many jobs run concurrently per group key (`GroupConcurrencyLimiter`). These layers are orthogonal — a queue can have a server rate limit while a worker independently applies its own limiter, and both fire on top of the always-on protocol limiter.

## Responsibilities & Scope

Owns:
- Per-queue rate limiting (token bucket, jobs/sec) and concurrency capping, stored per-shard in `LimiterManager` (`src/domain/queue/limiterManager.ts:10`).
- Token-bucket refill math and concurrency slot accounting (`src/domain/types/queue.ts:27`, `:66`).
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
- `Shard` wraps one `LimiterManager` per shard and exposes `setRateLimit` / `tryAcquireRateLimit` / `tryAcquireConcurrency` / `releaseConcurrency` (`src/domain/queue/shard.ts:52`, `:178`–`:203`).
- `QueueManager` routes limit mutations to the owning shard and write-throughs to SQLite (`src/application/queueManager.ts:1020`–`1056`).
- Pull operations gate on the limiters (`src/application/operations/pull.ts:263`, `:360`); `releaseJobResources` releases the concurrency slot on terminal transitions (`src/domain/queue/shard.ts:216`).
- `RateLimiterOptions` from `src/client/types.ts:471` configures the worker-side limiters.

External/runtime:
- Bun only: `Date.now()` for windows, `Bun.env` for protocol-limiter defaults, `setInterval`/`setTimeout`. No external libraries, no SQLite calls inside the limiter classes themselves.

## Public Interface

### Server-side classes

- `class LimiterManager` (`src/domain/queue/limiterManager.ts:10`)
  - `getState(name): QueueState`, `isPaused(name): boolean`, `pause(name)`, `resume(name)`
  - `setRateLimit(queue, limit: number): void` — creates `new RateLimiter(limit)`
  - `clearRateLimit(queue): void`
  - `tryAcquireRateLimit(queue): boolean` — `true` if no limiter is set
  - `setConcurrency(queue, limit): void` — reuses/updates an existing `ConcurrencyLimiter`
  - `clearConcurrency(queue): void`
  - `tryAcquireConcurrency(queue): boolean` — `true` if no limiter is set
  - `releaseConcurrency(queue): void`
  - `deleteQueue(queue): void`, `getQueueNames(): string[]`, `getStateMap(): Map<string, QueueState>`
- `class RateLimiter` (`src/domain/types/queue.ts:27`) — token bucket. `constructor(capacity, refillRate = capacity)`, `tryAcquire(): boolean`, `getTokens(): number`.
- `class ConcurrencyLimiter` (`src/domain/types/queue.ts:66`) — `constructor(limit)`, `tryAcquire(): boolean`, `release(): void`, `getActive()`, `getLimit()`, `setLimit(limit)`.
- `class ProtocolRateLimiter` (`src/infrastructure/server/rateLimiter.ts:70`) — `constructor(config?: Partial<RateLimiterConfig>)`, `isAllowed(clientId): boolean`, `getRemaining(clientId): number`, `removeClient(clientId)`, `stop()`. Module helpers `getRateLimiter(config?)` (lazy singleton) and `stopRateLimiter()`.

### Client-side classes

- `class WorkerRateLimiter` (`src/client/worker/workerRateLimiter.ts:12`) — `constructor(limiter: RateLimiterOptions | null)`, `canProcessWithinLimit(): boolean`, `recordJobForLimiter(): void`, `getTimeUntilNextSlot(): number`, `getRateLimiterInfo()`, `rateLimit(expireTimeMs): void`, `isRateLimited(): boolean`.
- `class GroupConcurrencyLimiter` (`src/client/worker/groupConcurrency.ts:11`) — `static fromOptions(limiter): GroupConcurrencyLimiter | null`, `canProcess(job): boolean`, `increment(job)`, `decrement(job)`, `getGroupValue(job)`, `getGroupCount(group)`, `getMax()`, `getGroupKey()`, `clear()`.

### Client SDK (Queue) methods (`src/client/queue/rateLimit.ts`, surfaced in `src/client/queue/queue.ts:438`)

- `setGlobalRateLimit(max: number, duration?: number)` — NOTE: `duration` is **ignored** (`src/client/queue/rateLimit.ts:38`); `max` is sent as the token-bucket capacity (jobs/sec).
- `removeGlobalRateLimit()`, `setGlobalConcurrency(concurrency)`, `removeGlobalConcurrency()`
- `rateLimit(expireTimeMs)` — temporary throttle (see Edge Cases).
- `getGlobalRateLimit()`, `getGlobalConcurrency()`, `getRateLimitTtl()`, `isMaxed()` — all are **stubs** that resolve to `null`/`0`/`false` (`src/client/queue/rateLimit.ts:33`, `:56`, `:75`, `:80`).

### TCP commands (`src/domain/types/command.ts:295`, handlers `src/infrastructure/server/handlers/advanced.ts:239`)

- `RateLimit { queue, limit }` — `limit` validated as finite number, else error `"limit must be a finite number"`.
- `RateLimitClear { queue }`
- `SetConcurrency { queue, limit }` — same finite-number validation.
- `ClearConcurrency { queue }`

### HTTP endpoints (`src/infrastructure/server/httpRouteQueueConfig.ts:84`)

- `PUT /queues/:queue/rate-limit` — body `{ limit }` → `RateLimit`
- `DELETE /queues/:queue/rate-limit` → `RateLimitClear`
- `PUT /queues/:queue/concurrency` — body `{ concurrency }` or `{ limit }` → `SetConcurrency`
- `DELETE /queues/:queue/concurrency` → `ClearConcurrency`
- Protocol limiter rejects raw requests with HTTP `429` + `{ ok:false, error:'Rate limit exceeded' }` (`src/infrastructure/server/http.ts:148`).

### CLI (`src/cli/commands/rateLimit.ts`)

- `rate-limit set <queue> <limit>` / `rate-limit clear <queue>`
- `concurrency set <queue> <limit>` / `concurrency clear <queue>`
- `limit` must be a positive number (jobs/sec for rate-limit, max concurrent for concurrency), else `CommandError`.

### Dashboard events emitted

`ratelimit:set`, `ratelimit:cleared`, `concurrency:set`, `concurrency:cleared` (handlers, `advanced.ts:247`–`285`); `ratelimit:rejected`, `concurrency:rejected` (pull gate, `pull.ts:264`, `:269`); `ratelimit:hit` (protocol limiter, `tcp.ts:219`, `http.ts:149`).

## Data Models

See [data-model](../data-model.md) for full definitions. Most relevant shapes:

```typescript
// src/domain/types/queue.ts:7 — in-memory per-queue control state
interface QueueState {
  readonly name: string;
  paused: boolean;
  rateLimit: number | null;        // tokens/sec capacity, null = unlimited
  concurrencyLimit: number | null; // max concurrent, null = unlimited
  activeCount: number;
}

// src/infrastructure/server/rateLimiter.ts:7 — protocol abuse limiter config
interface RateLimiterConfig {
  windowMs: number;        // default 60000
  maxRequests: number;     // default 10000
  cleanupIntervalMs?: number; // default 60000
}

// src/client/types.ts:471 — worker limiter (BullMQ v5 compatible)
interface RateLimiterOptions {
  max: number;       // jobs per window OR per-group cap when groupKey set
  duration: number;  // window in ms
  groupKey?: string; // job-data field; switches max to per-group concurrency
}
```

Persisted in the `queue_state` SQLite table (`src/infrastructure/persistence/schema.ts:123`): `name PRIMARY KEY, paused, rate_limit, concurrency_limit`.

## Business Logic / Control Flow

### Per-queue server limiting (pull path)

`tryPullFromShard` (`src/application/operations/pull.ts:254`) runs entirely inside the shard write lock:

1. If `state.paused` → return `null` (no job). (`pull.ts:259`)
2. `shard.tryAcquireRateLimit(queue)` — token bucket. On miss, emit `ratelimit:rejected` and return `null` so the puller waits. (`pull.ts:263`)
3. `shard.tryAcquireConcurrency(queue)` — atomically increments the active slot count. On miss, emit `concurrency:rejected` and return `null`. (`pull.ts:268`)
4. Loop `tryDequeueNextJob`: on `'job'` keep the acquired concurrency slot and return the job; on `'stop'` call `releaseConcurrency` (no job taken) and return `null`; on `'skip'` continue the loop reusing the already-acquired slot. (`pull.ts:273-284`)

Batch pull (`tryPullBatchFromShard`, `pull.ts:343`) acquires **one rate token + one concurrency slot per job** in the loop and releases the slot when a dequeue yields `'stop'`/`'skip'` (`pull.ts:358-375`). The concurrency slot taken at pull is released only on a terminal event — ack, fail-to-DLQ, stall handling, lock expiry, cancel — all routed through `shard.releaseJobResources` → `releaseConcurrency` (`src/domain/queue/shard.ts:216`). A requeue (push-back after a failed `moveToProcessing`) also releases the slot before re-pushing (`pull.ts:186`).

**Token bucket math** (`RateLimiter.refill`, `src/domain/types/queue.ts:50`): capacity = refillRate = `limit`. On each `tryAcquire`, tokens are topped up by `elapsedSeconds * limit` (capped at `limit`), then one token is consumed if `tokens >= 1`. So `limit` is **jobs/second**, allowing a burst up to `limit` then a sustained `limit`/sec. The bucket is consumed at pull time, not at completion.

### Mutation + persistence

`QueueManager.setRateLimit/setConcurrency` (and their clears) route to the owning shard's `LimiterManager` then call `persistQueueState` (`src/application/queueManager.ts:1020`–`1056`), which UPSERTs the row, or DELETEs it when the state has returned fully to default (not paused, no limits). On startup, `loadQueueState()` re-applies paused/rate/concurrency directly to the shard, in-memory only, avoiding a write-back loop (`src/application/backgroundTasks.ts:390`–`396`). This is the issue #100 fix; without it every queue silently un-paused and lost its limits on restart.

### Protocol-level limiting

Every inbound TCP `data` callback and HTTP request first calls `getRateLimiter().isAllowed(clientId)` (`src/infrastructure/server/tcp.ts:218`, `src/infrastructure/server/http.ts:148`). `clientId` is the socket-derived id (TCP) or `x-forwarded-for`/`x-real-ip`/`'unknown'` (HTTP). `isAllowed` reads the per-client `SlidingWindowDeque` count and rejects when `count >= maxRequests` within `windowMs`; otherwise it records the timestamp (`src/infrastructure/server/rateLimiter.ts:81`). On disconnect, `removeClient` drops the client's deque (`tcp.ts:313`, `http.ts:235`). The singleton is torn down via `stopRateLimiter()` on shutdown (`src/infrastructure/server/bootstrap.ts:189`).

### Worker-side rate limiting

In the worker constructor (`src/client/worker/worker.ts:180`):
```typescript
this.rateLimiter = new WorkerRateLimiter(opts.limiter?.groupKey ? null : (opts.limiter ?? null));
this.groupLimiter = GroupConcurrencyLimiter.fromOptions(opts.limiter);
```
Key invariant: **if `limiter.groupKey` is set, the `WorkerRateLimiter` is disabled** and `limiter.max` instead becomes a per-group concurrency cap. A single `limiter` option is therefore *either* a global rate limit *or* a per-group concurrency limit, never both.

- `poll()` (`worker.ts:658`) calls `canProcessWithinLimit()` before processing; if blocked it reschedules itself after `max(getTimeUntilNextSlot(), 10)` ms.
- A token is pushed on **job completion** via `recordJobForLimiter()` (`worker.ts:486`, `:910`). So the window measures completed throughput; `canProcessWithinLimit` returns `activeCount() < limiter.max` (`workerRateLimiter.ts:24`).
- `getTimeUntilNextSlot` returns `oldestToken + duration - now`, reading the oldest live token at the head pointer (O(1), `workerRateLimiter.ts:44`).
- `Worker.rateLimit(expireTimeMs)` (BullMQ v5 manual throttle) pushes `max` synthetic tokens timed to expire at `now + expireTimeMs` and sets `rateLimitExpiration`; `isRateLimited()` is `Date.now() < rateLimitExpiration` (`workerRateLimiter.ts:78`, `:92`).

### Worker-side group concurrency

`GroupConcurrencyLimiter` (`src/client/worker/groupConcurrency.ts`) tracks `activeByGroup: Map<groupValue, count>`:
- `getGroupValue(job)` reads `job.data[groupKey]`; `null`/`undefined`/missing → `null` (not subject to the limit); non-strings are stringified.
- `canProcess(job)` → `current < maxPerGroup` (jobs without a group always pass).
- The worker's `getNextEligibleJob()` (`worker.ts:787`) scans the pending buffer for the first job whose group has capacity, leaving group-blocked jobs buffered. `increment` is called in `startJob` (`worker.ts:876`), `decrement` in the `finally` after processing (`worker.ts:908`).
- **Group pull-ahead exception** (`worker.ts:824-838`): when a group limiter is set and the buffer is non-empty but unrunnable (all buffered jobs group-blocked), the leased count uses `activeJobs` instead of `pulledJobIds.size`, so the worker pulls ahead to discover jobs from other runnable groups instead of wedging.

## Concurrency & Locking

- Server per-queue rate/concurrency checks (`tryAcquireRateLimit`, `tryAcquireConcurrency`, `releaseConcurrency`) run **inside the shard write lock** held during pull (`pull.ts:255`), so check-and-acquire is atomic with respect to other pullers on the same shard. There is no separate limiter lock; the limiter state lives within the shard's lock domain. See the lock hierarchy in [Concurrency & Locking](./concurrency-and-locking.md).
- `LimiterManager` and its `RateLimiter`/`ConcurrencyLimiter` are single-threaded JS objects with no internal locking; safety comes entirely from the surrounding shard lock.
- The acquire-at-pull / release-at-terminal pattern means a slot is held for the full active lifetime of a job. Any terminal path that forgets to call `releaseJobResources` would leak a concurrency slot; all known paths (ack, fail, stall, lock-expiry, cancel, obliterate-time release) route through it.
- `ProtocolRateLimiter`, `WorkerRateLimiter`, and `GroupConcurrencyLimiter` are not lock-protected; each is owned by a single connection/worker context.

## Edge Cases & Failure Modes

- **Default = unlimited per queue.** No `RateLimiter`/`ConcurrencyLimiter` exists until explicitly set; `tryAcquire*` returns `true` when absent (`limiterManager.ts:63`, `:90`). The only always-on throttle is the protocol limiter at 10000 req/60s per client (the known "rate limit defaults to Infinity" caveat refers to per-queue limits being off by default).
- **`setGlobalRateLimit(max, duration)` drops `duration`** (`rateLimit.ts:38`) and the server reinterprets `max` as a token-bucket capacity/refill (jobs/sec) — semantically different from BullMQ's `{max per duration}`. The worker-side `WorkerRateLimiter` is the one that honors `{max, duration}`.
- **`Queue.rateLimit(expireTimeMs)` asymmetry** (`rateLimit.ts:63`): in embedded mode it sets the queue limit to `1` then auto-clears via `setTimeout(expireTimeMs)`; in TCP mode it sends `RateLimit limit:1` but **never schedules a clear** — the throttle stays at 1 job/sec until manually cleared. Treat TCP-mode temporary rate limit as sticky.
- **Stub getters:** `getGlobalRateLimit`, `getGlobalConcurrency`, `getRateLimitTtl`, `isMaxed` always return `null`/`0`/`false`; do not rely on them to read back limits.
- **Memory bounds.**
  - `SlidingWindowDeque` advances a head pointer for O(1) amortized expiry and compacts the array when `head > 1000` (`rateLimiter.ts:36`); the cleanup interval deletes empty per-client deques every `cleanupIntervalMs` (`rateLimiter.ts:130`).
  - `WorkerRateLimiter.evictExpired` advances head and compacts when more than half the token array is dead space (`workerRateLimiter.ts:108`).
  - `GroupConcurrencyLimiter.decrement` deletes a group's map entry once its count hits 0, so idle groups don't accumulate (`groupConcurrency.ts:74`).
- **Negative/non-positive limits:** CLI rejects `limit <= 0`; TCP handlers only require a finite number, so `RateLimit limit:0` would create a token bucket that never refills enough to grant a token (effectively blocks the queue). `ConcurrencyLimiter` with limit `0` blocks all pulls.
- **Batch pull partial fill:** if rate or concurrency runs out mid-batch the loop simply `break`s and returns the jobs gathered so far (`pull.ts:360`, `:363`) — never an error.
- **Slot release on requeue:** a failed `moveToProcessing` triggers `requeueJob`, which releases the concurrency slot and group slot before pushing the job back, preventing a permanent slot leak on transient pull failures (`pull.ts:185-192`).
- **Worker rate token recorded on completion, not start:** a long-running job does not consume a rate token until it finishes, so the window reflects completion throughput; combined with the concurrency cap this bounds true in-flight work.

## Configuration

Protocol-level limiter (`src/infrastructure/server/rateLimiter.ts:13`):

| Env var                   | Default | Meaning                                   |
| ------------------------- | ------- | ----------------------------------------- |
| `RATE_LIMIT_WINDOW_MS`    | 60000   | Sliding window size per client            |
| `RATE_LIMIT_MAX_REQUESTS` | 10000   | Max raw requests per client per window    |
| `RATE_LIMIT_CLEANUP_MS`   | 60000   | Interval to evict idle client deques      |

Per-queue limits are runtime-set (TCP/HTTP/CLI/SDK), persisted in `queue_state`, and have no env defaults (unset = unlimited).

Worker limits are set via `WorkerOptions.limiter: { max, duration, groupKey? }` (`src/client/types.ts:507`). `WorkerOptions.concurrency` (default 1) is a separate in-worker counter, not part of these limiter classes.

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
