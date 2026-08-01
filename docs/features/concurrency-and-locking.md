# Concurrency & Locking

> **Category:** Engine · **Source:** `src/shared/lock.ts`, `src/shared/semaphore.ts`, `src/application/lockManager.ts`, `src/application/lockOperations.ts`, `src/application/stallDetection.ts`, `src/domain/types/stall.ts`

## Purpose

This module provides the in-process synchronization primitives and the job-ownership machinery that keep bunqueue's sharded state consistent under concurrent access. It exposes two low-level primitives — an `RWLock` (used per-shard) and a `Semaphore` (used to bound per-connection command pipelining) — plus BullMQ-style job leasing (`JobLock` token + TTL) and stall detection (heartbeat timeout → retry/DLQ). It exists because the QueueManager mutates sharded in-memory structures (`shards[]`, `processingShards[]`) from many concurrent TCP commands and background timers, and because a worker that crashes mid-job must have its lease reclaimed without losing or double-running the job.

## Responsibilities & Scope

Owns:

- **Locking primitives** — `AsyncLock` (FIFO mutex), `RWLock` (multi-reader/single-writer, writer-priority), and the `withLock`/`withReadLock`/`withWriteLock` helpers (`src/shared/lock.ts`).
- **Concurrency limiter** — `Semaphore` + `withSemaphore` (`src/shared/semaphore.ts`).
- **Job leasing** — create/verify/renew/release of per-job `JobLock` tokens (`src/application/lockOperations.ts`).
- **Lock-expiration sweep** — `checkExpiredLocks`: reclaims jobs whose lease TTL elapsed, requeuing or moving to DLQ (`src/application/lockManager.ts`).
- **Stall detection** — two-phase heartbeat-timeout detection and recovery (`src/application/stallDetection.ts`, `src/domain/types/stall.ts`).

Does NOT own:

- The shard data structures, the lock *hierarchy* discipline at call sites, or `releaseJobResources` (concurrency-slot / group / unique-key release) — see [Core Queue Engine](./core-queue-engine.md) and [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md).
- The actual `pull`/`ack`/`fail` state transitions that *call* these lock APIs — see [Job Lifecycle](./job-lifecycle.md).
- Client-side `useLocks` / `heartbeatInterval` worker behavior — see [Client SDK: Worker](./client-worker-sdk.md).
- Background-task scheduling (the timers that drive `checkExpiredLocks`/`checkStalledJobs`) — see [Background Tasks](./background-tasks.md).

## Dependencies

Internal:

- `src/domain/types/job.ts` — `JobLock`, `createJobLock`, `isLockExpired`, `renewLock`, `DEFAULT_LOCK_TTL`, `LockToken`, `calculateBackoff`.
- `src/domain/types/stall.ts` — `StallConfig`, `StallAction`, `getStallAction`, `incrementStallCount`.
- `src/domain/types/dlq.ts` — `FailureReason`.
- `src/shared/hash.ts` — `shardIndex`, `processingShardIndex`, `SHARD_COUNT` (route a job to its shard / processing shard).
- `src/application/types/contexts.ts` — `LockContext`, `BackgroundContext` (the state bags these functions operate on).
- `src/shared/logger.ts` — `queueLog`.

External / runtime:

- Bun only: `Bun.env.LOCK_TIMEOUT_MS` (lock.ts:8), `Bun.randomUUIDv7()` for lease tokens (job.ts:491). No external libraries; timers via `setTimeout`/`setInterval`. SQLite is touched indirectly via `ctx.storage` for DLQ/delete persistence inside recovery paths.

## Public Interface

### `src/shared/lock.ts`

```typescript
export interface LockGuard { release(): void; }
export class LockTimeoutError extends Error {}

export class AsyncLock {
  acquire(timeoutMs?: number): Promise<LockGuard>; // FIFO mutex, default LOCK_TIMEOUT_MS
  isLocked(): boolean;
  getQueueLength(): number;
}

export class RWLock {
  acquireRead(timeoutMs?: number): Promise<LockGuard>;
  acquireWrite(timeoutMs?: number): Promise<LockGuard>; // sync fast path when uncontested
  getState(): { readers: number; writer: boolean; writerWaiting: number };
}

export function withLock<T>(lock: AsyncLock, fn: () => T | Promise<T>, timeoutMs?: number): Promise<T>;
export function withReadLock<T>(lock: RWLock, fn, timeoutMs?): Promise<T>;
export function withWriteLock<T>(lock: RWLock, fn, timeoutMs?): Promise<T>;
```

> Note: `RWLock` is the primitive actually used for per-shard locks (`shardLocks[]`, `processingLocks[]` are `RWLock[]`, instantiated by `queue-manager/state.ts`). `AsyncLock`/`withLock` are exported but not used by the shard machinery.

### `src/shared/semaphore.ts`

```typescript
export class Semaphore {
  constructor(maxPermits: number);
  acquire(): Promise<void>;
  tryAcquire(): boolean;
  release(): void;
  available(): number;
  waiting(): number;
}
export function withSemaphore<T>(semaphore: Semaphore, fn: () => Promise<T>): Promise<T>;
```

### `src/application/lockOperations.ts`

```typescript
export function createLock(jobId: JobId, owner: string, ctx: LockContext, ttl?: number): LockToken | null;
export function verifyLock(jobId: JobId, token: string, ctx: LockContext): boolean;
export function renewJobLock(jobId: JobId, token: string, ctx: LockContext, newTtl?: number): boolean;
export function renewJobLockBatch(items: Array<{ id: JobId; token: string; ttl?: number }>, ctx: LockContext): string[];
export function releaseLock(jobId: JobId, ctx: LockContext, token?: string): boolean;
export function getLockInfo(jobId: JobId, ctx: LockContext): JobLock | null;
```

### `src/application/lockManager.ts`

```typescript
export async function checkExpiredLocks(ctx: LockContext): Promise<void>;
// + re-exports of the lockOperations.ts functions and clientTracking.ts helpers
```

### `src/application/stallDetection.ts`

```typescript
export function checkStalledJobs(ctx: BackgroundContext): void;
```

### TCP commands that reach this module

Lease renewal / heartbeat flow through the TCP handlers (`src/infrastructure/server/handlers/monitoring.ts`) into `QueueManager`:

- `JobHeartbeat` / `JobHeartbeatBatch` → `renewJobLock` when a `token` is present, else updates `job.lastHeartbeat` (`queue-manager/locks.ts`).
- `ExtendLock` / `ExtendLocks` → `extendLock` → `renewJobLock`.
- `Heartbeat` → worker-level liveness (worker registry, not job leases).

Leases are created implicitly by `PULL`/`PULLB` via `pullWithLock`/`pullBatchWithLock` (`queue-manager/delivery.ts`) and released by `ACK`/`FAIL` (`queue-manager/ack.ts`). See [TCP Server Command Handlers](./tcp-server-handlers.md).

### Events emitted

Via `ctx.eventsManager.broadcast` and `ctx.dashboardEmit`:

- `Stalled` (`EventType.Stalled`) — on lock-expiry requeue (lockManager.ts:221) and on stall retry/DLQ (stallDetection.ts:118).
- `Failed` (`EventType.Failed`) — on lock-expiry max-stalls → DLQ (lockManager.ts:187).
- Dashboard events: `job:lock-expired` (lockManager.ts:149), `job:stalled` (stallDetection.ts:112).
- Webhook: `stalled` (stallDetection.ts:125).

## Data Models

Full type definitions live in the source files cited below. Most relevant here:

**`JobLock`** (`src/domain/types/job.ts:495`):

```typescript
interface JobLock {
  readonly jobId: JobId;
  readonly token: LockToken;     // Bun.randomUUIDv7()
  readonly owner: string;        // worker/client id
  readonly createdAt: number;    // load-bearing for the #101 re-lease guard
  expiresAt: number;
  lastRenewalAt: number;
  renewalCount: number;
  readonly ttl: number;          // DEFAULT_LOCK_TTL = 30_000 ms
}
```

`isLockExpired` is **inclusive**: `now >= lock.expiresAt` (job.ts:530). `renewLock` sets `expiresAt = now + ttl`, bumps `lastRenewalAt` and `renewalCount` (job.ts:534).

**`StallConfig`** (`src/domain/types/stall.ts:9`, defaults at stall.ts:21):

```typescript
interface StallConfig {
  enabled: boolean;       // default true
  stallInterval: number;  // default 30_000 ms (no-heartbeat timeout)
  maxStalls: number;      // default 3 (then → DLQ)
  gracePeriod: number;    // default 5_000 ms after start before checking
}
```

**`StallAction`** (stall.ts:92): `Retry` | `MoveToDlq` | `Keep`.

Lease state lives in `ctx.jobLocks: Map<JobId, JobLock>` and stall candidates live in `ctx.stalledCandidates: Set<JobId>` (`application/types/contexts.ts`, with storage and collection ownership in `queue-manager/state.ts`).

## Business Logic / Control Flow

### Primitives

**`AsyncLock`** is a FIFO mutex. `acquire` loops while `locked`, parking each waiter as a `QueueEntry { resolve, cancelled }` (lock.ts:42). Timeouts mark the entry `cancelled` and resolve in O(1) (lock.ts:54) instead of `indexOf`+`splice`; `release` skips cancelled entries at the head (lock.ts:81). `release` is **idempotent** via a `released` flag (lock.ts:77) so a double-release cannot clobber the next owner's `locked` flag.

**`RWLock`** allows many readers or one writer, with **writer priority**: `acquireRead` blocks while `this.writer || this.writerWaiting > 0` (lock.ts:125), preventing writer starvation. `acquireWrite` has a synchronous fast path when uncontested (`!writer && readers === 0`, lock.ts:179). On write release it wakes a waiting writer first, then drains all waiting readers (lock.ts:224–252). All guards carry an idempotent `released` flag.

**`Semaphore`** bounds concurrency. `acquire` consumes a permit or parks a resolver; `release` hands the permit directly to the next waiter (no counter round-trip) or, with no waiters, increments back up to `maxPermits` (semaphore.ts:39–48). Used by the TCP server: each connection holds a `Semaphore(MAX_CONCURRENT_PER_CONNECTION = 50)` and wraps every frame's command handling in `withSemaphore` (tcp.ts:27, 197, 284), capping in-flight pipelined commands per socket.

### Lease lifecycle

1. **Acquire** — on `PULL`, `pullWithLock` calls `createLock(jobId, owner, ctx, ttl)` (`queue-manager/delivery.ts`). `createLock` returns `null` unless the job is in `processing` and has no existing lock (lockOperations.ts:19–24, defensive against double-lease).
2. **Renew** — `JobHeartbeat`/`ExtendLock` → `renewJobLock`. Fails if no lock, token mismatch, or already expired (in which case the stale lock is deleted) (lockOperations.ts:48–73). On success it also refreshes `job.lastHeartbeat` for legacy stall detection (lockOperations.ts:66–69).
3. **Release** — `ACK`/`FAIL` → `releaseLock`. Returns `true` when there is nothing to release; if a `token` is supplied it must match (lockOperations.ts:96–107).

Manual management claims are also terminal for the current lease even when the
job itself is requeued. `releaseClaimedJobOwnership` removes the `jobLocks`
entry and detaches the id from every `clientJobs` owner in the same synchronous
processing-map claim used by `moveActiveToWait`, `moveToWaitingChildren`,
`moveJobToDelayed`, and active `discardJob`. Both deletions are idempotent, so a
concurrent disconnect cleanup cannot leave a lease behind or release the
requeued job a second time.

### ACK ownership check + #101 grace window

`ack` rejects a token only when `verifyLock` fails **and** `isExpiredButOwned` is false (`queue-manager/ack.ts`). `isExpiredButOwned` (`queue-manager/delivery.ts`) grants a late ACK iff all hold: (1) job still `processing`, (2) lock token still matches, (3) `job.startedAt <= lock.createdAt` — the **re-lease guard**. If a stall retry re-pulled the job, `startedAt` is newer than the lingering lock's `createdAt`, condition 3 fails, and the timed-out worker's late ACK is rejected to prevent double-completion. `throwIfOwnershipConflict` in the same delivery module raises only when the job is still `processing` with a live lock; if the background sweep already requeued it, the failed verification is swallowed silently.

### `checkExpiredLocks` (lock-expiry sweep)

Runs on the background timer at `stallCheckMs` (5 s), registered in `background/lifecycle.ts`. Three phases (lockManager.ts:40):

1. **Collect** (lock-free read) — scan `ctx.jobLocks`; for each `isLockExpired` lock, look up the job in its processing shard. If the job is gone, delete the orphan lock immediately (lockManager.ts:61–63).
2. **Group** by `shardIdx` then `procIdx` (lockManager.ts:71–84) so locks are acquired in hierarchy order.
3. **Process** under `withWriteLock(shardLocks[shardIdx])` → `withWriteLock(processingLocks[procIdx])` (lockManager.ts:87–97). For each job (`processExpiredLockInner`, lockManager.ts:105):
   - Remove from processing.
   - `cron:` jobs with preventOverlap are **discarded**, not requeued (#75) — release resources, drop from index, delete from SQLite (lockManager.ts:124–130).
   - Otherwise: `attempts++`, `startedAt = null`, `stallCount++`. If `attempts >= maxAttempts` or `stallCount >= maxStalls` → terminal DLQ; otherwise `requeueExpiredJob`.
   - Delete the lock and emit `job:lock-expired`.

`handleRecoveryBoundExceeded` calls `releaseJobResources` (else the concurrency slot leaks), `addToDlq`, then **persists both** `saveDlqEntry` and `deleteJob` — without both writes the `jobs` row survives as an orphan and a later retry re-inserts it, throwing `UNIQUE constraint failed: jobs.id` (#97). Those two writes require the caller's `LockContext` to carry `storage`: the background sweep's context builder omitted it until #110, so the persistence silently no-op'd on the only production path. `LockContext.storage` is now required (nullable) in `application/types/contexts.ts`, and `background/lifecycle.ts` always supplies it. `requeueExpiredJob` releases resources, re-pushes to the priority queue, re-increments queued counters, and notifies.

### `checkStalledJobs` (two-phase stall detection)

Runs on the background timer at `stallCheckMs` (5 s), registered in `background/lifecycle.ts`. Two-phase to avoid false positives (stallDetection.ts:21):

1. **Phase 1** — for each `jobId` carried over in `ctx.stalledCandidates`, re-check `getStallAction`. If the job vanished or stall detection is disabled, drop the candidate. A still-non-`Keep` action is confirmed (stallDetection.ts:25–47).
2. **Phase 2** — scan all `processingShards`; any job whose `getStallAction !== Keep` becomes a candidate for the **next** cycle (stallDetection.ts:50–62). A job must be flagged stalled in two consecutive cycles before action is taken.

`getStallAction` → `checkStall` (stall.ts:41): returns `Keep` if `startedAt === null`, still inside `gracePeriod`, or `now - lastHeartbeat <= stallInterval` (per-job `job.stallTimeout` overrides the config interval). Otherwise increments a hypothetical count and returns `MoveToDlq` when `>= maxStalls`, else `Retry`.

`handleStalledJob` (stallDetection.ts:79) re-acquires `shardLocks[idx]` → `processingLocks[procIdx]`, re-verifies the job is still in processing (stallDetection.ts:94) before acting, then calls `moveStalliedJobToDlq` or `retryStalliedJob`. A confirmed stall that would make `attempts >= maxAttempts` is terminal even when the stall-count action alone said retry. Events/webhooks are broadcast **after** the locked section, only if `handled` (stallDetection.ts:111). `retryStalliedJob` bumps stall count + attempts, computes `runAt = now + calculateBackoff(job)` (exponential w/ jitter), appends timeline entries (capped at `MAX_TIMELINE_ENTRIES`), re-pushes, and persists both retry counters via `updateForRetry`. Both stall paths discard `cron:` preventOverlap jobs instead of retrying/DLQ-ing.

## Concurrency & Locking

**Lock hierarchy** (acquire strictly in this order — CLAUDE.md): `jobIndex` → `completedJobs` → `shards[N]` → `processingShards[N]`. In practice `jobIndex` (a plain `Map`) and `completedJobs` (a `BoundedSet`) are **read lock-free first**, then the two real `RWLock` arrays are taken as write locks in order: `shardLocks[shardIdx]` **before** `processingLocks[procIdx]`. Both `checkExpiredLocks` (lockManager.ts:87–97) and `handleStalledJob` (stallDetection.ts:91–92) follow this; `checkExpiredLocks` pre-groups its work by `(shardIdx, procIdx)` specifically so it can take locks in hierarchy order even when many expired locks span shards.

**Lease vs. heartbeat (two independent stall signals).** A job can be reclaimed by either (a) `JobLock` TTL expiry (`checkExpiredLocks`), used when the worker pulled *with* a lock token, or (b) heartbeat-timeout stall detection (`checkStalledJobs`), driven by `job.lastHeartbeat`/`stallInterval`. `renewJobLock` updates both (lockOperations.ts:66–69), so a worker heartbeating its lease also keeps stall detection satisfied.

**Races handled:**

- *Late ACK after lock expiry* — the #101 grace window (`isExpiredButOwned`) honors a genuine same-instance completion while rejecting a re-pulled-job double-completion via the `createdAt >= startedAt` guard (`queue-manager/delivery.ts`).
- *Lingering lock is load-bearing* — the stall retry path requeues a job **without** deleting its (now-expired) lock; that lingering token is what the re-lease guard inspects and what the worker-dedup (#33) path keys on. The lock-expiry path, by contrast, *does* delete the lock so a re-lease installs a fresh token.
- *Concurrent completion vs. stall handler* — both `checkExpiredLocks` and `handleStalledJob` re-verify membership in `processingShards` under the locks before mutating (lockManager.ts:57, stallDetection.ts:94), so a job completed between phases is skipped and no stale `Stalled`/`Failed` event fires.
- *Atomic pull handoff (2.8.31):* the queue to processing transition happens in one synchronous critical section under the shard write lock: `tryDequeueNextJob` pops the job, inserts it into `processingShards`, and flips the `jobIndex` entry to `processing` before yielding (pull.ts:97-117). The processing-shard `Map` is written without taking `processingLocks` there; this is safe because until the flip no id-targeted critical section can be mid-operation on that id, and it avoids holding the hot shard write lock across an await. `finalizeProcessing` (pull.ts:133) then does only post-await bookkeeping (markActive persistence, counters, broadcast) and re-checks membership in `processingShards` first: if a management op (discard, moveToDelayed, obliterate) claimed the job in between, the pull does not deliver it to the worker.
- *Double release* — every `LockGuard` is idempotent.

## Edge Cases & Failure Modes

- **Lock timeout** — `acquire`/`acquireRead`/`acquireWrite` throw `LockTimeoutError` after `LOCK_TIMEOUT_MS` (default 5 s). Callers using `withWriteLock` propagate the rejection; background sweeps wrap calls in `.catch(...)` (`background/lifecycle.ts`).
- **Resource-slot leaks** — every reclaim path (`handleRecoveryBoundExceeded`, `requeueExpiredJob`, `moveStalliedJobToDlq`, `retryStalliedJob`) calls `shard.releaseJobResources(queue, uniqueKey, groupId, ownerId)` before moving the job; omitting it wedges the queue's concurrency limiter. Passing `ownerId` also prevents a stale generation from releasing a replacement job's unique key.
- **Orphan SQLite rows (#97)** — DLQ moves must `saveDlqEntry` + `deleteJob`; missing the delete leaves a `jobs` row that collides on retry with `UNIQUE constraint failed: jobs.id`.
- **Cron preventOverlap (#73/#75)** — `cron:`-prefixed jobs are discarded rather than requeued/DLQ'd on stall or lock expiry, since the scheduler re-creates them on the next tick; requeuing would cause "starts right away on reconnect".
- **False-positive suppression** — single-cycle hiccups never trigger action thanks to two-phase detection plus the `gracePeriod` after job start.
- **Idempotent `createLock`** — returns `null` if a lock already exists or the job isn't in `processing`, so a buggy double-pull cannot mint a second token.
- **Token-less heartbeat** — `jobHeartbeat`/`renewJobLock` without a token just bumps `job.lastHeartbeat`; only the heartbeat-stall path (not the lease-TTL path) is then satisfied.
- **`stallCount` monotonic and durable** — all reclaim mechanisms increment `stallCount`, and `updateForRetry` writes it to `jobs.stall_count`; a job flapping between stall, lock-expiry, and process restart still converges to `maxStalls` → DLQ instead of resetting to zero.
- **Invariant:** `processingLocks` are never acquired before `shardLocks`; violating this risks deadlock against the lifecycle paths in [Job Lifecycle](./job-lifecycle.md).

## Configuration

| Name | Default | Effect |
| --- | --- | --- |
| `LOCK_TIMEOUT_MS` (env) | `5000` | Default timeout for `AsyncLock`/`RWLock` acquisition (lock.ts:8). |
| `DEFAULT_LOCK_TTL` | `30_000` ms | Job lease TTL when `pullWithLock` is called without an explicit `ttl` (job.ts:507). |
| `StallConfig.enabled` | `true` | Per-queue toggle for stall detection (stall.ts:21). |
| `StallConfig.stallInterval` | `30_000` ms | No-heartbeat window before a job is a stall candidate; per-job `stallTimeout` overrides. |
| `StallConfig.maxStalls` | `3` | Stalls before the job is moved to DLQ. |
| `StallConfig.gracePeriod` | `5_000` ms | Quiet period after start before stall checks apply. |
| `stallCheckMs` (config) | `5_000` ms | Interval for **both** `checkStalledJobs` and `checkExpiredLocks` (`application/types/config.ts`, `background/lifecycle.ts`). |
| `MAX_CONCURRENT_PER_CONNECTION` | `50` | Per-socket semaphore permits for pipelined TCP command processing (tcp.ts:27). |

Per-queue `StallConfig` is set via `queue.setStallConfig({...})` (embedded) and read by the sweeps through `shard.getStallConfig(queue)`.

## Related Docs

- [Core Queue Engine](./core-queue-engine.md)
- [Job Lifecycle](./job-lifecycle.md)
- [Background Tasks](./background-tasks.md)
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md)
- [Client SDK: Worker](./client-worker-sdk.md)
- [Dead Letter Queue (DLQ)](./dead-letter-queue.md)
- [TCP Server Command Handlers](./tcp-server-handlers.md)
- [Data Structures](./data-structures.md)
