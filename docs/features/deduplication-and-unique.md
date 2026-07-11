# Deduplication & Unique Jobs

> **Category:** Jobs · **Source:** `src/domain/queue/uniqueKeyManager.ts`, `src/domain/types/deduplication.ts`, `src/client/queue/deduplication.ts`, `src/application/operations/push.ts`, `src/client/bunqueue/dedupDebounce.ts`, `src/client/forwarder.ts`

## Purpose

Prevents duplicate jobs from entering a queue. Two independent mechanisms cover different needs: **custom job IDs** (`jobId`) make a re-add idempotent — while the prior job is unfinished (waiting / waiting-children / **active**) the add is a no-op that returns the existing job, and once it has *completed* the deterministic id is recycled for a fresh job (#92) — and **unique keys** (`deduplication.id`) suppress / extend / replace pending jobs within an optional TTL window. Both checks run inside the shard write lock during `pushJob` so concurrent adds of the same key cannot race. This is how store-and-forward edges, cron `preventOverlap`, and BullMQ-style `deduplication`/`debounce` options avoid creating redundant work.

## Responsibilities & Scope

Owns:

- The `UniqueKeyManager` (`src/domain/queue/uniqueKeyManager.ts`) — per-queue `key → UniqueKeyEntry` registry with TTL expiry, one instance per `Shard`.
- The deduplication type model and strategy resolution (`reject` / `extend` / `replace`) in `src/domain/types/deduplication.ts`.
- The push-time dedup decision logic (`handleCustomId`, `handleDeduplication`) in `src/application/operations/push.ts`.
- The client-side `Bunqueue` dedup/debounce option merger (`DedupDebounceMerger`) and the read-only `getDeduplicationJobId` / `removeDeduplicationKey` client surface.

Does NOT own:

- The `customIdMap` LRU itself — it lives on the `QueueManager` (`src/application/queueManager.ts:162`) and is passed into the push context; see [Core Queue Engine](./core-queue-engine.md).
- Persistence of `unique_key` / `custom_id` columns and the recovery rehydration — see [Persistence](./persistence.md). Re-registration on startup happens in [Background Tasks](./background-tasks.md).
- Lock acquisition / ordering — see [Concurrency & Locking](./concurrency-and-locking.md).
- Resource release on completion/failure (`releaseJobResources`) — invoked from the ack/fail paths in [Job Lifecycle](./job-lifecycle.md).
- Cron overlap prevention — `preventOverlap` is implemented in [Scheduler & Cron](./scheduler-and-cron.md); this module only supplies the `cron:<name>` unique key semantics.

## Dependencies

Internal:

- `src/domain/types/job.ts` — `Job.uniqueKey`, `Job.customId`, `Job.deduplicationTtl/Extend/Replace`, `Job.debounceId/debounceTtl`, and `JobInput`/`createJob`.
- `src/domain/types/deduplication.ts` — `UniqueKeyEntry`, `DeduplicationOptions`, `DeduplicationStrategy`, plus the pure helpers `isUniqueKeyExpired` / `calculateExpiration` / `getDeduplicationStrategy`.
- `src/domain/queue/shard.ts` — delegates unique-key calls to its private `UniqueKeyManager` (`shard.ts:46`).
- `src/shared/lru.ts` (`LRUMap`) — backs `customIdMap`.
- `src/shared/lock.ts` (`withWriteLock`) — wraps the push critical section.

External / runtime: Bun + SQLite (the `jobs` table, partial index `idx_jobs_unique`). No external runtime dependencies.

## Public Interface

### `UniqueKeyManager` (`src/domain/queue/uniqueKeyManager.ts`)

```typescript
class UniqueKeyManager {
  isAvailable(queue: string, key: string): boolean;                          // :18
  getEntry(queue: string, key: string): UniqueKeyEntry | null;               // :29
  register(queue: string, key: string, jobId: JobId): void;                  // :40  (legacy, no TTL)
  registerWithTtl(queue: string, key: string, jobId: JobId, ttl?: number): void; // :45
  extendTtl(queue: string, key: string, ttl: number): boolean;               // :60
  release(queue: string, key: string): void;                                 // :68
  cleanExpired(): number;                                                     // :73
  clearQueue(queue: string): void;                                            // :88
  getMap(): Map<string, Map<string, UniqueKeyEntry>>;                         // :93
}
```

`getEntry` and `isAvailable` are lazy-expiring: they delete the entry in place if `isUniqueKeyExpired` returns true, so an expired key transparently frees its slot on the next read (`uniqueKeyManager.ts:21-24`, `:32-35`).

`Shard` exposes thin delegations (`src/domain/queue/shard.ts:125-155`): `isUniqueAvailable`, `getUniqueKeyEntry`, `registerUniqueKey`, `registerUniqueKeyWithTtl`, `extendUniqueKeyTtl`, `releaseUniqueKey`, `cleanExpiredUniqueKeys`, and the `uniqueKeys` getter.

### Deduplication types & helpers (`src/domain/types/deduplication.ts`)

```typescript
type DeduplicationStrategy = 'reject' | 'extend' | 'replace';               // :9
interface DeduplicationOptions { id: string; ttl?: number; extend?: boolean; replace?: boolean; } // :14
interface UniqueKeyEntry { jobId: JobId; expiresAt: number | null; registeredAt: number; }        // :28

getDeduplicationStrategy(opts): DeduplicationStrategy;   // :40  replace > extend > reject
isUniqueKeyExpired(entry, now=Date.now()): boolean;      // :49  expiresAt !== null && expiresAt <= now
calculateExpiration(ttl, now=Date.now()): number | null; // :56  ttl===undefined → null (never expires)
```

### Client-side helpers (`src/client/queue/deduplication.ts`)

```typescript
getDeduplicationJobId(ctx, deduplicationId): Promise<string | null>; // :13  embedded-only; TCP → null
removeDeduplicationKey(ctx, deduplicationId): Promise<number>;        // :25  embedded-only; returns 1 if found, else 0
```

Both are surfaced on `Queue` (`src/client/queue/queue.ts:494-498`). In TCP mode they always resolve to `null` / `0` — there is no wire command for them.

### `DedupDebounceMerger` (`src/client/bunqueue/dedupDebounce.ts`)

```typescript
class DedupDebounceMerger {
  constructor(dedup: BunqueueDeduplicationConfig | null, debounce: BunqueueDebounceConfig | null);
  get active(): boolean;
  merge(name: string, opts?: JobOptions, data?: unknown): JobOptions | undefined; // :21
}
```

### TCP commands / events

- This module does not own a dedicated TCP command. Dedup options ride on `PUSH` / `PUSHB` (`PushCommand.uniqueKey`, `.jobId`, `.dedup`, `.debounceId`, `.debounceTtl` — `src/domain/types/command.ts:27-56`).
- Event emitted on suppression: `EventType.Duplicated` = `'duplicated'` (`src/domain/types/queue.ts:121`), broadcast from the default reject path (`push.ts:189-194`).
- Dashboard-only events: `job:deduplicated` (strategy `extend` / `default`) and `batch:pushed` with a `duplicates` count.

## Data Models

Wire/job fields relevant here (full definitions in [data-model](../data-model.md)):

| Field | Source | Meaning |
| ----- | ------ | ------- |
| `JobOptions.jobId` | `client/types.ts:344` | Custom idempotency id → becomes `job.customId` and the job's primary key |
| `JobOptions.deduplication` | `client/types.ts:390` | `{ id, ttl?, extend?, replace? }` (BullMQ v5 compatible) |
| `JobOptions.debounce` | `client/types.ts:391` | `{ id, ttl }` (stored only — see gotcha below) |
| `Job.uniqueKey` | `domain/types/job.ts:107` | `= deduplication.id`; drives unique-key dedup |
| `Job.customId` | `domain/types/job.ts:108` | `= jobId` (explicit custom id only); mapped via `customIdMap` |
| `Job.deduplicationTtl/Extend/Replace` | `domain/types/job.ts:156-160` | persisted dedup strategy/TTL |
| `UniqueKeyEntry` | `domain/types/deduplication.ts:28` | in-memory `{ jobId, expiresAt, registeredAt }` |

Client → wire mapping (`src/client/queue/operations/add.ts:108-117`): `uniqueKey = deduplication?.id`, `customId = jobId` (only an **explicit** `jobId`), `dedup = { ttl, extend, replace }`. The dedup id is carried solely by `uniqueKey` into `handleDeduplication` and is deliberately **not** mirrored into `customId`: doing so made `handleCustomId` short-circuit a same-key re-add as an idempotent no-op *before* `handleDeduplication`'s suppress/replace/extend logic could run — silently dropping `replace`/`extend` in **embedded** mode. The TCP single-push (`core.ts:69` → `customId: cmd.jobId`) and bulk paths already send `customId = jobId` only, so this aligns embedded with the (correct) TCP behavior.

SQLite columns: `jobs.unique_key`, `jobs.custom_id` (`schema.ts:34-35`). Note `idx_jobs_unique ON jobs(queue, unique_key) WHERE unique_key IS NOT NULL` (`schema.ts:58-59`) is a **non-unique** index for lookup speed — uniqueness is enforced in memory, not by the database. Custom-id idempotency is backstopped by the `jobs.id` PRIMARY KEY (the custom id becomes the row id).

## Business Logic / Control Flow

Single push: `pushJob` takes the shard write lock and runs both checks inside it (`push.ts:257-296`).

### 1. Custom-ID idempotency — `handleCustomId` (`push.ts:61`)

1. No `customId` → return a fresh generated id (`push.ts:62-64`).
2. `id = jobId(input.customId)` (the custom id *is* the job id). Look up `customIdMap`. If the prior job is still **unfinished** (and not in `completedJobs`), the re-add is an idempotent no-op (BullMQ parity) — never a second insert of the same deterministic id, which would otherwise collide on the `jobs.id` PRIMARY KEY (durable → `UNIQUE constraint failed: jobs.id`; buffered → the write buffer silently drops the colliding insert). Three unfinished states are covered (`push.ts:82-95`):
   - **waiting / delayed / prioritized** — `jobIndex` type `queue` and present in the PriorityQueue → return the existing job.
   - **waiting-children** — `jobIndex` type `queue` but held in `shard.waitingDeps` (its row is already persisted, so it is *not* in the PriorityQueue) → idempotent skip via the existing id.
   - **active (processing)** — `jobIndex` type `processing`; the job was popped from the queue and is still running on a worker, with its row still on disk → idempotent skip via the existing id.
   For the latter two, `PushContext` has no `processingShards`, so it cannot fetch the live `Job`; `pushJob` / `pushJobBatch` rebuild a placeholder `{ ...job, id }` after `createJob` (mirrors `handleDeduplication`'s active path) and insert nothing. **Completed (#92) and DLQ jobs are terminal** and fall through to the reuse path below.
3. **Reuse path (#92):** if the same id sits in `completedJobs`, its row survives on disk (`markCompleted` is an UPDATE, not DELETE). Evict it from `completedJobs`, `completedJobsData`, `jobResults`, `jobIndex`, and `storage.deleteJob(id)` so the recycled id starts fresh as `waiting` rather than reporting `completed` (`push.ts:105-111`). This delete is gated on `completedJobs` — it is **not** run on every push, which would tax the customId hot path with a synchronous DELETE + buffer scan inside the shard lock (a measured ~2× push/addBulk regression).
4. Clear any stale timeout marker for the recycled id so stall-retry recovery is not wrongly discarded (guards against #33/#75 duplicate execution) (`push.ts:124`).
5. Record `customIdMap.set(customId, id)` and proceed (`push.ts:125`).

### 2. Unique-key dedup — `handleDeduplication` (`push.ts:133`)

1. No `job.uniqueKey` → `{ skip: false }` (`push.ts:140-142`).
2. No existing entry → `registerUniqueKeyWithTtl(queue, key, job.id, dedup?.ttl)` and allow insert (`push.ts:147-150`).
3. **`replace`** (`push.ts:155-165`): if the existing job is still in the queue, `q.remove` + `decrementQueued` + drop its `jobIndex` entry; release the old key, register the new job under it, allow insert.
4. **`extend`** (requires `dedup.extend && dedup.ttl`, `push.ts:168-181`): `extendUniqueKeyTtl`, drop the incoming `customId` mapping, and if the existing job is still present return `{ skip: true, existingId }` (emits `job:deduplicated` strategy `extend`). If the existing job vanished, throw `Duplicate unique_key (extended TTL)`.
5. **Default (reject, BullMQ-style)** (`push.ts:183-201`): drop the incoming `customId` mapping; if the existing job is still in the queue **or** still tracked in `jobIndex` (active/processing — key held until ack), broadcast `EventType.Duplicated`, emit `job:deduplicated`, and skip.
6. If the prior owner is gone from `jobIndex` (completed/failed), re-register the key for the new job and allow the insert (`push.ts:203-205`).

When a dedup result is `skip`, the caller returns the existing queued job if found, otherwise a placeholder carrying `existingId` so the caller sees the correct id without a duplicate insert (`push.ts:280-290`). The custom-id idempotency path uses the same placeholder shape for its active / waiting-children skips.

### 3. Batch push — `pushJobBatch` (`push.ts:323`)

Runs `handleCustomId` + `handleDeduplication` per input inside one shard lock; suppressed inputs (custom-id idempotent skip — queued, active, or waiting-children — or unique-key dedup) push the existing id into `resultIds` and continue, inserting nothing. Emits a `batch:pushed` dashboard event with `duplicates: inputs.length - inserted` (`push.ts:399-406`).

### Lookup — `getJobByCustomId` (`queryOperations.ts:105`)

`customIdMap.get(customId)` → `jobIndex` location → resolve from queue / `waitingDeps` / `waitingChildren` / `processingShards` / completed (storage fallback) / DLQ.

### Forwarder dedup key (`src/client/forwarder.ts:94`)

Store-and-forward assigns each forwarded job the deterministic remote id `fwd:<source.queueKey>:<localJobId>`, sent as the remote `jobId`. The server's custom-id idempotency then makes a re-forward after a crash/retry a no-op remotely. See [Store-and-Forward](./store-and-forward.md).

### Bunqueue auto-dedup/debounce (`dedupDebounce.ts:21`)

If a `Bunqueue` instance is configured with `deduplication`/`debounce`, `merge` injects defaults into each `add` (unless already set): `deduplication.id = "${name}:${JSON.stringify(data)}"` with `ttl ?? 3600000` and the configured `extend`/`replace`; `debounce.id = name`, `debounce.ttl`.

## Concurrency & Locking

Both `handleCustomId` and `handleDeduplication` execute **inside** the per-shard write lock (`withWriteLock(ctx.shardLocks[idx], …)`, `push.ts:257`, `:338`) — the comment at `push.ts:249` and `:321` notes this is deliberate to prevent check-then-insert races on the same custom id / unique key. The `customIdMap` write and the unique-key registration are therefore atomic with the queue insert.

Lock order follows the global hierarchy (`jobIndex` → `completedJobs` → `shards[N]`); the unique-key registry is private state of the held shard, so no extra lock is needed. See [Concurrency & Locking](./concurrency-and-locking.md).

Lifecycle / release:

- On successful ack and on terminal fail, `Shard.releaseJobResources(queue, uniqueKey, groupId)` releases the key (`shard.ts:216-217`; called from `ack.ts:93,230`, `ackHelpers.ts:148`). The default-reject path intentionally holds the key for active jobs until ack (`push.ts:183-188`).
- `ack` paths also delete the `customId` mapping (`ack.ts:98,174,258`, `ackHelpers.ts:223`).
- `cancelJob` releases the key whether the job is in the queue, `waitingChildren`, or `waitingDeps` (`jobManagement.ts:41,65`).

## Edge Cases & Failure Modes

- **TTL semantics:** `ttl` undefined ⇒ `expiresAt = null` ⇒ never expires (`calculateExpiration`, `deduplication.ts:60`). Expiry is lazy on read plus a periodic sweep.
- **Re-add of an active / waiting-children custom id is idempotent:** when the prior job has been popped from the PriorityQueue (active = `jobIndex` type `processing`; or waiting-children = type `queue` but held in `shard.waitingDeps`) its row is still on disk, so re-adding the same `jobId` must NOT take the reuse path — that would re-insert the same deterministic id and collide on the `jobs.id` PRIMARY KEY (durable → throws `UNIQUE constraint failed: jobs.id`; buffered → the write buffer silently drops the insert, turning the intended no-op into a reject). `handleCustomId` instead returns the existing job (or, when the live `Job` is unreachable without `processingShards`, a `{ ...job, id }` placeholder) and inserts nothing (`push.ts:82-95`). This is the custom-id twin of the unique-key fix #69. The guard is `!ctx.completedJobs.has(id)` plus a `queue`/`processing` location check, so completed (#92) and DLQ jobs still fall through to reuse.
- **Custom-id reuse after completion (#92):** completed rows survive on disk; the reuse path evicts them so the recycled id does not falsely report `completed` or collide on the PK at flush (`push.ts:105-111`).
- **Orphan-row reconciliation via upsert:** a durable `jobs` row can outlive its in-memory tracking when `obliterate()` (a fire-and-forget `void` over TCP — there is no `obliterateAsync`) or a write-buffer flush is reordered relative to an in-flight durable insert: the `customIdMap` / `jobIndex` entries are cleared but the row survives. A later re-add of the same custom id finds no mapping → reuse path → insert. To stop the recycled id colliding on the PK (`UNIQUE constraint failed: jobs.id`), **both** insert statements use `ON CONFLICT(id) DO UPDATE` (upsert): the orphan row is overwritten in place at insert time. The `DO UPDATE SET` clause covers **all** non-id columns — including the per-execution fields `started_at` / `completed_at` / `progress` / `progress_msg` / `last_heartbeat` / `stacktrace`, which are absent from the INSERT column list so `excluded.<col>` resolves to their DEFAULT (the fresh-job value). Without resetting these, an upsert over a previously-completed orphan would leave a brand-new `waiting` job reporting `progress=100` / a stale `completed_at` / a prior life's `stacktrace`. A brand-new id is a plain INSERT with **zero** extra cost, so the customId hot path is not taxed (`statements.ts` `insertJob`, `sqliteBatch.ts` batch insert). In the buffered batch path this also prevents one collision from failing the whole flush and dropping every innocent job batched in the same window (`reportLostJobs`). `jobs` has no triggers/FKs, so `DO UPDATE` has no cascade (unlike `REPLACE`). Covered by `test/repro-idem-active-readd.test.ts`.
- **Recycled-id timeout marker (#33/#75):** stale `timedOutJobs` entries are cleared on reuse so the new job's stall-retry recovery is not silently dropped (`push.ts:124`).
- **`extend` with vanished owner:** throws `Duplicate unique_key (extended TTL)` rather than silently inserting (`push.ts:180`).
- **Restart rehydration:** recovery re-populates `customIdMap` for pending jobs and re-registers unique keys via `registerUniqueKeyWithTtl(..., job.deduplicationTtl ?? undefined)` (`backgroundTasks.ts:356,361`). Completed jobs are deliberately **not** added to `customIdMap` during recovery to avoid LRU-evicting pending mappings (`backgroundTasks.ts:409-412`); custom-id collisions against completed jobs are caught by the storage fallback in `handleCustomId`.
- **Memory bounds:** `customIdMap` is an `LRUMap` capped at `maxCustomIds` (default `50_000`, `application/types.ts:37`; init `queueManager.ts:162`). Unique-key registries are swept every cleanup tick (`cleanExpiredUniqueKeys`, `cleanupTasks.ts:101`) and any single-queue registry exceeding **1000** entries is force-trimmed by half via insertion-order iteration (`cleanupTasks.ts:104-113`) — under churn a still-live key can be evicted, weakening (not breaking) dedup. LRU eviction of `customIdMap` likewise allows a re-add to slip through.
- **Debounce is metadata-only (gotcha):** `debounce` sets `job.debounceId`/`job.debounceTtl` (`add.ts:141-142`, `core.ts:89-90`) but does **not** populate `uniqueKey`, so it does not itself suppress pushes — `handleDeduplication` returns early when `uniqueKey` is falsy. BullMQ-style debounce behavior is achieved via `deduplication` with `extend: true` (which is what `DedupDebounceMerger` emits as `deduplication.id`).
- **Client helpers are read-only / embedded-only:** `removeDeduplicationKey` (`deduplication.ts:25`) returns `1` if a job exists for the id but does **not** actually release the key; `getDeduplicationJobId` returns `null` in TCP mode. The `JobProxy.removeDeduplicationKey` surface rejects with "not implemented — no server primitive available" (`jobProxy.ts:236-239,537-540`).
- **Obliterate:** `Shard` clears the per-queue unique-key registry (`uniqueKeyManager.clearQueue`, `shard.ts:484`) and `QueueManager` clears the whole registry on full obliterate (`queueManager.ts:1895`).
- **Cron overlap:** `preventOverlap` derives the unique key `cron:<name>` (`cronScheduler.ts` `effectiveUniqueKey`, `queueManager.ts:1268-1270`); `removeOrphanedCronJob` releases it for stale waiting jobs on re-upsert (`queueManager.ts:1279-1297`).

## Configuration

| Option / Env | Default | Effect |
| ------------ | ------- | ------ |
| `maxCustomIds` (QueueManager config, `application/types.ts:23,37`) | `50_000` | LRU cap for `customIdMap` |
| per-queue unique-key trim threshold (`cleanupTasks.ts:105`) | `1000` | force half-trim above this size |
| `JobOptions.jobId` | — | custom idempotency id |
| `JobOptions.deduplication.{id,ttl,extend,replace}` | `ttl` none / no expiry | unique-key dedup + strategy |
| `Bunqueue` `deduplication.ttl` (`dedupDebounce.ts:28`) | `3600000` ms | default dedup TTL for auto-injected keys |
| `JobOptions.debounce.{id,ttl}` | — | persisted debounce metadata (no push suppression on its own) |

No dedicated environment variables; the unique-key cleanup runs on the shared cleanup task (~10s — see [Background Tasks](./background-tasks.md)).

## Related Docs

- [Core Queue Engine](./core-queue-engine.md)
- [Job Lifecycle](./job-lifecycle.md)
- [Job Queries & Queue Control](./job-queries-and-control.md)
- [Scheduler & Cron](./scheduler-and-cron.md)
- [Background Tasks](./background-tasks.md)
- [Persistence](./persistence.md)
- [Store-and-Forward & BullMQ Compatibility](./store-and-forward.md)
- [Simple Mode (Bunqueue all-in-one)](./simple-mode.md)
- [Concurrency & Locking](./concurrency-and-locking.md)
- [Architecture](../architecture.md)
- [Data Model](../data-model.md)
