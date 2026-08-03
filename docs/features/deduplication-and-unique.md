# Deduplication & Unique Jobs

> **Category:** Jobs · **Source:** `src/domain/queue/uniqueKeyManager.ts`, `src/domain/types/deduplication.ts`, `src/client/queue/deduplication.ts`, `src/client/jobDeduplication.ts`, `src/application/operations/customId.ts`, `src/application/operations/pushDeduplication.ts`, `src/application/operations/pushAdmission.ts`, `src/application/operations/push.ts`, `src/application/operations/pushBatch.ts`, `src/client/bunqueue/dedupDebounce.ts`, `src/client/forwarder.ts`

## Purpose

Prevents duplicate jobs from entering a queue. Two independent mechanisms cover different needs: **custom job IDs** (`jobId`) make a re-add idempotent — while the prior job is unfinished (waiting / waiting-children / **active**) the add is a no-op that returns the existing job, and once it has *completed* the deterministic id is recycled for a fresh job (#92) — and **unique keys** (`deduplication.id`) suppress / extend / replace pending jobs within an optional TTL window. Both checks run inside the shard write lock during `pushJob` so concurrent adds of the same key cannot race. This is how store-and-forward edges, cron `preventOverlap`, and BullMQ-style `deduplication`/`debounce` options avoid creating redundant work.

## Responsibilities & Scope

Owns:

- The `UniqueKeyManager` (`src/domain/queue/uniqueKeyManager.ts`) — per-queue `key → UniqueKeyEntry` registry with TTL expiry, one instance per `Shard`.
- The deduplication type model and strategy resolution (`reject` / `extend` / `replace`) in `src/domain/types/deduplication.ts`.
- The push-time identity decisions (`handleCustomId`, `handleDeduplication`) and
  post-persistence publication in the focused application operation modules.
- The client-side `Bunqueue` dedup/debounce option merger (`DedupDebounceMerger`) and the live lookup/release surfaces on `Queue` and `Job`.

Does NOT own:

- The `customIdMap` LRU itself — it lives on `QueueManagerState` (`src/application/queue-manager/state.ts`) and is passed into the push context; see [Core Queue Engine](./core-queue-engine.md).
- Persistence of `unique_key` / `custom_id` columns and the recovery rehydration — see [Persistence](./persistence.md). Re-registration on startup happens in [Background Tasks](./background-tasks.md).
- Lock acquisition / ordering — see [Concurrency & Locking](./concurrency-and-locking.md).
- Resource release on completion/failure (`releaseJobResources`) — invoked from the ack/fail paths in [Job Lifecycle](./job-lifecycle.md).
- Cron overlap prevention — `preventOverlap` is implemented in [Scheduler & Cron](./scheduler-and-cron.md); this module only supplies the `cron:<name>` unique key semantics.

## Dependencies

Internal:

- `src/domain/types/job.ts` — `Job.uniqueKey`, `Job.customId`, `Job.deduplicationTtl/Extend/Replace`, `Job.debounceId/debounceTtl`, and `JobInput`/`createJob`.
- `src/domain/types/deduplication.ts` — `UniqueKeyEntry`, `DeduplicationOptions`, `DeduplicationStrategy`, plus the pure helpers `isUniqueKeyExpired` / `calculateExpiration` / `getDeduplicationStrategy`.
- `src/domain/queue/shard/keys.ts` — delegates unique-key calls to its private
  `UniqueKeyManager` (`keys.ts:7-32`).
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
  release(queue: string, key: string): boolean;
  releaseIfOwned(queue: string, key: string, ownerId: JobId): boolean;
  cleanExpired(): number;                                                     // :73
  clearQueue(queue: string): void;                                            // :88
  getMap(): Map<string, Map<string, UniqueKeyEntry>>;                         // :93
}
```

`getEntry` and `isAvailable` are lazy-expiring: they delete the entry in place if `isUniqueKeyExpired` returns true, so an expired key transparently frees its slot on the next read (`uniqueKeyManager.ts:21-24`, `:32-35`).

`Shard` exposes thin delegations: `isUniqueAvailable`, `getUniqueKeyEntry`, `registerUniqueKey`, `registerUniqueKeyWithTtl`, `extendUniqueKeyTtl`, `releaseUniqueKey`, `releaseUniqueKeyIfOwned`, `cleanExpiredUniqueKeys`, and the `uniqueKeys` getter.

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
getDeduplicationJobId(ctx, deduplicationId): Promise<string | null>;
removeDeduplicationKey(ctx, deduplicationId): Promise<number>;
removeJobDeduplicationKey(id, embedded, transport): Promise<boolean>;
```

The first two are surfaced on `Queue`; the owner-aware third helper backs every
public `Job.removeDeduplicationKey`. All three use `QueueManager` directly in
embedded mode and dedicated TCP commands remotely. Queue-level removal returns
`1` only when a live key was released. Job-level removal succeeds only when
that exact job is still the registered owner.

### `DedupDebounceMerger` (`src/client/bunqueue/dedupDebounce.ts`)

```typescript
class DedupDebounceMerger {
  constructor(dedup: BunqueueDeduplicationConfig | null, debounce: BunqueueDebounceConfig | null);
  get active(): boolean;
  merge(name: string, opts?: JobOptions, data?: unknown): JobOptions | undefined; // :21
}
```

### TCP commands / events

- Dedup options ride on `PUSH` / `PUSHB` (`PushCommand.uniqueKey`, `.jobId`, `.dedup`, `.debounceId`, `.debounceTtl`).
- `GetDeduplicationJobId { queue, deduplicationId }` returns `{ data: { jobId } }`.
- `RemoveDeduplicationKey { queue, deduplicationId }` returns `{ data: { count } }`.
- `RemoveJobDeduplicationKey { id }` returns `{ data: { removed } }` and enforces ownership.
- Event emitted on suppression: `EventType.Duplicated` = `'duplicated'` (`src/domain/types/queue.ts:139`), broadcast from the default reject path (`src/application/operations/pushDeduplication.ts:96-109`).
- Dashboard-only events: `job:deduplicated` (strategy `extend` / `default`) and `batch:pushed` with a `duplicates` count.

## Data Models

Wire/job fields relevant here (full definitions in [data-model](../data-model.md)):

| Field | Source | Meaning |
| ----- | ------ | ------- |
| `JobOptions.jobId` | `src/client/types/options.ts:49` | Custom idempotency id → becomes `job.customId` and the job's primary key |
| `JobOptions.deduplication` | `src/client/types/options.ts:66` | `{ id, ttl?, extend?, replace? }` (BullMQ v5 compatible) |
| `JobOptions.debounce` | `src/client/types/options.ts:67` | `{ id, ttl }` (stored only — see gotcha below) |
| `Job.uniqueKey` | `src/domain/types/jobs/model.ts:58` | `= deduplication.id`; drives unique-key dedup |
| `Job.customId` | `src/domain/types/jobs/model.ts:59` | `= jobId` (explicit custom id only); mapped via `customIdMap` |
| `Job.deduplicationTtl/Extend/Replace` | `src/domain/types/jobs/model.ts:82-84` | persisted dedup strategy/TTL |
| `UniqueKeyEntry` | `domain/types/deduplication.ts:28` | in-memory `{ jobId, expiresAt, registeredAt }` |

Client → wire mapping (`src/client/queue/operations/add/payload.ts:34-81`): `uniqueKey = deduplication?.id`, `customId = jobId` (only an **explicit** `jobId`), `dedup = { ttl, extend, replace }`. The dedup id is carried solely by `uniqueKey` into `handleDeduplication` and is deliberately **not** mirrored into `customId`: doing so made `handleCustomId` short-circuit a same-key re-add as an idempotent no-op *before* `handleDeduplication`'s suppress/replace/extend logic could run — silently dropping `replace`/`extend` in **embedded** mode. The TCP single-push (`core.ts:69` → `customId: cmd.jobId`) and bulk paths already send `customId = jobId` only, so this aligns embedded with the (correct) TCP behavior.

SQLite columns: `jobs.unique_key`, `jobs.custom_id` (`schema.ts:34-35`). Note `idx_jobs_unique ON jobs(queue, unique_key) WHERE unique_key IS NOT NULL` (`schema.ts:58-59`) is a **non-unique** index for lookup speed — uniqueness is enforced in memory, not by the database. Custom-id idempotency is backstopped by the `jobs.id` PRIMARY KEY (the custom id becomes the row id).

Consequently, custom IDs are broker-wide rather than queue-scoped. A live
generation suppresses the same ID even when the next push names another queue;
the existing job and its original data win. `uniqueKey`, by contrast, remains
scoped by queue.

## Business Logic / Control Flow

Single push: `pushJob` takes the required write locks and runs both checks inside them (`src/application/operations/push.ts:51-106`).

### 1. Custom-ID idempotency — `handleCustomId` (`customId.ts`)

1. No `customId` → return a fresh generated id.
2. `id = jobId(input.customId)` (the custom id *is* the job id). Look up `customIdMap`. If the prior job is still **unfinished** (and not in `completedJobs`), the re-add is an idempotent no-op (BullMQ parity) — never a second insert of the same deterministic id, which would otherwise collide on the `jobs.id` PRIMARY KEY. Three unfinished states are covered:
   - **waiting / delayed / prioritized** — `jobIndex` type `queue` and present in the PriorityQueue → return the existing job.
   - **waiting-children** — `jobIndex` type `queue` but held in `shard.waitingDeps` (its row is already persisted, so it is *not* in the PriorityQueue) → idempotent skip via the existing id.
   - **active (processing)** — `jobIndex` type `processing`; the job was popped from the queue and is still running on a worker, with its row still on disk → idempotent skip via the existing id.
   For the latter two, `PushContext` has no `processingShards`, so it cannot fetch the live `Job`; `pushJob` / `pushJobBatch` rebuild a placeholder `{ ...job, id }` after `createJob` (mirrors `handleDeduplication`'s active path) and insert nothing. **Completed (#92) and DLQ jobs are terminal** and fall through to the reuse path below.
3. **Terminal reuse is planned, not applied.** Completed, DLQ, and unreferenced
   payload-free dependency-completion generations produce a
   `CustomIdRetirement`. The inspection phase does not remove the old job,
   result, DLQ entry, completion proof, index, or map entry.
4. The matching SQLite admission retires the exact persisted generation and
   inserts the durable successor in one transaction. If that transaction is
   rejected, including because the disk is full, the previous generation and
   result remain authoritative before and after restart.
5. After persistence succeeds, `commitCustomIdAdmission` applies the matching
   RAM retirement, clears stale timeout/retired-lease markers, and records
   `customIdMap.set(customId, id)`. The recycled ID therefore exposes exactly
   one generation and a rejected candidate exposes none of its ownership.

### 2. Unique-key dedup — `handleDeduplication` (`src/application/operations/pushDeduplication.ts:43-114`)

1. No `job.uniqueKey` → `{ skip: false }`.
2. No existing entry → allow admission. Key ownership is registered only when
   the job is published after any required persistence succeeds.
3. **`replace`**: retire the exact pending owner from the runnable heap or
   dependency wait set together with its queue counters, temporal index,
   dependency consumer, custom-id mapping, and `jobIndex` entry. Durable
   replacement deletes the superseded SQLite row and inserts the new row in one
   transaction before publishing the in-memory transition. A same-key batch
   collapses intermediate generations so only its final replacement reaches
   persistence. Active owners are never removed. A durable replacement clears
   the active row's key and inserts the successor in one SQLite transaction;
   a non-durable successor follows the configured write-buffer semantics. RAM
   ownership moves only after the persistence step succeeds. The predecessor
   finishes under its old lease, and its later completion or failure cannot
   release the successor's key. Replacement is rejected before persistence when
   `DependencyResultTracker` reports any live consumer of the owner, because
   removing that id would strand the dependency graph.
4. **`extend`** (requires `dedup.extend && dedup.ttl`): extend the existing
   owner's TTL and return `{ skip: true, existingId }` when it is still queued
   (emits `job:deduplicated` strategy `extend`). If the owner vanished, throw
   `Duplicate unique_key (extended TTL)`.
5. **Default (reject, BullMQ-style):** if the owner is still in the queue **or**
   tracked in `jobIndex` (including active/processing, where the key is held
   until ACK), broadcast `EventType.Duplicated`, emit `job:deduplicated`, and
   skip. No candidate custom-ID or key ownership has been published to undo.
6. If the prior owner is gone from `jobIndex` (completed/failed), allow the new
   admission and register its key only after the required storage operation.

When a dedup result is `skip`, the caller returns the existing queued job if found, otherwise a placeholder carrying `existingId` so the caller sees the correct id without a duplicate insert (`src/application/operations/push.ts:76-85`). The custom-id idempotency path uses the same placeholder shape for its active / waiting-children skips.

### 3. Batch push — `pushJobBatch` (`src/application/operations/pushBatch.ts`)

Runs `handleCustomId` + `handleDeduplication` per input inside one shard lock; suppressed inputs (custom-id idempotent skip — queued, active, or waiting-children — or unique-key dedup) push the existing id into `resultIds` and continue, inserting nothing. Emits a `batch:pushed` dashboard event with `duplicates: inputs.length - inserted` after full success.

`PUSHB` is an ordered batch, not an atomic graph commit (`PUSHF` provides that
contract). If a later input throws after an earlier prefix was admitted, the
accepted prefix is persisted according to each job's `durable` option, counted,
announced and notified before the original error is returned. Inputs after the
failure are not evaluated. This prevents a rejected request from leaving a
RAM-only, uncounted prefix while avoiding a false all-or-nothing guarantee.

### Lookup — `getJobByCustomId` (`src/application/operations/query/jobLookup.ts:69-101`)

`customIdMap.get(customId)` → `jobIndex` location → resolve from queue / `waitingDeps` / `waitingChildren` / `processingShards` / completed (storage fallback) / DLQ.

### Forwarder dedup key (`src/client/forwarder.ts:94`)

Store-and-forward assigns each forwarded job the deterministic remote id `fwd:<source.queueKey>:<localJobId>`, sent as the remote `jobId`. The server's custom-id idempotency then makes a re-forward after a crash/retry a no-op remotely. See [Store-and-Forward](./store-and-forward.md).

### Bunqueue auto-dedup/debounce (`dedupDebounce.ts:21`)

If a `Bunqueue` instance is configured with `deduplication`/`debounce`, `merge` injects defaults into each `add` (unless already set): `deduplication.id = "${name}:${JSON.stringify(data)}"` with `ttl ?? 3600000` and the configured `extend`/`replace`; `debounce.id = name`, `debounce.ttl`.

## Concurrency & Locking

Both `handleCustomId` and `handleDeduplication` execute inside write locks acquired by `withPushWriteLocks`. The helper always locks the target queue shard; when a custom id identifies a live queued or DLQ generation owned by another shard, it also locks that owning shard, in ascending shard order, and revalidates the required set after acquisition. Cross-queue live lookup and terminal retirement therefore never mutate or inspect shard-private state unlocked, and cannot invert lock order. The `customIdMap` write, unique-key registration, queue insertion, indexes and counters are published together only after the required storage admission succeeds.

Lock order follows the global hierarchy (`jobIndex` → `completedJobs` → `shards[N]`); the unique-key registry is private state of the held shard, so no extra lock is needed. See [Concurrency & Locking](./concurrency-and-locking.md).

Lifecycle / release:

- On successful ack and on terminal fail, `Shard.releaseJobResources(queue, uniqueKey, groupId, ownerId)` releases the key only if the terminating generation still owns it. The default-reject path intentionally holds the key for active jobs until ack.
- `ack`/terminal-failure paths also delete the `customId` mapping (`ack/completion.ts:45`, `ack/failure.ts:29,124`, `ackHelpers.ts:184-187`).
- `cancelJob` releases the key owner-safely whether the job is in the queue, `waitingChildren`, or `waitingDeps`.
- Explicit release also calls `SqliteStorage.clearJobUniqueKey(ownerId)`, so a restart cannot rehydrate a key that the client already removed.

## Edge Cases & Failure Modes

- **TTL semantics:** `ttl` undefined ⇒ `expiresAt = null` ⇒ never expires (`calculateExpiration`, `deduplication.ts:60`). Expiry is lazy on read plus a periodic sweep.
- **Re-add of an active / waiting-children custom id is idempotent:** when the prior job has been popped from the PriorityQueue (active = `jobIndex` type `processing`; or waiting-children = type `queue` but held in `shard.waitingDeps`) its row is still on disk, so re-adding the same `jobId` must NOT take the reuse path — that would re-insert the same deterministic id and collide on the `jobs.id` PRIMARY KEY (durable → throws `UNIQUE constraint failed: jobs.id`; buffered → the write buffer silently drops the insert, turning the intended no-op into a reject). `handleCustomId` instead returns the existing job (or, when the live `Job` is unreachable without `processingShards`, an `existingId` result) and inserts nothing (`src/application/operations/customId.ts:45-65`). This is the custom-id twin of the unique-key fix #69. The guard is `!ctx.completedJobs.has(id)` plus a `queue`/`processing` location check, so completed (#92) and DLQ jobs still fall through to reuse.
- **Custom-id reuse after completion (#92):** completed rows survive on disk; the reuse path evicts them so the recycled id does not falsely report `completed` or collide on the PK at flush (`src/application/operations/customId.ts:67-73`).
- **Custom-id reuse after DLQ:** a terminal DLQ generation is retired from the owning shard, `jobIndex`, its O(1) counter, and SQLite before the replacement jobs row is inserted. Both `PUSH` and `PUSHB` share this path, so the deterministic id has exactly one observable generation and the old entry cannot keep `failed` inflated or reappear after restart.
- **Orphan-row reconciliation via upsert:** a durable `jobs` row can outlive its in-memory tracking when the legacy fire-and-forget `obliterate()` races an in-flight durable insert, or when a write-buffer flush is reordered relative to that insert. (`obliterateAsync()` is the authoritative ordered API for new code.) The `customIdMap` / `jobIndex` entries may be cleared while the row survives. A later re-add of the same custom id finds no mapping → reuse path → insert. To stop the recycled id colliding on the PK (`UNIQUE constraint failed: jobs.id`), **both** insert statements use `ON CONFLICT(id) DO UPDATE` (upsert): the orphan row is overwritten in place at insert time. The `DO UPDATE SET` clause covers **all** non-id columns — including the per-execution fields `started_at` / `completed_at` / `progress` / `progress_msg` / `last_heartbeat` / `stacktrace`, which are absent from the INSERT column list so `excluded.<col>` resolves to their DEFAULT (the fresh-job value). Without resetting these, an upsert over a previously-completed orphan would leave a brand-new `waiting` job reporting `progress=100` / a stale `completed_at` / a prior life's `stacktrace`. A brand-new id is a plain INSERT with **zero** extra cost, so the customId hot path is not taxed (`statements.ts` `insertJob`, `sqliteBatch.ts` batch insert). In the buffered batch path this also prevents one collision from failing the whole flush and dropping every innocent job batched in the same window (`reportLostJobs`). `jobs` has no triggers/FKs, so `DO UPDATE` has no cascade (unlike `REPLACE`). Covered by `test/repro-idem-active-readd.test.ts`.
- **Recycled-id timeout marker (#33/#75):** stale `timedOutJobs` entries are cleared on reuse so the new job's stall-retry recovery is not silently dropped (`src/application/operations/customId.ts:101-107`).
- **`extend` with vanished owner:** throws `Duplicate unique_key (extended TTL)` rather than silently inserting (`src/application/operations/pushDeduplication.ts:81-94`).
- **Restart rehydration:** `recoverPendingJobs` re-populates `customIdMap` for pending jobs and re-registers unique keys via `registerUniqueKeyWithTtl(..., job.deduplicationTtl ?? undefined)` (`background/recovery/pending.ts`). `recoverCompletedJobs` deliberately does **not** add completed jobs to `customIdMap`, avoiding LRU eviction of pending mappings (`background/recovery/restore.ts`); custom-id collisions against completed jobs are caught by the storage fallback in `handleCustomId`.
- **Replace durability:** after a successful durable replacement the retired id
  resolves as unknown and cannot be restored by recovery. A restart sees only
  the final payload, including when multiple same-key replacements arrived in
  one batch.
- **Active replacement durability:** the active row remains recoverable but no
  longer persists the deduplication key. The successor row and ownership transfer
  are committed together, so restart before the predecessor acknowledgement
  restores the successor as the only key owner.
- **Dependent owners cannot be replaced:** a replacement throws
  `Cannot replace deduplicated job <id> because live jobs depend on it` while a
  live job consumes that id. The owner, incoming custom-id reservation and
  dependency topology remain unchanged; use an explicit flow migration instead
  of replacing a dependency node.
- **Post-commit cleanup errors:** releasing obsolete dependency-completion pins
  happens after the replacement commit. If that cleanup fails, the accepted
  single job or batch prefix is still persisted, counted, announced and
  notified before the cleanup error is surfaced; a cleanup exception cannot
  leave committed jobs invisible to accounting.
- **Memory bounds:** `customIdMap` is an `LRUMap` capped at `maxCustomIds` (default `50_000` in `application/types/config.ts`; initialized in `queue-manager/state.ts`). Unique-key registries are swept every cleanup tick (`cleanExpiredUniqueKeys`, `cleanupTasks.ts`) and any single-queue registry exceeding **1000** entries is force-trimmed by half via insertion-order iteration — under churn a still-live key can be evicted, weakening (not breaking) dedup. LRU eviction of `customIdMap` likewise allows a re-add to slip through.
- **Debounce is metadata-only (gotcha):** `debounce` sets
  `job.debounceId`/`job.debounceTtl` (`operations/add/single.ts:63-64`,
  `operations/add/bulk.ts:63-64`, `domain/job/create.ts:72-73`) but does **not**
  populate `uniqueKey`, so it does not itself suppress pushes —
  `handleDeduplication` returns early when `uniqueKey` is falsy. BullMQ-style
  debounce behavior is achieved via `deduplication` with `extend: true` (which
  is what `DedupDebounceMerger` emits as `deduplication.id`).
- **Stale-generation protection:** queue-level release resolves the current owner before deleting the key; job-level release compares the requested job id to that owner. If a replacement generation has acquired the same key, an old `Job` returns `false` and cannot clear either the in-memory registry or the replacement row's `unique_key` value.
- **Obliterate:** `Shard` clears the per-queue unique-key registry (`uniqueKeyManager.clearQueue`, `domain/queue/shard/lifecycle.ts`) and `QueueManagerControl.obliterate` removes the queue's owned custom-id mappings (`queue-manager/control.ts`).
- **Cron overlap:** `preventOverlap` derives the unique key `cron:<name>` (`infrastructure/scheduler/cron/execution.ts`); `removeOrphanedCronJob` releases it for stale waiting jobs on re-upsert (`queue-manager/services.ts`).

## Configuration

| Option / Env | Default | Effect |
| ------------ | ------- | ------ |
| `maxCustomIds` (QueueManager config, `application/types/config.ts`) | `50_000` | LRU cap for `customIdMap` |
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
