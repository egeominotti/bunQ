# Data Structures (PriorityQueue, heaps, maps)

> **Category:** Engine · **Source:** `src/domain/queue/priorityQueue.ts`, `src/domain/queue/temporalManager.ts`, `src/domain/queue/temporalIndex.ts`, `src/shared/minHeap.ts`, `src/shared/skipList.ts`, `src/shared/lruMap.ts`, `src/shared/lruSet.ts`, `src/shared/boundedMap.ts`, `src/shared/boundedSet.ts`, `src/shared/ttlMap.ts`, `src/shared/histogram.ts`, `src/shared/lru.ts`

## Purpose

This module provides the in-memory data structures the engine is built on: an indexed 4-ary priority heap for ready jobs, a skip-list + min-heap based temporal index for delayed/cleanup ordering, and a family of bounded/LRU/TTL containers used for memory-capped metadata (results, custom IDs, logs, completed-job tracking). These are generic, dependency-free building blocks (no SQLite, no I/O) consumed by [Core Queue Engine](./core-queue-engine.md) shards, the [Scheduler & Cron](./scheduler-and-cron.md), and [Stats, Metrics & Monitoring](./stats-and-monitoring.md). They exist to keep hot-path operations at O(log n) or better while enforcing hard memory bounds on every unbounded collection.

## Responsibilities & Scope

Owns:
- `IndexedPriorityQueue` — the per-(shard, queue) ready-job ordering and O(1) lookup-by-id index.
- `TemporalManager` — the `createdAt`-ordered cleanup index and the `runAt`-ordered delayed-job tracker (min-heap + sets).
- `TemporalIndex` — one skip list per queue plus a reverse job-ID map for queue-local range scans and direct removal.
- `MinHeap<T>` — generic 4-ary min-heap used by `TemporalManager`, `TTLMap`, and `CronScheduler`.
- `SkipList<T>` — generic probabilistic sorted multiset/set with range queries.
- `LRUMap` / `LRUSet` / `BoundedMap` / `BoundedSet` / `TTLMap` — eviction-bounded containers.
- `Histogram` — fixed-bucket latency histogram for Prometheus output.

Does NOT own:
- Shard locking, job state transitions, or which job to pull — see [Core Queue Engine](./core-queue-engine.md) and [Job Lifecycle](./job-lifecycle.md).
- Persistence of any of these structures; they are pure in-memory. Durability is [Persistence](./persistence.md).
- Cron scheduling logic (only lends `MinHeap` to it) — see [Scheduler & Cron](./scheduler-and-cron.md).
- Unique-key/dedup semantics — see [Deduplication & Unique Jobs](./deduplication-and-unique.md).

## Dependencies

Internal:
- `IndexedPriorityQueue` and `TemporalManager` depend only on `src/domain/types/job` (`Job`, `JobId`).
- `TemporalManager` depends on `MinHeap` and `TemporalIndex`; `TemporalIndex` depends on `SkipList`.
- `TTLMap` depends on `MinHeap`.
- `BoundedMap` reuses the `MapLike` interface from `lruMap`; `BoundedSet` reuses `SetLike` from `lruSet`.
- `src/shared/lru.ts` is the barrel re-exporting `LRUMap`, `LRUSet`, `BoundedMap`, `BoundedSet`, `TTLMap`.

External/runtime: none beyond standard JS `Map`/`Set`, `Math.random` (skip-list level promotion), `setInterval` (`TTLMap` cleanup), and `Float64Array` (`Histogram` bucket counts). Zero third-party dependencies.

## Public Interface

### `IndexedPriorityQueue` (`src/domain/queue/priorityQueue.ts`)
Backed by a 4-ary heap (`D = 4`) of lightweight `HeapEntry` records plus a `Map<JobId, { job, generation }>` index.
- `get size: number`, `get isEmpty: boolean`
- `push(job: Job): void` — O(log₄ n)
- `pop(): Job | null` — pops highest-priority job, skipping stale heap entries
- `peek(): Job | null`
- `find(jobId: JobId): Job | null` — O(1)
- `has(jobId: JobId): boolean` — O(1)
- `remove(jobId: JobId): Job | null` — O(1); deletes from index only, heap entry left stale
- `updatePriority(jobId, newPriority, newLifo?): boolean` — O(log n)
- `updateRunAt(jobId, newRunAt): boolean` — O(log n)
- `values(): Job[]`, `clear(): void`
- `getStaleRatio(): number`, `needsCompaction(threshold = 0.2): boolean`, `compact(): void`

### `TemporalManager` (`src/domain/queue/temporalManager.ts`)
- `addToIndex(createdAt, jobId, queue): void` — O(log n)
- `getOldJobs(queue, thresholdMs, limit): Array<{ jobId, createdAt }>` — O(log n + k)
- `removeFromIndex(jobId): void`, `clearIndexForQueue(queue): void`
- `cleanOrphaned(validJobIds: Set<JobId>): number`
- `isDelayed(jobId): boolean`, `addDelayed(jobId, runAt): void`, `removeDelayed(jobId): boolean`
- `refreshDelayed(now: number): number` — O(k), returns count that became ready
- `get indexSize`, `get delayedCount`, `clearDelayed()`, `clear()`, `getSizes()`
- Exported type: `interface TemporalEntry { createdAt: number; jobId: JobId; queue: string }`

### `TemporalIndex` (`src/domain/queue/temporalIndex.ts`)
- `add(createdAt, jobId, queue): void`
- `getOldJobs(queue, threshold, limit): Array<{ jobId, createdAt }>` — O(log q + k), where `q` is that queue's index size
- `remove(jobId): void` — reverse lookup plus O(log q) skip-list deletion
- `clearQueue(queue): void`, `cleanOrphaned(validJobIds): number`, `clear(): void`
- `get size: number`

### `MinHeap<T>` (`src/shared/minHeap.ts`)
- `constructor(compare: (a: T, b: T) => number)`
- `get size`, `get isEmpty`, `push(item)`, `pop(): T | undefined`, `peek(): T | undefined`
- `clear()`, `toArray(): T[]`, `buildFrom(items: T[])` — O(n) heapify, `removeWhere(predicate): T | undefined` — O(n)

### `SkipList<T>` (`src/shared/skipList.ts`)
- `constructor(compare, maxLevel = 16, probability = 0.5, equals?)`
- `insert(value): boolean`, `delete(value): boolean`, `deleteWhere(predicate): T | null`
- `find(value): T | null`, `has(value): boolean`, `first(): T | null`, `shift(): T | null`
- `rangeUntil(maxValue, limit?): T[]` — O(log n + k), `takeWhile(predicate, limit?): T[]`
- `values(): Generator<T>`, `toArray(): T[]`, `clear()`, `removeAll(predicate): T[]`
- `get size`, `get isEmpty`

### Eviction-bounded containers (`src/shared/`)
- `LRUMap<K,V>` implements `MapLike<K,V>`; `constructor(maxSize, onEvict?)`; doubly-linked list, O(1) `get`/`set` with move-to-front; evicts tail (least recent).
- `LRUSet<T>` implements `SetLike<T>`; same eviction model for membership.
- `BoundedMap<K,V>` / `BoundedSet<T>` — FIFO batch eviction, no recency tracking; `evictBatchSize = max(1, floor(maxSize * 0.1))`.
- `TTLMap<K,V>` — `constructor(ttlMs, cleanupIntervalMs = 60_000)`; `set(key, value, ttlMs?)`, `get`, `has`, `delete`, `clear`, `stop()`; exposes `heapSize`, `staleEntryCount`. **Must call `stop()`** to clear the cleanup interval.
- `MapLike<K,V>` and `SetLike<T>` interfaces are exported for substitutability.

### `Histogram` (`src/shared/histogram.ts`)
- `constructor(buckets = DEFAULT_BUCKETS)` — buckets sorted ascending, `+Inf` bucket appended.
- `observe(value): void` — O(log b) bucket find + cumulative increment
- `getSum()`, `getCount()`, `percentile(p)`, `toPrometheus(name, help): string`, `reset()`

This module exposes no TCP commands, HTTP endpoints, CLI commands, or events directly; it is consumed by higher layers that do.

## Data Models

`Job` (`src/domain/types/job.ts:81`) is the payload stored in `IndexedPriorityQueue`; the ordering reads `priority` (`:85`), `lifo` (`:87`), `runAt` (`:90`), and `id` (`JobId`, a branded `string`, `:7`). Full definition in [data-model](../data-model.md).

Internal shapes:
- `HeapEntry { jobId, priority, runAt, lifo, generation: bigint }` — lightweight metadata held in the heap array; the full `Job` lives only in the index map.
- `TemporalEntry { createdAt, jobId, queue }` — skip-list node value, ordered by `(createdAt, jobId)`.
- `DelayedEntry { jobId, runAt }` — `MinHeap` entry for delayed jobs.

## Business Logic / Control Flow

### Priority ordering (`compareEntries`, `priorityQueue.ts:27`)
1. Higher `priority` first (`b.priority - a.priority`).
2. Tie-break by mode: if both `lifo`, newer job first via descending `jobId` string compare (`:35`-`:41`) — valid because default IDs are UUIDv7 (lexicographically time-ordered). For FIFO/mixed, earlier `runAt` first (`:44`), then older `jobId` ascending (`:50`). Direct string comparison is used instead of `localeCompare` (UUIDv7 is ASCII).

### Lazy invalidation via generations (`priorityQueue.ts`)
Each `push`/`updatePriority`/`updateRunAt` assigns a monotonically increasing `bigint` generation and stores it in both the index entry and a new heap entry. `pop`/`peek` (`:99`, `:119`) compare the top heap entry's generation against the index entry; on mismatch (`:105`) the entry is dropped via `removeTop()` and the scan continues. Consequences:
- `remove` (`:146`) and updates do **not** touch the heap — they only mutate the index, so the heap accumulates stale entries.
- `compact()` (`:244`) filters out stale entries and rebuilds via O(n) `heapify` (`:262`); `needsCompaction(threshold)` returns `getStaleRatio() > threshold`. Background tasks trigger this: `cleanupTasks.ts:27` uses threshold `0.2`, `statsManager.ts:309` uses `0.1`.
- `generation` is `bigint`, deliberately overflow-proof at extreme throughput.

One `IndexedPriorityQueue` is created lazily per queue name inside a shard (`shard.ts:97`-`:100`); `push`/`pop`/`find`/`remove` are driven by `src/application/operations/*` (e.g. `push.ts:208`, `ack.ts:235`).

### Temporal index & delayed refresh
- `TemporalIndex` owns a `SkipList<TemporalEntry>` per queue, ordered by the total key `createdAt → jobId`. A reverse `Map<JobId, TemporalEntry[]>` locates removals without scanning unrelated queues. `getOldJobs` starts directly in the requested queue's list and stops at the age threshold, preserving O(log q + k) multi-queue cleanup.
- Delayed jobs are tracked in three structures kept in sync: `delayedJobIds: Set`, `delayedHeap: MinHeap<DelayedEntry>` (by `runAt`), and `delayedRunAt: Map<JobId, number>` (current `runAt`). `refreshDelayed(now)` pops due entries and verifies `delayedRunAt` to reject stale removals or old deadlines.
- Delayed removal remains lazy, but `maybeCompactDelayedHeap` bounds retained stale entries. It clears immediately when no delayed jobs remain; otherwise, once at least 256 entries are stale and stale entries are at least as numerous as live entries, it rebuilds in O(n) with `MinHeap.buildFrom()`.

### Heaps
`MinHeap` and `IndexedPriorityQueue` are both 4-ary (`D = 4`): parent at `floor((idx-1)/4)`, children at `4*idx+1 .. 4*idx+4`. 4-ary is chosen for cache locality (children contiguous, fewer levels). `bubbleDown` scans up to 4 children sequentially picking the smallest (`minHeap.ts:114`). `MinHeap` backs `TemporalManager.delayedHeap`, `TTLMap.expiryHeap` (`ttlMap.ts:42`), and `CronScheduler.cronHeap` (`cronScheduler.ts:51`).

### Skip list
Probabilistic levels via `randomLevel()` (`skipList.ts:74`), `maxLevel = 16`, `probability = 0.5`. `insert` (`:89`) finds position per level, and when an `equals` fn is supplied scans the run of compare-equal nodes for a true duplicate (`:107`-`:116`), returning `false` if found. `delete` (`:156`) unlinks at each level then lowers `level`. Range/scan helpers (`rangeUntil`, `takeWhile`, `values`) walk level-0 forward pointers.

### Eviction containers
- `LRUMap.set` (`lruMap.ts:85`): existing key updates value + `moveToFront`; new key evicts `tail` (least recent) when `size >= maxSize`, firing `onEvict`, then `addToFront`. Iteration is tail→head (oldest→newest) to mimic native `Map` order.
- `BoundedMap`/`BoundedSet`: existing key is a fast no-op/update; at capacity, `evictBatch` removes the oldest 10% in one pass (`boundedMap.ts:44`, `boundedSet.ts:37`) to amortize iterator cost.
- `TTLMap`: `set` stamps `expiresAt = now + ttl` and pushes onto `expiryHeap`; updating an existing key increments `staleCount` (`ttlMap.ts:143`). `get` (`:126`) lazily evicts expired keys. `cleanup` (`:75`) pops the heap while `expiresAt <= now`, verifying the cache entry's `expiresAt` still matches before deleting (stale entries just decrement `staleCount`). `maybeCompact` (`:105`) rebuilds the heap once `staleCount / heapSize > 0.5` and `heapSize >= 100`.

### Histogram
`observe` (`histogram.ts:22`) binary-searches the bucket boundary then increments every bucket `>= value` (cumulative semantics). `percentile(p)` returns the first bucket whose cumulative count reaches `(p/100)*count`. `toPrometheus` emits `_bucket{le=...}`, `_sum`, `_count` lines including the `+Inf` bucket. Consumed by `LatencyTracker` (`latencyTracker.ts:10`-`:12`) for `bunqueue_push/pull/ack_duration_ms`.

## Concurrency & Locking

These structures are **not internally synchronized**. Bun/JS is single-threaded per event loop, and concurrency safety is provided by the caller: each `IndexedPriorityQueue`/`TemporalManager` instance lives inside a shard and is only touched while that shard's async lock is held — lock order `jobIndex → completedJobs → shards[N] → processingShards[N]`. See [Concurrency & Locking](./concurrency-and-locking.md). The only timer-driven mutation is `TTLMap`'s `setInterval` cleanup, which runs synchronously on the event loop. No leases or heartbeats live here.

## Edge Cases & Failure Modes

- **Stale heap entries (priority queue):** `remove`/`update` invalidate via generation rather than splicing, so the heap can grow beyond `size`. If `compact()` is never called the heap leaks memory; background cleanup/stats call `needsCompaction(0.2)`/`(0.1)` then `compact()` to bound it.
- **Generation overflow:** uses `bigint`, so it cannot overflow even at sustained extreme throughput.
- **Delayed-job staleness:** dead `DelayedEntry` records are rejected against `delayedRunAt` and periodically rebuilt; heap memory is therefore proportional to live delayed work plus a bounded stale threshold, not lifetime churn.
- **Same job ID at multiple timestamps:** the reverse map stores an entry array. `remove(jobId)` removes its earliest temporal entry, preserving the previous one-at-a-time semantics when tests or recovery insert multiple timestamps for one ID.
- **Queue-local clear:** `clearQueue` detaches reverse-map entries while deleting the queue skip list; `cleanOrphaned` removes every entry for IDs absent from the supplied valid set.
- **`TTLMap` interval leak:** the cleanup `setInterval` keeps the instance alive; failing to call `stop()` leaks memory (documented invariant at the top of `ttlMap.ts`).
- **Memory bounds** are enforced by capacity-constructed containers in `QueueManager` (`queueManager.ts:151`-`:163`): `completedJobsData` (`BoundedMap`), `completedJobs`/`depCompletions`/`timedOutJobs` (`BoundedSet`), `jobResults`/`customIdMap`/`jobLogs`/`perQueueMetrics` (`LRUMap`). Eviction is silent (LRU drops oldest-touched; Bounded drops oldest-inserted 10% batch).
- **`Histogram.percentile`** returns `0` for an empty histogram and clamps to the largest finite bucket for high percentiles; it reports bucket boundaries, not interpolated values.
- **Unused-in-`src` containers:** `LRUSet` and `TTLMap` are exported and unit-tested but currently have no production call site in `src/` (only `test/lru.test.ts`); they remain part of the public toolkit.

## Configuration

These structures take sizes as constructor arguments; defaults that bind them come from `DEFAULT_CONFIG` (`src/application/types.ts:33`):

| Collection (consumer) | Container | Default cap | Source |
| --- | --- | --- | --- |
| `completedJobs`, `completedJobsData`, `depCompletions`, `timedOutJobs` | `BoundedSet`/`BoundedMap` | `maxCompletedJobs = 50_000` | `types.ts:34` |
| `jobResults` | `LRUMap` | `maxJobResults = 10_000` | `types.ts:35` |
| `jobLogs` | `LRUMap` | `maxJobLogs = 10_000` | `types.ts:36` |
| `customIdMap` | `LRUMap` | `maxCustomIds = 50_000` | `types.ts:37` |

Other tunables: `SkipList` `maxLevel = 16`, `probability = 0.5`; `MinHeap`/`IndexedPriorityQueue` branching `D = 4`; delayed-heap compaction minimum `256` stale entries with `stale >= live`; `TTLMap` `cleanupIntervalMs = 60_000`, compaction `threshold = 0.5` / `minSize = 100`; priority-queue compaction thresholds `0.2` (cleanup) and `0.1` (stats); `Histogram` `DEFAULT_BUCKETS` `[0.1, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]` ms. No environment variables affect this module directly.

> Note: the memory-bounds table in the project README lists `jobResults` at 5,000; the code default is `10,000` (`types.ts:35`). The code value is authoritative.

## Related Docs

- [Core Queue Engine](./core-queue-engine.md) — shards that own `IndexedPriorityQueue` + `TemporalManager` instances
- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md) — operations driving heap push/pop
- [Concurrency & Locking](./concurrency-and-locking.md) — the locks protecting these non-synchronized structures
- [Background Tasks](./background-tasks.md) — compaction, delayed refresh, and cleanup scheduling
- [Scheduler & Cron](./scheduler-and-cron.md) — `MinHeap`-backed cron ordering
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md) — `Histogram`/`LatencyTracker` consumers
- [Persistence (SQLite, WriteBuffer, ReadThrough)](./persistence.md) — durability layer behind these in-memory structures
- [architecture](../architecture.md) · [data-model](../data-model.md)
