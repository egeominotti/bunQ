# Architecture Overview

bunqueue is a high-performance job queue for the [Bun](https://bun.sh) runtime. A
single process embeds **sharded in-memory priority queues** for hot-path job
movement, an **SQLite write-behind store** (WAL + MessagePack) for durability, and
two network transports — a binary **TCP** protocol on port `6789` and an **HTTP /
REST / SSE / WebSocket** API on port `6790`. The same engine can run fully
**in-process** (no sockets) behind a BullMQ-style SDK, drive a **workflow/saga
engine**, or expose itself to AI agents over a native **MCP server**.

The defining constraint is **zero external runtime infrastructure**: there is no
Redis, no broker, no companion service. Persistence is a local SQLite file; the
only npm dependencies are `croner` (cron parsing) and `msgpackr` (MessagePack)
([`package.json:74`](../package.json)). Everything else — hashing, heaps,
skip-lists, locks, the wire protocol, TLS — is built on Bun primitives.

A bare `bunqueue` invocation boots the full server; any other argv goes through the
CLI, which can itself boot the server (`start`) or act as a one-shot TCP client
([`src/main.ts:11`](../src/main.ts)). Both server paths funnel through one
`bootServer()` so they cannot drift
([`src/infrastructure/server/bootstrap.ts:73`](../src/infrastructure/server/bootstrap.ts)).

```
Producers ──add()──┐                          ┌──process()── Consumers
                   ▼                          ▼
            ┌──────────────────────────────────────┐
            │  Transports: embedded · TCP · HTTP    │
            └──────────────────┬───────────────────┘
                               ▼
                       ┌──────────────┐
                       │ QueueManager │  N shards + global indexes
                       └──────┬───────┘
                ┌─────────────┼──────────────┐
                ▼             ▼              ▼
          In-memory     SQLite (WAL)    Background tasks
          priority Qs   write-behind    (scheduler, stall,
                        + recovery       DLQ, cleanup, locks)
```

## Technology Stack & Rationale

| Choice | Where | Why |
| --- | --- | --- |
| **Bun runtime** (`>=1.3.9`) | [`package.json:147`](../package.json) | Native, fast TCP/TLS sockets (`Bun.listen`), bundled SQLite, `Bun.randomUUIDv7()` for time-ordered IDs, `Bun.s3` for backups, single-binary `bun build --compile`. The codebase is Bun-only and guards against Node at import (`src/require-bun.ts`, `src/bun-only.ts`). |
| **`bun:sqlite`** | [`src/infrastructure/persistence/sqlite.ts:7`](../src/infrastructure/persistence/sqlite.ts) | Embedded, zero-config, ACID durability with no separate process. WAL mode lets readers and the writer run concurrently. Avoids the operational weight of Redis/Postgres for a single-node queue. |
| **MessagePack** (`msgpackr`) | [`src/infrastructure/persistence/sqliteSerializer.ts`](../src/infrastructure/persistence/sqliteSerializer.ts) | ~2–3× faster + more compact than JSON for both the on-disk job blobs and the TCP wire format; preserves binary and numeric types losslessly. |
| **Native TCP + TLS** | [`src/infrastructure/server/tcp.ts`](../src/infrastructure/server/tcp.ts), [`src/config/resolve.ts:66`](../src/config/resolve.ts) | Length-prefixed binary frames over `Bun.listen` give ~100k+ ops/s without an HTTP/serialization tax. TLS is the same socket with `tls: { certFile, keyFile }`; partial cert/key fails fast at startup rather than silently serving plaintext. |
| **Zero external deps** | [`package.json:74`](../package.json) | Only `croner` + `msgpackr` ship at runtime; `@modelcontextprotocol/sdk` is an **optional** peer (only needed for the MCP binary). Smaller supply chain, trivial install, no version-skew between queue and broker. |
| **4-ary heaps / queue-local skip-lists** | [`src/shared/minHeap.ts:2`](../src/shared/minHeap.ts), [`src/domain/queue/priorityQueue.ts:56`](../src/domain/queue/priorityQueue.ts), [`src/domain/queue/temporalIndex.ts`](../src/domain/queue/temporalIndex.ts) | 4-ary branching improves cache locality vs binary heaps; one skip-list per queue orders cleanup candidates, with a reverse job-ID index for direct deletion. A compacting 4-ary min-heap tracks delayed jobs. |

## Layered Architecture

bunqueue follows a clean-architecture layering. Dependencies point **inward**:
`domain` depends on nothing; `application` depends on `domain`; `infrastructure`
and `client`/`cli`/`mcp` depend on `application` + `domain`; `shared` is depended
on by everyone.

```
shared  ←──────────── (utilities used by all layers)
  ▲
domain        Pure business logic — no I/O. Shard, IndexedPriorityQueue,
  ▲           DlqShard, UniqueKeyManager, TemporalManager, type definitions.
application   Use cases over the domain. QueueManager orchestrator +
  ▲           pure operation modules (push/pull/ack/query/control) invoked
  │           via context objects, plus DLQ/Events/Workers/Stats managers.
infrastructure  External edges: SQLite persistence, TCP/HTTP servers,
  ▲             cron scheduler, S3 backup, cloud agent. Drives application.
client · cli · mcp   Consumer-facing facades: SDK (Queue/Worker/Flow/
                     Workflow), CLI verbs, MCP tool surface.
```

- **`domain/`** — Pure, synchronous, side-effect-free. `Shard` composes
  `IndexedPriorityQueue` (waiting/delayed jobs), `DlqShard`, `UniqueKeyManager`
  (dedup), `LimiterManager` (rate/concurrency), `DependencyTracker`,
  `TemporalManager`/`TemporalIndex`, queue-scoped `WaiterManager`, `ShardCounters`
  ([`src/domain/queue/shard.ts`](../src/domain/queue/shard.ts)). Plus all type
  definitions in `domain/types/`. → [Data Structures](./features/data-structures.md),
  [Core Queue Engine](./features/core-queue-engine.md), [`./data-model.md`](./data-model.md).
- **`application/`** — The `QueueManager` central coordinator owns the shards,
  global indexes, and managers, and delegates every operation to pure modules
  through context objects built by `ContextFactory`
  ([`src/application/queueManager.ts:50`](../src/application/queueManager.ts),
  [`operations/`](../src/application/operations)). Active-job management claims
  are split into `jobMoveOperations.ts` (state/resource transitions) and
  `jobClaim.ts` (lease/client ownership cleanup). Houses DLQ, Events, Worker,
  JobLogs, Stats managers, the batch `QueueStatsAggregator`,
  [`DependencyResultTracker`](../src/application/dependencyResultTracker.ts) for
  live flow-result retention, and background-task wiring.
- **`infrastructure/`** — `SqliteStorage` (+ `WriteBuffer`, `BatchInsertManager`),
  `createTcpServer` / `createHttpServer`, `CronScheduler`, `S3BackupManager`,
  `CloudAgent`, plus `QueueCountsScheduler` for coalesced WS/SSE count updates. The `server/bootstrap.ts` is the single composition root.
- **`client/`** — In-process SDK: `Queue`, `Worker`, `SandboxedWorker`,
  `FlowProducer`, `QueueGroup`, `Bunqueue` (simple mode), the `Workflow`/`Engine`
  pair, and the TCP `TcpPool`/forwarder. Each transparently targets embedded or TCP.
- **`shared/`** — Cross-cutting primitives: `fnv1a`/`uuid`/`shardIndex`
  ([`src/shared/hash.ts`](../src/shared/hash.ts)), `RWLock`/`Semaphore`,
  `LRUMap`/`BoundedSet`/`BoundedMap`/`TtlMap`, `MinHeap`, `SkipList`, `Histogram`,
  `Logger`, `webhookValidation`.
- **`cli/`** — `bunqueue` executable: server boot detection + thin TCP client that
  maps verbs to protocol commands.
- **`mcp/`** — `bunqueue-mcp` binary exposing the queue to AI agents over MCP/stdio.

## Component Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                                  CLIENTS                                        │
│  Queue.add()/addBulk()  FlowProducer  Workflow Engine   Worker.process()        │
│  CLI verbs              MCP tools (bunqueue-mcp)                                 │
│        │                                                       ▲                 │
│        ▼  embedded? in-process call ─────────────────────────│                 │
│   ┌─────────┐                                            ┌─────────┐             │
│   │ TcpPool │◄────────── msgpack frames (TCP/TLS) ──────►│ TcpPool │             │
│   └────┬────┘                                            └────┬────┘             │
└────────┼─────────────────────────────────────────────────────┼──────────────────┘
         │ :6789 TCP                                            │
┌────────┼─────────────────────── SERVER ──────────────────────┼──────────────────┐
│        ▼                                                      ▼                  │
│  ┌───────────┐   handleCommand()   ┌───────────┐   :6790  ┌──────────────────┐   │
│  │ TcpServer │◄───── dispatch ────►│ QueueMgr  │◄────────►│ HttpServer       │   │
│  │ (frames,  │   (auth, routers,   │           │  REST    │ /jobs /stats     │   │
│  │ pipelining│    handlers)        │           │  SSE/WS  │ /metrics /health │   │
│  └───────────┘                     └─────┬─────┘          │ /dashboard       │   │
│                                          │                └──────────────────┘   │
│   ┌──────────────────────────────────────┼──────────────────────────────────┐   │
│   │                          QueueManager (orchestrator)                     │   │
│   │  ┌────────────────────────────────────────────────────────────────┐     │   │
│   │  │  N Shards (power of 2, auto from CPU cores, cap 64)             │     │   │
│   │  │  shardIndex(queue) = fnv1a(queue) & SHARD_MASK                  │     │   │
│   │  │  ┌─────────┬─────────┬───── ... ─────┬─────────┐                │     │   │
│   │  │  │ Shard 0 │ Shard 1 │               │ Shard N │  each:         │     │   │
│   │  │  │ PQueue  │ PQueue  │               │ PQueue  │  DLQ, dedup,   │     │   │
│   │  │  │ +RWLock │ +RWLock │               │ +RWLock │  limiter,deps  │     │   │
│   │  │  └─────────┴─────────┴───────────────┴─────────┘                │     │   │
│   │  │  processingShards[N] (active jobs)  + processingLocks[N]        │     │   │
│   │  └────────────────────────────────────────────────────────────────┘     │   │
│   │  Global indexes: jobIndex(Map) · completedJobs(BoundedSet) ·             │   │
│   │  jobResults(LRU) · customIdMap(LRU) · jobLogs(LRU) · jobLocks(Map)       │   │
│   └──────────────────────────────────┬───────────────────────────────────────┘   │
│                                       ▼                                          │
│   ┌────────────────────────────────────────────────────────────────────────┐   │
│   │  WriteBuffer (10ms / 100-job double-buffer) ─► SQLite (WAL, msgpack)     │   │
│   │  Recovery reads ◄─ jobs · job_results · dlq · cron_jobs · queue_state    │   │
│   └────────────────────────────────────────────────────────────────────────┘   │
│   ┌────────────────────────────────────────────────────────────────────────┐   │
│   │  Background tasks: CronScheduler · stall · lock-expiry · DLQ maint ·     │   │
│   │  dependency · cleanup/memory-bounds · monitoring · S3 backup · Cloud     │   │
│   └────────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
```

The TCP and HTTP servers are thin adapters: both decode a request into a command
and call the shared `handleCommand` dispatcher against the single `QueueManager`
instance created in `bootServer()`
([`src/infrastructure/server/bootstrap.ts:98`](../src/infrastructure/server/bootstrap.ts)).
See [TCP Server Handlers](./features/tcp-server-handlers.md) and
[HTTP / REST / SSE / WebSocket API](./features/http-api.md).

## Deployment Modes

**(a) Embedded (in-process).** `new Queue('q', { embedded: true, dataPath: './q.db' })`
instantiates a `QueueManager` inside the application process — no sockets, no
serialization. Lowest latency and the only mode where client-side stall detection
and DLQ config apply directly. Trade-off: scoped to one process; multiple
processes pointing at the same SQLite file is **not** supported for concurrent
writers.

**(b) Standalone server (TCP + HTTP).** `bunqueue start` (or bare `bunqueue`) boots
both transports, S3 backup, the cloud agent, the stats interval, and graceful
shutdown ([`bootstrap.ts:73`](../src/infrastructure/server/bootstrap.ts)). Many
clients connect over TCP `:6789` / HTTP `:6790`. Trade-off: a network hop and
msgpack encode/decode per op, but a single source of truth and full observability.

**(c) Distributed producer / consumer.** Separate processes run `Queue` (producers)
and `Worker` (consumers) against one standalone server over TCP. Workers register,
heartbeat, pull with leases, and ack/fail. Trade-off: horizontal worker scaling
and fault isolation; the central server remains the single coordination point.

**(d) Edge store-and-forward.** An embedded edge queue drains its local jobs to a
remote server via `embeddedQueue.forward({ to, queue })`
([`src/client/forwarder.ts`](../src/client/forwarder.ts)). Remote failures fall
back to local retry/DLQ, so nothing is lost; a deterministic remote jobId
`fwd:<queue>:<localId>` dedupes re-forwards within the server's custom-id
retention window. Trade-off: at-least-once delivery with bounded dedup; ideal for
IoT/edge that must tolerate intermittent connectivity. See
[Store-and-Forward](./features/store-and-forward.md).

## Request Data Flows

**PUSH** (`Queue.add`)
1. Client serializes the job and either calls `QueueManager.push()` directly
   (embedded) or sends a `PUSH`/`PUSHB` msgpack frame over the `TcpPool`.
2. TCP server decodes + authenticates the frame and dispatches to the push handler.
3. `QueueManager.push()` registers the queue name and delegates to `pushJob()`
   ([`queueManager.ts:299`](../src/application/queueManager.ts),
   [`operations/push.ts`](../src/application/operations/push.ts)).
4. `shardIndex(queue)` selects the shard; under its write lock the job is dedup-
   checked (custom id / unique key) and enqueued into the `IndexedPriorityQueue`
   (delayed jobs live there too, ordered by `runAt`, and are additionally
   tracked in the shard's temporal delayed min-heap).
5. The job is handed to the `WriteBuffer` (batched) — or written immediately when
   `durable: true` — and a `job:added` event is emitted. The new `Job` is returned.

**PULL** (`Worker` poll)
1. Worker requests work (`PULL`/`PULLB`, optionally with a lease/owner) for a queue.
2. Dispatch → `QueueManager.pull()` / `pullWithLock()`
   ([`queueManager.ts:309`](../src/application/queueManager.ts),
   [`operations/pull.ts`](../src/application/operations/pull.ts)).
3. Under the shard lock, the priority queue is scanned in priority order until
   the first ready job from an eligible FIFO group is found. Delayed or group-
   blocked entries are restored before returning, so an ineligible head cannot
   hide ready work from another group.
4. In the same synchronous critical section as the pop, the job is inserted into
   `processingShards[procIdx]` and its `jobIndex` entry flips to processing
   (state → `active`), so observers never see a stale location. Post-await
   bookkeeping (persist active state, counters, broadcast) runs in
   `finalizeProcessing` ([`operations/pull.ts:133`](../src/application/operations/pull.ts)),
   which skips delivery when a management op claimed the job in the meantime.
   A lock token is issued when leasing. Long-poll waits in a queue-specific
   `WaiterManager` bucket until a matching queue edge or the timeout; surplus
   notifications coalesce into one retry hint.
5. The job (and token) is returned; the worker registry counters update.

**ACK** (success)
1. Worker reports `ACK`/`ACKB` with the jobId, lock token, and optional result.
2. Dispatch → `ackJob()` / `ackJobBatch()`
   ([`operations/ack.ts`](../src/application/operations/ack.ts)).
3. The token is validated against `jobLocks`; a stale token (job already timed out
   / re-leased) is discarded so a retry is not skipped (`timedOutJobs` guard).
4. The job is removed from `processingShards`, added to `completedJobs`, its result
   stored in the `jobResults` LRU. The completed state and result are written
   directly to SQLite (`markCompleted`/`storeResult`, after flushing any pending
   buffered insert for that id), or the row is deleted on `removeOnComplete`;
   dependents are queued for resolution.
5. `job:completed` is emitted (events/webhooks/SSE/WS); repeat/cron successors and
   flow parents are scheduled.

**FAIL** (error)
1. Worker reports `FAIL` with jobId, token, and error message/stack.
2. Dispatch → `failJob()` ([`operations/ack.ts`](../src/application/operations/ack.ts)).
3. If `attempts < maxAttempts`, the job is re-enqueued with backoff delay (state
   `delayed`); otherwise it moves to the shard's **DLQ** with a `FailureReason`.
4. State + DLQ row are persisted; `job:failed` (and `job:dead` on exhaustion) is
   emitted. Parent-on-child-failure flow semantics may propagate the failure.

See [Job Lifecycle](./features/job-lifecycle.md) for the full state machine.

## Sharding Model

Queues are partitioned across `SHARD_COUNT` shards
([`src/shared/hash.ts`](../src/shared/hash.ts)). The count is **auto-detected** from
`navigator.hardwareConcurrency`: the next power of two ≥ core count, capped at 64,
defaulting to 4 if detection fails ([`hash.ts:28`](../src/shared/hash.ts)). Power of
two means the mask `SHARD_MASK = SHARD_COUNT - 1` ([`hash.ts:45`](../src/shared/hash.ts))
turns modulo into a single bitwise AND.

```ts
shardIndex(queue) = fnv1a(queue) & SHARD_MASK          // queue → shard
processingShardIndex(jobId) = fnv1a(jobId) & SHARD_MASK // active job → proc shard
```

`fnv1a` is a 32-bit FNV-1a hash ([`hash.ts:13`](../src/shared/hash.ts)) chosen for
speed and good string distribution. Each queue maps deterministically to one shard,
so all operations on a queue serialize through that shard's lock while different
queues run in parallel. **Active** jobs live in a separate `processingShards[N]`
map keyed by the job's UUIDv7 id (also FNV-hashed), decoupling consume-side
contention from the queue's own shard.

## Concurrency & Lock Hierarchy

bunqueue is single-process but highly concurrent (async I/O, many in-flight
commands). Consistency comes from per-shard `RWLock`s
([`src/shared/lock.ts`](../src/shared/lock.ts)) plus a strict acquisition order to
prevent deadlock. Locks must be acquired in this order:

```
1. jobIndex  →  2. completedJobs  →  3. shards[N]  →  4. processingShards[N]
```

The canonical pattern is **read the bare index first, then take the lock**:

```ts
const completed = completedJobs.has(id);     // cheap read, no lock
const shard = await shards[idx].acquire();    // RWLock write guard
try { /* mutate shard state */ } finally { shard.release(); }
```

`RWLock` supports multiple concurrent readers or one writer, with a
`LOCK_TIMEOUT_MS` (default 5000ms) bound so a stuck holder cannot wedge a shard
forever. `Semaphore` bounds worker concurrency. Job **leasing** issues a token on
pull; ACK/FAIL is rejected if the token no longer matches, which (with
`stalledCandidates` two-phase detection and `timedOutJobs`) safely handles workers
that die mid-job. See [Concurrency & Locking](./features/concurrency-and-locking.md).

## Persistence Model

`SqliteStorage` ([`src/infrastructure/persistence/sqlite.ts`](../src/infrastructure/persistence/sqlite.ts))
is created only when a `dataPath` is configured; otherwise bunqueue runs purely
in-memory. The database opens with performance PRAGMAs
([`schema.ts:7`](../src/infrastructure/persistence/schema.ts)):
`journal_mode = WAL`, `synchronous = NORMAL`, `cache_size = -64000` (64 MB),
`temp_store = MEMORY`, `mmap_size = 256 MB`, `busy_timeout = 5000`. WAL lets readers
proceed during the writer's flush.

- **Buffered (default).** Jobs flow into a `WriteBuffer` and are flushed in batches
  every **10 ms** or when **100** jobs accumulate
  ([`sqlite.ts:91`](../src/infrastructure/persistence/sqlite.ts)). The buffer is
  **double-buffered** — an `activeBuffer` keeps accepting writes while the
  `flushBuffer` is committed in one transaction, so writers never block on disk
  ([`sqliteBatch.ts:180`](../src/infrastructure/persistence/sqliteBatch.ts)).
- **Durable.** `add(..., { durable: true })` bypasses the buffer for an immediate
  synchronous write — zero data-loss window at lower throughput.
- **Resilience.** Flush failures retry with exponential backoff (100ms → 30s, 10
  tries); `SQLITE_FULL` flips a disk-full flag; permanent constraint violations
  (e.g. duplicate id) are dropped per-row without poisoning the batch, and
  exhausted batches surface via an `onCriticalLoss` callback.
- **Serialization.** Job payloads, results, and DLQ entries are stored as
  MessagePack blobs ([`sqliteSerializer.ts`](../src/infrastructure/persistence/sqliteSerializer.ts)).
- **Recovery.** On startup `bgTasks.recover()` batch-reads jobs, results, DLQ,
  cron, and queue control-state back into memory before serving traffic
  ([`queueManager.ts:202`](../src/application/queueManager.ts)).

Persisted tables: `jobs`, `job_results`, `dlq`, `cron_jobs`, `queue_state` (plus
the `migrations` bookkeeping table) — see
[Persistence](./features/persistence.md) and [`./data-model.md`](./data-model.md).

## Background Tasks

`startBackgroundTasks()` arms one timer per maintenance concern; intervals come
from `DEFAULT_CONFIG` ([`src/application/types.ts:33`](../src/application/types.ts),
[`src/application/backgroundTasks.ts:43`](../src/application/backgroundTasks.ts)).

| Task | Interval (default) | Purpose |
| --- | --- | --- |
| Cleanup + monitoring | `cleanupIntervalMs` = **10 s** | Enforce memory bounds, evict orphans, run monitoring checks |
| Job timeout sweep | `jobTimeoutCheckMs` = **5 s** | Fail/requeue jobs past their `timeout` |
| Stall check | `stallCheckMs` = **5 s** | Two-phase detection of unresponsive workers |
| Lock expiration | `stallCheckMs` = **5 s** | Reclaim leases whose token TTL elapsed |
| Dependency resolution | `dependencyCheckMs` = **30 s** (safety fallback) | Resolve flow/child deps; fast path is event-driven |
| DLQ maintenance | `dlqMaintenanceMs` = **60 s** | Age-based auto-purge + opt-in auto-retry |
| Cron scheduler | event-driven | Fire delayed/recurring jobs onto queues |
| S3 backup | `S3_BACKUP_INTERVAL` (6 h) | Gzip + checksummed snapshots, retention pruning |
| Stats log | `STATS_INTERVAL_MS` = **5 min** | Periodic queue/memory/worker stats line |

Memory bounds enforced by the bounded collections (cleanup evicts ~10% when full):

| Collection | Max | Eviction |
| --- | --- | --- |
| `completedJobs` | 50,000 | FIFO batch |
| `jobResults` | 10,000 | LRU |
| `jobLogs` | 10,000 | LRU |
| `customIdMap` | 50,000 | LRU |

See [Background Tasks](./features/background-tasks.md) and
[Scheduler & Cron](./features/scheduler-and-cron.md).

## Performance Characteristics

| Mode | Throughput (approx) | Data-loss window |
| --- | --- | --- |
| Buffered (default) | ~100k jobs/s | up to 10 ms (one flush interval) |
| Durable (`durable: true`) | ~10k jobs/s | none — synchronous write |
| Auto-batch over TCP (concurrent) | ~145k ops/s | none (same persistence as PUSH/PUSHB) |
| Auto-batch over TCP (sequential `await`) | ~10k ops/s | none |

Throughput comes from: 4-ary heaps (cache locality), per-shard parallelism, the
double-buffered write-behind path, UUIDv7 ids that sort by time (string compare,
no `localeCompare`), MessagePack framing, and transparent client-side add-batching
that coalesces concurrent `add()` calls into one `PUSHB` round-trip. The only
data-loss exposure is the ≤10 ms buffered window — eliminated per-job with
`durable: true`. Numbers are order-of-magnitude and hardware-dependent.

The cross-version `bench/fix-impact.ts` harness loads runtime modules from an
explicit source root and records raw samples plus correctness invariants. See
the [2026-07-16 core-fix benchmark](./benchmarks/fix-impact-2026-07-16.md) for
the current before/after methodology, results, and remaining hot paths.

## Reliability & Battle-Testing

The fast development loop runs targeted tests natively. The authoritative local
gate, `bun run test:sandbox`, builds the current worktree once and runs unit,
TCP, and embedded suites in three disposable non-root containers without host
mounts or external networking. CI provides an equivalent fresh VM per suite.
The companion `bun run test:sandbox:sdk` gate builds six language-specific
images and runs TypeScript, Python, PHP, Go, Rust, and Elixir
native/conformance suites with the same containment and telemetry format.
Those suites include independent-connection lease/idempotency races,
fixed-seed property corpora, malformed-input fuzz corpora, bounded spikes,
and durable SIGKILL/restart recovery. Go additionally runs its race detector.
Long-lived soak/stress profiles and live dependency-advisory checks are weekly
CI jobs because the local release gate is bounded and networkless.
The unit suite also runs a
[model-based state machine](./features/model-based-testing.md) against a fresh
real TCP broker and SQLite database per property run. Generated lifecycle,
batch, dependency, limiter, DLQ, and queue-control histories are checked after
every command at the API, aggregate-count, lock-token, and physical-storage
layers; histories can include an actual `SIGKILL` and are shrunk to a replayable
minimal counterexample.
The TCP functional runner adds a second boundary: every test file receives a
new server, dynamic ports, and a unique SQLite directory that is removed in
`finally`. See [Test Isolation and Reproducibility](./testing.md) for the threat
model, commands, cleanup behavior, and native-only benchmark policy.

Beyond the functional unit/integration suites, a set of adversarial "24/7
readiness" suites under `test/repro-*.test.ts` assert the delivery and resource
guarantees a continuously-running deployment depends on. Each drives a real
`QueueManager` + `createTcpServer` (several spawn the real `src/main.ts` process
against on-disk SQLite) and asserts hard invariants — not just "it ran".

| Category | File | What it proves |
| --- | --- | --- |
| Model-based state machine | `model-based/queue-model` | Generated valid command histories agree with a compact specification across API state, counts, SQLite/DLQ membership, payload/priority persistence, leases, dependencies, limiters, and `SIGKILL` recovery; failures shrink and replay by seed. |
| Protocol fuzzing | `repro-fuzz-protocol` | Malformed frames, corrupt MessagePack, lying length prefixes, >64 MB frames, torn/coalesced frames, and pre-auth commands never crash or wedge the server; the pre-auth gate never leaks. |
| Chaos / fault injection | `repro-chaos-fault-injection` | At-least-once redelivery when a worker dies mid-job; a heartbeated-then-dropped job is not double-dispatched but is reclaimed by lock-expiry; lock-expiry under contention loses nothing; cron next-run is monotonic under clock skew/DST. |
| Race / concurrency | `repro-race-concurrency` | N concurrent PULLs on one job → exactly one delivery; concurrent same-`jobId` PUSH → exactly one job; active re-add is an idempotent skip (no UNIQUE crash); cancel-during-active is a safe no-op; stale ACK vs lock-expiry never double-completes; K-workers×M-jobs drain processes each exactly once. |
| Crash-recovery | `repro-chaos-crash-recovery` | Under `SIGKILL` (no flush): durable jobs are never lost, ACKed durable jobs stay completed, paused-state and DLQ entries persist, an active-at-crash job is recovered (attempts incremented), and multi-cycle crash fuzzing loses nothing cumulatively. |
| Soak / endurance | `repro-chaos-soak` | Sustained produce/consume with a worker socket killed every ~400 ms: no job lost (server-authoritative), p99 latency does not drift, WAL stays bounded, and internal collections return to baseline after drain (no per-job/per-connection leak). Env-tunable (`SOAK_MS`) for real multi-hour runs. |
| Stress / degradation | `repro-stress-degradation` | A huge backlog stays bounded and responsive then drains; 100 slowloris connections are all terminated by the stall bound while a healthy client stays fast; >50 in-flight pipelined commands on one socket all complete; latency returns to baseline after a load spike. |
| Upgrade / rolling restart | `repro-upgrade-restart` | Graceful `SIGTERM` flushes the write buffer so even buffered (non-durable) jobs survive; waiting/completed(+result)/paused/DLQ state all round-trip a restart; rolling restarts under load lose nothing cumulatively. |
| Long-running semantics | `repro-longrunning-semantics` | Cron next-run does not drift across thousands of ticks (incl. DST); `jobResults` and the custom-id dedup map stay bounded under pressure (oldest evicted, newest retained); the DLQ is bounded to `maxEntries` and remains retryable. |

## Module Map

### Engineering tooling
- [Test Isolation and Reproducibility](./testing.md) — Pinned test image, parallel disposable unit/TCP/embedded containers, per-file TCP server and SQLite isolation, container resource time series, per-test/file timing KPIs, anomaly reports, CI equivalence, cleanup guarantees, and native-only benchmarks.
- [Model-Based Queue Verification](./features/model-based-testing.md) — `fast-check` command model against a real broker and SQLite, with layered invariants, shrinking, deterministic replay, dependency flows, limiters, and crash recovery.

### Core engine & data structures
- [Core Queue Engine (QueueManager & Shards)](./features/core-queue-engine.md) — Central coordinator that shards queues, owns global job indexes, and orchestrates all job operations by delegating to operation modules via context objects.
- [Data Structures (PriorityQueue, heaps, maps)](./features/data-structures.md) — Generic, dependency-free in-memory building blocks: an indexed 4-ary priority heap for queued jobs, a skip-list temporal cleanup index plus a 4-ary min-heap tracking delayed jobs, and bounded/LRU/TTL containers plus a latency histogram.
- [Concurrency & Locking](./features/concurrency-and-locking.md) — In-process synchronization primitives (RWLock, Semaphore) plus job-leasing and stall detection that keep bunqueue's sharded state consistent under concurrent access.

### Job operations
- [Job Lifecycle (push / pull / ack / fail)](./features/job-lifecycle.md) — The four primitive pure-logic operations (push, pull, ack, fail) that move a job through its state machine beneath the TCP/HTTP servers and embedded SDK.
- [Job Queries & Queue Control](./features/job-queries-and-control.md) — Read/control surface of QueueManager: point/list job queries, single-job mutations, and queue-wide lifecycle operations as pure context-driven functions.
- [Dead Letter Queue (DLQ)](./features/dead-letter-queue.md) — Terminal sink for jobs that exhausted retries/stalled/lost their lock, with inspect/filter/retry/purge plus opt-in time-based auto-retry and age-based auto-purge.
- [Deduplication & Unique Jobs](./features/deduplication-and-unique.md) — Prevents duplicate jobs via custom job-ID idempotency and TTL-scoped unique keys with reject/extend/replace strategies, checked atomically inside the shard write lock.
- [Rate Limiting & Concurrency Control](./features/rate-limiting-and-concurrency.md) — Per-queue rate limits and concurrency caps, enforced server-side and honored by workers, via the `RateLimit`/`RateLimitClear`/`SetConcurrency`/`ClearConcurrency` commands.

### Persistence & scheduling
- [Persistence (SQLite, WriteBuffer, recovery)](./features/persistence.md) — Durable SQLite-backed store (WAL + msgpack + buffered/double-buffered WriteBuffer) that persists jobs, results, DLQ, cron, and queue control-state and serves batched recovery reads on restart.
- [Scheduler & Cron](./features/scheduler-and-cron.md) — Event-driven server engine that fires recurring cron/interval jobs onto queues, persisting next-run/execution state for crash-safe at-most-once-per-slot scheduling.
- [Background Tasks](./features/background-tasks.md) — Periodic server-side maintenance: timers for timeouts, stall/lock recovery, DLQ upkeep, dependency resolution, memory-bound cleanup, monitoring, plus startup recovery from SQLite.

### Orchestration
- [FlowProducer & Job Dependencies](./features/flow-producer.md) — Client-side API for building parent/child job trees and dependency chains spanning queues, with BullMQ v5 compatibility.
- [Workflow Engine (saga orchestration)](./features/workflow-engine.md) — Multi-step saga orchestration built on a bunqueue Queue/Worker pair: a typed DSL of nodes driven one-node-per-job, with retries, parallelism, signals, loops, sub-workflows, SQLite-persisted state, and reverse-order compensation.

### Transports & protocol
- [TCP Wire Protocol & Framing](./features/tcp-protocol.md) — Binary length-prefixed MessagePack transport that frames, pipelines, and backpressure-manages all TCP client/server commands and responses.
- [TCP Server Command Handlers](./features/tcp-server-handlers.md) — Request-handling layer that authenticates decoded TCP commands, dispatches them through category routers to thin handler adapters, and shapes QueueManager results into typed responses; also wires the full server in bootstrap.
- [HTTP / REST / SSE / WebSocket API](./features/http-api.md) — HTTP transport (port 6790) exposing the queue control surface as REST plus SSE/WebSocket real-time event streams, diagnostics, and metrics — all adapting requests onto the shared handleCommand dispatcher.
- [Security: TLS, Auth, CORS](./features/security-tls-auth.md) — Transport TLS (TCP+HTTP), bearer-token auth on both transports, CORS, webhook SSRF validation, and HMAC signing for webhooks and the Cloud uplink.

### Client SDK
- [Client Transport (TCP pool, reconnect, batching)](./features/client-transport.md) — Wire-level TCP transport (pool, pipelining, reconnect, health, TLS, add-batching) used by the Queue and Worker SDKs.
- [Client SDK: Queue](./features/client-queue-sdk.md) — Producer-side BullMQ-style Queue<T> SDK that transparently drives embedded (in-process QueueManager) and TCP (msgpack-over-pool) backends.
- [Client SDK: Worker (& sandboxed)](./features/client-worker-sdk.md) — BullMQ-style consumer worker that pulls jobs (embedded or TCP), runs the user processor, and reports ack/fail — plus a process-isolated SandboxedWorker variant.
- [Polyglot SDK Quality Contract](./features/polyglot-sdks.md) — Shared production invariants, telemetry contract, native regression coverage, and isolated six-language release gate.
- [Rust Client SDK](./features/rust-sdk.md) — Standalone synchronous Queue, bounded-thread Worker, FlowProducer, rustls transport, and shared conformance driver.
- [Elixir Client SDK](./features/elixir-sdk.md) — OTP-native Queue, concurrent Worker, FlowProducer, verified TLS transport, structured telemetry, and shared conformance driver.
- [Simple Mode (Bunqueue all-in-one)](./features/simple-mode.md) — Thin all-in-one wrapper pairing a Queue and Worker with opt-in conveniences (routes, middleware, retry, circuit breaker, batching, aging, TTL, triggers, dedup, DLQ).
- [Store-and-Forward & BullMQ Compatibility](./features/store-and-forward.md) — Client-side edge store-and-forward (drain a local queue to a remote bunqueue server, idempotently) plus partial BullMQ v5 read-API shims on Queue.

### Observability & operations
- [Webhooks, Events & Job Logs](./features/webhooks-and-events.md) — Server-side observability: outbound HTTP webhooks, in-process event pub/sub, bounded per-job logs, and client-job ownership/disconnect release.
- [Worker Registry & Management](./features/workers-management.md) — Server-side in-memory registry of connected workers tracking liveness, queues, concurrency, and per-worker job counters; backs skipIfNoWorker crons and dashboard/HTTP/CLI worker visibility.
- [Stats, Metrics & Monitoring](./features/stats-and-monitoring.md) — Read-only aggregation of queue depth counts, cumulative counters, memory sizes, EMA throughput rates, and latency histograms, exposed via TCP Stats/Metrics/Prometheus/Ping, HTTP /stats /metrics /prometheus /health /dashboard, and the periodic stats log.
- [S3 Backup](./features/backup-s3.md) — Periodic gzip-compressed, SHA-256-checksummed SQLite snapshots to S3-compatible storage with retention pruning and validate-before-replace restore.
- [bunqueue Cloud Dashboard Integration](./features/cloud-integration.md) — Opt-in agent that pushes full server telemetry snapshots to the bunqueue.io dashboard over HTTP and receives whitelisted remote commands over WebSocket.

### Interfaces & configuration
- [Native MCP Server](./features/mcp-server.md) — Exposes bunqueue to AI agents over MCP/stdio via the bunqueue-mcp binary, registering 73 tools, 5 resources, and 3 prompts backed by either an embedded QueueManager or a remote TCP server.
- [CLI](./features/cli.md) — The bunqueue executable: boots the server or acts as a thin one-shot TCP client that maps CLI verbs to msgpack protocol commands and renders responses.
- [Configuration & Entrypoint](./features/configuration.md) — Config layer and process entrypoint: resolves config-file/env/default precedence into typed config, dispatches the bunqueue executable, and provides the Logger, VERSION, and Bun-only runtime guards.

See also the data dictionary in [`./data-model.md`](./data-model.md).
