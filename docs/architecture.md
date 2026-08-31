# Architecture Overview

bunqueue is a high-performance job queue for the [Bun](https://bun.sh) runtime.
Its default engine embeds **sharded in-memory priority queues** for hot-path job
movement and an **SQLite write-behind store** (WAL + MessagePack) for durability.
An optional server-only **PostgreSQL 15–18** engine makes the database
authoritative and coordinates multiple brokers with transactions and fenced
leases; 18.6 is pinned and recommended. Both server engines expose a binary **TCP** protocol on port `6789` and
an **HTTP / REST / SSE / WebSocket** API on port `6790`. The SQLite engine can
also run fully **in-process** (no sockets) behind a BullMQ-style SDK, drive a
**workflow/saga engine**, or expose itself to AI agents over a native **MCP
server**.

Zero external runtime infrastructure remains the default: memory/SQLite needs no
Redis, database server, broker, or companion service. Multi-broker mode opts into
PostgreSQL 15–18. The only npm runtime dependency is `msgpackr` (MessagePack);
cron parsing and PostgreSQL access use Bun's built-in `cron` and `SQL` APIs
([`package.json`](../package.json)). Everything else — hashing, heaps,
skip-lists, locks, the wire protocol, TLS — is built on Bun primitives.

A bare `bunqueue` invocation boots the full server; any other argv goes through the
CLI, which can itself boot the server (`start`) or act as a one-shot TCP client
([`src/main.ts:11`](../src/main.ts)). Both server paths funnel through one
`bootServer()` so they cannot drift
([`src/infrastructure/server/bootstrap.ts:94`](../src/infrastructure/server/bootstrap.ts)).

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

| Choice                                   | Where                                                                                                                                                                                                              | Why                                                                                                                                                                                                                                                                      |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Bun runtime** (`>=1.4.0`)              | [`package.json:164`](../package.json)                                                                                                                                                                              | Native, fast TCP/TLS sockets (`Bun.listen`), bundled SQLite, `Bun.randomUUIDv7()` for time-ordered IDs, `Bun.s3` for backups, single-binary `bun build --compile`. The codebase is Bun-only and guards against Node at import (`src/require-bun.ts`, `src/bun-only.ts`). |
| **`bun:sqlite`**                         | [`src/infrastructure/persistence/sqlite/state.ts`](../src/infrastructure/persistence/sqlite/state.ts)                                                                                                              | Embedded, zero-config, ACID durability with no separate process. WAL mode lets readers and the writer run concurrently. Avoids the operational weight of Redis/Postgres for a single-node queue.                                                                         |
| **Bun `SQL` + PostgreSQL 15–18**         | [`src/infrastructure/persistence/postgres/`](../src/infrastructure/persistence/postgres)                                                                                                                           | Optional server-only multi-broker coordination. PostgreSQL is the source of truth; row/advisory locks, `SKIP LOCKED`, database-clock leases, and durable events provide distributed ownership without adding a JavaScript database dependency.                           |
| **Bun `cron`**                           | [`src/infrastructure/scheduler/cronParser.ts`](../src/infrastructure/scheduler/cronParser.ts)                                                                                                                      | Native five-field calendar, timezone and DST evaluation. A small seconds-field adapter preserves bunqueue's documented six-field syntax without an external parser.                                                                                                      |
| **MessagePack** (`msgpackr`)             | [`src/shared/msgpack.ts`](../src/shared/msgpack.ts), [`src/infrastructure/persistence/sqliteSerializer.ts`](../src/infrastructure/persistence/sqliteSerializer.ts)                                                 | Compact binary storage and wire format. The shared hybrid decoder keeps the fast common path while preserving dangerous-looking JSON keys as safe own properties.                                                                                                        |
| **Native TCP + TLS**                     | [`src/infrastructure/server/tcp.ts`](../src/infrastructure/server/tcp.ts), [`src/config/resolve.ts:75`](../src/config/resolve.ts)                                                                                  | Length-prefixed binary frames over `Bun.listen` give ~100k+ ops/s without an HTTP/serialization tax. TLS is the same socket with `tls: { certFile, keyFile }`; partial cert/key fails fast at startup rather than silently serving plaintext.                            |
| **One runtime npm dependency**           | [`package.json`](../package.json)                                                                                                                                                                                  | Only `msgpackr` ships at runtime; `@modelcontextprotocol/sdk` is an **optional** peer needed only for the MCP binary. Cron and PostgreSQL use Bun's built-ins.                                                                                                           |
| **4-ary heaps / queue-local skip-lists** | [`src/shared/minHeap.ts:2`](../src/shared/minHeap.ts), [`src/domain/queue/priorityQueue.ts:56`](../src/domain/queue/priorityQueue.ts), [`src/domain/queue/temporalIndex.ts`](../src/domain/queue/temporalIndex.ts) | 4-ary branching improves cache locality vs binary heaps; one skip-list per queue orders cleanup candidates, with a reverse job-ID index for direct deletion. A compacting 4-ary min-heap tracks delayed jobs.                                                            |

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
infrastructure  External edges: SQLite/PostgreSQL persistence, TCP/HTTP servers,
  ▲             cron scheduler, S3 backup, cloud agent. Drives application.
client · cli · mcp   Consumer-facing facades: SDK (Queue/Worker/Flow/
                     Workflow), CLI verbs, MCP tool surface.
```

The CLI surface is centralized in `src/cli/commandRegistry.ts`;
`commandRouter.ts` translates registered network families to builders,
`globalOptions.ts` owns global parsing, `localOutput.ts` owns local output, and
`client.ts` owns only transport/execution. Help and complete
E2E/property/concurrency matrices are checked against the same registry.
TCP connections carry an abort signal into long-poll handlers so disconnecting
a CLI or SDK socket cancels its waiter before any later job or limiter token can
be claimed.

- **`domain/`** — Pure, synchronous, side-effect-free. `Shard` composes
  `IndexedPriorityQueue` (authoritative waiting/delayed membership),
  lazy `GroupScheduler` (ungrouped/delayed secondary heaps plus FIFO group lanes,
  hidden durable admission order, and round-robin rotation), `DlqShard`,
  `UniqueKeyManager` (dedup), `LimiterManager` (queue rate/concurrency),
  `GroupLimiterManager`, `DependencyTracker`,
  `TemporalManager`/`TemporalIndex`, queue-scoped `WaiterManager`, `ShardCounters`
  ([`src/domain/queue/shard.ts`](../src/domain/queue/shard.ts)). Plus all type
  definitions in `domain/types/`. → [Data Structures](./features/data-structures.md),
  [Core Queue Engine](./features/core-queue-engine.md), [`./data-model.md`](./data-model.md).
- **`application/`** — The six-line public `QueueManager` façade inherits a
  responsibility-ordered capability chain under `queue-manager/`. `state.ts`
  owns the shards, global indexes, and managers; delivery, ACK/failure, queries,
  control, limits, job management, stats, observability, dependency propagation,
  and lifecycle each live in their own module. Stateless hot-path algorithms
  remain under [`operations/`](../src/application/operations) and receive
  explicit contexts from `ContextFactory`; their contracts live separately in
  [`application/types/`](../src/application/types). Active-job management claims
  are split into `jobMoveOperations.ts` (state/resource transitions) and
  `jobClaim.ts` (lease/client ownership cleanup). Houses DLQ, Events, Worker,
  JobLogs, Stats managers, and the batch `QueueStatsAggregator`. DLQ reads/purge
  live in `dlqManager.ts`; manual and automatic retry transitions live in
  `dlqRetry.ts`. Selective permanent DLQ removal is durable-first, removes all
  recovered duplicates for the selected ID, and generation-guards cleanup of
  global custom-ID, dependency-result, result/log, job-index, and flow-failure
  ownership. Completion-chained repeat calculation and validation live in
  `repeatJobs.ts`, while `queue-manager/repeat.ts` owns successor dispatch and
  chain linking. Single-job admission lives in `operations/push.ts`, ordered
  accepted-prefix batches in `operations/pushBatch.ts`, and the persistence-
  before-publication boundary in `operations/pushAdmission.ts`; custom-ID,
  deduplication, insertion and parent-link planning remain separate focused
  modules. Pull orchestration, synchronous dequeue mutation, and post-lock
  persistence are split across `pull.ts`, `pullStateTransition.ts`, and
  [`pullFinalization.ts`](../src/application/operations/pullFinalization.ts),
  respectively. It also owns the
  [`DependencyResultTracker`](../src/application/dependencyResultTracker.ts) for
  live flow-result retention, and background-task wiring.
- **`infrastructure/`** — The ten-line `SqliteStorage` façade composes focused
  lifecycle, job, query, mutation, flow, control and record capabilities under
  `persistence/sqlite/`; persistence contracts live in `persistence/types/`.
  `sqlite/admission.ts` applies terminal-generation retirement and dependency-
  completion pins inside the same transaction as persistence-sensitive job
  admission; its ID-only contract lives in `types/admission.ts`.
  The optional `PostgresQueueStore` façade composes focused async modules under
  `persistence/postgres/`; `application/postgres-queue-manager/` adapts that
  database-authoritative store to the transport-facing `QueueManager` surface.
  `server/storageAdapter.ts` defines the persistence Strategy, immutable Registry,
  and lifecycle Facade used by the composition root. This keeps backend selection
  replaceable in unit tests and prevents feature modules from introducing driver
  branches into transports or the SQLite hot path. PostgreSQL rate-limit input is
  normalized once in `persistence/postgres/rateLimit.ts`; transport error
  boundaries share `server/errors.ts` so infrastructure diagnostics are redacted
  consistently without hiding domain failures.
  `jobOptionsBlob.ts` serializes repeat and advanced generation policy that has
  no dedicated legacy column, plus the hidden FIFO ordinal only for grouped
  SQLite jobs. PostgreSQL group ordering/schema/retention are isolated in
  `groupClaims.ts`, `groupSchema.ts`, `groupSchemaFingerprint.ts`, and
  `groupStateRetention.ts`.
  Server handler routing, protocol parsing, TCP connection/event-subscription
  state, HTTP routes, SSE and WebSocket state are likewise split by
  responsibility. Cloud command families, snapshot collectors and contracts
  live under `cloud/commands/`, `cloud/snapshot/` and `cloud/types/`.
  `cloud/queueAdapter/` adds a complete Strategy/Registry boundary: the local
  Strategy preserves memory/SQLite behavior, while PostgreSQL shared reads come
  from one durable repeatable-read model with no per-method cache fallback. The
  layer also provides `WriteBuffer`,
  `BatchInsertManager`,
  `createTcpServer` / `createHttpServer`, `CronScheduler`, `S3BackupManager`,
  `CloudAgent`, plus `QueueCountsScheduler` for coalesced WS/SSE count updates.
  HTTP responsibilities are split across `http.ts` (server/auth/upgrades),
  `httpRouter.ts` (REST dispatch), `httpEndpoints.ts` (health/stats/debug),
  `httpDashboardEndpoints.ts` (dashboard aggregation), and `httpResponse.ts`
  (response helpers). `server/bootstrap.ts` is the single composition root and
  supplies live TCP connection counts plus the pre-backup persistence flush.
- **`client/`** — In-process SDK: `Queue`, `Worker`, `SandboxedWorker`,
  `QueueEvents`, `FlowProducer`, `QueueGroup`, `Bunqueue` (simple mode), the
  `Workflow`/`Engine` pair, and the TCP `TcpPool`/forwarder. Queue reads are split between the
  generic conversion/query module, state aliases (`queryStates.ts`), and
  exhaustive TCP traversal (`queryTcpPages.ts`); see
  [Client SDK: Queue](./features/client-queue-sdk.md). Each facade transparently
  targets embedded or TCP where its contract permits it. Their public entry
  files are deliberately tiny façades: `Queue` is 27 lines and `Worker` /
  `SandboxedWorker` are four lines each. Runtime behavior is grouped under each
  component's `runtime/` directory, while contracts live in its `types/`
  directory.
  QueueEvents and Worker stall notifications share a focused dedicated TCP
  subscription adapter under `client/queue-events/`; public event payloads live
  separately under `client/types/events.ts`.
  Workflow shutdown ownership is centralized in `workflow/executionFence.ts`;
  node publication lives in `workflow/executorQueue.ts`, and the extracted
  `workflow/forEachRunner.ts` keeps each orchestration module below 300 lines.
- **`shared/`** — Cross-cutting primitives: `fnv1a`/`uuid`/`shardIndex`
  ([`src/shared/hash.ts`](../src/shared/hash.ts)), the stable lock façade
  (`lock.ts`) with focused `asyncLock.ts`/`rwLock.ts` implementations, `Semaphore`,
  `LRUMap`/`BoundedSet`/`BoundedMap`/`TtlMap`, `MinHeap`, `SkipList`, `Histogram`,
  `Logger`, `webhookValidation`, and the storage-health predicates/client-safe
  projection ([`src/shared/storageHealth.ts`](../src/shared/storageHealth.ts)).
- **`cli/`** — `bunqueue` executable: server boot detection + thin TCP client that
  maps verbs to protocol commands.
- **`mcp/`** — `bunqueue-mcp` binary exposing the queue to AI agents over MCP/stdio.

### Structural boundaries

Logic and types are intentionally separate, and every TypeScript source file
under `src/` is capped at 300 lines. A façade only establishes the stable import
surface; it does not duplicate implementation logic.

| Public surface                            | Focused implementation                                                                           | Dedicated contracts                            |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------ | ---------------------------------------------- |
| `application/queueManager.ts`             | `application/queue-manager/`, `application/operations/`, `application/background/`               | `application/types/`                           |
| `client/queue/queue.ts`                   | `client/queue/runtime/`, `client/queue/operations/`, job/DLQ helpers                             | `client/queue/types/`                          |
| `client/worker/worker.ts`                 | `client/worker/runtime/`, `processor.ts`, `processorOutcome.ts`, pull, heartbeat and ACK modules | `client/worker/types/`                         |
| `client/sandboxed/worker.ts`              | `client/sandboxed/runtime/`, wrapper and queue adapters                                          | `client/sandboxed/types/`                      |
| `infrastructure/persistence/sqlite.ts`    | `infrastructure/persistence/sqlite/`, buffering and schema modules                               | `infrastructure/persistence/types/`            |
| `infrastructure/persistence/postgres.ts`  | `infrastructure/persistence/postgres/`                                                           | `infrastructure/persistence/postgres/types.ts` |
| `application/postgresQueueManager.ts`     | `application/postgres-queue-manager/`                                                            | `application/postgres-queue-manager/state.ts`  |
| `infrastructure/server/storageManager.ts` | `infrastructure/server/storageAdapter.ts` and registered backend strategies                      | `config/types.ts`                              |

`test/source-architecture.test.ts` enforces the 300-line ceiling, the small
façade boundaries, the presence of dedicated type modules, and the absence of
line-number references from internal documentation into those façades. This is
an architectural regression gate, not a style convention left to review.

Public behavior remains transport-neutral across those boundaries. The
[documented feature verification matrix](./features/documented-feature-verification.md)
maps every Queue, Worker, Cron, and DLQ guide section to executable TCP and
embedded evidence. Shared contracts hold the assertions once; thin discovered
wrappers select the transport, preventing the two implementations from drifting.

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
│   │  depCompletions(recent FIFO + pinned) · jobResults(LRU) · customIdMap ·  │   │
│   │  jobLogs(LRU) · jobLocks(Map)                                             │   │
│   └──────────────────────────────────┬───────────────────────────────────────┘   │
│                                       ▼                                          │
│   ┌────────────────────────────────────────────────────────────────────────┐   │
│   │  WriteBuffer (10ms / 100-job double-buffer) ─► SQLite (WAL, msgpack)     │   │
│   │  Recovery ◄─ jobs · flow_failures · dep_proofs · results · dlq · cron     │   │
│   │              · queue_state · queue event/metric journal                    │   │
│   └────────────────────────────────────────────────────────────────────────┘   │
│   ┌────────────────────────────────────────────────────────────────────────┐   │
│   │  Background tasks: CronScheduler · stall · lock-expiry · DLQ maint ·     │   │
│   │  dependency · cleanup/memory-bounds · monitoring · S3 snapshot · Cloud   │   │
│   └────────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
```

The diagram above is the default memory/SQLite topology. PostgreSQL mode swaps
the in-memory-authoritative manager and SQLite write-behind boundary for one
`PostgresQueueManager` per broker and a shared database-authoritative store:

```
Clients ──► Broker A (TCP/HTTP) ──┐
                                  ├──► PostgreSQL 15–18
Clients ──► Broker B (TCP/HTTP) ──┘    jobs · leases · limits · cron · events
                 ▲                         │
                 └──── LISTEN/NOTIFY + durable event cursor ────┘
```

PostgreSQL transactions own correctness; each broker's local snapshot is a read
cache and event projection, never the distributed lock authority. See
[PostgreSQL 15–18 Multi-Broker Persistence](./features/postgres-multibroker.md).

The TCP and HTTP servers are thin adapters: both decode a request into a command
and call the shared `handleCommand` dispatcher against the single `QueueManager`
instance created in `bootServer()`
([`src/infrastructure/server/bootstrap.ts:126`](../src/infrastructure/server/bootstrap.ts)).
See [TCP Server Handlers](./features/tcp-server-handlers.md) and
[HTTP / REST / SSE / WebSocket API](./features/http-api.md).

Queue event subscriptions are the deliberate transport-level exception to
ordinary request dispatch because they mutate socket-owned state. The TCP
registry authenticates and records one queue per dedicated subscriber, owns one
lazy QueueManager bridge for all subscribers, and sends unsolicited
`type:'event'` envelopes through the same bounded write queues. Clients dispatch
those envelopes before `reqId` correlation and resubscribe after reconnect.
The command path is symmetric: each client socket owns an ordered 64 MiB write
queue, so a partial Bun TCP write is resumed on `drain` before any later frame.
That queue is cleared, never replayed, when the physical connection changes.

## Deployment Modes

**(a) Embedded (in-process).** `new Queue('q', { embedded: true, dataPath: './q.db' })`
instantiates a `QueueManager` inside the application process — no sockets, no
serialization. Lowest latency; synchronous snapshot helpers are available here,
while async variants provide the same authoritative stall and DLQ configuration
over TCP. Trade-off: scoped to one process; multiple
processes pointing at the same SQLite file is **not** supported for concurrent
writers.

**(b) Standalone SQLite server (TCP + HTTP).** `bunqueue start` (or bare `bunqueue`) boots
both transports, S3 backup, the cloud agent, the stats interval, and graceful
shutdown ([`bootstrap.ts:73`](../src/infrastructure/server/bootstrap.ts)). Many
clients connect over TCP `:6789` / HTTP `:6790`. Trade-off: a network hop and
msgpack encode/decode per op, but a single source of truth and full observability.

**(c) Multi-broker PostgreSQL server.** Two or more `bunqueue start` processes
select `storage.driver: 'postgres'`, use unique broker IDs, and share one
PostgreSQL 15–18 URL and namespace. Transactions, row locks, `SKIP LOCKED`, and
lease tokens coordinate claims and outcomes. Trade-off: an external database
and async storage hop in exchange for horizontally scalable brokers and shared
failover state. Embedded mode does not use this driver.

**(d) Distributed producer / consumer.** Separate processes run `Queue` (producers)
and `Worker` (consumers) against one or more standalone servers over TCP. Workers
register, heartbeat, pull with leases, and ack/fail. With SQLite the central
server is the single coordination point; PostgreSQL allows several brokers.

**(e) Edge store-and-forward.** An embedded edge queue drains its local jobs to a
remote server via `embeddedQueue.forward({ to, queue })`
([`src/client/forwarder.ts`](../src/client/forwarder.ts)). Remote failures fall
back to local retry/DLQ. With persistent local storage, queued work survives
uplink outages while its process or volume survives; a deterministic remote jobId
`fwd:<queue>:<localId>` dedupes re-forwards within the server's custom-id
retention window. Trade-off: at-least-once delivery with bounded dedup; ideal for
IoT/edge that must tolerate intermittent connectivity. See
[Store-and-Forward](./features/store-and-forward.md).

## Request Data Flows

**PUSH** (`Queue.add`)

1. Client serializes the job and either calls `QueueManager.push()` directly
   (embedded) or sends a `PUSH`/`PUSHB` msgpack frame over the `TcpPool`.
2. TCP server decodes + authenticates the frame and dispatches to the push handler.
3. In memory/SQLite mode, `QueueManager.push()` registers the queue name and delegates to `pushJob()`
   ([`queue-manager/delivery.ts`](../src/application/queue-manager/delivery.ts),
   [`operations/push.ts`](../src/application/operations/push.ts)).
4. `shardIndex(queue)` selects the shard. Under the required ascending write
   locks, the application inspects custom-ID, unique-key, dependency-completion
   and optional existing-parent state and builds a mutation plan without
   exposing the candidate.
5. When admission can reject synchronously, SQLite commits the candidate and
   all prerequisite retirement/pin/link changes first. A durable insert,
   terminal deterministic-ID reuse, dedup replacement, active-key transfer, or
   parent link therefore fails closed: an error leaves the old durable and RAM
   state authoritative, with no executable candidate or half-edge.
6. After commit, the application publishes heap/wait-set membership,
   `jobIndex`, counters and ownership maps together and emits `pushed`. A plain
   non-durable insert without admission metadata instead uses the normal 10 ms
   `WriteBuffer`; `pushJobBatch` applies the same sequence per item and retains
   its ordered accepted-prefix contract.
7. PostgreSQL mode takes a separate database-authoritative path: the override
   commits admission, ownership constraints, queue registration, and durable
   events in PostgreSQL, then refreshes the accepting broker's local projection
   before acknowledging. It does not acquire the base manager's shard locks or
   use SQLite's `WriteBuffer`.

**PUSHF** (`FlowProducer`)

1. The client preallocates every ID and compiles all roots into one fully
   resolved graph; embedded mode calls `QueueManager.pushFlow`, while the Bun
   TCP client and all six official external SDKs send one `PUSHF` frame.
2. The broker validates the complete graph before mutation, including numeric
   bounds, IDs, queues, option compatibility, edge symmetry, and cycles.
3. In memory/SQLite mode, it acquires the custom-ID lock when needed and every affected queue-shard
   write lock in ascending order, then rechecks ownership.
4. With a configured `dataPath`, one immediate SQLite transaction inserts every
   node and its initial `waiting`/`prioritized`/`delayed`/`waiting-children`
   state. A manager without storage skips this durability step.
5. Only after that transaction commits (or immediately in memory-only mode) are
   all heaps, dependency indexes, counters, `jobIndex`, and custom-ID ownership
   published under the held locks. Workers need the same shard locks, so no
   leaf is observable against partial topology.
6. Locks are released before notifications/events; the response contains one
   authoritative snapshot per committed job.
7. PostgreSQL mode instead commits every node, dependency edge, queue-registry
   mutation, and durable event in one database transaction. The accepting
   broker refreshes the affected queue projections after commit; no leaf is
   claimable from PostgreSQL before the complete graph is visible.

Previously published SDKs can still issue `PUSH` plus `UpdateParent`. If the
parent already declared the child, this compatibility command is a child-only
parent-id back-patch: it preserves an active/completed/DLQ parent exactly as it
is and atomically updates a persisted child/DLQ snapshot together with any
failure-outbox key. Only a genuinely new edge requires a queued parent and
mutates both sides of the topology.

**PULL** (`Worker` poll)

1. Worker requests work (`PULL`/`PULLB`, optionally with a lease/owner) for a queue.
2. Dispatch → `QueueManager.pull()` / `pullWithLock()`
   ([`queue-manager/delivery.ts`](../src/application/queue-manager/delivery.ts),
   [`operations/pull.ts`](../src/application/operations/pull.ts)); atomic
   queue-state transitions and the reusable dequeue scratch live in
   [`operations/pullStateTransition.ts`](../src/application/operations/pullStateTransition.ts),
   while post-lock persistence and event publication live in
   [`operations/pullFinalization.ts`](../src/application/operations/pullFinalization.ts).
3. A queue without grouped work keeps the ordinary primary-heap path and no
   secondary group state. On the first grouped insertion, `GroupScheduler`
   builds its view from authoritative membership; it is removed again after the
   last queued grouped job leaves. While active, it promotes due entries from a
   secondary delayed heap, serves ready ungrouped work first, then selects FIFO
   lane heads in round-robin group order. Per-group concurrency and fixed-window
   rate eligibility are checked before admission. Insert/remove hooks keep every
   secondary index synchronous with the primary queue, so blocked groups require
   no global-heap scan or temporary reinsertion.
4. In the same synchronous critical section as the pop, the job is inserted into
   `processingShards[procIdx]` and its `jobIndex` entry flips to processing
   (state → `active`), so observers never see a stale location. Post-await
   bookkeeping (persist active state, counters, broadcast) runs in
   `finalizeProcessing`, which skips delivery when a management op claimed the
   job in the meantime. `PULLB` persists all surviving handoffs through one
   `markActiveBatch` transaction; a failed transaction rolls back and retries
   each row through the non-fatal scalar path.
   A successful grouped claim advances the rotation cursor and consumes exactly
   one group rate token; every active exit releases its group count and wakes a
   matching waiter. A lock token is issued when leasing. Long-poll waits in a queue-specific
   `WaiterManager` bucket until a matching queue edge or the timeout; surplus
   notifications coalesce into one retry hint.
5. The job (and token) is returned; the worker registry counters update.

**ACK** (success)

1. Worker reports `ACK`/`ACKB` with the jobId, lock token, and optional result.
2. Dispatch → `ackJob()` / `ackJobBatch()`
   ([`operations/ack.ts`](../src/application/operations/ack.ts)).
3. The token is validated against `jobLocks`. The operation then returns
   extraction evidence from inside the processing lock. If a timeout won after
   validation, only the exact retired `{ jobId, startedAt, token }` generation
   is reported as ignored; a newer retry lease is untouched.
4. The job is removed from `processingShards`, added to `completedJobs`, its result
   stored in the `jobResults` LRU. A retained `ACKB` with no defined results
   flushes pending inserts and commits all completed states through one
   `markCompletedBatch` transaction. Result-bearing or `removeOnComplete`
   entries retain their ordered scalar/combined completion paths. A failed
   state batch rolls back before scalar retry. Dependents are queued for
   resolution only after the corresponding completion bookkeeping.
5. `job:completed` is emitted (events/webhooks/SSE/WS); repeat/cron successors and
   flow parents are scheduled.

For in-memory/SQLite `PUSHB`, `PULLB`, and `ACKB`, lifecycle emission uses
`EventsManager.broadcastBatch`. The manager-owned `QueueTelemetryJournal`
commits the ordered batch through `SqliteStorage.recordQueueEventsBatch`, while
ordinary event subscribers, completion waiters, and webhooks still receive
individual events in order. Journal retention runs once per affected queue and
terminal metric mutations are aggregated per queue/type after an exact
in-memory simulation of scalar bucket pruning. Job-state writes use their own
`markActiveBatch`/`markCompletedBatch` transactions, so best-effort
observability remains isolated from durable scheduling state.

**FAIL** (error)

1. Worker reports `FAIL` with jobId, token, and error message/stack.
2. Dispatch → `failJob()` ([`operations/ack.ts`](../src/application/operations/ack.ts)).
3. The same post-claim generation check as ACK suppresses a processor failure
   that arrives after an authoritative timeout. Worker and SandboxedWorker emit
   no local terminal event for that ignored outcome.
4. If `attempts < maxAttempts`, the job is re-enqueued with backoff delay (state
   `delayed`); otherwise it moves to the shard's **DLQ** with a `FailureReason`.
5. State + DLQ row are persisted; `job:failed` (and `job:dead` on exhaustion) is
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
shardIndex(queue) = fnv1a(queue) & SHARD_MASK; // queue → shard
processingShardIndex(jobId) = fnv1a(jobId) & SHARD_MASK; // active job → proc shard
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
([`src/shared/rwLock.ts`](../src/shared/rwLock.ts), exported by
[`src/shared/lock.ts`](../src/shared/lock.ts)) plus a strict acquisition order to
prevent deadlock. Locks must be acquired in this order:

```
1. jobIndex  →  2. completedJobs  →  3. shards[N]  →  4. processingShards[N]
```

The canonical pattern is **read the bare index first, then take the lock**:

```ts
const completed = completedJobs.has(id); // cheap read, no lock
const shard = await shards[idx].acquire(); // RWLock write guard
try {
  /* mutate shard state */
} finally {
  shard.release();
}
```

`RWLock` supports multiple concurrent readers or one writer. Writer ownership is
handed off FIFO and reserved before the waiter's promise resolves, so a newly
arriving operation cannot starve an older batched ACK. `LOCK_TIMEOUT_MS` (default
5000ms) bounds acquisition so a stuck holder cannot wedge a shard forever.
`Semaphore` bounds worker concurrency. Job **leasing** issues a token on pull;
ACK/FAIL is rejected if the token no longer matches. Together with
`stalledCandidates`, exact `RetiredTimeoutGeneration` records, and post-claim
outcome evidence, this safely handles workers that die or finish after their
deadline. See [Concurrency & Locking](./features/concurrency-and-locking.md).

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
  ([`sqlite/state.ts`](../src/infrastructure/persistence/sqlite/state.ts)). The buffer is
  **double-buffered** — ownership swaps from `activeBuffer` to `flushBuffer`, then
  the flush batch is committed in one synchronous transaction. The swap preserves
  retry ordering and keeps new writes out of the in-flight batch
  ([`writeBuffer.ts`](../src/infrastructure/persistence/writeBuffer.ts)).
- **Durable.** `add(..., { durable: true })` bypasses the buffer for an immediate
  synchronous commit — no bunqueue process-crash buffer window, at lower
  throughput. Host, filesystem, and physical-media durability still apply.
- **Resilience.** Flush failures retry with exponential backoff (100ms → 30s, 10
  tries); `SQLITE_FULL` flips a disk-full flag; permanent constraint violations
  (e.g. duplicate id) are dropped per-row without poisoning the batch, and
  exhausted batches surface via an `onCriticalLoss` callback.
- **Serialization.** Job payloads, results, and DLQ entries are stored as
  MessagePack blobs ([`sqliteSerializer.ts`](../src/infrastructure/persistence/sqliteSerializer.ts)).
- **Recovery.** On startup `bgTasks.recover()` batch-reads jobs, bounded
  dependency-completion proofs, results, DLQ, cron, and queue control-state
  back into memory before serving traffic
  ([`background/recovery/index.ts`](../src/application/background/recovery/index.ts)).

Persisted tables: `jobs`, `flow_failures`, `dependency_completions`,
`job_results`, `dlq`, `cron_jobs`, `queue_state`, `queue_events`,
`queue_metrics_meta`, `queue_metric_buckets` (plus the `migrations`
bookkeeping table) — see
[Persistence](./features/persistence.md) and [`./data-model.md`](./data-model.md).

### PostgreSQL multi-broker path

When the server resolves `storageDriver === 'postgres'`,
`ServerStorageManager` resolves the PostgreSQL Strategy from
`StorageAdapterRegistry`, constructs it, and initializes `PostgresQueueManager`
before binding either transport. Strategy fakes exercise creation, validation,
display, concurrent shutdown, and failed-shutdown retry without PostgreSQL.
PostgreSQL is authoritative for every lifecycle transition; there is no SQLite
write buffer and the existing SQLite code path is not invoked.

Admission composition is isolated in `postgres/admissionStore.ts`; its result
contract lives in
[`postgres/admissionResult.ts`](../src/infrastructure/persistence/postgres/admissionResult.ts),
and
[`postgres/serialAdmission.ts`](../src/infrastructure/persistence/postgres/serialAdmission.ts)
reconciles the final dependency generation without reordering its original
outbox event. The bulk implementation is split across `postgres/batchAdmission.ts`,
`postgres/claimSelection.ts`, `postgres/claimBatch.ts`, `postgres/batchCompletion.ts`,
`postgres/batchEvents.ts`, `postgres/completionEvents.ts`, `postgres/metricWrites.ts`,
`postgres/dependencyPromotion.ts`, `postgres/advisoryLocks.ts`,
`postgres/transactionRetry.ts`, and the manager-side
`postgres-queue-manager/batchSnapshot.ts`. Terminal ACK/failure delivery and its
shutdown drain live in `postgres-queue-manager/terminalDelivery.ts`; the
reentrant lifecycle boundary in `postgres-queue-manager/operationGate.ts` covers
all database-backed manager operations, and
[`postgres-queue-manager/config.ts`](../src/application/postgres-queue-manager/config.ts)
isolates their construction contract. Completion outcome/lifecycle/query modules
and destructive dependency/queue/mutation modules keep generation retention and
removal independently testable. Direct pending-child removal is a dedicated
fixed-point transaction in
[`postgres/removeUnprocessedChildren.ts`](../src/infrastructure/persistence/postgres/removeUnprocessedChildren.ts).
Manager snapshot views are pure
functions in `postgres-queue-manager/snapshotViews.ts`; the mutable Snapshot
owns only state transitions and bounds. `postgres/readModels.ts` loads coherent
manager and per-queue projections in `REPEATABLE READ READ ONLY` transactions,
`postgres/snapshotBudget.ts` rejects an oversized compatibility view before
decoding it, and `postgres/lifetimeMetricsFinalizer.ts` fences terminal counters
against the commit-ordered journal. `postgres/brokerSessions.ts` owns duplicate
ID detection, stale takeover, session locks, heartbeat, and exact-session
cleanup; lease and worker writers depend on that boundary. The Bun connection
factory applies PostgreSQL-native statement, lock, idle-transaction, and
application-name parameters. `postgres/eventCommitGc.ts` adaptively drains
orphaned commit envelopes in bounded database turns,
while `postgres-queue-manager/projectionRefreshes.ts` coalesces authoritative
per-job repair with job- and queue-scoped generation fencing; completion
projections retain the queue identity read from PostgreSQL so queue-wide
replacement cannot miss or resurrect a result. `postgres/eventRetention.ts` centralizes
the indexed journal cutoff, non-blocking inline lock, ordered candidate plan,
and blocking single-queue sweep used by manual trim and crash recovery.
`postgres/eventCatchupCursors.ts` owns the per-queue applied commit cursor and
the prune watermarks already accounted for, so a reader refreshes a queue when a
prune could have removed part of the commit it just applied, and refreshes it
only once per new watermark.
`postgres/dlqMaintenance.ts` composes normal DLQ upkeep with an authoritative
retention repair without changing the store's explicit maintenance API.
`postgres/dlqRetryPlan.ts` discovers failed consumers before taking locks, then
locks queue policy, dependency identities, and job rows in canonical order and
revalidates the current dependency edge set before auto-retry. This prevents a
reused dependency generation from invalidating completion evidence already used
to make a consumer runnable. Server process teardown is independently owned by
`server/shutdownCoordinator.ts`: duplicate signals share one task, optional
backup/Cloud failures cannot skip storage cleanup, transient storage close is
retried once with a bound, and an exit code is emitted in every terminal path.

- **Admission and outcomes.** Jobs, dependencies, result/DLQ state, repeat links,
  metrics, and durable events change inside PostgreSQL transactions. Independent
  `PUSHB`, unique-ID `ACKB`, claim updates, events, and completion metrics use
  set-based statements; feature-bearing conflicts retain the serial semantic
  path. Admission and completion acquire dependency-evidence locks in canonical
  order; single and batch admissions acquire their complete ID/deduplication key
  union through the same set-based lock order. Completion creates an immutable
  lock-plan Command before child row locks and promotes newly ready parents,
  including their payload timeline, version, state, and event, in that same
  transaction. Serial admission resolves ID/key deduplication before retiring
  the requested generation. Its final set reconciliation promotes or demotes
  only rows inserted by that transaction and corrects their already-ordered
  `pushed` payload before commit. Parent attachment/failure/recovery reuse the
  child dependency key, re-read the current parent, then acquire sorted parent
  keys before rows. High-cardinality advisory identities are length-prefixed,
  domain-separated, hashed to 64 bits, deduplicated, and acquired by physical
  lock key order. Core admission, claim, ACK, ACKB, and FAIL transactions replay
  once with jitter only after PostgreSQL reports a rollback-certain SQLSTATE;
  ambiguous connection failures are never replayed. Validated `PUSHF` graphs
  reuse their complete outer lock plan, retire completion generations once, and
  batch queue registration plus ordered `pushed` events per transaction rather
  than repeating that work for each node. Every PostgreSQL terminal
  path supplies its database
  transition timestamp to DLQ construction, keeping attempt, retry, and expiry
  comparisons on the same clock without changing SQLite's host-clock path.
- **Claims.** Default-policy claimers hold compatible queue-state share locks;
  configured rate/concurrency decisions retain an exclusive row lock. Indexed
  FIFO, mixed-order, and grouped selection paths apply `FOR UPDATE SKIP LOCKED`
  to narrow tuples before payload fetch. Grouped admissions allocate a
  `BIGINT` `group_order`, so lane selection stays FIFO across brokers, batch
  chunks, and restarts. The claim then assigns an opaque token plus database-
  clock lease deadline before commit. Bounded group-state retention preserves
  live jobs, explicit overrides, and effective rate windows while reclaiming
  inactive rotation rows.
- **Fencing and recovery.** ACK/FAIL/renewal lock the row and require the exact
  live token. Expired leases are recovered idempotently by any broker. Graceful
  broker/client release is an optimization, not the correctness boundary.
  Shutdown stops all new database-backed operation admission, drains every
  accepted admission, claim attempt, mutation, query, startup hydration, and
  deferred write, stops projection retries, awaits in-flight projection and
  keyed maintenance flights, then releases broker resources and closes the SQL
  pool. Nested
  async scopes are reentrant, while an empty long-poll owns no admission between
  claim attempts. Late disconnect cleanup clears local ownership without
  touching a closed pool.
- **Generation-safe removal.** Cancel/remove, clean/TTL/drain, DLQ pruning,
  completed retry, protected-cron cleanup, dedup replacement, and obliterate
  share dependency identity locks with admission. Candidate and live-consumer
  rows are revalidated after locking; bulk commands skip protected producers
  and queue obliteration rejects external live consumers. Completion-only rows
  are bounded newest-first without evicting proofs pinned by live dependencies.
  Retention uses independent 1,000-row transactions until no overage remains;
  startup awaits convergence, while identity-fenced coalesced post-commit retries
  and a periodic sweep repair interrupted cleanup or a later configuration
  reduction. DLQ startup and periodic sweeps aggregate only failed queues above
  their stored bound, lock the current policy, and reuse the same ordered,
  dependency-safe deletion transaction across concurrent brokers.
  `removeUnprocessedChildren` additionally locks the parent, direct pending
  candidates, and all live consumers, then computes the fixed point of children
  required by surviving consumers. It deletes safe generations and detaches
  protected shared children in one transaction while leaving active and terminal
  generations untouched.
- **Events.** A transactional outbox registers each event-producing transaction
  with a deferred commit sequencer. Its pre-commit trigger takes a
  namespace-scoped transaction advisory lock, allocates one value from a global
  `CACHE 1` sequence, and stamps a compact commit envelope plus any watermark.
  Immutable event rows join that envelope through `transaction_id`; brokers
  replay by `(commit_seq, event_id)`, while `pg_notify` is only a wake-up hint.
  Catch-up reads are bounded at 4,096 events, while affected-job projection
  repair is independently bounded at 1,000 IDs per query so a burst does not
  become one unbounded `ANY(...)` read.
  Inline retention never waits for a second queue lock after job locks are held;
  candidate tuples are locked by ascending ID, and a commit-aware autonomous
  sweep recalculates the cutoff under one per-queue advisory lock. Live
  notification sweeps acquire that lock non-blockingly and coalesce contention
  behind one bounded retry timer, leaving journal replay unblocked; manual and
  crash-recovery sweeps remain blocking. Cumulative per-queue prune frontiers
  refresh only a broker behind discarded history.
  A 256-event startup accumulator permits one authoritative retry when it
  overflows; a second overflow cannot starve readiness because affected queues
  retain coalesced dirty markers. Bootstrap and queue projections each
  use one repeatable-read MVCC snapshot. Journal payloads schedule current-row
  repair instead of mutating job or token state directly, and a direct claim
  supersedes its job generation before local publication. Projection flights use
  unique identities and reclaim settled generation entries, preventing stale
  reads without retaining one marker per historical job. Failed authoritative
  refreshes retain their coalesced dirty marker and retry with bounded backoff,
  so pruning cannot strand a broker on only the retained event subset. Per-queue
  failures participate in storage health until recovery, and shutdown stops
  retry loops.
- **Distributed control state.** Pause, limits, cron schedules, worker and broker
  heartbeats, logs, and queue metric totals are namespace-scoped database rows.

The PostgreSQL implementation combines a store Facade, focused transaction
scripts using an explicit context/transaction Unit of Work, a commit-ordered
transactional outbox observed by the event stream, and a local Snapshot read
model. Adding a feature means adding one focused persistence module and exposing
it through the Facade; SQL and lock boundaries stay explicit rather than hidden
behind a generic repository or ORM. Strategy fakes, explicit contexts, and
separate startup-accumulator, deferred-write, and event-health components keep
lifecycle and failure paths testable without coupling every test to a complete
server. The projection scheduler and keyed maintenance flights expose explicit
start/close/drain boundaries. The reentrant operation gate and atomic deferred-write admission make
shutdown races deterministic at the manager boundary. Concurrent drains share
an immutable sequence checkpoint, making error observation deterministic without
exposing queue internals.

The normalized table/index reference is in [Data Model](./data-model.md); the
algorithms and failure boundaries are in
[PostgreSQL 15–18 Multi-Broker Persistence](./features/postgres-multibroker.md).

### Workflow execution path

The workflow engine is a client-layer orchestrator over a normal Queue/Worker
pair, with its own synchronous SQLite state:

```
Workflow graph ──register/seal──► WorkflowExecutor
                                      │
Engine.start ──persist Execution──────┤──publish wf:step
                                      ▼
                               node admission
                        (state + cursor + in-flight claim)
                                      │
                 ┌────────────────────┼────────────────────┐
                 ▼                    ▼                    ▼
          runner / loops       wait / signals       child poller
                 └────────────────────┼────────────────────┘
                                      ▼
                         persist outcome + advance
                                      │ failure
                                      ▼
                         reverse compensation pass
```

Responsibilities are deliberately split to keep every hot module below 300
lines:

- `workflow.ts`, `workflowValidation.ts`, `workflowDefinition.ts` and
  `workflowIntrospection.ts` build, validate, fingerprint and seal graphs.
- `executor.ts`, `executorLifecycle.ts` and `executorNodes.ts` own admission,
  start/signal publication and node dispatch.
- `runner.ts`/`runnerTiming.ts`, `loops.ts`, `mapRunner.ts`,
  `subWorkflowRunner.ts`, `waitFor.ts` and `workflowDecisions.ts` own executable
  node semantics and durable decisions.
- `compensator.ts`, `compensationPass.ts`, `compensationChild.ts`,
  `compensationSupport.ts`, `compensationClaim.ts`, `unwindPlan.ts` and
  `rollbackControl.ts` own unwind policy, process-local claim handoff and
  operator recovery.
- `store.ts`, `storeListing.ts`, `storeSignals.ts`,
  `storeExecutionCodec.ts` and `storeMaintenance.ts` own SQLite state,
  deterministic listing, signal transactions and retention.
- `stepTypes.ts`, `executionTypes.ts` and `eventTypes.ts` form the public type
  model behind the stable `types.ts` barrel.

An execution is bound to a sealed definition hash. Branch, loop, item and child
input decisions are persisted before effects, while duplicate deliveries are
fenced in-process by `<executionId>:<nodeIndex>` and rejected when their cursor
is stale. This gives at-least-once replay only to work whose external outcome
is unknown; completed steps/maps and settled compensation outcomes are not
dispatched again.

An unwind has a separate process-global claim with a completion latch. If a
force-closed Engine overlaps a replacement Engine, the losing recovery waits
for the current owner, reloads the row through the replacement store, and
retries only while durable compensation remains owed. The mechanism prevents a
same-process lost wake-up; external effects still require stable idempotency
keys because it is not cross-process coordination.

See [Workflow Engine](./features/workflow-engine.md) and the workflow section in
[Data Model](./data-model.md).

## Background Tasks

`startBackgroundTasks()` arms one timer per maintenance concern; intervals come
from `DEFAULT_CONFIG` ([`application/types/config.ts`](../src/application/types/config.ts),
[`application/background/lifecycle.ts`](../src/application/background/lifecycle.ts)).

| Task                  | Interval (default)                               | Purpose                                                                                                                     |
| --------------------- | ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| Cleanup + monitoring  | `cleanupIntervalMs` = **10 s**                   | Enforce memory bounds, evict orphans, run monitoring checks                                                                 |
| Job timeout deadline  | per active `startedAt + timeout`                 | Fail/requeue at the earliest registered processing deadline                                                                 |
| Stall check           | `stallCheckMs` = **5 s**                         | Two-phase detection of unresponsive workers                                                                                 |
| Lock expiration       | `stallCheckMs` = **5 s**                         | Reclaim leases whose token TTL elapsed                                                                                      |
| Dependency resolution | `dependencyCheckMs` = **30 s** (safety fallback) | Resolve flow/child deps; fast path is event-driven                                                                          |
| DLQ maintenance       | `dlqMaintenanceMs` = **60 s**                    | Age-based auto-purge + opt-in auto-retry                                                                                    |
| Cron scheduler        | event-driven                                     | Fire delayed/recurring jobs onto queues                                                                                     |
| S3 backup             | `S3_BACKUP_INTERVAL` (6 h)                       | Flush buffer → `VACUUM INTO` WAL-safe snapshot → integrity check → gzip/SHA-256 → metadata-first S3 publication → retention |
| Stats log             | `STATS_INTERVAL_MS` = **5 min**                  | Periodic queue/memory/worker stats line                                                                                     |

Memory bounds enforced by the bounded collections (cleanup evicts ~10% when full):

| Collection                   | Max    | Eviction                                           |
| ---------------------------- | ------ | -------------------------------------------------- |
| `completedJobs`              | 50,000 | FIFO batch                                         |
| `depCompletions` recent tier | 50,000 | Exact FIFO; live-edge pins are released separately |
| `jobResults`                 | 10,000 | LRU                                                |
| `jobLogs`                    | 10,000 | LRU                                                |
| `customIdMap`                | 50,000 | LRU                                                |

See [Background Tasks](./features/background-tasks.md) and
[Scheduler & Cron](./features/scheduler-and-cron.md).

## Performance Characteristics

Current representative native measurements are operation-specific:

| Operation / topology                             | Median on the 2026-07-30 host | Persistence boundary                                            |
| ------------------------------------------------ | ----------------------------: | --------------------------------------------------------------- |
| Internal batched push, Embedded, 1M jobs         |                729,395 jobs/s | In-memory; no `dataPath`                                        |
| Public `addBulk`, Embedded, sustained 50K cell   |                186,384 jobs/s | On-disk buffered SQLite                                         |
| TCP `PUSHB`, fresh 50K sample                    |                158,779 jobs/s | Broker on-disk buffered SQLite                                  |
| TCP no-work worker drain, concurrency 50         |                 17,256 jobs/s | Full pull/process/ACK path                                      |
| Sequential `durable:true` add, Embedded / TCP    |         60,835 / 27,191 ops/s | Synchronous persistence                                         |
| Linear Workflow Engine, Embedded / TCP           |     2,700 / 3,187 workflows/s | Workflow SQLite plus 3 queue nodes                              |
| 12 tuned linear Workflow Engines, Embedded / TCP |   25,873 / 17,496 workflows/s | Independent stores/queues; TCP protocol cap raised and reported |

Throughput comes from: 4-ary heaps (cache locality), per-shard parallelism, the
double-buffered write-behind path, UUIDv7 ids that sort by time (string compare,
no `localeCompare`), MessagePack framing, and transparent client-side add-batching
that coalesces concurrent `add()` calls into one `PUSHB` round-trip. Bunqueue's
application-level data-loss exposure is the ≤10 ms buffered window — including normal TCP
`PUSH`/`PUSHB`, which acknowledge the in-memory acceptance while SQLite flushes
behind it. It is eliminated per job with `durable: true`; when SQLite is
configured, `PUSHF` always uses one immediate all-job transaction because
partial durable flow state is not a valid outcome. In memory-only mode,
`PUSHF` still provides atomic visibility but no crash durability. The internal
in-memory row has no persistence boundary and must not be presented as an
SQLite figure. Numbers are hardware-, scale-, and operation-dependent.

The cross-version `bench/fix-impact.ts` harness loads runtime modules from an
explicit source root and records raw samples plus correctness invariants. See
the [2026-07-16 core-fix benchmark](./benchmarks/fix-impact-2026-07-16.md) for
the current before/after methodology, results, and remaining hot paths.
The maintained runner catalogue and native publication contract live in
[Benchmarking and Performance Evidence](./features/benchmarks.md); the full
queue/Workflow campaign is
[Native Engineering Benchmark — 2026-07-30](./benchmarks/native-engineering-2026-07-30.md).

## Reliability & Battle-Testing

The fast development loop runs targeted tests natively. The authoritative local
gate, `bun run test:sandbox`, builds the current worktree once and runs unit,
TCP, and embedded suites in three disposable non-root containers without host
mounts or external networking. CI provides an equivalent fresh VM per suite.
The companion `bun run test:sandbox:sdk` gate builds six language-specific
images and runs TypeScript, Python, PHP, Go, Rust, and Elixir
native suites with the same containment and telemetry format. Each SDK runs the
18 shared protocol checks against both a temporary SQLite broker and an
isolated namespace on one disposable PostgreSQL 18.6 service. The containers
share only a dedicated Docker-internal network with no external route. The
harness strips bunqueue, PostgreSQL/libpq, AWS/S3, storage/TLS, and
delimiter-named credential variables from driver environments while preserving
non-secret toolchain names. Endpoint and per-check authentication are sent over
the driver protocol. This limits accidental environment exposure rather than
sandboxing an untrusted driver. Broker exit is observed, with `SIGKILL`
escalation, before SQLite or namespace cleanup begins. Suite settlement waits
for every started peer before aggregate cleanup. Docker teardown failures remain
owned and retryable, startup plus cleanup errors are aggregated, and PostgreSQL
containers are removed only after Docker confirms their creation.
Those suites include independent-connection lease/idempotency races,
native property-based flow planners with reproducible seeds and shrinking,
malformed-input fuzz corpora, bounded spikes, and durable SIGKILL/restart
recovery. Go additionally runs its race detector. A separate scheduled/manual
mutation campaign runs each ecosystem's mutation engine against those planner
properties; it is intentionally outside the bounded offline sandbox. The
TypeScript SDK is the one exception: StrykerJS was removed because its
dependency graph was the only source of the weekly advisory findings, so those
planners are covered by fast-check alone.
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

The dedicated [Core Public API End-to-End Matrix](./features/core-public-api-e2e.md)
adds a fail-closed API boundary. The TypeScript checker discovers every callable
method on exported core client classes, then the suite requires the exact
applicable method set to complete against a fresh embedded SQLite manager and a
real dynamic-port TCP broker backed by SQLite. Transport-only
`TcpConnectionPool` methods are real-TCP tested and explicitly marked embedded
`N/A`. GitHub Actions exposes this as the required `test-core-e2e` job; no
manually maintained class or method allowlist can hide a new uncovered API.

The Astro documentation site is deployed from the `docs/` Vercel project root.
Its `vercel.json` keeps the versioned TypeDoc dump crawlable but out of search
indexes with `X-Robots-Tag: noindex, follow`; per-page Markdown twins receive
`noindex` to avoid competing with their canonical HTML pages. Configuration
rationale belongs in this reference rather than in synthetic JSON properties:
Vercel rejects unknown header-rule keys before starting the Astro build.
`test/vercel-config.test.ts` pins that schema boundary so this class of
zero-duration deployment failure is caught locally.

Beyond the functional unit/integration suites, a set of adversarial "24/7
readiness" suites under `test/repro-*.test.ts` assert the delivery and resource
guarantees a continuously-running deployment depends on. Each drives a real
`QueueManager` + `createTcpServer` (several spawn the real `src/main.ts` process
against on-disk SQLite) and asserts hard invariants — not just "it ran".

| Category                  | File                          | What it proves                                                                                                                                                                                                                                                                                          |
| ------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Model-based state machine | `model-based/queue-model`     | Generated valid command histories agree with a compact specification across API state, counts, SQLite/DLQ membership, payload/priority persistence, leases, dependencies, limiters, and `SIGKILL` recovery; failures shrink and replay by seed.                                                         |
| Protocol fuzzing          | `repro-fuzz-protocol`         | Malformed frames, corrupt MessagePack, lying length prefixes, >64 MB frames, torn/coalesced frames, and pre-auth commands never crash or wedge the server; the pre-auth gate never leaks.                                                                                                               |
| Chaos / fault injection   | `repro-chaos-fault-injection` | At-least-once redelivery when a worker dies mid-job; a heartbeated-then-dropped job is not double-dispatched but is reclaimed by lock-expiry; lock-expiry under contention loses nothing; cron next-run is monotonic under clock skew/DST.                                                              |
| Race / concurrency        | `repro-race-concurrency`      | N concurrent PULLs on one job → exactly one delivery; concurrent same-`jobId` PUSH → exactly one job; active re-add is an idempotent skip (no UNIQUE crash); cancel-during-active is a safe no-op; stale ACK vs lock-expiry never double-completes; K-workers×M-jobs drain processes each exactly once. |
| Crash-recovery            | `repro-chaos-crash-recovery`  | Under `SIGKILL` (no flush): durable jobs are never lost, ACKed durable jobs stay completed, paused-state and DLQ entries persist, an active-at-crash job is recovered (attempts incremented), and multi-cycle crash fuzzing loses nothing cumulatively.                                                 |
| Soak / endurance          | `repro-chaos-soak`            | Sustained produce/consume with a worker socket killed every ~400 ms: no job lost (server-authoritative), p99 latency does not drift, WAL stays bounded, and internal collections return to baseline after drain (no per-job/per-connection leak). Env-tunable (`SOAK_MS`) for real multi-hour runs.     |
| Stress / degradation      | `repro-stress-degradation`    | A huge backlog stays bounded and responsive then drains; 100 slowloris connections are all terminated by the stall bound while a healthy client stays fast; >50 in-flight pipelined commands on one socket all complete; latency returns to baseline after a load spike.                                |
| Upgrade / rolling restart | `repro-upgrade-restart`       | Graceful `SIGTERM` flushes the write buffer so even buffered (non-durable) jobs survive; waiting/completed(+result)/paused/DLQ state all round-trip a restart; rolling restarts under load lose nothing cumulatively.                                                                                   |
| Long-running semantics    | `repro-longrunning-semantics` | Cron next-run does not drift across thousands of ticks (incl. DST); `jobResults` and the custom-id dedup map stay bounded under pressure (oldest evicted, newest retained); the DLQ is bounded to `maxEntries` and remains retryable.                                                                   |

## Module Map

### Engineering tooling

- [Test Isolation and Reproducibility](./testing.md) — Pinned test image, parallel disposable unit/TCP/embedded containers, per-file TCP server and SQLite isolation, container resource time series, per-test/file timing KPIs, anomaly reports, CI equivalence, cleanup guarantees, and native-only benchmarks.
- [Documentation Tooling](./features/documentation-tooling.md) — Astro data checks, API-reference generation, and the split Open Graph cover definitions/rendering pipeline under `docs/scripts/`.
- [Core Public API End-to-End Matrix](./features/core-public-api-e2e.md) — Compiler-discovered exact coverage of every callable Queue/Worker/Job/Cron/DLQ/Flow/Workflow facade method in every applicable mode, plus the TCP-only connection pool, without test doubles.
- [Benchmarking and Performance Evidence](./features/benchmarks.md) — Native measurement contract, evidence levels, runner catalogue, Workflow Engine single/scale harnesses, persistence labels, protocol-cap handling, integrity requirements, and publication checklist.
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

- [Persistence (SQLite, WriteBuffer, recovery)](./features/persistence.md) — Durable SQLite-backed store (WAL + msgpack + buffered/double-buffered WriteBuffer) that persists jobs, results, DLQ, cron, queue control-state, and the bounded per-queue event/metric journal, and serves batched recovery reads on restart.
- [PostgreSQL 15–18 Multi-Broker Persistence](./features/postgres-multibroker.md) — Optional database-authoritative server backend with transactional lifecycle updates, `SKIP LOCKED` claims, lease fencing/recovery, shared policies/cron/workers/job-state metrics, and durable LISTEN/NOTIFY replay across brokers.
- [Job Groups](./features/job-groups.md) — Round-robin/FIFO scheduling, server-side group limits and counts, lazy embedded secondary indexes, durable admission order, PostgreSQL rotation, and bounded group-state retention.
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
- [Public API Completeness](./features/public-api-completeness.md) — Exact audit of missing and partial public methods, embedded/TCP parity boundaries, DLQ Job factory wiring, and the per-method regression/E2E contract.
- [Polyglot SDK Quality Contract](./features/polyglot-sdks.md) — Shared production invariants, audited core-feature parity matrix, known semantic gaps, telemetry contract, and isolated six-language release gate.
- [Rust Client SDK](./features/rust-sdk.md) — Standalone synchronous Queue, bounded-thread Worker, FlowProducer, rustls transport, and shared conformance driver.
- [Elixir Client SDK](./features/elixir-sdk.md) — OTP-native Queue, concurrent Worker, FlowProducer, verified TLS transport, structured telemetry, and shared conformance driver.
- [Simple Mode (Bunqueue all-in-one)](./features/simple-mode.md) — Thin all-in-one wrapper pairing a Queue and Worker with opt-in conveniences (routes, middleware, retry, circuit breaker, batching, aging, TTL, triggers, dedup, DLQ).
- [Store-and-Forward & BullMQ Compatibility](./features/store-and-forward.md) — Client-side edge store-and-forward (drain a local queue to a remote bunqueue server, idempotently) plus partial BullMQ v5 read-API shims on Queue.

### Observability & operations

- [Webhooks, Events & Job Logs](./features/webhooks-and-events.md) — Server-side observability: outbound HTTP webhooks, in-process event pub/sub, bounded per-job logs, and client-job ownership/disconnect release.
- [Worker Registry & Management](./features/workers-management.md) — Server-side in-memory registry of connected workers tracking liveness, queues, concurrency, and per-worker job counters; backs skipIfNoWorker crons and dashboard/HTTP/CLI worker visibility.
- [Stats, Metrics & Monitoring](./features/stats-and-monitoring.md) — Read-only aggregation of queue depth counts, cumulative counters, bounded per-queue labels, standard process/build/connection collectors, S3 backup outcomes and latency histograms, exposed via TCP Stats/Metrics/Prometheus/Ping, HTTP /stats /metrics /prometheus /health /dashboard, and the periodic stats log.
- [S3 Backup](./features/backup-s3.md) — Periodic gzip-compressed, SHA-256-checksummed SQLite snapshots to S3-compatible storage with retention pruning and validate-before-replace restore.
- [bunqueue Cloud Dashboard Integration](./features/cloud-integration.md) — Opt-in agent that pushes full server telemetry snapshots to the bunqueue.io dashboard over HTTP and receives whitelisted remote commands over WebSocket.

### Interfaces & configuration

- [Native MCP Server](./features/mcp-server.md) — Exposes bunqueue to AI agents over MCP/stdio via the bunqueue-mcp binary, registering 73 tools, 5 resources, and 3 prompts backed by either an embedded QueueManager or a remote TCP server.
- [CLI](./features/cli.md) — The bunqueue executable: boots the server or acts as a thin one-shot TCP client that maps CLI verbs to msgpack protocol commands and renders responses.
- [Configuration & Entrypoint](./features/configuration.md) — Config layer and process entrypoint: resolves config-file/env/default precedence into typed config, dispatches the bunqueue executable, and provides the Logger, VERSION, and Bun-only runtime guards.

See also the data dictionary in [`./data-model.md`](./data-model.md).
