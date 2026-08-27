# bunqueue — Internal Technical Documentation

This folder is the **internal engineering reference** for bunqueue: a deep, code-accurate description of every subsystem, intended for contributors and operators who need to understand _how the system actually works_.

> **Scope.** This is distinct from the user-facing documentation site that lives under [`src/content/docs/`](./src/content/docs/) (an Astro Starlight site published to the web) and from the release [`changelog`](./src/content/docs/changelog.md). The site teaches _how to use_ bunqueue; this reference explains _how bunqueue is built_. Both must stay in sync with the code (see [Keeping these docs in sync](#keeping-these-docs-in-sync)).

bunqueue is a high-performance job queue for [Bun](https://bun.sh): the default
single-process engine uses sharded in-memory priority queues backed by a
write-behind SQLite store, while an optional PostgreSQL 15–18 server engine uses
database-authoritative transactions and leases for multi-broker deployments;
18.6 is the pinned and recommended release.
Both are fronted by binary-TCP and HTTP transports; the project also includes an
embedded SDK (`Queue`/`Worker`), a saga workflow engine, and a native MCP server
for AI agents.

---

## Start here

| Document                                                                                                    | What it covers                                                                                                                                                                                                                                                                |
| ----------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Architecture](./architecture.md)                                                                           | System overview, technology stack & rationale, layered design, component diagram, deployment modes, request data flows (PUSH/PULL/ACK/FAIL), sharding, lock hierarchy, persistence model, performance characteristics, and the full module map.                               |
| [Data Model](./data-model.md)                                                                               | Authoritative reference for the `Job` model & state machine, `JobOptions`, queue/DLQ/cron/worker/webhook types, the TCP `Command`/`Response` wire shapes, the complete SQLite schema (tables, indexes, migrations), and the in-memory collections with their eviction bounds. |
| [Test Isolation](./testing.md)                                                                              | Parallel disposable Docker validation, per-file TCP server/SQLite isolation, resource telemetry and anomaly KPIs, CI equivalence, cleanup guarantees, and the separate native-only benchmark policy.                                                                          |
| [Documented Feature Verification](./features/documented-feature-verification.md)                            | Section-by-section Queue, Worker, Cron, and DLQ traceability to real TCP and embedded functional contracts, including the symmetric parity gate.                                                                                                                              |
| [Core Public API End-to-End Matrix](./features/core-public-api-e2e.md)                                      | Compiler-discovered, fail-closed coverage of every Queue, Worker, Job, Cron, DLQ, Flow, Workflow, and related facade method against embedded and real TCP SQLite runtimes.                                                                                                    |
| [Benchmarking and Performance Evidence](./features/benchmarks.md)                                           | Evidence levels, native measurement contract, persistence/topology labels, maintained runner catalogue, Workflow Engine harness, environment controls, and publication checklist.                                                                                             |
| [Native PostgreSQL 15–18 Benchmark (2026-08-26)](./benchmarks/postgres-versions-2026-08-26.md)              | Repeated native PostgreSQL 15.19/16.15/17.11/18.6 comparison across one, two, and four independent brokers, with exact ID conservation, tail latency, WAL, deadlock, spill, fairness, CV, and CI95 evidence.                                                                  |
| [PostgreSQL 18 Performance Analysis (2026-08-26)](./benchmarks/postgres-performance-analysis-2026-08-26.md) | Four-broker bottleneck profile and controlled batch, pool, `work_mem`, transaction, WAL, spill, tail-latency, and before/after analysis, including rejected optimizations.                                                                                                    |
| [Documentation Tooling](./features/documentation-tooling.md)                                                | Astro content validation, generated API metadata, and the split Open Graph cover-data/rendering pipeline.                                                                                                                                                                     |
| [Model-Based Queue Verification](./features/model-based-testing.md)                                         | `fast-check` lifecycle and cross-queue/shard state machines against a real TCP broker and SQLite, with shrinking, seed replay, aggregate invariants, cache-boundary checks, and actual `SIGKILL` recovery.                                                                    |
| [Production Readiness End-to-End Test](./features/production-readiness-testing.md)                          | Company-style durable mixed workload over real TCP and SQLite, with concurrent workers, retry/DLQ/idempotency, health/metrics, and two broker restarts.                                                                                                                       |
| [Native Engineering Benchmark (2026-08-02)](./benchmarks/native-engineering-2026-08-02.md)                  | Apple M1 Max v2.8.56 release-candidate campaign: repeated Workflow distributions, queue/transport diagnostics, exact integrity gates, default protocol-limit evidence, and benchmark-runner regressions found during measurement.                                             |
| [Native Engineering Benchmark (2026-07-30)](./benchmarks/native-engineering-2026-07-30.md)                  | Native Ryzen 9 queue and Workflow Engine campaign: repeated Embedded/TCP throughput and latency, durable writes, tuning sweeps, default protocol cap, horizontal scaling, resource sampling, and integrity totals.                                                            |
| [Core Fix Impact Benchmark (2026-07-16)](./benchmarks/fix-impact-2026-07-16.md)                             | Reproducible before/after correctness and performance evidence for recovery, job queries, FIFO groups, statistics, temporal indexes, waiters, and delayed-heap retention.                                                                                                     |
| [Generated API Reference](./generated-api-reference.md)                                                     | How `bun run docs:api` turns the source into the per-version TypeDoc reference at `/reference/<version>/`, which entry points it covers, and the two collisions its layout avoids.                                                                                            |

**Suggested reading order:** Architecture → Data Model → the feature docs for the area you are touching.

---

## Feature reference (`features/`)

Each module has one file documenting its purpose, responsibilities, dependencies, public interface (real signatures / TCP commands / HTTP endpoints / events), data models, control flow with `path:line` citations, concurrency behavior, edge cases & failure modes, and configuration.

### Engine internals

| Document                                                       | Purpose                                                                                                                                                                                                                        |
| -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [Core Queue Engine](./features/core-queue-engine.md)           | Central coordinator that shards queues, owns the global job indexes, and orchestrates all job operations by delegating to operation modules via context objects.                                                               |
| [Data Structures](./features/data-structures.md)               | Dependency-free in-memory building blocks: an indexed 4-ary priority heap, queue-local temporal skip-lists with reverse job-ID lookup, a compacting delayed min-heap, and bounded/LRU/TTL containers plus a latency histogram. |
| [Concurrency & Locking](./features/concurrency-and-locking.md) | In-process synchronization primitives (RWLock, Semaphore) plus job-leasing and stall detection that keep the sharded state consistent under concurrent access.                                                                 |

### Jobs & lifecycle

| Document                                                                   | Purpose                                                                                                                                                                         |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Job Lifecycle](./features/job-lifecycle.md)                               | The push/pull/ack/fail state machine, including focused single/batch admission modules and the persistence-before-RAM publication boundary shared by TCP and embedded runtimes. |
| [Job Queries & Queue Control](./features/job-queries-and-control.md)       | Read/control surface of the engine: point/list job queries, single-job mutations, and queue-wide lifecycle operations (pause/resume/drain/obliterate/clean).                    |
| [Dead Letter Queue](./features/dead-letter-queue.md)                       | Terminal sink for jobs that exhausted retries / stalled / lost their lock, with inspect/filter/retry/purge plus opt-in time-based auto-retry and age-based auto-purge.          |
| [Deduplication & Unique Jobs](./features/deduplication-and-unique.md)      | Prevents duplicate jobs via custom job-ID idempotency and TTL-scoped unique keys with reject/extend/replace strategies, checked atomically inside the shard write lock.         |
| [Rate Limiting & Concurrency](./features/rate-limiting-and-concurrency.md) | Per-queue rate limits and concurrency caps, enforced server-side and honored by workers, via the `RateLimit`/`RateLimitClear`/`SetConcurrency`/`ClearConcurrency` commands.     |

### Scheduling & background work

| Document                                             | Purpose                                                                                                                                                                 |
| ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Scheduler & Cron](./features/scheduler-and-cron.md) | Event-driven server engine that fires recurring cron/interval jobs onto queues, persisting next-run/execution state for crash-safe at-most-once-per-slot scheduling.    |
| [Background Tasks](./features/background-tasks.md)   | Periodic server-side maintenance: timeouts, stall/lock recovery, DLQ upkeep, dependency resolution, memory-bound cleanup, monitoring, and startup recovery from SQLite. |

### Orchestration

| Document                                                       | Purpose                                                                                                                                                                                                               |
| -------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [FlowProducer & Job Dependencies](./features/flow-producer.md) | Atomic multi-queue graph creation, parent/child failure recovery, traversal safety, and the server-side [`DependencyResultTracker`](../src/application/dependencyResultTracker.ts).                                   |
| [Workflow Engine](./features/workflow-engine.md)               | Multi-step saga orchestration on a Queue/Worker pair: a typed DSL of nodes driven one-node-per-job, with retries, parallelism, signals, loops, sub-workflows, SQLite-persisted state, and reverse-order compensation. |

### Transport & protocol

| Document                                                         | Purpose                                                                                                                                                                                                          |
| ---------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Wire Protocol Specification](./protocol.md)                     | The normative, versioned client-facing spec of the TCP protocol (framing, envelope, command shapes, semantics, conformance requirements) — the contract every SDK is certified against via `sdk/conformance/`.   |
| [Rust Client SDK](./features/rust-sdk.md)                        | Official Rust Queue/Worker/Flow client, verified TLS transport, and protocol conformance driver.                                                                                                                 |
| [Elixir Client SDK](./features/elixir-sdk.md)                    | Official OTP-native Queue/Worker/Flow client, verified TLS transport, telemetry, and protocol conformance driver.                                                                                                |
| [Polyglot SDK Quality Contract](./features/polyglot-sdks.md)     | Shared production invariants, audited core-feature parity matrix, known semantic gaps, telemetry behavior, and isolated SDK validation gate.                                                                     |
| [TCP Wire Protocol & Framing](./features/tcp-protocol.md)        | Binary length-prefixed MessagePack transport that frames, pipelines, and backpressure-manages all TCP client/server commands and responses.                                                                      |
| [TCP Server Command Handlers](./features/tcp-server-handlers.md) | Request-handling layer that authenticates decoded commands, dispatches them through category routers to thin handler adapters, and shapes results into typed responses; also wires the full server in bootstrap. |
| [HTTP / REST / SSE / WebSocket API](./features/http-api.md)      | HTTP transport (port 6790) exposing the queue control surface as REST plus SSE/WebSocket real-time event streams, diagnostics, and metrics — all over the shared `handleCommand` dispatcher.                     |
| [Security: TLS, Auth, CORS](./features/security-tls-auth.md)     | Transport TLS (TCP+HTTP), bearer-token auth on both transports, CORS, webhook SSRF validation, and HMAC signing for webhooks and the Cloud uplink.                                                               |

### Persistence & operations

| Document                                                                        | Purpose                                                                                                                                                                                                                                                                                          |
| ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [Persistence](./features/persistence.md)                                        | Durable SQLite-backed store (WAL + msgpack + buffered/double-buffered WriteBuffer), including atomic admission metadata for terminal-ID retirement and dependency pins, plus batched recovery reads.                                                                                             |
| [PostgreSQL 15–18 Multi-Broker Persistence](./features/postgres-multibroker.md) | Optional database-authoritative server backend: focused admission/completion/destruction modules, generation-safe dependency locking and bounded proofs, atomic shared-child removal, lifecycle admission/drain, set-based claim/ACK, lease fencing/recovery, and commit-ordered durable replay. |
| [S3 Backup](./features/backup-s3.md)                                            | Periodic gzip-compressed, SHA-256-checksummed SQLite snapshots to S3-compatible storage with retention pruning and validate-before-replace restore.                                                                                                                                              |
| [Configuration & Entrypoint](./features/configuration.md)                       | Config layer and process entrypoint: resolves config-file/env/default precedence into typed config, dispatches the executable, and provides the Logger, VERSION, and Bun-only runtime guards.                                                                                                    |

### Client SDK

| Document                                                               | Purpose                                                                                                                                                               |
| ---------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Client Transport](./features/client-transport.md)                     | Wire-level TCP transport (pool, pipelining, reconnect, health, TLS, add-batching) used by the Queue and Worker SDKs.                                                  |
| [Client SDK: Queue](./features/client-queue-sdk.md)                    | Producer-side BullMQ-style `Queue<T>` SDK that transparently drives embedded (in-process) and TCP (msgpack-over-pool) backends.                                       |
| [Client SDK: Worker](./features/client-worker-sdk.md)                  | BullMQ-style consumer worker with coalesced capacity polling, embedded/TCP execution and batched outcomes, plus a process-isolated `SandboxedWorker` variant.         |
| [Public API Completeness](./features/public-api-completeness.md)       | Audited public-method contract, exact missing/partial-method count, embedded/TCP parity rules, and per-method regression/E2E coverage.                                |
| [Core Public API End-to-End Matrix](./features/core-public-api-e2e.md) | Automatically discovers callable core client instance methods and requires successful no-mock scenarios in every applicable embedded or real-TCP runtime.             |
| [Simple Mode (Bunqueue)](./features/simple-mode.md)                    | Thin all-in-one wrapper pairing a Queue and Worker with opt-in conveniences (routes, middleware, retry, circuit breaker, batching, aging, TTL, triggers, dedup, DLQ). |
| [Store-and-Forward](./features/store-and-forward.md)                   | Client-side edge store-and-forward (idempotently drain a local queue to a remote bunqueue server) plus partial BullMQ v5 read-API shims on `Queue`.                   |

### Observability

| Document                                                          | Purpose                                                                                                                                                                                                                                                                                   |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Stats, Metrics & Monitoring](./features/stats-and-monitoring.md) | Read-only global and one-pass batch per-queue aggregation, coalesced WS/SSE count updates, cumulative counters, memory sizes, EMA throughput rates, latency histograms, and the client-safe storage-health projection in [`src/shared/storageHealth.ts`](../src/shared/storageHealth.ts). |
| [Webhooks, Events & Job Logs](./features/webhooks-and-events.md)  | Server-side observability: outbound HTTP webhooks, in-process event pub/sub, bounded per-job logs, and client-job ownership/disconnect release.                                                                                                                                           |
| [Worker Registry & Management](./features/workers-management.md)  | Server-side in-memory registry of connected workers tracking liveness, queues, concurrency, and per-worker job counters; backs `skipIfNoWorker` crons and dashboard/HTTP/CLI worker visibility.                                                                                           |

### Integrations & interfaces

| Document                                                      | Purpose                                                                                                                                                                           |
| ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [CLI](./features/cli.md)                                      | The `bunqueue` executable: boots the server or acts as a thin one-shot TCP client that maps CLI verbs to msgpack protocol commands and renders responses.                         |
| [Native MCP Server](./features/mcp-server.md)                 | Exposes bunqueue to AI agents over MCP/stdio via the `bunqueue-mcp` binary, registering tools, resources, and prompts backed by either an embedded engine or a remote TCP server. |
| [bunqueue Cloud Integration](./features/cloud-integration.md) | Opt-in agent that pushes full server telemetry snapshots to the bunqueue.io dashboard over HTTP and receives whitelisted remote commands over WebSocket.                          |

---

## How the source maps to these docs

The runtime lives under [`src/`](../src) and follows a layered architecture (see [Architecture](./architecture.md)):

```
src/
├── domain/          # Pure business logic & types (Shard, PriorityQueue, DlqShard, types)
├── application/     # Use cases: QueueManager, operations/, managers, background tasks
├── infrastructure/  # External edges: SQLite/PostgreSQL persistence, servers, scheduler, backup, cloud
├── client/          # Embedded SDK: queue/, worker/, tcp/, workflow/, bunqueue (simple mode), forwarder
├── shared/          # Utilities: hash, lock, semaphore, lru/bounded/ttl maps, skipList, minHeap, histogram
├── cli/             # Command-line interface
├── mcp/             # Native MCP server + tools
└── config/          # Config resolution + entrypoint glue
```

Source directories under `src/benchmark/`, `bench/`, and `test/` are tooling
rather than runtime feature modules. They are covered by [Test
Isolation](./testing.md), the _Performance Characteristics_ and _Reliability &
Battle-Testing_ sections of [Architecture](./architecture.md), and the
reproducible reports under `benchmarks/`.

Documentation build tooling lives under `docs/scripts/`. The Open Graph
generator keeps static cover definitions in `og-covers.ts` and rendering in
`generate-og.ts`; see [Documentation Tooling](./features/documentation-tooling.md).

The Queue query split (`operations/query.ts`, `queryStates.ts`, and
`queryTcpPages.ts`) is documented in [Client SDK: Queue](./features/client-queue-sdk.md).
The job-admission split (`operations/push.ts`, `pushBatch.ts`,
`pushAdmission.ts`, `customId.ts`, `pushDeduplication.ts`, and `parentLink.ts`)
is documented in [Job Lifecycle](./features/job-lifecycle.md), with its
`persistence/sqlite/admission.ts` transaction contract documented in
[Persistence](./features/persistence.md).
The optional server-only PostgreSQL runtime lives under
`persistence/postgres/`, with its manager adapter under
`application/postgres-queue-manager/`; see
[PostgreSQL 15–18 Multi-Broker Persistence](./features/postgres-multibroker.md).
That feature reference covers the `admissionStore`, its typed
[`admissionResult.ts`](../src/infrastructure/persistence/postgres/admissionResult.ts)
contract and
[`serialAdmission.ts`](../src/infrastructure/persistence/postgres/serialAdmission.ts)
reconciler, the domain-separated 64-bit lock identities in
[`advisoryLocks.ts`](../src/infrastructure/persistence/postgres/advisoryLocks.ts),
the rollback-certain core transition replay in
[`transactionRetry.ts`](../src/infrastructure/persistence/postgres/transactionRetry.ts),
completion lifecycle/query, dependency destruction, destructive
mutation, queue destruction, DLQ repair/retry-plan facades, and manager-side
Snapshot/view, terminal-delivery, and operation-gate modules added to keep lock
plans, bounded read models, and shutdown draining independently testable. The
manager constructor contract is isolated in
[`config.ts`](../src/application/postgres-queue-manager/config.ts), while
[`removeUnprocessedChildren.ts`](../src/infrastructure/persistence/postgres/removeUnprocessedChildren.ts)
owns the atomic fixed-point transaction for direct pending children and shared
consumers. Server
teardown uses `infrastructure/server/shutdownCoordinator.ts` so cleanup retries,
timeouts, duplicate signals, and exit status can be tested without binding a
production server.

---

## Keeping these docs in sync

> **This documentation is part of the codebase and must never drift from it.**

Per the project's [`CLAUDE.md`](../CLAUDE.md), **any code change must update the corresponding docs in this folder in the same change-set** — never as a follow-up:

- Touching a module → update its `features/<slug>.md` (purpose, public interface, control flow, edge cases, config).
- New module → add a `features/<slug>.md` and link it from this README and from [Architecture](./architecture.md)'s module map.
- Changing a type, SQLite table/index, TCP command, HTTP endpoint, env var, or default → update [data-model.md](./data-model.md) and the relevant feature doc.
- Adding/removing a component or changing a data flow → update [architecture.md](./architecture.md) (component diagram, flows, module map).

A code change is not complete until `/docs` reflects it.
