# bunqueue — Internal Technical Documentation

This folder is the **internal engineering reference** for bunqueue: a deep, code-accurate description of every subsystem, intended for contributors and operators who need to understand *how the system actually works*.

> **Scope.** This is distinct from the user-facing documentation site that lives under [`src/content/docs/`](./src/content/docs/) (an Astro Starlight site published to the web) and from the release [`changelog`](./src/content/docs/changelog.md). The site teaches *how to use* bunqueue; this reference explains *how bunqueue is built*. Both must stay in sync with the code (see [Keeping these docs in sync](#keeping-these-docs-in-sync)).

bunqueue is a high-performance, zero-external-dependency job queue for [Bun](https://bun.sh): sharded in-memory priority queues backed by a write-behind SQLite store, fronted by binary-TCP and HTTP transports, with an embedded SDK (`Queue`/`Worker`), a saga workflow engine, and a native MCP server for AI agents.

---

## Start here

| Document | What it covers |
| --- | --- |
| [Architecture](./architecture.md) | System overview, technology stack & rationale, layered design, component diagram, deployment modes, request data flows (PUSH/PULL/ACK/FAIL), sharding, lock hierarchy, persistence model, performance characteristics, and the full module map. |
| [Data Model](./data-model.md) | Authoritative reference for the `Job` model & state machine, `JobOptions`, queue/DLQ/cron/worker/webhook types, the TCP `Command`/`Response` wire shapes, the complete SQLite schema (tables, indexes, migrations), and the in-memory collections with their eviction bounds. |
| [Test Isolation](./testing.md) | Parallel disposable Docker validation, per-file TCP server/SQLite isolation, resource telemetry and anomaly KPIs, CI equivalence, cleanup guarantees, and the separate native-only benchmark policy. |
| [Model-Based Queue Verification](./features/model-based-testing.md) | `fast-check` lifecycle and cross-queue/shard state machines against a real TCP broker and SQLite, with shrinking, seed replay, aggregate invariants, cache-boundary checks, and actual `SIGKILL` recovery. |
| [Production Readiness End-to-End Test](./features/production-readiness-testing.md) | Company-style durable mixed workload over real TCP and SQLite, with concurrent workers, retry/DLQ/idempotency, health/metrics, and two broker restarts. |
| [Core Fix Impact Benchmark (2026-07-16)](./benchmarks/fix-impact-2026-07-16.md) | Reproducible before/after correctness and performance evidence for recovery, job queries, FIFO groups, statistics, temporal indexes, waiters, and delayed-heap retention. |
| [Generated API Reference](./generated-api-reference.md) | How `bun run docs:api` turns the source into the per-version TypeDoc reference at `/reference/<version>/`, which entry points it covers, and the two collisions its layout avoids. |

**Suggested reading order:** Architecture → Data Model → the feature docs for the area you are touching.

---

## Feature reference (`features/`)

Each module has one file documenting its purpose, responsibilities, dependencies, public interface (real signatures / TCP commands / HTTP endpoints / events), data models, control flow with `path:line` citations, concurrency behavior, edge cases & failure modes, and configuration.

### Engine internals

| Document | Purpose |
| --- | --- |
| [Core Queue Engine](./features/core-queue-engine.md) | Central coordinator that shards queues, owns the global job indexes, and orchestrates all job operations by delegating to operation modules via context objects. |
| [Data Structures](./features/data-structures.md) | Dependency-free in-memory building blocks: an indexed 4-ary priority heap, queue-local temporal skip-lists with reverse job-ID lookup, a compacting delayed min-heap, and bounded/LRU/TTL containers plus a latency histogram. |
| [Concurrency & Locking](./features/concurrency-and-locking.md) | In-process synchronization primitives (RWLock, Semaphore) plus job-leasing and stall detection that keep the sharded state consistent under concurrent access. |

### Jobs & lifecycle

| Document | Purpose |
| --- | --- |
| [Job Lifecycle](./features/job-lifecycle.md) | The four primitive pure-logic operations (push, pull, ack, fail) that move a job through its state machine beneath the TCP/HTTP servers and embedded SDK. |
| [Job Queries & Queue Control](./features/job-queries-and-control.md) | Read/control surface of the engine: point/list job queries, single-job mutations, and queue-wide lifecycle operations (pause/resume/drain/obliterate/clean). |
| [Dead Letter Queue](./features/dead-letter-queue.md) | Terminal sink for jobs that exhausted retries / stalled / lost their lock, with inspect/filter/retry/purge plus opt-in time-based auto-retry and age-based auto-purge. |
| [Deduplication & Unique Jobs](./features/deduplication-and-unique.md) | Prevents duplicate jobs via custom job-ID idempotency and TTL-scoped unique keys with reject/extend/replace strategies, checked atomically inside the shard write lock. |
| [Rate Limiting & Concurrency](./features/rate-limiting-and-concurrency.md) | Per-queue rate limits and concurrency caps, enforced server-side and honored by workers, via the `RateLimit`/`RateLimitClear`/`SetConcurrency`/`ClearConcurrency` commands. |

### Scheduling & background work

| Document | Purpose |
| --- | --- |
| [Scheduler & Cron](./features/scheduler-and-cron.md) | Event-driven server engine that fires recurring cron/interval jobs onto queues, persisting next-run/execution state for crash-safe at-most-once-per-slot scheduling. |
| [Background Tasks](./features/background-tasks.md) | Periodic server-side maintenance: timeouts, stall/lock recovery, DLQ upkeep, dependency resolution, memory-bound cleanup, monitoring, and startup recovery from SQLite. |

### Orchestration

| Document | Purpose |
| --- | --- |
| [FlowProducer & Job Dependencies](./features/flow-producer.md) | Parent/child trees and chains plus the server-side [`DependencyResultTracker`](../src/application/dependencyResultTracker.ts) that retains results while live consumers need them. |
| [Workflow Engine](./features/workflow-engine.md) | Multi-step saga orchestration on a Queue/Worker pair: a typed DSL of nodes driven one-node-per-job, with retries, parallelism, signals, loops, sub-workflows, SQLite-persisted state, and reverse-order compensation. |

### Transport & protocol

| Document | Purpose |
| --- | --- |
| [Wire Protocol Specification](./protocol.md) | The normative, versioned client-facing spec of the TCP protocol (framing, envelope, command shapes, semantics, conformance requirements) — the contract every SDK is certified against via `sdk/conformance/`. |
| [Rust Client SDK](./features/rust-sdk.md) | Official Rust Queue/Worker/Flow client, verified TLS transport, and protocol conformance driver. |
| [Elixir Client SDK](./features/elixir-sdk.md) | Official OTP-native Queue/Worker/Flow client, verified TLS transport, telemetry, and protocol conformance driver. |
| [Polyglot SDK Quality Contract](./features/polyglot-sdks.md) | Shared production invariants, telemetry behavior, regression coverage, and isolated SDK validation gate. |
| [TCP Wire Protocol & Framing](./features/tcp-protocol.md) | Binary length-prefixed MessagePack transport that frames, pipelines, and backpressure-manages all TCP client/server commands and responses. |
| [TCP Server Command Handlers](./features/tcp-server-handlers.md) | Request-handling layer that authenticates decoded commands, dispatches them through category routers to thin handler adapters, and shapes results into typed responses; also wires the full server in bootstrap. |
| [HTTP / REST / SSE / WebSocket API](./features/http-api.md) | HTTP transport (port 6790) exposing the queue control surface as REST plus SSE/WebSocket real-time event streams, diagnostics, and metrics — all over the shared `handleCommand` dispatcher. |
| [Security: TLS, Auth, CORS](./features/security-tls-auth.md) | Transport TLS (TCP+HTTP), bearer-token auth on both transports, CORS, webhook SSRF validation, and HMAC signing for webhooks and the Cloud uplink. |

### Persistence & operations

| Document | Purpose |
| --- | --- |
| [Persistence](./features/persistence.md) | Durable SQLite-backed store (WAL + msgpack + buffered/double-buffered WriteBuffer) that persists jobs, results, DLQ, cron, and queue control-state and serves batched recovery reads on restart. |
| [S3 Backup](./features/backup-s3.md) | Periodic gzip-compressed, SHA-256-checksummed SQLite snapshots to S3-compatible storage with retention pruning and validate-before-replace restore. |
| [Configuration & Entrypoint](./features/configuration.md) | Config layer and process entrypoint: resolves config-file/env/default precedence into typed config, dispatches the executable, and provides the Logger, VERSION, and Bun-only runtime guards. |

### Client SDK

| Document | Purpose |
| --- | --- |
| [Client Transport](./features/client-transport.md) | Wire-level TCP transport (pool, pipelining, reconnect, health, TLS, add-batching) used by the Queue and Worker SDKs. |
| [Client SDK: Queue](./features/client-queue-sdk.md) | Producer-side BullMQ-style `Queue<T>` SDK that transparently drives embedded (in-process) and TCP (msgpack-over-pool) backends. |
| [Client SDK: Worker](./features/client-worker-sdk.md) | BullMQ-style consumer worker that pulls jobs (embedded or TCP), runs the user processor, and reports ack/fail — plus a process-isolated `SandboxedWorker` variant. |
| [Simple Mode (Bunqueue)](./features/simple-mode.md) | Thin all-in-one wrapper pairing a Queue and Worker with opt-in conveniences (routes, middleware, retry, circuit breaker, batching, aging, TTL, triggers, dedup, DLQ). |
| [Store-and-Forward](./features/store-and-forward.md) | Client-side edge store-and-forward (idempotently drain a local queue to a remote bunqueue server) plus partial BullMQ v5 read-API shims on `Queue`. |

### Observability

| Document | Purpose |
| --- | --- |
| [Stats, Metrics & Monitoring](./features/stats-and-monitoring.md) | Read-only global and one-pass batch per-queue aggregation, coalesced WS/SSE count updates, cumulative counters, memory sizes, EMA throughput rates, and latency histograms. |
| [Webhooks, Events & Job Logs](./features/webhooks-and-events.md) | Server-side observability: outbound HTTP webhooks, in-process event pub/sub, bounded per-job logs, and client-job ownership/disconnect release. |
| [Worker Registry & Management](./features/workers-management.md) | Server-side in-memory registry of connected workers tracking liveness, queues, concurrency, and per-worker job counters; backs `skipIfNoWorker` crons and dashboard/HTTP/CLI worker visibility. |

### Integrations & interfaces

| Document | Purpose |
| --- | --- |
| [CLI](./features/cli.md) | The `bunqueue` executable: boots the server or acts as a thin one-shot TCP client that maps CLI verbs to msgpack protocol commands and renders responses. |
| [Native MCP Server](./features/mcp-server.md) | Exposes bunqueue to AI agents over MCP/stdio via the `bunqueue-mcp` binary, registering tools, resources, and prompts backed by either an embedded engine or a remote TCP server. |
| [bunqueue Cloud Integration](./features/cloud-integration.md) | Opt-in agent that pushes full server telemetry snapshots to the bunqueue.io dashboard over HTTP and receives whitelisted remote commands over WebSocket. |

---

## How the source maps to these docs

The runtime lives under [`src/`](../src) and follows a layered architecture (see [Architecture](./architecture.md)):

```
src/
├── domain/          # Pure business logic & types (Shard, PriorityQueue, DlqShard, types)
├── application/     # Use cases: QueueManager, operations/, managers, background tasks
├── infrastructure/  # External edges: persistence (SQLite), server (TCP/HTTP), scheduler, backup, cloud
├── client/          # Embedded SDK: queue/, worker/, tcp/, workflow/, bunqueue (simple mode), forwarder
├── shared/          # Utilities: hash, lock, semaphore, lru/bounded/ttl maps, skipList, minHeap, histogram
├── cli/             # Command-line interface
├── mcp/             # Native MCP server + tools
└── config/          # Config resolution + entrypoint glue
```

Source directories under `src/benchmark/`, `bench/`, and `test/` are tooling
rather than runtime feature modules. They are covered by [Test
Isolation](./testing.md), the *Performance Characteristics* and *Reliability &
Battle-Testing* sections of [Architecture](./architecture.md), and the
reproducible reports under `benchmarks/`.

---

## Keeping these docs in sync

> **This documentation is part of the codebase and must never drift from it.**

Per the project's [`CLAUDE.md`](../CLAUDE.md), **any code change must update the corresponding docs in this folder in the same change-set** — never as a follow-up:

- Touching a module → update its `features/<slug>.md` (purpose, public interface, control flow, edge cases, config).
- New module → add a `features/<slug>.md` and link it from this README and from [Architecture](./architecture.md)'s module map.
- Changing a type, SQLite table/index, TCP command, HTTP endpoint, env var, or default → update [data-model.md](./data-model.md) and the relevant feature doc.
- Adding/removing a component or changing a data flow → update [architecture.md](./architecture.md) (component diagram, flows, module map).

A code change is not complete until `/docs` reflects it.
