---
title: Changelog & Release Notes
description: Complete version history and release notes for bunqueue job queue. Track new features, bug fixes, performance improvements, and breaking changes.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/getting-started.png
---

All notable changes to bunqueue are documented here.

## [2.5.2] - 2026-02-24

### Fixed
- **TCP deduplication** — `jobId` deduplication now works correctly in TCP mode. The auto-batcher was sending `jobId` instead of `customId` in PUSHB commands, causing the server to skip deduplication for all batched operations ([#10](https://github.com/egeominotti/bunqueue/issues/10))
- **CLI `--host` and `-p` flags** — `bunqueue start --host 127.0.0.1 -p 6666` now correctly binds to the specified host and port. Previously, `parseGlobalOptions()` consumed these flags as global options, removing them before the server could use them ([#9](https://github.com/egeominotti/bunqueue/issues/9))
- **Docker healthcheck** — Changed healthcheck URL from `localhost` to `127.0.0.1` to avoid IPv6 resolution issues in Alpine containers ([#7](https://github.com/egeominotti/bunqueue/issues/7))
- **TCP ping health check** — Fixed ping response parsing from `response.pong` to `response.data.pong` matching the actual server response structure ([#5](https://github.com/egeominotti/bunqueue/issues/5))

### Added
- Tests for PUSHB deduplication (same-batch and cross-batch)
- Tests for CLI server argument re-injection (`--host`, `-p`, `--host=VALUE`, `--port=VALUE`)
- Test for ping response structure validation
- E2E TCP deduplication test script (`scripts/tcp/test-dedup-tcp.ts`)

### Docs
- Updated deployment guide healthcheck example (`localhost` → `127.0.0.1`)
- Clarified that `jobId` deduplication works in both embedded and TCP modes
- Added `--host` flag example to CLI start command reference

## [2.5.1] - 2026-02-23

### Fixed
- **MCP error handling** — All 66 tool handlers now wrapped with `withErrorHandler` that catches backend exceptions and returns structured `{ error: "message" }` responses with `isError: true` instead of raw stack traces
- **MCP TCP connection** — `createBackend()` is now async and properly awaits TCP connection. Previously used fire-and-forget (`void backend.connect()`) which silently swallowed connection failures
- **MCP not-found responses** — `bunqueue_get_job`, `bunqueue_get_job_by_custom_id`, `bunqueue_get_progress`, and `bunqueue_get_cron` now return `isError: true` when resource is not found

### Added
- `src/mcp/tools/withErrorHandler.ts` — Reusable error boundary for MCP tool handlers
- 39 new MCP backend tests (75 total) — webhooks, worker management, monitoring, batch operations, heartbeat, progress, full lifecycle

## [2.5.0] - 2026-02-21

### Changed
- **MCP server rewrite** — Upgraded from custom implementation to official `@modelcontextprotocol/sdk` (v1.26.0) for full protocol compliance
- **66 tools** organized across 10 domain-specific files (jobTools, jobMgmtTools, consumptionTools, queueTools, dlqTools, cronTools, rateLimitTools, webhookTools, workerMgmtTools, monitoringTools)
- **5 MCP resources** for read-only AI context (stats, queues, crons, workers, webhooks)
- **Dual-mode backend** — Embedded (direct SQLite) and TCP (remote server) via `McpBackend` adapter interface

### Added
- TCP mode for MCP server — connect to remote bunqueue server via `BUNQUEUE_MODE=tcp`
- AI agent documentation and use cases
- MCP configuration guides for Claude Desktop, Claude Code, Cursor, and Windsurf

## [2.4.8] - 2026-02-16

### Fixed
- **`getJobs({ state: 'completed' })`** now correctly returns completed jobs instead of empty results

## [2.4.7] - 2026-02-14

### Performance
- **Event-driven cron scheduler** - Replaced 1s `setInterval` polling with precise `setTimeout` that wakes exactly when the next cron is due. Zero wasted ticks between executions:

  | Scenario | Before | After |
  |----------|--------|-------|
  | 1 cron every 5min | 300 ticks/5min (299 wasted) | 1 tick/5min |
  | 0 crons registered | 1 tick/sec (all wasted) | 0 ticks |
  | Cron in 3 hours | 10,800 wasted ticks | 1 tick at exact time |

- A 60s `setInterval` safety fallback catches edge cases (timer drift, missed events). Zero functional changes, zero API changes.

### Added
- `scripts/embedded/test-cron-event-driven.ts` - Operational test verifying cron timer precision

## [2.4.6] - 2026-02-14

### Performance
- **Event-driven dependency resolution** - Replaced 100ms `setInterval` polling with microtask-coalesced flush triggered on job completion. Dependency chain latency drops from hundreds of milliseconds to microseconds:

  | Scenario | Before (P50) | After (P50) | Speedup |
  |----------|-------------|------------|---------|
  | Single dep (A&rarr;B) | 100.05ms | 12.5&micro;s | **~8,000x** |
  | Chain (4 levels) | 300.43ms | 28.2&micro;s | **~10,700x** |
  | Fan-out (1&rarr;5) | 100.11ms | 31.0&micro;s | **~3,200x** |

- The previous 100ms interval is now a 30s safety fallback. Zero functional changes, zero API changes.
- Bonus: less CPU at idle (no more 10 calls/sec to `processPendingDependencies` when queue is empty).

### Added
- `src/benchmark/dependency-latency.bench.ts` - Benchmark for dependency chain resolution latency
- `src/application/taskErrorTracking.ts` - Extracted error tracking for reuse across modules

## [2.4.5] - 2026-02-14

### Fixed
- **Backoff jitter** - `calculateBackoff()` now applies jitter to prevent thundering herd when many jobs retry simultaneously. Exponential backoff uses ±50% jitter, fixed backoff uses ±20% jitter around the configured delay.
- **Backoff max cap** - Retry delays are now capped at 1 hour (`DEFAULT_MAX_BACKOFF = 3,600,000ms`) by default. Previously, attempt 20 with 1000ms base produced ~12 day delays. Configurable via `BackoffConfig.maxDelay`.
- **Recovery backoff bypass** - Startup recovery now uses `calculateBackoff(job)` instead of an inline exponential formula, correctly respecting `backoffConfig` (e.g., `{ type: 'fixed', delay: 5000 }` was ignored during recovery).

## [2.4.3] - 2026-02-14

### Fixed
- **Batch push now wakes all waiting workers** - `pushJobBatch` previously called `notify()` only once, causing only 1 of N waiting workers to wake up immediately. Others had to wait for their poll timeout (up to 30s with long-poll). Now each inserted job triggers a separate notification, waking all idle workers instantly.
- **Pending notifications counter** - `WaiterManager.pendingNotification` was a boolean flag, silently losing notifications when multiple pushes occurred with no waiting workers. Changed to an integer counter (`pendingNotifications`) so each notification is tracked and consumed individually.

## [2.4.2] - 2026-02-13

### Added
- **CPU-Intensive Workers guide** - New dedicated docs page for handling CPU-heavy jobs over TCP
  - Explains the ping health check failure chain that causes job loss after ~90s of CPU load
  - Connection tuning: `pingInterval: 0`, `commandTimeout: 60000`
  - Non-blocking CPU patterns with `await Bun.sleep(0)` yield
  - Default timeouts reference table
  - SandboxedWorker as alternative for truly CPU-bound work
- **CPU stress test script** - `scripts/stress-cpu-intensive.ts` (500 jobs, 5 CPU task types, concurrency 3)

## [2.4.1] - 2026-02-12

### Changed
- **Codebase refactoring** - Split 6 large files exceeding 300-line limit into smaller focused modules
  - `src/shared/lru.ts` (643 lines) → barrel re-export + 5 modules: `lruMap.ts`, `lruSet.ts`, `boundedSet.ts`, `boundedMap.ts`, `ttlMap.ts`
  - `src/client/jobConversion.ts` (499 lines) → 269 lines + `jobConversionTypes.ts`, `jobConversionHelpers.ts`
  - `src/domain/queue/shard.ts` (554 lines) → 484 lines + `waiterManager.ts`, `shardCounters.ts`
  - `src/application/queueManager.ts` (820 lines) → 774 lines (moved `getQueueJobCounts` to `statsManager.ts`)
  - `src/client/worker/worker.ts` (843 lines) → 596 lines + `workerRateLimiter.ts`, `workerHeartbeat.ts`, `workerPull.ts`
- All barrel re-exports preserve backward compatibility — zero breaking changes
- 12 new files created, 6 files modified

## [2.4.0] - 2026-02-11

### Added
- **Auto-batching for `queue.add()` over TCP** - Transparently batches concurrent `add()` calls into `PUSHB` commands
  - Zero overhead for sequential `await` usage (flush immediately when idle)
  - ~3x speedup for concurrent adds (buffers during in-flight flush)
  - Configurable: `autoBatch: { maxSize: 50, maxDelayMs: 5 }` (defaults)
  - Durable jobs bypass the batcher (sent as individual PUSH)
  - Disable with `autoBatch: { enabled: false }`
- **306 new tests** covering previously untested modules

## [2.3.1] - 2026-02-08

### Fixed
- **Non-numeric job IDs** - Allow non-numeric job IDs in HTTP routes
- Updated HTTP route tests to match non-numeric job ID support

## [2.3.0] - 2026-02-06

### Added
- **Latency Histograms** - Prometheus-compatible histograms for push, pull, and ack operations
  - Fixed bucket boundaries: 0.1ms to 10,000ms (15 buckets)
  - Full exposition format: `_bucket{le="..."}`, `_sum`, `_count`
  - Percentile calculation (p50, p95, p99) for SLO tracking
  - New files: `src/shared/histogram.ts`, `src/application/latencyTracker.ts`
- **Per-Queue Metric Labels** - Prometheus labels for per-queue drill-down
  - `bunqueue_queue_jobs_waiting{queue="..."}` (waiting, delayed, active, dlq)
  - Enables Grafana filtering and alerting per queue name
- **Throughput Tracker** - Real-time EMA-based rate tracking
  - `pushPerSec`, `pullPerSec`, `completePerSec`, `failPerSec`
  - O(1) per observation, zero GC pressure
  - Replaces placeholder zeros in `/stats` endpoint
  - New file: `src/application/throughputTracker.ts`
- **LOG_LEVEL Runtime Filtering** - `LOG_LEVEL` env var now works at runtime
  - Levels: `debug`, `info` (default), `warn`, `error`
  - Priority-based filtering with early return
- **39 new telemetry tests** across 5 test files:
  - `test/histogram.test.ts` (9 tests)
  - `test/latencyTracker.test.ts` (7 tests)
  - `test/perQueueMetrics.test.ts` (7 tests)
  - `test/throughputTracker.test.ts` (7 tests)
  - `test/telemetry-e2e.test.ts` (9 E2E integration tests)

### Changed
- `/stats` endpoint now returns real throughput and latency values
- Monitoring docs updated with per-queue metrics, histogram examples, and logging section
- HTTP API docs updated with new Prometheus output format

### Performance
- Telemetry overhead: ~0.003% (~25ns per operation via `Bun.nanoseconds()`)
- Benchmark results unchanged: 197K push/s (embedded), 39K push/s (TCP)

## [2.1.8] - 2026-02-06

### Fixed
- **pushJobBatch event emission** - `pushJobBatch` was silently dropping event broadcasts, causing subscribers and webhooks to miss all batch-pushed jobs. Added broadcast loop after batch insert to match single `pushJob` behavior.

### Added
- 4 regression tests for batch push event emission fix

### Changed
- Navbar simplified to show only logo without title text

## [2.1.7] - 2026-02-05

### Fixed
- **WriteBuffer silent data loss during shutdown** - `WriteBuffer.stop()` swallowed flush errors and silently dropped buffered jobs. Added `reportLostJobs()` to notify via `onCriticalError` callback when jobs cannot be persisted during shutdown.
- **Queue name consistency in TCP tests** - Fixed port hardcoding in queue-name-consistency test.

### Added
- **2,664 new tests across 37 files** - Comprehensive test coverage increase from 1,083 to 3,747 tests (+246%) with zero failures. Coverage spans core operations, data structures, managers, client TCP layer, server handlers, domain types, MCP handlers, and more.

## [2.1.6] - 2026-02-05

### Fixed
- **S3 backup hardening** - 10 bug fixes with 33 new tests:
  - Replace silent catch in cleanup with proper logging
  - Reject retention < 1 and intervalMs < 60s in config validation
  - Validate SQLite magic bytes before restore to prevent data corruption
  - Guard cleanup against retention=0 deleting all backups
  - Add S3 list pagination to handle >100 backups
  - Run WAL checkpoint before backup to include uncheckpointed data
  - Replace blocking gzipSync/gunzipSync with async CompressionStream
- **Flaky sandboxedWorker concurrent test** - Poll all 4 job results in parallel instead of sequentially to avoid exceeding the 5s test timeout.

### Added
- 33 new S3 backup tests covering config validation, backup/restore operations, cleanup, and manager lifecycle
- Documentation for gzip compression, SHA256 checksums, `.meta.json` files, scheduling details, AWS env var aliases, and restore safety notes

## [2.1.5] - 2026-02-05

### Fixed
- **uncaughtException and unhandledRejection handlers** - Previously, any uncaught error in background tasks or unhandled promise rejections would crash the server immediately without cleanup (write buffer not flushed, SQLite not closed, locks not released). Now the server performs graceful shutdown: logs the error with stack trace, stops TCP/HTTP servers, waits for active jobs, flushes the write buffer, and exits cleanly.
- Broken GitHub links in documentation (missing `/bunqueue` in paths)
- Stray separator in index.mdx causing build error

### Changed
- Migrated documentation from GitHub Pages to Vercel deployment
- SEO optimization across all 45 pages with improved titles and descriptions
- Documentation errors fixed, missing content added, and navbar modernized

## [2.1.4] - 2026-02-05

### Changed
- README split into Embedded and Server mode sections
- Added Docker server mode quick start with persistence documentation

## [2.1.3] - 2026-02-05

### Added
- **Type safety improvements** across client SDK
- Deployment modes section and fixed quick start examples in documentation

### Changed
- README improved with use cases, benchmarks, and BullMQ comparison

## [2.1.2] - 2026-02-04

### Fixed
- **Queue name consistency** - Fixed benchmark tests using different queue names for worker and queue in both embedded and TCP modes

### Changed
- Stats interval changed to 5 minutes with timestamp
- Removed verbose info/warn logs, keeping only errors
- Downgraded TypeScript to 5.7.3 for CI compatibility

### Added
- Queue name consistency tests to prevent regression
- Monitoring documentation added to sidebar Production section

## [2.1.1] - 2026-02-04

### Added
- **Prometheus + Grafana Monitoring Stack** - Complete observability setup:
  - Docker Compose profile for one-command monitoring deployment
  - Pre-configured Prometheus scraping with 5s interval
  - Comprehensive Grafana dashboard with 6 panel rows:
    - Overview: Waiting, Delayed, Active, Completed, DLQ, Workers, Cron, Uptime
    - Throughput: Jobs/sec graphs, queue depth over time
    - Success/Failure: Rate gauges, completed vs failed charts
    - Workers: Count, throughput, utilization gauge
    - Webhooks & Cron: Status and lifetime totals
    - Alerts: Visual indicators for DLQ, failure rate, backlog, workers
  - 8 pre-configured Prometheus alert rules:
    - `BunqueueDLQHigh` - DLQ > 100 for 5m (critical)
    - `BunqueueHighFailureRate` - Failure > 5% for 5m (warning)
    - `BunqueueQueueBacklog` - Waiting > 10k for 10m (warning)
    - `BunqueueNoWorkers` - No workers with waiting jobs (critical)
    - `BunqueueServerDown` - Server unreachable (critical)
    - `BunqueueLowThroughput` - < 1 job/s for 10m (warning)
    - `BunqueueWorkerOverload` - Utilization > 95% (warning)
    - `BunqueueJobsStuck` - Active jobs, no completions (warning)
- **Monitoring Documentation** - New guide at `/guide/monitoring/`

### Changed
- Docker Compose now supports `--profile monitoring` for optional stack

## [2.1.0] - 2026-02-04

### Performance
- **TCP Pipelining** - Major throughput improvement for TCP client operations:
  - Client-side: Multiple commands in flight per connection (up to 100 by default)
  - Server-side: Parallel command processing with `Promise.all()`
  - reqId-based response matching for correct command-response pairing
  - **125,000 ops/sec** in pipelining benchmarks (vs ~20,000 before)
  - Configurable via `pipelining: boolean` and `maxInFlight: number` options
- **SQLite indexes for high-throughput operations** - Added 4 new indexes for 30-50% faster queries:
  - `idx_jobs_state_started`: Stall detection now O(log n) instead of O(n) table scan
  - `idx_jobs_group_id`: Fast lookup for group operations
  - `idx_jobs_pending_priority`: Compound index for priority-ordered job retrieval
  - `idx_dlq_entered_at`: DLQ expiration cleanup now O(log n)
- **Date.now() caching in pull loop** - Reduced syscalls by caching timestamp per iteration (+3-5% throughput)

### Added
- **Hello command** for protocol version negotiation (`cmd: 'Hello'`)
- **Protocol version 2** with pipelining capability support
- **Semaphore utility** for server-side concurrency limiting (`src/shared/semaphore.ts`)
- Comprehensive pipelining test suites:
  - `test/protocol-reqid.test.ts` - 7 tests for reqId handling
  - `test/client-pipelining.test.ts` - 7 tests for client pipelining
  - `test/server-pipelining.test.ts` - 7 tests for server parallel processing
  - `test/backward-compat.test.ts` - 10 tests for backward compatibility
- **Fair benchmark comparison** (`bench/comparison/run.ts`):
  - Both bunqueue and BullMQ use identical parallel push strategy
  - Queue cleanup with `obliterate()` between tests
  - Results: **1.3x Push**, **3.2x Bulk Push**, **1.7x Process** vs BullMQ
- **Comprehensive benchmark** (`bench/comprehensive.ts`):
  - Embedded vs TCP mode comparison at scales [1K, 5K, 10K, 50K]
  - Log suppression for clean output
  - Peak results: **287K ops/sec** (Embedded Bulk), **149K ops/sec** (TCP Bulk)
  - Embedded mode is **2-4x faster** than TCP across all operations
- **New ConnectionOptions** - Added `pingInterval`, `commandTimeout`, `pipelining`, `maxInFlight` to public API

### Fixed
- **SQLITE_BUSY under high concurrency** - Added `PRAGMA busy_timeout = 5000` to wait for locks instead of failing immediately
- **"Database has closed" errors during shutdown** - Added `stopped` flag to WriteBuffer to prevent flush attempts after stop()
- **Critical: Worker pendingJobs race condition** - Concurrent `tryProcess()` calls could overwrite each other's job buffers, causing ~30% job loss under high concurrency. Now preserves existing buffered jobs when pulling new batches.
- **Connection options not passed through** - Worker, Queue, and FlowProducer now correctly pass `pingInterval`, `commandTimeout`, `pipelining`, and `maxInFlight` options to the TCP connection pool.

### Changed
- Schema version bumped to 5 (auto-migrates existing databases)
- TCP client now includes `reqId` in all commands for response matching
- Server processes multiple frames in parallel (max 50 concurrent per connection)
- **Documentation**: Rewrote comparison page with real benchmark data and methodology explanation

## [2.0.9] - 2026-02-03

### Fixed
- **Critical: Memory leak in EventsManager** - Cancelled waiters in `waitForJobCompletion()` were never removed from the `completionWaiters` map on timeout. Now properly cleaned up when timeout fires.
- **Critical: Lost notification TOCTOU race** - Fixed race condition in pull.ts where `notify()` could fire between `tryPullFromShard()` returning null and `waitForJob()` being called. Added `pendingNotification` flag to Shard to capture notifications when no waiters exist.
- **Critical: WriteBuffer data loss** - Added exponential backoff (100ms → 30s), max 10 retries, critical error callback, `stopGracefully()` method, and enhanced error callback with retry information. Previously, persistent errors caused infinite retries and shutdown lost pending jobs.
- **Critical: CustomIdMap race condition** - Concurrent pushes with same customId could create duplicates. Moved customIdMap check inside shard write lock for atomic check-and-insert.

### Added
- Comprehensive test suites for all bug fixes:
  - `test/bug-memory-leak-waiters.test.ts` - 5 tests verifying memory leak fix
  - `test/bug-lost-notification.test.ts` - 4 tests verifying notification fix
  - `test/bug-writebuffer-dataloss.test.ts` - 10 tests verifying WriteBuffer fix
  - `test/bug-verification-remaining.test.ts` - 7 tests verifying CustomId fix and JS concurrency model

## [2.0.3] - 2026-02-02

### Changed
- **Major refactor: Split queue.ts into modular architecture** (1955 → 485 lines)
  - Follows single responsibility principle with 14 focused modules
  - New modules: operations/add.ts, operations/counts.ts, operations/query.ts, operations/management.ts, operations/cleanup.ts, operations/control.ts
  - New modules: jobMove.ts, jobProxy.ts, bullmqCompat.ts, scheduler.ts, dlq.ts, stall.ts, rateLimit.ts, deduplication.ts, workers.ts, queueTypes.ts
  - All 894 unit tests, 25 TCP test suites, and 32 embedded test suites pass

### Fixed
- `getJob()` now properly awaits async manager.getJob() call
- `getJobCounts()` now uses queue-specific counts instead of global stats
- `promoteJobs()` implements correct iteration over delayed jobs
- `addBulk()` properly passes BullMQ v5 options (lifo, stackTraceLimit, keepLogs, etc.)
- `toPublicJob()` used for full job options support in getJob()
- `extendJobLock()` passes token parameter correctly

## [2.0.2] - 2026-02-02

### Fixed
- **Critical: Complete recovery logic for deduplication after restart** - Fixed all recovery scenarios that caused duplicate jobs after server restart:
  - **jobId deduplication** (`customIdMap`) - Now properly populated on recovery
  - **uniqueKey TTL deduplication** - Now restored with TTL settings via `registerUniqueKeyWithTtl()`
  - **Dependency recovery** - Now checks SQLite `job_results` table (not just in-memory `completedJobs`)
  - **Counter consistency** - Fixed `incrementQueued()` only called for main queue jobs, not `waitingDeps`

### Added
- `loadCompletedJobIds()` method in SQLite storage for dependency recovery
- `hasResult()` method to check if job result exists in SQLite
- Comprehensive recovery test suite (`test/recoveryLogic.test.ts`) with 8 tests covering all scenarios

## [2.0.1] - 2026-02-02

### Fixed
- **Critical: jobId deduplication not working after restart** - The `customIdMap` was not populated when recovering jobs from SQLite on server startup. This caused `getDeduplicationJobId()` to return `null` and allowed duplicate jobs with the same `jobId` to be created.

## [2.0.0] - 2026-02-02

### Added
- **Complete BullMQ v5 API Compatibility** - Full feature parity with BullMQ v5
  - **Worker Advanced Methods**
    - `rateLimit(expireTimeMs)` - Apply rate limiting to worker
    - `isRateLimited()` - Check if worker is currently rate limited
    - `startStalledCheckTimer()` - Start stalled job check timer
    - `delay(ms, abortController?)` - Delay worker processing with optional abort
  - **Job Advanced Methods**
    - `discard()` - Mark job as discarded
    - `getFailedChildrenValues()` - Get failed children job values
    - `getIgnoredChildrenFailures()` - Get ignored children failures
    - `removeChildDependency()` - Remove child dependency from parent
    - `removeDeduplicationKey()` - Remove deduplication key
    - `removeUnprocessedChildren()` - Remove unprocessed children jobs
  - **JobOptions**
    - `continueParentOnFailure` - Continue parent job when child fails
    - `ignoreDependencyOnFailure` - Ignore dependency on failure
    - `timestamp` - Custom job timestamp
  - **DeduplicationOptions**
    - `extend` - Extend TTL on duplicate
    - `replace` - Replace existing job on duplicate
- **Comprehensive Test Coverage** - 27 unit tests + 32 embedded script tests for new features

### Changed
- Major version bump to 2.0.0 reflecting complete BullMQ v5 compatibility
- Updated TypeScript types for all new features

## [1.9.9] - 2026-02-01

### Added
- **Comprehensive Functional Test Suite** - 28 new test files covering all major features
  - 14 embedded mode tests + 14 TCP mode tests
  - Tests for: advanced DLQ, job management, monitoring, rate limiting, stall detection, webhooks, queue groups, and more
  - All 24 embedded test suites pass (143/143 individual tests)

### Changed
- **BullMQ-Style Idempotency** - `jobId` option now returns existing job instead of throwing error
  - Duplicate job submissions are idempotent (same behavior as BullMQ)
  - Cleaner handling of retry scenarios without error handling
- Improved documentation for `jobId` deduplication behavior

### Fixed
- Embedded test suite now properly uses embedded mode (was incorrectly trying TCP)
- Fixed `getJobCounts()` in tests to use queue-specific `getJobs()` method
- Fixed async `getJob()` calls in job management tests
- Fixed PROMOTE, CHANGE PRIORITY, and MOVE TO DELAYED test logic

## [1.9.8] - 2026-01-31

### Changed
- **msgpackr Binary Protocol** - Switched TCP protocol from JSON to msgpackr binary
  - ~30% faster serialization/deserialization
  - Smaller message sizes

## [1.9.6] - 2026-01-31

### Added
- **Durable Writes** - New `durable: true` option for critical jobs
  - Bypasses write buffer for immediate disk persistence
  - Guarantees no data loss on process crash
  - Use for payments, orders, and critical events

### Changed
- **Reduced write buffer flush interval** from 50ms to 10ms
  - Smaller data loss window for non-durable jobs
  - Better balance between throughput and safety

## [1.9.4] - 2026-01-31

### Added
- **5 BullMQ-Compatible Features**
  - **Timezone support for cron jobs** - IANA timezones (e.g., "Europe/Rome", "America/New_York")
  - **`getCountsPerPriority()`** - Get job counts grouped by priority level
  - **`getJobs()` with pagination** - Filter by state, paginate with `start`/`end`, sort with `asc`
  - **`retryCompleted()`** - Re-queue completed jobs for reprocessing
  - **Advanced deduplication** - TTL-based unique keys with `extend` and `replace` strategies

### Changed
- **Documentation improvements**
  - Clear comparison table for Embedded vs TCP Server modes
  - Danger box warning about mixed modes causing "Command timeout" error
  - Added "Connecting from Client" section to Server guide

## [1.9.3] - 2026-01-31

### Added
- **Unix Socket Support** - TCP and HTTP servers can now bind to Unix sockets
  - Configure via `TCP_SOCKET_PATH` and `HTTP_SOCKET_PATH` environment variables
  - CLI flags `--tcp-socket` and `--http-socket`
  - Lower latency for local connections
- Socket status line in startup banner

### Fixed
- Test alignment for shard drain return type

## [1.9.2] - 2026-01-30

### Fixed
- **Critical Memory Leak** - Resolved `temporalIndex` leak causing 5.5M object retention after 1M jobs
  - Added `cleanOrphanedTemporalEntries()` method to Shard
  - Memory now properly released after job completion with `removeOnComplete: true`
  - `heapUsed` drops to ~6MB after processing (vs 264MB before fix)

### Changed
- Improved error logging in ackBatcher flush operations

## [1.9.1] - 2026-01-29

### Added
- **Two-Phase Stall Detection** - BullMQ-style stall detection to prevent false positives
  - Jobs marked as candidates on first check, confirmed stalled on second
  - Prevents requeuing jobs that complete between checks
- `stallTimeout` support in client push options
- Advanced health checks for TCP connections

### Fixed
- Defensive checks and cleanup for TCP pool and worker
- Server banner alignment between CLI and main.ts

### Changed
- Modularized client code into separate TCP, Worker, Queue, and Sandboxed modules

## [1.9.0] - 2026-01-28

### Added
- **TCP Client** - High-performance TCP client for remote server connections
  - Connection pooling with configurable pool size
  - Heartbeat keepalive mechanism
  - Batch pull/ACK operations (PULLB, ACKB with results)
  - Long polling support
  - Ping/pong health checks
- 4.7x faster push throughput with optimized TCP client

### Changed
- Connection pool enabled by default for TCP clients
- Improved ESLint compliance across TCP client code

## [1.6.8] - 2026-01-27

### Fixed
- Renamed bunq to bunqueue in Dockerfile
- CLI version now read dynamically from package.json

### Changed
- Centralized version in `shared/version.ts`

## [1.6.7] - 2026-01-26

### Added
- Dynamic version badge in documentation
- Mobile-responsive layout improvements
- Comprehensive stress tests

## [1.6.6] - 2026-01-25

### Fixed
- Counter updates when recovering jobs from SQLite on restart

## [1.6.5] - 2026-01-24

### Fixed
- Production readiness improvements with critical fixes

## [1.6.4] - 2026-01-23

### Fixed
- SQLite persistence for DLQ entries
- Client SDK persistence issues

## [1.6.3] - 2026-01-22

### Added
- **MCP Server** - Model Context Protocol server for AI assistant integration
  - Queue management tools for Claude, Cursor, and other AI assistants
  - BigInt serialization handling in stats

### Fixed
- Deployment guide documentation corrections

## [1.6.2] - 2026-01-21

### Added
- **SandboxedWorker** - Isolated worker processes for crash protection
- Hono and Elysia integration guides
- Section-specific OG images and sitemap

### Changed
- Enhanced SEO with Open Graph and Twitter meta tags
- Improved mobile responsiveness in documentation

## [1.6.1] - 2026-01-20

### Added
- Bunny ASCII art in server startup and CLI help
- Professional benchmark charts using QuickChart.io
- BullMQ vs bunqueue comparison benchmarks

### Changed
- Optimized event subscriptions and batch operations
- Replaced Math.random UUID with Bun.randomUUIDv7 (10x faster)
- High-impact algorithm optimizations

## [1.6.0] - 2026-01-19

### Added
- **Stall Detection** - Automatic recovery of unresponsive jobs
  - Configurable stall interval and max stalls
  - Grace period after job start
  - Automatic retry or move to DLQ
- **Advanced DLQ** - Enhanced Dead Letter Queue
  - Full metadata (reason, error, attempt history)
  - Auto-retry with exponential backoff
  - Filtering by reason, age, retriability
  - Statistics endpoint
  - Auto-purge expired entries
- **Worker Heartbeats** - Configurable heartbeat interval
- **Repeatable Jobs** - Support for recurring jobs with intervals or limits
- **Flow Producer** - Parent-child job relationships
- **Queue Groups** - Bulk operations across multiple queues

### Changed
- Updated banner to "written in TypeScript"
- Version now read from package.json dynamically

### Fixed
- DLQ entry return type consistency

## [1.5.0] - 2026-01-15

### Added
- S3 backup with configurable retention
- Support for Cloudflare R2, MinIO, DigitalOcean Spaces
- Backup CLI commands (now, list, restore, status)

### Changed
- Improved backup compression
- Better error messages for S3 configuration

## [1.4.0] - 2026-01-10

### Added
- Rate limiting per queue
- Concurrency limiting per queue
- Prometheus metrics endpoint
- Health check endpoint

### Changed
- Optimized batch operations (3x faster)
- Reduced memory usage for large queues

## [1.3.0] - 2026-01-05

### Added
- Cron job scheduling
- Webhook notifications
- Job progress tracking
- Job logs

### Fixed
- Memory leak in event listeners
- Race condition in batch acknowledgment

## [1.2.0] - 2025-12-28

### Added
- Priority queues
- Delayed jobs
- Retry with exponential backoff
- Job timeout

### Changed
- Improved SQLite schema with indexes
- Better error handling

## [1.1.0] - 2025-12-20

### Added
- TCP protocol for high-performance clients
- HTTP API with WebSocket support
- Authentication tokens
- CORS configuration

## [1.0.0] - 2025-12-15

### Added
- Initial release
- Queue and Worker classes
- SQLite persistence with WAL mode
- Basic DLQ support
- CLI for server and client operations
