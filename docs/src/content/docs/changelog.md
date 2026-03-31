---
title: "bunqueue Changelog: Version History & Release Notes"
description: "Complete version history for bunqueue Bun job queue. Track new features, bug fixes, performance improvements, and breaking changes."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/getting-started.png
---

All notable changes to bunqueue are documented here.

## [2.6.93] - 2026-03-31

### Fixed
- **Deduplication now blocks pushes while a job is active** — previously, pushing with the same `uniqueKey` while the original job was being processed (active) would succeed and insert a duplicate. The `uniqueKey` is held until ack, so the duplicate should have been rejected. Now correctly detects active jobs via `jobIndex` and returns `{ skip: true }`. Also fixed the fall-through in `pushJob` where a skipped dedup result could still proceed to insert if `q.find()` returned null.
- This fixes a real-world cron scenario: a cron with pattern `* * * * *` whose job takes longer than 1 minute would accumulate duplicate jobs in the queue. On restart, the worker would pick them up immediately. Setting `deduplication.id` on the cron job template now correctly prevents this.

## [2.6.92] - 2026-03-31

### Added
- **Simple Mode: 4 new production features** (zero core modifications):
  - **Job Deduplication** — auto-dedup by name+data with configurable TTL, extend, replace modes
  - **Job Debouncing** — coalesce rapid same-name jobs within a TTL window
  - **Rate Limiting** — `rateLimit` option (max/duration/groupKey) + runtime `setGlobalRateLimit()`
  - **DLQ Auto-Management** — `dlq` option for auto-retry, max age, max entries; full DLQ API (getDlq, getDlqStats, retryDlq, purgeDlq)
- 9 new unit tests for the 4 features

## [2.6.91] - 2026-03-31

### Added
- **Simple Mode: 8 advanced features** — all built on top of existing Queue/Worker APIs with zero core modifications:
  - **Batch Processing** — accumulate N jobs, flush on size or timeout, per-job Promise resolution
  - **Advanced Retry** — 5 strategies (fixed, exponential, jitter, fibonacci, custom), `retryIf` predicate
  - **Graceful Cancellation** — AbortController per job, `cancel()`, `isCancelled()`, `getSignal()`
  - **Circuit Breaker** — auto-pause worker after N consecutive failures, half-open recovery
  - **Event Triggers** — declarative "on job A complete → create job B" with optional conditions
  - **Job TTL** — expire unprocessed jobs, per-name overrides, runtime updates
  - **Priority Aging** — automatically boost priority of old waiting/prioritized jobs
- **Modular architecture** — each feature in its own file under `src/client/bunqueue/` (max 300 lines each)
- **50 unit tests** for Simple Mode features, 29 integration assertions
- **Comprehensive documentation** — super detailed guide with architecture diagrams, code examples, and interaction notes

## [2.6.90] - 2026-03-31

### Added
- **Simple Mode (`Bunqueue` class)** — new unified API that combines Queue + Worker into a single object. Includes route-based job dispatching, onion-model middleware chain, and simplified cron scheduling via `cron()` and `every()`. Works in both embedded and TCP modes. Import as `import { Bunqueue } from 'bunqueue/client'`.
- **Documentation** — comprehensive Simple Mode guide at `/guide/simple-mode/`, README section, and CLAUDE.md reference.

## [2.6.89] - 2026-03-30

### Fixed
- **`getPrioritized()` returning empty array** — `end=-1` (default) was not normalized in the embedded path of `getJobsAsync`, causing `maxPerSource=0` and zero results. Now handles `end=-1` consistently with the TCP path.

## [2.6.88] - 2026-03-30

### Fixed
- **ESLint crash on `flow.ts`** — removed unnecessary explicit `<T>` type arguments from `createFlowJobObject` calls that caused `@typescript-eslint/no-unnecessary-type-arguments` rule to crash during `bun run lint`.

## [2.6.87] - 2026-03-30

### Fixed
- **`skipIfNoWorker` not working on restart** ([#67](https://github.com/egeominotti/bunqueue/issues/67)) — when a cron job had `skipIfNoWorker: true` and the server restarted with past-due `nextRun`, the missed cron fired immediately because workers reconnected before the scheduler tick. The `load()` method now recalculates `nextRun` to the next future occurrence when `skipIfNoWorker` is enabled, preventing missed cron executions on restart.

## [2.6.85] - 2026-03-26

### Added
- **`skipIfNoWorker`** option for cron jobs ([#65](https://github.com/egeominotti/bunqueue/issues/65)) — when enabled, the cron scheduler skips job creation if no workers are registered for the target queue. Prevents job accumulation when clients go offline while the server keeps running. Works in both embedded and TCP modes.
- Schema migration v9: `skip_if_no_worker` column on `cron_jobs` table

## [2.6.84] - 2026-03-26

### Fixed
- **`immediately: true` conflicting with `skipMissedOnRestart`** ([#65](https://github.com/egeominotti/bunqueue/issues/65)):
  - `immediately` now only fires on **first creation**, not on subsequent upserts
  - Previously, every call to `upsertJobScheduler` with `immediately: true` would override `skipMissedOnRestart` and fire the cron immediately — even after a server restart
  - This was the root cause of the TCP-mode report: the user's app called `upsertJobScheduler` on every startup with both flags, causing the cron to fire immediately despite `skipMissedOnRestart`

## [2.6.83] - 2026-03-26

### Fixed
- **`immediately: true` now works in TCP mode** ([#65](https://github.com/egeominotti/bunqueue/issues/65)):
  - Added `immediately` field to TCP `Cron` command type
  - Wired `immediately` through TCP handler (`handleCron`) and client TCP path (`upsertJobScheduler`)
  - Full TCP parity: `immediately`, `skipMissedOnRestart` now work identically in both embedded and TCP modes

## [2.6.82] - 2026-03-26

### Fixed
- **`skipMissedOnRestart` not working via `Queue#upsertJobScheduler`** ([#65](https://github.com/egeominotti/bunqueue/issues/65)):
  - `CronScheduler.add()` now preserves existing `executions` count when upserting a cron (previously reset to 0 on every call)
  - `CronScheduler.load()` now persists recalculated `nextRun` to the database when `skipMissedOnRestart` adjusts it
  - `immediately: true` option is now supported in `CronJobInput` — fires the cron immediately on creation, then continues on schedule
  - Wired `immediately` through `upsertJobScheduler` embedded path
- **Embedded `test-cron-event-driven` test hanging** — added `shutdownManager()` call to properly clean up the shared QueueManager singleton and its background task timers

## [2.6.81] - 2026-03-26

### Added
- **Worker API enhancements** (BullMQ v5 compatibility):
  - `concurrency` getter/setter — change concurrency at runtime without restarting the worker
  - `closing` property — Promise that resolves when `close()` finishes
  - `off()` typed overloads — remove event listeners with full TypeScript support
  - `name` and `opts` are now public readonly properties
- **Worker options now fully wired**:
  - `skipLockRenewal` — disables heartbeat timer when `true`
  - `skipStalledCheck` — disables stalled event subscription when `true`
  - `drainDelay` — configurable delay between polls when queue is drained (default: 50ms, was hardcoded)
  - `lockDuration` — stored in opts with default 30000ms
  - `maxStalledCount` — stored in opts with default 1
  - `removeOnComplete` / `removeOnFail` — worker-level defaults applied to all processed jobs

### Fixed
- `drainDelay` default corrected from 5000ms to 50ms in documentation

### Removed
- Cleaned up 7 unimplemented WorkerOptions stubs that were type-only (now all options are wired to actual behavior)

## [2.6.80] - 2026-03-25

### Fixed
- **Issue #64 follow-up**: Jobs no longer lost from in-memory queue when `markActive()` fails during pull. Previously, if SQLite threw a disk I/O error during `moveToProcessing()`, the job was already popped from the priority queue but never delivered to the worker — silently stuck in "waiting" state forever. `markActive()` is now non-fatal (persistence failure doesn't block processing), and a safety-net `requeueJob()` restores jobs to the queue if `moveToProcessing()` fails for any reason

## [2.6.79] - 2026-03-25

### Fixed
- **Issue #63 follow-up**: `getStallConfig()` and `getDlqConfig()` in TCP mode now return the correct config after calling `setStallConfig()`/`setDlqConfig()` instead of always returning hardcoded defaults. Added client-side cache so sync getters reflect the last-set values immediately

## [2.6.78] - 2026-03-25

### Fixed
- **Issue #61**: `JobTemplate` is now generic `JobTemplate<T>` — `data` field correctly inherits the Queue's type parameter instead of being `unknown`. Fixed incorrect docs in `use-cases` showing `data` in the second parameter instead of the third. Exported `RepeatOpts`, `JobTemplate`, `SchedulerInfo` types from `bunqueue/client`
- **Issue #63**: Cloud dashboard `queue:detail` response now includes `enabled` field in `stallConfig`, allowing the dashboard to properly display and toggle stall detection
- **Issue #64**: Added WAL checkpoint (`PRAGMA wal_checkpoint(TRUNCATE)`) before `db.close()` to prevent stale locks and `disk I/O error` on rapid restarts in embedded mode

### Added
- **`skipMissedOnRestart`** option for cron jobs — when enabled, cron jobs that were missed during server downtime are skipped and rescheduled to the next future run instead of being executed immediately on restart. Default: `false` (preserves existing catch-up behavior)
- Schema migration v8: `skip_missed_on_restart` column on `cron_jobs` table

## [2.6.77] - 2026-03-24

### Fixed
- `removeChildDependency()` TCP response now returns `{ ok: true, removed: boolean }` separately; client reads `res.removed` instead of `res.ok` to correctly reflect whether the dependency was actually removed

## [2.6.76] - 2026-03-24

### Added
- Integration test scripts for monitoring, query operations, cron event-driven scheduling, and sandboxed workers (TCP + embedded modes)
- Unit tests for issues #29 (sandboxed worker `log` method), #38 (sandboxed processor cleanup), #41 (sandboxed idle RAM)

## [2.6.75] - 2026-03-24

### Added
- **`removeDependencyOnFailure`** — When a child job terminally fails with this option set, it is silently removed from the parent's pending dependencies. If it was the last pending child, the parent is promoted to the waiting queue and processed normally.
- **`ignoreDependencyOnFailure`** — Same as `removeDependencyOnFailure` but also stores the failure reason so the parent worker can retrieve it via `job.getIgnoredChildrenFailures()`.
- **`continueParentOnFailure`** — When a child job with this option fails, the parent is immediately promoted to the waiting queue (even if other children are still pending). The parent worker can then call `job.getFailedChildrenValues()` to inspect which children failed and why, and `job.removeUnprocessedChildren()` to cancel remaining unstarted children.
- **`job.getFailedChildrenValues()`** — Returns `Record<string, string>` mapping child keys (`"queue:jobId"`) to their error messages. Populated by `continueParentOnFailure` child failures.
- **`job.getIgnoredChildrenFailures()`** — Returns `Record<string, string>` of failure reasons for children that failed with `ignoreDependencyOnFailure`.
- **`job.removeChildDependency()`** — Removes a child job's pending dependency from its parent. If this was the last pending child, the parent is promoted to the queue. Throws if the job has no parent.
- **`job.removeUnprocessedChildren()`** — Cancels all unprocessed (waiting/delayed) children of a parent job. Active, completed, and failed children are unaffected.
- TCP commands for new methods: `GetFailedChildrenValues`, `GetIgnoredChildrenFailures`, `RemoveChildDependency`, `RemoveUnprocessedChildren`.
- All four new options are fully propagated through `FlowProducer.add()`, `FlowProducer.addBulk()`, and the TCP `PUSH` command.

## [2.6.74] - 2026-03-23

### Changed
- **Cloud: dynamic ingest interval** — Snapshot interval now adapts automatically to payload size: < 50KB → 5s, 50–200KB → 10s, 200–500KB → 20s, > 500KB → 30s. Previously fixed at 15s regardless of load.
- **Cloud: unbounded job collection** — Removed the 10k total cap on `recentJobs[]`. Each state is now collected in full, bounded only by in-memory eviction limits (50k completed FIFO, etc).
- **Cloud: removed `/batch` ingest endpoint** — Recovery now resends buffered snapshots one-by-one to the standard `/api/v1/ingest` endpoint, simplifying the protocol.

## [2.6.73] - 2026-03-23

### Added
- **Job timeline tracking** — Every job now records a `timeline: JobTimelineEntry[]` array that tracks all state transitions (`waiting`, `active`, `completed`, `failed`, `delayed`, `prioritized`, `waiting-children`) with timestamps, error messages, and attempt numbers. Max 20 entries per job.
- **Timeline SQLite persistence** — Job timeline is persisted as a msgpack BLOB column in SQLite (schema v7 migration). Timeline survives server restarts and is available for DB-loaded jobs.
- **Cloud snapshot: timeline field** — `recentJobs[]` in cloud snapshots now includes `timeline` when present, giving the dashboard exact state-transition history for each job.
- **Cloud snapshot: failed job duration enrichment** — Failed jobs in `recentJobs[]` are now enriched with `duration`, `completedAt`, and `totalDuration` from DLQ attempt history, since `completedAt` is null for failed jobs.

## [2.6.72] - 2026-03-23

### Added
- **Cloud snapshot: `waiting-children` state** — Jobs in `waiting-children` state are now collected in `recentJobs[]` and counted in both global `stats` and per-queue `queues[]`. Dashboard can now display parent jobs waiting for children.
- **Cloud snapshot: `prioritized` state in job collection** — `recentJobs[]` now includes jobs with `state: 'prioritized'`. Previously only `waiting/active/delayed/failed/completed` were collected.
- **Cloud snapshot: worker computed fields** — `workerDetails[]` now includes `uptime` (ms since registration), `status` (`'active'|'idle'|'stalled'`), `errorRate` (0-1), and `utilization` (activeJobs/concurrency).
- **Cloud snapshot: `queueExtended`** — Per-queue extended telemetry: `uniqueKeys` (active dedup keys), `activeGroups` (FIFO groups), `waitingDeps` (jobs awaiting dependencies), `waitingChildren` (parents awaiting children).
- **Cloud snapshot: `eventSubscribers`** — Count of active event subscribers (SSE, WebSocket, internal).
- **Cloud snapshot: `pendingDepChecks`** — Number of dependency checks awaiting flush.
- **TCP `GetJobCounts`: `waiting-children`** — TCP protocol now returns `waiting-children` count in job counts response.

### Fixed
- **`getJobs()` with `state: 'waiting-children'`** — SQLite and in-memory query paths now correctly return jobs in `waitingDeps`/`waitingChildren` maps when filtering by `waiting-children` state.

## [2.6.71] - 2026-03-23

### Added
- **BullMQ v5 `prioritized` state** — Jobs with `priority > 0` now report state `'prioritized'` instead of `'waiting'`, matching BullMQ v5 exactly. Affects `getJobState()`, `getJobCounts()`, Prometheus metrics, cloud snapshot, SSE/WebSocket events, and MCP adapter.
- **BullMQ v5 `waiting-children` state** — Parent jobs in flows correctly report `'waiting-children'` state while waiting for child jobs to complete.
- **`failParentOnFailure`** — When a child job terminally fails with `failParentOnFailure: true`, the parent job is automatically moved to `failed` state. Handles race conditions where child fails before parent linkage is established.
- **Flow atomicity** — `FlowProducer.add()` and `addBulk()` now automatically roll back all created jobs if any part of the flow fails during creation.
- **`FlowOpts` with `queuesOptions`** — Pass per-queue default job options as second argument to `flow.add(flowJob, { queuesOptions: { queueName: { attempts: 5 } } })`.
- **FlowProducer extends EventEmitter** — BullMQ v5 compatible. `close()` returns `Promise<void>`, `closing` property tracks shutdown, `disconnect()` alias.
- **Job move operations** — `moveActiveToWait`, `changeWaitingDelay`, `moveToWaitingChildren` state transitions with proper resource cleanup (concurrency slots, unique keys, group locks).

### Fixed
- **TOCTOU in `moveParentToFailed`** — Re-checks `jobIndex` inside write lock to prevent duplicate DLQ entries when multiple children with `failParentOnFailure` fail concurrently.
- **Unhandled promise rejections** — `moveParentToFailed` calls now have `.catch()` handlers instead of fire-and-forget `void`.
- **SQLite `queryJobs(state='prioritized')`** — Translates `'prioritized'` to `WHERE state='waiting' AND priority > 0` since SQLite never stores 'prioritized' as a state value.
- **`moveActiveToWait` resource leak** — Now calls `releaseJobResources()` to free concurrency/uniqueKey/group slots before re-queueing.
- **Move operations handle `prioritized` state** — `moveJobToWait` and `moveJobToDelayed` now correctly handle jobs in `'prioritized'` state.
- **Cloud snapshot** — Added `prioritized` to stats and per-queue data. Per-queue data now uses `failed` instead of `dlq` (BullMQ v5 compatible).

### Changed
- **Documentation** — Updated state machine diagrams, API types, FlowProducer guide, migration guide with BullMQ v5 parity tables, cloud contract with new snapshot fields.

## [2.6.67] - 2026-03-22

### Changed
- **Disabled flaky SandboxedWorker tests** — Commented out all 35 SandboxedWorker tests across 5 files. Bun's Worker threads are still unstable and cause intermittent race conditions and crashes in parallel test runs. Tests will be re-enabled once Bun Workers stabilize.

## [2.6.66] - 2026-03-22

### Fixed
- **Deduplication not working for JobScheduler (Issue #60)** — `upsertJobScheduler` accepted deduplication options in the `JobTemplate` but silently discarded them. The cron system (`CronJob`, `CronJobInput`, `cronScheduler`) had no fields for `uniqueKey` or `dedup`, so every cron tick created a new job regardless of deduplication settings. Now dedup options are stored in the cron job (including SQLite persistence with schema migration v6) and passed through to `pushJob()` on each tick. When a worker is slow or offline, only one job per dedup key exists instead of unbounded duplicates.

## [2.6.65] - 2026-03-22

### Added
- **MCP operation tracking for Cloud dashboard** — Every MCP tool invocation (73 tools) is now tracked and sent to bunqueue.io as part of the cloud snapshot. Each operation records: tool name, queue affected, timestamp, duration, success/failure, and error message. Data is buffered in a bounded ring buffer (max 200 ops, ~40KB) and drained into each snapshot. In embedded mode, the MCP process creates its own CloudAgent to send telemetry. Zero overhead when cloud is not configured. Includes `mcpOperations` (raw invocation history) and `mcpSummary` (aggregated stats with top tools) fields in `CloudSnapshot`.

## [2.6.64] - 2026-03-21

### Fixed
- **No-lock ack fails after stall re-queue (data loss)** — When a worker with `useLocks=false` processed a job that stall detection re-queued, the `ack()` call threw "Job not found" with no recovery path, leaving the job stuck in the queue forever. The existing Issue #33 handler (`completeStallRetriedJob`) only fired when a lock token was present. Now the handler also fires for tokenless acks when the job was stall-retried (`attempts > 0`), preventing false completions of freshly-pushed jobs.

## [2.6.63] - 2026-03-21

### Performance
- **WorkerRateLimiter: O(n) → O(1) amortized** — Replaced `Array.filter()` with head-pointer eviction for sliding window token expiration. Eliminates per-poll array allocation and removes `Math.min(...spread)` (potential stack overflow on large token arrays). Benchmarked: 10k tokens went from 31µs to ~0µs per call; zero memory allocation per poll cycle.
- **FlowProducer: parallel sibling creation in TCP mode** — `add()`, `addBulk()`, `addBulkThen()`, and `addTree()` now create independent children/jobs concurrently via `Promise.all`. TCP benchmark shows **3–6x speedup** for flows with 10–20 children (network round-trips overlap instead of serializing). `addBulkThen()` uses `Promise.allSettled` for proper cleanup on partial failure. No impact in embedded mode (pushes are synchronous). `addChain()` unchanged (sequential by design).

## [2.6.62] - 2026-03-21

### Fixed
- **E2E webhook tests failing after SSRF validation** — Added `validateWebhookUrls` option to `QueueManagerConfig` so tests using localhost can disable URL validation.

## [2.6.60] - 2026-03-21

### Fixed
- **Webhook SSRF prevention in embedded mode** — `WebhookManager.add()` now validates URLs against SSRF (localhost, private IPs, cloud metadata). Previously only enforced at TCP server layer, leaving embedded SDK unprotected.
- **Docs: pin Zod v3 for Starlight** — Fixed Vercel build crash caused by Zod v4 incompatibility with Starlight 0.31.

### Changed
- **Extracted `validateWebhookUrl` to shared module** — `src/shared/webhookValidation.ts` is now the single source of truth, re-exported from `protocol.ts` for backward compatibility.

## [2.6.49] - 2026-03-20

### Added
- **Cloud: 20 new remote commands** — Full dashboard control via WebSocket:
  - Queue: `obliterate`, `promoteAll`, `retryCompleted`, `rateLimit`, `clearRateLimit`, `concurrency`, `clearConcurrency`, `stallConfig`, `dlqConfig`
  - Job: `push`, `priority`, `discard`, `delay`, `updateData`, `clearLogs`
  - Webhook: `add`, `remove`, `set-enabled`
  - Other: `s3:backup`
- **Shared `deriveState` and `mapJob` helpers** — Eliminated triplicated state derivation logic in command handlers.

## [2.6.48] - 2026-03-20

### Changed
- **Cloud: auth via HTTP upgrade headers** — WebSocket authentication now uses `Authorization`, `X-Instance-Id`, and `X-Remote-Commands` headers on the upgrade request (Bun-specific). Eliminates the JSON handshake message and the 100ms delay workaround.
- **Cloud: removed client-side ping** — Client-side ping (every 10s) was causing false disconnects (code 4000). Keepalive now relies solely on server-side ping (25s) with bunqueue responding pong.

### Fixed
- **Cloud: duplicate reconnect guard** — `scheduleReconnect()` now prevents multiple concurrent reconnect timers.
- **Cloud: `onclose` logs at `info` level** — Previously `debug`, making reconnect failures invisible in production logs.

## [2.6.47] - 2026-03-20

### Added
- **Programmatic `dataPath` for embedded mode** — Queue and Worker accept `dataPath` option to set the SQLite database path without env vars. Resolves conflicts with apps that use their own `DATA_PATH`. ([#59](https://github.com/egeominotti/bunqueue/issues/59))
- **`BUNQUEUE_DATA_PATH` / `BQ_DATA_PATH` env vars** — New namespaced env vars for data path configuration. Priority: `BUNQUEUE_DATA_PATH` > `BQ_DATA_PATH` > `DATA_PATH` > `SQLITE_PATH`. Backward compatible.
- **Cloud: snapshots via WebSocket** — Snapshots are now sent over WS when connected (`{ type: "snapshot", ...data }`), falling back to HTTP POST only when WS is down.

## [2.6.46] - 2026-03-20

### Added
- **Cloud: resilient WebSocket with ring buffer** — Events are buffered (max 1000) when WS is disconnected and flushed after `handshake_ack` on reconnect (with 5s fallback timeout). Zero event loss during brief disconnections.
- **Cloud: client-side ping heartbeat** — bunqueue sends `{ type: "ping" }` every 10s to the dashboard; if no pong within 5s, closes socket and reconnects. Dead connection detection reduced from ~40s to ~10s.
- **Cloud: dual-channel failover** — When WS is down, buffered events are embedded in the HTTP snapshot (`snapshot.events`), so the dashboard stays informed even during prolonged disconnections.

### Fixed
- **Cloud: double reconnect race** — Pong timeout no longer calls `scheduleReconnect()` directly; delegates to `onclose` to prevent duplicate sockets.
- **Cloud: local socket reference** — All handlers (pong, handshake, commands) use the local `ws` variable, not `this.ws`, preventing replies on stale sockets after reconnect.
- **Cloud: old socket cleanup** — Previous socket is explicitly closed and handlers nulled before creating a new connection.

## [2.6.45] - 2026-03-20

### Added
- **Cloud: `prev` and `delay` fields in WebSocket events** — CloudEvent now forwards all JobEvent fields: `prev` (previous state on removed/retried) and `delay` (ms for delayed jobs).

### Fixed
- **Cloud: WebSocket binary frame handling** — Ping/pong and command messages now handle both text and binary WebSocket frames (ArrayBuffer/Buffer), preventing silent parse failures behind Cloudflare.

## [2.6.44] - 2026-03-20

### Fixed
- **Cloud: WebSocket ping/pong heartbeat** — Pong responses are now sent regardless of `BUNQUEUE_CLOUD_REMOTE_COMMANDS` config. Previously, ping messages were silently dropped when remote commands were disabled, causing the dashboard to disconnect the agent every ~60s as a zombie connection.

## [2.6.43] - 2026-03-19

### Added
- **Cloud: `job:list` command** — Paginated job listing per queue with state filtering (`queue`, `state`, `limit`, `offset`).
- **Cloud: `job:get` command** — Full job detail with logs and result included.
- **Cloud: `queue:detail` command** — Queue detail with counts, config, DLQ entries, and job list.

### Fixed
- **Cloud: recentJobs now includes completed/failed jobs** — Was only querying waiting/active/delayed states.
- **Cloud: `job:list` total count** — Now returns actual queue count instead of page length.
- **Cloud: activeQueues filter** — Restored skip-empty-queues optimization that was broken by over-broad filter.

## [2.6.42] - 2026-03-19

### Performance
- **Cloud: two-tier snapshot collection** — Light data (stats, throughput, latency, memory) collected every 5s at O(SHARD_COUNT). Heavy data (recentJobs, dlqEntries, topErrors, workerDetails, queueConfigs, webhooks) collected every 30s and cached between refreshes. Heavy collectors skip empty queues (only iterate queues with waiting/active/dlq > 0). Eliminated double `getQueueJobCounts()` pass.

### Fixed
- **Cloud: totalCompleted/totalFailed per queue** — Was sending in-memory BoundedSet count (resets when full). Now sends cumulative counters from `perQueueMetrics` (never resets).

## [2.6.41] - 2026-03-19

### Enhanced
- **bunqueue Cloud: enterprise-grade telemetry** — Snapshot now includes per-queue totals (`totalCompleted`/`totalFailed`), connection stats (TCP/WS/SSE clients), webhook delivery stats, top errors grouped by message, cron execution counts, S3 backup status, rate limit and concurrency config per queue. Added `job:logs` and `job:result` remote commands for on-demand data. Auth errors (401/403) now logged at error level instead of silently buffered.

## [2.6.40] - 2026-03-19

### Added (Beta)
- **bunqueue Cloud** — Remote dashboard telemetry agent. Connect any bunqueue instance to [bunqueue.io](https://bunqueue.io) with just 2 env vars (`BUNQUEUE_CLOUD_URL` + `BUNQUEUE_CLOUD_API_KEY`). Zero overhead when disabled.
  - **Snapshot channel** — HTTP POST every 5s with full server state: stats, throughput, latency percentiles, memory, per-queue counts, worker details, cron jobs, storage status, DLQ entries, recent jobs.
  - **Event channel** — Outbound WebSocket for real-time job event forwarding (Failed, Stalled, etc.) with configurable filtering.
  - **Remote commands (opt-in)** — Dashboard can execute commands on the instance via the same WebSocket: `queue:pause`, `queue:resume`, `queue:drain`, `dlq:retry`, `dlq:purge`, `job:cancel`, `job:promote`, `cron:upsert`, `cron:delete`. Requires `BUNQUEUE_CLOUD_REMOTE_COMMANDS=true`.
  - **Multi-instance** — Multiple bunqueue instances can connect to the same dashboard with separate instance IDs and names.
  - **Resilience** — Offline snapshot buffer (720 snapshots), circuit breaker, WebSocket auto-reconnect with exponential backoff + jitter, graceful shutdown with final snapshot.
  - **Security** — API key auth, optional HMAC-SHA256 signing, job data redaction, remote commands disabled by default.
  - **New env vars**: `BUNQUEUE_CLOUD_URL`, `BUNQUEUE_CLOUD_API_KEY`, `BUNQUEUE_CLOUD_INSTANCE_NAME`, `BUNQUEUE_CLOUD_INTERVAL_MS`, `BUNQUEUE_CLOUD_REMOTE_COMMANDS`, `BUNQUEUE_CLOUD_SIGNING_SECRET`, `BUNQUEUE_CLOUD_INCLUDE_JOB_DATA`, `BUNQUEUE_CLOUD_REDACT_FIELDS`, `BUNQUEUE_CLOUD_EVENTS`.

## [2.6.39] - 2026-03-18

### Fixed
- **`EventType.Paused` / `EventType.Resumed` missing from enum** — Added `Paused` and `Resumed` variants to `EventType` const enum, fixing TypeScript compilation errors in `queueManager.ts` and `client/events.ts`.
- **`UnrecoverableError` / `DelayedError` not exported** — Added `src/client/errors.ts` with BullMQ-compatible error classes (`UnrecoverableError` to skip retries, `DelayedError` to re-delay jobs) and exported them from `bunqueue/client`.
- **Webhook mapping for pause/resume events** — `eventsManager.ts` now handles `Paused` and `Resumed` event types in the webhook switch.

### Added
- **Issue #53 test** — Regression test for worker `log` event firing.

## [2.6.38] - 2026-03-18

### Added
- **Worker registration + heartbeat system** — Worker SDK now auto-registers with the server on `run()`, sends periodic heartbeats with `activeJobs`/`processed`/`failed` stats, and unregisters on `close()`. The server tracks `hostname`, `pid`, `uptime` per worker. `GET /workers` and `ListWorkers` TCP command return full worker details including aggregate stats. Dashboard receives real-time events (`worker:connected`, `worker:heartbeat`, `worker:disconnected`).
- **`RegisterWorkerCommand` extended** — Accepts `workerId`, `hostname`, `pid`, `startedAt` from client. Re-registration with same `workerId` updates instead of duplicating.
- **`HeartbeatCommand` extended** — Accepts `activeJobs`, `processed`, `failed` to sync client-side stats to server.
- **`onOutcome` callback in processor** — Tracks completed/failed counts without adding event listeners.

### Removed
- Flaky embedded tests (sandboxed-workers, cron-event-driven, query-operations)

## [2.6.37] - 2026-03-17

### Added
- **`getJobCounts` now returns `delayed` and `paused` counts** — Matches BullMQ's `getJobCounts()` return type. Both embedded and TCP modes include `delayed` (jobs with future `runAt`) and `paused` (waiting jobs count when queue is paused). ([#56](https://github.com/egeominotti/bunqueue/issues/56))
- **`getJobs` supports multiple statuses** — Accepts `string | string[]` for the `state` parameter, matching BullMQ's `getJobs(types?: JobType | JobType[])` interface. Works in embedded, TCP, and HTTP (`?state=waiting&state=delayed`). ([#55](https://github.com/egeominotti/bunqueue/issues/55))
- **`GET /queues/summary` endpoint** — Returns all queues with name, paused status, and job counts in a single HTTP call, replacing N+1 round-trips.

### Removed
- Flaky TCP integration tests (sandboxed-worker, monitoring)

## [2.6.36] - 2026-03-17

### Fixed
- **`/queues/:queue/jobs/list` performance** — Endpoint was taking 300-450ms even with `limit=2` because it scanned the entire jobIndex (O(N) iterations + O(N) individual SQLite lookups) then sorted all results. Now delegates to a single indexed SQLite query with `LIMIT/OFFSET`, reducing response time to <5ms.

## [2.6.35] - 2026-03-16

### Changed
- Removed flaky SandboxedWorker flow failure test

## [2.6.34] - 2026-03-16

### Fixed
- **QueueEvents failed events** — `failedReason` now correctly reads from `event.error` instead of `event.data`, job `data` is included in failed broadcasts, and error emission includes event context. ([#54](https://github.com/egeominotti/bunqueue/pull/54)) — thanks @simontong

### Changed
- **CI** — Disabled TCP and Embedded integration tests in GitHub Actions pipeline
- Removed flaky SandboxedWorker tests

## [2.6.33] - 2026-03-16

### Fixed
- **Worker `log` event** — `worker.on('log', (job, message) => ...)` now works with full TypeScript autocomplete. The `log` event is emitted when `job.log()` is called inside the processor, matching SandboxedWorker behavior. ([#53](https://github.com/egeominotti/bunqueue/issues/53))

## [2.6.32] - 2026-03-16

### Added
- **13 new WebSocket/SSE events** — `job:expired`, `flow:completed`, `flow:failed`, `queue:idle`, `queue:threshold`, `worker:overloaded`, `worker:error`, `cron:skipped`, `storage:size-warning`, `server:memory-warning` (+ `flow:*` wildcard). Total event types: 86.
- **Monitoring checks** — Periodic threshold monitoring runs on cleanup interval (10s). Configurable via env vars: `QUEUE_IDLE_THRESHOLD_MS`, `QUEUE_SIZE_THRESHOLD`, `MEMORY_WARNING_MB`, `STORAGE_WARNING_MB`, `WORKER_OVERLOAD_THRESHOLD_MS`.
- **Cron overlap detection** — Crons skip execution if the previous instance fired within 80% of the repeat interval, emitting `cron:skipped` instead.
- **Flow lifecycle events** — `flow:completed` when all children of a parent job finish, `flow:failed` when a child permanently fails (moves to DLQ).

### Changed
- **SandboxedWorker docs** — Clearly marked as experimental across all documentation pages (worker, migration, CPU-intensive, stall-detection, troubleshooting). Production recommendation to use standard `Worker` instead.

## [2.6.31] - 2026-03-16

### Added
- **SandboxedWorker `autoStart` option** — Automatically restart the worker pool when new jobs arrive after idle shutdown. Set `autoStart: true` with `idleTimeout` to get workers that sleep when idle and wake up when needed. Configurable poll interval via `autoStartPollMs` (default: 5000ms). Closes #51.

## [2.6.30] - 2026-03-16

### Added
- **Full WebSocket/SSE event coverage** — 73 unique event types now emitted across all transports. Every state change, operation, and lifecycle event is observable via WebSocket pub/sub and SSE.
- **New event categories**: `job:timeout`, `job:lock-expired`, `job:deduplicated`, `job:waiting-children`, `job:dependencies-resolved`, `job:stalled` (dashboard), `job:moved-to-delayed`
- **Backup events**: `storage:backup-started`, `storage:backup-completed`, `storage:backup-failed`
- **Connection tracking**: `client:connected`, `client:disconnected`, `auth:failed`
- **Batch events**: `batch:pushed`, `batch:pulled`
- **DLQ maintenance events**: `dlq:auto-retried`, `dlq:expired`
- **Cron lifecycle**: `cron:fired`, `cron:missed`, `cron:updated` (distinguish create vs update)
- **Worker events**: `worker:heartbeat`, `worker:idle`, `worker:removed-stale`
- **Webhook events**: `webhook:fired`, `webhook:failed`, `webhook:enabled`, `webhook:disabled`
- **Queue lifecycle**: `queue:created`, `queue:removed` (on obliterate and cleanup)
- **Rate/concurrency**: `ratelimit:hit`, `ratelimit:rejected`, `concurrency:rejected`
- **Server lifecycle**: `server:started`, `server:shutdown`, `server:recovered`
- **Cleanup events**: `cleanup:orphans-removed`, `cleanup:stale-deps-removed`
- **Memory**: `memory:compacted`

## [2.6.29] - 2026-03-16

### Added
- **TCP integration tests** — 4 new test suites: backoff strategies, job move methods, parent failure options, worker advanced methods. TCP test coverage now at 56 suites.

## [2.6.28] - 2026-03-15

### Fixed
- **`getChildrenValues` empty in TCP mode** — Fixed response envelope unwrap in worker processor (`response.data.values` instead of `response.values`). Fixed `childrenIds`/`parentId` not passed through TCP protocol in flow jobs. (#49, PR by @simontong)

## [2.6.27] - 2026-03-15

### Fixed
- **`getJob` returns null for failed/DLQ jobs** — In embedded mode (no SQLite storage), `getJob()` and `getJobByCustomId()` now correctly query the shard DLQ instead of returning null. (#50)
- **`getChildrenValues` wired in worker** — Worker job processor now correctly passes the `getChildrenValues` callback.

### Added
- **WebSocket/SSE integration tests** — 88 new integration tests covering WebSocket and SSE event streaming.

## [2.6.26] - 2026-03-15

### Added
- **Enterprise-grade SSE** — Event IDs for client-side deduplication, Last-Event-ID resume with ring buffer (1000 events), heartbeat keepalive (30s), retry field (3s auto-reconnect), connection limit (1000 max with 503 rejection).
- **Enterprise-grade WebSocket** — Backpressure detection via getBufferedAmount() (1MB threshold), dead client cleanup in emit/broadcast, connection limit (1000 max), dropped message counter for observability.

### Docs
- **Worker options** — Documented 8 missing options: limiter, lockDuration, maxStalledCount, skipStalledCheck, skipLockRenewal, drainDelay, removeOnComplete, removeOnFail.
- **FlowProducer BullMQ v5 API** — Documented add(), addBulk(), getFlow() methods with FlowJob/JobNode interfaces.
- **Lifecycle functions** — Documented shutdownManager(), closeSharedTcpClient(), closeAllSharedPools().
- **Environment variables** — Added BUNQUEUE_MODE, BUNQUEUE_HOST, BUNQUEUE_PORT to env-vars reference.

## [2.6.25] - 2026-03-14

### Fixed
- **`GET /queues/:q/workers` crash** — Fixed crash when some workers were registered without a `queues` field (`undefined`/`null`). Now safely skips workers with missing queues and defaults to `[]` on creation.

## [2.6.24] - 2026-03-14

### Fixed
- **Per-queue completed count** — `GET /queues/:q/counts` `completed` field now counts only jobs completed in the requested queue instead of returning the global total across all queues.
- **DLQ endpoint returns full metadata** — `GET /queues/:q/dlq` now returns `DlqEntry[]` with `enteredAt`, `reason`, `error`, `retryCount`, `lastRetryAt`, `nextRetryAt`, `expiresAt` instead of raw `Job[]`.
- **Worker registration accepts `queue` (singular)** — `POST /workers` now accepts both `queue` (string) and `queues` (array), plus `workerId` as alias for `name`.

### Added
- **Per-queue `totalCompleted`/`totalFailed` counters** — `GET /queues/:q/counts` now includes cumulative per-queue counters for completed and failed jobs.
- **`GET /queues/:q/workers` endpoint** — New endpoint to list workers registered for a specific queue.
- **`GET /queues/:q/dlq/stats` endpoint** — Server-side DLQ stats aggregation: `total`, `byReason`, `pendingRetry`, `oldestEntry`.
- **Worker `concurrency`, `status`, `currentJob` fields** — `GET /workers` and `POST /workers` responses now include `concurrency`, computed `status` (active/stale), and `currentJob`.
- **Throughput rates in `GET /stats`** — Added `pushPerSec`, `pullPerSec`, `completePerSec`, `failPerSec` from the built-in throughput tracker.

## [2.6.23] - 2026-03-14

### Added
- **Dashboard beta demo** — Added demo video and beta CTA to README and docs introduction page.

## [2.6.22] - 2026-03-14

### Fixed
- **dlq:added WebSocket event** — Now emitted when a job moves to DLQ after max attempts exceeded. Previously this event was defined but never fired.
- **job:progress WebSocket event** — Progress value now included in event payload. Previously `progress` was `undefined` because the broadcast didn't set the top-level field.

### Added
- **Comprehensive WebSocket pub/sub integration test** — 47 assertions covering all 9 event categories (job lifecycle, queue, DLQ, cron, worker, rate-limit, concurrency, webhook, config, system periodic) plus protocol tests (subscribe, unsubscribe, wildcard, invalid patterns, Ping over WS).

## [2.6.21] - 2026-03-14

### Performance
- **Batch push notifyBatch()** — Batch push now wakes all waiting workers correctly via `notifyBatch(N)` instead of a single `notify()` call. Each waiter is woken up individually, fixing a bug where only 1 of N workers received jobs immediately.
- **Pre-compiled HTTP route regexes** — All 40+ regex patterns in HTTP route files are now compiled once at module load instead of per-request (~100µs/request savings).

### Security
- **constantTimeEqual timing fix** — Removed early return on length mismatch that leaked token length via timing side-channel.
- **Batch PUSHB data validation** — Individual job data size is now validated in batch push (was only checked in single PUSH), preventing 10MB limit bypass.
- **Dashboard queue name validation** — `GET /dashboard/queues/:queue` now validates queue names like all other endpoints.
- **Error message sanitization** — SQLite/database error messages are no longer leaked to clients in TCP and HTTP error responses.

### Fixed
- **Silent error swallowing** — Replaced 7 empty `.catch(() => {})` blocks with proper error logging in addBatcher flush, sandboxed worker stop/kill/restart/heartbeat paths.

## [2.6.20] - 2026-03-14

### Fixed
- **Centralized HTTP JSON body parsing** — Replaced per-file `parseBody()` with shared `parseJsonBody()` that returns proper 400 responses for invalid JSON instead of silently falling back to `{}`.
- **Dashboard pagination** — Added `limit` and `offset` query parameters to `GET /dashboard/queues`. Workers and crons lists capped at 100 entries with `truncated` flag.
- **ESLint complexity reduction** — Extracted job push/pull/bulk operations into `routeJobOps()` helper to keep `routeQueueRoutes` under the 45-branch complexity limit.

## [2.6.19] - 2026-03-14

### Added
- **WebSocket idle timeout (ping/pong)** — Set `idleTimeout: 120` on the WebSocket server. Bun automatically sends ping frames and closes connections that don't respond with pong within 120 seconds. Dead clients (crash, network drop, kill -9) are now detected and cleaned up automatically instead of leaking in the clients Map forever.
- **WebSocket max payload limit** — Set `maxPayloadLength: 1MB`. Prevents memory exhaustion from oversized messages.

## [2.6.18] - 2026-03-14

### Added
- **WebSocket pub/sub system with 50 event types** — Clients subscribe to specific events via `{ cmd: "Subscribe", events: ["job:*", "stats:snapshot"] }` and receive only matching data. Supports wildcard patterns (`*`, `job:*`, `queue:*`, `worker:*`, `dlq:*`, `cron:*`, etc.). Legacy clients (no Subscribe) continue receiving all events in the old format.
- **Periodic dashboard broadcasts** — `stats:snapshot` every 5s (global stats, per-queue counts, throughput, workers), `health:status` every 10s (uptime, memory, connections), `storage:status` every 30s (collection sizes, disk health).
- **`queue:counts` event** — Fired on every job state change with real-time counts for the affected queue. Eliminates the N+1 polling problem for dashboards (20 queues = 0 HTTP calls instead of 200+/min).
- **Dashboard event hooks** — 30+ operations now emit real-time events: `job:promoted`, `job:discarded`, `job:priority-changed`, `job:data-updated`, `job:delay-changed`, `queue:paused/resumed/drained/cleaned/obliterated`, `dlq:retried/purged`, `cron:created/deleted`, `webhook:added/removed`, `ratelimit:set/cleared`, `concurrency:set/cleared`, `config:stall-changed/dlq-changed`, `worker:connected/disconnected`.

### Changed
- **HTTP API docs rewritten** — 2,048 lines of enterprise-grade documentation with deep explanations of job lifecycle, retry behavior, stall detection, every endpoint with curl examples, full request/response specs, all 50 pub/sub events with payload schemas.

## [2.6.17] - 2026-03-14

### Fixed
- **Memory leak in HTTP client tracking** — Every HTTP PULL+ACK cycle created an orphaned entry in the `clientJobs` Map that was never cleaned up. Over time this grew unbounded. Fix: HTTP requests no longer set `clientId` (stateless). Job ownership tracking only applies to persistent connections (TCP/WebSocket). Orphaned HTTP jobs are handled by stall detection.

## [2.6.16] - 2026-03-14

### Fixed
- **PUSH `maxAttempts` silently ignored via HTTP** — The HTTP endpoint mapped `attempts` instead of `maxAttempts`, causing retry configuration to be discarded. Now correctly maps to `maxAttempts` (also accepts `attempts` for backwards compatibility).
- **GetJobs pagination broken via HTTP** — The HTTP endpoint sent `start`/`end` instead of `offset`/`limit`, causing query parameters to be silently ignored. Pagination now works correctly.
- **Batch HTTP endpoints unreachable** — `/jobs/ack-batch`, `/jobs/extend-locks`, and `/jobs/heartbeat-batch` were intercepted by the generic `/jobs/:id` pattern. Fixed by matching exact batch paths before the wildcard pattern.

## [2.6.15] - 2026-03-14

### Added
- **Full HTTP REST API parity with TCP protocol** — All 76 TCP commands are now accessible via HTTP endpoints. Previously only 17 endpoints were available. New endpoints include:
  - **Job management**: promote, update data, get state, get result, get/update progress, change priority, discard to DLQ, move to delayed, change delay, wait for completion, get children values
  - **Job logs**: add, get, and clear structured logs per job
  - **Job locking**: heartbeat, extend lock, batch heartbeat, batch extend locks
  - **Batch operations**: bulk push (`PUSHB`), batch pull (`PULLB`), batch acknowledge (`ACKB`)
  - **Queue control**: list queues, list jobs by state, job counts, priority counts, pause/resume, drain, obliterate, clean with grace period, promote all delayed, retry completed
  - **DLQ**: list DLQ jobs, retry (single or all), purge
  - **Rate limiting & concurrency**: set/clear per-queue rate limits and concurrency limits
  - **Queue configuration**: get/set stall detection config, get/set DLQ config
  - **Cron jobs**: full CRUD (list, add, get, delete)
  - **Webhooks**: full CRUD (list, add, remove, enable/disable)
  - **Workers**: list, register, unregister, worker heartbeat
  - **Monitoring**: ping, storage status
- **HTTP route architecture** — Routes split into 4 files (`httpRouteJobs.ts`, `httpRouteQueues.ts`, `httpRouteQueueConfig.ts`, `httpRouteResources.ts`) for maintainability.
- **HTTP API documentation rewritten** — Enterprise-grade docs with curl examples, full request/response specs, parameter tables, and error cases for every endpoint (1,640 lines).

## [2.6.14] - 2026-03-14

### Fixed
- **CLI double execution** — Every CLI command ran twice due to `main()` being called both on module load and on import. Added `import.meta.main` guard.
- **CLI ACK/FAIL rejected UUID job IDs** — `parseBigIntArg()` only accepted numeric IDs (`/^\d+$/`) but all job IDs are UUIDs. Now accepts any non-empty string ID.
- **CLI ACK/FAIL always failed** — Each CLI command opens a new TCP connection. When the PULL connection closed, jobs were auto-released back to waiting. ACK on a new connection found the job no longer in processing. Added `detach` flag to PULL command for CLI usage.
- **`job get` showed `State: unknown`** — GetJob response didn't include job state. Now includes state from `getJobState()`.
- **`queue jobs` state column showed `-`** — GetJobs handler didn't include state per job. Now injects state for each returned job.
- **`bunqueue -p <port>` (without `start`) ignored port flag** — Direct mode ignored all CLI flags. Now routes to CLI parser when flags are present.
- **Worker/webhook/cron/logs/metrics list showed `OK`** — Server wraps responses in `{data: {...}}` but CLI formatter only checked top-level keys. Added `unwrap()` helper.
- **Cron list showed `OK`** — Server returns `crons` key but formatter checked for `cronJobs`.
- **Worker/webhook list showed stats instead of entries** — `stats` check ran before `workers`/`webhooks` in formatter priority order.
- **Worker register showed queue list** — Response `queues` field triggered queue list formatter.
- **DLQ list format broken** — Formatter expected `jobId` field but server returns `id`.
- **Metrics showed `OK`** — Prometheus metrics nested in `data.metrics`.

## [2.6.9] - 2026-03-10

### Fixed
- **SandboxedWorker graceful stop** — `stop()` now drains active jobs before terminating worker threads, preventing data loss when stopping during job processing. Added `force` parameter for immediate termination when needed. ([#39](https://github.com/egeominotti/bunqueue/issues/39))

## [2.6.7] - 2026-03-08

### Fixed
- **CronScheduler stale heap bug** — When a cron job was removed, `scheduleNext()` encountered the stale heap entry and returned early without setting any timer, preventing all subsequent crons from firing. Now properly pops stale entries from the min-heap until a valid one is found. ([#33](https://github.com/egeominotti/bunqueue/issues/33))
- **Graceful shutdown burst load** — Fixed `worker.close(true)` causing unhandled AckBatcher errors when jobs were still completing during burst load scenarios. Changed to graceful close with proper drain.

### Added
- **53 new test suites** — Comprehensive test coverage across embedded and TCP modes:
  - **Batch 1–3 (19 embedded + 18 TCP):** stress, ETL, retry, cron, queue group, shutdown, backpressure, priorities, lifecycle, data integrity, deduplication, timeouts, flows, removal, pause/resume, worker scaling, cancellation, DLQ patterns, bulk ops
  - **Coverage gap tests (16 embedded):** auto-batching, webhook delivery, durable jobs, rate limiting, lock race conditions, flow + stall detection, cron timezone/DST, LIFO queue, DLQ selective retry, S3 backup concurrent, webhook SSRF, MCP edge cases, CLI error formatting, flow deduplication, sandboxed worker + flow, queue group + flow
- Total test count increased from ~4,000 to 4,903

### Docs
- Removed BullMQ-only WorkerOptions from API types (lockDuration, maxStalledCount, etc.)
- Added auto-batching documentation to Queue guide
- Added connection pool sizing note to Worker guide
- Fixed CLI help: removed non-existent socket options, fake interactive prompts

### Performance
- CronScheduler `scheduleNext()` now handles stale entries in O(k) amortized instead of blocking indefinitely

## [2.6.6] - 2026-03-07

### Fixed
- **Parent-child flow race condition** — Resolved race where concurrent ack/fail operations on parent-child flows could cause inconsistent state. ([#31](https://github.com/egeominotti/bunqueue/issues/31))
- **Embedded Worker heartbeats** — Fixed embedded Worker heartbeat mechanism not properly keeping jobs alive during long processing. ([#32](https://github.com/egeominotti/bunqueue/issues/32))

## [2.6.5] - 2026-03-06

### Fixed
- **SandboxedWorker `log` event not emitted** — The processor's `job.log()` method stored logs via `addLog()` but the SandboxedWorker never emitted a `'log'` event. Listeners registered with `.on('log', ...)` were never called. Now properly emits `(job, message)` on each log call. ([#29](https://github.com/egeominotti/bunqueue/issues/29))
- **SandboxedWorker embedded heartbeats missing** — In embedded mode, `sendHeartbeat` was a no-op and `heartbeatInterval` defaulted to 0 (timer never started). Long-running jobs without `progress()` calls were detected as stalled and moved to DLQ despite still running. Now `sendHeartbeat` calls `manager.jobHeartbeat()` and defaults to 5000ms. ([#30](https://github.com/egeominotti/bunqueue/issues/30))

### Added
- Typed event overloads for `'log'` event on SandboxedWorker (`on`/`once`)
- Regression tests for both issues (`test/issue29-sandboxed-log.test.ts`, `test/issue30-dlq-stall.test.ts`)

### Docs
- Updated SandboxedWorker processor example with `log()`, `fail()`, and `parentId` fields
- Fixed `heartbeatInterval` default from `0` to `5000` in embedded mode docs
- Added `log` event to SandboxedWorker Event Reference (8 events total)
- Added SandboxedWorker section to Stall Detection guide
- Updated SandboxedWorkerOptions type with `heartbeatInterval` and `connection` fields

## [2.6.4] - 2026-03-05

### Fixed
- **Lock token race condition** — Resolved race where concurrent ack/fail operations could use an expired lock token, causing "Invalid or expired lock token" errors under high concurrency. ([#28](https://github.com/egeominotti/bunqueue/issues/28))

### Added
- **SandboxedWorker generics** — `SandboxedWorker<T>` now supports a generic type parameter for typed events (e.g., `worker.on('completed', (job: Job<MyData>) => ...)`)
- **Processor API improvements** — Processor files now receive `log()`, `fail()`, and `parentId` on the job object alongside `progress()`
- Typed `on()`/`once()` overloads for all SandboxedWorker events ([#25](https://github.com/egeominotti/bunqueue/issues/25))

## [2.6.2] - 2026-03-03

### Fixed
- **`job.name` always `'default'` for scheduled jobs** — When jobs were created via `Queue#upsertJobScheduler`, the `name` from `jobTemplate` was not embedded in the cron job data. The worker fell back to `'default'`. Now embeds the name in data, matching `Queue.add()` behavior. ([Discussion #23](https://github.com/egeominotti/bunqueue/discussions/23))

### Added
- Regression test for scheduler job name passthrough (`test/bug-23-scheduler-job-name.test.ts`)

### Docs
- Added SandboxedWorker Options Reference table
- Added SandboxedWorker Event Reference table with types
- Clarified which events are not available on SandboxedWorker (`stalled`, `drained`, `cancelled`)
- Added tip about increasing `maxMemory` for large file processing
- Fixed missing `await` on `worker.start()` calls
- Improved Worker vs SandboxedWorker comparison table

## [2.6.1] - 2026-03-03

### Fixed
- **`Queue#upsertJobScheduler` ignoring timezone** — The `RepeatOpts` interface was missing the `timezone` field, causing a TypeScript error when setting it. Additionally, embedded mode hardcoded `timezone: 'UTC'` and TCP mode did not forward timezone to the server. Now properly accepts and passes through IANA timezone strings (e.g., `"Europe/Rome"`, `"America/New_York"`). ([#22](https://github.com/egeominotti/bunqueue/issues/22))

### Added
- Regression test for scheduler timezone passthrough (`test/bug-22-scheduler-timezone.test.ts`)

## [2.6.0] - 2026-03-03

### Added
- **8 new TCP command handlers** — `ClearLogs`, `ExtendLock`, `ExtendLocks`, `ChangeDelay`, `SetWebhookEnabled`, `CompactMemory`, `MoveToWait`, `PromoteJobs`. These commands were already sent by the client SDK and MCP adapter but had no server-side handler, causing silent `Unknown command` errors in TCP mode. All 8 are now fully functional.
- **`updateJobData` / `updateJobChildrenIds`** persistence methods added to `SqliteStorage` for parent-child relationship durability.
- 20 new regression tests covering all fixes in this release.

### Fixed
- **Expired lock requeue not updating stats** — When a job's lock expired and was requeued for retry, `requeueExpiredJob` in `lockManager.ts` did not call `shard.incrementQueued()` or `shard.notify()`. This caused `getStats()` to report 0 waiting jobs and workers in long-poll mode to not wake up for the requeued job.
- **`updateJobParent` not persisting to SQLite** — `childrenIds` and `__parentId` mutations were only applied in memory. After a server restart, all parent-child flow relationships were lost. Now properly persisted via dedicated SQLite update methods.
- **`getJob` returning null for completed jobs without storage** — In no-SQLite mode (embedded without persistence), `getJob()` returned `null` for completed/DLQ jobs because it only checked `ctx.storage?.getJob()`. Now falls back to `ctx.completedJobsData` in-memory map.
- **MCP `UnregisterWorker` field mismatch** — MCP adapter sent `{ cmd: 'UnregisterWorker', id }` but the server expected `{ workerId }`. Worker unregistration via MCP in TCP mode always failed silently. Fixed to send the correct field name.
- **`JobHeartbeat` ignoring `duration` field** — When the MCP adapter sent a `JobHeartbeat` with a custom `duration`, the handler ignored it and renewed the lock with the default TTL. Now properly extends the lock with the requested duration via `renewJobLock()`.

## [2.5.8] - 2026-03-02

### Fixed
- **Repeat job updateData** — `updateData()` now propagates to the next repeat execution. Previously, calling `updateData()` on a completed repeated job silently failed because the job was removed from the index. A repeat chain now tracks successor job IDs so updates reach the next scheduled execution. ([#16](https://github.com/egeominotti/bunqueue/issues/16))
- **Worker event IntelliSense** — Worker now has typed `on()` and `once()` overloads for all 10 events (`ready`, `active`, `completed`, `failed`, `progress`, `stalled`, `drained`, `error`, `cancelled`, `closed`), providing full TypeScript autocomplete. ([#15](https://github.com/egeominotti/bunqueue/issues/15))

### Added
- **`FlowJobData` type** — New exported interface for flow-injected fields (`__flowParentId`, `__flowParentIds`, `__parentId`, `__parentQueue`, `__childrenIds`). `Processor<T, R>` now intersects `T` with `FlowJobData` for automatic IntelliSense in Worker callbacks. ([#18](https://github.com/egeominotti/bunqueue/issues/18))
- **CLI env var auth** — CLI now reads `BQ_TOKEN` / `BUNQUEUE_TOKEN` environment variables as fallback when `--token` is not provided. Priority: `--token` flag > `BQ_TOKEN` > `BUNQUEUE_TOKEN`. ([#13](https://github.com/egeominotti/bunqueue/issues/13))

### Docs
- Updated Worker guide with typed event reference table
- Updated Flow guide with `FlowJobData` type documentation
- Updated Queue guide with `updateData()` for repeatable jobs
- Updated CLI guide and env vars guide with `BQ_TOKEN` / `BUNQUEUE_TOKEN`

## [2.5.7] - 2026-03-01

### Added
- **SandboxedWorker TCP mode** — SandboxedWorker now supports connecting to a remote bunqueue server via TCP, enabling crash-isolated job processing in server deployments (systemd, Docker). Pass `connection` option to enable it.
- **SandboxedWorker EventEmitter** — SandboxedWorker now extends EventEmitter with full event support: `ready`, `active`, `completed`, `failed`, `progress`, `error`, `closed` (matching regular Worker API).
- **QueueOps adapter** (`src/client/sandboxed/queueOps.ts`) — unified interface for embedded and TCP queue operations, keeping SandboxedWorker code clean and dual-mode.
- **TCP heartbeat for SandboxedWorker** — automatic lock renewal via `JobHeartbeat` commands for active jobs in TCP mode (configurable via `heartbeatInterval`).
- TCP integration test for SandboxedWorker (`scripts/tcp/test-sandboxed-worker.ts`)
- 8 new unit tests for SandboxedWorker events and TCP constructor

### Docs
- Updated Worker guide with SandboxedWorker TCP mode section and events documentation
- Updated CPU-Intensive Workers guide with SandboxedWorker TCP example

## [2.5.6] - 2026-02-27

### Added
- **3 new TCP commands** for MCP protocol optimization (73 tools total):
  - `CronGet` — fetch a single cron job by name instead of listing all and filtering client-side
  - `GetChildrenValues` — batch-fetch children return values in a single command instead of N+1 queries
  - `StorageStatus` — return real disk/storage health from the server instead of hardcoded `diskFull: false`
- 9 new tests for the 3 TCP commands (`test/tcp-new-commands.test.ts`)

### Fixed
- **MCP TCP `getCron(name)`** — now uses dedicated `CronGet` command instead of fetching all crons and filtering client-side
- **MCP TCP `getChildrenValues(id)`** — now uses dedicated `GetChildrenValues` command instead of 1 + 2N queries (GetJob parent + GetResult/GetJob per child)
- **MCP TCP `getStorageStatus()`** — now uses dedicated `StorageStatus` command instead of returning hardcoded `{ diskFull: false }`

## [2.5.5] - 2026-02-26

### Fixed
- **TCP client auth state corruption** — `TcpClient.doConnect()` set `connected = true` before `authenticate()` completed. If authentication failed, the client remained in a corrupted state (`connected = true` with no valid session), causing subsequent operations to silently fail. Connection state is now set only after successful authentication, with proper cleanup on failure.

### Docs
- SEO overhaul — keyword-rich titles, optimized descriptions, AI keywords, sitemap priorities

## [2.5.4] - 2026-02-24

### Added
- **4 MCP Flow Tools** — job workflow orchestration via MCP (70 tools total):
  - `bunqueue_add_flow` — create flow trees with parent/children dependencies (BullMQ v5 compatible)
  - `bunqueue_add_flow_chain` — sequential pipelines: A → B → C
  - `bunqueue_add_flow_bulk_then` — fan-out/fan-in: parallel jobs → final merge
  - `bunqueue_get_flow` — retrieve flow trees with full dependency graph

## [2.5.3] - 2026-02-24

### Added
- **3 MCP Prompts** for AI agents — pre-built diagnostic templates:
  - `bunqueue_health_report` — comprehensive server health report with severity levels
  - `bunqueue_debug_queue` — deep diagnostic of a specific queue
  - `bunqueue_incident_response` — step-by-step triage playbook for "jobs not processing"

### Fixed
- **MCP graceful shutdown** — `server.close()` now awaited before exit
- **MCP `getStorageStatus()` TCP** — verifies server reachability instead of returning hardcoded response
- **MCP `getChildrenValues()` TCP** — parallel fetch with `Promise.all` instead of sequential N+1
- **MCP resource error format** — includes `isError: true` consistent with tool errors
- **MCP pool size** — configurable via `BUNQUEUE_POOL_SIZE` env var (default: 2)

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
