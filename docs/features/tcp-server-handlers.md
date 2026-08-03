# TCP Server Command Handlers

> **Category:** Transport · **Source:** `src/infrastructure/server/handler.ts`, `src/infrastructure/server/handlerRoutes.ts`, `src/infrastructure/server/handlers/`, `src/infrastructure/server/tcp/eventSubscriptions.ts`, `src/infrastructure/server/tcp/connections.ts`, `src/infrastructure/server/bootstrap.ts`, `src/infrastructure/server/types/`

## Purpose

This module is the request-handling layer of the server: it takes an already-decoded `Command` object (a msgpack frame unpacked by the TCP transport), authenticates it, dispatches it through a chain of category routers to the matching handler function, and returns a typed `Response`. Each handler is a thin adapter that validates wire input, calls one method on the `QueueManager`, and shapes the result into a response builder — it contains no queue business logic itself. `bootstrap.ts` is the single place that wires a full server together (`QueueManager` + TCP server + HTTP server + S3 backup + Cloud agent + stats interval + graceful shutdown), so both entry points (`bunqueue` bare and `bunqueue start`) cannot drift.

## Responsibilities & Scope

Owns:

- The top-level dispatch entry point `handleCommand(cmd, ctx)` (`handler.ts:48`) and the per-command authentication gate.
- Authentication: constant-time token comparison and the "Not authenticated" gate (`handler.ts:30`, `handler.ts:58`).
- Category routing: ten `route*Command` functions that switch on `cmd.cmd` and return `Response | null` (`handlerRoutes.ts`).
- Per-handler wire-input validation (queue name, job data size, numeric-field bounds, webhook URL/events, config-number sanitization) before delegating to `QueueManager`.
- Mapping `QueueManager` return values / thrown errors into the typed `Response` union via the `resp.*` builders.
- Client-job ownership registration on pull and de-registration on ack/fail (`registerClientJob` / `unregisterClientJob`).
- Emitting `*` dashboard events on mutating operations (e.g. `job:promoted`, `dlq:purged`, `cron:created`).
- Server bootstrap wiring and lifecycle (`bootstrap.ts`).
- Transport-owned `SubscribeEvents` / `UnsubscribeEvents` handling, including
  auth, queue validation, per-connection selection and manager-bridge cleanup.

Does NOT own:

- Framing, msgpack pack/unpack, pipelining semaphore, slowloris/write-queue bounds, connection lifecycle — see [TCP Wire Protocol & Framing](./tcp-protocol.md) and [Client Transport](./client-transport.md). The frame is already a decoded `Command` when it reaches `handleCommand`.
- The actual queue mutations, locking, and shard ownership — delegated entirely to `QueueManager`. See [Core Queue Engine](./core-queue-engine.md) and [Concurrency & Locking](./concurrency-and-locking.md).
- TLS/CORS/auth-token resolution and HTTP routing — see [Security: TLS, Auth, CORS](./security-tls-auth.md) and [HTTP / REST / SSE / WebSocket API](./http-api.md). This module only consumes the resolved `authTokens` set.
- Job state-machine semantics (push/pull/ack/fail) — see [Job Lifecycle](./job-lifecycle.md).

## Dependencies

Internal:

- `src/application/queueManager.ts` — the single delegate target for every handler; reached via `ctx.queueManager`. See [Core Queue Engine](./core-queue-engine.md).
- `src/domain/types/command.ts` — the discriminated `Command` union (switch key is `cmd.cmd`).
- `src/domain/types/response.ts` — the `Response` union and the `resp.*` builders (`ok`, `error`, `batch`, `job`, `nullableJob`, `pulledJob`, `pulledJobs`, `jobs`, `counts`, `stats`, `metrics`, `data`, `hello`).
- `src/domain/types/job.ts` — `jobId()` branding of wire strings to the `JobId` type.
- `src/infrastructure/server/protocol.ts` — `validateQueueName`, `validateJobData`, `validateJobOptions`, `validateNumericField`, `validateWebhookUrl`.
- `src/shared/hash.ts` — `constantTimeEqual` (auth), `SHARD_COUNT` (bootstrap banner/events).
- `src/shared/pausedView.ts` — `pausedView` (paused-aware count bucketing, #92).
- `src/application/throughputTracker.ts`, `src/application/latencyTracker.ts` — rate/latency snapshots for stats, metrics, dashboard.
- `src/domain/types/webhook.ts` — `WEBHOOK_EVENTS` allow-list for webhook validation.
- `bootstrap.ts` additionally depends on `./tcp`, `./http`, `../backup` (`S3BackupManager`), `../cloud` (`CloudAgent`), `./rateLimiter`, `../../config`, `../../shared/logger`.

External / runtime: Bun (`Bun.env`, `Bun.sleep`, `process.memoryUsage`, signal handlers), msgpack via the transport (not in this module directly). Zero third-party runtime deps.

## Public Interface

Exported functions:

- `handleCommand(cmd: Command, ctx: HandlerContext): Promise<Response>` (`handler.ts:48`) — the dispatch entry point.
- Ten category routers (`handlerRoutes.ts`): `routeCoreCommand`, `routeQueryCommand`, `routeManagementCommand` (all `async`, return `Promise<Response | null>`), and `routeQueueControlCommand`, `routeDlqCommand`, `routeRateLimitCommand`, `routeConfigCommand`, `routeCronCommand`, `routeMonitoringCommand`, `routeDashboardCommand` (sync, return `Response | null`).
- `bootServer(fileConfig: BunqueueConfig | null, config: ResolvedConfig): void` (`bootstrap.ts:94`).
- `PROTOCOL_VERSION = 3` and `SUPPORTED_CAPABILITIES = ['pipelining', 'separate-job-name']`, returned by `Hello`. Revision 3 uses top-level `job.name` and leaves `job.data` unchanged; legacy requests without a top-level name are decoded only by the inbound command handlers.
- `interface HandlerContext` re-exported from `handler.ts` (defined in `src/infrastructure/server/types.ts:8-16`).

TCP commands handled (exact `cmd.cmd` values), by router:

- **Auth** — handled inline before routing (`handler.ts:53`), always allowed.
- **Core** (`routeCoreCommand`): `PUSH`, `PUSHB`, `PUSHF`, `PULL`, `PULLB`, `ACK`, `ACKB`, `FAIL`.
- **Query** (`routeQueryCommand`): `GetJob`, `GetState`, `GetResult`, `GetJobCounts`, `GetCountsPerPriority`, `GetJobByCustomId`, `GetJobs`, `Count`, `GetProgress`, `GetChildrenValues`, `GetQueueLimits`, `GetDeduplicationJobId`. (Note: `Count`'s handler lives in `advanced.ts` and `GetProgress`'s in `management.ts`, despite being routed here.)
- **Management** (`routeManagementCommand`): `Cancel`, `Progress`, `Update`, `UpdateParent`, `ChangePriority`, `Promote`, `MoveToDelayed`, `MoveToWaitingChildren`, `Discard`, `WaitJob`, `ChangeDelay`, `MoveToWait`, `PromoteJobs`, `ExtendLock`, `ExtendLocks`, `GetFailedChildrenValues`, `GetIgnoredChildrenFailures`, `RemoveChildDependency`, `RemoveDeduplicationKey`, `RemoveJobDeduplicationKey`, `RemoveUnprocessedChildren`.
- **Queue control** (`routeQueueControlCommand`): `Pause`, `Resume`, `IsPaused`, `Drain`, `Obliterate`, `ListQueues`, `Clean`.
- **DLQ** (`routeDlqCommand`): `Dlq`, `GetDlqStats`, `RetryDlq`, `PurgeDlq`, `RetryCompleted`.
- **Rate** (`routeRateLimitCommand`): `RateLimit`, `RateLimitClear`, `SetConcurrency`, `ClearConcurrency`.
- **Config** (`routeConfigCommand`): `SetStallConfig`, `GetStallConfig`, `SetDlqConfig`, `GetDlqConfig`.
- **Cron** (`routeCronCommand`): `Cron`, `CronGet`, `CronDelete`, `CronList`.
- **Monitoring** (`routeMonitoringCommand`): `Stats`, `Metrics`, `Prometheus`, `AddLog`, `GetLogs`, `Heartbeat`, `JobHeartbeat`, `JobHeartbeatB`, `Ping`, `Hello`, `RegisterWorker`, `UnregisterWorker`, `ListWorkers`, `AddWebhook`, `RemoveWebhook`, `ListWebhooks`, `StorageStatus`, `ClearLogs`, `SetWebhookEnabled`, `CompactMemory`.
- **Dashboard** (`routeDashboardCommand`): `DashboardOverview`, `DashboardQueues`, `DashboardQueue`.
- **Event subscription** (intercepted by `src/infrastructure/server/tcp.ts:85-90` before `handleCommand`):
  `SubscribeEvents`, `UnsubscribeEvents`. They still pass the connection rate
  limiter and semaphore and return normal `reqId`-correlated responses.

Any unmatched command returns `resp.error("Unknown command: <cmd>")` (`handler.ts:95`).

Dashboard events emitted (non-exhaustive): `auth:failed`, `job:data-updated`, `job:priority-changed`, `job:promoted`, `job:moved-to-delayed`, `job:discarded`, `job:delay-changed`, `queue:cleaned`, `ratelimit:set`/`ratelimit:cleared`, `concurrency:set`/`concurrency:cleared`, `config:stall-changed`/`config:dlq-changed`, `worker:heartbeat`, `webhook:added`/`webhook:removed`, `cron:created`/`cron:updated`/`cron:deleted`, `dlq:retried`/`dlq:retry-all`/`dlq:purged`. Bootstrap additionally emits `server:started`, `server:shutdown` (`bootstrap.ts:196-200`, `bootstrap.ts:234`).

## Data Models

See [data-model](../data-model.md) for full definitions. The key shapes for this module:

- `HandlerContext` (`src/infrastructure/server/types.ts:8-16`): `{ queueManager: QueueManager; authTokens: Set<string>; authenticated: boolean; clientId?: string; signal?: AbortSignal }`. Constructed once per connection by `TcpConnectionRegistry.init` (`tcp/connections.ts:27-48`), called from `open` and idempotently from `data`, and mutated in place (notably `authenticated`). The signal aborts connection-scoped waits when the socket closes.
- `Command` — discriminated union keyed on `cmd`; each handler narrows via `Extract<Command, { cmd: '<X>' }>`.
- `Response` — discriminated success/error union; `ErrorResponse` is `{ ok: false; error: string; reqId? }`. Every response carries the request's `reqId` so a pipelined client can correlate out-of-order replies.

## Business Logic / Control Flow

Main dispatch (`handleCommand`, `handler.ts:48`):

1. Read `reqId` from the command.
2. If `cmd.cmd === 'Auth'`, call `handleAuth` immediately and return — auth is always allowed regardless of authentication state (`handler.ts:53`).
3. Auth gate: if `ctx.authTokens.size > 0 && !ctx.authenticated`, return `error('Not authenticated')` (`handler.ts:58`). When no tokens are configured, the TCP `open` handler pre-sets `ctx.authenticated = true` so this gate is a no-op.
4. Run the routers in fixed order (`handler.ts:65`–`93`): core → query → management → queue-control → dlq → rate-limit → config → cron → monitoring → dashboard. The first router returning a non-null `Response` wins (`if (result) return result;`).
5. If all routers return `null`, return `error('Unknown command: ...')`.
6. Any thrown error is caught at `handler.ts:96`; messages containing `SQLITE` or `database` are replaced with `'Internal server error'` to avoid leaking internals, everything else is passed through.

The TCP adapter handles event subscription commands beside this router because
they mutate socket-owned state. `handleEventSubscription` applies the same auth
gate, validates the selected queue, and asks `TcpConnectionRegistry` to attach
or clear the subscription. The registry lazily owns one QueueManager listener
for all TCP subscribers, writes only matching queue events, releases that
listener when idle, and clears it during server shutdown.

Auth (`handleAuth`, `handler.ts:30`): iterate configured tokens, comparing with `constantTimeEqual` (timing-safe). On match, set `ctx.authenticated = true` and return `ok()`; on no match, emit `auth:failed` and return `error('Invalid token')`.

Core paths:

- `handlePush` (`core.ts:20`): validates queue name, data size (≤10MB), and numeric option bounds, then validates each `dependsOn` id exists in `jobIndex` **or** `completedJobs` **or** `depCompletions` — the third check covers a `removeOnComplete` parent whose row was deleted, otherwise a late dependent is wrongly rejected (`core.ts:43`). On success returns the new job id via `resp.ok(job.id)`.
- `handlePushBatch` (`src/infrastructure/server/handlers/core.ts:104-124`): validates queue name, then runs `validatePushBatchJobs` (`pushBatchValidation.ts`) per job: the same data-size, `validateJobOptions` bounds, and `dependsOn` existence gate as `PUSH`, with the gate extended to accept the custom ids of **any jobs in the same batch** so order-independent intra-batch chains keep working. On violation returns an error naming the offending index (`jobs[i]: ...`); on success returns `resp.batch(ids)`.
- `handlePushFlow` (`flow.ts`): passes the fully resolved multi-queue graph to
  `QueueManager.pushFlow`. The application validator checks the complete batch
  before mutation, including strict string/array/boolean wire types, internal
  parent metadata, duplicate/missing/asymmetric edges, cycles, mutually
  exclusive failure policies, 10,000 jobs, 10 MB per job and 64 MB total flow
  data. Success uses `resp.data({ jobs })`; any validation, ownership, or
  persistence error rejects the whole command.
- `handlePull` (`src/infrastructure/server/handlers/core.ts:126-161`): caps `timeout` to `[0, 60000]`. If `cmd.owner` is set, uses `pullWithLock` and returns the lock `token` (`resp.pulledJob`); otherwise plain `pull` returning `resp.nullableJob`. Either way the job is registered against `ctx.clientId` for connection-loss release — unless the plain pull set `cmd.detach` (`:155-159`).
- `handlePullBatch` (`src/infrastructure/server/handlers/core.ts:163-208`): caps `count` to `[1, 1000]` and `timeout` to `[0, 60000]` (same bound as `PULL`); lock and non-lock branches both honor `cmd.timeout` (the plain branch calls `pullBatch(queue, count, cmd.timeout ?? 0)`, so a non-lock `PULLB` long-polls like `PULL`) and register every returned job with the client.
- `handleAck` / `handleAckBatch` (`src/infrastructure/server/handlers/core.ts`): ack with optional result/token; `ackBatchWithResults` is used only when `results.length === ids.length`, else the result-less `ackBatch`. Applied positions are unregistered from client tracking. Exact retired timeout/cron generations return structured ignored evidence and stay registered, so a duplicate ID cannot unregister a newer active lease.
- `handleFail` (`src/infrastructure/server/handlers/core.ts`): defensively coerces `cmd.stack` to `string[]` and slices to 100 elements before it reaches the domain (#74); the authoritative cap is later in `failJob` via `job.stackTraceLimit`. It propagates the same `applied:false` retired-generation envelope as ACK and unregisters only an applied transition.

Notable management/advanced flows:

- `handleMoveToWait` (`src/infrastructure/server/handlers/advanced/jobs.ts:154-183`): dispatches on the job's current state — `active`→`moveActiveToWait`, `delayed`→`promote`, `failed`→`retryDlq`, `waiting`/`prioritized`→no-op success, anything else→error. Mirrors the embedded branching in `jobMove.ts`.
- `handleMoveToWaitingChildren` requires an active job and delegates the full resource/index/persistence transition to `QueueManager`; a non-active id returns an error.
- The introspection handlers return live queue-limit status and owner-aware deduplication lookup/removal through `DataResponse` payloads.
- `handleDlq` returns both jobs and full filtered entries; `handleGetDlqStats` returns the authoritative aggregate. Filtered retries and completed retries pass their `count`/`timestamp` selectors to the manager instead of dropping them.
- `handleProgress` / `handleGetProgress` / `handleMoveToDelayed`: on failure they re-query `getJobState` to disambiguate "Job not found" (state `unknown`) from "not active" (`management.ts:36-42`, `management.ts:61-65`, `src/infrastructure/server/handlers/advanced/jobs.ts:69-85`).
- `handleWaitJob` (`src/infrastructure/server/handlers/advanced/jobs.ts:98-132`): caps `timeout` to `[0, 600000]` (default 30s); returns immediately if `job.completedAt` is set, otherwise awaits `waitForJobCompletion` (event-driven, no polling).
- Config setters (`handleSetStallConfig`, `handleSetDlqConfig`): run `sanitizeConfigNumbers` (`handlers/advanced/configNumbers.ts:7-27`) to coerce numeric strings and drop non-numeric garbage so the manager's merge never stores `NaN` (a string `stallInterval` would otherwise silently disable stall detection).

Query/dashboard count handling: `handleGetJobCounts` (`src/infrastructure/server/handlers/query.ts:63-88`) and `handleDashboardQueue` (`src/infrastructure/server/handlers/dashboard.ts:116`) both run `pausedView(waiting, prioritized, isPaused)` so a paused queue reports its ready jobs under `paused` rather than double-counting (#92, BullMQ semantics).

`handleGetJobs` forwards `cmd.asc ?? true` to the manager. Ordering therefore
occurs over the complete logical state result before `offset`/`limit` slicing;
an explicit `false` must not be collapsed to the ascending default.

Webhook creation (`handleAddWebhook`, `handlers/monitoring/webhooks.ts:13-29`): validates the URL (SSRF guard via `validateWebhookUrl`) and rejects any event not in `WEBHOOK_EVENTS` (a webhook on a dead event would be created "ok" then never fire).

Bootstrap (`bootServer`, `bootstrap.ts:94`): applies logging config, resolves cloud/TLS config (fails fast `process.exit(1)` on partial cert/key), prints the banner, constructs the `QueueManager`, then starts TCP + HTTP servers inside a try/catch that shuts the manager down and exits on bind failure. It then conditionally starts `S3BackupManager` (only when `dataPath` is set) and the `CloudAgent`, registers SIGINT/SIGTERM/`uncaughtException`/`unhandledRejection` handlers, and a stats interval. Graceful shutdown (`bootstrap.ts:202-239`) stops the servers and drains active jobs up to `shutdownTimeoutMs` before exiting.

## Concurrency & Locking

The handler layer itself takes **no locks** — every mutation is delegated to `QueueManager`, which owns the lock hierarchy (`jobIndex → completedJobs → shards[N] → processingShards[N]`). See [Concurrency & Locking](./concurrency-and-locking.md).

Concurrency relevant to this layer:

- The transport processes frames of a single connection in parallel under a per-connection `Semaphore(50)` (`tcp/constants.ts:1`, constructed at `tcp/connections.ts:42` and acquired at `src/infrastructure/server/tcp.ts:85`), so multiple `handleCommand` calls can be in flight on the same `ctx`. Handlers are therefore expected to be safe against concurrent invocation — they are, because they hold no per-call state and `QueueManager` serializes the underlying mutations.
- `ctx.authenticated` is the single shared mutable field; once `handleAuth` flips it to `true` it is monotonic for the connection's lifetime, so the concurrent reads in the auth gate are benign.
- Client-job ownership: `registerClientJob` (on pull) / `unregisterClientJob` (on applied ack/fail) keep a per-`clientId` set so that on disconnect the TCP `close` handler can call `releaseClientJobs` (with retry + force-release fallback) to requeue leased jobs. Ignored retired generations do not clear tracking for a possibly newer lease with the same ID. See [Worker Registry & Management](./workers-management.md).
- Lock tokens (`token`) are an opaque ownership credential minted by `pullWithLock`/`pullBatchWithLock` and verified by `ack`/`fail`/`extendLock`/`renewJobLock`; the handlers only pass them through.

## Edge Cases & Failure Modes

- **Error sanitization (double layer):** `handleCommand` catches and rewrites `SQLITE`/`database` errors to `'Internal server error'` (`handler.ts:96-101`); the transport's `processFrame` repeats the same sanitization as a secondary net (`src/infrastructure/server/tcp.ts:93-98`).
- **PUSHB validation parity:** batch push runs the same option bounds and `dependsOn` existence gate as single `PUSH` (`pushBatchValidation.ts`); a job `PUSH` would reject is rejected inside a batch too, with the error naming the offending index. `dependsOn` may additionally reference any same-batch custom id.
- **`Stats`/`Metrics` routing quirk:** these two are dispatched calling `handleStats(ctx, reqId)` / `handleMetrics(ctx, reqId)` without the `cmd` argument (`src/infrastructure/server/handler-routes/monitoring.ts:36-47`); all other handlers receive `cmd` first.
- **`MetricsData` placeholder fields:** `sqliteSizeMb` and `activeConnections` are hard-coded to `0` in `handleMetrics` (`src/infrastructure/server/handlers/management.ts:128-145`); real connection/SSE/WS counts are only surfaced via the bootstrap stats interval and the Cloud agent handles.
- **Idempotency / custom id:** `customId` (`cmd.jobId`) and `uniqueKey` dedup are enforced inside `QueueManager.push`, not here. The handler just forwards them. See [Deduplication & Unique Jobs](./deduplication-and-unique.md).
- **Graceful "values" fallback:** flow-value queries (`GetChildrenValues`, `GetFailedChildrenValues`, `GetIgnoredChildrenFailures`) catch internally and return `{ values: {} }` rather than an error (`query.ts:137-148`, `src/infrastructure/server/handlers/advanced/dependencies.ts:7-30`).
- **NaN / non-finite guards:** `validateNumericField` rejects `NaN`/`Infinity` (important for `WaitJob`/`PULL` timeouts, which a hand-rolled `<min`/`>max` check would let through and resolve instantly; `protocol/validation.ts:21-42`), and `toFiniteNumber` guards `RateLimit`/`SetConcurrency` limits (`handlers/advanced/configNumbers.ts:1-5`).
- **Auth bypass surface:** `Auth` is processed before the auth gate, so it is always reachable; failed attempts emit `auth:failed` but otherwise return a generic `Invalid token`. There is no per-connection attempt counter at this layer.
- **`ConnectionState.authenticated` is vestigial:** `protocol/commands.ts:29-31` sets it to `false`, but the authoritative auth flag is `HandlerContext.authenticated` (set to `authTokens.size === 0` at `tcp/connections.ts:35-40`). Do not read `state.authenticated` for gating.
- **Bootstrap fail-fast:** partial TLS cert/key or a port-bind failure calls `process.exit(1)` (after shutting down the manager for bind failures) rather than starting half a server (`bootstrap.ts:114-121`, `bootstrap.ts:131-157`).
- **Shutdown drain bound:** active jobs are awaited only up to `shutdownTimeoutMs`; jobs still active after the deadline are abandoned to the next process's stall detector.

## Configuration

Environment variables read directly within this module's files:

| Var | Default | Effect |
| --- | --- | --- |
| `WORKER_TIMEOUT_MS` | `30000` | `handlers/monitoring/workers.ts:10-13` — threshold for `ListWorkers` to mark a worker `active` vs `stale`. |
| `LOG_FORMAT` / `LOG_LEVEL` | unset | `bootstrap.ts:75` — JSON mode and log level (overridden by file config if present). |

Resolved-config fields consumed by `bootServer` (sourced from env/CLI/file via `../../config`): `tcpPort` (`TCP_PORT`, 6789), `httpPort` (`HTTP_PORT`, 6790), `hostname` (`HOST`), `authTokens` (`AUTH_TOKENS`), `corsOrigins` (`CORS_ALLOW_ORIGIN`), `requireAuthForMetrics` (`METRICS_AUTH`), `maxPrometheusQueues` (`METRICS_MAX_QUEUES`, 100), `dataPath` (`BUNQUEUE_DATA_PATH`), `tcpSocketPath`/`httpSocketPath`, `tlsCertFile`/`tlsKeyFile` (`TLS_CERT_FILE`/`TLS_KEY_FILE`), `shutdownTimeoutMs` (`SHUTDOWN_TIMEOUT_MS`, 30000), `statsIntervalMs` (`STATS_INTERVAL_MS`), `s3BackupEnabled`. See [Configuration & Entrypoint](./configuration.md) and [Security: TLS, Auth, CORS](./security-tls-auth.md).

Input-validation limits enforced by the handlers (from `protocol.ts`): queue name ≤256 chars and `^[a-zA-Z0-9_\-.:]+$`; job data ≤10MB; `PULL`/`PULLB` timeout `[0,60000]`; `PULLB` count `[1,1000]`; `WaitJob` timeout `[0,600000]`; option bounds for `priority` `[-1e6,1e6]`, `delay`/`ttl` ≤1yr, `timeout`/`backoff`/`stallTimeout` ≤1day, `maxAttempts` `[1,1000]`. `backoff` accepts either a number (ms) or the object form `{ type: 'fixed'|'exponential', delay }` (`validateBackoffField`) — `type` must be `fixed`/`exponential` and `delay` ≤1day, matching embedded parity; `PUSH`, `PUSHB` (per job, via `validatePushBatchJobs`) and `PUSHF` validate the applicable bounds. `PUSHF` additionally caps the full graph as described above.

## Related Docs

- [TCP Wire Protocol & Framing](./tcp-protocol.md) — the layer that decodes frames into `Command` and serializes `Response`.
- [HTTP / REST / SSE / WebSocket API](./http-api.md) — the parallel transport that reuses much of the same `QueueManager` surface.
- [Security: TLS, Auth, CORS](./security-tls-auth.md) — token resolution and `constantTimeEqual`.
- [Client Transport](./client-transport.md) — the client side (pool, reconnect, batching) that produces these commands.
- [Core Queue Engine](./core-queue-engine.md) and [Concurrency & Locking](./concurrency-and-locking.md) — the `QueueManager` delegate and its lock hierarchy.
- [Job Lifecycle](./job-lifecycle.md), [Job Queries & Queue Control](./job-queries-and-control.md), [Dead Letter Queue](./dead-letter-queue.md), [Scheduler & Cron](./scheduler-and-cron.md), [Webhooks, Events & Job Logs](./webhooks-and-events.md), [Worker Registry & Management](./workers-management.md), [Stats, Metrics & Monitoring](./stats-and-monitoring.md), [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md), [FlowProducer & Job Dependencies](./flow-producer.md) — the feature areas the individual handler groups front.
- [Configuration & Entrypoint](./configuration.md) — `bootServer` and the resolved config it consumes.
- [architecture](../architecture.md), [data-model](../data-model.md).
