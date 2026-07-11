# HTTP / REST / SSE / WebSocket API

> **Category:** Transport · **Source:** `src/infrastructure/server/http.ts`, `src/infrastructure/server/httpEndpoints.ts`, `src/infrastructure/server/httpRouteJobs.ts`, `src/infrastructure/server/httpRouteQueues.ts`, `src/infrastructure/server/httpRouteQueueConfig.ts`, `src/infrastructure/server/httpRouteResources.ts`, `src/infrastructure/server/sseHandler.ts`, `src/infrastructure/server/wsHandler.ts`

## Purpose

This module is the HTTP-side transport for bunqueue, served on port `6790` by default via `Bun.serve`. It exposes the full queue/job/worker/cron/webhook control surface as a REST API, plus liveness/diagnostic endpoints, Prometheus and JSON metrics, and a dashboard aggregation API. It also runs two push channels — Server-Sent Events (`/events`) and WebSocket (`/ws`) — for real-time job and dashboard event streaming. Every REST route is a thin adapter that translates `method + path + body` into the same internal `Command` objects the TCP server uses, then funnels them through the shared `handleCommand` dispatcher, so HTTP and TCP share identical business logic.

## Responsibilities & Scope

Owns:
- Bun HTTP server creation, binding (TCP port or Unix socket), and optional TLS termination (`createHttpServer`, `http.ts:74`).
- Request routing: URL/method → internal `Command`, via pre-compiled regexes and four sub-routers (jobs, queues, queue-config, resources).
- Auth gate, CORS headers/preflight, and per-IP rate limiting on the HTTP edge.
- Diagnostic/observability endpoints: `/health`, `/healthz`, `/live`, `/ready`, `/stats`, `/metrics`, `/prometheus`, `/gc`, `/heapstats`, `/dashboard*`.
- SSE and WebSocket lifecycle: connection limits, subscriptions, event fan-out, heartbeats, periodic broadcasts, ring-buffer replay, backpressure handling, and client-job release on disconnect.

Does NOT own (delegated):
- Actual command execution and queue mutations — delegated to `handleCommand` / `QueueManager` ([Core Queue Engine](./core-queue-engine.md), [TCP Server Command Handlers](./tcp-server-handlers.md)).
- Command parsing for the WS typed-command path — delegated to `parseCommand`/`serializeResponse` from the [TCP Wire Protocol & Framing](./tcp-protocol.md) module.
- TLS file loading/validation — delegated to `loadTlsOptions` ([Security: TLS, Auth, CORS](./security-tls-auth.md)).
- The event source itself — `QueueManager.subscribe` / `setDashboardEmit` produce the events that SSE/WS merely forward ([Webhooks, Events & Job Logs](./webhooks-and-events.md)).
- Prometheus text generation — `QueueManager.getPrometheusMetrics()` ([Stats, Metrics & Monitoring](./stats-and-monitoring.md)).

## Dependencies

Internal:
- `handleCommand`, `HandlerContext` (`handler.ts` / `types.ts`) — the command dispatcher and per-request context.
- `QueueManager` — `subscribe`, `setDashboardEmit`, `emitDashboardEvent`, `getStats`, `getQueueJobCounts`, `getPerQueueStats`, `getDlqEntries`, `getDlqStats`, `getStorageStatus`, `getMemoryStats`, `getPrometheusMetrics`, `releaseClientJobs`, `unregisterWorkersByClientId`, `workerManager`, `listCrons`.
- `parseCommand`, `serializeResponse`, `errorResponse`, `validateQueueName` (`protocol.ts`).
- `getRateLimiter()` (`rateLimiter.ts`) — sliding-window per-client limiter.
- `loadTlsOptions` (`tls.ts`); `constantTimeEqual`, `uuid` (`shared/hash.ts`); `throughputTracker`, `latencyTracker`; `pausedView` (`shared/pausedView.ts`); `VERSION`.

External/runtime:
- `Bun.serve` (server + native WebSocket), `Bun.gc`, `bun:jsc` `heapStats` (dynamic import in `heapStatsEndpoint`), `ReadableStream`/`TextEncoder` (SSE), `process.memoryUsage`/`process.uptime`. Zero third-party deps.

## Public Interface

Exported symbols:
- `createHttpServer(queueManager: QueueManager, config: HttpServerConfig)` — `http.ts:74`. Returns `{ server, wsClients, sseClients, getWsClientCount(), getSseClientCount(), stop() }`. `stop()` unsubscribes from the event bus, stops WS broadcasts, closes all SSE streams, and stops the Bun server.
- `interface HttpServerConfig` — `http.ts:60`: `port?`, `hostname?`, `socketPath?`, `authTokens?: string[]`, `corsOrigins?: string[]`, `requireAuthForMetrics?: boolean`, `tls?: TlsServerOptions`.
- `type HttpServer = ReturnType<typeof createHttpServer>`.
- `httpEndpoints.ts` helpers: `jsonResponse`, `parseJsonBody`, `corsResponse`, `healthEndpoint`, `gcEndpoint`, `heapStatsEndpoint`, `statsEndpoint`, `metricsEndpoint`, `dashboardOverviewEndpoint`, `dashboardQueuesEndpoint`, `dashboardQueueDetailEndpoint`.
- `class SseHandler` (`sseHandler.ts:62`), `class WsHandler` + `interface WsData` (`wsHandler.ts:186`/`166`).

### HTTP endpoints

Diagnostics / observability (handled in `fetch`/`routeRequest`, `http.ts`):

| Method · Path | Auth | Notes |
|---|---|---|
| `OPTIONS *` | no | CORS preflight (`http.ts:116`) |
| `GET /health` | no | health JSON + memory + connection counts (`http.ts:121`) |
| `GET /healthz`, `GET /live` | no | `200 "OK"` liveness (`http.ts:124`) |
| `GET /ready` | no | `{ ok, ready }` (`http.ts:127`) |
| `POST /gc` | yes | force `Bun.gc(true)` + `compactMemory` (`http.ts:132`) |
| `GET /heapstats` | yes | `bun:jsc` heap breakdown (`http.ts:137`) |
| `GET /prometheus` | conditional | text exposition; auth only if `requireAuthForMetrics` (`http.ts:179`) |
| `GET /stats` | yes | full stats + memory + per-sec rates (`http.ts:294`) |
| `GET /metrics` | yes | JSON totals only (`http.ts:297`) |
| `GET /dashboard` | yes | aggregated overview (`http.ts:302`) |
| `GET /dashboard/queues?limit&offset` | yes | paginated queue list, limit clamped 1–500 (`http.ts:305`) |
| `GET /dashboard/queues/:queue?includeJobs=true` | yes | single-queue detail (`http.ts:314`) |

Jobs (`routeJobRoutes`, `httpRouteJobs.ts:242`) — batch routes matched before generic `/jobs/:id`:

| Method · Path | Command |
|---|---|
| `POST /jobs/ack-batch` | `ACKB` |
| `POST /jobs/extend-locks` | `ExtendLocks` |
| `POST /jobs/heartbeat-batch` | `JobHeartbeatB` |
| `GET /jobs/custom/:customId` | `GetJobByCustomId` |
| `GET /jobs/:id` · `DELETE /jobs/:id` | `GetJob` · `Cancel` |
| `POST /jobs/:id/ack` · `POST /jobs/:id/fail` | `ACK` · `FAIL` |
| `POST /jobs/:id/promote` | `Promote` |
| `PUT /jobs/:id/data` | `Update` |
| `GET /jobs/:id/state` · `/result` · `/progress` | `GetState` · `GetResult` · `GetProgress` |
| `POST /jobs/:id/progress` | `Progress` |
| `PUT /jobs/:id/priority` | `ChangePriority` |
| `POST /jobs/:id/discard` | `Discard` |
| `POST /jobs/:id/move-to-delayed` · `PUT /jobs/:id/delay` | `MoveToDelayed` · `ChangeDelay` |
| `GET /jobs/:id/children` | `GetChildrenValues` |
| `GET/POST/DELETE /jobs/:id/logs` | `GetLogs` · `AddLog` · `ClearLogs` |
| `POST /jobs/:id/heartbeat` | `JobHeartbeat` |
| `POST /jobs/:id/wait` | `WaitJob` |
| `POST /jobs/:id/extend-lock` | `ExtendLock` |
| `POST /jobs/:id/move-to-wait` | `MoveToWait` |

Queues (`routeQueueRoutes`, `httpRouteQueues.ts:168`):

| Method · Path | Command / action |
|---|---|
| `GET /queues` · `GET /queues/summary` | `ListQueues` · `getQueuesSummary()` (summary is a **bare JSON array**, no `ok` wrapper, `httpRouteQueues.ts:182`) |
| `POST /queues/:queue/jobs` | `PUSH` |
| `GET /queues/:queue/jobs?timeout=` | `PULL` (long-poll) |
| `POST /queues/:queue/jobs/bulk` | `PUSHB` |
| `POST /queues/:queue/jobs/pull-batch` | `PULLB` |
| `GET /queues/:queue/jobs/list?state|status|states&limit&offset` | `GetJobs` |
| `GET /queues/:queue/workers` | `workerManager.getForQueue` |
| `GET /queues/:queue/counts` · `/count` · `/priority-counts` · `/paused` | `GetJobCounts` · `Count` · `GetCountsPerPriority` · `IsPaused` |
| `POST /queues/:queue/pause` · `/resume` · `/drain` · `/obliterate` | `Pause` · `Resume` · `Drain` · `Obliterate` |
| `POST /queues/:queue/clean` | `Clean` |
| `POST /queues/:queue/promote-jobs` · `/retry-completed` | `PromoteJobs` · `RetryCompleted` |

Queue config / DLQ / limits (`routeQueueConfigRoutes`, `httpRouteQueueConfig.ts:21`):

| Method · Path | Command / action |
|---|---|
| `GET /queues/:queue/dlq/stats` | `getDlqStats` |
| `GET /queues/:queue/dlq?limit&offset` | `getDlqEntries` (paginated slice) |
| `POST /queues/:queue/dlq/retry` · `/dlq/purge` | `RetryDlq` · `PurgeDlq` |
| `PUT/DELETE /queues/:queue/rate-limit` | `RateLimit` · `RateLimitClear` |
| `PUT/DELETE /queues/:queue/concurrency` | `SetConcurrency` (accepts `concurrency` or `limit`) · `ClearConcurrency` |
| `GET/PUT /queues/:queue/stall-config` | `GetStallConfig` · `SetStallConfig` |
| `GET/PUT /queues/:queue/dlq-config` | `GetDlqConfig` · `SetDlqConfig` |

Resources (`routeResourceRoutes`, `httpRouteResources.ts:21`):

| Method · Path | Command |
|---|---|
| `GET/POST /crons`, `GET/DELETE /crons/:name` | `CronList` · `Cron` · `CronGet` · `CronDelete` |
| `GET/POST /webhooks`, `DELETE /webhooks/:id`, `PUT /webhooks/:id/enabled` | `ListWebhooks` · `AddWebhook` · `RemoveWebhook` · `SetWebhookEnabled` |
| `GET/POST /workers`, `DELETE /workers/:id`, `POST /workers/:id/heartbeat` | `ListWorkers` · `RegisterWorker` · `UnregisterWorker` · `Heartbeat` |
| `GET /ping` · `GET /storage` | `Ping` · `StorageStatus` |

### Streaming endpoints

- `GET /events` and `GET /events/queues/:queue` — SSE stream; optional `Last-Event-ID` header for replay (`http.ts:168`).
- `GET /ws` and `GET /ws/queues/:queue` — WebSocket upgrade (`http.ts:154`).

### WebSocket protocol

Frame format (server → client): `{ event, ts, data }` (`wsHandler.ts:330`). Client commands over the socket:
- `{ cmd: "Subscribe", events: [...] }` / `{ cmd: "Unsubscribe", events: [...] }` — managed in `onMessage` before typed parsing (`wsHandler.ts:407`). Patterns validated against `VALID_PATTERNS` (`wsHandler.ts:41`): exact names, prefix wildcards (`job:*`, `queue:*`, …), or `*`.
- Any other JSON is parsed as a normal `Command` and routed through `handleCommand`; an `Auth` command that succeeds flips `ws.data.authenticated` (`wsHandler.ts:430`).

## Data Models

See [data-model](../data-model.md) for full definitions. Key shapes here:
- `HandlerContext` (`types.ts:8`): `{ queueManager, authTokens: Set<string>, authenticated: boolean, clientId? }`. For HTTP, `clientId` is intentionally absent (stateless; `http.ts:201`). For WS, `clientId = ws.data.id`.
- `WsData` (`wsHandler.ts:166`): `{ id, authenticated, queueFilter: string | null, subscriptions: Set<string> | null }`. `subscriptions === null` = legacy mode (receives every job event in the raw `JobEvent` shape).
- `SseClient` (`sseHandler.ts:29`): `{ id, controller, queueFilter }`. `BufferedEvent` (`sseHandler.ts:36`): `{ id, event, data, queue }` in the replay ring buffer.
- `JobEvent` (from `domain/types/queue`) is the input to both `broadcast` methods; `EVENT_MAP` (`sseHandler.ts:44`, `wsHandler.ts:25`) renames legacy `eventType`s (`pushed`→`job:pushed`, `pulled`→`job:active`, `drained`→`queue:drained`, …), defaulting unknown types to `job:<eventType>`.

## Business Logic / Control Flow

`fetch(req, server)` request pipeline (`http.ts:111`), short-circuiting in this order:
1. `OPTIONS` → `corsResponse` preflight (`http.ts:116`).
2. Unauthenticated liveness: `/health`, `/healthz`, `/live`, `/ready` (`http.ts:121`). These skip both auth and rate limiting.
3. Debug endpoints `POST /gc`, `GET /heapstats` → `checkAuth` then run (`http.ts:132`).
4. Rate limiting: `clientIp` from `x-forwarded-for[0]` / `x-real-ip` / `'unknown'`; if `getRateLimiter().isAllowed(ip)` is false → emit `ratelimit:hit`, return `429` (`http.ts:144`).
5. `/ws*` → auth, `wsHandler.canAccept()` (else `503`), then `server.upgrade(req, { data: { id: uuid(), authenticated: true, queueFilter, subscriptions: null } })` (`http.ts:154`).
6. `/events*` → auth, build SSE response via `sseHandler.createResponse(queueFilter, corsOrigin, lastEventId)` (`http.ts:168`).
7. `/prometheus` → conditional auth, return `text/plain; version=0.0.4` (`http.ts:179`).
8. General `checkAuth` for everything else; failure emits `auth:failed` (`http.ts:193`).
9. `routeRequest` (`http.ts:285`): stats/metrics/dashboard, then cascade through `routeJobRoutes` → `routeQueueRoutes` → `routeQueueConfigRoutes` → `routeResourceRoutes`, each returning a `Response` or `null` (no match). Fallthrough → `404`. Any thrown error → `500` JSON.

Each REST handler reads the body (`req.json()` or `parseJsonBody`, which tolerates empty body as `{}` and returns a `400` on malformed JSON — `httpEndpoints.ts:28`), builds the matching `Command`, awaits `handleCommand`, and maps the result to a status: typically `r.ok ? 200 : 400` (or `404` for read/lookup commands, plain `200` for list/idempotent commands).

Auth (`checkAuth`, `http.ts:50`): no-op when `authTokens` is empty; otherwise extracts the `Authorization: Bearer <token>` header and compares against every configured token with `constantTimeEqual` (timing-safe).

Event fan-out is wired once in `createHttpServer` (`http.ts:81`): `queueManager.subscribe` forwards each `JobEvent` to both `wsHandler.broadcast` and `sseHandler.broadcast`; `setDashboardEmit` routes non-job events (worker/queue/dlq/cron/…) to both handlers' `emitEvent`. Periodic broadcasters are started for both channels.

SSE broadcast (`sseHandler.broadcast`, `sseHandler.ts:149`): assigns a monotonic `id`, maps the event name, serializes `{ queue, jobId, timestamp, error?, progress?, prev?, delay? }`, buffers it in the ring (`bufferEvent`), enqueues `id:`/`event:`/`data:` to clients whose `queueFilter` matches (or is null), prunes clients whose `controller.enqueue` throws, then emits a `queue:counts` typed event for the affected queue. `createResponse` (`sseHandler.ts:319`) registers the client, immediately sends `retry: <ms>` + a `connected` frame, and on `resumeId > 0` replays buffered events with `id > lastEventId` (`replayEvents`, `sseHandler.ts:303`). Stream `cancel` deletes the client and releases its jobs/workers.

WS broadcast (`wsHandler.broadcast`, `wsHandler.ts:339`): for each client, skips on `queueFilter` mismatch; legacy clients (`subscriptions === null`) get the raw `JobEvent` JSON, subscribed clients get `{ event, ts, data }` only if the event matches a subscribed pattern (`matches`, `wsHandler.ts:175`). Then emits `queue:counts`. Sends go through `safeSend` (`wsHandler.ts:204`).

## Concurrency & Locking

The HTTP layer holds no shard locks itself; all locking happens inside `handleCommand`/`QueueManager` (see [Concurrency & Locking](./concurrency-and-locking.md)). Relevant connection-lifecycle behavior:
- **Stateless HTTP, no job ownership**: HTTP requests carry no `clientId`, so jobs pulled over REST are not tracked to a connection; orphaned/in-flight jobs are recovered only by stall detection (`http.ts:201`, see [Background Tasks](./background-tasks.md)).
- **WS/SSE own their pulled jobs**: on WS `close` (`http.ts:232`) and SSE stream `cancel` (`sseHandler.ts:344`), the handler calls `unregisterWorkersByClientId(clientId)` and `releaseClientJobs(clientId)` so jobs leased through that persistent connection are returned to the queue. WS additionally calls `getRateLimiter().removeClient(clientId)`.
- **WS idle/keepalive**: Bun auto-pings with a `120s` idle timeout and `maxPayloadLength` of 1 MiB (`http.ts:217`). SSE sends a `:heartbeat` comment every 30 s and prunes clients that error on write (`sseHandler.ts:130`).

## Edge Cases & Failure Modes

- **0-client early return** (perf invariant): both `SseHandler.broadcast` (`sseHandler.ts:155`) and `WsHandler.broadcast` (`wsHandler.ts:323`/`339`) and the typed-emit paths return immediately when there are no clients. This is load-bearing: without it every job event would still pay `JSON.stringify` + encode + ring-buffer + an O(queue-size) `getQueueJobCounts`, turning a bulk push into O(N²) even with no dashboard attached.
- **Connection limits**: SSE caps at `MAX_CLIENTS = 1000` → `503 "Too many SSE connections"` (`sseHandler.ts:320`); WS caps at `MAX_WS_CLIENTS = 1000` via `canAccept()` → `503` (`http.ts:157`). WS upgrade failure → `400`.
- **WS backpressure**: `safeSend` (`wsHandler.ts:204`) checks `ws.getBufferedAmount()`; above `BACKPRESSURE_BYTES` (1 MiB) it increments `droppedMessages` and *skips* the message (treats the client as alive but slow, does not disconnect). A `send` throw marks the client dead and removes it.
- **SSE replay bounds**: ring buffer holds the last `EVENT_BUFFER_SIZE = 1000` events; reconnects requesting older IDs silently miss the gap. Heartbeat = 30 s, advertised `retry: 3000` ms.
- **Body parsing**: routes using `parseJsonBody` treat empty body as `{}` (backward compat for optional-body routes); malformed JSON → `400 "Invalid JSON body"`. Routes using `req.json()` directly (PUSH, bulk, cron/webhook/worker create) also catch and return `400`. Worker heartbeat tolerates a missing/invalid body (`httpRouteResources.ts:189`).
- **State-filter aliasing (#95)**: `GET /queues/:queue/jobs/list` reads `state`, `status`, and `states`, each repeatable and comma-separated; previously only `state` was honored, so `?status=failed` silently returned the whole queue (`httpRouteQueues.ts:124`).
- **Paused-queue count consistency (#92)**: `dashboardQueueDetailEndpoint` runs `pausedView` so a paused queue reports ready jobs under `paused` (not `waiting`/`prioritized`), matching the per-state job lists (`httpEndpoints.ts:336`).
- **DLQ pagination robustness**: non-numeric `limit`/`offset` are ignored rather than producing an empty slice (`httpRouteQueueConfig.ts:44`).
- **Dashboard truncation**: workers and crons lists are capped at 100 items with a `truncated` flag (`httpEndpoints.ts:261`/`284`).
- **CORS injection on out-of-pipeline responses**: `withCors` (`http.ts:102`) adds `Access-Control-Allow-Origin` to health/ready/prometheus/debug responses only if not already set, never overwriting (audit #16–20).
- **CORS default gotcha**: `HttpServerConfig.corsOrigins` defaults to `['*']` inside `createHttpServer` (`http.ts:76`), but the server entrypoint always passes the env-resolved array, which defaults to `[]` when `CORS_ALLOW_ORIGIN` is unset (`config/resolve.ts:50`). With `[]`, `getCorsOrigin()` yields an empty `Access-Control-Allow-Origin` string rather than `*`. See [Security: TLS, Auth, CORS](./security-tls-auth.md).
- **Metrics auth asymmetry**: `/metrics` and `/stats` always require auth (when tokens are set) because they run after the general `checkAuth`; `/prometheus` requires auth only if `requireAuthForMetrics` (`METRICS_AUTH=true`); `/health*`, `/ready` never require auth.
- **TLS fail-fast**: `loadTlsOptions(config.tls)` runs before binding so bad cert/key paths abort startup rather than serving plaintext (`http.ts:250`).

## Configuration

Resolved in `config/resolve.ts` (config file > env > default) and passed into `HttpServerConfig`:

| Env var | Field | Default |
|---|---|---|
| `HTTP_PORT` | `port` | `6790` |
| `HOST` | `hostname` | `0.0.0.0` |
| `HTTP_SOCKET_PATH` | `socketPath` | unset (overrides host/port → Unix socket bind, `http.ts:252`) |
| `AUTH_TOKENS` (comma-sep) | `authTokens` | `[]` (auth disabled) |
| `CORS_ALLOW_ORIGIN` (comma-sep) | `corsOrigins` | `[]` (see CORS gotcha above) |
| `METRICS_AUTH` | `requireAuthForMetrics` | `false` |
| `TLS_CERT_FILE` + `TLS_KEY_FILE` | `tls` | unset (both required together) |
| `RATE_LIMIT_WINDOW_MS` / `RATE_LIMIT_MAX_REQUESTS` / `RATE_LIMIT_CLEANUP_MS` | rate limiter | `60000` / `10000` / `60000` (`rateLimiter.ts`) |

WS/SSE tuning constants are compile-time, not env-configurable: SSE `MAX_CLIENTS=1000`, `HEARTBEAT_MS=30000`, `RETRY_MS=3000`, `EVENT_BUFFER_SIZE=1000`; WS `MAX_WS_CLIENTS=1000`, `BACKPRESSURE_BYTES=1048576`, idle `120s`, `maxPayloadLength=1 MiB`.

## Related Docs

- [TCP Server Command Handlers](./tcp-server-handlers.md) — the shared `handleCommand` dispatcher.
- [TCP Wire Protocol & Framing](./tcp-protocol.md) — `parseCommand`/`serializeResponse` reused on the WS path.
- [Security: TLS, Auth, CORS](./security-tls-auth.md) — auth, CORS, and TLS termination details.
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md) — Prometheus/JSON metrics sources.
- [Webhooks, Events & Job Logs](./webhooks-and-events.md) — the event bus feeding SSE/WS.
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md) — per-queue limits set via these routes.
- [Configuration & Entrypoint](./configuration.md) — `resolveServerConfig` and `bootstrap`.
- [architecture](../architecture.md) · [data-model](../data-model.md)
