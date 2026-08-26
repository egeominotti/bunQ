# Native MCP Server

> **Category:** Integration · **Source:** `src/mcp/index.ts`, `src/mcp/server.ts`, `src/mcp/adapter.ts`, `src/mcp/backend/**/*.ts`, `src/mcp/types/adapter.ts`, `src/mcp/httpHandler.ts`, `src/mcp/prompts.ts`, `src/mcp/resources.ts`, `src/mcp/tools/*.ts`

## Purpose

The native MCP (Model Context Protocol) server exposes bunqueue to AI agents (Claude Desktop, IDEs, etc.) over stdio. It ships as the `bunqueue-mcp` binary and uses the official `@modelcontextprotocol/sdk` to register **73 tools**, **5 read-only resources**, and **3 prompt templates** that let an agent inspect, control, and consume from queues. The server can talk to bunqueue either **embedded** (direct SQLite via the shared `QueueManager`) or over **TCP** to a remote bunqueue instance, selected via `BUNQUEUE_MODE`. The `@modelcontextprotocol/sdk` (and its transitive `zod`) is an _optional_ peer dependency, so queue-only consumers never download it.

## Responsibilities & Scope

Owns:

- The `bunqueue-mcp` bin entrypoint and lazy SDK loading (`src/mcp/index.ts`).
- Tool/resource/prompt registration on an `McpServer` over `StdioServerTransport` (`src/mcp/server.ts`).
- The `McpBackend` abstraction and public adapter facade (`src/mcp/types/adapter.ts`, `src/mcp/adapter.ts`), with split embedded and TCP implementations under `src/mcp/backend/`.
- The `HttpHandlerRegistry`, which spawns embedded `Worker`s that auto-process jobs by issuing HTTP requests (`src/mcp/httpHandler.ts`).
- Per-invocation error handling + telemetry recording (`withErrorHandler`, `mcpTracker`).

Does NOT own (delegated):

- Actual queue/job logic — delegated to [Core Queue Engine](./core-queue-engine.md) (embedded) or the [TCP Server Command Handlers](./tcp-server-handlers.md) (TCP mode).
- Wire framing/connection pooling — delegated to [Client Transport](./client-transport.md) (`TcpConnectionPool`).
- Flow tree construction — delegated to [FlowProducer & Job Dependencies](./flow-producer.md) (`FlowProducer`).
- Cloud telemetry delivery — `mcpTracker` only buffers; delivery is owned by [bunqueue Cloud Dashboard Integration](./cloud-integration.md) (`CloudAgent`).

## Dependencies

Internal:

- `getSharedManager` / `shutdownManager` from `src/client/manager.ts` (embedded `QueueManager`).
- `FlowProducer` from `src/client/flow.ts` and `JobNode` from `src/client/flowTypes.ts`.
- `TcpConnectionPool` from `src/client/tcpPool.ts` (TCP mode).
- `Worker` from `src/client/worker/worker.ts` (HTTP handlers).
- `CloudAgent` from `src/infrastructure/cloud/cloudAgent.ts` (embedded telemetry).
- `WEBHOOK_EVENTS` from `src/domain/types/webhook.ts` (webhook tool schema), `VERSION` from `src/shared/version.ts`.

External / runtime:

- `@modelcontextprotocol/sdk` — `McpServer`, `StdioServerTransport` (optional peer dep, `^1.26.0`).
- `zod` — tool input schemas (resolved transitively from the SDK; not declared by bunqueue itself).
- Bun runtime: `fetch` + `AbortController` (HTTP handlers), SQLite via `QueueManager` (embedded).

## Public Interface

**Binary:** `bunqueue-mcp` → `./dist/mcp/index.js` (declared in `package.json` `bin`).

**Exported functions / classes:**

- `run(): Promise<void>` — `src/mcp/server.ts:39`. The server entrypoint; builds the `McpServer`, registers everything, wires shutdown, connects stdio.
- `createBackend(): Promise<McpBackend>` — `src/mcp/adapter.ts:21-32`. Returns `TcpBackend` (connected) when `BUNQUEUE_MODE === 'tcp'`, else `EmbeddedBackend`.
- `EmbeddedBackend`, `TcpBackend` (both `implements McpBackend`) — `src/mcp/backend/embedded/index.ts:6` / `src/mcp/backend/tcp/index.ts:5`.
- `HttpHandlerRegistry` with `register(queue, handler)`, `unregister(queue): boolean`, `list()`, `shutdown()` — `src/mcp/httpHandler.ts:22`.
- `withErrorHandler<T>(toolName, fn)` — `src/mcp/tools/withErrorHandler.ts:31`.
- `mcpTracker` (singleton `McpOperationTracker`) with `record`, `drain`, `peek`, `getSummary`, `count` — `src/mcp/tools/mcpTracker.ts:105`.
- Registration functions: `registerJobTools`, `registerJobMgmtTools`, `registerConsumptionTools`, `registerQueueTools`, `registerDlqTools`, `registerCronTools`, `registerRateLimitTools`, `registerWebhookTools`, `registerWorkerMgmtTools`, `registerMonitoringTools`, `registerFlowTools`, `registerHandlerTools`, `registerResources`, `registerPrompts`.
- Exported types: `McpBackend`, `JobCounts`, `SerializedJob`, `SerializedCron`, `WebhookInfo`, `WorkerInfo`, `FlowJobInput`, `FlowStepInput`, `FlowNodeResult` are defined in `src/mcp/types/adapter.ts:4-174` and re-exported by `src/mcp/adapter.ts:9-19`; `HttpHandler` (`src/mcp/httpHandler.ts:9-15`); `McpOperation`, `McpSummary` (`src/mcp/tools/mcpTracker.ts`).

**MCP transport:** stdio only (`StdioServerTransport`, `src/mcp/server.ts:104`). No HTTP/SSE listener is opened by the MCP server itself.

**Tools registered (73 total, all prefixed `bunqueue_`):**

| Group (file)                           | Count | Tools                                                                                                                                                                                                        |
| -------------------------------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Job (`jobTools.ts`)                    | 11    | `add_job`, `add_jobs_bulk`, `get_job`, `get_job_state`, `get_job_result`, `cancel_job`, `promote_job`, `update_progress`, `get_children_values`, `get_job_by_custom_id`, `wait_for_job`                      |
| Job mgmt (`jobMgmtTools.ts`)           | 6     | `update_job_data`, `change_job_priority`, `move_to_delayed`, `discard_job`, `get_progress`, `change_delay`                                                                                                   |
| Consumption (`consumptionTools.ts`)    | 8     | `pull_job`, `pull_job_batch`, `ack_job`, `ack_job_batch`, `fail_job`, `job_heartbeat`, `job_heartbeat_batch`, `extend_lock`                                                                                  |
| Queue control (`queueTools.ts`)        | 11    | `list_queues`, `count_jobs`, `get_jobs`, `get_job_counts`, `pause_queue`, `resume_queue`, `drain_queue`, `obliterate_queue`, `clean_queue`, `is_paused`, `get_counts_per_priority`                           |
| DLQ (`dlqTools.ts`)                    | 4     | `get_dlq`, `retry_dlq`, `purge_dlq`, `retry_completed`                                                                                                                                                       |
| Cron (`cronTools.ts`)                  | 4     | `add_cron`, `list_crons`, `get_cron`, `delete_cron`                                                                                                                                                          |
| Rate/concurrency (`rateLimitTools.ts`) | 4     | `set_rate_limit`, `clear_rate_limit`, `set_concurrency`, `clear_concurrency`                                                                                                                                 |
| Webhook (`webhookTools.ts`)            | 4     | `add_webhook`, `remove_webhook`, `list_webhooks`, `set_webhook_enabled`                                                                                                                                      |
| Worker mgmt (`workerMgmtTools.ts`)     | 3     | `register_worker`, `unregister_worker`, `worker_heartbeat`                                                                                                                                                   |
| Monitoring (`monitoringTools.ts`)      | 11    | `get_stats`, `get_queue_stats`, `list_workers`, `get_job_logs`, `add_job_log`, `get_storage_status`, `get_per_queue_stats`, `get_memory_stats`, `get_prometheus_metrics`, `clear_job_logs`, `compact_memory` |
| Flow (`flowTools.ts`)                  | 4     | `add_flow`, `add_flow_chain`, `add_flow_bulk_then`, `get_flow`                                                                                                                                               |
| HTTP handler (`handlerTools.ts`)       | 3     | `register_handler`, `unregister_handler`, `list_handlers`                                                                                                                                                    |

70 of these route through the `McpBackend`; the 3 handler tools act on the in-process `HttpHandlerRegistry` and have no backend equivalent.

**Resources (`src/mcp/resources.ts`):** `bunqueue://stats` (`:34`), `bunqueue://queues` (`:53`), `bunqueue://crons` (`:78`), `bunqueue://workers` (`:97`), `bunqueue://webhooks` (`:116`) — all `application/json`, read-only.

**Prompts (`src/mcp/prompts.ts`):** `bunqueue_health_report` (`:32`), `bunqueue_debug_queue` (`:111`, arg `queue`), `bunqueue_incident_response` (`:177`, optional arg `queue`).

## Data Models

All tool replies are `{ content: [{ type: 'text', text: JSON.stringify(...) }], isError? }`. The principal serialized shapes (see [data-model](../data-model.md) for the underlying `Job`/cron/webhook types):

- `SerializedJob` — `{ id, name, queue, data, priority, state?, progress, attempts, maxAttempts, createdAt (ISO), startedAt? (ISO) }`. Built by `serializeMcpJob` (embedded, `backend/serializers.ts:6-19`) or `parseJob` (TCP, `backend/tcp/base.ts:46-61`); both stringify the numeric `id` and timestamps. TCP parsing accepts legacy payloads where the name was stored inside `data`, but always returns `name` and clean user `data` separately.
- `JobCounts` — `{ waiting, prioritized, delayed, active, completed, failed, paused }`.
- `SerializedCron` — `{ name, queue, schedule?, repeatEvery?, nextRun (ISO|null), executions }`.
  The embedded serializer accepts the normalized domain `CronJob`, where the
  inactive scheduling field is `null`, and converts it to `undefined`. The TCP
  adapter performs the same boundary conversion on `CronInfo`, so JSON output
  omits `schedule` for interval crons and `repeatEvery` for pattern crons in
  both modes. TCP creation reads the authoritative nested `cron` response,
  including its ISO `nextRun` and preserved execution count.
- `WebhookInfo` — `{ id, url, events[], queue?, enabled }`.
- `WorkerInfo` — `{ id, name, queues[], active, processed, failed, lastHeartbeat }`.
- `FlowNodeResult` — recursive `{ jobId, name, queueName, children? }` (`backend/serializers.ts:34-40`, `types/adapter.ts:70-75`).
- `McpOperation` — `{ tool, queue|null, timestamp, durationMs, success, error|null }`; `McpSummary` — `{ totalInvocations, successCount, failureCount, avgDurationMs, topTools[] }`.
- `HttpHandler` — `{ url, method: 'GET'|'POST'|'PUT'|'DELETE', headers?, body?, timeoutMs? }`.

## Business Logic / Control Flow

**Startup (`bunqueue-mcp`):**

1. `index.ts:75` calls `launch()`, which dynamically `import('./server.js')` and calls `run()` (`index.ts:53`). The dynamic import keeps the SDK + zod out of the static graph.
2. If the import throws `ERR_MODULE_NOT_FOUND` (or a "Cannot find module/package" message) mentioning `@modelcontextprotocol/sdk` or `zod`, the guard at `index.ts:62-68` prints an install hint (`bun add @modelcontextprotocol/sdk`) and exits 1. Any other error is printed as `Fatal error:` and exits 1.
3. `run()` (`server.ts:39`) calls `createBackend()`, constructs `McpServer` (`name: 'bunqueue-mcp'`, `version: VERSION`), instantiates a `HttpHandlerRegistry`, then registers all 12 tool groups + resources + prompts (`server.ts:51-66`).
4. In embedded mode with `BUNQUEUE_CLOUD_URL` set, a `CloudAgent` is created and given `getMcpOperations` (`server.ts:70-86`). **Invariant:** `mcpTracker.getSummary()` is called _before_ `mcpTracker.drain()` because `drain()` empties the buffer (`server.ts:79-82`).
5. Connects a `StdioServerTransport` (`server.ts:104`) and writes a startup line to stderr (`server.ts:107`).

**Per-tool invocation:** every handler is wrapped by `withErrorHandler(toolName, fn)` (`withErrorHandler.ts:31`). It records `start = Date.now()`, runs `fn`, and on return records an `McpOperation` to `mcpTracker` (queue extracted from `args.queue` or `args.queueName`, `:19`). Thrown errors are caught, recorded with `success: false`, and returned as `{ isError: true, content: [{ text: JSON.stringify({ error }) }] }` (`:48-62`) — so tool errors surface as MCP error results rather than transport failures.

**Backend dispatch:** each tool calls one `McpBackend` method.

- `EmbeddedBackend` calls the matching `QueueManager` method. For example, `addJob` passes `name` and user `data` as separate fields to `manager.push` (`backend/embedded/jobs.ts:6-20`). Flow tools lazily instantiate one embedded `FlowProducer` (`backend/embedded/base.ts:11-18`).
- `TcpBackend` translates to wire commands via `pool.send` (e.g. `PUSH`,
  `PUSHB`, `PUSHF`, `PULL`, `ACK`, `FAIL`, `GetJob`, `Cron`,
  `DashboardQueues`). Flow tools route through a TCP `FlowProducer`
  (`backend/tcp/base.ts:31-40`, `backend/tcp/index.ts:6-29`), so the Bun backend receives the atomic graph contract.

**HTTP handler registration** (`register_handler` → `HttpHandlerRegistry.register`, `httpHandler.ts:25`): stops any existing handler on the same queue, then spawns an embedded `Worker(queue, processor, { embedded: true, concurrency: 1 })`. The processor opens an `AbortController` with `setTimeout(timeoutMs ?? 30_000)` (`:36`), issues `fetch(url, init)` (body = `handler.body ?? job.data` for non-GET/DELETE), parses JSON or text by `content-type`, and throws on non-2xx so the job fails and retries through normal worker semantics.

## Concurrency & Locking

The MCP server holds no locks of its own; concurrency safety is delegated to the backend ([Concurrency & Locking](./concurrency-and-locking.md) for embedded, TCP server for remote). Notable points:

- `mcpTracker` is a single-threaded in-process singleton (no locking needed); `record` and `drain` mutate one array.
- HTTP handlers run as real `Worker`s with `concurrency: 1` per queue, participating in normal lock-based job ownership; lease/heartbeat behavior is the Worker's, not the MCP layer's.
- `extend_lock`/`job_heartbeat` tools forward to the lock/heartbeat machinery. In TCP mode `extendLock` sends a `JobHeartbeat` command carrying `token`/`duration` (`backend/tcp/jobs.ts:118-121`); the embedded path calls `manager.extendLock(id, token, duration)` directly (`backend/embedded/jobs.ts:128-130`).

## Edge Cases & Failure Modes

- **Optional SDK absent:** handled with an actionable install hint and exit 1 (`index.ts:62-68`).
- **Telemetry buffer bound:** `mcpTracker` is a ring buffer capped at `MAX_BUFFER_SIZE = 200`; at capacity each `record` does `Array.shift()` (O(n) but n≤200, ~40KB max, `mcpTracker.ts:44-49`). If no `CloudAgent` consumer drains it, the buffer self-bounds by evicting oldest.
- **Tool errors are non-fatal:** `withErrorHandler` converts thrown errors to `isError` results; `extractErrorText` truncates raw non-JSON error text to 200 chars (`withErrorHandler.ts:77`).
- **Not-found results:** `get_job`, `get_job_by_custom_id`, `get_progress`, `get_flow`, `get_cron` return `{ isError: true, error: '...not found' }` rather than throwing.
- **Pagination translation (issue #87):** the MCP `get_jobs` tool exposes `start`/`end`, but the TCP protocol uses `offset`/`limit`; `TcpBackend.getJobs` translates `limit = end - start` (`backend/tcp/queues.ts:5-18`) so pagination actually applies in TCP mode.
- **Per-queue stats (issue #87):** `TcpBackend.getPerQueueStats` queries `DashboardQueues` (not `Metrics`) to get a real per-queue breakdown (`backend/tcp/services.ts:140-156`).
- **Response-envelope variance:** `TcpBackend` defensively reads nested envelopes — `GetJobCounts` under `response.counts`, `ListWorkers` under `response.data.workers` (falling back to `response.workers`), and `StorageStatus` under `response.data` (`backend/tcp/queues.ts:20-31`, `backend/tcp/services.ts:111-124`, `backend/tcp/services.ts:176-182`).
- **Storage-health redaction:** the embedded backend applies the same
  `clientStorageStatus` projection as the TCP server. Non-disk SQL/network
  diagnostics never enter an MCP tool result; SQLite disk-full keeps its
  actionable error. The TCP backend inherits the projection from the
  `StorageStatus` command.
- **Cron null normalization:** the domain and TCP protocol intentionally retain
  `null` for the inactive cron scheduling field, while the MCP
  `SerializedCron` contract uses optional fields. Both backends normalize the
  boundary to `undefined`; regression coverage exercises `add`, `list` and
  `get` for interval and pattern crons against embedded and a real TCP broker.
  The same shared contract also runs explicitly in both functional matrices via
  `scripts/embedded/test-mcp-cron-serialization.ts` and
  `scripts/tcp/test-mcp-cron-serialization.ts`, including metadata, deletion
  and post-delete lookup behavior.
- **Cron creation errors:** protocol-level `{ ok: false, error }` responses are
  rejected with the broker error instead of being converted into fabricated
  cron metadata. Successful responses must contain a valid nested cron
  envelope; malformed success payloads are rejected.
- **HTTP handler single-per-queue:** registering a handler on a queue that already has one silently `unregister`s the previous worker first (`httpHandler.ts:27-29`); only one handler per queue can be active.
- **HTTP handler timeout:** request aborts via `AbortController` after `timeoutMs` (default 30s; tool schema clamps 1000–120000ms); the timer is always cleared in `finally` (`httpHandler.ts:68`).
- **Webhook event validation:** `add_webhook` accepts only `WEBHOOK_EVENTS` (`job.pushed|started|completed|failed|progress`); `job.stalled` is accepted by stored types for backward-compat but is never emitted.
- **Tool input bounds:** `wait_for_job` timeout 100–30000ms; `pull_job`/`pull_job_batch` timeout 0–30000ms with `count` 1–1000; `update_progress` 0–100.
- **Graceful shutdown:** on SIGINT/SIGTERM, `run()` stops the `CloudAgent`, shuts the handler registry (closes all spawned workers), `backend.shutdown()` (closes pool/FlowProducer/manager), `server.close()`, then `process.exit(0)`; cleanup errors are swallowed (`server.ts:89-101`).
- **Embedded `getStats` BigInt:** serialized via a `JSON.stringify` replacer that coerces `bigint`→`Number` to keep the payload JSON-safe (`backend/embedded/services.ts:91-100`).

## Configuration

| Env var                                                             | Default           | Effect                                                                                                                                                                                                                                                                                                                                                        |
| ------------------------------------------------------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BUNQUEUE_MODE`                                                     | `embedded`        | `tcp` selects `TcpBackend`; anything else → `EmbeddedBackend` (`adapter.ts:21-32`).                                                                                                                                                                                                                                                                           |
| `BUNQUEUE_HOST`                                                     | `localhost`       | TCP host (TCP mode).                                                                                                                                                                                                                                                                                                                                          |
| `BUNQUEUE_PORT`                                                     | `6789`            | TCP port (parsed with `parseInt`, TCP mode).                                                                                                                                                                                                                                                                                                                  |
| `BUNQUEUE_TOKEN`                                                    | —                 | Auth token forwarded to the TCP pool / FlowProducer.                                                                                                                                                                                                                                                                                                          |
| `BUNQUEUE_POOL_SIZE`                                                | `2`               | TCP connection pool size (`backend/tcp/base.ts:17-24`).                                                                                                                                                                                                                                                                                                       |
| `BUNQUEUE_DATA_PATH` / `BQ_DATA_PATH` / `DATA_PATH` / `SQLITE_PATH` | unset → in-memory | SQLite path for the embedded `QueueManager`, resolved by the shared manager in that precedence order (`src/client/manager.ts:13-17`). When none is set, `QueueManager` gets no `dataPath` and runs with **no persistence at all** (jobs live only in memory); the `./data/bunq.db` default belongs to the standalone server bootstrap, not to `bunqueue-mcp`. |
| `BUNQUEUE_CLOUD_URL`                                                | —                 | When set in embedded mode, enables `CloudAgent` MCP telemetry (`server.ts:70`). Other `BUNQUEUE_CLOUD_*` vars apply via the agent — see [bunqueue Cloud Dashboard Integration](./cloud-integration.md).                                                                                                                                                       |

## Related Docs

- [architecture](../architecture.md), [data-model](../data-model.md)
- [Core Queue Engine](./core-queue-engine.md) — embedded backend target
- [TCP Server Command Handlers](./tcp-server-handlers.md) / [Client Transport](./client-transport.md) — TCP backend target
- [Client SDK: Worker (& sandboxed)](./client-worker-sdk.md) — workers spawned by HTTP handlers
- [FlowProducer & Job Dependencies](./flow-producer.md) — flow tools
- [Scheduler & Cron](./scheduler-and-cron.md) — cron tools
- [Dead Letter Queue (DLQ)](./dead-letter-queue.md) — DLQ tools
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md) — rate/concurrency tools
- [Webhooks, Events & Job Logs](./webhooks-and-events.md) — webhook/log tools
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md) — monitoring tools
- [bunqueue Cloud Dashboard Integration](./cloud-integration.md) — `mcpTracker` consumer
- [Configuration & Entrypoint](./configuration.md) — data path resolution
