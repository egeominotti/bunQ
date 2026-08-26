# bunqueue Cloud Dashboard Integration

> **Category:** Integration · **Source:** `src/infrastructure/cloud/cloudAgent.ts`, `src/infrastructure/cloud/queueAdapter/`, `src/infrastructure/cloud/snapshotCollector.ts`, `src/infrastructure/cloud/commands/`, `src/infrastructure/cloud/snapshot/`, `src/infrastructure/cloud/types/`, `src/infrastructure/cloud/httpSender.ts`, `src/infrastructure/cloud/wsSender.ts`, `src/infrastructure/cloud/commandHandler.ts`, `src/infrastructure/cloud/circuitBreaker.ts`, `src/infrastructure/cloud/buffer.ts`, `src/infrastructure/cloud/config.ts`, `src/infrastructure/cloud/redact.ts`

## Purpose

The Cloud agent ships telemetry from a running bunqueue instance to the hosted bunqueue.io dashboard for remote monitoring and (optionally) remote control. It collects a full server snapshot (stats, queues, jobs, DLQ, workers, crons, webhooks, locks, analytics) and POSTs it to an HTTP ingest endpoint on a self-tuning interval, while keeping an authenticated WebSocket open to receive dashboard-issued commands. It is entirely opt-in: when `BUNQUEUE_CLOUD_URL` / `BUNQUEUE_CLOUD_API_KEY` are unset, `loadCloudConfig` returns `null` and no agent is created, so there is zero runtime overhead (`config.ts:15`, `cloudAgent.ts:56`).

## Responsibilities & Scope

Owns:

- Periodic snapshot collection from `QueueManager` and pushing it to the dashboard's `POST /api/v1/ingest` endpoint (`httpSender.ts:26`).
- An outbound WebSocket to `/api/v1/commands` used **only** for remote commands and keepalive — no bulk telemetry flows over WS (`wsSender.ts:41`, header comment `cloudAgent.ts:7`).
- Buffering snapshots when the dashboard is unreachable, and a circuit breaker to stop hammering a dead endpoint (`buffer.ts`, `circuitBreaker.ts`).
- Redacting configured job-data fields and optionally omitting job data entirely before it leaves the process (`redact.ts`, `cloudAgent.ts:229`).
- Dispatching whitelisted remote commands through the selected `CloudQueueAdapter`
  Strategy and returning camelCased results over WS (`commandHandler.ts`,
  `queueAdapter/registry.ts`, `commands.ts`).
- HMAC-signing the compressed payload when a signing secret is configured (`httpSender.ts:99`).

Does NOT own:

- The dashboard / ingest server (external SaaS at bunqueue.io).
- Queue/job mutation logic — every command delegates to `QueueManager` methods owned by the [Core Queue Engine](./core-queue-engine.md), [Job Queries & Control](./job-queries-and-control.md), [DLQ](./dead-letter-queue.md), [Scheduler & Cron](./scheduler-and-cron.md), and [Webhooks & Events](./webhooks-and-events.md).
- Local stats/metrics surfaces — see [Stats, Metrics & Monitoring](./stats-and-monitoring.md) and [HTTP API](./http-api.md).
- Server connection counts, S3 backup status, SQLite stats, and MCP operation history — these are injected via `ServerHandles` from [Configuration & Entrypoint](./configuration.md), [S3 Backup](./backup-s3.md), and the [MCP Server](./mcp-server.md).

## Dependencies

Internal:

- `CloudQueueAdapter` (`queueAdapter/types.ts`) — complete engine boundary for
  shared snapshot and command data. `LocalCloudQueueAdapter` delegates to the
  existing memory/SQLite surface; `PostgresCloudQueueAdapter` uses only durable
  PostgreSQL methods for shared state.
- `QueueManager` remains the source of explicitly process-local memory,
  throughput, latency, connection, webhook, task, and MCP telemetry.
- `throughputTracker`, `latencyTracker`, `backgroundTasks.getTaskErrorStats` (`application/*`) — rate/latency/error analytics folded into snapshots (`snapshotCollector.ts:9-12`).
- `shared/version` (`VERSION`), `shared/logger` (`cloudLog`).
- `domain/types/job` (`jobId` brand constructor) and `domain/types/queue` (`JobEvent`).

External / runtime:

- `msgpackr` (`pack`/`unpack`) — the only third-party import in the module; payloads are MessagePack-encoded.
- Bun built-ins: `Bun.zstdCompress` / `Bun.zstdDecompress` (compression), `fetch` + `AbortSignal.timeout` (HTTP), global `WebSocket` with Bun's header-bearing constructor, `crypto.subtle` (HMAC-SHA256), `Bun.env`, `Bun.sleep`, `os` (`hostname`/`arch`/`platform`/`cpus`), `process.memoryUsage`.

## Public Interface

Exported from `src/infrastructure/cloud/index.ts`:

- `class CloudAgent` — orchestrator.
  - `static create(queueManager: QueueManager, dataPath?: string): CloudAgent | null` — loads env config, returns `null` if disabled (`cloudAgent.ts:56`). Used by the [MCP Server](./mcp-server.md).
  - `static createFromConfig(queueManager: QueueManager, config: CloudConfig): CloudAgent` — for the config-file flow; always constructs + starts (`cloudAgent.ts:66`). Used by `bootstrap.ts`.
  - `setServerHandles(handles: ServerHandles): void` — injects connection/backup/SQLite/MCP probes (`cloudAgent.ts:51`).
  - `start(): void`, `stop(): Promise<void>`, `getInstanceId(): string`, `getSnapshot(): Promise<CloudSnapshot>`.
- `function loadCloudConfig(dataPath?: string): CloudConfig | null` (`config.ts:10`).
- Types `CloudConfig`, `CloudSnapshot`, `CloudEvent` (`types.ts`).

Internal but load-bearing:

- `collectSnapshot(params: CollectSnapshotParams): Promise<CloudSnapshot>` (`snapshotCollector.ts:44-210`) and its `ServerHandles` interface (`types/collector.ts:4-35`).
- `handleCommand(queueManager, cmd: CloudCommand, context?): Promise<CloudCommandResult>` (`commandHandler.ts:44-100`).
- `resolveCloudQueueAdapter(queueManager): CloudQueueAdapter` selects and caches
  one complete Strategy for the manager lifetime. There is no per-method
  PostgreSQL-to-local fallback.
- `redactData(data, redactFields): unknown` (`redact.ts:11`).
- `class HttpSender`, `class WsSender`, `class CircuitBreaker`, `class SnapshotBuffer`.

Network endpoints **called** (this process is the client, not a server):

- `POST {BUNQUEUE_CLOUD_URL}/api/v1/ingest` — snapshot upload, `Content-Type: application/x-msgpack`, `Content-Encoding: zstd` (`httpSender.ts:26,92`).
- `WS {BUNQUEUE_CLOUD_URL→ws(s)}/api/v1/commands` — command channel (`wsSender.ts:41`).

Remote command actions (whitelist composed in `commands.ts:8-12` from `commands/queues.ts`, `commands/jobs.ts`, and `commands/integrations.ts`):
`queue:pause`, `queue:resume`, `queue:drain`, `queue:clean`, `queue:obliterate`, `queue:promoteAll`, `queue:retryCompleted`, `queue:rateLimit`, `queue:clearRateLimit`, `queue:concurrency`, `queue:clearConcurrency`, `queue:stallConfig`, `queue:dlqConfig`, `queue:detail`, `queue:list`, `job:cancel`, `job:promote`, `job:push`, `job:priority`, `job:discard`, `job:delay`, `job:updateData`, `job:clearLogs`, `job:retry`, `job:logs`, `job:result`, `job:list`, `job:get`, `job:listAll`, `dlq:retry`, `dlq:purge`, `cron:upsert`, `cron:delete`, `webhook:add`, `webhook:remove`, `webhook:set-enabled`, `stats:refresh`, `s3:backup`. Plus `snapshot:get`, handled specially in `handleCommand` because it needs the agent's `getSnapshot` context (`commandHandler.ts:49-68`).

WS control messages handled: incoming `ping` → reply `pong`; incoming `pong` ignored; `handshake_ack` logged; `command` dispatched (`wsSender.ts:120-137`).

## Data Models

The wire payload is `CloudSnapshot` (`types/snapshot.ts:41-88`) — a large flat object. Key groups:

- Identity / lifecycle: `instanceId`, `instanceName`, `version`, `hostname`, `pid`, `startedAt`, `timestamp`, `sequenceId`, optional `shutdown` (set `true` only on the final flush, `cloudAgent.ts:151`).
- Aggregates: `stats` (counts + `totalPushed/Pulled/Completed/Failed` serialized as **strings** to avoid 53-bit overflow), `throughput`, `latency` (averages + p50/p95/p99), `memory`, `collections` (internal map/heap sizes), `workers`, `storage`, `taskErrors`.
- Per-queue arrays/maps: `queues`, `queueConfigs`, `queueThroughput`, `queueWaitTime`, `queueRetryRate`, `queueBacklogVelocity`, `queuePriorityDistribution`, `queueExtended` (dedup/groups/deps).
- Detail lists: `recentJobs` (all queues, all states, capped at 1000 jobs per queue per state via `start: 0, end: 999`, `snapshot/jobs.ts:132-149`), `dlqEntries` (with `attemptHistory`, uncapped), `workerDetails`, `webhooks`, `topErrors`, `stallDetails`, `activeLocks`, `crons`, `jobResults`, `jobLogEntries`.
- Optional: `events?: CloudEvent[]` (buffered job events drained into the snapshot), `mcpOperations`/`mcpSummary`, `s3Backup`, `sqliteStats`, `runtime`.

`CloudEvent` (`types/event.ts:1-14`) wraps a `JobEvent` (`eventType`, `queue`, `jobId`, `error?`, `progress?`, `data?`, `prev?`, `delay?`). `CloudCommand` / `CloudCommandResult` (`types/command.ts:5-40`) are the WS command envelope: `{ type:'command', id, action, ...args }` → `{ type:'command_result', id, success, data?|error? }`.

`CloudConfig` (`types/config.ts:1-18`) is the parsed environment configuration; see [Configuration](#configuration) for its runtime defaults. For full domain shapes (Job, DLQ entry, lock) see [data-model](../data-model.md).

## Business Logic / Control Flow

### Startup (`cloudAgent.ts:73`)

1. The first `start()` marks the agent running, logs the connection and, if `useHttp`, fires one `sendSnapshot()` then `scheduleNext()`. The factories already call it; repeated calls are no-ops, including an explicit call on a factory-created agent.
2. If `useWebSocket`, a `WsSender` is constructed. When `remoteCommands` is on, a command handler is registered that calls `handleCommand(...)` and pushes the result back via `wsSender.sendRaw` (`cloudAgent.ts:91-108`). Then `wsSender.connect()` opens the socket.
3. `subscribeToEvents()` registers a `QueueManager.subscribe` callback (`cloudAgent.ts:210`).

### Snapshot push (`cloudAgent.ts:188-212` → `snapshotCollector.ts:44-210`)

1. `collectSnapshot` first asks the selected adapter for one
   `CloudSnapshotSource`, then maps that immutable source into the wire payload.
   Memory/SQLite builds it from the unchanged local structures. PostgreSQL reads
   jobs, queue registry/config, aggregate counts, lifetime terminal totals,
   results, logs, workers, crons, and leases inside one bounded
   `REPEATABLE READ READ ONLY` transaction; a receiving broker therefore does
   not mix cache generations or omit work committed through another broker.
   Storage health is copied through `clientStorageStatus`: SQLite disk-full
   retains its actionable error, while non-disk PostgreSQL/driver diagnostics
   are reduced to `Internal server error` before either a full snapshot or an
   immediate stats refresh leaves the process.
   Queue-detail reads explicitly include the requested queue even if it only
   has configuration state and no registered jobs. This preserves SQLite
   configuration-only views, including disabled stall detection, while the
   PostgreSQL adapter resolves the same view from durable queue state.
2. The adapter bounds live PostgreSQL detail to 1000 jobs per queue/state and
   retained completion/result configuration caps. The pure snapshot mapper
   applies redaction after the durable transaction has closed.
3. `enrichFailedJobDurations` back-fills `duration`/`completedAt`/`totalDuration` for failed jobs using the last DLQ attempt-history entry (`snapshot/collector.ts:45-68`).
4. Throughput/backlog analytics are **delta-based**: module-level `previousQueueTotals` / `previousQueueWaiting` maps hold the previous snapshot's totals; rates require >0.5s (throughput) / >0.1min (backlog) elapsed to emit (`snapshot/analytics.ts:4-8`, `snapshot/analytics.ts:18-29`, `snapshot/analytics.ts:124-137`).
5. Buffered events are spliced into `snapshot.events` (`cloudAgent.ts:203-208`), then `httpSender.send(snapshot)`.

### HTTP send (`httpSender.ts:30`)

1. Flush any buffered snapshots first (`flushBuffer`, FIFO, one at a time, stops on first failure, `httpSender.ts:56`).
2. If the circuit breaker is open, buffer and return without a network call (`httpSender.ts:36`).
3. `post()` packs to MessagePack, zstd-compresses at level 6, records `lastCompressedKB`, sets auth + timestamp headers, optionally adds `X-Signature` = hex HMAC-SHA256 of the compressed bytes, and `fetch`es with a 10s `AbortSignal.timeout` (`httpSender.ts:77-116`).
4. On success → `circuitBreaker.onSuccess()`; on throw → `onFailure()` + buffer the snapshot. `401`/`403` are logged at error level (visible auth/plan failures) before re-throwing (`httpSender.ts:118-131`).

### Adaptive interval (`cloudAgent.ts:167-186`)

`computeInterval()` chooses the next delay purely from `lastCompressedKB`: `<50KB → 5s`, `<200KB → 10s`, `<500KB → 20s`, else `30s`. `scheduleNext()` re-arms one identity-checked `setTimeout` after each push. A cleared or superseded callback is a no-op and cannot detach the active handle. The configured `intervalMs` is logged at startup but is **not** used for scheduling.

### Remote command path (`wsSender.ts:115-171` → `commandHandler.ts:44-100`)

1. Incoming binary frame is auto-detected as zstd (magic `28 b5 2f fd`) or plain msgpack, or JSON text, then unpacked (`wsSender.ts:146-171`).
2. A `command` is forwarded only if `remoteCommands` is set and `action`+`id` are present (`wsSender.ts:129`).
3. `handleCommand` looks up the action in `COMMANDS`; unknown actions return
   `success:false` with `Unknown command:`. The handler receives the resolved
   `CloudQueueAdapter` and `CommandContext`, the raw result is
   `camelKeys`-normalized (PascalCase→camelCase, skipping user-data keys), and
   wrapped in a `command_result`. `job:list` normalizes `offset` and `limit` to
   finite non-negative integers and uses a half-open `[start, end)` page on both
   adapters, matching the unchanged local `QueueManager.getJobs` contract.
   Infrastructure failures from both regular handlers and `snapshot:get` are
   sanitized in the response; their raw detail remains available only in the
   local Cloud error log.
4. Result is zstd(msgpack)-encoded and sent back over WS (`wsSender.ts:141`).

For `s3:backup`, `CloudAgent` resolves its current `ServerHandles` at command
time and invokes the injected `triggerBackup`. `bootstrap.ts` exposes this
trigger only when S3 backup is enabled and a manager exists. A disabled setup or
a rejected backup therefore returns an explicit `command_result` with
`success:false`; it cannot report a false successful no-op. Resolving the handle
at command time is required because `start()` precedes `setServerHandles()`.

### Shutdown (`cloudAgent.ts:123-165`)

Effect-idempotent through the `stopped` flag. It clears timers, unsubscribes
events, collects one shutdown snapshot with `shutdown=true`, and races
`httpSender.send` against a 2s `Bun.sleep` (best-effort), then stops the WS
sender. Shutdown is terminal, matching `WsSender`: a later `start()` is a no-op
and cannot recreate a snapshot timer, WebSocket, or event subscription.
Concurrent `stop()` callers do not share a completion promise, and a regular
snapshot already in flight is not cancelled or awaited; the shutdown marker is
therefore best-effort rather than a network-ordering barrier. The server
shutdown coordinator calls it after the shutdown event and active-job drain,
bounds the whole Cloud stop at 2.5s, and continues to storage cleanup on error.

## Concurrency & Locking

The Cloud layer does not take queue locks directly. The local Strategy delegates
to the normal manager locks. The PostgreSQL Strategy delegates mutations to
durable transactional commands and obtains a coherent read-only snapshot from
one repeatable-read transaction (see [Concurrency & Locking](./concurrency-and-locking.md)).
Coordination in the agent itself is single-threaded JS event-loop based:

- Snapshot scheduling is a self-re-arming `setTimeout` chain (`scheduleNext`), so at most one regular snapshot is in flight per cycle. The sequential `.then` chain controls normal sends; timer ownership and lifecycle checks reject stale callbacks after stop. `stop()` can still overlap that already-started send with its best-effort shutdown snapshot, as documented above.
- WS reconnect uses exponential backoff with jitter, doubling from 1s up to 30s, guarded by a single `reconnectTimer` so concurrent reconnects cannot stack (`wsSender.ts:188-201`).
- The dashboard sends `ping` every ~25s; the agent replies `pong` to keep the socket alive (`wsSender.ts:9,120`).
- HMAC `CryptoKey` is imported lazily once and cached (`hmacKey ??=`, `httpSender.ts:100`).

## Edge Cases & Failure Modes

- **Disabled by default / hard requirements:** missing `BUNQUEUE_CLOUD_URL` or `BUNQUEUE_CLOUD_API_KEY` → `null` (silent, no overhead). Missing `BUNQUEUE_CLOUD_INSTANCE_ID` → `console.error` + `null`, so a misconfigured instance silently never connects (`config.ts:17-21`).
- **Offline buffering:** failed sends are pushed into `SnapshotBuffer` (default cap 720); when full the **oldest** snapshot is dropped (`buffer.ts:15`). On reconnect the buffer is flushed FIFO before the new snapshot, so the dashboard backfills historical gaps. Buffer drain stops on the first re-failure to preserve order.
- **Circuit breaker:** after `threshold` (default 5) consecutive failures it OPENs; while open, `canExecute()` returns `false` and snapshots are buffered without a network call. After `resetMs` (default 60s) it transitions HALF_OPEN and allows one probe; a single failure in HALF_OPEN re-opens immediately (`circuitBreaker.ts:44`).
- **Event ring buffer:** `eventBuffer` is capped at `EVENT_BUFFER_MAX = 1000`; oldest events are dropped when full (`cloudAgent.ts:23,236`). Events filtered by `eventFilter` are discarded before buffering. Buffered events are only delivered embedded in the next HTTP snapshot, so with `useHttp:false` events are collected but never sent.
- **Redaction is shallow:** `redactData` replaces only **top-level** keys named in `redactFields` with `'[REDACTED]'`; nested fields are not traversed (`redact.ts:11`). With `includeJobData:false`, job data is omitted entirely from `recentJobs`/`dlqEntries`/events.
- **Read failure semantics:** the local Strategy preserves defensive per-queue
  collection. A PostgreSQL durable read-model failure rejects the complete
  snapshot rather than publishing a partially authoritative mix. Webhook and
  process-local stall probes remain best effort; stall details cap at 20 jobs.
- **Command safety:** only whitelisted actions execute; arguments are taken from optional fields with `?? ''`/defaults, so a missing `queue`/`jobId` becomes an empty-string call rather than a crash (`commands.ts`). Command handler errors are caught and returned as `success:false` rather than tearing down the socket.
- **WS best-effort send:** `sendRaw` no-ops if not connected and swallows send errors — a command result can be silently dropped if the socket flaps between receipt and reply (`wsSender.ts:93`).
- **Big-int safety:** lifetime counters are sent as strings in `CloudSnapshot.stats` to survive msgpack/JSON without precision loss (`snapshotCollector.ts:124-138`).
- **Metric authority:** PostgreSQL `totalCompleted`/`totalFailed` are durable
  namespace totals. `totalPushed`/`totalPulled`, latency, throughput, memory,
  connections, webhooks, and task/MCP values remain broker/process-local.

### Known stale / unwired code (verify before relying on)

- `CollectSnapshotParams.includeHeavy` is declared and always passed `true`, but `collectSnapshot` never reads it — the "light every 15s / heavy every 90s" file-header comment is aspirational; **every snapshot is full** (`snapshotCollector.ts:2-6`, `types/collector.ts:38-49`, callers `cloudAgent.ts:145-155`, `cloudAgent.ts:191-201`, `cloudAgent.ts:259-269`).
- `statsUpdateTimer` is declared and cleared in `stop()` but **never assigned** — there is no live 15s `stats_update` WS push (`cloudAgent.ts:32,128`).
- `buildStatsRefresh` (`statsRefresh.ts`) and `buildStatsUpdate` (`statsUpdate.ts`) are exported but not referenced anywhere in `src`; the `stats:refresh` command uses the selected adapter source. Treat both files as legacy/unwired.
- Post-command immediate-snapshot trigger is commented out — the dashboard is expected to refresh via WS command results, not via an HTTP re-push (`cloudAgent.ts:100-103`).

## Configuration

All via `Bun.env`, parsed once in `loadCloudConfig` (`config.ts`). **Defaults below are the actual code defaults.**

| Env var                                    | Default                | Notes                                                                             |
| ------------------------------------------ | ---------------------- | --------------------------------------------------------------------------------- |
| `BUNQUEUE_CLOUD_URL`                       | — (required)           | Trailing slashes stripped; `http→ws` for the command socket.                      |
| `BUNQUEUE_CLOUD_API_KEY`                   | — (required)           | Sent as `Authorization: Bearer …` on both channels.                               |
| `BUNQUEUE_CLOUD_INSTANCE_ID`               | — (required)           | Missing → error + disabled.                                                       |
| `BUNQUEUE_CLOUD_SIGNING_SECRET`            | `null`                 | Enables `X-Signature` HMAC-SHA256 over the compressed body.                       |
| `BUNQUEUE_CLOUD_INSTANCE_NAME`             | `os.hostname()`        | Human-readable label.                                                             |
| `BUNQUEUE_CLOUD_INTERVAL_MS`               | `15000`                | **Parsed but unused** — actual cadence is adaptive (5–30s) via `computeInterval`. |
| `BUNQUEUE_CLOUD_INCLUDE_JOB_DATA`          | `true` (`!== 'false'`) | Set to `false` to omit job data.                                                  |
| `BUNQUEUE_CLOUD_REDACT_FIELDS`             | `[]`                   | Comma-separated top-level keys → `'[REDACTED]'`.                                  |
| `BUNQUEUE_CLOUD_EVENTS`                    | `[]` (all)             | Comma-separated `eventType` allowlist.                                            |
| `BUNQUEUE_CLOUD_BUFFER_SIZE`               | `720`                  | Offline snapshot ring-buffer cap.                                                 |
| `BUNQUEUE_CLOUD_CIRCUIT_BREAKER_THRESHOLD` | `5`                    | Consecutive failures to OPEN.                                                     |
| `BUNQUEUE_CLOUD_CIRCUIT_BREAKER_RESET_MS`  | `60000`                | OPEN→HALF_OPEN delay.                                                             |
| `BUNQUEUE_CLOUD_USE_WEBSOCKET`             | `true` (`!== 'false'`) | Command channel.                                                                  |
| `BUNQUEUE_CLOUD_USE_HTTP`                  | `true` (`!== 'false'`) | Snapshot upload channel.                                                          |
| `BUNQUEUE_CLOUD_REMOTE_COMMANDS`           | `true` (`!== 'false'`) | Allows the dashboard to mutate this instance; set `false` for read-only.          |

`dataPath` is not an env var here — it is threaded in from the caller (`CloudAgent.create(qm, dataPath)`).

## Related Docs

- [Core Queue Engine](./core-queue-engine.md) — the telemetry source and command target.
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md) — local stats/Prometheus surfaces.
- [HTTP API](./http-api.md) — the in-process HTTP/SSE/WS server (distinct from the outbound Cloud client).
- [Job Queries & Queue Control](./job-queries-and-control.md), [Dead Letter Queue (DLQ)](./dead-letter-queue.md), [Scheduler & Cron](./scheduler-and-cron.md), [Webhooks, Events & Job Logs](./webhooks-and-events.md), [Workers Management](./workers-management.md) — operations invoked by remote commands.
- [Native MCP Server](./mcp-server.md) — one of the two entrypoints that constructs the agent.
- [S3 Backup](./backup-s3.md) — `s3:backup` command and `getBackupStatus` handle.
- [Configuration & Entrypoint](./configuration.md) — server bootstrap wiring of `setServerHandles`.
- [Security: TLS, Auth, CORS](./security-tls-auth.md) — auth/HMAC context.
- [architecture](../architecture.md), [data-model](../data-model.md).
